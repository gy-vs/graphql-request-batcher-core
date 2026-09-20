import { AsyncLocalStorage } from 'node:async_hooks';

/**
 * Request-scoped GraphQL field batcher.
 *
 * Scheduling protocol
 * --------------------
 * 1. A field resolver calls `load*()`. The key is normalized and placed in the
 *    request/access/batcher partition. Multiple calls with the same canonical
 *    key create independent promises but one `EntryState` and one waiter set.
 * 2. The dispatch is scheduled with `queueMicrotask`. All loads performed in
 *    the same synchronous GraphQL execution tick therefore coalesce.
 * 3. At flush time, queued keys are snapshotted in first-seen order and passed
 *    to the batch function exactly once. Keys whose last waiter was cancelled
 *    are removed before the snapshot.
 * 4. New loads made after the snapshot get a later microtask dispatch. This is
 *    how nested resolvers safely enqueue a second batch.
 * 5. Values, null values, and confirmed missing entities are cached by default
 *    for the remainder of the request partition. Per-key errors are cached only
 *    when explicitly requested. Whole-batch failures are never cached.
 * 6. Disposing the request scope rejects every unfinished waiter, aborts batch
 *    I/O through the supplied AbortSignal, and clears all partitions.
 *
 * Key protocol
 * ------------
 * Keys are reduced to a deterministic canonical string before coalescing or
 * caching. `null`, `undefined`, `false`, the string `"false"`, compound arrays,
 * and object fields therefore cannot collide. Plain objects use recursively
 * sorted enumerable string keys. Objects may implement `toBatchKey()`, and a
 * batcher may provide a custom normalizer for IDs, Buffers, Maps, etc.
 *
 * Result/backfill protocol
 * ------------------------
 * The batch function may return entries in any order. Every requested key must
 * appear exactly once. A result is one of:
 * - `{ status: 'value', key, value }` - a value, including `null`
 * - `{ status: 'missing', key }` - confirmed nonexistence
 * - `{ status: 'error', key, error, cache? }` - a per-key failure
 *
 * Omitting a key, returning it twice, or returning an unknown key invalidates
 * the entire batch and rejects every waiter in that dispatch; nothing is
 * cached and the keys may be retried.
 */

export type CanonicalBatchKey = string;

export type BatchResultEntry<K, V> =
  | { readonly status: 'value'; readonly key: K; readonly value: V }
  | { readonly status: 'missing'; readonly key: K }
  | {
      readonly status: 'error';
      readonly key: K;
      readonly error: unknown;
      readonly cache?: boolean;
    };

export type LoadResult<V> =
  | { readonly status: 'value'; readonly value: V }
  | { readonly status: 'missing' };

export interface BatchRunContext<K, C> {
  readonly batcherId: string;
  readonly keys: readonly K[];
  readonly context: C | undefined;
  readonly permissions: unknown;
  readonly snapshot: unknown;
  readonly signal: AbortSignal;
}

export type BatchRunner<K, V, C> = (
  context: BatchRunContext<K, C>,
) => Iterable<BatchResultEntry<K, V>> | PromiseLike<Iterable<BatchResultEntry<K, V>>>;

export interface BatcherDefinition<K, V, C = unknown> {
  readonly id: string;
  readonly run: BatchRunner<K, V, C>;
  /** Custom stable encoder. It must return a non-empty string. */
  readonly normalizeKey?: (key: K) => string;
  /** Cache resolved values, including null. Default: true. */
  readonly cacheValues?: boolean;
  /** Cache confirmed missing entities. Default: true. */
  readonly cacheMissing?: boolean;
  /** Default value for per-key error entries' `cache` flag. Default: false. */
  readonly cacheErrors?: boolean;
}

export interface BatchPromise<T> extends Promise<T> {
  /**
   * Cancel this field's promise. Cancelling one waiter never cancels a key that
   * still has other waiters. A no-op once the field has settled.
   */
  cancel(reason?: unknown): void;
}

export interface AccessIdentity {
  /**
   * Permission/authorization context. Object references are compared directly;
   * reuse a stable object or primitive for the same logical authorization.
   */
  readonly permissions?: unknown;
  /** Transaction/snapshot object or identifier. */
  readonly snapshot?: unknown;
}

export interface BatchScope<C = unknown> extends AccessIdentity {
  readonly context: C | undefined;
  readonly signal: AbortSignal;
  readonly disposed: boolean;

  batcher<K, V>(definition: BatcherDefinition<K, V, C>): FieldBatcher<K, V>;

  /**
   * Return a view of the same request and lifecycle with an isolated cache
   * partition. Different permissions or snapshots never share entries.
   */
  withAccess(access: AccessIdentity): BatchScope<C>;

  /**
   * Release this scope. Disposing the root request scope ends every access
   * view; disposing a view returned by `withAccess` ends only that partition.
   */
  dispose(reason?: unknown): void;
}

export interface FieldBatcher<K, V> {
  readonly id: string;
  /** Resolve a value; a confirmed missing entity rejects MissingResultError. */
  load(key: K): BatchPromise<V>;
  /** Resolve a value, or null for both null values and missing entities. */
  loadOptional(key: K): BatchPromise<V | null>;
  /** Resolve a state that distinguishes a null value from nonexistence. */
  loadState(key: K): BatchPromise<LoadResult<V>>;
  /** Drop a cached result, then load. In-flight joins are not interrupted. */
  refresh(key: K): BatchPromise<V>;
}

export interface CreateBatchScopeOptions<C> extends AccessIdentity {
  readonly context?: C;
  /** Aborting this signal disposes the request scope. */
  readonly signal?: AbortSignal;
}

export class BatcherError extends Error {
  constructor(message: string, cause?: unknown) {
    super(message);
    this.name = new.target.name;
    if (cause !== undefined) this.cause = cause;
  }
}

export class KeyNormalizationError extends BatcherError {
  constructor(
    public readonly batcherId: string,
    public readonly key: unknown,
    cause?: unknown,
  ) {
    super(`Batch key for batcher ${batcherId} could not be normalized`, cause);
  }
}

export class MissingResultError extends BatcherError {
  constructor(
    public readonly batcherId: string,
    public readonly key: unknown,
  ) {
    super(`Batch result for ${batcherId} key is missing`);
  }
}

export class BatchExecutionError extends BatcherError {
  constructor(
    public readonly batcherId: string,
    cause: unknown,
    public readonly key?: unknown,
  ) {
    super(`Batch function for ${batcherId} failed`, cause);
  }
}

export class InvalidBatchResultError extends BatcherError {
  constructor(
    public readonly batcherId: string,
    reason: string,
    cause?: unknown,
  ) {
    super(`Invalid result from batcher ${batcherId}: ${reason}`, cause);
  }
}

export class FieldCancelledError extends BatcherError {
  constructor(
    public readonly batcherId: string,
    public readonly key: unknown,
    reason?: unknown,
  ) {
    super(`Batch field ${batcherId} was cancelled`, reason);
  }
}

export class ScopeDisposedError extends BatcherError {
  constructor(
    public readonly batcherId?: string,
    public readonly key?: unknown,
    reason?: unknown,
  ) {
    super('Batch scope was disposed before the result completed', reason);
  }
}

export class BatchCycleError extends BatcherError {
  constructor(
    public readonly batcherId: string,
    public readonly key: unknown,
  ) {
    super(
      `Batcher ${batcherId} recursively waited for its own in-flight key; this would deadlock`,
    );
  }
}

export function defineBatcher<K, V, C = unknown>(
  definition: BatcherDefinition<K, V, C>,
): BatcherDefinition<K, V, C> {
  return definition;
}

/**
 * Canonical encoding for JSON-ish primitive, array, and plain-object keys.
 * Class instances can expose `toBatchKey()`; use a batcher normalizer for
 * other types.
 */
export function stableBatchKey(value: unknown): string {
  return writeKey(value, new Set<object>());
}

function writeKey(value: unknown, path: Set<object>): string {
  if (value === null) return 'null:';
  if (value === undefined) return 'undefined:';

  const type = typeof value;
  if (type === 'string') return `s${(value as string).length}:${value as string}`;
  if (type === 'number') return `n:${numberTag(value as number)}`;
  if (type === 'bigint') return `b:${String(value)}`;
  if (type === 'boolean') return `bool:${String(value)}`;
  if (type === 'symbol' || type === 'function') {
    throw new TypeError(`Unsupported batch key type ${type}`);
  }

  const object = value as { toBatchKey?: () => unknown };
  if (typeof object.toBatchKey === 'function') {
    const identity = object.toBatchKey();
    if (
      identity === null ||
      ['string', 'number', 'bigint', 'boolean', 'undefined'].includes(
        typeof identity,
      )
    ) {
      return `custom:${writeKey(identity, path)}`;
    }
    throw new TypeError('toBatchKey() must return a primitive key');
  }

  if (path.has(object)) throw new TypeError('Circular batch key');
  path.add(object);

  try {
    if (Array.isArray(value)) {
      return `a${value.length}:[${value.map((item) => writeKey(item, path)).join('')}]`;
    }

    if (isPlainObject(object)) {
      const record = object as Record<string, unknown>;
      const keys = Object.keys(record).sort();
      const body = keys
        .map((key) => `k${key.length}:${key}=${writeKey(record[key], path)}`)
        .join(';');
      return `o${keys.length}:{${body}}`;
    }

    throw new TypeError(
      'Batch key must be a primitive, array, plain object, or implement toBatchKey()',
    );
  } finally {
    path.delete(object);
  }
}

function numberTag(value: number): string {
  if (Number.isNaN(value)) return 'NaN';
  if (Object.is(value, -0)) return '-0';
  if (value === Number.POSITIVE_INFINITY) return 'Infinity';
  if (value === Number.NEGATIVE_INFINITY) return '-Infinity';
  return String(value);
}

function isPlainObject(value: object): value is Record<string, unknown> {
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}

function canonicalize<K, V, C>(
  definition: BatcherDefinition<K, V, C>,
  key: K,
): CanonicalBatchKey {
  try {
    if (definition.normalizeKey) {
      const normalized = definition.normalizeKey(key);
      if (typeof normalized !== 'string' || normalized.length === 0) {
        throw new TypeError('Key normalizer must return a non-empty string');
      }
      return `x:${normalized}`;
    }
    return stableBatchKey(key);
  } catch (cause) {
    throw new KeyNormalizationError(definition.id, key, cause);
  }
}

type InternalEntryStatus = 'queued' | 'running' | 'value' | 'missing' | 'error';
type WaiterKind = 'required' | 'optional' | 'state';

interface Waiter {
  kind: WaiterKind;
  settled: boolean;
  resolve: (value: unknown) => void;
  reject: (reason: unknown) => void;
}

interface EntryState<K> {
  batcherId: string;
  key: K;
  canonical: CanonicalBatchKey;
  status: InternalEntryStatus;
  waiters: Set<Waiter>;
  value?: unknown;
  error?: unknown;
}

interface BatcherState {
  queued: Map<CanonicalBatchKey, EntryState<unknown>>;
  entries: Map<CanonicalBatchKey, EntryState<unknown>>;
  scheduled: boolean;
}

interface ViewState {
  batchers: Map<BatcherDefinition<unknown, unknown, unknown>, BatcherState>;
  controller: AbortController;
  disposed: boolean;
  disposeReason?: unknown;
  unsubscribeRootSignal?: () => void;
}

interface ScopeRoot<C> {
  context: C | undefined;
  controller: AbortController;
  disposed: boolean;
  disposeReason?: unknown;
  views: Map<unknown, Map<unknown, ViewState>>;
  unsubscribeExternalSignal?: () => void;
}

interface ActiveBatchRun {
  batcher: BatcherDefinition<unknown, unknown, unknown>;
  keys: ReadonlySet<CanonicalBatchKey>;
  parent?: ActiveBatchRun;
}

const activeRuns = new AsyncLocalStorage<ActiveBatchRun>();

class FieldBatcherImpl<K, V, C> implements FieldBatcher<K, V> {
  constructor(
    private readonly scope: ScopeImpl<C>,
    private readonly definition: BatcherDefinition<K, V, C>,
    private readonly view: ViewState,
    private readonly state: BatcherState,
  ) {}

  get id(): string {
    return this.definition.id;
  }

  load(key: K): BatchPromise<V> {
    return this.begin<V>(key, 'required');
  }

  loadOptional(key: K): BatchPromise<V | null> {
    return this.begin<V | null>(key, 'optional');
  }

  loadState(key: K): BatchPromise<LoadResult<V>> {
    return this.begin<LoadResult<V>>(key, 'state');
  }

  refresh(key: K): BatchPromise<V> {
    if (this.scope.root.disposed || this.view.disposed) {
      return rejectedBatchPromise(
        new ScopeDisposedError(
          this.definition.id,
          key,
          this.scope.root.disposed ? this.scope.root.disposeReason : this.view.disposeReason,
        ),
      );
    }

    let canonical: CanonicalBatchKey;
    try {
      canonical = canonicalize(this.definition, key);
    } catch (error) {
      return rejectedBatchPromise(error);
    }

    if (isActiveInCurrentBatch(
      this.definition as BatcherDefinition<unknown, unknown, unknown>,
      canonical,
    )) {
      return rejectedBatchPromise(new BatchCycleError(this.definition.id, key));
    }

    const entry = this.state.entries.get(canonical);
    if (entry && (entry.status === 'value' || entry.status === 'missing' || entry.status === 'error')) {
      this.state.entries.delete(canonical);
    }

    return this.load(key);
  }

  private begin<T>(key: K, kind: WaiterKind): BatchPromise<T> {
    if (this.scope.root.disposed || this.view.disposed) {
      return rejectedBatchPromise(
        new ScopeDisposedError(
          this.definition.id,
          key,
          this.scope.root.disposed ? this.scope.root.disposeReason : this.view.disposeReason,
        ),
      );
    }

    let canonical: CanonicalBatchKey;
    try {
      canonical = canonicalize(this.definition, key);
    } catch (error) {
      return rejectedBatchPromise(error);
    }

    if (isActiveInCurrentBatch(
      this.definition as BatcherDefinition<unknown, unknown, unknown>,
      canonical,
    )) {
      return rejectedBatchPromise(new BatchCycleError(this.definition.id, key));
    }

    let entry = this.state.entries.get(canonical) as EntryState<K> | undefined;
    if (entry) {
      if (entry.status === 'queued' || entry.status === 'running') {
        return this.invite<T>(entry, kind);
      }
      return settledAs<T>(entry, kind);
    }

    entry = {
      batcherId: this.definition.id,
      key,
      canonical,
      status: 'queued',
      waiters: new Set<Waiter>(),
    };
    this.state.entries.set(canonical, entry as EntryState<unknown>);
    this.state.queued.set(canonical, entry as EntryState<unknown>);
    schedule(this.scope, this.view, this.definition, this.state);

    return this.invite<T>(entry, kind);
  }

  private invite<T>(entry: EntryState<K>, kind: WaiterKind): BatchPromise<T> {
    let cancel: (reason?: unknown) => void = () => {};
    const promise = new Promise<T>((resolve, reject) => {
      const waiter: Waiter = {
        kind,
        settled: false,
        resolve: resolve as (value: unknown) => void,
        reject,
      };
      entry.waiters.add(waiter);

      cancel = (reason?: unknown) => {
        if (waiter.settled) return;
        waiter.settled = true;
        entry.waiters.delete(waiter);
        waiter.reject(new FieldCancelledError(this.definition.id, entry.key, reason));

        // The batch has not been snapshotted, so a key with no remaining
        // waiters is removed and is not sent to the batch function.
        if (entry.status === 'queued' && entry.waiters.size === 0) {
          this.state.queued.delete(entry.canonical);
          this.state.entries.delete(entry.canonical);
        }
      };
    }) as BatchPromise<T>;

    promise.cancel = (reason?: unknown) => cancel(reason);
    return promise;
  }
}

function settledAs<T>(entry: EntryState<unknown>, kind: WaiterKind): BatchPromise<T> {
  const promise = Promise.resolve().then(() => deliver<T>(entry, kind)) as BatchPromise<T>;
  promise.cancel = () => {};
  return promise;
}

function deliver<T>(entry: EntryState<unknown>, kind: WaiterKind): T {
  if (entry.status === 'value') {
    if (kind === 'state') {
      return { status: 'value', value: entry.value } as T;
    }
    return entry.value as T;
  }

  if (entry.status === 'missing') {
    if (kind === 'state') return { status: 'missing' } as T;
    if (kind === 'optional') return null as T;
    throw new MissingResultError(entry.batcherId, entry.key);
  }

  throw entry.error;
}

function rejectedBatchPromise<T>(reason: unknown): BatchPromise<T> {
  const promise = Promise.reject(reason) as BatchPromise<T>;
  // Disposed/cyclic call sites can legitimately fire and forget; retain the
  // rejection for any caller chain while preventing an unhandled-rejection
  // event for callers that do not observe it.
  promise.catch(() => {});
  promise.cancel = () => {};
  return promise;
}

function isActiveInCurrentBatch(
  batcher: BatcherDefinition<unknown, unknown, unknown>,
  canonical: CanonicalBatchKey,
): boolean {
  for (let active = activeRuns.getStore(); active; active = active.parent) {
    if (active.batcher === batcher && active.keys.has(canonical)) return true;
  }
  return false;
}

function schedule<K, V, C>(
  scope: ScopeImpl<C>,
  view: ViewState,
  definition: BatcherDefinition<K, V, C>,
  state: BatcherState,
): void {
  if (state.scheduled || scope.root.disposed || view.disposed) return;
  state.scheduled = true;

  queueMicrotask(() => {
    state.scheduled = false;
    void flush(scope, view, definition, state);
  });
}

async function flush<K, V, C>(
  scope: ScopeImpl<C>,
  view: ViewState,
  definition: BatcherDefinition<K, V, C>,
  state: BatcherState,
): Promise<void> {
  const root = scope.root;
  if (root.disposed || view.disposed) return;

  const queued = Array.from(state.queued.values());
  state.queued.clear();

  const entries = queued.filter((entry) => entry.waiters.size > 0);
  if (entries.length === 0) return;

  const byKey = new Map<CanonicalBatchKey, EntryState<unknown>>();
  const activeKeys = new Set<CanonicalBatchKey>();
  for (const entry of entries) {
    entry.status = 'running';
    byKey.set(entry.canonical, entry);
    activeKeys.add(entry.canonical);
  }

  const active: ActiveBatchRun = {
    batcher: definition as BatcherDefinition<unknown, unknown, unknown>,
    keys: activeKeys,
    parent: activeRuns.getStore(),
  };

  try {
    const result = await activeRuns.run(active, () =>
      Promise.resolve(
        definition.run({
          batcherId: definition.id,
          keys: entries.map((entry) => entry.key as K),
          context: root.context,
          permissions: scope.permissions,
          snapshot: scope.snapshot,
          signal: view.controller.signal,
        }),
      ),
    );

    if (root.disposed || view.disposed) return;
    commitResults(definition, state, byKey, result);
  } catch (cause) {
    if (root.disposed || view.disposed) return;

    for (const entry of entries) {
      const perKeyError =
        cause instanceof InvalidBatchResultError
          ? cause
          : new BatchExecutionError(definition.id, cause, entry.key);
      rejectAndRemove(state, entry, perKeyError);
    }
  }
}

function commitResults<K, V, C>(
  definition: BatcherDefinition<K, V, C>,
  state: BatcherState,
  expected: Map<CanonicalBatchKey, EntryState<unknown>>,
  result: Iterable<BatchResultEntry<K, V>>,
): void {
  const list = Array.from(result);
  const seen = new Set<CanonicalBatchKey>();
  const updates: Array<[EntryState<unknown>, BatchResultEntry<K, V>]> = [];

  for (const item of list) {
    if (!item || typeof item !== 'object') {
      throw new InvalidBatchResultError(definition.id, 'result entry is not an object');
    }

    const canonical = canonicalize(definition, item.key);
    const entry = expected.get(canonical);
    if (!entry) {
      throw new InvalidBatchResultError(
        definition.id,
        `result contains key that was not requested: ${String(canonical)}`,
      );
    }
    if (seen.has(canonical)) {
      throw new InvalidBatchResultError(
        definition.id,
        `result contains duplicate key: ${String(canonical)}`,
      );
    }

    if (item.status === 'value') {
      if (!('value' in item)) {
        throw new InvalidBatchResultError(definition.id, 'value entry lacks value');
      }
    } else if (item.status === 'missing') {
      // Distinct from value: null.
    } else if (item.status === 'error') {
      if (!('error' in item)) {
        throw new InvalidBatchResultError(definition.id, 'error entry lacks error');
      }
    } else {
      throw new InvalidBatchResultError(definition.id, 'unknown result status');
    }

    seen.add(canonical);
    updates.push([entry, item]);
  }

  if (seen.size !== expected.size) {
    throw new InvalidBatchResultError(
      definition.id,
      `${expected.size - seen.size} requested key(s) were omitted from the result`,
    );
  }

  for (const [entry, item] of updates) {
    let cache: boolean;

    if (item.status === 'value') {
      entry.status = 'value';
      entry.value = item.value;
      entry.error = undefined;
      cache = definition.cacheValues !== false;
    } else if (item.status === 'missing') {
      entry.status = 'missing';
      entry.value = undefined;
      entry.error = undefined;
      cache = definition.cacheMissing !== false;
    } else {
      entry.status = 'error';
      entry.value = undefined;
      entry.error = item.error;
      cache = item.cache ?? definition.cacheErrors ?? false;
    }

    settleWaiters(entry);
    if (!cache) state.entries.delete(entry.canonical);
  }
}

function settleWaiters(entry: EntryState<unknown>): void {
  const waiters = Array.from(entry.waiters);
  entry.waiters.clear();

  for (const waiter of waiters) {
    waiter.settled = true;
    try {
      waiter.resolve(deliver(entry, waiter.kind));
    } catch (error) {
      waiter.reject(error);
    }
  }
}

function rejectAndRemove(
  state: BatcherState,
  entry: EntryState<unknown>,
  error: unknown,
): void {
  rejectWaiters(entry, error);
  state.entries.delete(entry.canonical);
}

function rejectWaiters(entry: EntryState<unknown>, error: unknown): void {
  for (const waiter of Array.from(entry.waiters)) {
    if (waiter.settled) continue;
    waiter.settled = true;
    waiter.reject(error);
  }
  entry.waiters.clear();
}

class ScopeImpl<C> implements BatchScope<C> {
  constructor(
    readonly root: ScopeRoot<C>,
    readonly permissions: unknown = undefined,
    readonly snapshot: unknown = undefined,
    private readonly isRootView = false,
  ) {}

  get context(): C | undefined {
    return this.root.context;
  }

  get signal(): AbortSignal {
    return this.view().controller.signal;
  }

  get disposed(): boolean {
    return this.root.disposed || this.view().disposed;
  }

  batcher<K, V>(definition: BatcherDefinition<K, V, C>): FieldBatcher<K, V> {
    if (this.root.disposed) {
      throw new ScopeDisposedError(definition.id, undefined, this.root.disposeReason);
    }

    const view = this.view();
    if (view.disposed) {
      throw new ScopeDisposedError(definition.id, undefined, view.disposeReason);
    }

    let state = view.batchers.get(
      definition as BatcherDefinition<unknown, unknown, unknown>,
    );
    if (!state) {
      state = {
        queued: new Map(),
        entries: new Map(),
        scheduled: false,
      };
      view.batchers.set(
        definition as BatcherDefinition<unknown, unknown, unknown>,
        state,
      );
    }

    return new FieldBatcherImpl(this, definition, view, state) as FieldBatcher<K, V>;
  }

  withAccess(access: AccessIdentity): BatchScope<C> {
    if (this.root.disposed) {
      throw new ScopeDisposedError(undefined, undefined, this.root.disposeReason);
    }
    return new ScopeImpl(
      this.root,
      access.permissions,
      access.snapshot,
    );
  }

  dispose(reason?: unknown): void {
    if (this.isRootView) {
      disposeRoot(this.root, reason);
    } else {
      disposeView(this.root, this.permissions, this.snapshot, reason);
    }
  }

  view(): ViewState {
    let bySnapshot = this.root.views.get(this.permissions);
    if (!bySnapshot) {
      bySnapshot = new Map<unknown, ViewState>();
      this.root.views.set(this.permissions, bySnapshot);
    }

    let view = bySnapshot.get(this.snapshot);
    if (!view) {
      const controller = new AbortController();
      view = {
        batchers: new Map(),
        controller,
        disposed: false,
      };
      bySnapshot.set(this.snapshot, view);

      const onRootAbort = () => disposeView(
        this.root,
        this.permissions,
        this.snapshot,
        this.root.disposeReason,
      );
      if (this.root.controller.signal.aborted) {
        onRootAbort();
      } else {
        this.root.controller.signal.addEventListener('abort', onRootAbort, {
          once: true,
        });
        view.unsubscribeRootSignal = () => {
          this.root.controller.signal.removeEventListener('abort', onRootAbort);
        };
      }
    }
    return view;
  }
}

function disposeView<C>(
  root: ScopeRoot<C>,
  permissions: unknown,
  snapshot: unknown,
  reason?: unknown,
): void {
  if (root.disposed) {
    const bySnapshot = root.views.get(permissions);
    const view = bySnapshot?.get(snapshot);
    if (view) disposeExistingView(view, root.disposeReason);
    return;
  }

  const bySnapshot = root.views.get(permissions);
  const view = bySnapshot?.get(snapshot);
  if (!view || view.disposed) return;
  disposeExistingView(view, reason);
}

function disposeExistingView(view: ViewState, reason?: unknown): void {
  const disposeError = new ScopeDisposedError(undefined, undefined, reason);
  view.disposed = true;
  view.disposeReason = reason ?? disposeError;
  view.controller.abort(view.disposeReason);
  view.unsubscribeRootSignal?.();

  for (const [definition, state] of view.batchers) {
    for (const entry of state.entries.values()) {
      if (entry.status === 'queued' || entry.status === 'running') {
        rejectWaiters(
          entry,
          new ScopeDisposedError(
            (definition as { id?: string }).id,
            entry.key,
            view.disposeReason,
          ),
        );
      }
    }
    state.queued.clear();
    state.entries.clear();
  }
  view.batchers.clear();
}

function disposeRoot<C>(root: ScopeRoot<C>, reason?: unknown): void {
  if (root.disposed) return;

  const disposeError = new ScopeDisposedError(undefined, undefined, reason);
  root.disposed = true;
  root.disposeReason = reason ?? disposeError;
  root.controller.abort(root.disposeReason);
  root.unsubscribeExternalSignal?.();

  for (const bySnapshot of root.views.values()) {
    for (const view of bySnapshot.values()) {
      if (!view.disposed) disposeExistingView(view, root.disposeReason);
    }
  }
  root.views.clear();
}

export function createBatchScope<C = unknown>(
  options: CreateBatchScopeOptions<C> = {},
): BatchScope<C> {
  const root: ScopeRoot<C> = {
    context: options.context,
    controller: new AbortController(),
    disposed: false,
    views: new Map(),
  };

  const scope = new ScopeImpl(root, options.permissions, options.snapshot, true);

  if (options.signal) {
    const externalSignal = options.signal;
    const onExternalAbort = () => scope.dispose(externalSignal.reason);
    root.unsubscribeExternalSignal = () => {
      externalSignal.removeEventListener('abort', onExternalAbort);
    };

    if (externalSignal.aborted) {
      scope.dispose(externalSignal.reason);
    } else {
      externalSignal.addEventListener('abort', onExternalAbort, { once: true });
    }
  }

  return scope;
}
