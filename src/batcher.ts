import { AsyncLocalStorage } from 'node:async_hooks';
import { normalizeKey } from './key.js';

/** 请求已结束：未完成的加载一律以此错误拒绝。 */
export class RequestClosedError extends Error {
  constructor(message = 'request batcher is closed') {
    super(message);
    this.name = 'RequestClosedError';
  }
}

/** 批量函数执行期间递归加载本批在途键：立即拒绝，避免死锁/死循环。 */
export class RecursiveLoadError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'RecursiveLoadError';
  }
}

const VALUE = Symbol('BatchResult.value');
const ERROR = Symbol('BatchResult.error');

/**
 * 单个键的回填结果：
 * - 普通值（含 `null`）——该键解析为此值并写入缓存；
 * - `Error` 实例或 {@link err} 包装——该键以此错误拒绝（逐键错误）；
 * - {@link ok} 包装——值本身是 `Error` 时的显式成功形式。
 */
export type BatchResult<V> =
  | V
  | Error
  | { readonly [VALUE]: true; readonly value: V }
  | { readonly [ERROR]: true; readonly error: unknown };

/** 显式成功结果，用于值本身可能是 `Error` 的场景。 */
export function ok<V>(value: V): BatchResult<V> {
  return { [VALUE]: true, value };
}

/** 逐键错误结果。 */
export function err(error: unknown): BatchResult<never> {
  return { [ERROR]: true, error };
}

/**
 * 批量函数协议：
 * - 入参是去重后的键（按首次入队顺序，保留原始键对象）；
 * - 返回 `[键, 结果]` 对的任意可迭代对象（`Map` 亦可），允许乱序、
 *   允许部分缺失；返回的键按规范化形式与请求键匹配，不要求对象同一；
 * - 缺失的键以 `missingValue`（默认 `null`）解析，且不写缓存——
 *   “取到空值”会缓存，“不存在”不缓存，两者严格区分；
 * - 未请求的键忽略；同一键重复返回时以先出现者为准；
 * - 整体抛错/拒绝时，本批所有等待者以同一错误拒绝，且不写缓存。
 */
export type BatchLoadFn<K, V> = (
  keys: readonly K[],
) =>
  | Iterable<readonly [K, BatchResult<V>]>
  | Promise<Iterable<readonly [K, BatchResult<V>]>>;

export interface LoaderOptions<K, V> {
  batchFn: BatchLoadFn<K, V>;
  /** 自定义键规范化；默认使用 {@link normalizeKey}。 */
  cacheKeyFn?: (key: K) => string;
  /** 单批最大键数，超出切片为多个批次；默认不限。 */
  maxBatchSize?: number;
  /** 是否启用请求内缓存，默认 true。 */
  cache?: boolean;
  /** 逐键错误是否写缓存，默认 true。整体失败永不写缓存。 */
  cacheErrors?: boolean;
  /** 结果中缺失的键以此值解析（默认 null），且不写缓存。 */
  missingValue?: V;
}

export interface LoadOptions {
  /** 取消单个字段的加载；不影响同一键的其他等待者。 */
  signal?: AbortSignal;
}

export interface LoaderHandle<K, V> {
  load(key: K, options?: LoadOptions): Promise<V>;
  /** 逐键 settle，单键错误不会拖垮整批调用方。 */
  loadMany(
    keys: readonly K[],
    options?: LoadOptions,
  ): Promise<Array<PromiseSettledResult<V>>>;
}

export interface LoaderRegistry {
  /**
   * 按名获取（或首次注册）loader。同一作用域内同名 loader 只注册一次，
   * 后续调用返回既有实例并忽略新选项。
   */
  loader<K, V>(name: string, options: LoaderOptions<K, V>): LoaderHandle<K, V>;
}

export interface RequestBatcher {
  /** 默认（无上下文）作用域。 */
  loader<K, V>(name: string, options: LoaderOptions<K, V>): LoaderHandle<K, V>;
  /**
   * 权限/事务作用域。`ctx` 经键规范化区分作用域（应包含权限主体与事务
   * 快照标识），也可用 `scopeId` 显式指定。规范串相同的作用域共享
   * loader 与缓存；不同作用域的批次与缓存完全隔离，绝不共享结果。
   */
  forContext(ctx: unknown, scopeId?: string): LoaderRegistry;
  readonly closed: boolean;
  /**
   * 请求结束：拒绝全部未完成项（已排队与在途），清空并释放缓存；
   * 返回的 promise 在在途批量函数全部 settle（结果被丢弃）后兑现。
   */
  close(): Promise<void>;
}

type CacheSlot<V> =
  | { readonly ok: true; readonly value: V }
  | { readonly ok: false; readonly error: unknown };

interface Waiter<V> {
  resolve(value: V): void;
  reject(reason: unknown): void;
  signal?: AbortSignal;
  onAbort?: () => void;
}

interface Entry<K, V> {
  key: K;
  waiters: Array<Waiter<V>>;
}

/** 批量函数执行上下文：记录调用链上所有正在执行的 loader，用于递归防护。 */
interface DispatchContext {
  readonly dispatching: ReadonlySet<object>;
}

function toSlot<V>(raw: BatchResult<V>): CacheSlot<V> {
  if (raw instanceof Error) return { ok: false, error: raw };
  if (typeof raw === 'object' && raw !== null) {
    const branded = raw as Record<PropertyKey, unknown>;
    if (branded[VALUE] === true) return { ok: true, value: branded.value as V };
    if (branded[ERROR] === true) return { ok: false, error: branded.error };
  }
  return { ok: true, value: raw as V };
}

class Loader<K, V> implements LoaderHandle<K, V> {
  private readonly cache = new Map<string, CacheSlot<V>>();
  private readonly queue = new Map<string, Entry<K, V>>();
  private readonly inflight = new Map<string, Entry<K, V>>();
  private readonly active = new Set<Promise<void>>();
  private readonly cacheEnabled: boolean;
  private readonly cacheErrors: boolean;
  private readonly maxBatchSize: number;
  private readonly missingValue: V;
  private readonly keyFn: (key: K) => string;
  private scheduled = false;
  private closed = false;

  constructor(
    private readonly options: LoaderOptions<K, V>,
    private readonly als: AsyncLocalStorage<DispatchContext>,
  ) {
    this.cacheEnabled = options.cache !== false;
    this.cacheErrors = options.cacheErrors !== false;
    this.maxBatchSize = Math.max(1, options.maxBatchSize ?? Infinity);
    this.missingValue = options.missingValue ?? (null as V);
    this.keyFn = options.cacheKeyFn ?? ((key: K) => normalizeKey(key));
  }

  load(key: K, loadOptions: LoadOptions = {}): Promise<V> {
    const { signal } = loadOptions;
    if (this.closed) return Promise.reject(new RequestClosedError());
    if (signal?.aborted) return Promise.reject(signal.reason);
    let cacheKey: string;
    try {
      cacheKey = this.keyFn(key);
    } catch (error) {
      return Promise.reject(error);
    }
    if (this.cacheEnabled) {
      const hit = this.cache.get(cacheKey);
      if (hit !== undefined) {
        // 每次调用都返回独立 promise；缓存槽区分空值与不存在（has 语义）。
        return hit.ok ? Promise.resolve(hit.value) : Promise.reject(hit.error);
      }
    }
    const flying = this.inflight.get(cacheKey);
    if (flying) {
      const ctx = this.als.getStore();
      if (ctx && ctx.dispatching.has(this as object)) {
        return Promise.reject(
          new RecursiveLoadError(
            `recursive load of in-flight key ${cacheKey} from within its own batch function`,
          ),
        );
      }
      // 在途键只发送一次：后来的调用方并入等待者，各自持有独立 promise。
      return this.attach(flying, signal);
    }
    let entry = this.queue.get(cacheKey);
    if (!entry) {
      entry = { key, waiters: [] };
      this.queue.set(cacheKey, entry);
      this.schedule();
    }
    return this.attach(entry, signal, () => {
      // 最后一个等待者取消且尚未派发：把键从队列中摘除，不再发送。
      if (this.queue.get(cacheKey) === entry) this.queue.delete(cacheKey);
    });
  }

  loadMany(
    keys: readonly K[],
    options: LoadOptions = {},
  ): Promise<Array<PromiseSettledResult<V>>> {
    return Promise.allSettled(keys.map((key) => this.load(key, options)));
  }

  private attach(
    entry: Entry<K, V>,
    signal: AbortSignal | undefined,
    onEmpty?: () => void,
  ): Promise<V> {
    return new Promise<V>((resolve, reject) => {
      const waiter: Waiter<V> = { resolve, reject };
      if (signal) {
        const onAbort = () => {
          const index = entry.waiters.indexOf(waiter);
          if (index >= 0) entry.waiters.splice(index, 1);
          reject(signal.reason);
          if (entry.waiters.length === 0) onEmpty?.();
        };
        waiter.onAbort = onAbort;
        waiter.signal = signal;
        signal.addEventListener('abort', onAbort, { once: true });
      }
      entry.waiters.push(waiter);
    });
  }

  private settle(entry: Entry<K, V>, slot: CacheSlot<V>): void {
    const waiters = entry.waiters.splice(0);
    for (const waiter of waiters) {
      if (waiter.signal && waiter.onAbort) {
        waiter.signal.removeEventListener('abort', waiter.onAbort);
      }
      if (slot.ok) waiter.resolve(slot.value);
      else waiter.reject(slot.error);
    }
  }

  private schedule(): void {
    if (this.scheduled) return;
    this.scheduled = true;
    queueMicrotask(() => {
      this.scheduled = false;
      this.flush();
    });
  }

  /**
   * 刷新只处理本轮开始时已排队的键；派发期间（含批量函数同步执行中）
   * 新入队的键进入下一轮，绝不在本轮递归刷新。
   */
  private flush(): void {
    if (this.closed) {
      const stranded = [...this.queue.values()];
      this.queue.clear();
      for (const entry of stranded) {
        this.settle(entry, { ok: false, error: new RequestClosedError() });
      }
      return;
    }
    let remaining = this.queue.size;
    while (remaining > 0 && this.queue.size > 0) {
      const chunk = new Map<string, Entry<K, V>>();
      for (const [cacheKey, entry] of this.queue) {
        chunk.set(cacheKey, entry);
        if (chunk.size >= this.maxBatchSize) break;
      }
      for (const cacheKey of chunk.keys()) this.queue.delete(cacheKey);
      for (const [cacheKey, entry] of chunk) this.inflight.set(cacheKey, entry);
      remaining -= chunk.size;
      this.dispatch(chunk);
    }
  }

  private dispatch(chunk: Map<string, Entry<K, V>>): void {
    const keys = [...chunk.values()].map((entry) => entry.key);
    const parent = this.als.getStore();
    const dispatching = new Set(parent?.dispatching);
    dispatching.add(this as object);
    let results: ReturnType<BatchLoadFn<K, V>>;
    try {
      results = this.als.run({ dispatching }, () => this.options.batchFn(keys));
    } catch (error) {
      this.failChunk(chunk, error);
      return;
    }
    const done: Promise<void> = Promise.resolve(results).then(
      (resolved) => {
        try {
          this.backfill(chunk, resolved);
        } catch (error) {
          this.failChunk(chunk, error);
        }
      },
      (error: unknown) => this.failChunk(chunk, error),
    );
    this.active.add(done);
    void done.finally(() => this.active.delete(done));
  }

  /** 批量函数整体失败：本批全部等待者以同一错误拒绝，不写缓存。 */
  private failChunk(chunk: Map<string, Entry<K, V>>, error: unknown): void {
    for (const [cacheKey, entry] of chunk) {
      this.inflight.delete(cacheKey);
      this.settle(entry, { ok: false, error });
    }
  }

  /** 按键回填：乱序/部分缺失/逐键错误/未知键/重复键均在此规范化。 */
  private backfill(
    chunk: Map<string, Entry<K, V>>,
    results: Iterable<readonly [K, BatchResult<V>]>,
  ): void {
    if (this.closed) {
      // 请求已结束：等待者已被拒绝、缓存已释放，迟到结果直接丢弃。
      for (const cacheKey of chunk.keys()) this.inflight.delete(cacheKey);
      return;
    }
    const slots = new Map<string, CacheSlot<V>>();
    for (const pair of results) {
      if (!Array.isArray(pair) || pair.length !== 2) continue;
      const [key, raw] = pair;
      let cacheKey: string;
      try {
        cacheKey = this.keyFn(key);
      } catch {
        continue;
      }
      if (!chunk.has(cacheKey) || slots.has(cacheKey)) continue;
      slots.set(cacheKey, toSlot(raw));
    }
    for (const [cacheKey, entry] of chunk) {
      this.inflight.delete(cacheKey);
      const slot = slots.get(cacheKey);
      if (slot !== undefined) {
        if (this.cacheEnabled && (slot.ok || this.cacheErrors)) {
          this.cache.set(cacheKey, slot);
        }
        this.settle(entry, slot);
      } else {
        // 键在结果中缺失：以 missingValue 解析，但不写缓存（与空值区分）。
        this.settle(entry, { ok: true, value: this.missingValue });
      }
    }
  }

  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    const error = new RequestClosedError();
    const queued = [...this.queue.values()];
    this.queue.clear();
    const flying = [...this.inflight.values()];
    this.inflight.clear();
    for (const entry of queued) this.settle(entry, { ok: false, error });
    for (const entry of flying) this.settle(entry, { ok: false, error });
    this.cache.clear();
    await Promise.allSettled([...this.active]);
  }
}

/**
 * 创建请求作用域的批处理器。每个 GraphQL 请求应持有独立实例，
 * 请求结束时调用 {@link RequestBatcher.close}。
 */
export function createRequestBatcher(): RequestBatcher {
  const als = new AsyncLocalStorage<DispatchContext>();
  const scopes = new Map<string, Map<string, Loader<never, never>>>();
  let closed = false;

  const registryFor = (scopeKey: string): LoaderRegistry => {
    let loaders = scopes.get(scopeKey);
    if (!loaders) {
      loaders = new Map();
      scopes.set(scopeKey, loaders);
    }
    const registry = loaders;
    return {
      loader<K, V>(name: string, options: LoaderOptions<K, V>): LoaderHandle<K, V> {
        const existing = registry.get(name);
        if (existing) return existing as unknown as LoaderHandle<K, V>;
        const created = new Loader<K, V>(options, als);
        if (closed) void created.close();
        registry.set(name, created as unknown as Loader<never, never>);
        return created;
      },
    };
  };

  return {
    loader<K, V>(name: string, options: LoaderOptions<K, V>): LoaderHandle<K, V> {
      return registryFor(`scope:${normalizeKey(undefined)}`).loader(name, options);
    },
    forContext(ctx: unknown, scopeId?: string): LoaderRegistry {
      const scopeKey = scopeId ?? normalizeKey(ctx);
      return registryFor(`scope:${scopeKey}`);
    },
    get closed() {
      return closed;
    },
    async close() {
      if (closed) return;
      closed = true;
      const loaders = [...scopes.values()].flatMap((scope) => [...scope.values()]);
      await Promise.all(loaders.map((loader) => loader.close()));
      scopes.clear();
    },
  };
}
