import { describe, expect, it, vi } from 'vitest';
import {
  BatchCycleError,
  BatchExecutionError,
  FieldCancelledError,
  InvalidBatchResultError,
  MissingResultError,
  ScopeDisposedError,
  createBatchScope,
  defineBatcher,
  stableBatchKey,
  type BatchResultEntry,
  type BatchScope,
} from '../src/index.js';

interface User {
  id: string;
  name: string;
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

const userBatcher = defineBatcher<string, User>({
  id: 'user',
  run: vi.fn(),
});

function value<K, V>(key: K, value: V): BatchResultEntry<K, V> {
  return { status: 'value', key, value };
}

function missing<K, V>(key: K): BatchResultEntry<K, V> {
  return { status: 'missing', key };
}

function error<K, V>(
  key: K,
  error: unknown,
  cache = false,
): BatchResultEntry<K, V> {
  return { status: 'error', key, error, cache };
}

describe('batch scheduling', () => {
  it('coalesces same-tick duplicate keys into one runner call', async () => {
    const run = vi.fn(async ({ keys }) =>
      keys.map((key) => value(key, { id: key, name: `User ${key}` })),
    );
    const scope = createBatchScope();
    const users = scope.batcher(defineBatcher({ id: 'user', run }));

    const [a, b, c] = await Promise.all([
      users.load('1'),
      users.load('1'),
      users.load('2'),
    ]);

    expect(run).toHaveBeenCalledTimes(1);
    expect(run.mock.calls[0][0].keys).toEqual(['1', '2']);
    expect(a).toEqual(b);
    expect(c.name).toBe('User 2');
    expect(a).not.toBe(c);
  });

  it('returns independent promises even when their key is deduplicated', async () => {
    const run = vi.fn(async ({ keys }) =>
      keys.map((key) => value(key, { id: key, name: 'same object' })),
    );
    const scope = createBatchScope();
    const users = scope.batcher(defineBatcher({ id: 'user', run }));

    const p1 = users.load('1');
    const p2 = users.load('1');
    expect(p1).not.toBe(p2);
    p2.cancel();
    await expect(p2).rejects.toBeInstanceOf(FieldCancelledError);
    await expect(p1).resolves.toEqual({ id: '1', name: 'same object' });
    expect(run).toHaveBeenCalledTimes(1);
    expect(run.mock.calls[0][0].keys).toEqual(['1']);
  });

  it('serves subsequent same-request loads from cache without another call', async () => {
    const run = vi.fn(async ({ keys }) =>
      keys.map((key) => value(key, { id: key, name: 'cached' })),
    );
    const scope = createBatchScope();
    const users = scope.batcher(defineBatcher({ id: 'user', run }));

    await users.load('1');
    await Promise.resolve();
    await users.load('1');

    expect(run).toHaveBeenCalledTimes(1);
  });

  it('allows nested resolvers to enqueue a second dispatch', async () => {
    const seen: string[][] = [];
    const run = vi.fn(async ({ keys }) => {
      seen.push([...keys]);
      return keys.map((key) => value(key, { id: key, name: key }));
    });
    const scope = createBatchScope();
    const users = scope.batcher(defineBatcher({ id: 'nested', run }));

    const first = users.load('parent');
    const nested = first.then(() => users.load('child'));

    await expect(nested).resolves.toEqual({ id: 'child', name: 'child' });
    expect(seen).toEqual([['parent'], ['child']]);
  });
});

describe('key normalization', () => {
  it('canonicalizes compound keys without primitive or object collisions', () => {
    expect(stableBatchKey(null)).not.toBe(stableBatchKey('null'));
    expect(stableBatchKey(undefined)).not.toBe(stableBatchKey('undefined'));
    expect(stableBatchKey(false)).not.toBe(stableBatchKey('false'));
    expect(stableBatchKey(1)).not.toBe(stableBatchKey('1'));
    expect(stableBatchKey(-0)).not.toBe(stableBatchKey(0));
    expect(stableBatchKey({ a: 1, b: 2 })).toBe(stableBatchKey({ b: 2, a: 1 }));
    expect(stableBatchKey(['a', 1])).not.toBe(stableBatchKey({ a: 1 }));
  });

  it('deduplicates structurally equivalent composite keys', async () => {
    const run = vi.fn(async ({ keys }) =>
      keys.map((key) => value(key, `${key.type}:${key.id}`)),
    );
    const scope = createBatchScope();
    const loader = scope.batcher(
      defineBatcher<{ type: string; id: string }, string>({ id: 'composite', run }),
    );

    const results = await Promise.all([
      loader.load({ type: 'account', id: '7' }),
      loader.load({ id: '7', type: 'account' }),
    ]);

    expect(run.mock.calls[0][0].keys).toEqual([{ type: 'account', id: '7' }]);
    expect(results).toEqual(['account:7', 'account:7']);
  });

  it('supports custom normalizers and toBatchKey', async () => {
    class AccountId {
      constructor(readonly value: string) {}
      toBatchKey() {
        return `account:${this.value}`;
      }
    }

    const run = vi.fn(async ({ keys }) =>
      keys.map((key) => value(key, key.value)),
    );
    const scope = createBatchScope();
    const accounts = scope.batcher(
      defineBatcher<AccountId, string>({ id: 'accounts', run }),
    );

    await expect(
      Promise.all([
        accounts.load(new AccountId('1')),
        accounts.load(new AccountId('1')),
      ]),
    ).resolves.toEqual(['1', '1']);
    expect(run.mock.calls[0][0].keys).toHaveLength(1);
  });
});

describe('backfill', () => {
  it('accepts out-of-order results', async () => {
    const run = vi.fn(async ({ keys }) =>
      [...keys].reverse().map((key) => value(key, `v${key}`)),
    );
    const scope = createBatchScope();
    const loader = scope.batcher(defineBatcher({ id: 'ordered', run }));

    await expect(
      Promise.all([loader.load('a'), loader.load('b'), loader.load('c')]),
    ).resolves.toEqual(['va', 'vb', 'vc']);
  });

  it('distinguishes cached null from confirmed missing', async () => {
    const run = vi.fn(async ({ keys }) =>
      keys.map((key) =>
        key === 'null-key' ? value(key, null) : missing<string, null>(key),
      ),
    );
    const scope = createBatchScope();
    const loader = scope.batcher(defineBatcher<string, null>({ id: 'absence', run }));

    const results = await Promise.allSettled([
      loader.load('null-key'),
      loader.load('missing-key'),
      loader.loadOptional('missing-key'),
      loader.loadState('null-key'),
      loader.loadState('missing-key'),
    ]);

    expect(results[0]).toEqual({ status: 'fulfilled', value: null });
    expect(results[1]).toMatchObject({
      status: 'rejected',
      reason: expect.any(MissingResultError),
    });
    expect(results[2]).toEqual({ status: 'fulfilled', value: null });
    expect(results[3]).toEqual({
      status: 'fulfilled',
      value: { status: 'value', value: null },
    });
    expect(results[4]).toEqual({
      status: 'fulfilled',
      value: { status: 'missing' },
    });

    await Promise.all([
      loader.load('null-key'),
      loader.loadOptional('missing-key'),
      loader.loadState('missing-key'),
    ]);
    expect(run).toHaveBeenCalledTimes(1);
  });

  it('delivers per-key errors while allowing other keys to resolve', async () => {
    const failure = new Error('forbidden key');
    const run = vi.fn(async ({ keys }) => [
      value(keys[0], 'ok'),
      error(keys[1], failure),
    ]);
    const scope = createBatchScope();
    const loader = scope.batcher(defineBatcher<string, string>({ id: 'partial', run }));

    await expect(Promise.all([loader.load('ok'), loader.load('bad')])).rejects.toBe(
      failure,
    );
    await expect(loader.load('ok')).resolves.toBe('ok');
  });

  it('retries per-key errors by default but caches them when requested', async () => {
    const runWithoutCache = vi
      .fn<() => Promise<Array<BatchResultEntry<string, string>>>>()
      .mockResolvedValueOnce([error('x', new Error('temporary'))])
      .mockResolvedValueOnce([value('x', 'recovered')]);

    const retrying = createBatchScope().batcher(
      defineBatcher({ id: 'retry-error', run: runWithoutCache }),
    );
    await expect(retrying.load('x')).rejects.toThrow('temporary');
    await expect(retrying.load('x')).resolves.toBe('recovered');

    const cachedFailure = new Error('permanent');
    const runCached = vi.fn(async () => [error('x', cachedFailure, true)]);
    const caching = createBatchScope().batcher(
      defineBatcher({ id: 'cached-error', run: runCached }),
    );
    await expect(caching.load('x')).rejects.toBe(cachedFailure);
    await expect(caching.load('x')).rejects.toBe(cachedFailure);
    expect(runCached).toHaveBeenCalledTimes(1);
  });

  it('fails all keys when the batch function throws and permits retry', async () => {
    const boom = new Error('database unavailable');
    const run = vi
      .fn<() => Promise<Array<BatchResultEntry<string, string>>>>()
      .mockRejectedValueOnce(boom)
      .mockImplementationOnce(async ({ keys }) =>
        keys.map((key) => value(key, 'recovered')),
      );
    const scope = createBatchScope();
    const loader = scope.batcher(defineBatcher({ id: 'whole-failure', run }));

    await expect(
      Promise.all([loader.load('a'), loader.load('b')]),
    ).rejects.toMatchObject({
      cause: boom,
      key: 'a',
    });
    await expect(Promise.all([loader.load('a'), loader.load('b')])).resolves.toEqual([
      'recovered',
      'recovered',
    ]);
    expect(run).toHaveBeenCalledTimes(2);
  });

  it('rejects omitted, duplicate, and unknown result keys', async () => {
    const scope = createBatchScope();
    const omitted = scope.batcher(
      defineBatcher<string, string>({
        id: 'omitted',
        run: async ({ keys }) => [value(keys[0], 'a')],
      }),
    );
    await expect(Promise.all([omitted.load('a'), omitted.load('b')])).rejects.toBeInstanceOf(
      InvalidBatchResultError,
    );

    const duplicate = scope.batcher(
      defineBatcher<string, string>({
        id: 'duplicate',
        run: async () => [value('a', 1 as never), value('a', 2 as never)],
      }),
    );
    await expect(duplicate.load('a')).rejects.toBeInstanceOf(InvalidBatchResultError);

    const unknown = scope.batcher(
      defineBatcher<string, string>({
        id: 'unknown',
        run: async () => [value('z' as never, 'z')],
      }),
    );
    await expect(unknown.load('a')).rejects.toBeInstanceOf(InvalidBatchResultError);
  });
});

describe('cancellation and disposal', () => {
  it('removes a key before dispatch only when all waiters cancel', async () => {
    const run = vi.fn(async ({ keys }) =>
      keys.map((key) => value(key, `v${key}`)),
    );
    const scope = createBatchScope();
    const loader = scope.batcher(defineBatcher({ id: 'cancel-queue', run }));

    const removable = loader.load('remove');
    removable.cancel();
    const kept = loader.load('keep');

    await expect(removable).rejects.toBeInstanceOf(FieldCancelledError);
    await expect(kept).resolves.toBe('vkeep');
    expect(run.mock.calls[0][0].keys).toEqual(['keep']);
  });

  it('does not cancel an in-flight key with another waiter', async () => {
    const gate = deferred<Array<BatchResultEntry<string, string>>>();
    const run = vi.fn(() => gate.promise);
    const scope = createBatchScope();
    const loader = scope.batcher(defineBatcher({ id: 'cancel-running', run }));

    const a = loader.load('x');
    const b = loader.load('x');
    const cancelled = a.catch((error) => error);
    await Promise.resolve();
    a.cancel();
    gate.resolve([value('x', 'done')]);

    await expect(b).resolves.toBe('done');
    await expect(cancelled).resolves.toBeInstanceOf(FieldCancelledError);
  });

  it('rejects unfinished loads and aborts signal when request is disposed', async () => {
    const gate = deferred<Array<BatchResultEntry<string, string>>>();
    const run = vi.fn(({ signal }) => {
      return new Promise<Array<BatchResultEntry<string, string>>>((_, reject) => {
        signal.addEventListener('abort', () =>
          reject(signal.reason ?? new Error('aborted')),
        );
        gate.resolve([]);
      });
    });
    const scope = createBatchScope();
    const loader = scope.batcher(defineBatcher({ id: 'dispose', run }));
    const pending = loader.load('x').catch((error) => error);

    await Promise.resolve();
    scope.dispose(new Error('request ended'));
    gate.resolve([value('x', 'late')]);

    await expect(pending).resolves.toBeInstanceOf(ScopeDisposedError);
    expect(scope.signal.aborted).toBe(true);
    expect(scope.disposed).toBe(true);
    expect(() => scope.batcher(userBatcher)).toThrow(ScopeDisposedError);
    await expect(loader.load('late')).rejects.toBeInstanceOf(ScopeDisposedError);
  });

  it('disposes only its own permission partition and still ends with the request', async () => {
    const adminGate = deferred<Array<BatchResultEntry<string, string>>>();
    const userGate = deferred<Array<BatchResultEntry<string, string>>>();
    const adminDef = defineBatcher<string, string>({
      id: 'admin-dispose',
      run: ({ signal }) =>
        new Promise((_, reject) => {
          signal.addEventListener('abort', () => reject(signal.reason));
          return adminGate.promise;
        }),
    });
    const userDef = defineBatcher<string, string>({
      id: 'user-remains',
      run: () => userGate.promise,
    });
    const request = createBatchScope();
    const admin = request.withAccess({ permissions: 'admin' });
    const user = request.withAccess({ permissions: 'user' });

    const adminLoad = admin.batcher(adminDef).load('x').catch((error) => error);
    const userLoad = user.batcher(userDef).load('x');
    await Promise.resolve();

    admin.dispose(new Error('admin context ended'));
    userGate.resolve([value('x', 'user result')]);

    await expect(adminLoad).resolves.toBeInstanceOf(ScopeDisposedError);
    await expect(userLoad).resolves.toBe('user result');
    expect(admin.signal.aborted).toBe(true);

    request.dispose();
  });

  it('does not share cached data across requests', async () => {
    const run = vi.fn(async ({ keys }) =>
      keys.map((key) => value(key, `v${key}`)),
    );
    const def = defineBatcher({ id: 'request-isolation', run });

    const first = createBatchScope();
    const second = createBatchScope();
    await first.batcher(def).load('1');
    await second.batcher(def).load('1');

    expect(run).toHaveBeenCalledTimes(2);
  });
});

describe('access context isolation', () => {
  it('separates permission contexts and transaction snapshots', async () => {
    const run = vi.fn(async ({ keys, permissions, snapshot }) =>
      keys.map((key) => value(key, `${permissions}:${snapshot}:${key}`)),
    );
    const def = defineBatcher({ id: 'access', run });
    const request = createBatchScope();
    const admin = request.withAccess({ permissions: 'admin', snapshot: 's1' });
    const user = request.withAccess({ permissions: 'user', snapshot: 's1' });
    const adminSnapshot2 = request.withAccess({
      permissions: 'admin',
      snapshot: 's2',
    });

    await Promise.all([
      admin.batcher(def).load('1'),
      user.batcher(def).load('1'),
      adminSnapshot2.batcher(def).load('1'),
    ]);
    await admin.batcher(def).load('1');
    await user.batcher(def).load('1');

    expect(run).toHaveBeenCalledTimes(3);
  });

  it('shares cache only among stable identical access references', async () => {
    const run = vi.fn(async ({ keys }) =>
      keys.map((key) => value(key, 'same')),
    );
    const def = defineBatcher({ id: 'stable-access', run });
    const permissions = { role: 'reader' };
    const snapshot = { version: 1 };
    const request = createBatchScope();

    const a = request.withAccess({ permissions, snapshot });
    const b = request.withAccess({ permissions, snapshot });
    await a.batcher(def).load('1');
    await b.batcher(def).load('1');

    expect(run).toHaveBeenCalledTimes(1);
  });
});

describe('deadlock protection', () => {
  it('rejects a batch that refreshes its own in-flight key', async () => {
    const scope = createBatchScope();
    const recursive = scope.batcher(
      defineBatcher<string, string>({
        id: 'recursive',
        run: async ({ keys }) => {
          await expect(recursive.refresh(keys[0])).rejects.toBeInstanceOf(
            BatchCycleError,
          );
          return keys.map((key) => value(key, 'done'));
        },
      }),
    );

    await expect(recursive.load('x')).resolves.toBe('done');
  });

  it('permits a different batcher to be read from within a batch function', async () => {
    const scope = createBatchScope();
    const other = scope.batcher(
      defineBatcher<string, string>({
        id: 'other',
        run: vi.fn(async ({ keys }) =>
          keys.map((key) => value(key, `other:${key}`)),
        ),
      }),
    );
    const first = scope.batcher(
      defineBatcher<string, string>({
        id: 'first',
        run: vi.fn(async ({ keys }) =>
          Promise.all(
            keys.map(async (key) => value(key, await other.load('dep'))),
          ),
        ),
      }),
    );

    await expect(first.load('x')).resolves.toBe('other:dep');
  });
});
