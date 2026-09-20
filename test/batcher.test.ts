import { describe, expect, it } from 'vitest';
import {
  createRequestBatcher,
  err,
  KeyNormalizationError,
  normalizeKey,
  ok,
  RecursiveLoadError,
  RequestClosedError,
  type BatchResult,
} from '../src/index.js';

const tick = () => new Promise<void>((resolve) => setTimeout(resolve, 0));

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

describe('normalizeKey', () => {
  it('区分类型与特殊数值', () => {
    expect(normalizeKey(1)).not.toBe(normalizeKey('1'));
    expect(normalizeKey(null)).not.toBe(normalizeKey('null'));
    expect(normalizeKey(null)).not.toBe(normalizeKey(undefined));
    expect(normalizeKey(NaN)).toBe(normalizeKey(NaN));
    expect(normalizeKey(0)).not.toBe(normalizeKey(-0));
    expect(normalizeKey(10n)).toBe(normalizeKey(10n));
  });

  it('复合键与属性次序无关，数组保序', () => {
    expect(normalizeKey({ a: 1, b: [2, { c: null }] })).toBe(
      normalizeKey({ b: [2, { c: null }], a: 1 }),
    );
    expect(normalizeKey([1, 2])).not.toBe(normalizeKey([2, 1]));
    expect(normalizeKey({ a: 1 })).not.toBe(normalizeKey({ a: 1, b: undefined }));
  });

  it('拒绝循环结构与非 plain object', () => {
    const cyclic: Record<string, unknown> = {};
    cyclic.self = cyclic;
    expect(() => normalizeKey(cyclic)).toThrow(KeyNormalizationError);
    class Entity {}
    expect(() => normalizeKey(new Entity())).toThrow(KeyNormalizationError);
    expect(() => normalizeKey(() => 1)).toThrow(KeyNormalizationError);
  });
});

describe('批次调度', () => {
  it('同一轮事件循环内的加载合并为一次批量调用', async () => {
    const batcher = createRequestBatcher();
    const calls: number[][] = [];
    const loader = batcher.loader<number, number>('n', {
      batchFn: async (keys) => {
        calls.push([...keys]);
        return keys.map((k) => [k, k * 10] as const);
      },
    });
    const [a, b, c] = await Promise.all([
      loader.load(1),
      loader.load(2),
      loader.load(3),
    ]);
    expect([a, b, c]).toEqual([10, 20, 30]);
    expect(calls).toEqual([[1, 2, 3]]);
    await batcher.close();
  });

  it('重复键只发送一次，每个调用方收到独立 promise', async () => {
    const batcher = createRequestBatcher();
    const calls: number[][] = [];
    const loader = batcher.loader<number, number>('n', {
      batchFn: async (keys) => {
        calls.push([...keys]);
        return keys.map((k) => [k, k * 10] as const);
      },
    });
    const p1 = loader.load(1);
    const p2 = loader.load(1);
    const p3 = loader.load(1);
    expect(p1).not.toBe(p2);
    expect(p2).not.toBe(p3);
    await expect(p1).resolves.toBe(10);
    await expect(p2).resolves.toBe(10);
    await expect(p3).resolves.toBe(10);
    expect(calls).toEqual([[1]]);
    await batcher.close();
  });

  it('嵌套解析器的二次入队进入下一轮批次', async () => {
    const batcher = createRequestBatcher();
    const calls: number[][] = [];
    const loader = batcher.loader<number, number>('n', {
      batchFn: async (keys) => {
        calls.push([...keys]);
        return keys.map((k) => [k, k * 10] as const);
      },
    });
    // 模拟嵌套解析器：等第一批完成后才发起第二批加载
    const first = await loader.load(1);
    expect(first).toBe(10);
    const [b, c] = await Promise.all([loader.load(2), loader.load(3)]);
    expect([b, c]).toEqual([20, 30]);
    expect(calls).toEqual([[1], [2, 3]]);
    await batcher.close();
  });

  it('maxBatchSize 将超大队列切片为多个批次', async () => {
    const batcher = createRequestBatcher();
    const calls: number[][] = [];
    const loader = batcher.loader<number, number>('n', {
      maxBatchSize: 2,
      batchFn: async (keys) => {
        calls.push([...keys]);
        return keys.map((k) => [k, k] as const);
      },
    });
    await Promise.all([loader.load(1), loader.load(2), loader.load(3)]);
    expect(calls).toEqual([[1, 2], [3]]);
    await batcher.close();
  });

  it('在途键只发送一次：派发后到来的相同键并入等待者', async () => {
    const batcher = createRequestBatcher();
    const gate = deferred<void>();
    const calls: number[][] = [];
    const loader = batcher.loader<number, number>('n', {
      batchFn: async (keys) => {
        calls.push([...keys]);
        await gate.promise;
        return keys.map((k) => [k, k * 10] as const);
      },
    });
    const p1 = loader.load(1);
    await tick(); // 第一批已派发，键 1 在途
    const p2 = loader.load(1);
    gate.resolve();
    await expect(p1).resolves.toBe(10);
    await expect(p2).resolves.toBe(10);
    expect(calls).toEqual([[1]]);
    await batcher.close();
  });
});

describe('键规范化与缓存', () => {
  it('复合键去重：属性次序不同的键合并为一次读取', async () => {
    const batcher = createRequestBatcher();
    const calls: unknown[][] = [];
    const loader = batcher.loader<Record<string, unknown>, string>('e', {
      batchFn: async (keys) => {
        calls.push([...keys]);
        // 返回全新的键对象实例，且乱序：按规范串匹配
        return [...keys]
          .reverse()
          .map((k) => [{ ...k }, `v:${JSON.stringify(k)}`] as const);
      },
    });
    const [a, b] = await Promise.all([
      loader.load({ tenant: 't1', id: 1 }),
      loader.load({ id: 1, tenant: 't1' }),
    ]);
    expect(a).toBe(b);
    expect(calls).toHaveLength(1);
    expect(calls[0]).toHaveLength(1);
    await batcher.close();
  });

  it('支持 cacheKeyFn 自定义规范化', async () => {
    const batcher = createRequestBatcher();
    const calls: number = 0;
    let count = calls;
    const loader = batcher.loader<{ id: number; label: string }, number>('e', {
      cacheKeyFn: (key) => `id:${key.id}`,
      batchFn: async (keys) => {
        count += 1;
        return keys.map((k) => [k, k.id] as const);
      },
    });
    await loader.load({ id: 1, label: 'a' });
    const hit = await loader.load({ id: 1, label: 'b' });
    expect(hit).toBe(1);
    expect(count).toBe(1);
    await batcher.close();
  });

  it('空值与不存在严格区分：null 被缓存，缺失不缓存', async () => {
    const batcher = createRequestBatcher();
    const calls: number[][] = [];
    const loader = batcher.loader<number, number | null>('e', {
      batchFn: async (keys) => {
        calls.push([...keys]);
        // 键 1 显式返回 null；键 2 在结果中缺失
        return keys.filter((k) => k === 1).map((k) => [k, null] as const);
      },
    });
    const [first, second] = await Promise.all([loader.load(1), loader.load(2)]);
    expect(first).toBeNull();
    expect(second).toBeNull();
    await expect(loader.load(1)).resolves.toBeNull(); // 缓存命中，不再发送
    await expect(loader.load(2)).resolves.toBeNull(); // 未缓存，重新读取
    expect(calls).toEqual([[1, 2], [2]]);
    await batcher.close();
  });
});

describe('结果回填协议', () => {
  it('乱序返回按键正确回填', async () => {
    const batcher = createRequestBatcher();
    const loader = batcher.loader<number, string>('n', {
      batchFn: async (keys) =>
        [...keys].reverse().map((k) => [k, `v${k}`] as const),
    });
    const [a, b, c] = await Promise.all([
      loader.load(1),
      loader.load(2),
      loader.load(3),
    ]);
    expect([a, b, c]).toEqual(['v1', 'v2', 'v3']);
    await batcher.close();
  });

  it('逐键错误只拒绝对应键，其余键正常兑现', async () => {
    const batcher = createRequestBatcher();
    const loader = batcher.loader<number, number>('n', {
      batchFn: async (keys): Promise<Array<readonly [number, BatchResult<number>]>> =>
        keys.map((k) =>
          k === 2
            ? ([k, err(new Error('boom'))] as const)
            : ([k, k * 10] as const),
        ),
    });
    const results = await loader.loadMany([1, 2, 3]);
    expect(results[0]).toEqual({ status: 'fulfilled', value: 10 });
    expect(results[1].status).toBe('rejected');
    expect((results[1] as PromiseRejectedResult).reason).toBeInstanceOf(Error);
    expect(results[2]).toEqual({ status: 'fulfilled', value: 30 });
    await batcher.close();
  });

  it('逐键错误默认写缓存，重试不再发送', async () => {
    const batcher = createRequestBatcher();
    let count = 0;
    const loader = batcher.loader<number, number>('n', {
      batchFn: async (keys): Promise<Array<readonly [number, BatchResult<number>]>> => {
        count += 1;
        return keys.map((k) => [k, new Error(`e${k}`)] as const);
      },
    });
    await expect(loader.load(1)).rejects.toThrow('e1');
    await expect(loader.load(1)).rejects.toThrow('e1');
    expect(count).toBe(1);
    await batcher.close();
  });

  it('批量函数整体失败：全部等待者以同一错误拒绝且不写缓存', async () => {
    const batcher = createRequestBatcher();
    let count = 0;
    const loader = batcher.loader<number, number>('n', {
      batchFn: async (keys) => {
        count += 1;
        if (count === 1) throw new Error('total failure');
        return keys.map((k) => [k, k * 10] as const);
      },
    });
    const p1 = loader.load(1);
    const p2 = loader.load(2);
    await expect(p1).rejects.toThrow('total failure');
    await expect(p2).rejects.toThrow('total failure');
    // 整体失败不缓存：下一轮重新读取并成功
    await expect(loader.load(1)).resolves.toBe(10);
    expect(count).toBe(2);
    await batcher.close();
  });

  it('未知键忽略，重复返回的键以先出现者为准', async () => {
    const batcher = createRequestBatcher();
    const loader = batcher.loader<number, string>('n', {
      batchFn: async () =>
        [
          [999, 'unknown'],
          [1, 'first'],
          [1, 'second'],
        ] as const,
    });
    await expect(loader.load(1)).resolves.toBe('first');
    await batcher.close();
  });

  it('ok() 包装允许值本身是 Error', async () => {
    const batcher = createRequestBatcher();
    const sentinel = new Error('as value');
    const loader = batcher.loader<number, Error>('n', {
      batchFn: async (keys): Promise<Array<readonly [number, BatchResult<Error>]>> =>
        keys.map((k) => [k, ok(sentinel)] as const),
    });
    await expect(loader.load(1)).resolves.toBe(sentinel);
    await batcher.close();
  });
});

describe('取消语义', () => {
  it('唯一等待者在派发前取消：键不再发送', async () => {
    const batcher = createRequestBatcher();
    const calls: number[][] = [];
    const loader = batcher.loader<number, number>('n', {
      batchFn: async (keys) => {
        calls.push([...keys]);
        return keys.map((k) => [k, k] as const);
      },
    });
    const ac = new AbortController();
    const p = loader.load(1, { signal: ac.signal });
    ac.abort(new Error('cancelled'));
    await expect(p).rejects.toThrow('cancelled');
    await tick();
    expect(calls).toEqual([]);
    await batcher.close();
  });

  it('取消单个字段不取消仍有等待者的键', async () => {
    const batcher = createRequestBatcher();
    const calls: number[][] = [];
    const loader = batcher.loader<number, number>('n', {
      batchFn: async (keys) => {
        calls.push([...keys]);
        return keys.map((k) => [k, k * 10] as const);
      },
    });
    const ac = new AbortController();
    const p1 = loader.load(1, { signal: ac.signal });
    const p2 = loader.load(1);
    ac.abort();
    await expect(p1).rejects.toBeInstanceOf(DOMException);
    await expect(p2).resolves.toBe(10);
    expect(calls).toEqual([[1]]);
    await batcher.close();
  });

  it('派发后取消单个等待者：其余等待者照常兑现', async () => {
    const batcher = createRequestBatcher();
    const gate = deferred<void>();
    const loader = batcher.loader<number, number>('n', {
      batchFn: async (keys) => {
        await gate.promise;
        return keys.map((k) => [k, k * 10] as const);
      },
    });
    const ac = new AbortController();
    const p1 = loader.load(1, { signal: ac.signal });
    const p2 = loader.load(1);
    await tick(); // 已派发
    ac.abort();
    await expect(p1).rejects.toBeInstanceOf(DOMException);
    gate.resolve();
    await expect(p2).resolves.toBe(10);
    await batcher.close();
  });
});

describe('请求结束', () => {
  it('拒绝已排队与在途的未完成项并释放缓存', async () => {
    const batcher = createRequestBatcher();
    const gate = deferred<void>();
    const calls: number[][] = [];
    const loader = batcher.loader<number, number>('n', {
      batchFn: async (keys) => {
        calls.push([...keys]);
        await gate.promise;
        return keys.map((k) => [k, k * 10] as const);
      },
    });
    const p1 = loader.load(1);
    const p2 = loader.load(2);
    await tick(); // 已派发，在途
    const closing = batcher.close();
    await expect(p1).rejects.toBeInstanceOf(RequestClosedError);
    await expect(p2).rejects.toBeInstanceOf(RequestClosedError);
    // close 等待在途批量函数 settle，迟到结果被丢弃
    let settled = false;
    void closing.then(() => {
      settled = true;
    });
    await tick();
    expect(settled).toBe(false);
    gate.resolve();
    await closing;
    expect(settled).toBe(true);
    // 已关闭：新的加载立即拒绝
    await expect(loader.load(3)).rejects.toBeInstanceOf(RequestClosedError);
    expect(batcher.closed).toBe(true);
  });

  it('关闭时仍在队列中的键被拒绝且不再派发', async () => {
    const batcher = createRequestBatcher();
    const calls: number[][] = [];
    const loader = batcher.loader<number, number>('n', {
      batchFn: async (keys) => {
        calls.push([...keys]);
        return keys.map((k) => [k, k] as const);
      },
    });
    const p = loader.load(1); // 已排队，尚未 flush
    const closing = batcher.close();
    await expect(p).rejects.toBeInstanceOf(RequestClosedError);
    await closing;
    await tick();
    expect(calls).toEqual([]);
  });

  it('close 幂等', async () => {
    const batcher = createRequestBatcher();
    await Promise.all([batcher.close(), batcher.close()]);
    expect(batcher.closed).toBe(true);
  });
});

describe('权限上下文与事务快照隔离', () => {
  it('不同权限主体与事务快照绝不共享批次与缓存', async () => {
    const batcher = createRequestBatcher();
    const seen: string[] = [];
    const make =
      (tag: string) =>
      async (keys: readonly number[]) => {
        seen.push(`${tag}:${keys.join(',')}`);
        return keys.map((k) => [k, `${tag}${k}`] as const);
      };
    const alice = batcher
      .forContext({ principal: 'alice', txSnapshot: 'tx1' })
      .loader<number, string>('e', { batchFn: make('A') });
    const aliceTx2 = batcher
      .forContext({ principal: 'alice', txSnapshot: 'tx2' })
      .loader<number, string>('e', { batchFn: make('T2') });
    const bob = batcher
      .forContext({ principal: 'bob', txSnapshot: 'tx1' })
      .loader<number, string>('e', { batchFn: make('B') });
    const [va, vt, vb] = await Promise.all([
      alice.load(1),
      aliceTx2.load(1),
      bob.load(1),
    ]);
    expect([va, vt, vb]).toEqual(['A1', 'T21', 'B1']);
    expect(seen).toHaveLength(3);
    expect(seen).toContain('A:1');
    expect(seen).toContain('T2:1');
    expect(seen).toContain('B:1');
    // 相同权限主体 + 相同事务快照：共享缓存，不再读取
    const aliceAgain = batcher
      .forContext({ txSnapshot: 'tx1', principal: 'alice' })
      .loader<number, string>('e', { batchFn: make('A2') });
    await expect(aliceAgain.load(1)).resolves.toBe('A1');
    expect(seen).toHaveLength(3);
    await batcher.close();
  });

  it('支持显式 scopeId', async () => {
    const batcher = createRequestBatcher();
    let count = 0;
    const make = async (keys: readonly number[]) => {
      count += 1;
      return keys.map((k) => [k, k] as const);
    };
    const a1 = batcher.forContext(undefined, 'alice:tx1').loader('e', { batchFn: make });
    const a2 = batcher.forContext(undefined, 'alice:tx1').loader('e', { batchFn: make });
    const b = batcher.forContext(undefined, 'bob:tx1').loader('e', { batchFn: make });
    await a1.load(1);
    await a2.load(1); // 同 scopeId：缓存命中
    await b.load(1); // 不同 scopeId：独立读取
    expect(count).toBe(2);
    await batcher.close();
  });
});

describe('递归防护', () => {
  it('批量函数执行中加载本批在途键：立即拒绝而非死锁', async () => {
    const batcher = createRequestBatcher();
    let caught: unknown;
    const loader = batcher.loader<number, number>('n', {
      batchFn: async (keys) => {
        caught = await loader.load(keys[0]).then(
          (value) => ({ value }),
          (error: unknown) => error,
        );
        return keys.map((k) => [k, k * 2] as const);
      },
    });
    await expect(loader.load(5)).resolves.toBe(10);
    expect(caught).toBeInstanceOf(RecursiveLoadError);
    await batcher.close();
  });

  it('批量函数内加载其他键进入下一轮，可正常完成', async () => {
    const batcher = createRequestBatcher();
    const calls: number[][] = [];
    const loader = batcher.loader<number, number>('n', {
      batchFn: async (keys) => {
        calls.push([...keys]);
        const out: Array<readonly [number, number]> = [];
        for (const k of keys) {
          if (k === 1) {
            const two = await loader.load(2); // 新键：下一轮批次
            out.push([k, two]);
          } else {
            out.push([k, k * 10]);
          }
        }
        return out;
      },
    });
    await expect(loader.load(1)).resolves.toBe(20);
    expect(calls).toEqual([[1], [2]]);
    await batcher.close();
  });

  it('批量函数同步再入队不递归刷新同一批次', async () => {
    const batcher = createRequestBatcher();
    const calls: number[][] = [];
    const loader = batcher.loader<number, number>('n', {
      batchFn: (keys) => {
        calls.push([...keys]);
        if (keys.includes(1)) {
          // 同步再入队一个新键：必须进入下一轮，而不是递归刷新本轮
          void loader.load(2).then(() => undefined, () => undefined);
        }
        return keys.map((k) => [k, k] as const);
      },
    });
    await expect(loader.load(1)).resolves.toBe(1);
    await tick();
    expect(calls).toEqual([[1], [2]]);
    await batcher.close();
  });
});
