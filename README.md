# GraphQL runtime core

TypeScript library for value completion and request-scoped field batching.

Run `npm install`, then `npm test` and `npm run build`.

## 请求作用域字段批处理器

`createRequestBatcher()` 为每个 GraphQL 请求创建独立的批处理器：同一轮
事件循环内的实体读取被合并为一次批量调用，同一请求内相同键的结果被缓存。
**不同请求、不同权限上下文、不同事务快照绝不共享批次与缓存。**

```ts
import { createRequestBatcher, err } from 'graphql-request-batcher-core';

// 每个请求创建一个；请求结束必须 close()
const batcher = createRequestBatcher();

// 权限主体 + 事务快照界定作用域；也可用 forContext(undefined, scopeId)
const scoped = batcher.forContext({ principal: user.id, txSnapshot: tx.id });

const userLoader = scoped.loader('user', {
  batchFn: async (ids) => {
    const rows = await db.usersByIds(ids);       // 允许乱序、部分缺失
    return rows.map((r) => [r.id, r] as const);  // [键, 结果] 对；Map 亦可
  },
});

// 解析器中：同一轮的多次 load 合并为一次批量调用
const [a, b] = await Promise.all([userLoader.load(1), userLoader.load(2)]);

await batcher.close(); // 拒绝未完成项并释放缓存
```

### 批次调度协议

- `load()` 把键加入当前队列，并以微任务调度一次刷新（flush）；同一轮
  事件循环（含同一微任务检查点之前）入队的键合并为一个批次。
- 刷新只处理本轮开始时已排队的键；派发期间（含批量函数同步执行中）
  新入队的键进入**下一轮**，绝不递归刷新本轮——嵌套解析器的二次入队
  因此安全，且不会死循环。
- 重复键（规范化后相同）只发送一次；每个调用方各自持有独立 promise。
- `maxBatchSize` 可把超大队列切片为多个批次。
- 键已派发但未回填（在途）时，相同键的新 `load()` 并入既有等待者，
  不重复发送。

### 键规范化协议

- 默认 `normalizeKey`：原始类型按类型打标签（`1` ≠ `"1"`，`null` ≠
  `"null"` ≠ `undefined`，`NaN`/`-0` 有确定形式）；数组保序；对象按键名
  排序，故复合键与属性次序无关（`{a:1,b:2}` ≡ `{b:2,a:1}`）。
- 仅支持 plain object / 数组 / 原始类型 / `Date`；class 实例、函数、
  symbol、循环结构抛 `KeyNormalizationError`，此时应提供 `cacheKeyFn`。
- 同一套规范化同时用于批次去重、请求缓存、结果回填匹配与作用域判别。

### 结果按键回填协议

批量函数返回 `[键, 结果]` 对的任意可迭代对象（`Map` 亦可）：

- **乱序**：返回顺序任意，按键的规范化形式匹配，不要求键对象同一。
- **部分缺失**：结果中缺失的键以 `missingValue`（默认 `null`）解析，
  且**不写缓存**；显式返回 `null` 的键**会**写缓存——空值与不存在
  严格区分。
- **逐键错误**：结果值为 `Error` 实例或 `err(e)` 包装时，仅该键拒绝
  （默认写缓存，可用 `cacheErrors: false` 关闭）；值本身可能是 `Error`
  时用 `ok(value)` 显式包装。
- **整体失败**：批量函数抛错/拒绝时，本批全部等待者以同一错误拒绝，
  不写缓存，下一轮自动重试。
- 未请求的键忽略；同一键重复返回时以先出现者为准。

### 取消与请求结束

- `load(key, { signal })` 取消单个字段：等待者从键上摘除；若该键已无
  等待者且尚未派发，则不再发送；仍有其他等待者的键不受影响。
- `batcher.close()`：已排队与在途的等待者一律以 `RequestClosedError`
  拒绝，缓存清空释放；在途批量函数的迟到结果被丢弃。返回的 promise 在
  在途批量函数全部 settle 后兑现。`close` 幂等，关闭后 `load` 立即拒绝。

### 递归防护

批量函数执行期间（含其异步延续，经 `AsyncLocalStorage` 追踪）加载本批
**在途**的同一个键会立即以 `RecursiveLoadError` 拒绝，而不是死锁；
加载**其他**键则进入下一轮批次，可正常完成（支持跨批次的依赖加载，
包括跨 loader 的循环检测）。
