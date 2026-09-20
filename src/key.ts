/**
 * 键规范化协议（key normalization）。
 *
 * 规范化把任意支持的键值映射为唯一的规范字符串，语义相等的键必须得到
 * 相同的规范串，语义不同的键绝不能碰撞：
 *
 * - 原始类型按类型打标签：`number:1` 与 `string:"1"` 不同；
 *   `NaN`、`-0`、`Infinity` 各有确定形式；`null` / `undefined` / 字符串
 *   `"null"` 两两不同。
 * - 数组保序递归规范化；对象按键名排序后递归规范化，因此
 *   `{a:1,b:2}` 与 `{b:2,a:1}` 是同一个键（复合键与属性次序无关）。
 * - 仅支持 plain object；Date 按时间戳规范化；class 实例、函数、
 *   symbol、循环结构一律抛 {@link KeyNormalizationError}，
 *   调用方应改用 `cacheKeyFn` 自行投影。
 *
 * 同一套规范化同时用于：批次内去重、请求缓存、批量函数返回结果的按键
 * 匹配，以及权限/事务作用域的判别，保证四处语义一致。
 */
export class KeyNormalizationError extends TypeError {
  constructor(message: string) {
    super(message);
    this.name = 'KeyNormalizationError';
  }
}

export function normalizeKey(key: unknown): string {
  return normalize(key, new Set());
}

function normalize(key: unknown, seen: Set<object>): string {
  if (key === null) return 'null';
  switch (typeof key) {
    case 'undefined':
      return 'undefined';
    case 'boolean':
      return key ? 'true' : 'false';
    case 'number':
      if (Number.isNaN(key)) return 'number:NaN';
      if (Object.is(key, -0)) return 'number:-0';
      return `number:${key}`;
    case 'bigint':
      return `bigint:${key}`;
    case 'string':
      return `string:${JSON.stringify(key)}`;
    case 'object':
      break;
    default:
      throw new KeyNormalizationError(
        `cannot normalize key of type ${typeof key}; provide a cacheKeyFn`,
      );
  }
  if (key instanceof Date) return `date:${key.getTime()}`;
  if (seen.has(key)) {
    throw new KeyNormalizationError('circular key structure');
  }
  if (Array.isArray(key)) {
    seen.add(key);
    try {
      return `array:[${key.map((item) => normalize(item, seen)).join(',')}]`;
    } finally {
      seen.delete(key);
    }
  }
  const proto: unknown = Object.getPrototypeOf(key);
  if (proto !== Object.prototype && proto !== null) {
    throw new KeyNormalizationError(
      'cannot normalize non-plain object key; provide a cacheKeyFn',
    );
  }
  seen.add(key);
  try {
    const entries = Object.keys(key as Record<string, unknown>)
      .sort()
      .map(
        (name) =>
          `${JSON.stringify(name)}:${normalize((key as Record<string, unknown>)[name], seen)}`,
      );
    return `object:{${entries.join(',')}}`;
  } finally {
    seen.delete(key);
  }
}
