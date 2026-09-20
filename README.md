# GraphQL runtime core

TypeScript library for GraphQL value completion and request-scoped field batching.

## Request batcher

Create one batch scope per GraphQL request. A scope owns its caches and
`AbortController`. `withAccess()` returns a view of the same request with an
isolated cache partition for a different permission context or transaction
snapshot.

```ts
import { createBatchScope, defineBatcher } from './dist/index.js';

const accounts = defineBatcher({
  id: 'accounts',
  async run({ keys, permissions, snapshot, signal }) {
    const rows = await db.accounts.whereIn('id', keys).forSnapshot(snapshot);
    return keys.map((id) => {
      const row = rows.find((candidate) => candidate.id === id);
      return row
        ? { status: 'value', key: id, value: row }
        : { status: 'missing', key: id };
    });
  },
});

export function resolveField(root, args, context) {
  const scope = context.batchScope;
  return scope.batcher(accounts).load(args.id);
}
```

- Loads in the same synchronous execution tick are coalesced with a microtask.
- Duplicate keys are sent once; each caller still owns an independent promise.
- Results can be unordered. Every requested key must appear exactly once.
- `{ status: 'value', value: null }` is cached as a null value;
  `{ status: 'missing' }` is a separate cached nonexistence state.
- Per-key errors use `{ status: 'error', error }` and are retryable unless
  `cache: true` is set. Whole-batch failures and malformed result sets are never
  cached.
- Dispose the scope at request end to reject unfinished loads, abort I/O, and
  release caches.

Run `npm install`, then `npm test` and `npm run build`.
