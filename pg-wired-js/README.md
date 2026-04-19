# pg-wired

High-performance PostgreSQL driver for JavaScript runtimes, binding the [`pg-wired`](../pg-wired) Rust wire-protocol implementation via [napi-rs](https://napi.rs).

## Runtime support

The published package ships one prebuilt native binding per platform triple. The same artifact loads in every modern JavaScript runtime with N-API:

| runtime | supported | notes |
|---|---|---|
| Node.js >= 22.6 | yes | no flags needed |
| Bun >= 1.1      | yes | no flags needed |
| Deno >= 2.1     | yes | run with `--allow-all --unstable-node-globals --unstable-detect-cjs` (or equivalent `deno.json` config) |

There are no runtime-specific packages (no `pg-wired-node`, `pg-wired-bun`, `pg-wired-deno`). One install covers all three.

## Install

```
npm install pg-wired
# or
bun add pg-wired
# or (deno) add to deno.json imports: "pg-wired": "npm:pg-wired"
```

## Quick start

```js
import { connect } from 'pg-wired';

const conn = await connect('postgres://user:pass@localhost:5432/app');

// Native JS values in, native JS values out (binary wire protocol).
const res = await conn.query(
  'SELECT $1::int + $2::int AS sum',
  [2, 3],
);
console.log(res.rows[0][0]); // 5 (Number, not "5")

await conn.close();
```

## Features

- **Full binary wire protocol** for `conn.query`, `pool.query`, and `Pipeline.push`. Params and results travel as typed bytes; the JS layer decodes cells to native values per column OID: `Number` for int2/int4/float4/float8, `BigInt` for int8, `Date` for `timestamptz`/`date`, parsed JSON for `jsonb`, UUID string for `uuid`, `Buffer` for `bytea`.
- **Extended-protocol prepared statements** with a per-connection warm cache. Repeated SQL skips Parse.
- **Pipelining** via `conn.pipeline()` with automatic cold-parse ordering + warm-parallel dispatch.
- **Connection pool** (`createPool`) with round-robin dispatch, background health monitor, and transparent reconnect.
- **Transactions + savepoints** via `conn.transaction(fn, { isolationLevel?, readOnly?, deferrable? })`. The handle proxies query methods with nested `savepoint()` support.
- **AbortSignal** on every query method. Aborting sends a backend cancel on the side channel; the connection is reusable after the cancel completes.
- **TLS negotiation** via `sslmode=disable|prefer|require` (default `prefer`). TLS uses rustls with native-root trust.
- **LISTEN/NOTIFY** iterator, `queryStream` for row-streaming, `queryColumnar` for bulk row scans, and `copyIn`/`copyOut` for COPY protocol.
- **Structured errors**: `PgError` exposes `code`, `severity`, `detail`, `hint`, `position`.

## Typed params and results

The driver keeps an OID → codec registry (`binary.TYPE_OID`). Passing explicit OIDs avoids client-side inference and lets PostgreSQL pick the best plan.

```js
import { connect, binary } from 'pg-wired';
const OID = binary.TYPE_OID;

const res = await conn.query(
  'SELECT $1::int8 AS n, $2::timestamptz AS ts, $3::jsonb AS meta',
  [9_999_999_999n, new Date(), { env: 'prod' }],
  [OID.INT8, OID.TIMESTAMPTZ, OID.JSONB],
);
// res.rows[0] = [9999999999n, Date, { env: 'prod' }]
```

If no OIDs are supplied, each value is inferred (bigint → int8, number → float8, boolean → bool, Buffer → bytea, Date → timestamptz, plain object → jsonb).

### Escape hatches

- `conn.queryRaw(sql, params, oids, paramFormats, resultFormats)` returns raw cell buffers. Use for types we don't decode natively.
- `conn.simpleQuery(sql)` uses the simple-query protocol (always text). Useful for DDL and multi-statement scripts.
- `conn.queryColumnar(sql, params, oids)` returns a flat `(data, offsets, nulls)` triple for minimum allocation overhead on large result sets.

## Source

The driver is authored in TypeScript (`src-ts/index.ts`) and compiled to CommonJS in `lib/`. Tests, benches, and scripts are `.mts` files that run directly under Node's native type-stripping, Bun, and Deno (no transpile step).

```
npm run build          # napi build + tsc
npm run typecheck      # tsc -p tsconfig.check.json (src + tests + bench + scripts)
```

## Testing

```
PG_URL=postgres://user:pass@localhost:5432/db npm test             # node --test __test__/*.test.mts
PG_URL=... npm run test:bun
PG_URL=... npm run test:deno
PG_URL=... npm run test:matrix   # runs the same suite under every installed runtime
```

`test:matrix` skips any runtime that isn't on `PATH`, so CI can run it everywhere.

## License

MIT.
