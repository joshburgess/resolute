# Resolute

> **res·o·lute** /ˈrezəˌlo͞ot/
>
> *adjective*: admirably purposeful, determined, and unwavering.
>
> *noun*: one who is steadfast and unyielding in purpose, firm in conviction even in the face of opposition or difficulty.

A ground-up PostgreSQL client stack for Rust. Compile-time checked queries, binary wire protocol, async connection pool. No dependencies on tokio-postgres, sqlx, deadpool, bb8, or any other existing PostgreSQL or connection pool library.

## Crates

| Crate | Description |
|-------|-------------|
| **pg-wired** | PostgreSQL wire protocol v3. Async connections, extended query protocol (Parse/Bind/Describe/Execute/Sync), binary format, statement caching, pipelining, LISTEN/NOTIFY, COPY, cancellation, optional TLS (rustls). |
| **pg-pool** | Generic async connection pool. Checkout/checkin, 6 lifecycle hooks (`before_acquire`/`on_create`/`on_checkout`/`on_checkin`/`after_release`/`on_destroy`, connection-aware where applicable), idle timeout, health monitoring, metrics, min/max connections, drain. Works with any connection type via the `Poolable` trait. |
| **pg-wired-js** | JavaScript bindings for `pg-wired` via napi-rs. Published to npm as `pg-wired`. One prebuilt native binding per platform, same artifact works in Node.js >= 22.6, Bun >= 1.1, and Deno >= 2.1. Full binary wire protocol, extended-protocol prepared statements, pipelining, connection pool, transactions with savepoints, `AbortSignal` cancellation, TLS, LISTEN/NOTIFY, streaming, COPY. |
| **resolute** | Compile-time checked queries. 7 query macros with type overrides (`"col: Type"`), `Executor` trait, `atomic()` with savepoint nesting, named params, `FromRow` (with `skip`/`default`/`json`/`try_from`/`flatten`), `PgEnum` (string + integer-backed), `PgComposite`, `PgDomain` (with array OID inheritance), `ExclusivePool` with lifecycle hooks, `PgListener`, streaming, pipelining, COPY, retry, auto-reconnect, metrics. |
| **resolute-derive** | Proc-macro crate. `FromRow`, `PgEnum`, `PgComposite`, `PgDomain` derives and `#[resolute::test]` attribute macro. |
| **resolute-macros** | Proc-macro crate. Compile-time query validation against a live database or offline cache. Named parameter rewriting. |
| **resolute-cli** | CLI tool. Offline cache management (`prepare`, `check`), migrations (`create`, `run`, `revert`, `status`, `info`, `validate`, `seed`), database lifecycle (`create`, `drop`). |

## Quick Start

```rust
use resolute::{Client, query};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("DATABASE_URL")?;
    let client = Client::connect_from_str(&url).await?;

    let authors = query!("SELECT id, name FROM authors WHERE id = $1", 1)
        .fetch_all(&client)
        .await?;

    for a in &authors {
        println!("{}: {}", a.id, a.name);
    }
    Ok(())
}
```

## Who Resolute is For

Resolute is for you if:

- You have used sqlx and like its workflow and general selling points: compile-time checked queries against a live database, offline cache for CI, derive-driven `FromRow`. You do not want to give any of that up.
- You only target PostgreSQL. You are not looking for a cross-database abstraction and you do not want the lowest-common-denominator API that comes with one. You would rather have first-class support for PostgreSQL features (LISTEN/NOTIFY, advisory locks, COPY, composites, domains, ranges, integer-backed enums) than portability to MySQL or SQLite.
- You want a more flexible, more convenient API than sqlx offers: named parameters that survive dollar-quoting and casts, an `Executor` trait that takes `&self` so generic helpers compose, an `atomic()` method that dispatches `BEGIN` or `SAVEPOINT` based on the receiver so transactional helpers nest without knowing the caller's context, and richer `FromRow` attributes. See "Why Resolute" below for the full list.
- You care about performance on small, frequent queries. Resolute implements the wire protocol, pool, and typed surface from scratch rather than layering over tokio-postgres, and it uses the binary wire format throughout. Benchmarks against the same PostgreSQL instance show roughly 4 to 5 times faster parameter encoding and 2 to 3 times faster end-to-end latency on small queries. Under concurrent load the gap is 2 to 2.5 times with the exclusive `ExclusivePool` and 3 to 8 times with the multiplexed `SharedPool`. On large result sets resolute is up to 2.5 times faster. On server-bound queries (heavy aggregations, scans) the two drivers are at parity. See the "Performance" section for the full picture.

If that sounds like you, keep reading.

## Why Resolute

**Owns the full stack.** Most Rust database libraries build on top of tokio-postgres or a shared protocol layer. Resolute implements the PostgreSQL wire protocol from scratch (`pg-wired`), the connection pool from scratch (`pg-pool`), and the typed query layer on top. This means fewer transitive dependencies, fewer abstraction layers, and full control over performance.

**Non-consuming Executor.** sqlx's `Executor` trait consumes `self`, making it awkward to run multiple queries on the same generic executor. Resolute's `Executor` uses `&self`. Write a function once, call it with a `Client`, `Transaction`, or `Pool`.

**Named parameters.** Use `:name` syntax in both compile-time macros and runtime queries. Handles `::` casts, string literals, comments, and dollar-quoting correctly. Not available in sqlx.

**Context-aware transactions.** `db.atomic(|db| ...)` issues `BEGIN/COMMIT` when called on a `Client`, `SAVEPOINT/RELEASE` when called on a `Transaction`. Write composable transactional functions without knowing the caller's context.

**Rich FromRow derive.** Beyond basic column mapping, resolute's `FromRow` supports `skip` (use defaults), `default` (fall back on missing/NULL), `json` (serde deserialization), `try_from` (type conversion with validation), and `flatten` (nested struct composition). sqlx's `FromRow` has similar attributes but resolute's `default` handles both missing columns and NULL values.

**Integer-backed enums.** `#[derive(PgEnum)]` with `#[repr(i32)]` stores enum variants as integers, not strings. Useful for existing schemas with integer status columns. Not available in sqlx without manual `Type`/`Encode`/`Decode` impls.

**Query type overrides.** `query!(r#"SELECT id as "id: UserId" FROM users"#)` maps columns to custom Rust types in compile-time macros. Works with any type that implements `Decode`.

**Binary format everywhere.** Parameters and results use PostgreSQL's binary wire format. No text parsing, no intermediate representations. Roughly 4-5x faster than sqlx on encode (most types), 2-3x faster on small-query latency, 2-2.5x faster on large-result decode, 2-2.5x faster under concurrent load with `ExclusivePool` (exclusive), and 3-8x faster with `SharedPool` (multiplexed; the writer task fuses concurrent submissions into batched writes). The Performance section has the full breakdown.

**Statement caching.** `Parse` once per connection, `Bind+Execute` on reuse. Pseudo-LRU cache with 256 entries per connection.

**Message coalescing.** The writer task batches concurrent requests into a single `write()` syscall. The reader task FIFO-matches responses. Multiple queries from different tasks share one TCP connection efficiently.

## Architecture

```
resolute               ── query macros, Executor trait, typed API
  ├── resolute-derive  ── proc-macro derives
  ├── resolute-macros  ── compile-time query validation
  ├── pg-wired         ── PostgreSQL wire protocol v3
  └── pg-pool          ── generic async connection pool

resolute-cli           ── offline cache + migrations CLI
  └── pg-wired

pg-wired-js            ── JS bindings (Node, Bun, Deno) via napi-rs
  └── pg-wired
```

## Performance

Benchmarked against sqlx 0.8 on the same queries, same PostgreSQL instance. Numbers below are from an Apple M4 Max running `postgres:17-alpine` over a local Docker socket; see [`docs/PERFORMANCE.md`](docs/PERFORMANCE.md) for methodology.

**Encode throughput (in-process, no network):**

| Type | resolute | sqlx | Speedup |
|------|----------|------|---------|
| `i32` | 3.3 ns | 14 ns | 4.3x |
| `i64` | 3.0 ns | 15 ns | 4.9x |
| `String` (39 bytes) | 5.2 ns | 26 ns | 5.0x |
| `Uuid` | 3.0 ns | 16 ns | 5.4x |
| `DateTime<Utc>` | 3.9 ns | 19 ns | 4.9x |
| `serde_json::Value` (small object) | 63 ns | 146 ns | 2.3x |

**End-to-end query latency (single connection, localhost Postgres):**

| Scenario | resolute | sqlx | Speedup |
|----------|----------|------|---------|
| `SELECT 1` | 78 µs | 189 µs | 2.4x |
| Parameterized `SELECT $1::int4` | 76 µs | 141 µs | 1.9x |
| 3-column / 3-param SELECT | 62 µs | 152 µs | 2.5x |
| 100-row `generate_series` | 72 µs | 158 µs | 2.2x |
| INSERT + DELETE round-trip | 132 µs | 300 µs | 2.3x |
| Warm prepared-statement cache hit | 58 µs | 187 µs | 3.2x |

**Concurrent load and result-heavy queries:**

Resolute ships two pool flavors: `ExclusivePool` (exclusive checkout, needed for transactions and other session-stateful work) and `SharedPool` (multiplexed: many tasks share each connection's writer task, no semaphore, no waiter queue). The shared pool is the right choice for self-contained `SELECT` / DML workloads and produces the largest concurrency wins.

| Scenario | resolute (exclusive) | resolute (shared) | sqlx | excl/sqlx | shared/sqlx |
|----------|----------------------|-------------------|------|-----------|-------------|
| 4 connections, 16 concurrent `SELECT 1` tasks | 355 µs | 237 µs | 798 µs | 2.25x | 3.37x |
| 1 connection, 8 concurrent tasks (coalescing test) | 372 µs | 117 µs | 938 µs | 2.52x | 8.02x |
| 8 connections, 64 concurrent tasks (oversubscribed) | 1.11 ms | 542 µs | 2.68 ms | 2.42x | 4.95x |
| `SELECT count(*) FROM generate_series(1, 100k)` (server-bound) | 5.52 ms | n/a | 5.69 ms | 1.03x (parity) | n/a |
| 10 000 single-column rows (large result decode) | 775 µs | n/a | 1.89 ms | 2.44x | n/a |
| 1 000 rows × 10 mixed-type columns (wide-row decode) | 826 µs | n/a | 925 µs | 1.12x | n/a |

A few notes on what to take from this:

- **Both pool flavors beat sqlx by 2x+ on concurrent workloads.** `ExclusivePool` (exclusive checkout) skips `DISCARD ALL` on return when the session was not actually mutated, so the bonus round-trip that sqlx pays per checkout disappears for plain `SELECT` / DML. `SharedPool` adds writer-task multiplexing on top of that: many tasks submit concurrently to the same connection and the writer fuses their messages into one `write_all`. The single-connection coalescing test (8 tasks, 1 conn) shows this most clearly: 117 µs for 8 round-trips fused into one batch.
- **Use `ExclusivePool` for session-stateful work.** Transactions, advisory locks, `SET LOCAL`, prepared-statement reuse keyed by name. The exclusive checkout is required for correctness; `DISCARD ALL` runs only when the connection actually mutated session state.
- **Use `SharedPool` for self-contained concurrent SELECT / DML.** No semaphore, no waiter queue, batched writes through the writer task.
- **Server-bound queries are at parity.** When Postgres is doing real work, driver overhead is noise.
- **Large result decode and wide rows are resolute wins** (2.44x and 1.12x), driven by the binary wire format and zero-copy `Bytes`-backed cells.

Run benchmarks: `cargo bench -p resolute`

## Feature Flags

| Feature | Default | Enables |
|---------|---------|---------|
| `chrono` | yes | `NaiveDate`, `NaiveTime`, `NaiveDateTime`, `DateTime<Utc>` |
| `json` | yes | `serde_json::Value` for JSON/JSONB |
| `uuid` | yes | `uuid::Uuid` |
| `test-utils` | no | Exposes the `resolute::test_db` module: ephemeral test databases (`TestDb::create`, `create_with_migrations`, `drop_db`) and env-driven connection helpers (`test_addr`, `test_user`, `test_password`, `test_database`, `test_database_url`). Read by `RESOLUTE_TEST_ADDR`, `RESOLUTE_TEST_USER`, `RESOLUTE_TEST_PASSWORD`, `RESOLUTE_TEST_DB`. Also enabled automatically by the `#[resolute::test]` attribute macro. |

pg-wired has an optional `tls` feature for rustls-based TLS connections.

## Documentation

**API guide.** See [`resolute/README.md`](resolute/README.md) for the full API with examples covering named params, transactions, custom types, streaming, pipelining, COPY, retry, reconnect, pooling, migrations, and more.

**Architecture.** [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) is the top-level tour of how the layers compose. Each crate also has its own deep-dive:

- [`pg-wired/ARCHITECTURE.md`](pg-wired/ARCHITECTURE.md): wire protocol v3, statement cache, TCP coalescing, FIFO response matching, TLS, SCRAM.
- [`pg-pool/ARCHITECTURE.md`](pg-pool/ARCHITECTURE.md): the pool engine, lifecycle hooks, drain protocol.
- [`resolute/ARCHITECTURE.md`](resolute/ARCHITECTURE.md): `Executor` trait, `atomic()` dispatch, derives, typed client.
- [`resolute-macros/ARCHITECTURE.md`](resolute-macros/ARCHITECTURE.md): the compile-time query validator, named-param rewriter, describe pipeline, offline cache.

**Performance.** [`docs/PERFORMANCE.md`](docs/PERFORMANCE.md) explains where the speedup comes from: binary encode fast path, statement caching, TCP write coalescing, FIFO response matching, lock-free atomics. Also describes the benchmark methodology.

## Minimum Supported Rust Version

Resolute builds on stable Rust 1.85 and newer. Bumping the MSRV is considered a minor-version change.

## License

Dual licensed under [Apache 2.0](LICENSE-APACHE) or [MIT](LICENSE-MIT).
