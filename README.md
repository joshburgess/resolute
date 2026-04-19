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
| **resolute** | Compile-time checked queries. 7 query macros with type overrides (`"col: Type"`), `Executor` trait, `atomic()` with savepoint nesting, named params, `FromRow` (with `skip`/`default`/`json`/`try_from`/`flatten`), `PgEnum` (string + integer-backed), `PgComposite`, `PgDomain` (with array OID inheritance), `TypedPool` with lifecycle hooks, `PgListener`, streaming, pipelining, COPY, retry, auto-reconnect, metrics. |
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
- You care about performance. Resolute implements the wire protocol, pool, and typed surface from scratch rather than layering over tokio-postgres, and it uses the binary wire format throughout. Benchmarks against the same PostgreSQL instance show it running 2 to 5 times faster than sqlx on parameter encoding and roughly 2.4 times faster on end-to-end query latency. The "Performance" section below lists the full numbers.

If that sounds like you, keep reading.

## Why Resolute

**Owns the full stack.** Most Rust database libraries build on top of tokio-postgres or a shared protocol layer. Resolute implements the PostgreSQL wire protocol from scratch (`pg-wired`), the connection pool from scratch (`pg-pool`), and the typed query layer on top. This means fewer transitive dependencies, fewer abstraction layers, and full control over performance.

**Non-consuming Executor.** sqlx's `Executor` trait consumes `self`, making it awkward to run multiple queries on the same generic executor. Resolute's `Executor` uses `&self`. Write a function once, call it with a `Client`, `Transaction`, or `Pool`.

**Named parameters.** Use `:name` syntax in both compile-time macros and runtime queries. Handles `::` casts, string literals, comments, and dollar-quoting correctly. Not available in sqlx.

**Context-aware transactions.** `db.atomic(|db| ...)` issues `BEGIN/COMMIT` when called on a `Client`, `SAVEPOINT/RELEASE` when called on a `Transaction`. Write composable transactional functions without knowing the caller's context.

**Rich FromRow derive.** Beyond basic column mapping, resolute's `FromRow` supports `skip` (use defaults), `default` (fall back on missing/NULL), `json` (serde deserialization), `try_from` (type conversion with validation), and `flatten` (nested struct composition). sqlx's `FromRow` has similar attributes but resolute's `default` handles both missing columns and NULL values.

**Integer-backed enums.** `#[derive(PgEnum)]` with `#[repr(i32)]` stores enum variants as integers, not strings. Useful for existing schemas with integer status columns. Not available in sqlx without manual `Type`/`Encode`/`Decode` impls.

**Query type overrides.** `query!(r#"SELECT id as "id: UserId" FROM users"#)` maps columns to custom Rust types in compile-time macros. Works with any type that implements `Decode`.

**Binary format everywhere.** Parameters and results use PostgreSQL's binary wire format. No text parsing, no intermediate representations. 2-5x faster than sqlx on encode, 2.3-2.5x faster on query latency.

**Statement caching.** `Parse` once per connection, `Bind+Execute` on reuse. LRU cache with 256 entries per connection.

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

Benchmarked against sqlx 0.8 on the same queries, same PostgreSQL instance:

| Benchmark | resolute | sqlx | Speedup |
|-----------|----------|------|---------|
| Encode i32 | 4.5 ns | 22 ns | 4.9x |
| Encode String | 8.2 ns | 35 ns | 4.3x |
| Encode UUID | 5.1 ns | 24 ns | 4.7x |
| Decode i32 | 1.8 ns | 5.2 ns | 2.9x |
| Query (SELECT 1) | 89 us | 210 us | 2.4x |
| Query (10 cols) | 125 us | 295 us | 2.4x |

Run benchmarks: `cargo bench -p resolute`

## Feature Flags

| Feature | Default | Enables |
|---------|---------|---------|
| `chrono` | yes | `NaiveDate`, `NaiveTime`, `NaiveDateTime`, `DateTime<Utc>` |
| `json` | yes | `serde_json::Value` for JSON/JSONB |
| `uuid` | yes | `uuid::Uuid` |

pg-wired has an optional `tls` feature for rustls-based TLS connections.

## Documentation

**API guide.** See [`resolute/README.md`](resolute/README.md) for the full API with examples covering named params, transactions, custom types, streaming, pipelining, COPY, retry, reconnect, pooling, migrations, and more.

**Architecture.** [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) is the top-level tour of how the layers compose. Each crate also has its own deep-dive:

- [`pg-wired/ARCHITECTURE.md`](pg-wired/ARCHITECTURE.md): wire protocol v3, statement cache, TCP coalescing, FIFO response matching, TLS, SCRAM.
- [`pg-pool/ARCHITECTURE.md`](pg-pool/ARCHITECTURE.md): the pool engine, lifecycle hooks, drain protocol.
- [`resolute/ARCHITECTURE.md`](resolute/ARCHITECTURE.md): `Executor` trait, `atomic()` dispatch, derives, typed client.
- [`resolute-macros/ARCHITECTURE.md`](resolute-macros/ARCHITECTURE.md): the compile-time query validator, named-param rewriter, describe pipeline, offline cache.

**Performance.** [`docs/PERFORMANCE.md`](docs/PERFORMANCE.md) explains where the speedup comes from: binary encode fast path, statement caching, TCP write coalescing, FIFO response matching, lock-free atomics. Also describes the benchmark methodology.

**Roadmap.** [`docs/ROADMAP.md`](docs/ROADMAP.md).

## License

Dual licensed under [Apache 2.0](LICENSE-APACHE) or [MIT](LICENSE-MIT).
