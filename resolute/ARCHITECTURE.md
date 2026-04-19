# resolute architecture

resolute is the typed query surface. It layers on top of pg-wired (raw protocol) and pg-pool (generic pool) to provide: the `Executor` trait, compile-time checked queries via the `query!` macros, a derive-driven type system (`FromRow`, `PgEnum`, `PgComposite`, `PgDomain`), context-aware transactions via `atomic()`, and a reconnecting / retrying / pooling toolbox.

This document is for people modifying resolute. Read the [`README`](README.md) for the API surface first.

## Module layout

```
src/
  lib.rs             public surface, re-exports
  executor.rs        the Executor trait, impls for Client, Transaction, PooledTypedClient
  query.rs           Client, Transaction, Pipeline, RowStream (pg-wired bridge)
  row.rs             Row struct, FromRow trait
  encode.rs          Encode trait, SqlParam object-safe wrapper, primitive + array impls
  decode.rs          Decode, DecodeText traits, binary and text decoders
  checked.rs         CheckedQuery<T>, UncheckedQuery (produced by query! macros)
  error.rs           TypedError
  oid.rs             TypeOid enum (repr u32) with round-trip helpers
  pg_type.rs         PgType trait (const OID and ARRAY_OID), scalar + Vec impls
  named_params.rs    :name -> $N rewriter (runtime path)
  pooled.rs          TypedPool, PooledTypedClient
  reconnect.rs       ReconnectingClient (ArcSwap + Mutex)
  retry.rs           RetryPolicy with exponential backoff
  listener.rs        PgListener (dedicated connection)
  metrics.rs         lock-free atomic counters, Prometheus exposition
  admin.rs           admin SQL helpers
  migrate.rs         migration runner
  newtypes.rs        PgNumeric, PgDate, PgTimestamp, PgInet wrappers
  range.rs           PgRange<T>
  types.rs           TypeInfo metadata
  test_db.rs         TestDb for #[resolute::test]
```

## The Executor trait

`Executor` ([`src/executor.rs:50`](src/executor.rs)) is the polymorphism point. Generic functions work by taking `&impl Executor`. The trait is deliberately `&self`:

```rust
pub trait Executor: Send + Sync {
    fn query<'a>(&'a self, sql: &'a str, params: &'a [&'a dyn SqlParam])
        -> impl Future<Output = Result<Vec<Row>, TypedError>> + Send + 'a;
    fn execute<'a>(&'a self, sql: &'a str, params: &'a [&'a dyn SqlParam])
        -> impl Future<Output = Result<u64, TypedError>> + Send + 'a;
    fn atomic<'a, T: Send + 'a>(
        &'a self,
        f: impl FnOnce(&'a Self) -> Pin<Box<dyn Future<Output = Result<T, TypedError>> + Send + 'a>>
              + Send + 'a,
    ) -> impl Future<Output = Result<T, TypedError>> + Send + 'a;
    fn copy_in <'a>(&'a self, sql: &'a str, data: &'a [u8])
        -> impl Future<Output = Result<u64, TypedError>> + Send + 'a;
    fn copy_out<'a>(&'a self, sql: &'a str)
        -> impl Future<Output = Result<Vec<u8>, TypedError>> + Send + 'a;
    // provided methods: query_one, query_opt, query_named, execute_named, ping
}
```

Three contrasts with sqlx's `Executor` worth knowing:

1. **No associated types.** No `Database` parameter, no `TransactionManager`. resolute is PostgreSQL-only, so the monomorphised types live directly in the trait.
2. **`&self` instead of consuming `self`.** Because `AsyncConn` internally serialises concurrent submissions through its mpsc channel, there is no Rust-level invariant that requires exclusive access. Callers share one `&Client` across many concurrent queries, and generic functions can call multiple methods on the same `&impl Executor` without re-threading ownership through every call.
3. **RPIT over boxing.** Every method returns `impl Future` (with `#[allow(clippy::manual_async_fn)]`). No async-trait box allocation on every call.

Implementors: `Client`, `Transaction<'_>`, `PooledTypedClient` ([`src/executor.rs:202-378`](src/executor.rs)). `ReconnectingClient` intentionally does **not** implement `Executor`: it exposes freestanding `query` / `execute` methods instead, because its reconnect-on-error behaviour cannot be generic over arbitrary trait methods.

## Client: the pg-wired bridge

`Client` ([`src/query.rs:47`](src/query.rs)) holds one `AsyncConn` and nothing else. No retry layer, no caching (that lives in pg-wired), no reconnect (see `ReconnectingClient`). Its `query` method:

1. Records a tracing span with `sql`, `rows`, `elapsed_us`.
2. Calls `conn.exec_query(sql, params, ...)`.
3. On `PgWireError::Pg(err)` with SQLSTATE `26000` or `0A000` (stale prepared statement), invalidates the cache entry and retries once. This handles the `DISCARD ALL` race where a prepared statement has been dropped server-side out from under us.
4. Maps `PgWireError` into `TypedError`, tags with SQL via `.with_sql(sql)` so error messages always include context.
5. Records metrics (`record_query(elapsed_us)` or `record_query_error()`).

The bridge is thin because pg-wired already does the hard work (statement cache, coalescing, FIFO match). `Client` is essentially typed-error-mapping and tracing around the raw wire.

## atomic(): how the BEGIN / SAVEPOINT dispatch works

`atomic` is a method on `Executor`, and each `impl Executor for X` has its own body. There is no runtime flag and no dynamic dispatch: the behaviour difference is pure monomorphisation.

- **`Client::atomic`** ([`src/executor.rs:234`](src/executor.rs)): issues `BEGIN`, runs the closure, then `COMMIT` on `Ok` or `ROLLBACK` on `Err`.
- **`Transaction::atomic`** ([`src/executor.rs:291`](src/executor.rs)): allocates a savepoint name from `SAVEPOINT_COUNTER: AtomicU64` (line 27) and issues `SAVEPOINT resolute_sp_{id}`, then `RELEASE SAVEPOINT` / `ROLLBACK TO SAVEPOINT`.
- **`PooledTypedClient::atomic`** ([`src/executor.rs:356`](src/executor.rs)): same as `Client` since a pooled checkout is not already inside a transaction.

Because each impl picks the right SQL, a generic function written against `&impl Executor` and calling `db.atomic(...)` does the right thing without ever inspecting the concrete type. This is how resolute gives you "auto-BEGIN on Client, auto-SAVEPOINT on Transaction" composability: a helper function written once works at both levels of nesting, without either knowing whether it is the outermost or an inner caller.

The savepoint counter is a process-wide monotonic `AtomicU64`, so names never collide between concurrent tasks sharing one transaction (rare but legal).

## Transaction

```rust
// src/query.rs:806
pub struct Transaction<'a> {
    pub(crate) client: &'a Client,
    pub(crate) done: bool,
}
```

No savepoint stack, no depth counter: `Transaction` represents exactly one `BEGIN` block. Nesting happens through `atomic()` dispatch, not through Transaction state.

Explicit commit is required:

- `commit(mut self)` sets `done = true`, sends `COMMIT`.
- `rollback(mut self)` same, with `ROLLBACK`.
- `Drop` ([`src/query.rs:853`](src/query.rs)) checks `!self.done` and emits `tracing::warn!`. It cannot send `ROLLBACK` because `Drop::drop` is not async. The comment at line 863 notes that PostgreSQL auto-rolls-back the open transaction on the next statement, so the correctness concern is "the DB will recover," not "we might leak a lock."

This is a deliberate choice over a Drop-based auto-rollback: an async drop would require blocking the runtime or spawning a task, both of which are surprising failure modes. Explicit `.commit()` / `.rollback()` keeps the call site honest.

## FromRow derive: how the attributes expand

The FromRow derive ([`resolute-derive/src/lib.rs:36`](../resolute-derive/src/lib.rs)) emits inline code that calls three runtime helpers on `Row` ([`src/row.rs:67`](src/row.rs)):

- `Row::get_by_name<T>`: decode by column name.
- `Row::get_opt_by_name<T>`: decode, returning `None` on NULL.
- `Row::has_column`: check for column presence in the `RowDescription`.

Attributes map to these primitives:

| Attribute | Expansion |
|---|---|
| (default) | `row.get_by_name::<T>("name")?` |
| `skip` | `Default::default()` |
| `flatten` | `<T as FromRow>::from_row(row)?` |
| `try_from = "S"` | `row.get_by_name::<S>("name")?.try_into()?` |
| `json` | `serde_json::from_value(row.get_by_name::<serde_json::Value>("name")?)?` |
| `default` | `if row.has_column("name") { row.get_by_name(...) else fail? } else { Default::default() }` |

Incompatible combinations (`skip + default`, `flatten + json`, etc.) error at compile time inside the derive, not at runtime.

## PgEnum: string versus integer-backed

The derive ([`resolute-derive/src/pg_enum.rs:30`](../resolute-derive/src/pg_enum.rs)) inspects `#[repr(...)]` first via `get_repr_int`. An `i16`/`i32`/`i64` repr routes to `derive_int_enum`; anything else routes to `derive_string_enum`.

- **String path**: `Encode::encode` writes the variant label as UTF-8. `Decode::decode` matches `str::from_utf8(buf)` against the label set. `type_oid()` returns `TypeOid::Unspecified` so PostgreSQL resolves the enum OID from context (the column's declared type).
- **Integer path**: `Encode::encode` casts the discriminant to the repr type (`*self as i32`) and delegates to that type's `encode`, which produces the big-endian wire bytes. `Decode::decode` reads the fixed-width integer and matches against `(expr) as repr_type` arms. Default OIDs come from the repr (`i32` => `TypeOid::Int4`). Custom OIDs via `#[pg_type(oid = N)]` override.

Both paths check that required discriminants are present. The integer path emits a compile error if any variant lacks `= N`, intentionally preventing the silent-breakage case where reordering variants changes the stored value (see the "Design decisions" note in the crate README).

## PgComposite and PgDomain

**Composite binary format** ([`resolute-derive/src/pg_composite.rs`](../resolute-derive/src/pg_composite.rs)) follows the PostgreSQL documented layout:

```
nfields: i32
for each field:
    oid: u32
    len: i32             (-1 for NULL)
    data: [u8; len]
```

`Encode` writes the header, then each field's OID (from its `PgType::OID`) followed by `encode_param`, which adds the length prefix. Text-format decode is not supported for composites: the encoding is too complex for the ad-hoc parser, and the binary protocol is always available in practice.

**Domain types** ([`resolute-derive/src/pg_domain.rs`](../resolute-derive/src/pg_domain.rs)) are transparent newtypes. All encode / decode paths delegate to `self.0`. The interesting piece is `ARRAY_OID`:

```rust
const ARRAY_OID: u32 = if CUSTOM_ARRAY_OID != 0 {
    CUSTOM_ARRAY_OID
} else {
    <Inner as PgType>::ARRAY_OID
};
```

This is a const expression evaluated at compile time. `Email(String)` inherits `String::ARRAY_OID = 1009` (`_text`) with zero runtime overhead, so PostgreSQL knows how to handle `Email[]` columns and parameters correctly. Explicit `#[pg_type(array_oid = N)]` overrides the inheritance for non-standard cases.

## TypedPool

`TypedPool` ([`src/pooled.rs:20`](src/pooled.rs)) wraps `Arc<ConnPool<AsyncPoolable>>`. The default path (`TypedPool::connect`) registers no lifecycle hooks: `ConnPool::new(config, LifecycleHooks::default())`. `AsyncPoolable::reset` (in pg-pool, [`async_wire.rs:37`](../pg-pool/src/async_wire.rs)) already does `DISCARD ALL` and clears the pg-wired statement cache, so the most important cleanup is wired in at the pg-pool layer, not at TypedPool.

To attach custom hooks (tracing on checkout, metrics on create), use `TypedPool::new(config, hooks)` and pass a `LifecycleHooks<AsyncPoolable>`.

`PooledTypedClient` holds a `PoolGuard<AsyncPoolable>` (pg-pool's checkout token). Its query methods call `Client::query_on_conn` / `execute_on_conn` ([`src/query.rs:107`](src/query.rs)), which are `pub(crate)` helpers that accept a `&AsyncConn` directly. This avoids double-wrapping: a `Client` newtype around the `PoolGuard` would have to reach through twice on every call.

## PgListener

`PgListener` ([`src/listener.rs:22`](src/listener.rs)) is always on a dedicated connection. Sharing a connection with query traffic would conflict with FIFO response matching, because `LISTEN` is a synchronous SQL statement but `NotificationResponse` arrives asynchronously.

Internally it wraps a pg-wired `PgPipeline` (the synchronous low-level client). `recv` loops over `self.pipeline.conn().recv_msg()` and filters for `BackendMsg::NotificationResponse`, discarding `ParameterStatus` and other asynchronous messages.

The `AsyncConn`-based notification path (forwarding to `notification_tx`) exists in pg-wired for a different use case: query traffic that happens to coincide with a notification on the same connection. `PgListener` is the dedicated-connection model.

## Streaming, pipelining, COPY

- **Streaming** ([`src/query.rs:490`](src/query.rs)): `query_stream` assembles the same `Parse+Bind+Describe+Execute+Sync` sequence, submits via `AsyncConn::submit_stream`, and returns a `RowStream` that polls an `mpsc::Receiver` in its `Stream` impl. Default buffer size is 256 rows (`Client::DEFAULT_STREAM_BUFFER`).
- **Pipelining** ([`src/query.rs:900`](src/query.rs)): `Pipeline` batches multiple `Parse+Bind+Execute+Sync` frames into one `BytesMut`. It is write-coalescing: one large buffer, one set of responses in FIFO order. Not true async pipelining (each call still awaits one `ReadyForQuery`), but enough to eliminate per-query round-trips.
- **COPY**: direct delegation to `AsyncConn::copy_in` / `AsyncConn::copy_out`.

## Retry

`RetryPolicy` ([`src/retry.rs:37`](src/retry.rs)) holds `{ max_retries, initial_backoff, max_backoff }` and does exponential backoff (`backoff = (backoff * 2).min(max_backoff)`). It retries only on transient errors:

- `PgWireError::Io`
- `PgWireError::ConnectionClosed`
- SQLSTATE `08000 / 08001 / 08003 / 08004 / 08006` (connection exceptions)
- SQLSTATE `40001` (serialization failure)
- SQLSTATE `40P01` (deadlock detected)
- SQLSTATE `57P01 / 57P02 / 57P03` (operator intervention: admin shutdown, crash shutdown, cannot connect now)

Everything else passes through without retry. Syntax errors, constraint violations, and permission denied are never retried.

`RetryPolicy::execute` is generic over `&impl Executor`, so the same policy works against a `Client`, `Transaction`, or `TypedPool` checkout.

## ReconnectingClient

```rust
// src/reconnect.rs:45
pub struct ReconnectingClient {
    client: ArcSwap<Client>,
    reconnect_lock: Mutex<()>,
    addr: String, user: String, password: String, database: String,
    init_sql: Vec<String>,
}
```

Read paths are lock-free: `ArcSwap::load()` gives a short-lived `Guard<Arc<Client>>`. The `reconnect_lock` only matters during a reconnect handshake. Critically, the reconnect method double-checks `client.load().is_alive()` **after** acquiring the mutex ([`src/reconnect.rs:75`](src/reconnect.rs)). This is the thundering-herd fix: when a connection drops, many concurrent tasks notice simultaneously and all try to reconnect. The first through the mutex performs the reconnect; every other task finds the freshly replaced client on its second `is_alive` check and skips the work.

The newly built `Client` is installed via `ArcSwap::store(Arc::new(new_client))`, which is lock-free relative to readers.

## Error type

`TypedError` ([`src/error.rs`](src/error.rs)) is the unified error the typed API returns. Variants cover:

- `Pool(PoolError)`, `Timeout`, `MissingParam(String)`, `Io(io::Error)`, `Config(String)`, `QueryFailed(String)`.
- `PgWire(PgWireError)` for lower-level errors that propagate up.
- Per-error SQL context via `.with_sql(sql)`. `Client` always attaches SQL at the call site so error messages include the failing statement.

## Metrics

`metrics.rs` exposes seven `AtomicU64` counters, all updated with `Ordering::Relaxed`:

- `query_total`, `query_errors`, `query_duration_us_total` (for rate computations)
- `pool_timeouts`
- `reconnect_total`, `reconnect_failures`
- `notification_total`

`metrics::snapshot()` returns a struct of current values. `metrics::gather()` returns Prometheus exposition format. Both read without locking, so they are safe to call from any thread at any frequency.

## Design decisions summary

- **PostgreSQL only.** The whole stack assumes wire protocol v3, PostgreSQL OIDs, and PostgreSQL-specific semantics (LISTEN/NOTIFY, advisory locks, COPY). No `Any` abstraction.
- **Non-consuming Executor.** `&self` on every method. Generic functions compose freely.
- **atomic() via monomorphisation.** No runtime context flag. Each `impl` picks the right SQL.
- **Explicit commit on Transaction.** No async drop. Caller must call `.commit()` or `.rollback()`.
- **Derive macros do heavy lifting.** `FromRow`, `PgEnum`, `PgComposite`, `PgDomain` emit direct calls to small runtime helpers rather than using generic reflection. Each derived impl is as efficient as a hand-written one.
