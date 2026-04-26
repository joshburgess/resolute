# Architecture

Resolute is a layered stack. This document covers how the layers compose and where responsibilities live. For the details inside each layer, see the per-crate docs:

- [`pg-wired/ARCHITECTURE.md`](../pg-wired/ARCHITECTURE.md): wire protocol v3, statement cache, TCP coalescing, FIFO response matching, TLS, SCRAM.
- [`pg-pool/ARCHITECTURE.md`](../pg-pool/ARCHITECTURE.md): the generic pool engine, lifecycle hooks, drain protocol.
- [`resolute/ARCHITECTURE.md`](../resolute/ARCHITECTURE.md): `Executor` trait, `atomic()` dispatch, `FromRow` derive, typed client.
- [`resolute-macros/ARCHITECTURE.md`](../resolute-macros/ARCHITECTURE.md): the compile-time query validator, named-param rewriter, describe pipeline, offline cache.

Cross-cutting performance analysis lives in [`PERFORMANCE.md`](PERFORMANCE.md).

## Layer responsibilities

Dependency graph:

```
resolute-cli
  ├── resolute
  └── pg-wired       (directly, for describe in prepare/check)

resolute
  ├── resolute-macros    (proc-macro, compile-time)
  ├── resolute-derive    (proc-macro, compile-time)
  ├── pg-wired
  └── pg-pool

pg-wired, pg-pool  ──►  tokio
```

| Crate | Role |
|---|---|
| `resolute-cli` | Offline cache management (`prepare`, `check`), migrations, database lifecycle |
| `resolute` | Typed query surface: `Executor` trait, `atomic()`, `Client`, `TypedPool`, `ReconnectingClient`, `RetryPolicy`, `PgListener`, `Encode`/`Decode`, `FromRow` |
| `resolute-macros` | Compile-time query validation (`query!` and variants), named-param rewriter, offline cache |
| `resolute-derive` | Proc-macro derives: `FromRow`, `PgEnum`, `PgComposite`, `PgDomain` |
| `pg-wired` | PostgreSQL wire protocol v3: async connection, statement cache, TCP coalescing, TLS, SCRAM |
| `pg-pool` | Generic async connection pool (agnostic to PostgreSQL), lifecycle hooks, drain |
| `tokio` | Async runtime (external dependency) |

A few invariants fall out of this layout:

- **pg-wired does not know about typed values.** It encodes/decodes raw `Option<&[u8]>`, not `i32` or `String`. Binary vs text is a format-code flag on Bind. Caller owns interpretation.
- **pg-pool does not know about PostgreSQL.** It takes any `Poolable` type. The PG-specific adapters (`WirePoolable`, `AsyncPoolable`) live in pg-pool behind a `wire` feature flag so non-PG users of pg-pool have no unwanted dependencies.
- **resolute is the integration layer.** It owns the typed surface (`Encode`, `Decode`, `FromRow`, `Executor`), ties a pool to a `Client` ergonomically (`TypedPool`), and provides the reconnecting / retrying / listener toolbox.
- **resolute-macros is the compile-time layer.** It is a peer of resolute, not a layer above. It depends on pg-wired directly for the one describe call it makes at build time, and emits code that uses resolute's runtime types.

## Request life cycle

Tracing a single `query!` call from source through to response:

1. **Compile time** (resolute-macros):
   1. Parse `query!("SELECT id FROM t WHERE x = :x", x = 1)` input.
   2. Rewrite `:x` to `$1`, produce ordered params.
   3. Hash the rewritten SQL.
   4. Look up `.resolute/query-{hash}.json`. Cache hit: skip to 5. Miss and online: connect to `$DATABASE_URL` via pg-wired, `Parse + Describe + Sync`, extract param OIDs, column OIDs, nullability from `pg_attribute`. Write cache file. Miss and offline: compile error.
   5. Generate a struct, param type assertions, and a `CheckedQuery<__QueryResult_HASH>` value.
2. **Runtime** (resolute):
   1. User calls `.fetch_all(&client)` on the `CheckedQuery`. This dispatches through the `Executor` trait.
   2. `Client::query` encodes params using the `Encode` trait impls (big-endian binary writes into a `BytesMut`), builds the `Bind` message with per-param / per-column format codes, and assembles the full `Parse + Bind + Execute + Sync` sequence (first call) or `Bind + Execute + Sync` (cache hit, via pg-wired's statement cache).
   3. Submits to `AsyncConn` via `submit(buf, collector)`.
3. **Wire layer** (pg-wired):
   1. `submit` pushes a `PipelineRequest` onto the writer's mpsc channel.
   2. The writer task is already blocked on `rx.recv()`. It wakes, clears the write buffer, appends this request, drains any concurrently queued requests, and issues one `write_all` syscall.
   3. `PendingResponse` entries are pushed to the shared `VecDeque`, reader is notified.
   4. The reader parses backend messages, feeds the collector, and on `ReadyForQuery` sends the result on the per-request oneshot.
4. **Back up** (resolute):
   1. `Client::query` decodes `DataRow`s into `Row`s, maps each through the `mapper: fn(&Row) -> T` emitted by the macro, returns `Vec<T>`.

The handoff points are all narrow: typed values become bytes at `Encode`, bytes become typed values at `Row::get`, everything in between is `&[u8]`.

## Pool integration

A `TypedPool` is a thin wrapper over `pg-pool`'s `ConnPool<AsyncPoolable>`. The Poolable impl (`AsyncPoolable` in pg-pool, under `wire` feature) carries two PG-aware behaviours:

- `has_pending_data` is `!conn.is_alive()`: a dead `AsyncConn` is destroyed rather than returned to idle.
- `reset` is conditional: a per-connection "state mutated" flag is set on non-idle `ReadyForQuery` transaction status (or by an explicit `mark_state_mutated()` call from code that issues `SET`, advisory locks, `LISTEN`, etc. via simple-query). On checkin, if the flag is clear (the common case for plain Bind/Execute/Sync workloads), the reset is a no-op round-trip. If the flag is set, `DISCARD ALL` is sent and pg-wired's per-connection statement cache is cleared so the next checkout starts clean.

All six pg-pool lifecycle hooks (`before_acquire`, `on_create`, `on_checkout`, `on_checkin`, `after_release`, `on_destroy`) are available through `TypedPool::new` for users who need them.

## Transaction composition

`atomic()` is a method on the `Executor` trait, and each concrete `impl Executor for X` defines its own body. Because Rust picks the impl at monomorphisation time, a generic helper:

```rust
async fn transfer(db: &impl Executor, from: i32, to: i32, amt: i64) -> Result<(), _> {
    db.atomic(|db| Box::pin(async move { ... })).await
}
```

does `BEGIN/COMMIT` when called with a `Client` and `SAVEPOINT/RELEASE` when called with a `Transaction`, with no runtime introspection. This is the key composability property: arbitrary transactional helpers nest correctly without any of them knowing whether they are the outermost transaction or an inner call.

## Reconnecting client vs pool

Two different approaches to "keep working through connection failures":

- **`ReconnectingClient`**: one logical connection that silently rebuilds the `AsyncConn` under ArcSwap when the underlying connection drops. Use it for long-lived single-threaded workloads (daemons, listeners, CLI tools).
- **`TypedPool`**: N connections, dead ones replaced by a maintenance task that ticks every `maintenance_interval` (default 10s). Use it for application servers where many tasks contend.

Both compose with `RetryPolicy` for business-level transient errors (serialization failures, deadlocks).

## Testing strategy

- **Unit tests per crate**: pg-wired has extensive protocol-level tests using fake TCP streams; pg-pool has a deterministic `MockPoolable` for lifecycle tests.
- **Integration tests in resolute**: spin up a real PostgreSQL via `docker compose` (port 54322), exercise the full stack.
- **Proptest** (resolute): round-trip encode / decode for every type, fuzz the named-param rewriter and text array parser.
- **Trybuild** (resolute): compile-fail tests for derive attribute combinations that should be rejected.
- **`#[resolute::test]`**: attribute macro that creates a fresh database per test. Tests run in parallel without stepping on each other.

## Extending the stack

If you want to add a new PostgreSQL type:

1. Implement `Encode` and `Decode` in resolute for the binary wire format.
2. Add the OID to `TypeOid` in [`resolute/src/oid.rs`](../resolute/src/oid.rs).
3. Add the `PgType` const-impl (OID + ARRAY_OID) in [`resolute/src/pg_type.rs`](../resolute/src/pg_type.rs).
4. If it is user-definable (like a custom composite), add a derive path in resolute-derive.
5. Map the OID to the Rust type in [`resolute-macros/src/lib.rs`](../resolute-macros/src/lib.rs) so `query!` can pick the right Rust type from a describe.

If you want to add a new `Executor` implementor:

1. Implement `Executor` for your type in a downstream crate (the trait is `pub`).
2. `atomic()` must dispatch either `BEGIN/COMMIT` (top-level) or `SAVEPOINT/RELEASE` (nested) depending on the invariant your type represents.
3. Respect `&self`: concurrent calls should be safe. If your type wraps something with interior mutability, arrange for concurrent submissions to serialize through it (like `AsyncConn` does).
