# Changelog

All notable changes to the Resolute workspace will be documented here. This
project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
once it reaches 1.0; prior to that, breaking changes may land in minor
releases.

## [Unreleased]

## [pg-wired 0.3.0, pg-pool 0.3.0, resolute-macros 0.2.0, resolute-cli 0.2.0, resolute 0.4.0] - 2026-05-06

### Fixed

- `SharedPool` no longer races on the per-connection statement cache.
  Under concurrent submissions to the same multiplexed `AsyncConn`, two
  callers could observe a freshly-allocated cache entry and submit a
  Bind-only request before the allocator's Parse-bearing request had
  reached the writer FIFO, causing the server to reject the Bind with
  `26000: prepared statement "sN" does not exist`. The fix queues
  `Parse + Sync` into the writer channel under the cache lock and only
  publishes the cache entry once that send has been accepted, so any
  caller who later sees the name in the cache is guaranteed that Parse
  is ahead of their Bind in FIFO order. If the writer channel is full,
  the entry is not published; the caller gets a unique name and emits
  Parse atomically with Bind/Execute, so no other caller can race in.
  Regression test: `resolute/tests/shared_pool_stress_test.rs`
  (proptest-driven concurrent stress harness with replay-based shrinking
  for timing-dependent failures).

### Changed (breaking)

- `pg_wired::AsyncConn::lookup_or_alloc` now takes a `param_oids: &[u32]`
  argument so it can pre-queue `Parse` with the correct parameter type
  hints. Callers must compute `param_oids` before calling. All callers
  inside `resolute` and `pg-wired-js` have been updated. This is the
  reason the dependent crates (`pg-pool`, `resolute-macros`,
  `resolute-cli`, `resolute`) bump their major version: their public
  APIs are unchanged, but they transitively pin a major bump of
  `pg-wired`.

## [0.3.0] - 2026-05-06

### Changed (breaking)

- Renamed `TypedPool` to `ExclusivePool`, `PooledTypedClient` to
  `PooledClient`, and `SharedTypedClient` to `SharedClient`. The old
  names were misleading: the type name has nothing to do with whether
  queries are typed (every pool in this crate gives back typed rows). It
  describes pool semantics. `ExclusivePool` checks a connection out for
  one caller at a time. `SharedPool` (previously `SharedTypedPool`)
  multiplexes many concurrent callers onto a small set of connections
  driven by `pg-wired`'s writer/reader split. `TypedError` is unchanged.

  Migration: `s/TypedPool/ExclusivePool/g`,
  `s/PooledTypedClient/PooledClient/g`,
  `s/SharedTypedClient/SharedClient/g`,
  `s/SharedTypedPool/SharedPool/g`. The behavior of
  `ExclusivePool` is identical to the previous `TypedPool`.

### Added

- `SharedPool::exec_transaction(setup_sql, query_sql, params, param_oids)`:
  packages a setup statement (e.g. `BEGIN; SET LOCAL ROLE …`) and a
  query into a single pipelined batch over one of the pool's
  multiplexed connections, returning the query's `Vec<RawRow>`.
- `SharedPool::exec_query(sql, params, param_oids)`: single-statement
  variant on the same pipelined path.
- `RawRow` is now re-exported at the crate root (`resolute::RawRow`)
  for callers that consume `exec_transaction` / `exec_query` results.

These two methods make `SharedPool` viable as the hot path for
request-per-transaction servers (REST, GraphQL, RPC) where every
request opens its own short transaction. Such workloads previously had
to use `ExclusivePool`, which capped throughput at
`pool_size / per-request-transaction-time`. `SharedPool` removes that
ceiling: four connections sustain 20-24K rps in our REST benchmarks
(see `postgrest-rust`'s PERFORMANCE.md), versus ~4K rps on
`ExclusivePool` at the same pool size.

## [0.1.0] - 2026-04-25

First cut of the Resolute stack across six crates. The workspace aims to cover
the full PostgreSQL client story for Rust without depending on
`tokio-postgres`, `sqlx`, `deadpool`, or `bb8`.

### Added

#### `pg-wired`

Ground-up async PostgreSQL wire protocol v3 implementation.

- Reader and writer task pair with message coalescing and FIFO response
  matching.
- Extended query protocol: Parse, Bind, Describe, Execute, Sync, Close, with
  per-connection pseudo-LRU statement cache (256 entries).
- SCRAM-SHA-256 (and `-PLUS` channel binding under TLS) with MD5 fallback.
- Rustls-backed TLS under the `tls` feature, including optional mTLS.
- LISTEN / NOTIFY, COPY IN / OUT, query cancellation via `CancelToken`.
- `AsyncConn` high-level wrapper and `PipelineResponse` collector for
  multi-query pipelines.

#### `pg-pool`

Generic async connection pool keyed on a `Poolable` trait, with a
first-class `pg-wired` integration under the `wire` feature.

- Checkout / checkin with min and max connection tuning.
- Six lifecycle hooks (`on_create`, `before_acquire`, `on_checkout`,
  `on_checkin`, `after_release`, `on_destroy`) for per-connection
  `SET application_name`, `RESET ALL`, and similar.
- Idle-connection reaper and optional health-probe task.
- Prometheus-shaped `Metrics` snapshot (active, idle, waiters, creates,
  destroys, timeouts) and a graceful `drain()` protocol.

#### `resolute`

The typed query surface layered on `pg-wired` and `pg-pool`.

- Compile-time query validation via `query!`, `query_as!`, `query_scalar!`,
  `query_file!`, `query_file_as!`, `query_file_scalar!`, and a
  `query_unchecked!` escape hatch.
- Column type overrides (`"col: Type"`) and named parameter rewriting that
  handles `::` casts, quoted identifiers, string literals, comments, and
  dollar quoting.
- `Executor` trait with `&self` receiver so `Client`, `Transaction`,
  `PooledClient`, and `PooledTransaction` all compose through the same
  generic helpers.
- Context-aware `atomic()` that expands to `BEGIN/COMMIT` on a `Client` and
  to `SAVEPOINT/RELEASE` inside a `Transaction`.
- `PgListener` with auto-reconnect, `ExclusivePool` with lifecycle hooks,
  streaming row iterators, query pipelining, and COPY helpers.
- Derives: `FromRow` (with `rename`, `skip`, `default`, `json`, `try_from`,
  `flatten`), `PgEnum` (string- and integer-backed), `PgComposite`,
  `PgDomain` (with array OID inheritance).
- Custom types: `PgDate`, `PgInet`, `PgNumeric`, `PgTimestamp`, `PgRange`,
  plus binary-format encode / decode for every built-in PostgreSQL type
  covered by the driver.
- Migration runner, admin helpers, retry policies, transparent reconnect,
  and a `test-utils` feature that exposes the ephemeral `TestDb`.

#### `resolute-derive`

Proc macros backing the derives and the `#[resolute::test]` attribute.

#### `resolute-macros`

Proc macros backing the `query!` family. Queries are validated against a
live database on first build and cached under `.resolute/` keyed by SQL
hash. Honors `RESOLUTE_OFFLINE=1` for CI and air-gapped builds.

#### `resolute-cli`

CLI companion: `prepare`, `check`, `migrate` (create / run / revert /
status / info / validate / seed), and `database` (create / drop).

### Notes

- MSRV: Rust 1.85.
- License: MIT or Apache-2.0 (dual).
- Published crates: `pg-wired`, `pg-pool`, `resolute`, `resolute-derive`,
  `resolute-macros`, `resolute-cli`. The `pg-wired-js` crate in the
  workspace is a napi-rs crate for the npm ecosystem and is not published
  to crates.io.

[Unreleased]: https://github.com/joshburgess/resolute/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/joshburgess/resolute/releases/tag/v0.3.0
[0.1.0]: https://github.com/joshburgess/resolute/releases/tag/v0.1.0
