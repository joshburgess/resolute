# Changelog

All notable changes to the Resolute workspace will be documented here. This
project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
once it reaches 1.0; prior to that, breaking changes may land in minor
releases.

## [Unreleased]

## [0.1.0] - Initial public release

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
  `PooledTypedClient`, and `PooledTransaction` all compose through the same
  generic helpers.
- Context-aware `atomic()` that expands to `BEGIN/COMMIT` on a `Client` and
  to `SAVEPOINT/RELEASE` inside a `Transaction`.
- `PgListener` with auto-reconnect, `TypedPool` with lifecycle hooks,
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

- MSRV: Rust 1.75.
- License: MIT or Apache-2.0 (dual).
- Published crates: `pg-wired`, `pg-pool`, `resolute`, `resolute-derive`,
  `resolute-macros`, `resolute-cli`. The `pg-wired-js` crate in the
  workspace is a napi-rs crate for the npm ecosystem and is not published
  to crates.io.

[Unreleased]: https://github.com/joshburgess/resolute/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/joshburgess/resolute/releases/tag/v0.1.0
