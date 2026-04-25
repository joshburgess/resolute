# Contributing to Resolute

Thanks for your interest in contributing. This document covers what you need to get a working dev environment, the conventions the codebase follows, and what reviewers will look for in a PR.

## Quick start

Resolute is a Cargo workspace. Every change should pass formatting, lint, doc, and test gates locally before review.

```bash
# 1. Get a PostgreSQL test instance running on the canonical port (54322).
docker compose up -d

# 2. Build and check.
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -Dwarnings
RUSTDOCFLAGS=-Dwarnings cargo doc --workspace --no-deps
cargo test --workspace
```

Tests, examples, and benches default to `127.0.0.1:54322`, role `postgres`, password `postgres`, database `postgrest_test`. Override via `RESOLUTE_TEST_ADDR`, `RESOLUTE_TEST_USER`, `RESOLUTE_TEST_PASSWORD`, `RESOLUTE_TEST_DB` if you want to point at a different cluster.

The `pg-wired` TLS runtime test (`pg-wired/tests/tls_test.rs`) is skipped unless you point it at a TLS-enabled Postgres. CI does this automatically; for a local run, set `RESOLUTE_TLS_TEST_ADDR` to `host:port` and `RESOLUTE_TLS_TEST_CA_DER` to the path of the DER-encoded server CA cert, then run `cargo test -p pg-wired --features tls --test tls_test`.

## Workspace layout

| Crate | Purpose |
|-------|---------|
| `pg-wired` | PostgreSQL wire protocol v3 client. No SQL parsing, no type system. |
| `pg-pool` | Generic async connection pool. Works with any `Poolable` connection. |
| `resolute` | Compile-time checked queries, derives, typed `Client`/`Pool`/`Transaction`. |
| `resolute-derive` | Procedural macros: `FromRow`, `PgEnum`, `PgComposite`, `PgDomain`, `#[resolute::test]`. |
| `resolute-macros` | Compile-time query validation against a live database or offline cache. |
| `resolute-cli` | Offline cache management, migrations, database lifecycle commands. |
| `pg-wired-js` | napi-rs JavaScript bindings for `pg-wired`. |

Per-crate architecture docs live in each crate's `ARCHITECTURE.md`. The cross-cutting tour is in `docs/ARCHITECTURE.md`. Performance methodology and numbers are in `docs/PERFORMANCE.md`.

## Coding conventions

### Style and lints

- `cargo fmt --all` formats every file. CI rejects unformatted code.
- `cargo clippy --workspace --all-targets -- -Dwarnings` must pass. Suppress lints with `#[allow(...)]` only when the lint is wrong for the call site, and add a one-line comment explaining why.
- Publishing crates set `#![deny(missing_docs)]`. Every exposed item gets a one-line `///`. Keep it short: name plus one sentence of purpose.
- Avoid em dashes in prose (commit messages, PR descriptions, READMEs, doc comments). Use a period, comma, colon, or parentheses depending on the relationship.

### Public API stability

User-facing structs and enums are marked `#[non_exhaustive]` so we can add fields and variants without breaking downstream construction. When adding a new field, also add a `Default` impl path for it, and document the field with one line.

When changing an existing public signature, check whether downstream tests in this workspace and the published crates need a coordinated update. Mention the breakage in the PR.

### Errors

- `pg-wired` returns `PgWireError`. `resolute` returns `TypedError`. Both are `#[non_exhaustive]`.
- Don't add fallbacks or validation for impossible cases. Trust internal invariants. Validate at boundaries (env vars, user input, server messages).
- Use `tracing::warn!` / `tracing::error!` for non-fatal failures (e.g., advisory lock release that fails during shutdown). Do not eat the error silently.

### Tests

- Integration tests live in `<crate>/tests/*.rs` and require a running PostgreSQL instance. Unit tests live alongside the source files.
- Tests should be deterministic. Use `TestDb::create` (gated on `test-utils`) for tests that mutate schema; otherwise share the seeded `postgrest_test` database.
- Add a regression test for every bug fix. The test should fail without the fix and pass with it.

### Benchmarks

Criterion benches live in `<crate>/benches/`. Run with `cargo bench -p <crate>`. Benchmark numbers in PRs are advisory unless the change is performance-motivated, in which case include before/after results.

## Commit and PR conventions

- Write commits in the imperative voice. Title under 70 characters. Body explains the *why*, not the *what*.
- One logical change per commit. Squash WIP fixups before opening the PR.
- The PR title should be self-explanatory; the body should describe what changed and how reviewers can verify it. Link any related issue.
- Don't add AI/Claude/Anthropic attribution to commits, PRs, or issues.

## Release checklist

For maintainers cutting a release, see `RELEASING.md`.

## License

Resolute is dual-licensed under Apache 2.0 (`LICENSE-APACHE`) or MIT (`LICENSE-MIT`) at your option. By contributing you agree that your changes are licensed under the same terms.
