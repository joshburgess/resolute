# resolute-cli

Command-line companion for the `resolute` query macros and a
lightweight migration runner. Ships one binary: `resolute-cli`.

## What it does

- **`prepare`**: scans the crate for `query!` / `query_as!` / etc
  invocations, connects to the live database, and caches the column
  and parameter metadata in `.sqlx/` so subsequent builds don't need
  a database. Check this directory into source control if you want
  offline / reproducible builds.
- **`check`**: re-runs `prepare` in verify mode, flagging any queries
  whose schema has drifted from the cache.
- **`migrate`**: `create`, `run`, `revert`, `status`, `info`,
  `validate`, `seed`. Plain SQL files up / down, tracking table
  named `_resolute_migrations`.
- **`database`**: `create` / `drop` lifecycle helpers for dev and CI
  test scaffolding.

## Install

```
cargo install --path resolute-cli
# or from the workspace:
cargo run -p resolute-cli -- --help
```

## Usage sketch

```
export DATABASE_URL=postgres://user:pass@localhost:5432/mydb

resolute-cli prepare               # populate .sqlx/
resolute-cli migrate create add_users
resolute-cli migrate run
resolute-cli migrate status
```

## License

Dual licensed under [Apache 2.0](../LICENSE-APACHE) or [MIT](../LICENSE-MIT). See the [workspace root](../README.md) for the broader project.
