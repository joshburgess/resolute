# Stalwart Roadmap

## Ship it

### 1. Publish to crates.io
Publish all 6 crates: pg-wired, pg-pool, stalwart, stalwart-derive, stalwart-macros, stalwart-cli. The crates are ready — this is the single highest-leverage thing.

### 2. CI for the stalwart repo
GitHub Actions pipeline: fmt, clippy, unit tests, integration tests (docker postgres). No CI exists on the repo yet.

### 3. LICENSE file
README says MIT but no LICENSE file exists.

---

## Close remaining gaps with sqlx

### 4. `#[sqlx(transparent)]`-style in query macros
We have `PgDomain` and type overrides, but sqlx users can also do `#[sqlx(transparent)]` on enums wrapping integers without explicit discriminants (just `#[repr(i32)]` + transparent). Our integer enum requires explicit discriminants, which is stricter but arguably better. Consider whether to relax this or document the design choice.

### 5. Compile-fail tests
We have 2 compile-fail tests. sqlx has many more. Add tests for:
- Invalid FromRow attribute combos (skip + rename, flatten + json, json + try_from)
- PgDomain on non-tuple structs
- PgDomain on tuple structs with multiple fields
- PgEnum on enums with non-unit variants (fields)
- Integer enum without explicit discriminants
- Integer enum with unsupported repr (u32, u8)
- FromRow on enums (not structs)
- FromRow on tuple structs

### 6. `Any` database abstraction
Not needed — stalwart is PostgreSQL-only by design. This is a non-goal. Document it explicitly.

---

## Deepen what we already have

### 7. Transaction isolation levels
Support `client.begin_with("SERIALIZABLE")` or `BEGIN ISOLATION LEVEL READ COMMITTED`. The `begin()` method should accept an optional isolation level parameter or have a `begin_serializable()` / `begin_repeatable_read()` variant.

### 8. Advisory locks
Wrappers for `pg_advisory_lock`, `pg_try_advisory_lock`, `pg_advisory_unlock`, and their session/transaction-scoped variants. These are commonly used for distributed locking and should be first-class.

### 9. Connection string format
Support `key=value` libpq format in addition to URIs:
```
host=localhost port=5432 dbname=mydb user=postgres sslmode=require
```
Currently only `postgres://` URIs are supported. Many deployment environments use the key=value format (e.g., `PGCONNSTR`).

### 10. Range types
Support `int4range`, `int8range`, `numrange`, `tsrange`, `tstzrange`, `daterange`. These are increasingly common for time-series, scheduling, and constraint modeling. Neither sqlx nor stalwart handles them well. Implement as a `PgRange<T>` generic type with `Encode`/`Decode`.

### 11. ENUM OID resolution
`PgEnum` currently sets `OID=0` (Unspecified). If we queried `pg_type` for the enum's actual OID at connection time (or cached it in the offline build cache), arrays of custom enums would work natively without type override hacks. This would also improve error messages from PostgreSQL when type mismatches occur.

---

## Developer experience

### 12. `cargo doc` quality pass
Ensure all public items have doc comments with examples. Verify examples compile in rustdoc (`cargo doc --no-deps`). Add module-level documentation for key modules (encode, decode, executor, pooled, migrate).

### 13. `stalwart-cli migrate` test harness
Integration tests for the CLI binary itself: `prepare`, `check`, `migrate run/revert/status/info/validate/seed`, `database create/drop`. Run against a real PostgreSQL instance in CI.

### 14. Error messages
When a query fails, include the SQL in the error message. stalwart does this in some paths (e.g., `QueryFailed` variant) but not all. Audit all error paths and ensure the SQL context is preserved through the chain. This is one of the most impactful DX improvements — a stack trace with "decode error at column 3" is useless without the query.
