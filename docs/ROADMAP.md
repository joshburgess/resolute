# Resolute Roadmap

## Ship it

### 1. Publish to crates.io
Publish all 6 crates: pg-wired, pg-pool, resolute, resolute-derive, resolute-macros, resolute-cli. The crates are ready. This is the single highest-leverage thing.

### 2. CI for the resolute repo
GitHub Actions pipeline: fmt, clippy, unit tests, integration tests (docker postgres). No CI exists on the repo yet.

### 3. LICENSE file
README says MIT but no LICENSE file exists.

---

## Close remaining gaps with sqlx

### 4. `#[sqlx(transparent)]`-style in query macros (DONE)
Documented as intentional design decision. Resolute's integer enum requires explicit discriminants. Implicit discriminants are fragile (reordering variants silently changes database values). Documented in `resolute/README.md` under "Design decisions" and in the integer-backed enums section.

### 5. Compile-fail tests (DONE, 13 tests)
- FromRow: skip+rename, skip+default, flatten+json, flatten+try_from, json+try_from, on enum, on tuple struct
- PgDomain: on named struct, multiple fields, on enum
- PgEnum: on struct, with fields, integer enum without discriminants

### 6. `Any` database abstraction (DONE, documented as non-goal)
Documented in `resolute/README.md` under "Design decisions": resolute is PostgreSQL-only by design. Single-database focus enables full leverage of PostgreSQL features without lowest-common-denominator abstractions.

---

## Deepen existing features

### 7. Transaction isolation levels (DONE)
`IsolationLevel` enum with `ReadCommitted`, `RepeatableRead`, `Serializable`. Usage: `client.begin_with(IsolationLevel::Serializable)`.

### 8. Advisory locks (DONE)
Session-level: `advisory_lock(key)`, `try_advisory_lock(key)`, `advisory_unlock(key)`. Transaction-level: `advisory_xact_lock(key)`, `try_advisory_xact_lock(key)`.

### 9. Connection string format (DONE)
`Client::connect_from_str()` accepts both `postgres://` URIs and `key=value` libpq format. Public `parse_connection_string()` function.

### 10. Range types (DONE)
`PgRange<T>` generic type with binary and text encode/decode. PgType impls for `int4range`, `int8range`, `numrange`, `daterange`, `tsrange`, `tstzrange`. OID mappings in query macros.

### 11. ENUM OID resolution (DONE)
`Client::lookup_type_oid(name)` and `Client::lookup_type_oids(name)` query `pg_type` at runtime to discover custom type OIDs and their array OIDs. Users can use these for diagnostics or type registration.

---

## Developer experience

### 12. `cargo doc` quality pass (DONE)
Audit found 95%+ doc coverage. All public items across all modules have doc comments. All trait methods documented with examples. All public structs and enums have field-level documentation.

### 13. `resolute-cli migrate` test harness (DONE)
Integration tests for CLI binary: `migrate create` (file generation), `migrate run/status/revert` (full lifecycle against real PostgreSQL), `database create/drop`, help text output validation.

### 14. Error messages (DONE)
Audit found SQL context properly included in all primary query/execute paths via `with_sql()`. Fixed remaining gap: COPY operations (`copy_in`, `copy_out`) in both `Client` and `PooledTypedClient` now include SQL in error context.
