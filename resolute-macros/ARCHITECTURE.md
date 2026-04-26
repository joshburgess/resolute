# resolute-macros architecture

resolute-macros is the compile-time query validation engine behind `query!`, `query_as!`, `query_scalar!`, and the file-based variants. It parses SQL at compile time, rewrites named parameters, talks to a live PostgreSQL instance to describe the query (or reads a cached describe from `.resolute/`), and emits typed Rust code.

This document covers the internals. See the [`README`](README.md) for the usage surface.

## Module layout

```
src/
  lib.rs      all 7 proc-macros, the rewrite and describe pipeline, code generation
  cache.rs    CacheEntry / CachedColumn serde structs, cache_dir discovery, read/write
```

The entire proc-macro crate is about 1000 lines in two files. The design priority is "predictable and fast on the compile-time path": no async runtime beyond what's needed for one describe call, no heap ceremony in the named-param rewriter.

## The macro pipeline

For `query!` (the most general case), `query_impl` ([`lib.rs:341`](src/lib.rs)) runs the following steps at compile time:

```
1. parse input       QueryInput::parse     (sql literal + params)
2. rewrite :name     resolve_named          -> (rewritten_sql, ordered_params)
3. hash sql          hash_sql               (FNV-1a)
4. cache lookup      cache::read_cache(hash)
5. live describe     describe_live          if cache miss AND !offline
6. cache write       cache::write_cache     after a successful live describe
7. generate output   quote! struct + CheckedQuery
```

Each step is a standalone function, so adding a variant (like `query_file_as!`) means wiring the pieces together differently rather than forking the pipeline.

## Input parsing

`QueryInput::parse` ([`lib.rs:35`](src/lib.rs)) accepts:

```
query!("SELECT ...", arg1, arg2)                   // positional
query!("SELECT ...", name1 = expr1, name2 = expr2) // named
```

Positional and named forms cannot mix. The parser uses `syn::parse` to validate the SQL literal and the param list. If any param after the first is named but the SQL uses no `:name` syntax, or vice versa, it emits a clear error at the callsite.

For `query_as!` and `query_scalar!` the input shape varies slightly (leading target type), but the SQL + params structure is the same.

## Named parameter rewriter

`rewrite_named_params` ([`lib.rs:882`](src/lib.rs)) is a hand-written character-by-character scanner. The same logic is duplicated verbatim in the runtime path ([`resolute/src/named_params.rs`](../resolute/src/named_params.rs)) so runtime `query_named` and compile-time `query!(:name, ...)` behave identically.

The scanner handles false-positive avoidance in priority order:

| Construct | Handling |
|---|---|
| `--` line comment | Consumed verbatim until end of line |
| `/* ... */` block comment | Consumed verbatim until closing `*/` |
| `'...'` string literal | Consumed verbatim, including `''` escapes |
| `"..."` quoted identifier | Consumed verbatim |
| `$tag$...$tag$` dollar-quoted string | Parse opening tag, scan char-by-char for exact matching closing tag |
| `::` cast operator | Emit both colons, advance by 2 |
| `:<ident>` named param | Rewrite to `$N` and record the ordinal |

Dollar quoting is the only genuinely hard case. A PostgreSQL string like `$func$SELECT :name$func$` must not be rewritten: the `:name` is inside a string. The scanner precomputes the tag character slice at the opening `$tag$`, then compares `chars[i..i+tag_len]` against that slice on each `$` it encounters inside. This avoids allocating a `String` per `$`, which matters because macro-time cost shows up on every recompile.

Named params are numbered by first occurrence. Duplicates (`:id` appearing twice) reuse the same `$N`, which in turn maps to a single binding in the generated `params` vector.

## SQL hashing

`hash_sql` ([`lib.rs:867`](src/lib.rs)) computes FNV-1a over the rewritten SQL bytes. The hash is stable and fast, 64 bits, cross-platform identical. It is used as both the cache filename (`.resolute/query-{hash:016x}.json`) and the generated struct name (`__QueryResult_{hash}`).

FNV-1a was chosen over SipHash / AHash for stability: the cache files are portable across OSes and Rust versions. The cryptographic weakness of FNV is irrelevant because the input is trusted source SQL, not adversarial input.

## Cache layout

```
.resolute/
  query-0123456789abcdef.json
  query-fedcba9876543210.json
  ...
```

One file per unique (post-rewrite) SQL. The filename hash matches the compile-time hash.

```json
// CacheEntry schema (cache.rs:21)
{
  "sql": "SELECT id, name FROM authors WHERE id = $1",
  "hash": 81985529216486895,
  "param_oids": [23],
  "columns": [
    { "name": "id",   "type_oid": 23,   "nullable": false },
    { "name": "name", "type_oid": 1043, "nullable": true  }
  ]
}
```

`nullable` uses `#[serde(default)]` for forward compatibility with old cache files that predate nullability tracking. Stale caches (missing `nullable`) default to `true`, which is the conservative and correct fallback.

### Cache directory discovery

`cache_dir` ([`cache.rs:34`](src/cache.rs)) walks up from `$CARGO_MANIFEST_DIR` looking for:

1. An existing `.resolute/` directory. Found on the first hit.
2. A `Cargo.toml` containing `[workspace]`. If found, use `$WORKSPACE_ROOT/.resolute/`.

This finds the workspace-level cache in a workspace crate without the user having to configure anything. If neither is found, the cache falls back to `$CARGO_MANIFEST_DIR/.resolute/`.

### When entries are written

A cache entry is **written** only after a successful live describe, when the prior read missed ([`lib.rs:130`](src/lib.rs)). Write failures are non-fatal (a warning is printed) because they can happen legitimately in CI sandboxes. Read-side is always consulted; offline-only builds never write.

## Live describe

`describe_live` ([`lib.rs:145`](src/lib.rs)) is the online path. It spins up a current-thread Tokio runtime (`tokio::runtime::Builder::new_current_thread()`), connects to `$DATABASE_URL`, and performs:

1. `WireConn::connect(database_url)`: full startup + auth + TLS handshake via pg-wired.
2. `conn.describe_statement(sql)`: sends `Parse + Describe(S) + Sync`, receives:
   - `ParameterDescription` with the param OIDs.
   - `RowDescription` with each column's name, type OID, table OID, and column attnum.
3. A nullability side-query (detailed below).
4. Drop the connection.

The runtime is scoped to this one describe call. Each `query!` invocation gets its own runtime and its own connection, which sounds expensive but is not: builds cache the result, so this path only runs when SQL changes or the cache is fresh.

### Nullability inference

By default, every column starts nullable: `nullable = true` at [`lib.rs:178`](src/lib.rs) with the comment "Default: assume nullable".

After `describe_statement` returns `RowDescription`, resolute-macros formats one simple-query against `pg_attribute` with the `(attrelid, attnum)` pairs inlined, then sends it as a single `Query` message:

```sql
SELECT attrelid, attnum, attnotnull
FROM pg_attribute
WHERE (attrelid=12345 AND attnum=1)
   OR (attrelid=12345 AND attnum=2)
   ...
```

It batches all `(table_oid, column_id)` pairs from the `RowDescription` where `table_oid != 0 && column_id > 0`. Columns with `table_oid = 0` are computed projections (literals, expressions, function calls, CASE, coalesce with a non-table input, etc.): they fail the filter and stay `nullable = true`. The OIDs come from the trusted `RowDescription` payload, not from user input, so direct interpolation is safe here; the runtime path that handles user-supplied parameters is parameterised separately.

For pairs that do get queried, `attnotnull = true` flips the column to `nullable = false`. This is the correct conservative inference: the server cannot guarantee non-nullness for arbitrary expressions, only for literal column references.

Practical consequence: `query!("SELECT 1 AS n")` always returns `Option<i32>`. To project a literal as non-null, cast it and let `query_as!` with a target struct override the type.

## `RESOLUTE_OFFLINE` gating

The `RESOLUTE_OFFLINE` environment variable is checked inside `resolve_metadata` ([`lib.rs:109`](src/lib.rs)). If the cache read returns `None` and `RESOLUTE_OFFLINE` is `"true"` or `"1"`, the macro emits a compile error instead of attempting a live describe.

`DATABASE_URL` is only read inside `describe_live` (so offline builds never require it). The two env vars together form the CI contract:

- Dev machine: set `DATABASE_URL`, unset `RESOLUTE_OFFLINE`. Live describes populate `.resolute/`.
- Committed: `.resolute/` contents.
- CI / Docker build: `RESOLUTE_OFFLINE=true` set, `DATABASE_URL` unset. Builds from cache only.
- Drift check: run `resolute-cli check` in CI to verify `.resolute/` matches the current live schema.

## Generated output

For `query!`, the macro expands to a block expression:

```rust
{
    // 1. per-param type assertions (compile errors if mismatched)
    let _check_param_0 = resolute::internal::__resolute_check_param::<i32>(arg1);

    // 2. an anonymous result struct named by hash
    #[derive(Debug)]
    pub struct __QueryResult_81985529216486895 {
        pub id: i32,
        pub name: Option<String>,
    }

    // 3. the CheckedQuery<T> value
    resolute::CheckedQuery::<__QueryResult_81985529216486895> {
        sql: "SELECT id, name FROM authors WHERE id = $1",
        params: vec![&arg1 as &dyn SqlParam],
        mapper: |row| Ok(__QueryResult_81985529216486895 {
            id:   row.get(0)?,
            name: row.get_opt(1)?,
        }),
    }
}
```

Three pieces worth pointing out:

- **Per-param type assertions** are compile-time only, generated for each param with its OID-inferred Rust type. A `query!("... WHERE id = :id", id = "not an int")` produces a type error before any runtime error occurs.
- **The struct is locally scoped**. Each `query!` callsite produces its own struct. If two sites use the same SQL they get the same hash and therefore the same struct name, but because each struct is defined inside its own block expression they do not collide.
- **The mapper is a function pointer, not a closure over the row**. `CheckedQuery<T>::fetch_all` takes the mapper and calls it per row.

`query_as!` differs in one line: it skips the struct generation and uses `<TargetType as FromRow>::from_row(row)` as the mapper. `query_scalar!` enforces a single-column result and uses `row.get(0)?` directly, with no struct at all.

## query_file!, query_file_as!, query_file_scalar!

These read the SQL from a file path relative to `$CARGO_MANIFEST_DIR` at macro-expansion time. Otherwise the pipeline is identical. The named-param rewriter, hash, cache, describe, and generation steps all run unchanged. File inclusion is done via `std::fs::read_to_string` inside the proc macro.

## query_unchecked!

Escape hatch for "I need macro-built SQL with named-param rewriting but no type check." Skips describe and cache entirely. Produces an `UncheckedQuery` (not `CheckedQuery<T>`), which accepts generic `FromRow` targets at fetch time. Useful for dynamic SQL assembled at runtime that still wants `:name` rewriting, and for test fixtures against an unavailable DB.

## Why FNV

FNV-1a is chosen for its stability across Rust versions, operating systems, and CPU architectures: a committed cache file must reproduce the same filename on any developer's machine, CI runner, or Docker image. The `CacheEntry` schema is resolute-specific and tracks per-column nullability alongside OIDs.
