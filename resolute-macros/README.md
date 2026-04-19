# resolute-macros

Proc-macro crate that powers the compile-time checked query macros
re-exported from [`resolute`](../resolute). You usually won't depend
on this crate directly, only through `resolute`.

## What it provides

- **`query!`**: compile-time validated query returning a generated
  row struct. Column types and nullability come from the database.
- **`query_as!`**: same, but mapped into a struct that derives
  `FromRow`.
- **`query_scalar!`**: single-column, single-row convenience.
- **`query_file!` / `query_file_as!` / `query_file_scalar!`**: read
  the SQL from a file (resolved relative to `CARGO_MANIFEST_DIR`).
- **`query_unchecked!`**: escape hatch that skips the live-DB check
  when you need a macro-built query without a schema round-trip.

Named parameters (`:name`) are rewritten to `$N` positional params
at compile time, including in the file-based variants. The rewriter
is aware of `::` casts, string literals, comments, and dollar
quoting, so you won't get false positives from embedded tokens.

## How the compile-time check works

1. Hash the SQL string.
2. Look up the hash in `.sqlx/` on disk. If present, use the cached
   metadata and skip the network round-trip.
3. Otherwise, connect to `DATABASE_URL`, send Parse + Describe,
   record the param OIDs and the column (name, OID, nullability)
   tuple, and write a cache file for future offline builds.

`nullable` defaults to `true` and is only flipped to `false` when
the column is pulled from a real table (`pg_attribute.attnotnull`).
Computed columns stay nullable, which means `query!` returns
`Option<T>` for literal projections.

## Environment

| var | meaning |
|---|---|
| `DATABASE_URL` | Required for live describes. `prepare`/`check` via the [`resolute-cli`](../resolute-cli) tool use the same variable. |
| `RESOLUTE_OFFLINE` | When `true`/`1`, never dial out. Fail the build if a query hash isn't in the cache. |

## License

Dual licensed under [Apache 2.0](../LICENSE-APACHE) or [MIT](../LICENSE-MIT). See the [workspace root](../README.md) for the broader project.
