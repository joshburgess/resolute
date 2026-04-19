# resolute-derive

Proc-macro crate backing the `#[derive(...)]` and attribute macros
exposed at the [`resolute`](../resolute) crate root. You don't need
to depend on this directly: re-exports live in `resolute` itself.

## What it provides

- **`#[derive(FromRow)]`**: map a row (from `query`, `query!`, a
  stream, or COPY) into a struct. Field attributes: `rename`,
  `skip`, `default`, `json` (serde round-trip), `try_from = "T"`,
  `flatten` (compose nested structs).
- **`#[derive(PgEnum)]`**: PostgreSQL enum types. Works in
  `#[repr(i32)]` mode too, which is handy for legacy schemas where
  the status column is a bare integer.
- **`#[derive(PgComposite)]`**: custom composite types, including
  their array OIDs.
- **`#[derive(PgDomain)]`**: newtypes over a base SQL type, with
  automatic array-OID inheritance.
- **`#[resolute::test]`**: test attribute that spins up a temporary
  database, runs migrations, hands the test a `Client`, and cleans
  up on completion.

## Usage

```rust
use resolute::FromRow;

#[derive(FromRow)]
struct Author {
    id: i32,
    name: String,
    #[from_row(default)]
    bio: Option<String>,
}
```

## License

Dual licensed under [Apache 2.0](../LICENSE-APACHE) or [MIT](../LICENSE-MIT). See the [workspace root](../README.md) for the broader project.
