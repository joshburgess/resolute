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

## Testing

`resolute-derive` has no dedicated test target. Instead:

- **Successful expansion** is exercised by every derive in the `resolute`
  crate's tests (~378 tests in `resolute/tests/`), which import the macros
  and rely on the generated code at runtime.
- **Compile-time failure** is covered by trybuild fixtures in
  `resolute/tests/compile_fail_derive/`, run by
  `derive_compile_fail_tests` in `resolute/tests/compile_fail_derive_test.rs`.
  Each `.rs` fixture has a paired `.stderr` snapshot that the harness
  diffs against the actual compiler output.

This setup catches both "the macro produced wrong code" (via downstream
runtime tests) and "the macro accepted invalid input" (via trybuild)
without requiring a separate `tests/` directory inside this crate.

## License

Dual licensed under [Apache 2.0](https://github.com/joshburgess/resolute/blob/main/LICENSE-APACHE) or [MIT](https://github.com/joshburgess/resolute/blob/main/LICENSE-MIT). See the [workspace root](https://github.com/joshburgess/resolute#readme) for the broader project.
