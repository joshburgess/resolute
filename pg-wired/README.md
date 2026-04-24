# pg-wired

PostgreSQL wire protocol v3 in async Rust, with no dependency on
`tokio-postgres`. The runtime layer for the [`resolute`](../resolute)
stack, but usable on its own when you want a bare wire client.

## What it covers

- Async connection with SCRAM-SHA-256 (and `-PLUS` channel binding when
  TLS is active) and MD5 fallback.
- Extended query protocol: Parse, Bind, Describe, Execute, Sync, Close.
  Format-agnostic at the type level: callers supply pre-encoded bytes
  with a per-column `FormatCode` of Text or Binary. Resolute drives this
  in binary.
- Per-connection statement cache (pseudo-LRU, 256 entries). Parse once,
  Bind + Execute on reuse.
- Pipelining with writer coalescing and FIFO response matching so
  concurrent tasks share one TCP connection efficiently.
- LISTEN/NOTIFY, COPY IN/OUT, query cancellation via `CancelToken`,
  negotiated TLS (rustls) under the `tls` feature.

## Minimal example

```rust
use pg_wired::WireConn;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut conn = WireConn::connect("127.0.0.1:5432", "user", "pass", "mydb").await?;
    let (_param_oids, rows, _tag) = conn
        .exec_query("SELECT $1::int4 + 1", &[Some(b"41".as_slice())], &[])
        .await?;
    println!("{:?}", rows);
    Ok(())
}
```

Most applications will want the typed query surface in
[`resolute`](../resolute) rather than raw byte slices.

## Features

| feature | default | enables |
|---|---|---|
| `tls` | no | rustls-backed TLS negotiation (`sslmode=prefer` / `require`) with optional mTLS. |

## Architecture

See [`ARCHITECTURE.md`](https://github.com/joshburgess/resolute/blob/main/pg-wired/ARCHITECTURE.md) for the internals: the reader/writer task split, message coalescing, FIFO response matching, the statement cache, TLS negotiation, and SCRAM.

## License

Dual licensed under [Apache 2.0](https://github.com/joshburgess/resolute/blob/main/LICENSE-APACHE) or [MIT](https://github.com/joshburgess/resolute/blob/main/LICENSE-MIT). See the [workspace root](https://github.com/joshburgess/resolute#readme) for the broader project.
