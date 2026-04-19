# pg-pool

Generic async connection pool for any connection type that implements
the `Poolable` trait. Ships with a first-class
[`pg-wired`](../pg-wired) integration behind the default `wire`
feature, but the core pool is protocol-agnostic.

## What's in the box

- Checkout / checkin with configurable min and max connections.
- Six lifecycle hooks covering create, acquire, checkout, checkin,
  release, and destroy. Connection-aware where applicable, so you can
  e.g. `RESET ALL` at checkin or `SET application_name` at checkout.
- Idle timeout with a background reaper.
- Optional health-probe task that pings idle connections and evicts
  ones the server has torn down.
- `Metrics` snapshot (active, idle, waiters, creates, destroys, timeouts)
  for Prometheus scraping or ad-hoc logging.
- Graceful drain: stop accepting new waiters and return once all
  in-flight checkouts return.

## Minimal example

```rust
use pg_pool::{Pool, PoolConfig};
use pg_wired::WireConn;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pool: Pool<WireConn> = Pool::builder(PoolConfig::default())
        .with_constructor(|| async {
            WireConn::connect("127.0.0.1:5432", "user", "pass", "mydb").await
        })
        .build()
        .await?;

    let conn = pool.acquire().await?;
    // use `conn` like any `WireConn` ...
    drop(conn); // returns to the pool
    Ok(())
}
```

## Features

| feature | default | enables |
|---|---|---|
| `wire` | yes | `Poolable` impl for `pg_wired::WireConn` plus helper constructors. |

## License

Dual licensed under [Apache 2.0](../LICENSE-APACHE) or [MIT](../LICENSE-MIT). See the [workspace root](../README.md) for the broader project.
