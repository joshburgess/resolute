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

```rust,no_run
use pg_pool::{ConnPool, ConnPoolConfig, LifecycleHooks};
use pg_pool::wire::WirePoolable;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = ConnPoolConfig::default();
    config.addr = "127.0.0.1:5432".into();
    config.user = "postgres".into();
    config.password = "postgres".into();
    config.database = "mydb".into();

    let pool = ConnPool::<WirePoolable>::new(config, LifecycleHooks::default()).await?;

    let guard = pool.get().await?;
    // use `guard.conn()` / `guard.conn_mut()` ...
    drop(guard); // returns to the pool
    Ok(())
}
```

## Features

| feature | default | enables |
|---|---|---|
| `wire` | yes | `Poolable` impl for `pg_wired::WireConn` plus helper constructors. |

## Architecture

See [`ARCHITECTURE.md`](https://github.com/joshburgess/resolute/blob/main/pg-pool/ARCHITECTURE.md) for the internals: the `Poolable` trait, data layout (atomics + mutexed deques), the acquire and release paths, the six lifecycle hooks, the drain protocol, and hook safety rules.

## License

Dual licensed under [Apache 2.0](https://github.com/joshburgess/resolute/blob/main/LICENSE-APACHE) or [MIT](https://github.com/joshburgess/resolute/blob/main/LICENSE-MIT). See the [workspace root](https://github.com/joshburgess/resolute#readme) for the broader project.
