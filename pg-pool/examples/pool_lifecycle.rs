//! End-to-end tour of the pg-pool API: build a config, register lifecycle
//! hooks, warm the pool up, check connections in and out concurrently, run a
//! query against a checked-out connection, and drain on shutdown.
//!
//! Run against the workspace's docker-compose Postgres:
//!
//! ```bash
//! docker compose up -d
//! cargo run -p pg-pool --example pool_lifecycle
//! ```
//!
//! Override the connection target with the standard test env vars:
//!
//! ```bash
//! RESOLUTE_TEST_ADDR=127.0.0.1:5432 \
//! RESOLUTE_TEST_USER=alice \
//! RESOLUTE_TEST_PASSWORD=secret \
//! RESOLUTE_TEST_DB=mydb \
//! cargo run -p pg-pool --example pool_lifecycle
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use pg_pool::wire::WirePoolable;
use pg_pool::{ConnPool, ConnPoolConfig, LifecycleHooks};
use pg_wired::PgPipeline;

fn env_or<'a>(var: &str, default: &'a str) -> std::borrow::Cow<'a, str> {
    match std::env::var(var) {
        Ok(v) => std::borrow::Cow::Owned(v),
        Err(_) => std::borrow::Cow::Borrowed(default),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = env_or("RESOLUTE_TEST_ADDR", "127.0.0.1:54322");
    let user = env_or("RESOLUTE_TEST_USER", "postgres");
    let pass = env_or("RESOLUTE_TEST_PASSWORD", "postgres");
    let db = env_or("RESOLUTE_TEST_DB", "postgrest_test");

    // 1. Build the config. ConnPoolConfig is `#[non_exhaustive]`, so start
    // from Default and assign the fields you care about. Anything you don't
    // touch keeps its default value.
    let mut config = ConnPoolConfig::default();
    config.addr = addr.into_owned();
    config.user = user.into_owned();
    config.password = pass.into_owned();
    config.database = db.into_owned();
    config.min_idle = 1;
    config.max_size = 4;
    config.checkout_timeout = Duration::from_secs(5);

    // 2. Register lifecycle hooks. Each hook is optional. Here we count
    // creates, checkouts, and check-ins so the example can demonstrate that
    // returns are happening as expected.
    let creates = Arc::new(AtomicU64::new(0));
    let checkouts = Arc::new(AtomicU64::new(0));
    let checkins = Arc::new(AtomicU64::new(0));

    let hooks = {
        let c = Arc::clone(&creates);
        let co = Arc::clone(&checkouts);
        let ci = Arc::clone(&checkins);
        let mut hooks = LifecycleHooks::<WirePoolable>::default();
        hooks.on_create = Some(Box::new(move |_conn| {
            c.fetch_add(1, Ordering::Relaxed);
        }));
        hooks.on_checkout = Some(Box::new(move |_conn| {
            co.fetch_add(1, Ordering::Relaxed);
        }));
        hooks.on_checkin = Some(Box::new(move |_conn| {
            ci.fetch_add(1, Ordering::Relaxed);
        }));
        hooks
    };

    // 3. Build the pool. ConnPool::new spins up the maintenance task.
    let pool = ConnPool::<WirePoolable>::new(config, hooks).await?;

    // 4. Warm up to two idle connections so the first checkouts don't pay for
    // a TCP round trip + auth handshake.
    pool.warm_up(2).await;
    println!("after warm_up: {}", pool.status());

    // 5. Run a real query against one checked-out connection. PoolGuard::take
    // consumes the guard and gives back the underlying WirePoolable. We unwrap
    // its pg_wired::WireConn and wrap it in a PgPipeline for queries. Note
    // that take() removes the connection from the pool: it will not be
    // returned on drop. That is the right move when you want to consume the
    // connection (e.g., long-running listener) and the wrong move for normal
    // request/response work, where you should hold onto the guard and let
    // Drop return the connection.
    {
        let guard = pool.get().await?;
        let wire = guard.take().0;
        let mut pipe = PgPipeline::new(wire);
        let rows = pipe.query("SELECT 42::int4 AS n", &[], &[]).await?;
        let val = std::str::from_utf8(rows[0].cell(0).unwrap())?;
        println!("pipelined query returned: {val}");
        // pipe (and the underlying WireConn) goes out of scope here. The
        // pool's total count is now one less than before this block.
    }

    // 6. Run a few concurrent checkouts that DO release back to the pool.
    // PoolGuard returns the connection to the pool on drop; no manual release
    // is needed.
    let handles: Vec<_> = (0..6)
        .map(|i| {
            let pool = Arc::clone(&pool);
            tokio::spawn(async move {
                let guard = pool.get().await.expect("checkout failed");
                // Just hold the connection for a moment to simulate work.
                tokio::time::sleep(Duration::from_millis(10)).await;
                println!("worker {i}: held connection then released");
                drop(guard);
            })
        })
        .collect();

    for h in handles {
        h.await?;
    }

    // 7. Inspect metrics. PoolMetrics is a snapshot you can ship to Prometheus
    // (or any other sink). Counters are monotonic.
    let m = pool.metrics();
    println!(
        "metrics: total = {}, idle = {}, in_use = {}, checkouts = {}, created = {}",
        m.total, m.idle, m.in_use, m.total_checkouts, m.total_created,
    );
    println!(
        "hook counters: creates = {}, checkouts = {}, checkins = {}",
        creates.load(Ordering::Relaxed),
        checkouts.load(Ordering::Relaxed),
        checkins.load(Ordering::Relaxed),
    );

    // 8. Drain. Closes idle connections, refuses new checkouts, and waits for
    // outstanding guards to be returned. Always do this on shutdown so
    // PostgreSQL sees a clean Terminate rather than a half-open socket.
    pool.drain().await;
    println!("drained: {}", pool.status());

    Ok(())
}
