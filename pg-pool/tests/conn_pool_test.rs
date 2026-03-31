//! Thorough integration tests for ConnPool.
//! Requires: docker compose up -d (PostgreSQL on port 54322)

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use pg_pool::{ConnPool, ConnPoolConfig, LifecycleHooks, PoolGuard};
use pg_pool::wire::WirePoolable;
use pg_wire::{PgWireError, WireConn};

type Pool = ConnPool<WirePoolable>;

const ADDR: &str = "127.0.0.1:54322";
const USER: &str = "postgres";
const PASS: &str = "postgres";
const DB: &str = "postgrest_test";

fn test_config() -> ConnPoolConfig {
    ConnPoolConfig {
        addr: ADDR.to_string(),
        user: USER.to_string(),
        password: PASS.to_string(),
        database: DB.to_string(),
        min_idle: 1,
        max_size: 5,
        max_lifetime: Duration::from_secs(300),
        max_lifetime_jitter: Duration::from_secs(0), // no jitter for determinism
        checkout_timeout: Duration::from_secs(2),
        maintenance_interval: Duration::from_secs(3600), // 1h — effectively disabled for tests
        test_on_checkout: true,
    }
}

// ---------------------------------------------------------------------------
// Basic pool lifecycle
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_pool_create_with_min_idle() {
    let pool = Pool::new(test_config(), LifecycleHooks::default())
        .await
        .unwrap();

    let m = pool.metrics();
    assert_eq!(m.total, 1, "should pre-fill min_idle=1 connection");
    assert_eq!(m.idle, 1);
    assert_eq!(m.in_use, 0);
    assert_eq!(m.total_created, 1);
}

#[tokio::test]
async fn test_pool_create_min_idle_zero() {
    let mut config = test_config();
    config.min_idle = 0;
    let pool = Pool::new(config, LifecycleHooks::default())
        .await
        .unwrap();

    let m = pool.metrics();
    assert_eq!(m.total, 0, "min_idle=0 means no pre-fill");
    assert_eq!(m.total_created, 0);
}

#[tokio::test]
async fn test_pool_create_min_idle_multiple() {
    let mut config = test_config();
    config.min_idle = 3;
    let pool = Pool::new(config, LifecycleHooks::default())
        .await
        .unwrap();

    let m = pool.metrics();
    assert_eq!(m.total, 3);
    assert_eq!(m.idle, 3);
    assert_eq!(m.total_created, 3);
}

// ---------------------------------------------------------------------------
// Checkout and checkin
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_checkout_basic() {
    let pool = Pool::new(test_config(), LifecycleHooks::default())
        .await
        .unwrap();

    let mut guard = pool.get().await.unwrap();
    let m = pool.metrics();
    assert_eq!(m.in_use, 1);
    assert_eq!(m.total_checkouts, 1);

    // Use the connection.
    let (rows, _) = send_query(&mut guard, "SELECT 1").await;
    assert_eq!(rows.len(), 1);

    // Drop returns it.
    drop(guard);
    // Give the return_conn spawn a moment.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let m = pool.metrics();
    assert_eq!(m.in_use, 0);
    assert_eq!(m.idle, 1);
}

#[tokio::test]
async fn test_checkout_reuses_idle_connection() {
    let pool = Pool::new(test_config(), LifecycleHooks::default())
        .await
        .unwrap();

    // First checkout uses pre-filled connection.
    let g1 = pool.get().await.unwrap();
    let m = pool.metrics();
    assert_eq!(m.total_created, 1, "reuses pre-filled, no new creation");
    assert_eq!(m.total_checkouts, 1);
    drop(g1);
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Second checkout reuses the returned connection.
    let _g2 = pool.get().await.unwrap();
    let m = pool.metrics();
    assert_eq!(m.total_created, 1, "still only 1 created — reused");
    assert_eq!(m.total_checkouts, 2);
}

#[tokio::test]
async fn test_checkout_creates_new_when_no_idle() {
    let mut config = test_config();
    config.min_idle = 0;
    let pool = Pool::new(config, LifecycleHooks::default())
        .await
        .unwrap();

    assert_eq!(pool.metrics().total, 0);

    let _g = pool.get().await.unwrap();
    let m = pool.metrics();
    assert_eq!(m.total, 1, "created a new connection on demand");
    assert_eq!(m.total_created, 1);
    assert_eq!(m.in_use, 1);
}

#[tokio::test]
async fn test_multiple_checkouts_grow_pool() {
    let mut config = test_config();
    config.min_idle = 0;
    config.max_size = 3;
    let pool = Pool::new(config, LifecycleHooks::default())
        .await
        .unwrap();

    let g1 = pool.get().await.unwrap();
    let g2 = pool.get().await.unwrap();
    let g3 = pool.get().await.unwrap();

    let m = pool.metrics();
    assert_eq!(m.total, 3);
    assert_eq!(m.in_use, 3);
    assert_eq!(m.idle, 0);
    assert_eq!(m.total_created, 3);

    drop(g1);
    drop(g2);
    drop(g3);
}

// ---------------------------------------------------------------------------
// Connection actually works after checkout
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_checkout_connection_functional_select() {
    let pool = Pool::new(test_config(), LifecycleHooks::default())
        .await
        .unwrap();

    let mut g = pool.get().await.unwrap();
    let (rows, _tag) = send_query(&mut g, "SELECT 42 AS n").await;
    assert_eq!(rows.len(), 1);
    assert_eq!(col_str(&rows[0][0]), "42");
}

#[tokio::test]
async fn test_checkout_connection_functional_after_return_and_reuse() {
    let pool = Pool::new(test_config(), LifecycleHooks::default())
        .await
        .unwrap();

    // First use.
    {
        let mut g = pool.get().await.unwrap();
        let (rows, _) = send_query(&mut g, "SELECT 'first' AS val").await;
        assert_eq!(col_str(&rows[0][0]), "first");
    }
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Second use — same connection reused.
    {
        let mut g = pool.get().await.unwrap();
        let (rows, _) = send_query(&mut g, "SELECT 'second' AS val").await;
        assert_eq!(col_str(&rows[0][0]), "second");
    }
    assert_eq!(pool.metrics().total_created, 1, "connection was reused");
}

#[tokio::test]
async fn test_checkout_connection_functional_with_pipeline() {
    let pool = Pool::new(test_config(), LifecycleHooks::default())
        .await
        .unwrap();

    let g = pool.get().await.unwrap();
    let wire_poolable = g.take();
    let mut pipeline = pg_wire::PgPipeline::new(wire_poolable.0);
    // Use the taken connection via PgPipeline.
    let rows = pipeline
        .query("SELECT $1::text AS val", &[Some(b"test" as &[u8])], &[0])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(col_str(&rows[0][0]), "test");
    assert_eq!(pool.metrics().total, 0, "take() removes from pool");
}

// ---------------------------------------------------------------------------
// Max size enforcement
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_max_size_blocks_when_full() {
    let mut config = test_config();
    config.min_idle = 0;
    config.max_size = 2;
    config.checkout_timeout = Duration::from_millis(200);
    let pool = Pool::new(config, LifecycleHooks::default())
        .await
        .unwrap();

    let _g1 = pool.get().await.unwrap();
    let _g2 = pool.get().await.unwrap();
    assert_eq!(pool.metrics().total, 2);

    // Third checkout should timeout.
    let result = pool.get().await;
    assert!(result.is_err());
    match result {
        Err(e) => {
            let msg = e.to_string();
            assert!(
                msg.contains("timeout") || msg.contains("Timeout") || msg.contains("capacity"),
                "expected timeout error, got: {msg}"
            );
        }
        Ok(_) => panic!("expected error"),
    }
    assert_eq!(pool.metrics().total_timeouts, 1);
}

#[tokio::test]
async fn test_max_size_unblocks_on_checkin() {
    let mut config = test_config();
    config.min_idle = 0;
    config.max_size = 1;
    config.checkout_timeout = Duration::from_secs(2);
    let pool = Pool::new(config, LifecycleHooks::default())
        .await
        .unwrap();

    let g1 = pool.get().await.unwrap();

    // Spawn a task that waits for a connection.
    let pool2 = Arc::clone(&pool);
    let handle = tokio::spawn(async move {
        let g = pool2.get().await.unwrap();
        let m = pool2.metrics();
        assert_eq!(m.total_checkouts, 2);
        drop(g);
    });

    // Give the waiter time to enqueue.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Return the first connection — this should wake the waiter.
    drop(g1);

    // Waiter should complete successfully.
    handle.await.unwrap();
}

// ---------------------------------------------------------------------------
// Waiter queue and dead-waiter skipping
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_waiter_queue_fifo_order() {
    let mut config = test_config();
    config.min_idle = 0;
    config.max_size = 1;
    config.checkout_timeout = Duration::from_secs(3);
    let pool = Pool::new(config, LifecycleHooks::default())
        .await
        .unwrap();

    let g1 = pool.get().await.unwrap();

    let order = Arc::new(std::sync::Mutex::new(Vec::new()));

    // Spawn two waiters.
    let pool2 = Arc::clone(&pool);
    let order2 = Arc::clone(&order);
    let h1 = tokio::spawn(async move {
        let _g = pool2.get().await.unwrap();
        order2.lock().unwrap().push(1);
        // Hold briefly so second waiter can't get it yet.
        tokio::time::sleep(Duration::from_millis(20)).await;
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    let pool3 = Arc::clone(&pool);
    let order3 = Arc::clone(&order);
    let h2 = tokio::spawn(async move {
        let _g = pool3.get().await.unwrap();
        order3.lock().unwrap().push(2);
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Release first connection — waiter 1 should get it first.
    drop(g1);
    h1.await.unwrap();

    // Now waiter 2 gets it after waiter 1 drops.
    tokio::time::sleep(Duration::from_millis(100)).await;
    h2.await.unwrap();

    let final_order = order.lock().unwrap().clone();
    assert_eq!(final_order, vec![1, 2], "FIFO order");
}

#[tokio::test]
async fn test_dead_waiter_skipping() {
    let mut config = test_config();
    config.min_idle = 0;
    config.max_size = 1;
    config.checkout_timeout = Duration::from_millis(100);
    let pool = Pool::new(config, LifecycleHooks::default())
        .await
        .unwrap();

    let g1 = pool.get().await.unwrap();

    // Spawn a waiter that will time out (dead waiter).
    let pool2 = Arc::clone(&pool);
    let h_dead = tokio::spawn(async move {
        let result = pool2.get().await;
        assert!(result.is_err(), "dead waiter should timeout");
    });

    // Wait for it to timeout.
    h_dead.await.unwrap();

    // Spawn a real waiter.
    let pool3 = Arc::clone(&pool);
    let h_real = tokio::spawn(async move {
        let g = pool3.get().await.unwrap();
        // Verify connection works.
        drop(g);
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Return the connection — the dead waiter should be skipped.
    drop(g1);

    h_real.await.unwrap();
}

// ---------------------------------------------------------------------------
// Checkout timeout
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_checkout_timeout_fires() {
    let mut config = test_config();
    config.min_idle = 0;
    config.max_size = 1;
    config.checkout_timeout = Duration::from_millis(100);
    let pool = Pool::new(config, LifecycleHooks::default())
        .await
        .unwrap();

    let _g = pool.get().await.unwrap();

    let start = std::time::Instant::now();
    let result = pool.get().await;
    let elapsed = start.elapsed();

    assert!(result.is_err());
    assert!(elapsed >= Duration::from_millis(90), "should wait ~100ms");
    assert!(elapsed < Duration::from_millis(500), "shouldn't wait too long");
    assert_eq!(pool.metrics().total_timeouts, 1);
}

#[tokio::test]
async fn test_checkout_timeout_counter_accumulates() {
    let mut config = test_config();
    config.min_idle = 0;
    config.max_size = 1;
    config.checkout_timeout = Duration::from_millis(50);
    let pool = Pool::new(config, LifecycleHooks::default())
        .await
        .unwrap();

    let _g = pool.get().await.unwrap();

    for _ in 0..5 {
        let _ = pool.get().await;
    }

    assert_eq!(pool.metrics().total_timeouts, 5);
}

// ---------------------------------------------------------------------------
// Lifecycle hooks
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_lifecycle_hooks_on_create() {
    let counter = Arc::new(AtomicU64::new(0));
    let c = Arc::clone(&counter);
    let hooks = LifecycleHooks {
        on_create: Some(Box::new(move || {
            c.fetch_add(1, Ordering::Relaxed);
        })),
        ..Default::default()
    };

    let mut config = test_config();
    config.min_idle = 2;
    let pool = Pool::new(config, hooks).await.unwrap();
    assert_eq!(counter.load(Ordering::Relaxed), 2, "on_create fired for min_idle");

    // Checkout that creates a new conn.
    let _g1 = pool.get().await.unwrap();
    let _g2 = pool.get().await.unwrap();
    // A third checkout triggers a new creation.
    let _g3 = pool.get().await.unwrap();
    assert_eq!(counter.load(Ordering::Relaxed), 3);
}

#[tokio::test]
async fn test_lifecycle_hooks_on_checkout() {
    let counter = Arc::new(AtomicU64::new(0));
    let c = Arc::clone(&counter);
    let hooks = LifecycleHooks {
        on_checkout: Some(Box::new(move || {
            c.fetch_add(1, Ordering::Relaxed);
        })),
        ..Default::default()
    };

    let pool = Pool::new(test_config(), hooks).await.unwrap();

    let g1 = pool.get().await.unwrap();
    assert_eq!(counter.load(Ordering::Relaxed), 1);
    drop(g1);
    tokio::time::sleep(Duration::from_millis(50)).await;

    let _g2 = pool.get().await.unwrap();
    assert_eq!(counter.load(Ordering::Relaxed), 2);
}

#[tokio::test]
async fn test_lifecycle_hooks_on_checkin() {
    let counter = Arc::new(AtomicU64::new(0));
    let c = Arc::clone(&counter);
    let hooks = LifecycleHooks {
        on_checkin: Some(Box::new(move || {
            c.fetch_add(1, Ordering::Relaxed);
        })),
        ..Default::default()
    };

    let pool = Pool::new(test_config(), hooks).await.unwrap();

    let g1 = pool.get().await.unwrap();
    assert_eq!(counter.load(Ordering::Relaxed), 0, "no checkin yet");
    drop(g1);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(counter.load(Ordering::Relaxed), 1, "checkin fired on return");
}

#[tokio::test]
async fn test_lifecycle_hooks_on_destroy() {
    let counter = Arc::new(AtomicU64::new(0));
    let c = Arc::clone(&counter);
    let hooks = LifecycleHooks {
        on_destroy: Some(Box::new(move || {
            c.fetch_add(1, Ordering::Relaxed);
        })),
        ..Default::default()
    };

    let pool = Pool::new(test_config(), hooks).await.unwrap();
    assert_eq!(counter.load(Ordering::Relaxed), 0);

    // Drain destroys idle connections.
    pool.drain().await;
    assert!(counter.load(Ordering::Relaxed) >= 1, "on_destroy fired during drain");
}

#[tokio::test]
async fn test_all_hooks_fire_in_sequence() {
    let log = Arc::new(std::sync::Mutex::new(Vec::<&'static str>::new()));

    let l1 = Arc::clone(&log);
    let l2 = Arc::clone(&log);
    let l3 = Arc::clone(&log);
    let l4 = Arc::clone(&log);

    let hooks = LifecycleHooks {
        on_create: Some(Box::new(move || { l1.lock().unwrap().push("create"); })),
        on_checkout: Some(Box::new(move || { l2.lock().unwrap().push("checkout"); })),
        on_checkin: Some(Box::new(move || { l3.lock().unwrap().push("checkin"); })),
        on_destroy: Some(Box::new(move || { l4.lock().unwrap().push("destroy"); })),
    };

    let mut config = test_config();
    config.min_idle = 0;
    let pool = Pool::new(config, hooks).await.unwrap();

    let g = pool.get().await.unwrap();
    drop(g);
    tokio::time::sleep(Duration::from_millis(50)).await;

    pool.drain().await;

    let events = log.lock().unwrap().clone();
    assert_eq!(events[0], "create");
    assert_eq!(events[1], "checkout");
    assert_eq!(events[2], "checkin");
    assert_eq!(events[3], "destroy");
}

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_metrics_accuracy() {
    let mut config = test_config();
    config.min_idle = 2;
    config.max_size = 5;
    let pool = Pool::new(config, LifecycleHooks::default())
        .await
        .unwrap();

    let m = pool.metrics();
    assert_eq!(m.total, 2);
    assert_eq!(m.idle, 2);
    assert_eq!(m.in_use, 0);
    assert_eq!(m.total_checkouts, 0);
    assert_eq!(m.total_timeouts, 0);

    let g1 = pool.get().await.unwrap();
    let g2 = pool.get().await.unwrap();
    let m = pool.metrics();
    assert_eq!(m.in_use, 2);
    assert_eq!(m.idle, 0);
    assert_eq!(m.total_checkouts, 2);

    drop(g1);
    tokio::time::sleep(Duration::from_millis(50)).await;
    let m = pool.metrics();
    assert_eq!(m.in_use, 1);
    assert_eq!(m.idle, 1);

    drop(g2);
    tokio::time::sleep(Duration::from_millis(50)).await;
    let m = pool.metrics();
    assert_eq!(m.in_use, 0);
    assert_eq!(m.idle, 2);
}

#[tokio::test]
async fn test_metrics_total_created_and_destroyed() {
    let mut config = test_config();
    config.min_idle = 0;
    config.max_size = 3;
    let pool = Pool::new(config, LifecycleHooks::default())
        .await
        .unwrap();

    // Create 3 connections.
    let g1 = pool.get().await.unwrap();
    let g2 = pool.get().await.unwrap();
    let g3 = pool.get().await.unwrap();
    assert_eq!(pool.metrics().total_created, 3);

    // Return all and drain.
    drop(g1);
    drop(g2);
    drop(g3);
    tokio::time::sleep(Duration::from_millis(100)).await;

    pool.drain().await;
    let m = pool.metrics();
    assert_eq!(m.total_created, 3);
    assert_eq!(m.total_destroyed, 3);
    assert_eq!(m.total, 0);
}

#[tokio::test]
async fn test_status_string() {
    let pool = Pool::new(test_config(), LifecycleHooks::default())
        .await
        .unwrap();

    let status = pool.status();
    assert!(status.contains("total=1"));
    assert!(status.contains("idle=1"));
    assert!(status.contains("in_use=0"));
}

// ---------------------------------------------------------------------------
// Graceful drain
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_drain_destroys_idle_connections() {
    let mut config = test_config();
    config.min_idle = 3;
    let pool = Pool::new(config, LifecycleHooks::default())
        .await
        .unwrap();

    assert_eq!(pool.metrics().total, 3);

    pool.drain().await;

    let m = pool.metrics();
    assert_eq!(m.total, 0);
    assert_eq!(m.idle, 0);
    assert_eq!(m.total_destroyed, 3);
}

#[tokio::test]
async fn test_drain_rejects_new_checkouts() {
    let pool = Pool::new(test_config(), LifecycleHooks::default())
        .await
        .unwrap();

    pool.drain().await;

    let result = pool.get().await;
    assert!(result.is_err());
    match result {
        Err(e) => {
            let msg = e.to_string();
            assert!(msg.contains("draining") || msg.contains("Draining"), "got: {msg}");
        }
        Ok(_) => panic!("expected draining error"),
    }
}

#[tokio::test]
async fn test_drain_waits_for_in_use_connections() {
    let mut config = test_config();
    config.min_idle = 0;
    let pool = Pool::new(config, LifecycleHooks::default())
        .await
        .unwrap();

    let g = pool.get().await.unwrap();

    // Spawn drain — it should block until g is returned.
    let pool2 = Arc::clone(&pool);
    let drain_handle = tokio::spawn(async move {
        pool2.drain().await;
    });

    // Verify drain is still waiting.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!drain_handle.is_finished(), "drain should still be waiting");

    // Return the connection.
    drop(g);

    // Drain should complete.
    tokio::time::timeout(Duration::from_secs(2), drain_handle)
        .await
        .expect("drain should complete within timeout")
        .unwrap();

    assert_eq!(pool.metrics().total, 0);
}

#[tokio::test]
async fn test_drain_destroys_returned_connections() {
    let mut config = test_config();
    config.min_idle = 0;
    let pool = Pool::new(config, LifecycleHooks::default())
        .await
        .unwrap();

    let g1 = pool.get().await.unwrap();
    let g2 = pool.get().await.unwrap();

    // Start drain.
    let pool2 = Arc::clone(&pool);
    let drain_handle = tokio::spawn(async move {
        pool2.drain().await;
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Return connections — they should be destroyed, not returned to idle.
    drop(g1);
    drop(g2);

    tokio::time::timeout(Duration::from_secs(2), drain_handle)
        .await
        .expect("drain completes")
        .unwrap();

    let m = pool.metrics();
    assert_eq!(m.total, 0);
    assert_eq!(m.idle, 0);
    assert_eq!(m.total_destroyed, 2);
}

// ---------------------------------------------------------------------------
// PoolGuard take()
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_pool_guard_take() {
    let mut config = test_config();
    config.min_idle = 0;
    let pool = Pool::new(config, LifecycleHooks::default())
        .await
        .unwrap();

    let g = pool.get().await.unwrap();
    assert_eq!(pool.metrics().total, 1);
    assert_eq!(pool.metrics().in_use, 1);

    let _conn: WirePoolable = g.take();

    // Connection removed from pool tracking.
    assert_eq!(pool.metrics().total, 0);
    assert_eq!(pool.metrics().in_use, 0);
}

#[tokio::test]
async fn test_pool_guard_deref() {
    let pool = Pool::new(test_config(), LifecycleHooks::default())
        .await
        .unwrap();

    let g = pool.get().await.unwrap();
    // Deref to WireConn — verify has_pending_data accessible.
    assert!(!g.has_pending_data());
}

// ---------------------------------------------------------------------------
// Concurrent checkout/checkin stress test
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_concurrent_checkout_checkin() {
    let mut config = test_config();
    config.min_idle = 0;
    config.max_size = 5;
    config.checkout_timeout = Duration::from_secs(5);
    let pool = Pool::new(config, LifecycleHooks::default())
        .await
        .unwrap();

    let mut handles = Vec::new();
    for i in 0..20 {
        let pool = Arc::clone(&pool);
        handles.push(tokio::spawn(async move {
            let mut g = pool.get().await.unwrap();
            // Do a quick query.
            let _ = send_query(&mut g, &format!("SELECT {i}")).await;
            // Hold briefly.
            tokio::time::sleep(Duration::from_millis(10)).await;
            drop(g);
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    let m = pool.metrics();
    assert_eq!(m.total_checkouts, 20);
    assert!(m.total_created <= 5, "should not exceed max_size");
    assert_eq!(m.in_use, 0, "all returned");
}

#[tokio::test]
async fn test_high_concurrency_no_deadlock() {
    let mut config = test_config();
    config.min_idle = 2;
    config.max_size = 3;
    config.checkout_timeout = Duration::from_secs(10);
    let pool = Pool::new(config, LifecycleHooks::default())
        .await
        .unwrap();

    // 50 concurrent tasks competing for 3 connections.
    let mut handles = Vec::new();
    for _ in 0..50 {
        let pool = Arc::clone(&pool);
        handles.push(tokio::spawn(async move {
            let g = pool.get().await.unwrap();
            tokio::time::sleep(Duration::from_millis(5)).await;
            drop(g);
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    let m = pool.metrics();
    assert_eq!(m.total_checkouts, 50);
    assert_eq!(m.in_use, 0);
}

// ---------------------------------------------------------------------------
// Connection expiry (max_lifetime)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_expired_connections_evicted_on_checkout() {
    let mut config = test_config();
    config.min_idle = 0;
    config.max_size = 5;
    config.max_lifetime = Duration::from_millis(100);
    config.max_lifetime_jitter = Duration::ZERO;
    let pool = Pool::new(config, LifecycleHooks::default())
        .await
        .unwrap();

    // Create and return a connection.
    let g = pool.get().await.unwrap();
    drop(g);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(pool.metrics().total_created, 1);

    // Wait for it to expire.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Checkout — expired conn should be evicted, new one created.
    let _g2 = pool.get().await.unwrap();
    let m = pool.metrics();
    assert_eq!(m.total_created, 2, "old expired, new created");
    assert!(m.total_destroyed >= 1, "expired one destroyed");
}

// ---------------------------------------------------------------------------
// Connection invalidation scenario
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_connection_invalid_after_pg_terminate() {
    // ConnPool's health check (has_pending_data) only checks the read buffer,
    // not the socket state. A remotely-killed connection passes the check
    // and gets handed out. This is a known limitation shared by deadpool/bb8.
    // Verify: (1) the dead connection fails on use, (2) after take(),
    // a fresh connection can be obtained.
    let mut config = test_config();
    config.min_idle = 0;
    let pool = Pool::new(config, LifecycleHooks::default())
        .await
        .unwrap();

    // Get connection and find its PG PID.
    let mut g = pool.get().await.unwrap();
    let (rows, _) = send_query(&mut g, "SELECT pg_backend_pid()").await;
    let pid = col_str(&rows[0][0]).parse::<i32>().unwrap();
    drop(g);
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Kill the PG backend from a separate connection.
    let mut killer = WireConn::connect(ADDR, USER, PASS, DB).await.unwrap();
    let _ = send_query_raw(
        &mut killer,
        &format!("SELECT pg_terminate_backend({pid})"),
    )
    .await;

    tokio::time::sleep(Duration::from_millis(200)).await;

    // First checkout gives the dead connection. Query fails.
    let mut g = pool.get().await.unwrap();
    let result = send_query_try(&mut g, "SELECT 1").await;
    assert!(result.is_err(), "query on killed connection should fail");

    // Take the dead connection out of the pool entirely.
    let _dead_conn = g.take();
    assert_eq!(pool.metrics().total, 0, "dead conn removed from pool");

    // Fresh checkout creates a new working connection.
    let mut g2 = pool.get().await.unwrap();
    let (rows, _) = send_query(&mut g2, "SELECT 1").await;
    assert_eq!(col_str(&rows[0][0]), "1");
    assert_eq!(pool.metrics().total_created, 2, "new connection was created");
}

// ---------------------------------------------------------------------------
// Error handling
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_pool_with_invalid_address() {
    let mut config = test_config();
    config.addr = "127.0.0.1:1".to_string(); // invalid port
    config.min_idle = 0;
    config.checkout_timeout = Duration::from_millis(500);
    let pool = Pool::new(config, LifecycleHooks::default())
        .await
        .unwrap();

    let result = pool.get().await;
    assert!(result.is_err(), "should fail to connect to invalid address");
}

#[tokio::test]
async fn test_pool_create_with_invalid_address_and_min_idle() {
    let mut config = test_config();
    config.addr = "127.0.0.1:1".to_string();
    config.min_idle = 3;
    // Should not panic — just warns and creates pool with 0 idle.
    let pool = Pool::new(config, LifecycleHooks::default())
        .await
        .unwrap();
    assert_eq!(pool.metrics().total, 0, "failed pre-fill is not fatal");
}

// ---------------------------------------------------------------------------
// Drain correctness (missed-notification regression test)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_drain_completes_with_rapid_return() {
    // Regression test: drain() must not hang when connections are returned
    // at the same time drain starts (missed-notification bug).
    let mut config = test_config();
    config.min_idle = 0;
    config.max_size = 5;
    let pool = Pool::new(config, LifecycleHooks::default())
        .await
        .unwrap();

    // Check out several connections.
    let guards: Vec<_> = futures_collect(
        (0..5).map(|_| {
            let pool = Arc::clone(&pool);
            async move { pool.get().await.unwrap() }
        }),
    )
    .await;

    assert_eq!(pool.metrics().in_use, 5);

    // Spawn drain — it should wait for all to return.
    let pool2 = Arc::clone(&pool);
    let drain_handle = tokio::spawn(async move {
        pool2.drain().await;
    });

    // Return all connections rapidly in parallel.
    tokio::time::sleep(Duration::from_millis(50)).await;
    drop(guards);

    // Drain should complete within a reasonable time.
    tokio::time::timeout(Duration::from_secs(5), drain_handle)
        .await
        .expect("drain should not hang")
        .unwrap();

    assert_eq!(pool.metrics().total, 0);
}

#[tokio::test]
async fn test_drain_with_no_connections() {
    // Drain on empty pool (total_count already 0) should return immediately.
    let mut config = test_config();
    config.min_idle = 0;
    let pool = Pool::new(config, LifecycleHooks::default())
        .await
        .unwrap();

    assert_eq!(pool.metrics().total, 0);

    // Should not hang.
    tokio::time::timeout(Duration::from_secs(1), pool.drain())
        .await
        .expect("drain on empty pool should complete immediately");
}

// ---------------------------------------------------------------------------
// Maintenance max_size enforcement
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_maintenance_does_not_exceed_max_size() {
    let mut config = test_config();
    config.min_idle = 3;
    config.max_size = 3;
    config.maintenance_interval = Duration::from_millis(100); // fast maintenance
    let pool = Pool::new(config, LifecycleHooks::default())
        .await
        .unwrap();

    // Check out all connections to trigger maintenance replenishment.
    let g1 = pool.get().await.unwrap();
    let g2 = pool.get().await.unwrap();
    let g3 = pool.get().await.unwrap();

    // While all 3 are checked out, maintenance will try to replenish.
    // It must NOT exceed max_size=3.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let m = pool.metrics();
    assert!(m.total <= 3, "maintenance must not exceed max_size, got total={}", m.total);

    drop(g1);
    drop(g2);
    drop(g3);
}

#[tokio::test]
async fn test_concurrent_get_does_not_exceed_max_size() {
    let mut config = test_config();
    config.min_idle = 0;
    config.max_size = 3;
    config.checkout_timeout = Duration::from_secs(5);
    let pool = Pool::new(config, LifecycleHooks::default())
        .await
        .unwrap();

    // Spawn 10 concurrent gets. Only 3 should succeed at a time.
    let mut handles = Vec::new();
    for _ in 0..10 {
        let pool = Arc::clone(&pool);
        handles.push(tokio::spawn(async move {
            let g = pool.get().await.unwrap();
            tokio::time::sleep(Duration::from_millis(50)).await;
            drop(g);
        }));
    }

    // Check pool doesn't overshoot.
    tokio::time::sleep(Duration::from_millis(20)).await;
    let m = pool.metrics();
    assert!(m.total <= 3, "concurrent gets must respect max_size, got total={}", m.total);

    for h in handles {
        h.await.unwrap();
    }
}

// ---------------------------------------------------------------------------
// AsyncConn reader notify (regression: no busy-wait)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_async_conn_no_cpu_spin_when_idle() {
    // Verify that an idle AsyncConn doesn't burn CPU.
    // We can't measure CPU directly, but we can verify it works
    // correctly after being idle for a while.
    let conn = pg_wire::AsyncConn::new(
        pg_wire::WireConn::connect(ADDR, USER, PASS, DB).await.unwrap()
    );

    // Let the reader sit idle.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Now execute a query — should work fine.
    let rows = conn.exec_query("SELECT 1 AS n", &[], &[]).await.unwrap();
    assert_eq!(col_str(&rows[0][0]), "1");

    // And another after more idle time.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let rows = conn.exec_query("SELECT 2 AS n", &[], &[]).await.unwrap();
    assert_eq!(col_str(&rows[0][0]), "2");
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Collect futures concurrently (simple join_all replacement).
async fn futures_collect<F, T>(futs: impl IntoIterator<Item = F>) -> Vec<T>
where
    F: std::future::Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    let handles: Vec<_> = futs.into_iter().map(tokio::spawn).collect();
    let mut results = Vec::with_capacity(handles.len());
    for h in handles {
        results.push(h.await.unwrap());
    }
    results
}

/// Send a simple query via WireConn's raw protocol and collect rows.
async fn send_query(
    guard: &mut PoolGuard<WirePoolable>,
    sql: &str,
) -> (Vec<Vec<Option<Vec<u8>>>>, String) {
    use bytes::BytesMut;
    use pg_wire::protocol::types::FrontendMsg;

    let conn: &mut WireConn = &mut guard.conn_mut().0;
    let mut buf = BytesMut::new();
    pg_wire::protocol::frontend::encode_message(
        &FrontendMsg::Query(sql.as_bytes()),
        &mut buf,
    );
    conn.send_raw(&buf).await.unwrap();
    conn.collect_rows().await.unwrap()
}

async fn send_query_raw(
    conn: &mut WireConn,
    sql: &str,
) -> (Vec<Vec<Option<Vec<u8>>>>, String) {
    use bytes::BytesMut;
    use pg_wire::protocol::types::FrontendMsg;

    let mut buf = BytesMut::new();
    pg_wire::protocol::frontend::encode_message(
        &FrontendMsg::Query(sql.as_bytes()),
        &mut buf,
    );
    conn.send_raw(&buf).await.unwrap();
    conn.collect_rows().await.unwrap()
}

async fn send_query_try(
    guard: &mut PoolGuard<WirePoolable>,
    sql: &str,
) -> Result<(Vec<Vec<Option<Vec<u8>>>>, String), PgWireError> {
    use bytes::BytesMut;
    use pg_wire::protocol::types::FrontendMsg;

    let conn: &mut WireConn = &mut guard.conn_mut().0;
    let mut buf = BytesMut::new();
    pg_wire::protocol::frontend::encode_message(
        &FrontendMsg::Query(sql.as_bytes()),
        &mut buf,
    );
    conn.send_raw(&buf).await?;
    conn.collect_rows().await
}

fn col_str(col: &Option<Vec<u8>>) -> String {
    String::from_utf8(col.as_ref().unwrap().clone()).unwrap()
}
