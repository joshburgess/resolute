//! Integration tests for pg-pool with pg-wire backend.
//! Requires: docker compose up -d (PostgreSQL on port 54322)

use std::sync::Arc;
use std::time::Duration;

use pg_pool::{ConnPool, ConnPoolConfig, LifecycleHooks};
use pg_pool::wire::WirePoolable;

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
        max_lifetime_jitter: Duration::from_secs(0),
        checkout_timeout: Duration::from_secs(2),
        maintenance_interval: Duration::from_secs(3600),
        test_on_checkout: true,
    }
}

// ---------------------------------------------------------------------------
// Basic lifecycle
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_pool_create() {
    let pool = Pool::new(test_config(), LifecycleHooks::default())
        .await
        .unwrap();
    let m = pool.metrics();
    assert_eq!(m.total, 1);
    assert_eq!(m.idle, 1);
}

#[tokio::test]
async fn test_pool_create_min_idle_zero() {
    let mut config = test_config();
    config.min_idle = 0;
    let pool = Pool::new(config, LifecycleHooks::default())
        .await
        .unwrap();
    assert_eq!(pool.metrics().total, 0);
}

// ---------------------------------------------------------------------------
// Checkout and checkin
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_checkout_basic() {
    let pool = Pool::new(test_config(), LifecycleHooks::default())
        .await
        .unwrap();
    let g = pool.get().await.unwrap();
    assert_eq!(pool.metrics().in_use, 1);
    assert!(!g.has_pending_data());
    drop(g);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(pool.metrics().in_use, 0);
}

#[tokio::test]
async fn test_checkout_reuses_connection() {
    let pool = Pool::new(test_config(), LifecycleHooks::default())
        .await
        .unwrap();
    let _g1 = pool.get().await.unwrap();
    drop(_g1);
    tokio::time::sleep(Duration::from_millis(50)).await;
    let _g2 = pool.get().await.unwrap();
    assert_eq!(pool.metrics().total_created, 1);
}

#[tokio::test]
async fn test_checkout_creates_on_demand() {
    let mut config = test_config();
    config.min_idle = 0;
    let pool = Pool::new(config, LifecycleHooks::default())
        .await
        .unwrap();
    let _g = pool.get().await.unwrap();
    assert_eq!(pool.metrics().total_created, 1);
}

// ---------------------------------------------------------------------------
// Max size
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_max_size_blocks() {
    let mut config = test_config();
    config.min_idle = 0;
    config.max_size = 2;
    config.checkout_timeout = Duration::from_millis(200);
    let pool = Pool::new(config, LifecycleHooks::default())
        .await
        .unwrap();
    let _g1 = pool.get().await.unwrap();
    let _g2 = pool.get().await.unwrap();
    let result = pool.get().await;
    assert!(result.is_err());
    assert_eq!(pool.metrics().total_timeouts, 1);
}

#[tokio::test]
async fn test_max_size_unblocks() {
    let mut config = test_config();
    config.min_idle = 0;
    config.max_size = 1;
    config.checkout_timeout = Duration::from_secs(2);
    let pool = Pool::new(config, LifecycleHooks::default())
        .await
        .unwrap();
    let g1 = pool.get().await.unwrap();
    let pool2 = Arc::clone(&pool);
    let h = tokio::spawn(async move { pool2.get().await.unwrap(); });
    tokio::time::sleep(Duration::from_millis(100)).await;
    drop(g1);
    h.await.unwrap();
}

// ---------------------------------------------------------------------------
// Waiter queue
// ---------------------------------------------------------------------------

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

    // Dead waiter times out.
    let pool2 = Arc::clone(&pool);
    tokio::spawn(async move { let _ = pool2.get().await; }).await.unwrap();

    // Real waiter should get the connection.
    let pool3 = Arc::clone(&pool);
    let h = tokio::spawn(async move { pool3.get().await.unwrap(); });
    tokio::time::sleep(Duration::from_millis(50)).await;
    drop(g1);
    h.await.unwrap();
}

// ---------------------------------------------------------------------------
// Lifecycle hooks
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_hooks_all_fire() {
    let log = Arc::new(std::sync::Mutex::new(Vec::<&'static str>::new()));
    let (l1, l2, l3, l4) = (log.clone(), log.clone(), log.clone(), log.clone());

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
    assert_eq!(events, vec!["create", "checkout", "checkin", "destroy"]);
}

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_metrics() {
    let mut config = test_config();
    config.min_idle = 2;
    let pool = Pool::new(config, LifecycleHooks::default())
        .await
        .unwrap();
    let m = pool.metrics();
    assert_eq!(m.total, 2);
    assert_eq!(m.idle, 2);
    assert_eq!(m.in_use, 0);
}

// ---------------------------------------------------------------------------
// Drain
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_drain() {
    let mut config = test_config();
    config.min_idle = 3;
    let pool = Pool::new(config, LifecycleHooks::default())
        .await
        .unwrap();
    pool.drain().await;
    assert_eq!(pool.metrics().total, 0);
    let result = pool.get().await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_drain_waits_for_in_use() {
    let mut config = test_config();
    config.min_idle = 0;
    let pool = Pool::new(config, LifecycleHooks::default())
        .await
        .unwrap();
    let g = pool.get().await.unwrap();
    let pool2 = Arc::clone(&pool);
    let h = tokio::spawn(async move { pool2.drain().await; });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!h.is_finished());
    drop(g);
    tokio::time::timeout(Duration::from_secs(2), h).await.unwrap().unwrap();
}

#[tokio::test]
async fn test_drain_empty_pool() {
    let mut config = test_config();
    config.min_idle = 0;
    let pool = Pool::new(config, LifecycleHooks::default())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), pool.drain()).await.unwrap();
}

// ---------------------------------------------------------------------------
// PoolGuard
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_take() {
    let mut config = test_config();
    config.min_idle = 0;
    let pool = Pool::new(config, LifecycleHooks::default())
        .await
        .unwrap();
    let g = pool.get().await.unwrap();
    let _conn = g.take();
    assert_eq!(pool.metrics().total, 0);
}

// ---------------------------------------------------------------------------
// Concurrency
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_concurrent_checkout() {
    let mut config = test_config();
    config.min_idle = 0;
    config.max_size = 5;
    config.checkout_timeout = Duration::from_secs(5);
    let pool = Pool::new(config, LifecycleHooks::default())
        .await
        .unwrap();

    let mut handles = Vec::new();
    for _ in 0..20 {
        let pool = Arc::clone(&pool);
        handles.push(tokio::spawn(async move {
            let g = pool.get().await.unwrap();
            tokio::time::sleep(Duration::from_millis(10)).await;
            drop(g);
        }));
    }
    for h in handles { h.await.unwrap(); }
    assert_eq!(pool.metrics().total_checkouts, 20);
    assert_eq!(pool.metrics().in_use, 0);
}

// ---------------------------------------------------------------------------
// Expiry
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_expired_eviction() {
    let mut config = test_config();
    config.min_idle = 0;
    config.max_lifetime = Duration::from_millis(100);
    config.max_lifetime_jitter = Duration::ZERO;
    let pool = Pool::new(config, LifecycleHooks::default())
        .await
        .unwrap();

    let g = pool.get().await.unwrap();
    drop(g);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(pool.metrics().total_created, 1);

    tokio::time::sleep(Duration::from_millis(100)).await;
    let _g2 = pool.get().await.unwrap();
    assert_eq!(pool.metrics().total_created, 2);
}

// ---------------------------------------------------------------------------
// Error handling
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_invalid_address() {
    let mut config = test_config();
    config.addr = "127.0.0.1:1".to_string();
    config.min_idle = 0;
    let pool = Pool::new(config, LifecycleHooks::default())
        .await
        .unwrap();
    let result = pool.get().await;
    assert!(result.is_err());
}
