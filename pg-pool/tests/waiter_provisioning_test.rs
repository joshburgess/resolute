//! Targeted tests for the pool's waiter-provisioning behavior.
//!
//! When a returned connection is destroyed (because `has_pending_data()`
//! reports true or `reset()` returns false), any task currently parked
//! on the waiter queue should still receive a fresh connection rather
//! than waiting until `checkout_timeout`.
//!
//! These tests use a `MockConn` so the pool logic is exercised in
//! isolation — no PostgreSQL or docker required.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use pg_pool::{ConnPool, ConnPoolConfig, LifecycleHooks, Poolable};

#[derive(Debug)]
struct MockError;

impl std::fmt::Display for MockError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "mock error")
    }
}

impl std::error::Error for MockError {}

#[derive(Debug)]
struct MockConn {
    broken: Arc<AtomicBool>,
    reset_should_fail: Arc<AtomicBool>,
}

impl MockConn {
    fn mark_broken(&self) {
        self.broken.store(true, Ordering::Release);
    }

    fn mark_reset_failing(&self) {
        self.reset_should_fail.store(true, Ordering::Release);
    }
}

impl Poolable for MockConn {
    type Error = MockError;

    async fn connect(
        _addr: &str,
        _user: &str,
        _password: &str,
        _database: &str,
    ) -> Result<Self, Self::Error> {
        Ok(Self {
            broken: Arc::new(AtomicBool::new(false)),
            reset_should_fail: Arc::new(AtomicBool::new(false)),
        })
    }

    fn has_pending_data(&self) -> bool {
        self.broken.load(Ordering::Acquire)
    }

    async fn reset(&self) -> bool {
        !self.reset_should_fail.load(Ordering::Acquire)
    }
}

fn config_max_size_one() -> ConnPoolConfig {
    let mut c = ConnPoolConfig::default();
    c.min_idle = 0;
    c.max_size = 1;
    c.checkout_timeout = Duration::from_secs(2);
    c.maintenance_interval = Duration::from_secs(3600);
    c.max_lifetime = Duration::from_secs(300);
    c.max_lifetime_jitter = Duration::ZERO;
    c.test_on_checkout = true;
    c
}

/// A waiter parked at `max_size` capacity must still receive a fresh
/// connection when the returning connection is destroyed because
/// `has_pending_data()` is true.
#[tokio::test]
async fn test_waiter_provisioned_after_broken_conn_destroyed() {
    let pool = ConnPool::<MockConn>::new(config_max_size_one(), LifecycleHooks::default())
        .await
        .unwrap();

    let g1 = pool.get().await.unwrap();
    g1.mark_broken();

    let pool2 = Arc::clone(&pool);
    let h = tokio::spawn(async move { pool2.get().await });

    // Give the spawned task a moment to park as a waiter.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        pool.metrics().waiters,
        1,
        "spawned task should be parked as a waiter"
    );

    drop(g1);

    let g2 = tokio::time::timeout(Duration::from_millis(500), h)
        .await
        .expect("waiter should have been provisioned promptly, not waited for checkout_timeout")
        .unwrap()
        .unwrap();

    let m = pool.metrics();
    assert_eq!(m.in_use, 1);
    assert_eq!(m.total, 1);
    assert!(
        m.total_created >= 2,
        "expected at least 2 connections created (original + replacement), got {}",
        m.total_created
    );
    assert_eq!(m.total_destroyed, 1, "broken conn should have been destroyed");
    assert_eq!(m.total_timeouts, 0);

    drop(g2);
}

/// Same scenario but the connection was healthy — the destruction is
/// triggered by `reset()` returning false on return. The waiter should
/// still receive a fresh connection.
#[tokio::test]
async fn test_waiter_provisioned_after_reset_fails() {
    let pool = ConnPool::<MockConn>::new(config_max_size_one(), LifecycleHooks::default())
        .await
        .unwrap();

    let g1 = pool.get().await.unwrap();
    g1.mark_reset_failing();

    let pool2 = Arc::clone(&pool);
    let h = tokio::spawn(async move { pool2.get().await });

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(pool.metrics().waiters, 1);

    drop(g1);

    let _g2 = tokio::time::timeout(Duration::from_millis(500), h)
        .await
        .expect("waiter should have been provisioned after reset failure")
        .unwrap()
        .unwrap();

    let m = pool.metrics();
    assert!(m.total_created >= 2, "got {}", m.total_created);
    assert_eq!(m.total_destroyed, 1);
    assert_eq!(m.total_timeouts, 0);
}

/// When a connection is destroyed on return but no waiter is parked,
/// the pool should *not* eagerly create a replacement (it would only
/// be needed if min_idle requires it, which the maintenance task handles).
#[tokio::test]
async fn test_no_replacement_when_no_waiter() {
    let pool = ConnPool::<MockConn>::new(config_max_size_one(), LifecycleHooks::default())
        .await
        .unwrap();

    let g1 = pool.get().await.unwrap();
    g1.mark_broken();

    drop(g1);

    // Let the return task run.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let m = pool.metrics();
    assert_eq!(
        m.total_created, 1,
        "no waiter parked, so no replacement should be provisioned"
    );
    assert_eq!(m.total_destroyed, 1);
    assert_eq!(m.total, 0);
}

/// If the parked waiter's future was cancelled before the broken conn
/// is returned, the provisioned replacement should not be lost — it
/// should land on the idle stack so the next caller can use it.
#[tokio::test]
async fn test_replacement_falls_through_to_idle_when_waiter_cancelled() {
    let pool = ConnPool::<MockConn>::new(config_max_size_one(), LifecycleHooks::default())
        .await
        .unwrap();

    let g1 = pool.get().await.unwrap();
    g1.mark_broken();

    // Spawn a waiter, then abort it before g1 is returned.
    let pool2 = Arc::clone(&pool);
    let h = tokio::spawn(async move { pool2.get().await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(pool.metrics().waiters, 1);
    h.abort();
    let _ = h.await;
    // The waiter's oneshot::Sender is now closed; the queue still holds
    // the dead entry until return_conn_async tries to send.

    drop(g1);

    // Allow the return task to run, attempt the dead waiter, and park
    // the replacement on idle.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let m = pool.metrics();
    assert_eq!(m.total_destroyed, 1);
    assert!(
        m.total_created >= 2,
        "replacement should have been created, got {}",
        m.total_created
    );
    assert_eq!(m.total, 1, "replacement should be parked on idle");
    assert_eq!(m.in_use, 0);

    // Next checkout should reuse the idle replacement, not create another.
    let created_before = m.total_created;
    let _g2 = pool.get().await.unwrap();
    assert_eq!(
        pool.metrics().total_created,
        created_before,
        "next checkout should reuse the parked replacement"
    );
}
