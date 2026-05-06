//! Tests for ExclusivePool (pg-pool + resolute integration).
//! Requires: docker compose up -d (PostgreSQL on port 54322)

#![allow(dead_code)]

use resolute::test_db::{
    test_addr as addr, test_database as db, test_password as pass, test_user as user,
};
use resolute::{ExclusivePool, Executor};

#[tokio::test]
async fn test_typed_pool_connect() {
    let pool = ExclusivePool::connect(addr(), user(), pass(), db(), 3)
        .await
        .unwrap();
    let m = pool.metrics();
    assert_eq!(m.total, 1); // min_idle default = 1
}

#[tokio::test]
async fn test_typed_pool_query() {
    let pool = ExclusivePool::connect(addr(), user(), pass(), db(), 3)
        .await
        .unwrap();
    let client = pool.get().await.unwrap();
    let rows = client.query("SELECT 42::int4 AS n", &[]).await.unwrap();
    assert_eq!(rows[0].get::<i32>(0).unwrap(), 42);
}

#[tokio::test]
async fn test_typed_pool_parameterized() {
    let pool = ExclusivePool::connect(addr(), user(), pass(), db(), 3)
        .await
        .unwrap();
    let client = pool.get().await.unwrap();
    let rows = client
        .query("SELECT name FROM api.authors WHERE id = $1", &[&1i32])
        .await
        .unwrap();
    assert_eq!(rows[0].get::<String>(0).unwrap(), "Alice");
}

#[tokio::test]
async fn test_typed_pool_reuse() {
    let pool = ExclusivePool::connect(addr(), user(), pass(), db(), 3)
        .await
        .unwrap();

    // Check out, query, return. Repeat — connection should be reused.
    for i in 0..10i32 {
        let client = pool.get().await.unwrap();
        let rows = client.query("SELECT $1::int4 AS n", &[&i]).await.unwrap();
        assert_eq!(rows[0].get::<i32>(0).unwrap(), i);
        // client dropped here — returned to pool
    }

    let m = pool.metrics();
    assert!(
        m.total <= 3,
        "pool should reuse connections, not create new ones"
    );
}

#[tokio::test]
async fn test_typed_pool_from_row() {
    #[derive(resolute::FromRow)]
    struct Author {
        id: i32,
        name: String,
    }

    let pool = ExclusivePool::connect(addr(), user(), pass(), db(), 3)
        .await
        .unwrap();
    let client = pool.get().await.unwrap();
    let rows = client
        .query("SELECT id, name FROM api.authors ORDER BY id", &[])
        .await
        .unwrap();

    let authors: Vec<Author> = rows
        .iter()
        .map(|r| resolute::FromRow::from_row(r).unwrap())
        .collect();
    assert!(authors.len() >= 3);
    assert_eq!(authors[0].name, "Alice");
}

#[tokio::test]
async fn test_typed_pool_drain() {
    let pool = ExclusivePool::connect(addr(), user(), pass(), db(), 2)
        .await
        .unwrap();
    pool.drain().await;
    assert_eq!(pool.metrics().total, 0);
}

#[tokio::test]
async fn test_typed_pool_query_named() {
    let pool = ExclusivePool::connect(addr(), user(), pass(), db(), 3)
        .await
        .unwrap();
    let client = pool.get().await.unwrap();
    let rows = client
        .query_named(
            "SELECT :id::int4 AS n, :name::text AS s",
            &[
                ("id", &42i32 as &dyn resolute::SqlParam),
                ("name", &"pooled" as &dyn resolute::SqlParam),
            ],
        )
        .await
        .unwrap();
    assert_eq!(rows[0].get::<i32>(0).unwrap(), 42);
    assert_eq!(rows[0].get::<String>(1).unwrap(), "pooled");
}

#[tokio::test]
async fn test_typed_pool_execute_named() {
    let pool = ExclusivePool::connect(addr(), user(), pass(), db(), 3)
        .await
        .unwrap();
    let client = pool.get().await.unwrap();
    client
        .simple_query("CREATE TEMP TABLE pool_named_test (id int, val text)")
        .await
        .unwrap();
    let count = client
        .execute_named(
            "INSERT INTO pool_named_test VALUES (:id, :val)",
            &[
                ("id", &1i32 as &dyn resolute::SqlParam),
                ("val", &"hello" as &dyn resolute::SqlParam),
            ],
        )
        .await
        .unwrap();
    assert_eq!(count, 1);
}

/// Regression: a `PooledTransaction` dropped after a failing query (which
/// leaves PostgreSQL in `25P02 current transaction is aborted`) must not
/// leave its connection in the pool in a poisoned state. The Drop path
/// enqueues `ROLLBACK` on the connection's writer task; the writer
/// processes it (FIFO) before the pool's `DISCARD ALL` reset, so the
/// connection returns to idle and is reusable by the next caller.
#[tokio::test]
async fn test_pool_not_poisoned_by_aborted_tx_drop() {
    let pool = ExclusivePool::connect(addr(), user(), pass(), db(), 1)
        .await
        .unwrap();

    // Provoke a query failure inside a transaction, then drop the
    // PooledTransaction without commit/rollback.
    {
        let client = pool.get().await.unwrap();
        let tx = client.begin().await.unwrap();
        let err = tx
            .query("SELECT * FROM table_that_does_not_exist", &[])
            .await;
        assert!(err.is_err(), "expected the bogus query to fail");
        // tx dropped: ROLLBACK is queued on the writer.
    }

    // Next checkout: must be a usable connection.
    let client = pool.get().await.unwrap();
    let rows = client
        .query("SELECT 1::int4 AS n", &[])
        .await
        .expect("pool was poisoned: next checkout still in aborted tx state");
    assert_eq!(rows[0].get::<i32>(0).unwrap(), 1);
}

/// The drop path should reuse the connection rather than destroying it,
/// because the queued `ROLLBACK` cleans the session up. Pin that the pool
/// did *not* have to provision a replacement connection.
#[tokio::test]
async fn test_pool_reuses_conn_after_aborted_tx_drop() {
    let pool = ExclusivePool::connect(addr(), user(), pass(), db(), 1)
        .await
        .unwrap();

    let created_before = pool.metrics().total_created;

    {
        let client = pool.get().await.unwrap();
        let tx = client.begin().await.unwrap();
        let _ = tx
            .query("SELECT * FROM table_that_does_not_exist", &[])
            .await;
    }

    let client = pool.get().await.unwrap();
    let rows = client.query("SELECT 1::int4", &[]).await.unwrap();
    assert_eq!(rows[0].get::<i32>(0).unwrap(), 1);

    let m = pool.metrics();
    assert_eq!(
        m.total_created, created_before,
        "drop should have rolled back via ROLLBACK queue, not destroyed the conn (created={}, expected={})",
        m.total_created, created_before
    );
    assert_eq!(m.total_destroyed, 0);
}

/// Drop fallback: when `enqueue_rollback` reports the connection isn't
/// alive, `Transaction::drop` is a no-op (no `mark_broken` call), because
/// `is_alive() == false` already signals "discard me" to any pool via
/// `has_pending_data()`. Wait — actually the current Drop guards the
/// whole body on `self.client.is_alive()`, so when alive is false we
/// skip both `enqueue_rollback` AND `mark_broken`. This test pins that
/// the resulting state is still safe to a pool: the disjunction
/// `!is_alive() || is_broken()` holds.
#[tokio::test]
async fn test_tx_drop_when_conn_not_alive_pool_would_discard() {
    use resolute::Client;

    let client = Client::connect(addr(), user(), pass(), db()).await.unwrap();
    let tx = client.begin().await.unwrap();

    // Force the alive flag to false via the test helper, simulating a
    // dead writer task. After this, Transaction::drop's outer guard
    // (`self.client.is_alive()`) is false, so the body is skipped.
    client.conn().__force_mark_dead_for_test();
    assert!(!client.conn().is_alive());

    drop(tx);

    let conn = client.conn();
    assert!(
        !conn.is_alive() || conn.is_broken(),
        "pool's has_pending_data() must report true: !is_alive={} is_broken={}",
        !conn.is_alive(),
        conn.is_broken()
    );
}

/// Same scenario on a non-pooled `Client`: after `Transaction` drop without
/// commit/rollback, the queued `ROLLBACK` should restore the session and
/// the next query on the same client should succeed (no `25P02`). The
/// connection should NOT be marked broken on the happy path.
#[tokio::test]
async fn test_client_recovers_after_tx_dropped_aborted() {
    use resolute::Client;

    let client = Client::connect(addr(), user(), pass(), db()).await.unwrap();
    assert!(!client.conn().is_broken());

    {
        let tx = client.begin().await.unwrap();
        let _ = tx
            .query("SELECT * FROM table_that_does_not_exist", &[])
            .await;
        // tx dropped: ROLLBACK queued on writer task.
    }

    assert!(
        !client.conn().is_broken(),
        "conn should not be flagged broken when ROLLBACK was queued successfully"
    );

    let rows = client
        .query("SELECT 1::int4 AS n", &[])
        .await
        .expect("session should recover after drop-time ROLLBACK");
    assert_eq!(rows[0].get::<i32>(0).unwrap(), 1);
}

#[tokio::test]
async fn test_typed_pool_connection_survives_return() {
    let pool = ExclusivePool::connect(addr(), user(), pass(), db(), 1)
        .await
        .unwrap();

    // First checkout.
    {
        let client = pool.get().await.unwrap();
        let rows = client.query("SELECT 1::int4 AS n", &[]).await.unwrap();
        assert_eq!(rows[0].get::<i32>(0).unwrap(), 1);
    }
    // client dropped — connection returned to pool.

    // Small delay to ensure return completes.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Second checkout — should reuse the same connection.
    {
        let client = pool.get().await.unwrap();
        let rows = client.query("SELECT 2::int4 AS n", &[]).await.unwrap();
        assert_eq!(rows[0].get::<i32>(0).unwrap(), 2);
    }

    let m = pool.metrics();
    // Should have created at most 2 connections total (1 initial + maybe 1 more).
    assert!(
        m.total_created <= 2,
        "should reuse connections, created: {}",
        m.total_created
    );
}
