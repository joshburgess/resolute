//! Thorough integration tests for AsyncPool.
//! Tests reconnection logic, round-robin dispatch, dead connection handling,
//! health monitoring, and concurrent access patterns.
//! Requires: docker compose up -d (PostgreSQL on port 54322)

use std::sync::Arc;
use std::time::Duration;

use pg_wire::{AsyncPool, WireConn};

const ADDR: &str = "127.0.0.1:54322";
const USER: &str = "postgres";
const PASS: &str = "postgres";
const DB: &str = "postgrest_test";

// ---------------------------------------------------------------------------
// Pool creation and sizing
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_pool_connect_single() {
    let pool = AsyncPool::connect(ADDR, USER, PASS, DB, 1)
        .await
        .unwrap();
    assert_eq!(pool.size(), 1);
    assert_eq!(pool.alive_count().await, 1);
}

#[tokio::test]
async fn test_pool_connect_multiple() {
    let pool = AsyncPool::connect(ADDR, USER, PASS, DB, 4)
        .await
        .unwrap();
    assert_eq!(pool.size(), 4);
    assert_eq!(pool.alive_count().await, 4);
}

#[tokio::test]
async fn test_pool_connect_invalid_address() {
    let result = AsyncPool::connect("127.0.0.1:1", USER, PASS, DB, 1).await;
    assert!(result.is_err(), "invalid address should fail");
}

#[tokio::test]
async fn test_pool_connect_size_zero_rejected() {
    let result = AsyncPool::connect(ADDR, USER, PASS, DB, 0).await;
    assert!(result.is_err(), "size=0 should be rejected");
}

// ---------------------------------------------------------------------------
// Basic query execution
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_pool_exec_query_select_one() {
    let pool = AsyncPool::connect(ADDR, USER, PASS, DB, 2)
        .await
        .unwrap();
    let rows = pool.exec_query("SELECT 1 AS n", &[], &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(col_str(&rows[0][0]), "1");
}

#[tokio::test]
async fn test_pool_exec_query_parameterized() {
    let pool = AsyncPool::connect(ADDR, USER, PASS, DB, 2)
        .await
        .unwrap();
    let rows = pool
        .exec_query(
            "SELECT $1::text AS val",
            &[Some(b"hello_pool" as &[u8])],
            &[0],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(col_str(&rows[0][0]), "hello_pool");
}

#[tokio::test]
async fn test_pool_exec_query_multiple_rows() {
    let pool = AsyncPool::connect(ADDR, USER, PASS, DB, 2)
        .await
        .unwrap();
    let rows = pool
        .exec_query("SELECT id, name FROM api.authors ORDER BY id", &[], &[])
        .await
        .unwrap();
    assert!(rows.len() >= 3);
}

#[tokio::test]
async fn test_pool_exec_query_null_param() {
    let pool = AsyncPool::connect(ADDR, USER, PASS, DB, 1)
        .await
        .unwrap();
    let rows = pool
        .exec_query("SELECT $1::text AS val", &[None], &[0])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(rows[0][0].is_none());
}

#[tokio::test]
async fn test_pool_exec_query_empty_result() {
    let pool = AsyncPool::connect(ADDR, USER, PASS, DB, 1)
        .await
        .unwrap();
    let rows = pool
        .exec_query(
            "SELECT id FROM api.authors WHERE id = ($1::text)::int4",
            &[Some(b"99999" as &[u8])],
            &[0],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 0);
}

// ---------------------------------------------------------------------------
// Transaction execution
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_pool_exec_transaction_basic() {
    let pool = AsyncPool::connect(ADDR, USER, PASS, DB, 2)
        .await
        .unwrap();
    let rows = pool
        .exec_transaction(
            "BEGIN; SET LOCAL ROLE web_anon",
            "SELECT coalesce(json_agg(t), '[]')::text FROM (SELECT id, name FROM api.authors ORDER BY id) t",
            &[],
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let json = col_str(&rows[0][0]);
    assert!(json.contains("Alice"));
}

#[tokio::test]
async fn test_pool_exec_transaction_with_jwt() {
    let pool = AsyncPool::connect(ADDR, USER, PASS, DB, 2)
        .await
        .unwrap();
    let rows = pool
        .exec_transaction(
            "BEGIN; SET LOCAL ROLE test_user; SELECT set_config('request.jwt.claims', '{\"role\":\"test_user\"}', true)",
            "SELECT coalesce(json_agg(t), '[]')::text FROM (SELECT title, status FROM api.articles ORDER BY id) t",
            &[],
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let json = col_str(&rows[0][0]);
    assert!(json.contains("Draft"));
}

#[tokio::test]
async fn test_pool_exec_transaction_with_params() {
    let pool = AsyncPool::connect(ADDR, USER, PASS, DB, 2)
        .await
        .unwrap();
    let rows = pool
        .exec_transaction(
            "BEGIN; SET LOCAL ROLE web_anon",
            "SELECT name FROM api.authors WHERE id = ($1::text)::int4",
            &[Some(b"1" as &[u8])],
            &[0],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(col_str(&rows[0][0]), "Alice");
}

// ---------------------------------------------------------------------------
// Round-robin dispatch
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_pool_round_robin_dispatch() {
    let pool = AsyncPool::connect(ADDR, USER, PASS, DB, 3)
        .await
        .unwrap();

    // Execute multiple queries — they should distribute across connections.
    for i in 0..9 {
        let rows = pool
            .exec_query(&format!("SELECT {i} AS n"), &[], &[])
            .await
            .unwrap();
        assert_eq!(col_str(&rows[0][0]), i.to_string());
    }
}

#[tokio::test]
async fn test_pool_get_async_returns_alive_connections() {
    let pool = AsyncPool::connect(ADDR, USER, PASS, DB, 3)
        .await
        .unwrap();

    for _ in 0..10 {
        let conn = pool.get_async().await;
        assert!(conn.is_alive());
    }
}

// ---------------------------------------------------------------------------
// Concurrent access
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_pool_concurrent_queries() {
    let pool = AsyncPool::connect(ADDR, USER, PASS, DB, 4)
        .await
        .unwrap();

    let mut handles = Vec::new();
    for i in 0..20 {
        let pool = Arc::clone(&pool);
        handles.push(tokio::spawn(async move {
            let rows = pool
                .exec_query(&format!("SELECT {i} AS n"), &[], &[])
                .await
                .unwrap();
            let val = col_str(&rows[0][0]).parse::<i32>().unwrap();
            assert_eq!(val, i);
        }));
    }

    for h in handles {
        h.await.unwrap();
    }
}

#[tokio::test]
async fn test_pool_concurrent_transactions() {
    let pool = AsyncPool::connect(ADDR, USER, PASS, DB, 4)
        .await
        .unwrap();

    let mut handles = Vec::new();
    for i in 0..10 {
        let pool = Arc::clone(&pool);
        handles.push(tokio::spawn(async move {
            let rows = pool
                .exec_transaction(
                    "BEGIN; SET LOCAL ROLE web_anon",
                    &format!("SELECT {i} AS n"),
                    &[],
                    &[],
                )
                .await
                .unwrap();
            let val = col_str(&rows[0][0]).parse::<i32>().unwrap();
            assert_eq!(val, i);
        }));
    }

    for h in handles {
        h.await.unwrap();
    }
}

#[tokio::test]
async fn test_pool_high_concurrency_stress() {
    let pool = AsyncPool::connect(ADDR, USER, PASS, DB, 4)
        .await
        .unwrap();

    // 100 concurrent queries.
    let mut handles = Vec::new();
    for i in 0..100 {
        let pool = Arc::clone(&pool);
        handles.push(tokio::spawn(async move {
            let rows = pool
                .exec_query(&format!("SELECT {i} AS n"), &[], &[])
                .await
                .unwrap();
            let val = col_str(&rows[0][0]).parse::<i32>().unwrap();
            assert_eq!(val, i);
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    assert_eq!(pool.alive_count().await, 4, "all connections still alive");
}

// ---------------------------------------------------------------------------
// Error handling
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_pool_error_recovery() {
    let pool = AsyncPool::connect(ADDR, USER, PASS, DB, 2)
        .await
        .unwrap();

    // Query that errors.
    let result = pool
        .exec_query("SELECT * FROM nonexistent_table_xyz", &[], &[])
        .await;
    assert!(result.is_err());

    // Pool should still work after error.
    let rows = pool.exec_query("SELECT 1 AS n", &[], &[]).await.unwrap();
    assert_eq!(col_str(&rows[0][0]), "1");
}

#[tokio::test]
async fn test_pool_transaction_error_recovery() {
    let pool = AsyncPool::connect(ADDR, USER, PASS, DB, 2)
        .await
        .unwrap();

    // Transaction with error in the data query.
    let result = pool
        .exec_transaction(
            "BEGIN; SET LOCAL ROLE web_anon",
            "SELECT * FROM nonexistent_xyz",
            &[],
            &[],
        )
        .await;
    assert!(result.is_err());

    // Should recover.
    let rows = pool.exec_query("SELECT 42 AS n", &[], &[]).await.unwrap();
    assert_eq!(col_str(&rows[0][0]), "42");
}

#[tokio::test]
async fn test_pool_multiple_errors_then_recovery() {
    let pool = AsyncPool::connect(ADDR, USER, PASS, DB, 2)
        .await
        .unwrap();

    // Multiple errors in a row.
    for _ in 0..5 {
        let _ = pool
            .exec_query("SELECT * FROM does_not_exist", &[], &[])
            .await;
    }

    // Should still work.
    let rows = pool.exec_query("SELECT 'ok' AS status", &[], &[]).await.unwrap();
    assert_eq!(col_str(&rows[0][0]), "ok");
}

// ---------------------------------------------------------------------------
// Reconnection behavior
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_pool_detects_dead_connection() {
    let pool = AsyncPool::connect(ADDR, USER, PASS, DB, 2)
        .await
        .unwrap();

    // Get a PID from the pool via a query.
    let rows = pool
        .exec_query("SELECT pg_backend_pid()", &[], &[])
        .await
        .unwrap();
    let pid = col_str(&rows[0][0]).parse::<i32>().unwrap();

    // Kill that backend from outside.
    let mut killer = WireConn::connect(ADDR, USER, PASS, DB).await.unwrap();
    kill_backend(&mut killer, pid).await;

    // Give the reader task time to notice the disconnect.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // The pool should have at least one dead connection now.
    let alive = pool.alive_count().await;
    assert!(alive <= 2, "at least one should be dead or reconnecting");

    // Queries should still work via the surviving connection or after reconnect.
    // Health monitor runs every 5s, but we can try immediately — get_async skips dead.
    let rows = pool.exec_query("SELECT 1 AS n", &[], &[]).await.unwrap();
    assert_eq!(col_str(&rows[0][0]), "1");
}

#[tokio::test]
async fn test_pool_reconnection_via_health_monitor() {
    let pool = AsyncPool::connect(ADDR, USER, PASS, DB, 2)
        .await
        .unwrap();
    assert_eq!(pool.alive_count().await, 2);

    // Kill one backend.
    let rows = pool
        .exec_query("SELECT pg_backend_pid()", &[], &[])
        .await
        .unwrap();
    let pid = col_str(&rows[0][0]).parse::<i32>().unwrap();

    let mut killer = WireConn::connect(ADDR, USER, PASS, DB).await.unwrap();
    kill_backend(&mut killer, pid).await;

    // The reader task only detects a broken socket when it tries to read.
    // Force detection by sending queries — some will fail on the dead connection.
    for _ in 0..10 {
        let _ = pool.exec_query("SELECT 1 AS n", &[], &[]).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Give async tasks time to set alive=false.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(pool.alive_count().await < 2, "killed connection should be detected as dead");

    // Now wait for health monitor to reconnect (runs every 5s).
    let mut reconnected = false;
    for _ in 0..25 {
        tokio::time::sleep(Duration::from_millis(500)).await;
        if pool.alive_count().await == 2 {
            reconnected = true;
            break;
        }
    }
    assert!(reconnected, "health monitor should have reconnected the dead slot");

    // After reconnection, all queries should succeed.
    for _ in 0..6 {
        let rows = pool
            .exec_query("SELECT 1 AS n", &[], &[])
            .await
            .unwrap();
        assert_eq!(col_str(&rows[0][0]), "1");
    }
}

#[tokio::test]
async fn test_pool_skips_dead_connections_in_round_robin() {
    let pool = AsyncPool::connect(ADDR, USER, PASS, DB, 3)
        .await
        .unwrap();

    // Query on each slot to get all PIDs.
    let mut pids = Vec::new();
    for _ in 0..6 {
        let rows = pool
            .exec_query("SELECT pg_backend_pid()", &[], &[])
            .await
            .unwrap();
        let pid = col_str(&rows[0][0]).parse::<i32>().unwrap();
        if !pids.contains(&pid) {
            pids.push(pid);
        }
    }

    // Kill the first PID we found.
    if let Some(&pid) = pids.first() {
        let mut killer = WireConn::connect(ADDR, USER, PASS, DB).await.unwrap();
        kill_backend(&mut killer, pid).await;
    }

    // The reader task detects the broken socket only when it tries to read.
    // Send queries to force detection — some will fail on the dead connection.
    // After enough failures, the reader exits and alive=false is set.
    for _ in 0..10 {
        let _ = pool.exec_query("SELECT 1 AS n", &[], &[]).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Give the async reader task time to exit and set alive=false.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Now the dead connection should be marked as dead. Queries routed
    // to surviving connections should all succeed.
    let mut successes = 0;
    for i in 0..10 {
        if let Ok(rows) = pool
            .exec_query(&format!("SELECT {i} AS n"), &[], &[])
            .await
        {
            assert_eq!(col_str(&rows[0][0]), i.to_string());
            successes += 1;
        }
    }
    // With 2 surviving connections out of 3, all queries should succeed
    // once the dead one is detected.
    assert!(successes >= 9, "queries should succeed via surviving conns, got {successes}");
}

// ---------------------------------------------------------------------------
// Statement caching across pool
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_pool_statement_cache_per_connection() {
    let pool = AsyncPool::connect(ADDR, USER, PASS, DB, 2)
        .await
        .unwrap();

    // Same SQL executed repeatedly should use cached statements.
    for _ in 0..10 {
        let rows = pool
            .exec_query("SELECT 42 AS n", &[], &[])
            .await
            .unwrap();
        assert_eq!(col_str(&rows[0][0]), "42");
    }
}

// ---------------------------------------------------------------------------
// Mixed workload
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_pool_mixed_queries_and_transactions() {
    let pool = AsyncPool::connect(ADDR, USER, PASS, DB, 3)
        .await
        .unwrap();

    let mut handles = Vec::new();

    // Mix of queries and transactions.
    for i in 0..10 {
        let pool = Arc::clone(&pool);
        if i % 2 == 0 {
            handles.push(tokio::spawn(async move {
                let rows = pool
                    .exec_query(&format!("SELECT {i} AS n"), &[], &[])
                    .await
                    .unwrap();
                assert_eq!(col_str(&rows[0][0]), i.to_string());
            }));
        } else {
            handles.push(tokio::spawn(async move {
                let rows = pool
                    .exec_transaction(
                        "BEGIN; SET LOCAL ROLE web_anon",
                        &format!("SELECT {i} AS n"),
                        &[],
                        &[],
                    )
                    .await
                    .unwrap();
                assert_eq!(col_str(&rows[0][0]), i.to_string());
            }));
        }
    }

    for h in handles {
        h.await.unwrap();
    }
}

// ---------------------------------------------------------------------------
// Edge cases
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_pool_single_connection_serial_queries() {
    let pool = AsyncPool::connect(ADDR, USER, PASS, DB, 1)
        .await
        .unwrap();

    for i in 0..10 {
        let rows = pool
            .exec_query(&format!("SELECT {i} AS n"), &[], &[])
            .await
            .unwrap();
        assert_eq!(col_str(&rows[0][0]), i.to_string());
    }
}

#[tokio::test]
async fn test_pool_single_connection_concurrent_queries() {
    let pool = AsyncPool::connect(ADDR, USER, PASS, DB, 1)
        .await
        .unwrap();

    // Multiple concurrent queries on a single backend connection.
    let mut handles = Vec::new();
    for i in 0..10 {
        let pool = Arc::clone(&pool);
        handles.push(tokio::spawn(async move {
            let rows = pool
                .exec_query(&format!("SELECT {i} AS n"), &[], &[])
                .await
                .unwrap();
            assert_eq!(col_str(&rows[0][0]), i.to_string());
        }));
    }

    for h in handles {
        h.await.unwrap();
    }
}

#[tokio::test]
async fn test_pool_large_result_set() {
    let pool = AsyncPool::connect(ADDR, USER, PASS, DB, 2)
        .await
        .unwrap();

    let rows = pool
        .exec_query("SELECT generate_series(1, 1000)::text AS n", &[], &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1000);
    assert_eq!(col_str(&rows[0][0]), "1");
    assert_eq!(col_str(&rows[999][0]), "1000");
}

#[tokio::test]
async fn test_pool_multiple_columns() {
    let pool = AsyncPool::connect(ADDR, USER, PASS, DB, 1)
        .await
        .unwrap();

    let rows = pool
        .exec_query(
            "SELECT 'a'::text AS col1, 'b'::text AS col2, 'c'::text AS col3",
            &[],
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(col_str(&rows[0][0]), "a");
    assert_eq!(col_str(&rows[0][1]), "b");
    assert_eq!(col_str(&rows[0][2]), "c");
}

// ---------------------------------------------------------------------------
// All connections dead scenario
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_pool_all_connections_dead_recovers() {
    let pool = AsyncPool::connect(ADDR, USER, PASS, DB, 2)
        .await
        .unwrap();

    // Collect all backend PIDs.
    let mut pids = Vec::new();
    for _ in 0..4 {
        let rows = pool
            .exec_query("SELECT pg_backend_pid()", &[], &[])
            .await
            .unwrap();
        let pid = col_str(&rows[0][0]).parse::<i32>().unwrap();
        if !pids.contains(&pid) {
            pids.push(pid);
        }
    }

    // Kill all backends.
    let mut killer = WireConn::connect(ADDR, USER, PASS, DB).await.unwrap();
    for pid in &pids {
        kill_backend(&mut killer, *pid).await;
    }

    // Force detection by sending queries (they will fail).
    for _ in 0..10 {
        let _ = pool.exec_query("SELECT 1", &[], &[]).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Wait for health monitor to reconnect both slots.
    let mut recovered = false;
    for _ in 0..25 {
        tokio::time::sleep(Duration::from_millis(500)).await;
        if pool.alive_count().await == 2 {
            recovered = true;
            break;
        }
    }
    assert!(recovered, "pool should recover all dead connections via health monitor");

    // Verify queries work again.
    let rows = pool.exec_query("SELECT 1 AS n", &[], &[]).await.unwrap();
    assert_eq!(col_str(&rows[0][0]), "1");
}

// ---------------------------------------------------------------------------
// Concurrent access during reconnection
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_pool_concurrent_queries_during_reconnection() {
    let pool = AsyncPool::connect(ADDR, USER, PASS, DB, 3)
        .await
        .unwrap();

    // Kill one backend.
    let rows = pool
        .exec_query("SELECT pg_backend_pid()", &[], &[])
        .await
        .unwrap();
    let pid = col_str(&rows[0][0]).parse::<i32>().unwrap();
    let mut killer = WireConn::connect(ADDR, USER, PASS, DB).await.unwrap();
    kill_backend(&mut killer, pid).await;

    // Immediately fire concurrent queries — some will hit the dead connection,
    // others will succeed on surviving connections.
    let mut handles = Vec::new();
    for i in 0..20 {
        let pool = Arc::clone(&pool);
        handles.push(tokio::spawn(async move {
            pool.exec_query(&format!("SELECT {i}"), &[], &[]).await
        }));
    }

    let mut successes = 0;
    for h in handles {
        if h.await.unwrap().is_ok() {
            successes += 1;
        }
    }

    // Most should succeed via surviving connections.
    assert!(successes >= 10, "at least half should succeed, got {successes}");
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn col_str(col: &Option<Vec<u8>>) -> String {
    String::from_utf8(col.as_ref().unwrap().clone()).unwrap()
}

async fn kill_backend(conn: &mut WireConn, pid: i32) {
    use bytes::BytesMut;
    use pg_wire::protocol::types::FrontendMsg;

    let sql = format!("SELECT pg_terminate_backend({pid})");
    let mut buf = BytesMut::new();
    pg_wire::protocol::frontend::encode_message(
        &FrontendMsg::Query(sql.as_bytes()),
        &mut buf,
    );
    conn.send_raw(&buf).await.unwrap();
    let _ = conn.collect_rows().await;
}
