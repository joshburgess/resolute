//! Tests for the async writer/reader connection.
//! Requires: docker compose up -d (PostgreSQL on port 54322)

use pg_wired::{AsyncConn, WireConn};

mod test_env;
use test_env::{addr, db, pass, user};

async fn connect() -> AsyncConn {
    let conn = WireConn::connect(addr(), user(), pass(), db())
        .await
        .unwrap();
    AsyncConn::new(conn)
}

#[tokio::test]
async fn test_async_simple_query() {
    let ac = connect().await;
    let rows = ac.exec_query("SELECT 1 AS n", &[], &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(std::str::from_utf8(rows[0].cell(0).unwrap()).unwrap(), "1");
}

#[tokio::test]
async fn test_async_parameterized() {
    let ac = connect().await;
    let rows = ac
        .exec_query("SELECT $1::text AS val", &[Some(b"hello" as &[u8])], &[0])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        std::str::from_utf8(rows[0].cell(0).unwrap()).unwrap(),
        "hello"
    );
}

#[tokio::test]
async fn test_async_multiple_rows() {
    let ac = connect().await;
    let rows = ac
        .exec_query("SELECT id, name FROM api.authors ORDER BY id", &[], &[])
        .await
        .unwrap();
    assert!(rows.len() >= 3);
}

#[tokio::test]
async fn test_async_pipeline_transaction() {
    let ac = connect().await;
    let rows = ac
        .exec_transaction(
            "BEGIN; SET LOCAL ROLE web_anon",
            "SELECT coalesce(json_agg(t), '[]')::text FROM (SELECT id, name FROM api.authors ORDER BY id) t",
            &[],
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let json = std::str::from_utf8(rows[0].cell(0).unwrap()).unwrap();
    assert!(json.contains("Alice"));
}

#[tokio::test]
async fn test_async_pipeline_with_jwt() {
    let ac = connect().await;
    let rows = ac
        .exec_transaction(
            "BEGIN; SET LOCAL ROLE test_user; SELECT set_config('request.jwt.claims', '{\"role\":\"test_user\"}', true)",
            "SELECT coalesce(json_agg(t), '[]')::text FROM (SELECT title, status FROM api.articles ORDER BY id) t",
            &[],
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let json = std::str::from_utf8(rows[0].cell(0).unwrap()).unwrap();
    assert!(json.contains("Draft"));
}

#[tokio::test]
async fn test_async_statement_cache() {
    let ac = connect().await;
    // First call — cache miss (Parse + Bind + Execute + Sync).
    let r1 = ac.exec_query("SELECT 42 AS n", &[], &[]).await.unwrap();
    // Second call — cache hit (Bind + Execute + Sync only).
    let r2 = ac.exec_query("SELECT 42 AS n", &[], &[]).await.unwrap();
    assert_eq!(r1.len(), 1);
    assert_eq!(r2.len(), 1);
}

#[tokio::test]
async fn test_async_concurrent_queries() {
    let ac = std::sync::Arc::new(connect().await);

    // Fire 10 concurrent queries on the same connection.
    let mut handles = Vec::new();
    for i in 0..10 {
        let ac = ac.clone();
        handles.push(tokio::spawn(async move {
            let sql = format!("SELECT {} AS n", i);
            let rows = ac.exec_query(&sql, &[], &[]).await.unwrap();
            let val = std::str::from_utf8(rows[0].cell(0).unwrap())
                .unwrap()
                .parse::<i32>()
                .unwrap();
            assert_eq!(val, i);
        }));
    }

    for h in handles {
        h.await.unwrap();
    }
}

// ---------------------------------------------------------------------------
// Error handling and recovery
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_async_error_recovery() {
    let ac = connect().await;

    // Query that errors (nonexistent table).
    let result = ac
        .exec_query("SELECT * FROM nonexistent_xyz_table", &[], &[])
        .await;
    assert!(result.is_err());

    // Connection should still work after error.
    let rows = ac.exec_query("SELECT 1 AS n", &[], &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(std::str::from_utf8(rows[0].cell(0).unwrap()).unwrap(), "1");
}

#[tokio::test]
async fn test_async_multiple_errors_then_recovery() {
    let ac = connect().await;

    for _ in 0..5 {
        let _ = ac.exec_query("SELECT * FROM no_such_table", &[], &[]).await;
    }

    // Should still work.
    let rows = ac
        .exec_query("SELECT 'ok' AS status", &[], &[])
        .await
        .unwrap();
    assert_eq!(std::str::from_utf8(rows[0].cell(0).unwrap()).unwrap(), "ok");
}

#[tokio::test]
async fn test_async_transaction_error_recovery() {
    let ac = connect().await;

    // Transaction with error in data query.
    let result = ac
        .exec_transaction(
            "BEGIN; SET LOCAL ROLE web_anon",
            "SELECT * FROM no_such_table_xyz",
            &[],
            &[],
        )
        .await;
    assert!(result.is_err());

    // Connection should recover.
    let rows = ac.exec_query("SELECT 42 AS n", &[], &[]).await.unwrap();
    assert_eq!(std::str::from_utf8(rows[0].cell(0).unwrap()).unwrap(), "42");
}

// ---------------------------------------------------------------------------
// Edge cases
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_async_empty_result() {
    let ac = connect().await;
    let rows = ac
        .exec_query(
            "SELECT id FROM api.authors WHERE id = ($1::text)::int4",
            &[Some(b"99999" as &[u8])],
            &[0],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 0);
}

#[tokio::test]
async fn test_async_null_result() {
    let ac = connect().await;
    let rows = ac
        .exec_query("SELECT $1::text AS val", &[None], &[0])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!((rows[0].try_cell(0) == Some(None)));
}

#[tokio::test]
async fn test_async_large_result() {
    let ac = connect().await;
    let rows = ac
        .exec_query("SELECT generate_series(1, 500)::text AS n", &[], &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 500);
}

#[tokio::test]
async fn test_async_multiple_columns() {
    let ac = connect().await;
    let rows = ac
        .exec_query(
            "SELECT 'a'::text AS c1, 'b'::text AS c2, 'c'::text AS c3",
            &[],
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(std::str::from_utf8(rows[0].cell(0).unwrap()).unwrap(), "a");
    assert_eq!(std::str::from_utf8(rows[0].cell(1).unwrap()).unwrap(), "b");
    assert_eq!(std::str::from_utf8(rows[0].cell(2).unwrap()).unwrap(), "c");
}

#[tokio::test]
async fn test_async_statement_cache_eviction() {
    let ac = connect().await;

    // Execute 260 unique queries to trigger cache eviction (limit is 256).
    for i in 0..260 {
        let sql = format!("SELECT {i} AS n");
        let rows = ac.exec_query(&sql, &[], &[]).await.unwrap();
        assert_eq!(rows.len(), 1);
    }

    // After eviction, re-execute earlier queries — should still work (re-parsed).
    let rows = ac.exec_query("SELECT 0 AS n", &[], &[]).await.unwrap();
    assert_eq!(std::str::from_utf8(rows[0].cell(0).unwrap()).unwrap(), "0");
}

#[tokio::test]
async fn test_async_sequential_transactions() {
    let ac = connect().await;

    // Multiple transactions in sequence on the same connection.
    for i in 0..5 {
        let rows = ac
            .exec_transaction(
                "BEGIN; SET LOCAL ROLE web_anon",
                &format!("SELECT {i} AS n"),
                &[],
                &[],
            )
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            std::str::from_utf8(rows[0].cell(0).unwrap()).unwrap(),
            i.to_string()
        );
    }
}

/// `enqueue_rollback` succeeds on a live connection. The queued ROLLBACK
/// must execute before any subsequent request: if the connection is in an
/// aborted-tx state (`25P02`) when `enqueue_rollback` is called, the next
/// query should succeed because the writer task processes the ROLLBACK
/// first (FIFO).
#[tokio::test]
async fn test_async_enqueue_rollback_clears_aborted_tx() {
    let ac = connect().await;

    ac.exec_query("BEGIN", &[], &[]).await.unwrap();
    let err = ac
        .exec_query("SELECT * FROM table_does_not_exist_xyz", &[], &[])
        .await;
    assert!(err.is_err(), "bogus query should fail");
    // Connection is now in 25P02 aborted state.

    assert!(
        ac.enqueue_rollback(),
        "enqueue_rollback should succeed on a live connection"
    );

    // Next query goes onto the writer queue after ROLLBACK and must succeed.
    let rows = ac
        .exec_query("SELECT 1::int4", &[], &[])
        .await
        .expect("session should be clean after queued ROLLBACK");
    assert_eq!(std::str::from_utf8(rows[0].cell(0).unwrap()).unwrap(), "1");
    assert!(!ac.is_broken());
}

/// `enqueue_rollback` short-circuits to `false` when `is_alive()` returns
/// false. Drop paths use this signal to fall back to `mark_broken` so a
/// pool discards the connection rather than handing back a session in an
/// indeterminate state.
///
/// We use `__force_mark_dead_for_test` to avoid racing against the real
/// writer-task exit, which is timing-dependent on socket close detection.
#[tokio::test]
async fn test_async_enqueue_rollback_returns_false_when_not_alive() {
    let ac = connect().await;
    assert!(ac.is_alive());
    assert!(
        ac.enqueue_rollback(),
        "baseline: enqueue should succeed while alive"
    );

    ac.__force_mark_dead_for_test();
    assert!(!ac.is_alive());
    assert!(
        !ac.enqueue_rollback(),
        "enqueue_rollback must return false once is_alive() is false so callers fall back to mark_broken"
    );
}

// The `try_send` failure branches of enqueue_rollback (full channel and
// closed channel) are covered by unit tests in `src/async_conn.rs`, which
// drive the inner `try_enqueue_rollback` helper directly with a tiny
// synthetic mpsc channel. Filling the real connection's 256-slot request
// channel through the public `AsyncConn` API isn't practical, so the
// helper was extracted specifically to make those branches testable.

#[tokio::test]
async fn test_async_high_concurrency() {
    let ac = std::sync::Arc::new(connect().await);

    // 50 concurrent queries.
    let mut handles = Vec::new();
    for i in 0..50 {
        let ac = ac.clone();
        handles.push(tokio::spawn(async move {
            let sql = format!("SELECT {i} AS n");
            let rows = ac.exec_query(&sql, &[], &[]).await.unwrap();
            let val = std::str::from_utf8(rows[0].cell(0).unwrap())
                .unwrap()
                .parse::<i32>()
                .unwrap();
            assert_eq!(val, i);
        }));
    }

    for h in handles {
        h.await.unwrap();
    }
}
