//! Basic integration tests for pg-wire.
//! Requires: docker compose up -d (PostgreSQL on port 54322)

use pg_wire::{PgPipeline, WireConn};

const ADDR: &str = "127.0.0.1:54322";
const USER: &str = "postgres";
const PASS: &str = "postgres";
const DB: &str = "postgrest_test";

async fn connect() -> PgPipeline {
    let conn = WireConn::connect(ADDR, USER, PASS, DB).await.unwrap();
    PgPipeline::new(conn)
}

#[tokio::test]
async fn test_connect() {
    let _pg = connect().await;
}

#[tokio::test]
async fn test_simple_query() {
    let mut pg = connect().await;
    pg.simple_query("SELECT 1").await.unwrap();
}

#[tokio::test]
async fn test_parameterized_query() {
    let mut pg = connect().await;
    let rows = pg
        .query(
            "SELECT $1::text AS greeting",
            &[Some(b"hello" as &[u8])],
            &[0], // 0 = let server infer type
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let val = rows[0][0].as_ref().unwrap();
    assert_eq!(std::str::from_utf8(val).unwrap(), "hello");
}

#[tokio::test]
async fn test_query_multiple_rows() {
    let mut pg = connect().await;
    let rows = pg
        .query(
            "SELECT id, name FROM api.authors ORDER BY id",
            &[],
            &[],
        )
        .await
        .unwrap();
    assert!(rows.len() >= 3); // seed data has 3 authors
}

#[tokio::test]
async fn test_query_with_filter() {
    let mut pg = connect().await;
    let rows = pg
        .query(
            "SELECT name FROM api.authors WHERE id = ($1::text)::int4",
            &[Some(b"1" as &[u8])],
            &[0], // text format, server infers type
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let name = std::str::from_utf8(rows[0][0].as_ref().unwrap()).unwrap();
    assert_eq!(name, "Alice");
}

#[tokio::test]
async fn test_statement_cache() {
    let mut pg = connect().await;
    // First call: Parse + Bind + Execute + Sync
    let rows1 = pg
        .query("SELECT 1 AS n", &[], &[])
        .await
        .unwrap();
    // Second call: cache hit — Bind + Execute + Sync (no Parse)
    let rows2 = pg
        .query("SELECT 1 AS n", &[], &[])
        .await
        .unwrap();
    assert_eq!(rows1.len(), 1);
    assert_eq!(rows2.len(), 1);
}

#[tokio::test]
async fn test_pipeline_with_setup() {
    let mut pg = connect().await;
    let rows = pg
        .pipeline_with_setup(
            "SET search_path TO api",
            "SELECT name FROM authors ORDER BY id",
            &[],
            &[],
        )
        .await
        .unwrap();
    assert!(rows.len() >= 3);
    // Reset search path for other tests
    pg.simple_query("SET search_path TO public").await.unwrap();
}

#[tokio::test]
async fn test_pipeline_transaction() {
    let mut pg = connect().await;
    let rows = pg
        .pipeline_transaction(
            "BEGIN; SET LOCAL ROLE web_anon",
            "SELECT coalesce(json_agg(t), '[]')::text FROM (SELECT id, name FROM api.authors ORDER BY id) t",
            &[],
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let json = std::str::from_utf8(rows[0][0].as_ref().unwrap()).unwrap();
    assert!(json.contains("Alice"));
}

#[tokio::test]
async fn test_pipeline_transaction_with_jwt() {
    let mut pg = connect().await;
    let rows = pg
        .pipeline_transaction(
            "BEGIN; SET LOCAL ROLE test_user; SELECT set_config('request.jwt.claims', '{\"role\":\"test_user\"}', true)",
            "SELECT coalesce(json_agg(t), '[]')::text FROM (SELECT id, title, status FROM api.articles ORDER BY id) t",
            &[],
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let json = std::str::from_utf8(rows[0][0].as_ref().unwrap()).unwrap();
    // test_user should see all articles (including drafts)
    assert!(json.contains("draft") || json.contains("Draft"));
}

#[tokio::test]
async fn test_error_recovery() {
    let mut pg = connect().await;
    // Query that errors (table doesn't exist).
    let result = pg.query("SELECT * FROM nonexistent_table", &[], &[]).await;
    assert!(result.is_err());
    // Connection should still be usable after error.
    let rows = pg.query("SELECT 1 AS n", &[], &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
}

#[tokio::test]
async fn test_null_parameter() {
    let mut pg = connect().await;
    let rows = pg
        .query(
            "SELECT $1::text AS val",
            &[None],
            &[0],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(rows[0][0].is_none()); // NULL result
}
