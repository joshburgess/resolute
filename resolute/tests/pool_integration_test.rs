//! Tests for TypedPool (pg-pool + resolute integration).
//! Requires: docker compose up -d (PostgreSQL on port 54322)

#![allow(dead_code)]

use resolute::{Executor, TypedPool};

const ADDR: &str = "127.0.0.1:54322";
const USER: &str = "postgres";
const PASS: &str = "postgres";
const DB: &str = "postgrest_test";

#[tokio::test]
async fn test_typed_pool_connect() {
    let pool = TypedPool::connect(ADDR, USER, PASS, DB, 3).await.unwrap();
    let m = pool.metrics();
    assert_eq!(m.total, 1); // min_idle default = 1
}

#[tokio::test]
async fn test_typed_pool_query() {
    let pool = TypedPool::connect(ADDR, USER, PASS, DB, 3).await.unwrap();
    let client = pool.get().await.unwrap();
    let rows = client.query("SELECT 42::int4 AS n", &[]).await.unwrap();
    assert_eq!(rows[0].get::<i32>(0).unwrap(), 42);
}

#[tokio::test]
async fn test_typed_pool_parameterized() {
    let pool = TypedPool::connect(ADDR, USER, PASS, DB, 3).await.unwrap();
    let client = pool.get().await.unwrap();
    let rows = client
        .query("SELECT name FROM api.authors WHERE id = $1", &[&1i32])
        .await
        .unwrap();
    assert_eq!(rows[0].get::<String>(0).unwrap(), "Alice");
}

#[tokio::test]
async fn test_typed_pool_reuse() {
    let pool = TypedPool::connect(ADDR, USER, PASS, DB, 3).await.unwrap();

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

    let pool = TypedPool::connect(ADDR, USER, PASS, DB, 3).await.unwrap();
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
    let pool = TypedPool::connect(ADDR, USER, PASS, DB, 2).await.unwrap();
    pool.drain().await;
    assert_eq!(pool.metrics().total, 0);
}

#[tokio::test]
async fn test_typed_pool_query_named() {
    let pool = TypedPool::connect(ADDR, USER, PASS, DB, 3).await.unwrap();
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
    let pool = TypedPool::connect(ADDR, USER, PASS, DB, 3).await.unwrap();
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

#[tokio::test]
async fn test_typed_pool_connection_survives_return() {
    let pool = TypedPool::connect(ADDR, USER, PASS, DB, 1).await.unwrap();

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
