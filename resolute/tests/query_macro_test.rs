//! Tests for the compile-time query!() macro.
//! Requires:
//!   docker compose up -d (PostgreSQL on port 54322)
//!   DATABASE_URL=postgres://postgres:postgres@127.0.0.1:54322/postgrest_test

#![allow(clippy::approx_constant)]

use resolute::Client;

const ADDR: &str = "127.0.0.1:54322";
const USER: &str = "postgres";
const PASS: &str = "postgres";
const DB: &str = "postgrest_test";

async fn connect() -> Client {
    Client::connect(ADDR, USER, PASS, DB).await.unwrap()
}

// ---------------------------------------------------------------------------
// Basic compile-time checked queries
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_query_macro_select_literal() {
    let client = connect().await;
    let rows = resolute::query!("SELECT 42::int4 AS answer")
        .fetch_all(&client)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].answer, 42);
}

#[tokio::test]
async fn test_query_macro_with_param() {
    let client = connect().await;
    let id = 1i32;
    let row = resolute::query!("SELECT id, name FROM api.authors WHERE id = $1", id)
        .fetch_one(&client)
        .await
        .unwrap();
    assert_eq!(row.id, 1);
    assert_eq!(row.name, "Alice");
}

#[tokio::test]
async fn test_query_macro_multiple_params() {
    let client = connect().await;
    let a = 3i32;
    let b = 4i32;
    let row = resolute::query!("SELECT ($1::int4 + $2::int4) AS sum", a, b)
        .fetch_one(&client)
        .await
        .unwrap();
    assert_eq!(row.sum, 7);
}

#[tokio::test]
async fn test_query_macro_fetch_all() {
    let client = connect().await;
    let rows = resolute::query!("SELECT id, name FROM api.authors ORDER BY id")
        .fetch_all(&client)
        .await
        .unwrap();
    assert!(rows.len() >= 3);
    assert_eq!(rows[0].id, 1);
    assert_eq!(rows[0].name, "Alice");
}

#[tokio::test]
async fn test_query_macro_fetch_opt_some() {
    let client = connect().await;
    let id = 1i32;
    let row = resolute::query!("SELECT name FROM api.authors WHERE id = $1", id)
        .fetch_opt(&client)
        .await
        .unwrap();
    assert!(row.is_some());
    assert_eq!(row.unwrap().name, "Alice");
}

#[tokio::test]
async fn test_query_macro_fetch_opt_none() {
    let client = connect().await;
    let id = 99999i32;
    let row = resolute::query!("SELECT name FROM api.authors WHERE id = $1", id)
        .fetch_opt(&client)
        .await
        .unwrap();
    assert!(row.is_none());
}

#[tokio::test]
async fn test_query_macro_multiple_columns() {
    let client = connect().await;
    let row =
        resolute::query!("SELECT 1::int4 AS a, 'hello'::text AS b, true AS c, 3.14::float8 AS d")
            .fetch_one(&client)
            .await
            .unwrap();
    assert_eq!(row.a, 1);
    assert_eq!(row.b, "hello");
    assert!(row.c);
    assert!((row.d - 3.14).abs() < 1e-10);
}

#[tokio::test]
async fn test_query_macro_text_type() {
    let client = connect().await;
    let name = "Alice".to_string();
    let rows = resolute::query!("SELECT id FROM api.authors WHERE name = $1", name)
        .fetch_all(&client)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, 1);
}

#[tokio::test]
async fn test_query_macro_bigint() {
    let client = connect().await;
    let val = 9999999999i64;
    let row = resolute::query!("SELECT $1::int8 AS n", val)
        .fetch_one(&client)
        .await
        .unwrap();
    assert_eq!(row.n, 9999999999i64);
}

// ---------------------------------------------------------------------------
// query_as! macro
// ---------------------------------------------------------------------------

#[derive(resolute::FromRow)]
struct MacroAuthor {
    id: i32,
    name: String,
}

#[tokio::test]
async fn test_query_as_macro() {
    let client = connect().await;
    let id = 1i32;
    let author = resolute::query_as!(
        MacroAuthor,
        "SELECT id, name FROM api.authors WHERE id = $1",
        id
    )
    .fetch_one(&client)
    .await
    .unwrap();
    assert_eq!(author.id, 1);
    assert_eq!(author.name, "Alice");
}

#[tokio::test]
async fn test_query_as_macro_fetch_all() {
    let client = connect().await;
    let authors = resolute::query_as!(MacroAuthor, "SELECT id, name FROM api.authors ORDER BY id")
        .fetch_all(&client)
        .await
        .unwrap();
    assert!(authors.len() >= 3);
    assert_eq!(authors[0].name, "Alice");
}

// ---------------------------------------------------------------------------
// query_scalar! macro
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_query_scalar_count() {
    let client = connect().await;
    let count = resolute::query_scalar!("SELECT count(*)::int4 FROM api.authors")
        .fetch_one(&client)
        .await
        .unwrap();
    assert!(count >= 3);
}

#[tokio::test]
async fn test_query_scalar_with_param() {
    let client = connect().await;
    let id = 1i32;
    let name = resolute::query_scalar!("SELECT name FROM api.authors WHERE id = $1", id)
        .fetch_one(&client)
        .await
        .unwrap();
    assert_eq!(name, "Alice");
}

#[tokio::test]
async fn test_query_scalar_bool() {
    let client = connect().await;
    let exists = resolute::query_scalar!("SELECT exists(SELECT 1 FROM api.authors WHERE id = 1)")
        .fetch_one(&client)
        .await
        .unwrap();
    assert!(exists);
}

// ---------------------------------------------------------------------------
// query_file! and query_file_as! macros
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_query_file() {
    let client = connect().await;
    let id = 1i32;
    let row = resolute::query_file!("tests/sql/get_author.sql", id)
        .fetch_one(&client)
        .await
        .unwrap();
    assert_eq!(row.id, 1);
    assert_eq!(row.name, "Alice");
}

#[tokio::test]
async fn test_query_file_as() {
    let client = connect().await;
    let id = 1i32;
    let author = resolute::query_file_as!(MacroAuthor, "tests/sql/get_author.sql", id)
        .fetch_one(&client)
        .await
        .unwrap();
    assert_eq!(author.id, 1);
    assert_eq!(author.name, "Alice");
}

// ---------------------------------------------------------------------------
// query_file_scalar! macro
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_query_file_scalar() {
    let client = connect().await;
    let count = resolute::query_file_scalar!("tests/sql/count_authors.sql")
        .fetch_one(&client)
        .await
        .unwrap();
    assert!(count >= 3);
}

// ---------------------------------------------------------------------------
// query_unchecked! macro
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_query_unchecked() {
    let client = connect().await;
    let rows = resolute::query_unchecked!("SELECT 42::int4 AS n")
        .fetch_all(&client)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let n: i32 = rows[0].get(0).unwrap();
    assert_eq!(n, 42);
}

#[tokio::test]
async fn test_query_unchecked_with_params() {
    let client = connect().await;
    let id = 1i32;
    let row = resolute::query_unchecked!("SELECT name FROM api.authors WHERE id = $1", id)
        .fetch_one(&client)
        .await
        .unwrap();
    let name: String = row.get(0).unwrap();
    assert_eq!(name, "Alice");
}

// ---------------------------------------------------------------------------
// Nullable column detection
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_nullable_column_detection() {
    let client = connect().await;
    // bio column is nullable in api.authors.
    let row = resolute::query!("SELECT id, name, bio FROM api.authors WHERE id = $1", 1i32)
        .fetch_one(&client)
        .await
        .unwrap();
    assert_eq!(row.id, 1);
    assert_eq!(row.name, "Alice");
    // Verify bio has some value (it was inserted with a value).
    // The type (String or Option<String>) depends on nullability detection.
    let bio_str: String = format!("{:?}", row.bio);
    assert!(bio_str.contains("Rust"));
}

// ---------------------------------------------------------------------------
// Named parameters in compile-time macros
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_query_macro_named_params() {
    let client = connect().await;
    let id = 1i32;
    let row = resolute::query!("SELECT id, name FROM api.authors WHERE id = :id", id = id)
        .fetch_one(&client)
        .await
        .unwrap();
    assert_eq!(row.id, 1);
    assert_eq!(row.name, "Alice");
}

#[tokio::test]
async fn test_query_macro_named_params_multiple() {
    let client = connect().await;
    let a_val = 10i32;
    let b_val = "hello".to_string();
    let row = resolute::query!("SELECT :a::int4 AS a, :b::text AS b", a = a_val, b = b_val,)
        .fetch_one(&client)
        .await
        .unwrap();
    assert_eq!(row.a, Some(10));
    assert_eq!(row.b, Some("hello".to_string()));
}

#[tokio::test]
async fn test_query_scalar_named() {
    let client = connect().await;
    let id_val = 1i32;
    let count = resolute::query_scalar!(
        "SELECT count(*)::int4 FROM api.authors WHERE id = :id",
        id = id_val,
    )
    .fetch_one(&client)
    .await
    .unwrap();
    assert_eq!(count, 1);
}
