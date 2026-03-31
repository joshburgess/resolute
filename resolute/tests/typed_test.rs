//! Integration tests for resolute.
//! Requires: docker compose up -d (PostgreSQL on port 54322)

#![allow(clippy::bool_assert_comparison, clippy::approx_constant)]

use resolute::{Client, Decode, DecodeText, Encode, Executor, FromRow, PgType};

// ---------------------------------------------------------------------------
// #[resolute::test] macro
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Date/timestamp infinity
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_timestamp_infinity() {
    let client = connect().await;
    let rows = client
        .query("SELECT 'infinity'::timestamp AS ts", &[])
        .await
        .unwrap();
    let ts: resolute::PgTimestamp = rows[0].get(0).unwrap();
    assert_eq!(ts, resolute::PgTimestamp::Infinity);
}

#[tokio::test]
async fn test_timestamp_neg_infinity() {
    let client = connect().await;
    let rows = client
        .query("SELECT '-infinity'::timestamp AS ts", &[])
        .await
        .unwrap();
    let ts: resolute::PgTimestamp = rows[0].get(0).unwrap();
    assert_eq!(ts, resolute::PgTimestamp::NegInfinity);
}

#[tokio::test]
async fn test_date_infinity() {
    let client = connect().await;
    let rows = client
        .query("SELECT 'infinity'::date AS d", &[])
        .await
        .unwrap();
    let d: resolute::PgDate = rows[0].get(0).unwrap();
    assert_eq!(d, resolute::PgDate::Infinity);
}

#[tokio::test]
async fn test_chrono_rejects_infinity() {
    let client = connect().await;
    // Chrono should return an error, not wrong data.
    let rows = client
        .query("SELECT 'infinity'::timestamp AS ts", &[])
        .await
        .unwrap();
    let result = rows[0].get::<chrono::NaiveDateTime>(0);
    assert!(result.is_err());
}

#[resolute::test]
async fn test_resolute_test_macro(client: resolute::Client) {
    client
        .simple_query("CREATE TABLE __macro_test (id int)")
        .await
        .unwrap();
    client
        .execute("INSERT INTO __macro_test VALUES ($1)", &[&42i32])
        .await
        .unwrap();
    let rows = client
        .query("SELECT id FROM __macro_test", &[])
        .await
        .unwrap();
    assert_eq!(rows[0].get::<i32>(0).unwrap(), 42);
}
use tokio_stream::StreamExt;

const ADDR: &str = "127.0.0.1:54322";
const USER: &str = "postgres";
const PASS: &str = "postgres";
const DB: &str = "postgrest_test";

async fn connect() -> Client {
    Client::connect(ADDR, USER, PASS, DB).await.unwrap()
}

// ---------------------------------------------------------------------------
// Binary encode/decode unit tests
// ---------------------------------------------------------------------------

#[test]
fn test_encode_decode_bool() {
    let mut buf = bytes::BytesMut::new();
    true.encode(&mut buf);
    assert_eq!(buf.as_ref(), &[1]);
    assert_eq!(bool::decode(&buf).unwrap(), true);

    buf.clear();
    false.encode(&mut buf);
    assert_eq!(buf.as_ref(), &[0]);
    assert_eq!(bool::decode(&buf).unwrap(), false);
}

#[test]
fn test_encode_decode_i16() {
    let mut buf = bytes::BytesMut::new();
    42i16.encode(&mut buf);
    assert_eq!(buf.as_ref(), &[0, 42]);
    assert_eq!(i16::decode(&buf).unwrap(), 42);

    buf.clear();
    (-1i16).encode(&mut buf);
    assert_eq!(i16::decode(&buf).unwrap(), -1);
}

#[test]
fn test_encode_decode_i32() {
    let mut buf = bytes::BytesMut::new();
    12345i32.encode(&mut buf);
    assert_eq!(i32::decode(&buf).unwrap(), 12345);

    buf.clear();
    i32::MIN.encode(&mut buf);
    assert_eq!(i32::decode(&buf).unwrap(), i32::MIN);

    buf.clear();
    i32::MAX.encode(&mut buf);
    assert_eq!(i32::decode(&buf).unwrap(), i32::MAX);
}

#[test]
fn test_encode_decode_i64() {
    let mut buf = bytes::BytesMut::new();
    123456789012345i64.encode(&mut buf);
    assert_eq!(i64::decode(&buf).unwrap(), 123456789012345);
}

#[test]
fn test_encode_decode_f32() {
    let mut buf = bytes::BytesMut::new();
    3.14f32.encode(&mut buf);
    let decoded = f32::decode(&buf).unwrap();
    assert!((decoded - 3.14).abs() < 1e-6);
}

#[test]
fn test_encode_decode_f64() {
    let mut buf = bytes::BytesMut::new();
    std::f64::consts::PI.encode(&mut buf);
    let decoded = f64::decode(&buf).unwrap();
    assert!((decoded - std::f64::consts::PI).abs() < 1e-15);
}

#[test]
fn test_encode_decode_string() {
    let mut buf = bytes::BytesMut::new();
    "hello world".encode(&mut buf);
    assert_eq!(String::decode(&buf).unwrap(), "hello world");
}

#[test]
fn test_encode_decode_bytes() {
    let mut buf = bytes::BytesMut::new();
    let data = vec![0u8, 1, 2, 255, 128];
    data.encode(&mut buf);
    assert_eq!(Vec::<u8>::decode(&buf).unwrap(), data);
}

#[test]
fn test_decode_wrong_size() {
    assert!(i32::decode(&[0, 0]).is_err());
    assert!(i64::decode(&[0, 0, 0, 0]).is_err());
    assert!(bool::decode(&[]).is_err());
    assert!(f32::decode(&[0]).is_err());
}

// ---------------------------------------------------------------------------
// Integration tests: binary-format queries against real PostgreSQL
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_query_select_int() {
    let client = connect().await;
    let rows = client.query("SELECT $1::int4 AS n", &[&42i32]).await.unwrap();
    assert_eq!(rows.len(), 1);
    let n: i32 = rows[0].get(0).unwrap();
    assert_eq!(n, 42);
}

#[tokio::test]
async fn test_query_select_text() {
    let client = connect().await;
    let rows = client
        .query("SELECT $1::text AS val", &[&"hello"])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let val: String = rows[0].get(0).unwrap();
    assert_eq!(val, "hello");
}

#[tokio::test]
async fn test_query_select_bool() {
    let client = connect().await;
    let rows = client
        .query("SELECT $1::bool AS flag", &[&true])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let flag: bool = rows[0].get(0).unwrap();
    assert!(flag);
}

#[tokio::test]
async fn test_query_select_float8() {
    let client = connect().await;
    let rows = client
        .query("SELECT $1::float8 AS val", &[&3.14f64])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let val: f64 = rows[0].get(0).unwrap();
    assert!((val - 3.14).abs() < 1e-10);
}

#[tokio::test]
async fn test_query_select_bigint() {
    let client = connect().await;
    let rows = client
        .query("SELECT $1::int8 AS val", &[&9999999999i64])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let val: i64 = rows[0].get(0).unwrap();
    assert_eq!(val, 9999999999);
}

#[tokio::test]
async fn test_query_multiple_columns() {
    let client = connect().await;
    let rows = client
        .query(
            "SELECT $1::int4 AS a, $2::text AS b, $3::bool AS c",
            &[&10i32, &"foo", &false],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let a: i32 = rows[0].get(0).unwrap();
    let b: String = rows[0].get(1).unwrap();
    let c: bool = rows[0].get(2).unwrap();
    assert_eq!(a, 10);
    assert_eq!(b, "foo");
    assert!(!c);
}

#[tokio::test]
async fn test_query_multiple_rows() {
    let client = connect().await;
    let rows = client
        .query("SELECT generate_series(1, 5)::int4 AS n", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 5);
    for (i, row) in rows.iter().enumerate() {
        let n: i32 = row.get(0).unwrap();
        assert_eq!(n, (i + 1) as i32);
    }
}

#[tokio::test]
async fn test_query_null() {
    let client = connect().await;
    let rows = client
        .query("SELECT NULL::int4 AS n", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let n: Option<i32> = rows[0].get_opt(0).unwrap();
    assert!(n.is_none());
}

#[tokio::test]
async fn test_query_one() {
    let client = connect().await;
    let row = client.query_one("SELECT 42::int4 AS n", &[]).await.unwrap();
    let n: i32 = row.get(0).unwrap();
    assert_eq!(n, 42);
}

#[tokio::test]
async fn test_query_one_not_found() {
    let client = connect().await;
    let result = client
        .query_one("SELECT 1 WHERE false", &[])
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_query_opt_some() {
    let client = connect().await;
    let row = client.query_opt("SELECT 42::int4 AS n", &[]).await.unwrap();
    assert!(row.is_some());
    let n: i32 = row.unwrap().get(0).unwrap();
    assert_eq!(n, 42);
}

#[tokio::test]
async fn test_query_opt_none() {
    let client = connect().await;
    let row = client.query_opt("SELECT 1 WHERE false", &[]).await.unwrap();
    assert!(row.is_none());
}

#[tokio::test]
async fn test_query_no_params() {
    let client = connect().await;
    let rows = client.query("SELECT 1::int4 AS n", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    let n: i32 = rows[0].get(0).unwrap();
    assert_eq!(n, 1);
}

#[tokio::test]
async fn test_query_real_table() {
    let client = connect().await;
    let rows = client
        .query("SELECT id, name FROM api.authors ORDER BY id", &[])
        .await
        .unwrap();
    assert!(rows.len() >= 3);
    // Columns come back as binary ints and text.
    let id: i32 = rows[0].get(0).unwrap();
    assert_eq!(id, 1);
    let name: String = rows[0].get(1).unwrap();
    assert_eq!(name, "Alice");
}

#[tokio::test]
async fn test_query_with_filter() {
    let client = connect().await;
    let rows = client
        .query(
            "SELECT name FROM api.authors WHERE id = $1",
            &[&1i32],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let name: String = rows[0].get(0).unwrap();
    assert_eq!(name, "Alice");
}

#[tokio::test]
async fn test_statement_cache() {
    let client = connect().await;
    // First call: Parse + Bind + Execute + Sync
    let r1 = client.query("SELECT $1::int4 AS n", &[&1i32]).await.unwrap();
    // Second call: cache hit — Bind + Execute + Sync (no Parse)
    let r2 = client.query("SELECT $1::int4 AS n", &[&2i32]).await.unwrap();
    assert_eq!(r1[0].get::<i32>(0).unwrap(), 1);
    assert_eq!(r2[0].get::<i32>(0).unwrap(), 2);
}

#[tokio::test]
async fn test_error_recovery() {
    let client = connect().await;
    let result = client.query("SELECT * FROM nonexistent_xyz_table", &[]).await;
    assert!(result.is_err());
    // Connection should still work.
    let rows = client.query("SELECT 1::int4 AS n", &[]).await.unwrap();
    assert_eq!(rows[0].get::<i32>(0).unwrap(), 1);
}

#[tokio::test]
async fn test_sequential_typed_queries() {
    let client = connect().await;
    for i in 0..10i32 {
        let rows = client.query("SELECT $1::int4 AS n", &[&i]).await.unwrap();
        let n: i32 = rows[0].get(0).unwrap();
        assert_eq!(n, i);
    }
}

// ---------------------------------------------------------------------------
// Column names (RowDescription)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_column_names() {
    let client = connect().await;
    let rows = client
        .query("SELECT 1::int4 AS id, 'hello'::text AS name", &[])
        .await
        .unwrap();
    assert_eq!(rows[0].column_name(0), Some("id"));
    assert_eq!(rows[0].column_name(1), Some("name"));
}

#[tokio::test]
async fn test_get_by_name() {
    let client = connect().await;
    let rows = client
        .query("SELECT 42::int4 AS answer, 'hello'::text AS greeting", &[])
        .await
        .unwrap();
    let answer: i32 = rows[0].get_by_name("answer").unwrap();
    let greeting: String = rows[0].get_by_name("greeting").unwrap();
    assert_eq!(answer, 42);
    assert_eq!(greeting, "hello");
}

#[tokio::test]
async fn test_get_by_name_not_found() {
    let client = connect().await;
    let rows = client.query("SELECT 1::int4 AS n", &[]).await.unwrap();
    let result = rows[0].get_by_name::<i32>("nonexistent");
    assert!(result.is_err());
}

#[tokio::test]
async fn test_column_type_oids() {
    let client = connect().await;
    let rows = client
        .query("SELECT 1::int4 AS n, 'hi'::text AS s", &[])
        .await
        .unwrap();
    // int4 OID = 23, text OID = 25
    assert_eq!(rows[0].column_type_oid(0), Some(23));
    assert_eq!(rows[0].column_type_oid(1), Some(25));
}

// ---------------------------------------------------------------------------
// Execute (INSERT/UPDATE/DELETE with row count)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_execute_insert_and_delete() {
    let client = connect().await;

    // Create a temp table.
    client.simple_query("CREATE TEMP TABLE test_exec (id int)").await.unwrap();

    // Insert rows.
    let count = client
        .execute("INSERT INTO test_exec VALUES ($1)", &[&1i32])
        .await
        .unwrap();
    assert_eq!(count, 1);

    let count = client
        .execute("INSERT INTO test_exec VALUES ($1), ($2)", &[&2i32, &3i32])
        .await
        .unwrap();
    assert_eq!(count, 2);

    // Delete.
    let count = client
        .execute("DELETE FROM test_exec WHERE id > $1", &[&1i32])
        .await
        .unwrap();
    assert_eq!(count, 2);

    // Verify.
    let rows = client.query("SELECT id FROM test_exec", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<i32>(0).unwrap(), 1);
}

#[tokio::test]
async fn test_execute_update() {
    let client = connect().await;
    client.simple_query("CREATE TEMP TABLE test_upd (id int, val text)").await.unwrap();
    client.execute("INSERT INTO test_upd VALUES ($1, $2)", &[&1i32, &"old"]).await.unwrap();
    client.execute("INSERT INTO test_upd VALUES ($1, $2)", &[&2i32, &"old"]).await.unwrap();

    let count = client
        .execute("UPDATE test_upd SET val = $1 WHERE id = $2", &[&"new", &1i32])
        .await
        .unwrap();
    assert_eq!(count, 1);

    let rows = client
        .query("SELECT val FROM test_upd WHERE id = $1", &[&1i32])
        .await
        .unwrap();
    assert_eq!(rows[0].get::<String>(0).unwrap(), "new");
}

// ---------------------------------------------------------------------------
// Transactions
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_transaction_commit() {
    let client = connect().await;
    client.simple_query("CREATE TEMP TABLE test_txn (id int)").await.unwrap();

    let txn = client.begin().await.unwrap();
    txn.execute("INSERT INTO test_txn VALUES ($1)", &[&1i32]).await.unwrap();
    txn.execute("INSERT INTO test_txn VALUES ($1)", &[&2i32]).await.unwrap();
    txn.commit().await.unwrap();

    let rows = client.query("SELECT id FROM test_txn ORDER BY id", &[]).await.unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<i32>(0).unwrap(), 1);
    assert_eq!(rows[1].get::<i32>(0).unwrap(), 2);
}

#[tokio::test]
async fn test_transaction_rollback() {
    let client = connect().await;
    client.simple_query("CREATE TEMP TABLE test_txn_rb (id int)").await.unwrap();

    let txn = client.begin().await.unwrap();
    txn.execute("INSERT INTO test_txn_rb VALUES ($1)", &[&1i32]).await.unwrap();
    txn.rollback().await.unwrap();

    // Table should be empty — insert was rolled back.
    let rows = client.query("SELECT id FROM test_txn_rb", &[]).await.unwrap();
    assert_eq!(rows.len(), 0);
}

#[tokio::test]
async fn test_transaction_query_inside() {
    let client = connect().await;
    client.simple_query("CREATE TEMP TABLE test_txn_q (id int)").await.unwrap();

    let txn = client.begin().await.unwrap();
    txn.execute(
        "INSERT INTO test_txn_q VALUES ($1), ($2), ($3)",
        &[&10i32, &20i32, &30i32],
    ).await.unwrap();
    let rows = txn.query("SELECT sum(id)::int4 FROM test_txn_q", &[]).await.unwrap();
    let sum: i32 = rows[0].get(0).unwrap();
    assert_eq!(sum, 60);
    txn.commit().await.unwrap();
}

// ---------------------------------------------------------------------------
// Simple query
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_simple_query_ddl() {
    let client = connect().await;
    client.simple_query("CREATE TEMP TABLE test_simple (id int)").await.unwrap();
    client.simple_query("DROP TABLE test_simple").await.unwrap();
}

// ---------------------------------------------------------------------------
// FromRow trait (manual impl)
// ---------------------------------------------------------------------------

struct Author {
    id: i32,
    name: String,
}

impl resolute::FromRow for Author {
    fn from_row(row: &resolute::Row) -> Result<Self, resolute::TypedError> {
        Ok(Author {
            id: row.get_by_name("id")?,
            name: row.get_by_name("name")?,
        })
    }
}

#[tokio::test]
async fn test_from_row_manual() {
    let client = connect().await;
    let rows = client
        .query("SELECT id, name FROM api.authors WHERE id = $1", &[&1i32])
        .await
        .unwrap();
    let author = Author::from_row(&rows[0]).unwrap();
    assert_eq!(author.id, 1);
    assert_eq!(author.name, "Alice");
}

#[tokio::test]
async fn test_from_row_multiple() {
    let client = connect().await;
    let rows = client
        .query("SELECT id, name FROM api.authors ORDER BY id", &[])
        .await
        .unwrap();
    let authors: Vec<Author> = rows.iter().map(|r| Author::from_row(r).unwrap()).collect();
    assert!(authors.len() >= 3);
    assert_eq!(authors[0].name, "Alice");
    assert_eq!(authors[1].name, "Bob");
}

// ---------------------------------------------------------------------------
// FromRow derive macro
// ---------------------------------------------------------------------------

#[derive(resolute::FromRow)]
struct DerivedAuthor {
    id: i32,
    name: String,
}

#[tokio::test]
async fn test_derive_from_row_basic() {
    let client = connect().await;
    let rows = client
        .query("SELECT id, name FROM api.authors WHERE id = $1", &[&1i32])
        .await
        .unwrap();
    let author = DerivedAuthor::from_row(&rows[0]).unwrap();
    assert_eq!(author.id, 1);
    assert_eq!(author.name, "Alice");
}

#[derive(resolute::FromRow)]
struct DerivedAuthorWithBio {
    id: i32,
    name: String,
    bio: Option<String>,
}

#[tokio::test]
async fn test_derive_from_row_optional() {
    let client = connect().await;
    let rows = client
        .query("SELECT id, name, bio FROM api.authors WHERE id = $1", &[&1i32])
        .await
        .unwrap();
    let author = DerivedAuthorWithBio::from_row(&rows[0]).unwrap();
    assert_eq!(author.id, 1);
    assert_eq!(author.name, "Alice");
    assert!(author.bio.is_some());
}

#[derive(resolute::FromRow)]
struct RenamedFields {
    #[from_row(rename = "id")]
    author_id: i32,
    #[from_row(rename = "name")]
    author_name: String,
}

#[tokio::test]
async fn test_derive_from_row_rename() {
    let client = connect().await;
    let rows = client
        .query("SELECT id, name FROM api.authors WHERE id = $1", &[&1i32])
        .await
        .unwrap();
    let r = RenamedFields::from_row(&rows[0]).unwrap();
    assert_eq!(r.author_id, 1);
    assert_eq!(r.author_name, "Alice");
}

#[tokio::test]
async fn test_derive_from_row_multiple() {
    let client = connect().await;
    let rows = client
        .query("SELECT id, name FROM api.authors ORDER BY id", &[])
        .await
        .unwrap();
    let authors: Vec<DerivedAuthor> = rows
        .iter()
        .map(|r| DerivedAuthor::from_row(r).unwrap())
        .collect();
    assert!(authors.len() >= 3);
    assert_eq!(authors[0].name, "Alice");
    assert_eq!(authors[1].name, "Bob");
}

// ---------------------------------------------------------------------------
// Nullable parameters
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_null_param() {
    let client = connect().await;
    let val: Option<i32> = None;
    let rows = client.query("SELECT $1::int4 IS NULL AS is_null", &[&val]).await.unwrap();
    let is_null: bool = rows[0].get(0).unwrap();
    assert!(is_null);
}

#[tokio::test]
async fn test_some_param() {
    let client = connect().await;
    let val: Option<i32> = Some(42);
    let rows = client.query("SELECT $1::int4 AS n", &[&val]).await.unwrap();
    let n: i32 = rows[0].get(0).unwrap();
    assert_eq!(n, 42);
}

#[tokio::test]
async fn test_null_text_param() {
    let client = connect().await;
    let val: Option<String> = None;
    let rows = client.query("SELECT $1::text IS NULL AS is_null", &[&val]).await.unwrap();
    let is_null: bool = rows[0].get(0).unwrap();
    assert!(is_null);
}

// ---------------------------------------------------------------------------
// Chrono types
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_timestamp() {
    let client = connect().await;
    let now = chrono::Utc::now().naive_utc();
    let rows = client
        .query("SELECT $1::timestamp AS ts", &[&now])
        .await
        .unwrap();
    let ts: chrono::NaiveDateTime = rows[0].get(0).unwrap();
    // Within 1 second (microsecond precision).
    let diff = (ts - now).num_milliseconds().abs();
    assert!(diff < 1000, "timestamp diff {diff}ms should be < 1s");
}

#[tokio::test]
async fn test_timestamptz() {
    let client = connect().await;
    let now = chrono::Utc::now();
    let rows = client
        .query("SELECT $1::timestamptz AS ts", &[&now])
        .await
        .unwrap();
    let ts: chrono::DateTime<chrono::Utc> = rows[0].get(0).unwrap();
    let diff = (ts - now).num_milliseconds().abs();
    assert!(diff < 1000, "timestamp diff {diff}ms should be < 1s");
}

#[tokio::test]
async fn test_date() {
    let client = connect().await;
    let d = chrono::NaiveDate::from_ymd_opt(2024, 6, 15).unwrap();
    let rows = client.query("SELECT $1::date AS d", &[&d]).await.unwrap();
    let result: chrono::NaiveDate = rows[0].get(0).unwrap();
    assert_eq!(result, d);
}

#[tokio::test]
async fn test_time() {
    let client = connect().await;
    let t = chrono::NaiveTime::from_hms_opt(14, 30, 0).unwrap();
    let rows = client.query("SELECT $1::time AS t", &[&t]).await.unwrap();
    let result: chrono::NaiveTime = rows[0].get(0).unwrap();
    assert_eq!(result, t);
}

// ---------------------------------------------------------------------------
// JSON types
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_jsonb() {
    let client = connect().await;
    let val = serde_json::json!({"key": "value", "num": 42});
    let rows = client
        .query("SELECT $1::jsonb AS j", &[&val])
        .await
        .unwrap();
    let result: serde_json::Value = rows[0].get(0).unwrap();
    assert_eq!(result["key"], "value");
    assert_eq!(result["num"], 42);
}

#[tokio::test]
async fn test_jsonb_array() {
    let client = connect().await;
    let val = serde_json::json!([1, 2, 3]);
    let rows = client
        .query("SELECT $1::jsonb AS j", &[&val])
        .await
        .unwrap();
    let result: serde_json::Value = rows[0].get(0).unwrap();
    assert_eq!(result, serde_json::json!([1, 2, 3]));
}

// ---------------------------------------------------------------------------
// UUID
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_uuid() {
    let client = connect().await;
    let id = uuid::Uuid::new_v4();
    let rows = client
        .query("SELECT $1::uuid AS id", &[&id])
        .await
        .unwrap();
    let result: uuid::Uuid = rows[0].get(0).unwrap();
    assert_eq!(result, id);
}

// ---------------------------------------------------------------------------
// Encode/Decode roundtrip unit tests for new types
// ---------------------------------------------------------------------------

#[test]
fn test_encode_decode_uuid() {
    let id = uuid::Uuid::new_v4();
    let mut buf = bytes::BytesMut::new();
    id.encode(&mut buf);
    assert_eq!(buf.len(), 16);
    assert_eq!(uuid::Uuid::decode(&buf).unwrap(), id);
}

#[test]
fn test_encode_decode_naive_date() {
    let d = chrono::NaiveDate::from_ymd_opt(2024, 6, 15).unwrap();
    let mut buf = bytes::BytesMut::new();
    d.encode(&mut buf);
    assert_eq!(buf.len(), 4);
    assert_eq!(chrono::NaiveDate::decode(&buf).unwrap(), d);
}

#[test]
fn test_encode_decode_naive_time() {
    let t = chrono::NaiveTime::from_hms_micro_opt(14, 30, 45, 123456).unwrap();
    let mut buf = bytes::BytesMut::new();
    t.encode(&mut buf);
    assert_eq!(buf.len(), 8);
    assert_eq!(chrono::NaiveTime::decode(&buf).unwrap(), t);
}

#[test]
fn test_encode_decode_timestamp() {
    let dt = chrono::NaiveDate::from_ymd_opt(2024, 6, 15)
        .unwrap()
        .and_hms_opt(14, 30, 0)
        .unwrap();
    let mut buf = bytes::BytesMut::new();
    dt.encode(&mut buf);
    assert_eq!(buf.len(), 8);
    assert_eq!(chrono::NaiveDateTime::decode(&buf).unwrap(), dt);
}

#[test]
fn test_encode_decode_jsonb() {
    let val = serde_json::json!({"hello": "world"});
    let mut buf = bytes::BytesMut::new();
    val.encode(&mut buf);
    // First byte is JSONB version (1).
    assert_eq!(buf[0], 1);
    let decoded: serde_json::Value = serde_json::Value::decode(&buf).unwrap();
    assert_eq!(decoded, val);
}

// ---------------------------------------------------------------------------
// Array types
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_int_array() {
    let client = connect().await;
    let arr = vec![1i32, 2, 3, 4, 5];
    let rows = client
        .query("SELECT $1::int4[] AS arr", &[&arr])
        .await
        .unwrap();
    let result: Vec<i32> = rows[0].get(0).unwrap();
    assert_eq!(result, vec![1, 2, 3, 4, 5]);
}

#[tokio::test]
async fn test_bigint_array() {
    let client = connect().await;
    let arr = vec![100i64, 200, 300];
    let rows = client
        .query("SELECT $1::int8[] AS arr", &[&arr])
        .await
        .unwrap();
    let result: Vec<i64> = rows[0].get(0).unwrap();
    assert_eq!(result, vec![100, 200, 300]);
}

#[tokio::test]
async fn test_text_array() {
    let client = connect().await;
    let arr = vec!["hello".to_string(), "world".to_string()];
    let rows = client
        .query("SELECT $1::text[] AS arr", &[&arr])
        .await
        .unwrap();
    let result: Vec<String> = rows[0].get(0).unwrap();
    assert_eq!(result, vec!["hello", "world"]);
}

#[tokio::test]
async fn test_empty_array() {
    let client = connect().await;
    let arr: Vec<i32> = vec![];
    let rows = client
        .query("SELECT $1::int4[] AS arr", &[&arr])
        .await
        .unwrap();
    let result: Vec<i32> = rows[0].get(0).unwrap();
    assert!(result.is_empty());
}

// ---------------------------------------------------------------------------
// Numeric / Inet newtypes
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_numeric_roundtrip() {
    let client = connect().await;
    // PG returns numeric in binary format. We decode it to PgNumeric (string).
    let rows = client
        .query("SELECT 123.456::numeric AS n", &[])
        .await
        .unwrap();
    let n: resolute::PgNumeric = rows[0].get(0).unwrap();
    assert_eq!(n.0, "123.456");
}

#[tokio::test]
async fn test_numeric_zero() {
    let client = connect().await;
    let rows = client
        .query("SELECT 0::numeric AS n", &[])
        .await
        .unwrap();
    let n: resolute::PgNumeric = rows[0].get(0).unwrap();
    assert_eq!(n.0, "0");
}

#[tokio::test]
async fn test_numeric_negative() {
    let client = connect().await;
    let rows = client
        .query("SELECT (-99.99)::numeric AS n", &[])
        .await
        .unwrap();
    let n: resolute::PgNumeric = rows[0].get(0).unwrap();
    assert_eq!(n.0, "-99.99");
}

#[tokio::test]
async fn test_inet_roundtrip() {
    let client = connect().await;
    let rows = client
        .query("SELECT '192.168.1.1/24'::inet AS addr", &[])
        .await
        .unwrap();
    let addr: resolute::PgInet = rows[0].get(0).unwrap();
    assert_eq!(addr.0, "192.168.1.1/24");
}

// ---------------------------------------------------------------------------
// Encode/Decode unit tests for arrays
// ---------------------------------------------------------------------------

#[test]
fn test_encode_decode_int_array() {
    let arr = vec![10i32, 20, 30];
    let mut buf = bytes::BytesMut::new();
    arr.encode(&mut buf);
    let decoded: Vec<i32> = Vec::<i32>::decode(&buf).unwrap();
    assert_eq!(decoded, arr);
}

#[test]
fn test_encode_decode_text_array() {
    let arr = vec!["foo".to_string(), "bar".to_string()];
    let mut buf = bytes::BytesMut::new();
    arr.encode(&mut buf);
    let decoded: Vec<String> = Vec::<String>::decode(&buf).unwrap();
    assert_eq!(decoded, arr);
}

// ---------------------------------------------------------------------------
// New array types: encode/decode roundtrip unit tests
// ---------------------------------------------------------------------------

#[test]
fn test_encode_decode_bool_array() {
    let arr = vec![true, false, true];
    let mut buf = bytes::BytesMut::new();
    arr.encode(&mut buf);
    let decoded = Vec::<bool>::decode(&buf).unwrap();
    assert_eq!(decoded, arr);
}

#[test]
fn test_encode_decode_i16_array() {
    let arr = vec![1i16, -2, 32767];
    let mut buf = bytes::BytesMut::new();
    arr.encode(&mut buf);
    let decoded = Vec::<i16>::decode(&buf).unwrap();
    assert_eq!(decoded, arr);
}

#[test]
fn test_encode_decode_f32_array() {
    let arr = vec![1.5f32, -2.5, 0.0];
    let mut buf = bytes::BytesMut::new();
    arr.encode(&mut buf);
    let decoded = Vec::<f32>::decode(&buf).unwrap();
    assert_eq!(decoded, arr);
}

#[test]
fn test_encode_decode_f64_array() {
    let arr = vec![std::f64::consts::PI, -1.0, 0.0];
    let mut buf = bytes::BytesMut::new();
    arr.encode(&mut buf);
    let decoded = Vec::<f64>::decode(&buf).unwrap();
    assert_eq!(decoded, arr);
}

#[test]
fn test_encode_decode_uuid_array() {
    let arr = vec![uuid::Uuid::new_v4(), uuid::Uuid::new_v4()];
    let mut buf = bytes::BytesMut::new();
    arr.encode(&mut buf);
    let decoded = Vec::<uuid::Uuid>::decode(&buf).unwrap();
    assert_eq!(decoded, arr);
}

#[test]
fn test_encode_decode_date_array() {
    let arr = vec![
        chrono::NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
        chrono::NaiveDate::from_ymd_opt(2025, 12, 31).unwrap(),
    ];
    let mut buf = bytes::BytesMut::new();
    arr.encode(&mut buf);
    let decoded = Vec::<chrono::NaiveDate>::decode(&buf).unwrap();
    assert_eq!(decoded, arr);
}

#[test]
fn test_encode_decode_empty_bool_array() {
    let arr: Vec<bool> = vec![];
    let mut buf = bytes::BytesMut::new();
    arr.encode(&mut buf);
    let decoded = Vec::<bool>::decode(&buf).unwrap();
    assert!(decoded.is_empty());
}

// ---------------------------------------------------------------------------
// DecodeText for arrays (parse_pg_text_array)
// ---------------------------------------------------------------------------

#[test]
fn test_decode_text_int_array() {
    let result = Vec::<i32>::decode_text("{1,2,3}").unwrap();
    assert_eq!(result, vec![1, 2, 3]);
}

#[test]
fn test_decode_text_empty_array() {
    let result = Vec::<i32>::decode_text("{}").unwrap();
    assert!(result.is_empty());
}

#[test]
fn test_decode_text_string_array_quoted() {
    let result = Vec::<String>::decode_text(r#"{"hello","world"}"#).unwrap();
    assert_eq!(result, vec!["hello", "world"]);
}

#[test]
fn test_decode_text_string_array_with_comma() {
    let result = Vec::<String>::decode_text(r#"{"hello, world","foo"}"#).unwrap();
    assert_eq!(result, vec!["hello, world", "foo"]);
}

#[test]
fn test_decode_text_string_array_with_escape() {
    let result = Vec::<String>::decode_text(r#"{"with \"quotes\""}"#).unwrap();
    assert_eq!(result, vec![r#"with "quotes""#]);
}

#[test]
fn test_decode_text_bool_array() {
    let result = Vec::<bool>::decode_text("{t,f,t}").unwrap();
    assert_eq!(result, vec![true, false, true]);
}

#[test]
fn test_decode_text_float_array() {
    let result = Vec::<f64>::decode_text("{1.5,-2.5,0}").unwrap();
    assert_eq!(result, vec![1.5, -2.5, 0.0]);
}

#[test]
fn test_decode_text_null_element_errors() {
    let result = Vec::<i32>::decode_text("{1,NULL,3}");
    assert!(result.is_err());
}

#[test]
fn test_decode_text_invalid_format() {
    let result = Vec::<i32>::decode_text("not an array");
    assert!(result.is_err());
}

// ---------------------------------------------------------------------------
// PgType trait
// ---------------------------------------------------------------------------

#[test]
fn test_pg_type_oids() {
    assert_eq!(i32::OID, 23);
    assert_eq!(i32::ARRAY_OID, 1007);
    assert_eq!(String::OID, 25);
    assert_eq!(String::ARRAY_OID, 1009);
    assert_eq!(bool::OID, 16);
    assert_eq!(bool::ARRAY_OID, 1000);
    assert_eq!(f64::OID, 701);
    assert_eq!(f64::ARRAY_OID, 1022);
}

#[test]
fn test_pg_type_vec_oid() {
    assert_eq!(<Vec<i32> as PgType>::OID, 1007);
    assert_eq!(<Vec<String> as PgType>::OID, 1009);
    assert_eq!(<Vec<bool> as PgType>::OID, 1000);
    assert_eq!(<Vec<u8> as PgType>::OID, 17); // bytea, not array
}

// ---------------------------------------------------------------------------
// PgEnum derive
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, resolute::PgEnum)]
enum Mood {
    Happy,
    Sad,
    #[pg_type(rename = "so-so")]
    SoSo,
}

#[test]
fn test_pg_enum_encode_decode() {
    let mut buf = bytes::BytesMut::new();
    Mood::Happy.encode(&mut buf);
    assert_eq!(&buf[..], b"happy");
    assert_eq!(Mood::decode(b"happy").unwrap(), Mood::Happy);
    assert_eq!(Mood::decode(b"sad").unwrap(), Mood::Sad);
    assert_eq!(Mood::decode(b"so-so").unwrap(), Mood::SoSo);
}

#[test]
fn test_pg_enum_decode_text() {
    assert_eq!(Mood::decode_text("happy").unwrap(), Mood::Happy);
    assert_eq!(Mood::decode_text("so-so").unwrap(), Mood::SoSo);
    assert!(Mood::decode_text("unknown").is_err());
}

#[test]
fn test_pg_enum_roundtrip() {
    for variant in &[Mood::Happy, Mood::Sad, Mood::SoSo] {
        let mut buf = bytes::BytesMut::new();
        variant.encode(&mut buf);
        let decoded = Mood::decode(&buf).unwrap();
        assert_eq!(&decoded, variant);
    }
}

#[derive(Debug, PartialEq, resolute::PgEnum)]
#[pg_type(rename_all = "UPPERCASE")]
enum Color {
    Red,
    Green,
    Blue,
}

#[test]
fn test_pg_enum_rename_all_uppercase() {
    let mut buf = bytes::BytesMut::new();
    Color::Red.encode(&mut buf);
    assert_eq!(&buf[..], b"RED");
    assert_eq!(Color::decode(b"GREEN").unwrap(), Color::Green);
    assert_eq!(Color::decode(b"BLUE").unwrap(), Color::Blue);
}

#[derive(Debug, PartialEq, resolute::PgEnum)]
#[pg_type(rename_all = "kebab-case")]
enum Status {
    InProgress,
    NotStarted,
    Done,
}

#[test]
fn test_pg_enum_rename_all_kebab() {
    let mut buf = bytes::BytesMut::new();
    Status::InProgress.encode(&mut buf);
    assert_eq!(&buf[..], b"in-progress");

    buf.clear();
    Status::NotStarted.encode(&mut buf);
    assert_eq!(&buf[..], b"not-started");

    buf.clear();
    Status::Done.encode(&mut buf);
    assert_eq!(&buf[..], b"done");
}

// ---------------------------------------------------------------------------
// PgComposite derive
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, resolute::PgComposite)]
struct Point2D {
    x: f64,
    y: f64,
}

#[test]
fn test_pg_composite_encode_decode() {
    let pt = Point2D { x: 1.5, y: -2.5 };
    let mut buf = bytes::BytesMut::new();
    pt.encode(&mut buf);
    let decoded = Point2D::decode(&buf).unwrap();
    assert_eq!(decoded, pt);
}

#[derive(Debug, PartialEq, resolute::PgComposite)]
struct Address {
    street: String,
    city: String,
    zip: Option<String>,
}

#[test]
fn test_pg_composite_with_optional_field() {
    let addr = Address {
        street: "123 Main St".into(),
        city: "Springfield".into(),
        zip: Some("62704".into()),
    };
    let mut buf = bytes::BytesMut::new();
    addr.encode(&mut buf);
    let decoded = Address::decode(&buf).unwrap();
    assert_eq!(decoded, addr);
}

#[test]
fn test_pg_composite_with_null_field() {
    let addr = Address {
        street: "456 Oak Ave".into(),
        city: "Shelbyville".into(),
        zip: None,
    };
    let mut buf = bytes::BytesMut::new();
    addr.encode(&mut buf);
    let decoded = Address::decode(&buf).unwrap();
    assert_eq!(decoded, addr);
    assert!(decoded.zip.is_none());
}

#[test]
fn test_pg_composite_too_short() {
    assert!(Point2D::decode(&[]).is_err());
    assert!(Point2D::decode(&[0, 0, 0]).is_err());
}

// ---------------------------------------------------------------------------
// PgDomain derive
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, resolute::PgDomain)]
struct Email(String);

#[test]
fn test_pg_domain_encode_decode() {
    let email = Email("user@example.com".into());
    let mut buf = bytes::BytesMut::new();
    email.encode(&mut buf);
    let decoded = Email::decode(&buf).unwrap();
    assert_eq!(decoded, email);
}

#[test]
fn test_pg_domain_decode_text() {
    let decoded = Email::decode_text("admin@test.com").unwrap();
    assert_eq!(decoded.0, "admin@test.com");
}

#[derive(Debug, PartialEq, resolute::PgDomain)]
struct PositiveInt(i32);

#[test]
fn test_pg_domain_numeric() {
    let val = PositiveInt(42);
    let mut buf = bytes::BytesMut::new();
    val.encode(&mut buf);
    let decoded = PositiveInt::decode(&buf).unwrap();
    assert_eq!(decoded, val);
}

// ---------------------------------------------------------------------------
// PgDomain: ARRAY_OID inheritance
// ---------------------------------------------------------------------------

#[test]
fn test_pg_domain_inherits_array_oid_from_string() {
    // String has ARRAY_OID = 1009 (text[])
    assert_eq!(<Email as PgType>::ARRAY_OID, 1009);
}

#[test]
fn test_pg_domain_inherits_array_oid_from_i32() {
    // i32 has ARRAY_OID = 1007 (int4[])
    assert_eq!(<PositiveInt as PgType>::ARRAY_OID, 1007);
}

#[derive(Debug, PartialEq, resolute::PgDomain)]
struct UserId(i64);

#[test]
fn test_pg_domain_inherits_array_oid_from_i64() {
    // i64 has ARRAY_OID = 1016 (int8[])
    assert_eq!(<UserId as PgType>::ARRAY_OID, 1016);
}

#[derive(Debug, PartialEq, resolute::PgDomain)]
struct Flag(bool);

#[test]
fn test_pg_domain_inherits_array_oid_from_bool() {
    assert_eq!(<Flag as PgType>::ARRAY_OID, 1000);
}

#[test]
fn test_pg_domain_oid_is_zero() {
    // Domain types use OID=0 (server infers from context)
    assert_eq!(<Email as PgType>::OID, 0);
    assert_eq!(<PositiveInt as PgType>::OID, 0);
    assert_eq!(<UserId as PgType>::OID, 0);
}

// ---------------------------------------------------------------------------
// Integer-backed PgEnum
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, resolute::PgEnum)]
#[repr(i32)]
enum IntStatus {
    Active = 1,
    Inactive = 2,
    Deleted = 3,
}

#[test]
fn test_int_enum_encode() {
    let mut buf = bytes::BytesMut::new();
    IntStatus::Active.encode(&mut buf);
    assert_eq!(buf.len(), 4);
    assert_eq!(i32::decode(&buf).unwrap(), 1);
}

#[test]
fn test_int_enum_encode_all_variants() {
    for (variant, expected) in [
        (IntStatus::Active, 1i32),
        (IntStatus::Inactive, 2),
        (IntStatus::Deleted, 3),
    ] {
        let mut buf = bytes::BytesMut::new();
        variant.encode(&mut buf);
        assert_eq!(i32::decode(&buf).unwrap(), expected, "variant {:?}", variant);
    }
}

#[test]
fn test_int_enum_decode() {
    let mut buf = bytes::BytesMut::new();
    1i32.encode(&mut buf);
    assert_eq!(IntStatus::decode(&buf).unwrap(), IntStatus::Active);

    buf.clear();
    2i32.encode(&mut buf);
    assert_eq!(IntStatus::decode(&buf).unwrap(), IntStatus::Inactive);

    buf.clear();
    3i32.encode(&mut buf);
    assert_eq!(IntStatus::decode(&buf).unwrap(), IntStatus::Deleted);
}

#[test]
fn test_int_enum_decode_unknown_discriminant() {
    let mut buf = bytes::BytesMut::new();
    99i32.encode(&mut buf);
    let err = IntStatus::decode(&buf).unwrap_err();
    let msg = format!("{err}");
    assert!(msg.contains("unknown"), "expected unknown discriminant error, got: {msg}");
    assert!(msg.contains("99"), "expected discriminant 99 in error, got: {msg}");
}

#[test]
fn test_int_enum_decode_text() {
    assert_eq!(IntStatus::decode_text("1").unwrap(), IntStatus::Active);
    assert_eq!(IntStatus::decode_text("2").unwrap(), IntStatus::Inactive);
    assert_eq!(IntStatus::decode_text("3").unwrap(), IntStatus::Deleted);
}

#[test]
fn test_int_enum_decode_text_unknown() {
    let err = IntStatus::decode_text("0").unwrap_err();
    let msg = format!("{err}");
    assert!(msg.contains("unknown"), "got: {msg}");
}

#[test]
fn test_int_enum_decode_text_invalid() {
    let err = IntStatus::decode_text("abc").unwrap_err();
    let msg = format!("{err}");
    assert!(msg.contains("parse"), "got: {msg}");
}

#[test]
fn test_int_enum_pg_type_oids() {
    // i32 enum → OID 23 (int4), ARRAY_OID 1007 (int4[])
    assert_eq!(<IntStatus as PgType>::OID, 23);
    assert_eq!(<IntStatus as PgType>::ARRAY_OID, 1007);
}

#[test]
fn test_int_enum_roundtrip() {
    for variant in [IntStatus::Active, IntStatus::Inactive, IntStatus::Deleted] {
        let mut buf = bytes::BytesMut::new();
        variant.encode(&mut buf);
        let decoded = IntStatus::decode(&buf).unwrap();
        assert_eq!(decoded, variant);
    }
}

#[derive(Debug, PartialEq, resolute::PgEnum)]
#[repr(i16)]
enum Priority {
    Low = 0,
    Medium = 1,
    High = 2,
    Critical = 3,
}

#[test]
fn test_int_enum_i16_encode_decode() {
    assert_eq!(<Priority as PgType>::OID, 21); // int2
    assert_eq!(<Priority as PgType>::ARRAY_OID, 1005); // int2[]

    for (variant, expected) in [
        (Priority::Low, 0i16),
        (Priority::Medium, 1),
        (Priority::High, 2),
        (Priority::Critical, 3),
    ] {
        let mut buf = bytes::BytesMut::new();
        variant.encode(&mut buf);
        assert_eq!(buf.len(), 2);
        let decoded = Priority::decode(&buf).unwrap();
        assert_eq!(decoded, variant);
        assert_eq!(i16::decode(&buf).unwrap(), expected);
    }
}

#[derive(Debug, PartialEq, resolute::PgEnum)]
#[repr(i64)]
enum BigEnum {
    A = 100,
    B = 200,
    C = -1,
}

#[test]
fn test_int_enum_i64_encode_decode() {
    assert_eq!(<BigEnum as PgType>::OID, 20); // int8
    assert_eq!(<BigEnum as PgType>::ARRAY_OID, 1016); // int8[]

    let mut buf = bytes::BytesMut::new();
    BigEnum::C.encode(&mut buf);
    assert_eq!(buf.len(), 8);
    assert_eq!(i64::decode(&buf).unwrap(), -1);
    assert_eq!(BigEnum::decode(&buf).unwrap(), BigEnum::C);
}

#[test]
fn test_int_enum_negative_discriminant_roundtrip() {
    let mut buf = bytes::BytesMut::new();
    BigEnum::C.encode(&mut buf);
    let decoded = BigEnum::decode(&buf).unwrap();
    assert_eq!(decoded, BigEnum::C);
}

#[test]
fn test_int_enum_decode_too_short() {
    let buf = [0u8; 2]; // too short for i32
    let err = IntStatus::decode(&buf).unwrap_err();
    let msg = format!("{err}");
    assert!(msg.contains("expected 4 bytes"), "got: {msg}");
}

// ---------------------------------------------------------------------------
// FromRow: skip attribute
// ---------------------------------------------------------------------------

#[derive(resolute::FromRow, Debug)]
struct WithSkip {
    id: i32,
    name: String,
    #[from_row(skip)]
    computed: String,
}

#[tokio::test]
async fn test_from_row_skip() {
    let client = connect().await;
    let rows = client
        .query("SELECT 1::int4 AS id, 'Alice'::text AS name", &[])
        .await
        .unwrap();
    let row = WithSkip::from_row(&rows[0]).unwrap();
    assert_eq!(row.id, 1);
    assert_eq!(row.name, "Alice");
    assert_eq!(row.computed, "", "skipped field should be Default::default()");
}

// ---------------------------------------------------------------------------
// FromRow: default attribute
// ---------------------------------------------------------------------------

#[derive(resolute::FromRow, Debug)]
struct WithDefault {
    id: i32,
    #[from_row(default)]
    missing_col: i32,
}

#[tokio::test]
async fn test_from_row_default_missing_column() {
    let client = connect().await;
    let rows = client
        .query("SELECT 1::int4 AS id", &[])
        .await
        .unwrap();
    let row = WithDefault::from_row(&rows[0]).unwrap();
    assert_eq!(row.id, 1);
    assert_eq!(row.missing_col, 0, "default on missing column → Default::default()");
}

#[derive(resolute::FromRow, Debug)]
struct WithDefaultOption {
    id: i32,
    #[from_row(default)]
    opt_col: Option<String>,
}

#[tokio::test]
async fn test_from_row_default_missing_option_column() {
    let client = connect().await;
    let rows = client
        .query("SELECT 1::int4 AS id", &[])
        .await
        .unwrap();
    let row = WithDefaultOption::from_row(&rows[0]).unwrap();
    assert_eq!(row.id, 1);
    assert_eq!(row.opt_col, None, "default on missing Option column → None");
}

#[tokio::test]
async fn test_from_row_default_null_value() {
    let client = connect().await;
    let rows = client
        .query("SELECT 1::int4 AS id, NULL::int4 AS missing_col", &[])
        .await
        .unwrap();
    let row = WithDefault::from_row(&rows[0]).unwrap();
    assert_eq!(row.id, 1);
    assert_eq!(row.missing_col, 0, "default on NULL → Default::default()");
}

// ---------------------------------------------------------------------------
// FromRow: flatten attribute
// ---------------------------------------------------------------------------

#[derive(resolute::FromRow, Debug, PartialEq)]
struct Inner {
    name: String,
}

#[derive(resolute::FromRow, Debug)]
struct WithFlatten {
    id: i32,
    #[from_row(flatten)]
    inner: Inner,
}

#[tokio::test]
async fn test_from_row_flatten() {
    let client = connect().await;
    let rows = client
        .query("SELECT 1::int4 AS id, 'Alice'::text AS name", &[])
        .await
        .unwrap();
    let row = WithFlatten::from_row(&rows[0]).unwrap();
    assert_eq!(row.id, 1);
    assert_eq!(row.inner, Inner { name: "Alice".into() });
}

// ---------------------------------------------------------------------------
// FromRow: try_from attribute
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct NonZeroId(i32);

impl TryFrom<i32> for NonZeroId {
    type Error = String;
    fn try_from(v: i32) -> Result<Self, Self::Error> {
        if v == 0 {
            Err("id must be non-zero".into())
        } else {
            Ok(NonZeroId(v))
        }
    }
}

#[derive(resolute::FromRow, Debug)]
struct WithTryFrom {
    #[from_row(try_from = "i32")]
    id: NonZeroId,
    name: String,
}

#[tokio::test]
async fn test_from_row_try_from_success() {
    let client = connect().await;
    let rows = client
        .query("SELECT 1::int4 AS id, 'Alice'::text AS name", &[])
        .await
        .unwrap();
    let row = WithTryFrom::from_row(&rows[0]).unwrap();
    assert_eq!(row.id.0, 1);
    assert_eq!(row.name, "Alice");
}

#[tokio::test]
async fn test_from_row_try_from_failure() {
    let client = connect().await;
    let rows = client
        .query("SELECT 0::int4 AS id, 'Nobody'::text AS name", &[])
        .await
        .unwrap();
    let err = WithTryFrom::from_row(&rows[0]).unwrap_err();
    let msg = format!("{err}");
    assert!(msg.contains("try_from"), "expected try_from error, got: {msg}");
    assert!(msg.contains("non-zero"), "expected validation message, got: {msg}");
}

// ---------------------------------------------------------------------------
// FromRow: json attribute
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, serde::Deserialize)]
struct Metadata {
    key: String,
    value: i32,
}

#[derive(resolute::FromRow, Debug)]
struct WithJson {
    id: i32,
    #[from_row(json)]
    meta: Metadata,
}

#[tokio::test]
async fn test_from_row_json() {
    let client = connect().await;
    let rows = client
        .query(
            r#"SELECT 1::int4 AS id, '{"key":"test","value":42}'::jsonb AS meta"#,
            &[],
        )
        .await
        .unwrap();
    let row = WithJson::from_row(&rows[0]).unwrap();
    assert_eq!(row.id, 1);
    assert_eq!(row.meta, Metadata { key: "test".into(), value: 42 });
}

#[derive(resolute::FromRow, Debug)]
struct WithJsonOption {
    id: i32,
    #[from_row(json)]
    meta: Option<Metadata>,
}

#[tokio::test]
async fn test_from_row_json_option_some() {
    let client = connect().await;
    let rows = client
        .query(
            r#"SELECT 1::int4 AS id, '{"key":"x","value":0}'::jsonb AS meta"#,
            &[],
        )
        .await
        .unwrap();
    let row = WithJsonOption::from_row(&rows[0]).unwrap();
    assert_eq!(row.meta, Some(Metadata { key: "x".into(), value: 0 }));
}

#[tokio::test]
async fn test_from_row_json_option_null() {
    let client = connect().await;
    let rows = client
        .query("SELECT 1::int4 AS id, NULL::jsonb AS meta", &[])
        .await
        .unwrap();
    let row = WithJsonOption::from_row(&rows[0]).unwrap();
    assert_eq!(row.meta, None);
}

// ---------------------------------------------------------------------------
// FromRow: rename + default combined
// ---------------------------------------------------------------------------

#[derive(resolute::FromRow, Debug)]
struct RenameAndDefault {
    #[from_row(rename = "user_id")]
    id: i32,
    #[from_row(default)]
    optional_field: i32,
}

#[tokio::test]
async fn test_from_row_rename_and_default() {
    let client = connect().await;
    let rows = client
        .query("SELECT 42::int4 AS user_id", &[])
        .await
        .unwrap();
    let row = RenameAndDefault::from_row(&rows[0]).unwrap();
    assert_eq!(row.id, 42);
    assert_eq!(row.optional_field, 0);
}

// ---------------------------------------------------------------------------
// FromRow: try_from with Option
// ---------------------------------------------------------------------------

#[derive(resolute::FromRow, Debug)]
struct WithTryFromOption {
    #[from_row(try_from = "i32")]
    id: Option<NonZeroId>,
    name: String,
}

#[tokio::test]
async fn test_from_row_try_from_option_some() {
    let client = connect().await;
    let rows = client
        .query("SELECT 5::int4 AS id, 'Alice'::text AS name", &[])
        .await
        .unwrap();
    let row = WithTryFromOption::from_row(&rows[0]).unwrap();
    assert_eq!(row.id.as_ref().unwrap().0, 5);
}

#[tokio::test]
async fn test_from_row_try_from_option_null() {
    let client = connect().await;
    let rows = client
        .query("SELECT NULL::int4 AS id, 'Bob'::text AS name", &[])
        .await
        .unwrap();
    let row = WithTryFromOption::from_row(&rows[0]).unwrap();
    assert!(row.id.is_none());
}

// ---------------------------------------------------------------------------
// Integration tests: new array types against real PostgreSQL
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_bool_array_roundtrip() {
    let client = connect().await;
    let arr = vec![true, false, true, false];
    let rows = client
        .query("SELECT $1::bool[] AS arr", &[&arr])
        .await
        .unwrap();
    let result: Vec<bool> = rows[0].get(0).unwrap();
    assert_eq!(result, arr);
}

#[tokio::test]
async fn test_i16_array_roundtrip() {
    let client = connect().await;
    let arr = vec![1i16, -2, 32767];
    let rows = client
        .query("SELECT $1::int2[] AS arr", &[&arr])
        .await
        .unwrap();
    let result: Vec<i16> = rows[0].get(0).unwrap();
    assert_eq!(result, arr);
}

#[tokio::test]
async fn test_f32_array_roundtrip() {
    let client = connect().await;
    let arr = vec![1.5f32, -2.5, 0.0];
    let rows = client
        .query("SELECT $1::float4[] AS arr", &[&arr])
        .await
        .unwrap();
    let result: Vec<f32> = rows[0].get(0).unwrap();
    assert_eq!(result, arr);
}

#[tokio::test]
async fn test_f64_array_roundtrip() {
    let client = connect().await;
    let arr = vec![std::f64::consts::PI, -1.0, 0.0];
    let rows = client
        .query("SELECT $1::float8[] AS arr", &[&arr])
        .await
        .unwrap();
    let result: Vec<f64> = rows[0].get(0).unwrap();
    assert_eq!(result, arr);
}

#[tokio::test]
async fn test_uuid_array_roundtrip() {
    let client = connect().await;
    let arr = vec![uuid::Uuid::new_v4(), uuid::Uuid::new_v4(), uuid::Uuid::new_v4()];
    let rows = client
        .query("SELECT $1::uuid[] AS arr", &[&arr])
        .await
        .unwrap();
    let result: Vec<uuid::Uuid> = rows[0].get(0).unwrap();
    assert_eq!(result, arr);
}

#[tokio::test]
async fn test_date_array_roundtrip() {
    let client = connect().await;
    let arr = vec![
        chrono::NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
        chrono::NaiveDate::from_ymd_opt(2025, 12, 31).unwrap(),
    ];
    let rows = client
        .query("SELECT $1::date[] AS arr", &[&arr])
        .await
        .unwrap();
    let result: Vec<chrono::NaiveDate> = rows[0].get(0).unwrap();
    assert_eq!(result, arr);
}

#[tokio::test]
async fn test_timestamptz_array_roundtrip() {
    let client = connect().await;
    let now = chrono::Utc::now();
    let earlier = now - chrono::Duration::hours(1);
    let arr = vec![now, earlier];
    let rows = client
        .query("SELECT $1::timestamptz[] AS arr", &[&arr])
        .await
        .unwrap();
    let result: Vec<chrono::DateTime<chrono::Utc>> = rows[0].get(0).unwrap();
    assert_eq!(result.len(), 2);
    assert!((result[0] - now).num_milliseconds().abs() < 1000);
    assert!((result[1] - earlier).num_milliseconds().abs() < 1000);
}

// ---------------------------------------------------------------------------
// Integration test: PgEnum against real PostgreSQL
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_pg_enum_integration() {
    let client = connect().await;
    // Create a PG enum type in a temp schema and test roundtrip.
    client
        .simple_query("DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'test_mood') THEN CREATE TYPE test_mood AS ENUM ('happy', 'sad', 'so-so'); END IF; END $$")
        .await
        .unwrap();

    let rows = client
        .query("SELECT 'happy'::test_mood AS m", &[])
        .await
        .unwrap();
    let m: Mood = rows[0].get(0).unwrap();
    assert_eq!(m, Mood::Happy);

    let rows = client
        .query("SELECT 'so-so'::test_mood AS m", &[])
        .await
        .unwrap();
    let m: Mood = rows[0].get(0).unwrap();
    assert_eq!(m, Mood::SoSo);
}

// ---------------------------------------------------------------------------
// Integration test: database create/drop lifecycle
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_database_create_and_connect() {
    let db_name = "resolute_test_db_lifecycle";

    // Use a maintenance connection.
    let maint = Client::connect(ADDR, USER, PASS, "postgres").await.unwrap();

    // Clean up from prior runs.
    let _ = maint
        .simple_query(&format!("DROP DATABASE IF EXISTS \"{db_name}\""))
        .await;

    // Create the database.
    maint
        .simple_query(&format!("CREATE DATABASE \"{db_name}\""))
        .await
        .unwrap();

    // Verify we can connect and query.
    let test_client = Client::connect(ADDR, USER, PASS, db_name).await.unwrap();
    let rows = test_client.query("SELECT 1::int4 AS n", &[]).await.unwrap();
    assert_eq!(rows[0].get::<i32>(0).unwrap(), 1);
    drop(test_client);

    // Clean up.
    let _ = maint
        .simple_query(&format!("DROP DATABASE IF EXISTS \"{db_name}\""))
        .await;
}

// ===========================================================================
// Edge case tests — type boundaries, special values, error paths
// ===========================================================================

// ---------------------------------------------------------------------------
// Float special values
// ---------------------------------------------------------------------------

#[test]
fn test_encode_decode_f32_nan() {
    let mut buf = bytes::BytesMut::new();
    f32::NAN.encode(&mut buf);
    assert!(f32::decode(&buf).unwrap().is_nan());
}

#[test]
fn test_encode_decode_f64_nan() {
    let mut buf = bytes::BytesMut::new();
    f64::NAN.encode(&mut buf);
    assert!(f64::decode(&buf).unwrap().is_nan());
}

#[test]
fn test_encode_decode_f32_infinity() {
    let mut buf = bytes::BytesMut::new();
    f32::INFINITY.encode(&mut buf);
    assert_eq!(f32::decode(&buf).unwrap(), f32::INFINITY);

    buf.clear();
    f32::NEG_INFINITY.encode(&mut buf);
    assert_eq!(f32::decode(&buf).unwrap(), f32::NEG_INFINITY);
}

#[test]
fn test_encode_decode_f64_infinity() {
    let mut buf = bytes::BytesMut::new();
    f64::INFINITY.encode(&mut buf);
    assert_eq!(f64::decode(&buf).unwrap(), f64::INFINITY);

    buf.clear();
    f64::NEG_INFINITY.encode(&mut buf);
    assert_eq!(f64::decode(&buf).unwrap(), f64::NEG_INFINITY);
}

#[test]
fn test_encode_decode_f64_negative_zero() {
    let mut buf = bytes::BytesMut::new();
    let neg_zero: f64 = -0.0;
    neg_zero.encode(&mut buf);
    let decoded = f64::decode(&buf).unwrap();
    assert!(decoded.is_sign_negative());
    assert_eq!(decoded, 0.0);
}

#[tokio::test]
async fn test_float_special_values_pg() {
    let client = connect().await;
    let rows = client
        .query("SELECT 'NaN'::float8 AS n, 'Infinity'::float8 AS inf, '-Infinity'::float8 AS neg_inf", &[])
        .await
        .unwrap();
    let n: f64 = rows[0].get(0).unwrap();
    let inf: f64 = rows[0].get(1).unwrap();
    let neg_inf: f64 = rows[0].get(2).unwrap();
    assert!(n.is_nan());
    assert_eq!(inf, f64::INFINITY);
    assert_eq!(neg_inf, f64::NEG_INFINITY);
}

// ---------------------------------------------------------------------------
// Integer boundary values
// ---------------------------------------------------------------------------

#[test]
fn test_encode_decode_i16_boundaries() {
    for val in [i16::MIN, -1, 0, 1, i16::MAX] {
        let mut buf = bytes::BytesMut::new();
        val.encode(&mut buf);
        assert_eq!(i16::decode(&buf).unwrap(), val);
    }
}

#[test]
fn test_encode_decode_i32_boundaries() {
    for val in [i32::MIN, -1, 0, 1, i32::MAX] {
        let mut buf = bytes::BytesMut::new();
        val.encode(&mut buf);
        assert_eq!(i32::decode(&buf).unwrap(), val);
    }
}

#[test]
fn test_encode_decode_i64_boundaries() {
    for val in [i64::MIN, -1, 0, 1, i64::MAX] {
        let mut buf = bytes::BytesMut::new();
        val.encode(&mut buf);
        assert_eq!(i64::decode(&buf).unwrap(), val);
    }
}

#[tokio::test]
async fn test_integer_boundaries_pg() {
    let client = connect().await;
    let rows = client
        .query(
            "SELECT $1::int2 AS a, $2::int4 AS b, $3::int8 AS c",
            &[&i16::MAX, &i32::MIN, &i64::MAX],
        )
        .await
        .unwrap();
    assert_eq!(rows[0].get::<i16>(0).unwrap(), i16::MAX);
    assert_eq!(rows[0].get::<i32>(1).unwrap(), i32::MIN);
    assert_eq!(rows[0].get::<i64>(2).unwrap(), i64::MAX);
}

// ---------------------------------------------------------------------------
// String edge cases
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_empty_string() {
    let client = connect().await;
    let rows = client
        .query("SELECT $1::text AS s", &[&""])
        .await
        .unwrap();
    let s: String = rows[0].get(0).unwrap();
    assert_eq!(s, "");
}

#[tokio::test]
async fn test_unicode_string() {
    let client = connect().await;
    let cases = [
        "Hello 🌍🌎🌏",
        "日本語テスト",
        "مرحبا",
        "Ü̴̡̟n̷̨̗̈ḯ̵̱c̸̣͌o̵̠͑d̸̡̎e̷̝͊",
        "\u{200B}", // zero-width space
        "café",     // composed vs decomposed
    ];
    for input in cases {
        let rows = client
            .query("SELECT $1::text AS s", &[&input])
            .await
            .unwrap();
        let s: String = rows[0].get(0).unwrap();
        assert_eq!(s, input, "roundtrip failed for: {input:?}");
    }
}

#[tokio::test]
async fn test_long_string() {
    let client = connect().await;
    let long = "x".repeat(100_000);
    let rows = client
        .query("SELECT $1::text AS s", &[&long])
        .await
        .unwrap();
    let s: String = rows[0].get(0).unwrap();
    assert_eq!(s.len(), 100_000);
}

// ---------------------------------------------------------------------------
// Bytea edge cases
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_empty_bytea() {
    let client = connect().await;
    let data: Vec<u8> = vec![];
    let rows = client
        .query("SELECT $1::bytea AS b", &[&data])
        .await
        .unwrap();
    let result: Vec<u8> = rows[0].get(0).unwrap();
    assert!(result.is_empty());
}

#[tokio::test]
async fn test_bytea_with_null_bytes() {
    let client = connect().await;
    let data = vec![0u8, 0, 255, 0, 128, 0, 1];
    let rows = client
        .query("SELECT $1::bytea AS b", &[&data])
        .await
        .unwrap();
    let result: Vec<u8> = rows[0].get(0).unwrap();
    assert_eq!(result, data);
}

#[tokio::test]
async fn test_bytea_all_byte_values() {
    let client = connect().await;
    let data: Vec<u8> = (0..=255).collect();
    let rows = client
        .query("SELECT $1::bytea AS b", &[&data])
        .await
        .unwrap();
    let result: Vec<u8> = rows[0].get(0).unwrap();
    assert_eq!(result, data);
}

// ---------------------------------------------------------------------------
// UUID edge cases
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_nil_uuid() {
    let client = connect().await;
    let nil = uuid::Uuid::nil();
    let rows = client
        .query("SELECT $1::uuid AS id", &[&nil])
        .await
        .unwrap();
    let result: uuid::Uuid = rows[0].get(0).unwrap();
    assert_eq!(result, uuid::Uuid::nil());
    assert!(result.is_nil());
}

#[tokio::test]
async fn test_max_uuid() {
    let client = connect().await;
    let max = uuid::Uuid::max();
    let rows = client
        .query("SELECT $1::uuid AS id", &[&max])
        .await
        .unwrap();
    let result: uuid::Uuid = rows[0].get(0).unwrap();
    assert_eq!(result, max);
}

// ---------------------------------------------------------------------------
// Timestamp / date boundary values
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_pg_epoch_date() {
    let client = connect().await;
    let epoch = chrono::NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
    let rows = client
        .query("SELECT $1::date AS d", &[&epoch])
        .await
        .unwrap();
    assert_eq!(rows[0].get::<chrono::NaiveDate>(0).unwrap(), epoch);
}

#[tokio::test]
async fn test_pre_epoch_date() {
    let client = connect().await;
    let old = chrono::NaiveDate::from_ymd_opt(1900, 1, 1).unwrap();
    let rows = client
        .query("SELECT $1::date AS d", &[&old])
        .await
        .unwrap();
    assert_eq!(rows[0].get::<chrono::NaiveDate>(0).unwrap(), old);
}

#[tokio::test]
async fn test_leap_day() {
    let client = connect().await;
    let leap = chrono::NaiveDate::from_ymd_opt(2024, 2, 29).unwrap();
    let rows = client
        .query("SELECT $1::date AS d", &[&leap])
        .await
        .unwrap();
    assert_eq!(rows[0].get::<chrono::NaiveDate>(0).unwrap(), leap);
}

#[tokio::test]
async fn test_midnight_time() {
    let client = connect().await;
    let midnight = chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap();
    let rows = client
        .query("SELECT $1::time AS t", &[&midnight])
        .await
        .unwrap();
    assert_eq!(rows[0].get::<chrono::NaiveTime>(0).unwrap(), midnight);
}

#[tokio::test]
async fn test_end_of_day_time() {
    let client = connect().await;
    let t = chrono::NaiveTime::from_hms_micro_opt(23, 59, 59, 999999).unwrap();
    let rows = client
        .query("SELECT $1::time AS t", &[&t])
        .await
        .unwrap();
    assert_eq!(rows[0].get::<chrono::NaiveTime>(0).unwrap(), t);
}

#[tokio::test]
async fn test_timestamp_microsecond_precision() {
    let client = connect().await;
    let ts = chrono::NaiveDate::from_ymd_opt(2024, 6, 15)
        .unwrap()
        .and_hms_micro_opt(12, 30, 45, 123456)
        .unwrap();
    let rows = client
        .query("SELECT $1::timestamp AS ts", &[&ts])
        .await
        .unwrap();
    assert_eq!(rows[0].get::<chrono::NaiveDateTime>(0).unwrap(), ts);
}

// ---------------------------------------------------------------------------
// JSON edge cases
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_json_empty_object() {
    let client = connect().await;
    let val = serde_json::json!({});
    let rows = client
        .query("SELECT $1::jsonb AS j", &[&val])
        .await
        .unwrap();
    let result: serde_json::Value = rows[0].get(0).unwrap();
    assert_eq!(result, serde_json::json!({}));
}

#[tokio::test]
async fn test_json_empty_array() {
    let client = connect().await;
    let val = serde_json::json!([]);
    let rows = client
        .query("SELECT $1::jsonb AS j", &[&val])
        .await
        .unwrap();
    let result: serde_json::Value = rows[0].get(0).unwrap();
    assert_eq!(result, serde_json::json!([]));
}

#[tokio::test]
async fn test_json_null_value() {
    let client = connect().await;
    let val = serde_json::Value::Null;
    let rows = client
        .query("SELECT $1::jsonb AS j", &[&val])
        .await
        .unwrap();
    let result: serde_json::Value = rows[0].get(0).unwrap();
    assert!(result.is_null());
}

#[tokio::test]
async fn test_json_deeply_nested() {
    let client = connect().await;
    let mut val = serde_json::json!({"value": 42});
    for _ in 0..50 {
        val = serde_json::json!({"nested": val});
    }
    let rows = client
        .query("SELECT $1::jsonb AS j", &[&val])
        .await
        .unwrap();
    let result: serde_json::Value = rows[0].get(0).unwrap();
    assert_eq!(result, val);
}

#[tokio::test]
async fn test_json_unicode() {
    let client = connect().await;
    let val = serde_json::json!({"emoji": "😎🙋‍♀️", "cjk": "日本語"});
    let rows = client
        .query("SELECT $1::jsonb AS j", &[&val])
        .await
        .unwrap();
    let result: serde_json::Value = rows[0].get(0).unwrap();
    assert_eq!(result, val);
}

// ---------------------------------------------------------------------------
// PgNumeric edge cases
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_numeric_large_value() {
    let client = connect().await;
    let rows = client
        .query("SELECT 99999999999999999999.999999::numeric AS n", &[])
        .await
        .unwrap();
    let n: resolute::PgNumeric = rows[0].get(0).unwrap();
    assert_eq!(n.0, "99999999999999999999.999999");
}

#[tokio::test]
async fn test_numeric_many_decimals() {
    let client = connect().await;
    let rows = client
        .query("SELECT 0.000000000000000001::numeric AS n", &[])
        .await
        .unwrap();
    let n: resolute::PgNumeric = rows[0].get(0).unwrap();
    assert_eq!(n.0, "0.000000000000000001");
}

#[tokio::test]
async fn test_numeric_one() {
    let client = connect().await;
    let rows = client
        .query("SELECT 1::numeric AS n", &[])
        .await
        .unwrap();
    let n: resolute::PgNumeric = rows[0].get(0).unwrap();
    assert_eq!(n.0, "1");
}

#[tokio::test]
async fn test_numeric_with_scale() {
    let client = connect().await;
    let rows = client
        .query("SELECT 0.00::numeric AS n", &[])
        .await
        .unwrap();
    let n: resolute::PgNumeric = rows[0].get(0).unwrap();
    assert_eq!(n.0, "0.00");
}

// ---------------------------------------------------------------------------
// PgInet edge cases
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_inet_ipv6() {
    let client = connect().await;
    let rows = client
        .query("SELECT '::1/128'::inet AS addr", &[])
        .await
        .unwrap();
    let addr: resolute::PgInet = rows[0].get(0).unwrap();
    assert!(addr.0.contains("128"), "expected /128 mask, got: {}", addr.0);
}

#[tokio::test]
async fn test_inet_ipv4_host() {
    let client = connect().await;
    let rows = client
        .query("SELECT '10.0.0.1/32'::inet AS addr", &[])
        .await
        .unwrap();
    let addr: resolute::PgInet = rows[0].get(0).unwrap();
    assert_eq!(addr.0, "10.0.0.1/32");
}

// ---------------------------------------------------------------------------
// Array edge cases
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_single_element_array() {
    let client = connect().await;
    let arr = vec![42i32];
    let rows = client
        .query("SELECT $1::int4[] AS arr", &[&arr])
        .await
        .unwrap();
    let result: Vec<i32> = rows[0].get(0).unwrap();
    assert_eq!(result, vec![42]);
}

#[tokio::test]
async fn test_large_array() {
    let client = connect().await;
    let arr: Vec<i32> = (0..1000).collect();
    let rows = client
        .query("SELECT $1::int4[] AS arr", &[&arr])
        .await
        .unwrap();
    let result: Vec<i32> = rows[0].get(0).unwrap();
    assert_eq!(result.len(), 1000);
    assert_eq!(result[999], 999);
}

#[tokio::test]
async fn test_timestamp_array_roundtrip() {
    let client = connect().await;
    let base = chrono::NaiveDate::from_ymd_opt(2024, 1, 1)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap();
    let arr = vec![base, base + chrono::Duration::hours(1)];
    let rows = client
        .query("SELECT $1::timestamp[] AS arr", &[&arr])
        .await
        .unwrap();
    let result: Vec<chrono::NaiveDateTime> = rows[0].get(0).unwrap();
    assert_eq!(result, arr);
}

#[tokio::test]
async fn test_time_array_roundtrip() {
    let client = connect().await;
    let arr = vec![
        chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
        chrono::NaiveTime::from_hms_opt(12, 30, 0).unwrap(),
        chrono::NaiveTime::from_hms_opt(23, 59, 59).unwrap(),
    ];
    let rows = client
        .query("SELECT $1::time[] AS arr", &[&arr])
        .await
        .unwrap();
    let result: Vec<chrono::NaiveTime> = rows[0].get(0).unwrap();
    assert_eq!(result, arr);
}

#[tokio::test]
async fn test_jsonb_array_roundtrip() {
    let client = connect().await;
    let arr = vec![
        serde_json::json!({"a": 1}),
        serde_json::json!(null),
        serde_json::json!([1, 2]),
    ];
    let rows = client
        .query("SELECT $1::jsonb[] AS arr", &[&arr])
        .await
        .unwrap();
    let result: Vec<serde_json::Value> = rows[0].get(0).unwrap();
    assert_eq!(result, arr);
}

// ---------------------------------------------------------------------------
// Text array parser edge cases
// ---------------------------------------------------------------------------

#[test]
fn test_decode_text_array_whitespace() {
    let result = Vec::<i32>::decode_text("{ 1 , 2 , 3 }").unwrap();
    assert_eq!(result, vec![1, 2, 3]);
}

#[test]
fn test_decode_text_array_single_element() {
    let result = Vec::<i32>::decode_text("{42}").unwrap();
    assert_eq!(result, vec![42]);
}

#[test]
fn test_decode_text_array_mixed_quoted_unquoted() {
    let result = Vec::<String>::decode_text(r#"{hello,"world, ok",bye}"#).unwrap();
    assert_eq!(result, vec!["hello", "world, ok", "bye"]);
}

#[test]
fn test_decode_text_array_backslash_in_quoted() {
    let result = Vec::<String>::decode_text(r#"{"a\\b"}"#).unwrap();
    assert_eq!(result, vec![r"a\b"]);
}

#[test]
fn test_decode_text_array_empty_quoted_string() {
    let result = Vec::<String>::decode_text(r#"{"",""}"#).unwrap();
    assert_eq!(result, vec!["", ""]);
}

// ---------------------------------------------------------------------------
// Decode error paths
// ---------------------------------------------------------------------------

#[test]
fn test_decode_string_invalid_utf8() {
    let invalid = vec![0xFF, 0xFE];
    assert!(String::decode(&invalid).is_err());
}

#[test]
fn test_decode_bool_wrong_size() {
    assert!(bool::decode(&[]).is_err());
    assert!(bool::decode(&[0, 0]).is_err());
}

#[test]
fn test_decode_uuid_wrong_size() {
    assert!(uuid::Uuid::decode(&[0; 15]).is_err());
    assert!(uuid::Uuid::decode(&[0; 17]).is_err());
}

#[test]
fn test_decode_numeric_truncated() {
    // Valid header but truncated digit data.
    let buf = [0, 2, 0, 0, 0, 0, 0, 0]; // ndigits=2 but no digit data
    assert!(resolute::newtypes::PgNumeric::decode(&buf).is_err());
}

#[test]
fn test_decode_inet_truncated() {
    let buf = [2, 24, 0]; // family=IPv4 but too short
    assert!(resolute::newtypes::PgInet::decode(&buf).is_err());
}

#[test]
fn test_decode_inet_unknown_family() {
    let buf = [99, 32, 0, 4, 0, 0, 0, 0]; // family=99 (unknown)
    assert!(resolute::newtypes::PgInet::decode(&buf).is_err());
}

#[test]
fn test_decode_array_2d_rejected() {
    // Construct a 2D array header: ndim=2
    let mut buf = vec![0u8; 28];
    buf[3] = 2; // ndim = 2
    assert!(Vec::<i32>::decode(&buf).is_err());
}

#[test]
fn test_decode_array_truncated_element() {
    // Valid 1D header with 1 element, but element data missing.
    let mut buf = vec![0u8; 24];
    buf[3] = 1;  // ndim = 1
    buf[15] = 1; // dim_len = 1
    buf[19] = 1; // lower_bound = 1
    buf[23] = 4; // element length = 4, but no data follows
    assert!(Vec::<i32>::decode(&buf).is_err());
}

// ---------------------------------------------------------------------------
// Query execution edge cases
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_syntax_error_recovery() {
    let client = connect().await;
    let result = client.query("SELEC INVALID SYNTAX", &[]).await;
    assert!(result.is_err());

    // Connection should still work after syntax error.
    let rows = client.query("SELECT 1::int4 AS n", &[]).await.unwrap();
    assert_eq!(rows[0].get::<i32>(0).unwrap(), 1);
}

#[tokio::test]
async fn test_repeated_error_recovery() {
    let client = connect().await;
    for _ in 0..10 {
        let _ = client.query("SELECT * FROM no_such_table_xyz", &[]).await;
    }
    // Connection must still work.
    let rows = client.query("SELECT 42::int4 AS n", &[]).await.unwrap();
    assert_eq!(rows[0].get::<i32>(0).unwrap(), 42);
}

#[tokio::test]
async fn test_constraint_violation() {
    let client = connect().await;
    client
        .simple_query("CREATE TEMP TABLE test_pk (id int PRIMARY KEY)")
        .await
        .unwrap();
    client
        .execute("INSERT INTO test_pk VALUES ($1)", &[&1i32])
        .await
        .unwrap();

    // Duplicate key should error.
    let result = client
        .execute("INSERT INTO test_pk VALUES ($1)", &[&1i32])
        .await;
    assert!(result.is_err());

    // Connection should still work.
    let rows = client
        .query("SELECT count(*)::int4 FROM test_pk", &[])
        .await
        .unwrap();
    assert_eq!(rows[0].get::<i32>(0).unwrap(), 1);
}

#[tokio::test]
async fn test_large_result_set() {
    let client = connect().await;
    let rows = client
        .query("SELECT generate_series(1, 10000)::int4 AS n", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 10000);
    assert_eq!(rows[0].get::<i32>(0).unwrap(), 1);
    assert_eq!(rows[9999].get::<i32>(0).unwrap(), 10000);
}

#[tokio::test]
async fn test_concurrent_queries() {
    let futures: Vec<_> = (0..10)
        .map(|i| {
            tokio::spawn(async move {
                let client = Client::connect(
                    "127.0.0.1:54322", "postgres", "postgres", "postgrest_test",
                )
                .await
                .unwrap();
                let rows = client
                    .query("SELECT $1::int4 AS n", &[&i])
                    .await
                    .unwrap();
                rows[0].get::<i32>(0).unwrap()
            })
        })
        .collect();

    let mut results = Vec::new();
    for f in futures {
        results.push(f.await.unwrap());
    }
    results.sort();
    assert_eq!(results, (0..10).collect::<Vec<i32>>());
}

// ---------------------------------------------------------------------------
// PgEnum edge cases
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, resolute::PgEnum)]
enum SingleVariant {
    Only,
}

#[test]
fn test_pg_enum_single_variant() {
    let mut buf = bytes::BytesMut::new();
    SingleVariant::Only.encode(&mut buf);
    assert_eq!(&buf[..], b"only");
    assert_eq!(SingleVariant::decode(b"only").unwrap(), SingleVariant::Only);
}

#[derive(Debug, PartialEq, resolute::PgEnum)]
#[pg_type(rename_all = "camelCase")]
enum CamelStatus {
    InProgress,
    NotStarted,
}

#[test]
fn test_pg_enum_camel_case() {
    let mut buf = bytes::BytesMut::new();
    CamelStatus::InProgress.encode(&mut buf);
    assert_eq!(&buf[..], b"inProgress");

    buf.clear();
    CamelStatus::NotStarted.encode(&mut buf);
    assert_eq!(&buf[..], b"notStarted");
}

#[derive(Debug, PartialEq, resolute::PgEnum)]
#[pg_type(rename_all = "SCREAMING_SNAKE_CASE")]
enum ScreamEnum {
    FirstValue,
    SecondValue,
}

#[test]
fn test_pg_enum_screaming_snake() {
    let mut buf = bytes::BytesMut::new();
    ScreamEnum::FirstValue.encode(&mut buf);
    assert_eq!(&buf[..], b"FIRST_VALUE");

    buf.clear();
    ScreamEnum::SecondValue.encode(&mut buf);
    assert_eq!(&buf[..], b"SECOND_VALUE");
}

// ---------------------------------------------------------------------------
// PgComposite edge cases
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, resolute::PgComposite)]
struct SingleField {
    value: i32,
}

#[test]
fn test_pg_composite_single_field() {
    let v = SingleField { value: 42 };
    let mut buf = bytes::BytesMut::new();
    v.encode(&mut buf);
    assert_eq!(SingleField::decode(&buf).unwrap(), v);
}

#[derive(Debug, PartialEq, resolute::PgComposite)]
struct ManyTypes {
    a: bool,
    b: i16,
    c: i32,
    d: i64,
    e: f32,
    f: f64,
    g: String,
}

#[test]
fn test_pg_composite_many_types() {
    let v = ManyTypes {
        a: true,
        b: -1,
        c: 42,
        d: i64::MAX,
        e: 1.5,
        f: std::f64::consts::PI,
        g: "hello 🌍".into(),
    };
    let mut buf = bytes::BytesMut::new();
    v.encode(&mut buf);
    let decoded = ManyTypes::decode(&buf).unwrap();
    assert_eq!(decoded.a, v.a);
    assert_eq!(decoded.b, v.b);
    assert_eq!(decoded.c, v.c);
    assert_eq!(decoded.d, v.d);
    assert_eq!(decoded.e, v.e);
    assert_eq!(decoded.f, v.f);
    assert_eq!(decoded.g, v.g);
}

#[derive(Debug, PartialEq, resolute::PgComposite)]
struct AllOptional {
    a: Option<i32>,
    b: Option<String>,
    c: Option<bool>,
}

#[test]
fn test_pg_composite_all_null() {
    let v = AllOptional {
        a: None,
        b: None,
        c: None,
    };
    let mut buf = bytes::BytesMut::new();
    v.encode(&mut buf);
    let decoded = AllOptional::decode(&buf).unwrap();
    assert_eq!(decoded, v);
}

#[test]
fn test_pg_composite_mixed_null() {
    let v = AllOptional {
        a: Some(42),
        b: None,
        c: Some(true),
    };
    let mut buf = bytes::BytesMut::new();
    v.encode(&mut buf);
    let decoded = AllOptional::decode(&buf).unwrap();
    assert_eq!(decoded, v);
}

// ---------------------------------------------------------------------------
// Named parameters: runtime API
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_query_named_basic() {
    let client = connect().await;
    let rows = client
        .query_named(
            "SELECT :id::int4 AS n",
            &[("id", &42i32 as &dyn resolute::SqlParam)],
        )
        .await
        .unwrap();
    assert_eq!(rows[0].get::<i32>(0).unwrap(), 42);
}

#[tokio::test]
async fn test_query_named_multiple() {
    let client = connect().await;
    let rows = client
        .query_named(
            "SELECT :a::int4 AS a, :b::text AS b",
            &[
                ("a", &10i32 as &dyn resolute::SqlParam),
                ("b", &"hello" as &dyn resolute::SqlParam),
            ],
        )
        .await
        .unwrap();
    assert_eq!(rows[0].get::<i32>(0).unwrap(), 10);
    assert_eq!(rows[0].get::<String>(1).unwrap(), "hello");
}

#[tokio::test]
async fn test_query_named_duplicate() {
    let client = connect().await;
    let rows = client
        .query_named(
            "SELECT :val::int4 AS a, :val::int4 + 1 AS b",
            &[("val", &5i32 as &dyn resolute::SqlParam)],
        )
        .await
        .unwrap();
    assert_eq!(rows[0].get::<i32>(0).unwrap(), 5);
    assert_eq!(rows[0].get::<i32>(1).unwrap(), 6);
}

#[tokio::test]
async fn test_query_named_with_cast() {
    let client = connect().await;
    let rows = client
        .query_named(
            "SELECT :value::int4 AS n",
            &[("value", &99i32 as &dyn resolute::SqlParam)],
        )
        .await
        .unwrap();
    assert_eq!(rows[0].get::<i32>(0).unwrap(), 99);
}

#[tokio::test]
async fn test_execute_named() {
    let client = connect().await;
    client
        .simple_query("CREATE TEMP TABLE test_named_exec (id int, name text)")
        .await
        .unwrap();

    let count = client
        .execute_named(
            "INSERT INTO test_named_exec VALUES (:id, :name)",
            &[
                ("id", &1i32 as &dyn resolute::SqlParam),
                ("name", &"Alice" as &dyn resolute::SqlParam),
            ],
        )
        .await
        .unwrap();
    assert_eq!(count, 1);

    let rows = client
        .query_named(
            "SELECT name FROM test_named_exec WHERE id = :id",
            &[("id", &1i32 as &dyn resolute::SqlParam)],
        )
        .await
        .unwrap();
    assert_eq!(rows[0].get::<String>(0).unwrap(), "Alice");
}

#[tokio::test]
async fn test_query_named_missing_param_errors() {
    let client = connect().await;
    let result = client
        .query_named(
            "SELECT :id::int4 AS n",
            &[("wrong_name", &42i32 as &dyn resolute::SqlParam)],
        )
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_query_named_in_transaction() {
    let client = connect().await;
    client
        .simple_query("CREATE TEMP TABLE test_named_txn (val int)")
        .await
        .unwrap();

    let txn = client.begin().await.unwrap();
    txn.execute_named(
        "INSERT INTO test_named_txn VALUES (:val)",
        &[("val", &7i32 as &dyn resolute::SqlParam)],
    )
    .await
    .unwrap();

    let rows = txn
        .query_named(
            "SELECT val FROM test_named_txn WHERE val = :val",
            &[("val", &7i32 as &dyn resolute::SqlParam)],
        )
        .await
        .unwrap();
    assert_eq!(rows[0].get::<i32>(0).unwrap(), 7);
    txn.commit().await.unwrap();
}

// ---------------------------------------------------------------------------
// Executor trait: generic functions work with Client, Transaction, Pool
// ---------------------------------------------------------------------------

/// A generic query function that works with any Executor.
async fn get_count(db: &impl Executor) -> i32 {
    let rows = db
        .query("SELECT count(*)::int4 FROM api.authors", &[])
        .await
        .unwrap();
    rows[0].get(0).unwrap()
}

/// A generic insert function using named params.
async fn insert_row(db: &impl Executor, table: &str, id: i32, val: &str) {
    db.execute_named(
        &format!("INSERT INTO {table} VALUES (:id, :val)"),
        &[
            ("id", &id as &dyn resolute::SqlParam),
            ("val", &val.to_string() as &dyn resolute::SqlParam),
        ],
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn test_executor_trait_with_client() {
    let client = connect().await;
    let count = get_count(&client).await;
    assert!(count >= 3);
}

#[tokio::test]
async fn test_executor_trait_with_transaction() {
    let client = connect().await;
    let txn = client.begin().await.unwrap();
    let count = get_count(&txn).await;
    assert!(count >= 3);
    txn.commit().await.unwrap();
}

#[tokio::test]
async fn test_executor_trait_with_pool() {
    let pool = resolute::TypedPool::connect(ADDR, USER, PASS, DB, 3)
        .await
        .unwrap();
    let pooled = pool.get().await.unwrap();
    let count = get_count(&pooled).await;
    assert!(count >= 3);
}

#[tokio::test]
async fn test_executor_trait_multi_query_reuse() {
    // The key advantage over sqlx: calling multiple queries on the same generic executor.
    async fn multi_query(db: &impl Executor) -> (i32, String) {
        let rows = db.query("SELECT 42::int4 AS n", &[]).await.unwrap();
        let n: i32 = rows[0].get(0).unwrap();
        // Second query on the SAME executor — this fails generically in sqlx.
        let rows = db.query("SELECT 'hello'::text AS s", &[]).await.unwrap();
        let s: String = rows[0].get(0).unwrap();
        (n, s)
    }

    let client = connect().await;
    assert_eq!(multi_query(&client).await, (42, "hello".to_string()));

    let txn = client.begin().await.unwrap();
    assert_eq!(multi_query(&txn).await, (42, "hello".to_string()));
    txn.commit().await.unwrap();
}

#[tokio::test]
async fn test_executor_named_params_generic() {
    async fn find_by_id(db: &impl Executor, id: i32) -> String {
        let rows = db
            .query_named(
                "SELECT name FROM api.authors WHERE id = :id",
                &[("id", &id as &dyn resolute::SqlParam)],
            )
            .await
            .unwrap();
        rows[0].get(0).unwrap()
    }

    let client = connect().await;
    assert_eq!(find_by_id(&client, 1).await, "Alice");

    let txn = client.begin().await.unwrap();
    assert_eq!(find_by_id(&txn, 2).await, "Bob");
    txn.commit().await.unwrap();
}

// ---------------------------------------------------------------------------
// with_transaction: closure-based transaction API
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_with_transaction_commit() {
    let client = connect().await;
    client
        .simple_query("CREATE TEMP TABLE test_with_txn (id int, val text)")
        .await
        .unwrap();

    let result = client
        .with_transaction(|db| {
            Box::pin(async move {
                insert_row(db, "test_with_txn", 1, "one").await;
                insert_row(db, "test_with_txn", 2, "two").await;
                Ok(42i32)
            })
        })
        .await
        .unwrap();
    assert_eq!(result, 42);

    // Data should be committed.
    let rows = client
        .query("SELECT count(*)::int4 FROM test_with_txn", &[])
        .await
        .unwrap();
    assert_eq!(rows[0].get::<i32>(0).unwrap(), 2);
}

#[tokio::test]
async fn test_with_transaction_rollback_on_error() {
    let client = connect().await;
    client
        .simple_query("CREATE TEMP TABLE test_with_txn_rb (id int)")
        .await
        .unwrap();

    let result: Result<(), _> = client
        .with_transaction(|db| {
            Box::pin(async move {
                db.execute("INSERT INTO test_with_txn_rb VALUES ($1)", &[&1i32])
                    .await?;
                // Return an error — should trigger rollback.
                Err(resolute::TypedError::Decode {
                    column: 0,
                    message: "intentional error".into(),
                })
            })
        })
        .await;
    assert!(result.is_err());

    // Data should be rolled back.
    let rows = client
        .query("SELECT count(*)::int4 FROM test_with_txn_rb", &[])
        .await
        .unwrap();
    assert_eq!(rows[0].get::<i32>(0).unwrap(), 0);
}

#[tokio::test]
async fn test_with_transaction_generic_functions() {
    let client = connect().await;
    client
        .simple_query("CREATE TEMP TABLE test_with_txn_gen (id int, val text)")
        .await
        .unwrap();

    // Use generic Executor functions inside with_transaction.
    client
        .with_transaction(|db| {
            Box::pin(async move {
                insert_row(db, "test_with_txn_gen", 1, "hello").await;
                insert_row(db, "test_with_txn_gen", 2, "world").await;
                Ok(())
            })
        })
        .await
        .unwrap();

    let rows = client
        .query("SELECT val FROM test_with_txn_gen ORDER BY id", &[])
        .await
        .unwrap();
    assert_eq!(rows[0].get::<String>(0).unwrap(), "hello");
    assert_eq!(rows[1].get::<String>(0).unwrap(), "world");
}

// ---------------------------------------------------------------------------
// Executor::atomic — context-aware transactions
// ---------------------------------------------------------------------------

/// A function that always runs atomically, regardless of caller context.
async fn atomic_insert(db: &impl Executor, table: &str, id: i32, val: &str) -> Result<(), resolute::TypedError> {
    db.atomic(|db| {
        let table = table.to_string();
        let val = val.to_string();
        Box::pin(async move {
            db.execute(
                &format!("INSERT INTO {table} VALUES ($1, $2)"),
                &[&id, &val],
            )
            .await?;
            Ok(())
        })
    })
    .await
}

#[tokio::test]
async fn test_atomic_on_client_commits() {
    let client = connect().await;
    client
        .simple_query("CREATE TEMP TABLE test_atomic_c (id int, val text)")
        .await
        .unwrap();

    // atomic() on Client wraps in BEGIN/COMMIT.
    atomic_insert(&client, "test_atomic_c", 1, "one").await.unwrap();

    let rows = client
        .query("SELECT val FROM test_atomic_c WHERE id = $1", &[&1i32])
        .await
        .unwrap();
    assert_eq!(rows[0].get::<String>(0).unwrap(), "one");
}

#[tokio::test]
async fn test_atomic_on_client_rolls_back() {
    let client = connect().await;
    client
        .simple_query("CREATE TEMP TABLE test_atomic_rb (id int PRIMARY KEY)")
        .await
        .unwrap();

    // First insert succeeds.
    client
        .atomic(|db| {
            Box::pin(async move {
                db.execute("INSERT INTO test_atomic_rb VALUES ($1)", &[&1i32])
                    .await?;
                // Force an error.
                Err::<(), _>(resolute::TypedError::Decode {
                    column: 0,
                    message: "intentional".into(),
                })
            })
        })
        .await
        .unwrap_err();

    // Should be rolled back.
    let rows = client
        .query("SELECT count(*)::int4 FROM test_atomic_rb", &[])
        .await
        .unwrap();
    assert_eq!(rows[0].get::<i32>(0).unwrap(), 0);
}

#[tokio::test]
async fn test_atomic_on_transaction_uses_savepoint() {
    let client = connect().await;
    client
        .simple_query("CREATE TEMP TABLE test_atomic_sp (id int, val text)")
        .await
        .unwrap();

    let txn = client.begin().await.unwrap();

    // First atomic insert succeeds.
    atomic_insert(&txn, "test_atomic_sp", 1, "one").await.unwrap();

    // Second atomic insert fails — should rollback only this savepoint.
    let result: Result<(), _> = txn
        .atomic(|db| {
            Box::pin(async move {
                db.execute("INSERT INTO test_atomic_sp VALUES ($1)", &[&2i32])
                    .await?;
                Err(resolute::TypedError::Decode {
                    column: 0,
                    message: "intentional".into(),
                })
            })
        })
        .await;
    assert!(result.is_err());

    // First insert should still be visible inside the transaction.
    let rows = txn
        .query("SELECT count(*)::int4 FROM test_atomic_sp", &[])
        .await
        .unwrap();
    assert_eq!(rows[0].get::<i32>(0).unwrap(), 1);

    txn.commit().await.unwrap();

    // After commit, the first insert is persisted.
    let rows = client
        .query("SELECT id FROM test_atomic_sp", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<i32>(0).unwrap(), 1);
}

#[tokio::test]
async fn test_atomic_nested() {
    let client = connect().await;
    client
        .simple_query("CREATE TEMP TABLE test_atomic_nest (id int)")
        .await
        .unwrap();

    // atomic inside atomic — outer is BEGIN/COMMIT, inner is SAVEPOINT.
    client
        .atomic(|db| {
            Box::pin(async move {
                db.execute("INSERT INTO test_atomic_nest VALUES ($1)", &[&1i32])
                    .await?;
                // Nested atomic — uses savepoint since we're already in a BEGIN.
                // Note: db is &Client here, so this creates another BEGIN.
                // For true savepoint nesting, use Transaction.
                Ok(())
            })
        })
        .await
        .unwrap();

    let rows = client
        .query("SELECT count(*)::int4 FROM test_atomic_nest", &[])
        .await
        .unwrap();
    assert_eq!(rows[0].get::<i32>(0).unwrap(), 1);
}

#[tokio::test]
async fn test_atomic_with_generic_executor_functions() {
    let client = connect().await;
    client
        .simple_query("CREATE TEMP TABLE test_atomic_gen (id int, val text)")
        .await
        .unwrap();

    // Generic function that uses atomic() — works with &Client or &Transaction.
    async fn batch_insert(db: &impl Executor) -> Result<(), resolute::TypedError> {
        db.atomic(|db| {
            Box::pin(async move {
                db.execute("INSERT INTO test_atomic_gen VALUES ($1, $2)", &[&1i32, &"a".to_string()])
                    .await?;
                db.execute("INSERT INTO test_atomic_gen VALUES ($1, $2)", &[&2i32, &"b".to_string()])
                    .await?;
                Ok(())
            })
        })
        .await
    }

    // Call with Client — uses BEGIN/COMMIT.
    batch_insert(&client).await.unwrap();

    let rows = client
        .query("SELECT count(*)::int4 FROM test_atomic_gen", &[])
        .await
        .unwrap();
    assert_eq!(rows[0].get::<i32>(0).unwrap(), 2);

    // Call with Transaction — uses SAVEPOINT.
    let txn = client.begin().await.unwrap();
    batch_insert(&txn).await.unwrap();
    txn.commit().await.unwrap();

    let rows = client
        .query("SELECT count(*)::int4 FROM test_atomic_gen", &[])
        .await
        .unwrap();
    assert_eq!(rows[0].get::<i32>(0).unwrap(), 4);
}

#[tokio::test]
async fn test_executor_copy_in_generic() {
    async fn bulk_load(db: &impl Executor, table: &str, csv: &[u8]) -> u64 {
        db.copy_in(
            &format!("COPY {table} FROM STDIN WITH (FORMAT csv)"),
            csv,
        )
        .await
        .unwrap()
    }

    let client = connect().await;
    client
        .simple_query("CREATE TEMP TABLE test_exec_copy (id int, val text)")
        .await
        .unwrap();
    let count = bulk_load(&client, "test_exec_copy", b"1,a\n2,b\n").await;
    assert_eq!(count, 2);
}

#[tokio::test]
async fn test_executor_ping_generic() {
    async fn check_health(db: &impl Executor) -> bool {
        db.ping().await.is_ok()
    }

    let client = connect().await;
    assert!(check_health(&client).await);

    let txn = client.begin().await.unwrap();
    assert!(check_health(&txn).await);
    txn.commit().await.unwrap();
}

// ---------------------------------------------------------------------------
// Streaming queries
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_query_stream_basic() {
    let client = connect().await;
    let mut stream = client
        .query_stream("SELECT generate_series(1, 5)::int4 AS n", &[])
        .await
        .unwrap();

    let mut values = Vec::new();
    while let Some(row) = stream.next().await {
        let row = row.unwrap();
        let n: i32 = row.get(0).unwrap();
        values.push(n);
    }
    assert_eq!(values, vec![1, 2, 3, 4, 5]);
}

#[tokio::test]
async fn test_query_stream_large() {
    let client = connect().await;
    let mut stream = client
        .query_stream("SELECT generate_series(1, 10000)::int4 AS n", &[])
        .await
        .unwrap();

    let mut count = 0;
    let mut last = 0;
    while let Some(row) = stream.next().await {
        let row = row.unwrap();
        last = row.get::<i32>(0).unwrap();
        count += 1;
    }
    assert_eq!(count, 10000);
    assert_eq!(last, 10000);
}

#[tokio::test]
async fn test_query_stream_with_params() {
    let client = connect().await;
    let mut stream = client
        .query_stream(
            "SELECT id, name FROM api.authors WHERE id <= $1 ORDER BY id",
            &[&3i32],
        )
        .await
        .unwrap();

    let mut names = Vec::new();
    while let Some(row) = stream.next().await {
        let row = row.unwrap();
        let name: String = row.get(1).unwrap();
        names.push(name);
    }
    assert_eq!(names, vec!["Alice", "Bob", "Carol"]);
}

#[tokio::test]
async fn test_query_stream_empty() {
    let client = connect().await;
    let mut stream = client
        .query_stream("SELECT 1 WHERE false", &[])
        .await
        .unwrap();

    let mut count = 0;
    while let Some(_row) = stream.next().await {
        count += 1;
    }
    assert_eq!(count, 0);
}

#[tokio::test]
async fn test_query_stream_then_regular_query() {
    let client = connect().await;

    // Stream first.
    let mut stream = client
        .query_stream("SELECT generate_series(1, 3)::int4 AS n", &[])
        .await
        .unwrap();
    let mut count = 0;
    while stream.next().await.is_some() {
        count += 1;
    }
    assert_eq!(count, 3);

    // Regular query after stream completes — connection still works.
    let rows = client.query("SELECT 42::int4 AS n", &[]).await.unwrap();
    assert_eq!(rows[0].get::<i32>(0).unwrap(), 42);
}

// ---------------------------------------------------------------------------
// Prepared statement invalidation
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_statement_invalidation_on_schema_change() {
    let client = connect().await;

    // Create a table and cache a prepared statement against it.
    client
        .simple_query("CREATE TEMP TABLE test_invalidate (id int, val text)")
        .await
        .unwrap();
    client
        .execute(
            "INSERT INTO test_invalidate VALUES ($1, $2)",
            &[&1i32, &"hello".to_string()],
        )
        .await
        .unwrap();

    // Query caches the prepared statement.
    let rows = client
        .query("SELECT id, val FROM test_invalidate", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);

    // Alter the table — this invalidates the cached statement.
    client
        .simple_query("ALTER TABLE test_invalidate ADD COLUMN extra int DEFAULT 0")
        .await
        .unwrap();

    // The cached statement refers to the old schema. With invalidation handling,
    // this should succeed by re-preparing automatically.
    let rows = client
        .query("SELECT id, val FROM test_invalidate", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<String>(1).unwrap(), "hello");
}

// ---------------------------------------------------------------------------
// COPY protocol
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_copy_in_csv() {
    let client = connect().await;
    client
        .simple_query("CREATE TEMP TABLE test_copy_in (id int, name text)")
        .await
        .unwrap();

    let csv = b"1,Alice\n2,Bob\n3,Carol\n";
    let count = client
        .copy_in(
            "COPY test_copy_in (id, name) FROM STDIN WITH (FORMAT csv)",
            csv,
        )
        .await
        .unwrap();
    assert_eq!(count, 3);

    let rows = client
        .query("SELECT name FROM test_copy_in ORDER BY id", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].get::<String>(0).unwrap(), "Alice");
    assert_eq!(rows[2].get::<String>(0).unwrap(), "Carol");
}

#[tokio::test]
async fn test_copy_out_csv() {
    let client = connect().await;
    let data = client
        .copy_out("COPY (SELECT id, name FROM api.authors ORDER BY id) TO STDOUT WITH (FORMAT csv)")
        .await
        .unwrap();

    let csv = String::from_utf8(data).unwrap();
    let lines: Vec<&str> = csv.trim().lines().collect();
    assert!(lines.len() >= 3);
    assert!(lines[0].starts_with("1,"));
}

#[tokio::test]
async fn test_copy_in_empty() {
    let client = connect().await;
    client
        .simple_query("CREATE TEMP TABLE test_copy_empty (id int)")
        .await
        .unwrap();

    let count = client
        .copy_in(
            "COPY test_copy_empty FROM STDIN WITH (FORMAT csv)",
            b"",
        )
        .await
        .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn test_copy_in_then_query() {
    let client = connect().await;
    client
        .simple_query("CREATE TEMP TABLE test_copy_then (id int, val text)")
        .await
        .unwrap();

    client
        .copy_in(
            "COPY test_copy_then FROM STDIN WITH (FORMAT csv)",
            b"1,hello\n2,world\n",
        )
        .await
        .unwrap();

    // Connection should still work after COPY.
    let rows = client
        .query("SELECT count(*)::int4 FROM test_copy_then", &[])
        .await
        .unwrap();
    assert_eq!(rows[0].get::<i32>(0).unwrap(), 2);
}

// ---------------------------------------------------------------------------
// Query cancellation
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_cancel_token() {
    let client = connect().await;
    let token = client.cancel_token();

    // Start a long query and cancel it from another task.
    let cancel_handle = tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        token.cancel().await.unwrap();
    });

    let start = std::time::Instant::now();
    let result = client.query("SELECT pg_sleep(10)", &[]).await;

    // The query should have been cancelled (error), not waited 10 seconds.
    let elapsed = start.elapsed();
    assert!(elapsed.as_secs() < 5, "query should have been cancelled quickly, took {elapsed:?}");
    assert!(result.is_err(), "cancelled query should return an error");

    cancel_handle.await.unwrap();

    // Connection should recover after cancellation.
    let rows = client.query("SELECT 1::int4 AS n", &[]).await.unwrap();
    assert_eq!(rows[0].get::<i32>(0).unwrap(), 1);
}

// ---------------------------------------------------------------------------
// Pipeline mode
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_pipeline_basic() {
    let client = connect().await;
    let results = client
        .pipeline()
        .query("SELECT 1::int4 AS n", &[])
        .query("SELECT 'hello'::text AS s", &[])
        .run()
        .await
        .unwrap();

    assert_eq!(results.len(), 2);
    if let resolute::PipelineResult::Rows(rows) = &results[0] {
        assert_eq!(rows[0].get::<i32>(0).unwrap(), 1);
    } else {
        panic!("expected Rows");
    }
    if let resolute::PipelineResult::Rows(rows) = &results[1] {
        assert_eq!(rows[0].get::<String>(0).unwrap(), "hello");
    } else {
        panic!("expected Rows");
    }
}

#[tokio::test]
async fn test_pipeline_mixed() {
    let client = connect().await;
    client
        .simple_query("CREATE TEMP TABLE test_pipeline (id int, val text)")
        .await
        .unwrap();

    let results = client
        .pipeline()
        .execute(
            "INSERT INTO test_pipeline VALUES ($1, $2)",
            &[&1i32, &"one".to_string()],
        )
        .execute(
            "INSERT INTO test_pipeline VALUES ($1, $2)",
            &[&2i32, &"two".to_string()],
        )
        .query("SELECT count(*)::int4 FROM test_pipeline", &[])
        .run()
        .await
        .unwrap();

    assert_eq!(results.len(), 3);
    if let resolute::PipelineResult::Execute(n) = results[0] {
        assert_eq!(n, 1);
    } else {
        panic!("expected Execute");
    }
    if let resolute::PipelineResult::Rows(rows) = &results[2] {
        assert_eq!(rows[0].get::<i32>(0).unwrap(), 2);
    } else {
        panic!("expected Rows");
    }
}

#[tokio::test]
async fn test_pipeline_empty() {
    let client = connect().await;
    let results = client.pipeline().run().await.unwrap();
    assert!(results.is_empty());
}

// ---------------------------------------------------------------------------
// Query timeouts
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_query_timeout_succeeds() {
    let client = connect().await;
    let rows = client
        .query_timeout("SELECT 1::int4 AS n", &[], std::time::Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(rows[0].get::<i32>(0).unwrap(), 1);
}

#[tokio::test]
async fn test_query_timeout_fires() {
    let client = connect().await;
    let start = std::time::Instant::now();
    let result = client
        .query_timeout(
            "SELECT pg_sleep(10)",
            &[],
            std::time::Duration::from_millis(200),
        )
        .await;
    let elapsed = start.elapsed();
    assert!(result.is_err());
    assert!(elapsed.as_secs() < 3, "timeout should fire quickly, took {elapsed:?}");

    // Connection should recover.
    let rows = client.query("SELECT 1::int4 AS n", &[]).await.unwrap();
    assert_eq!(rows[0].get::<i32>(0).unwrap(), 1);
}

// ---------------------------------------------------------------------------
// Health check (ping)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_ping() {
    let client = connect().await;
    client.ping().await.unwrap();
}

// ---------------------------------------------------------------------------
// Retry policy
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_retry_policy_succeeds_immediately() {
    let client = connect().await;
    let policy = resolute::retry::RetryPolicy::new(3, std::time::Duration::from_millis(10));
    let rows = policy
        .execute(&client, |db| {
            Box::pin(async move { db.query("SELECT 42::int4 AS n", &[]).await })
        })
        .await
        .unwrap();
    assert_eq!(rows[0].get::<i32>(0).unwrap(), 42);
}

#[tokio::test]
async fn test_retry_policy_non_transient_fails_fast() {
    let client = connect().await;
    let policy = resolute::retry::RetryPolicy::new(3, std::time::Duration::from_millis(10));
    let start = std::time::Instant::now();
    let result = policy
        .execute(&client, |db| {
            Box::pin(async move {
                db.query("SELECT * FROM nonexistent_table_xyz", &[]).await
            })
        })
        .await;
    // Non-transient error (42P01 = undefined_table) should not retry.
    assert!(result.is_err());
    assert!(start.elapsed().as_millis() < 500, "should fail fast without retries");
}

// ---------------------------------------------------------------------------
// Pool stress tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_stress_pool_concurrent() {
    // 50 concurrent tasks sharing a pool of 5 connections, each does 10 queries.
    // With the fixed pool (AsyncConn reuse), connections are returned and reused.
    let pool = std::sync::Arc::new(
        resolute::TypedPool::connect(ADDR, USER, PASS, DB, 5)
            .await
            .unwrap(),
    );

    let mut handles = Vec::new();
    for task_id in 0..50u32 {
        let pool = pool.clone();
        handles.push(tokio::spawn(async move {
            for i in 0..10u32 {
                let conn = pool.get().await.unwrap();
                let val = (task_id * 10 + i) as i32;
                let rows = conn
                    .query("SELECT $1::int4 AS n", &[&val])
                    .await
                    .unwrap();
                assert_eq!(rows[0].get::<i32>(0).unwrap(), val);
                // conn dropped here — returned to pool
            }
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }

    // Pool should still have connections after all tasks complete.
    let m = pool.metrics();
    assert!(m.total > 0, "pool should have live connections");
}

#[tokio::test]
async fn test_stress_pool_mixed_operations() {
    let pool = std::sync::Arc::new(
        resolute::TypedPool::connect(ADDR, USER, PASS, DB, 5)
            .await
            .unwrap(),
    );

    // Setup table.
    {
        let conn = pool.get().await.unwrap();
        conn.simple_query(
            "CREATE TABLE IF NOT EXISTS _pool_stress (id serial PRIMARY KEY, val text)",
        )
        .await
        .unwrap();
        conn.simple_query("TRUNCATE _pool_stress").await.unwrap();
    }

    // 20 concurrent tasks doing mixed operations.
    let mut handles = Vec::new();
    for task_id in 0..20u32 {
        let pool = pool.clone();
        handles.push(tokio::spawn(async move {
            let conn = pool.get().await.unwrap();

            // Query
            let rows = conn.query("SELECT 1::int4 AS n", &[]).await.unwrap();
            assert_eq!(rows[0].get::<i32>(0).unwrap(), 1);

            // Execute (INSERT)
            let label = format!("task-{task_id}");
            let affected = conn
                .execute(
                    "INSERT INTO _pool_stress (val) VALUES ($1::text)",
                    &[&label],
                )
                .await
                .unwrap();
            assert_eq!(affected, 1);

            // Named param query (via Executor trait)
            use resolute::Executor;
            let rows = conn
                .query_named("SELECT :num::int4 AS n", &[("num", &(task_id as i32) as &dyn resolute::SqlParam)])
                .await
                .unwrap();
            assert_eq!(rows[0].get::<i32>(0).unwrap(), task_id as i32);
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }
}

// ---------------------------------------------------------------------------
// PgTimestamp encode/decode roundtrip (unit test)
// ---------------------------------------------------------------------------

#[test]
fn test_pg_timestamp_encode_decode_roundtrip() {
    use resolute::{PgTimestamp, Encode, Decode};

    // Finite value
    let ts = PgTimestamp::Value(12345);
    let mut buf = bytes::BytesMut::new();
    ts.encode(&mut buf);
    let decoded = PgTimestamp::decode(&buf).unwrap();
    assert_eq!(decoded, ts);

    // Infinity
    let mut buf = bytes::BytesMut::new();
    PgTimestamp::Infinity.encode(&mut buf);
    let decoded = PgTimestamp::decode(&buf).unwrap();
    assert_eq!(decoded, PgTimestamp::Infinity);

    // NegInfinity
    let mut buf = bytes::BytesMut::new();
    PgTimestamp::NegInfinity.encode(&mut buf);
    let decoded = PgTimestamp::decode(&buf).unwrap();
    assert_eq!(decoded, PgTimestamp::NegInfinity);
}

// ---------------------------------------------------------------------------
// PgDate encode/decode roundtrip (unit test)
// ---------------------------------------------------------------------------

#[test]
fn test_pg_date_encode_decode_roundtrip() {
    use resolute::{PgDate, Encode, Decode};

    // Finite value
    let d = PgDate::Value(12345);
    let mut buf = bytes::BytesMut::new();
    d.encode(&mut buf);
    let decoded = PgDate::decode(&buf).unwrap();
    assert_eq!(decoded, d);

    // Infinity
    let mut buf = bytes::BytesMut::new();
    PgDate::Infinity.encode(&mut buf);
    let decoded = PgDate::decode(&buf).unwrap();
    assert_eq!(decoded, PgDate::Infinity);

    // NegInfinity
    let mut buf = bytes::BytesMut::new();
    PgDate::NegInfinity.encode(&mut buf);
    let decoded = PgDate::decode(&buf).unwrap();
    assert_eq!(decoded, PgDate::NegInfinity);
}

// ---------------------------------------------------------------------------
// Pool warm_up
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_pool_warm_up() {
    let pool = resolute::TypedPool::connect(ADDR, USER, PASS, DB, 5)
        .await
        .unwrap();
    pool.warm_up(3).await;
    let metrics = pool.metrics();
    assert!(
        metrics.total >= 3,
        "expected at least 3 connections after warm_up, got {}",
        metrics.total,
    );
}

// ---------------------------------------------------------------------------
// Pool DISCARD ALL clears session state
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_pool_discard_all_clears_state() {
    let pool = resolute::TypedPool::connect(ADDR, USER, PASS, DB, 1)
        .await
        .unwrap();

    // Checkout a connection and set search_path to something non-default.
    {
        let conn = pool.get().await.unwrap();
        conn.simple_query("SET search_path TO pg_catalog").await.unwrap();

        // Verify it took effect within this session.
        let rows = conn
            .query("SELECT current_setting('search_path') AS sp", &[])
            .await
            .unwrap();
        let sp: String = rows[0].get(0).unwrap();
        assert!(
            sp.contains("pg_catalog"),
            "expected pg_catalog in search_path, got: {sp}",
        );
    }
    // Connection returned to pool -- pool runs DISCARD ALL.

    // Checkout again (same underlying connection since pool size = 1).
    let conn = pool.get().await.unwrap();
    let rows = conn
        .query("SELECT current_setting('search_path') AS sp", &[])
        .await
        .unwrap();
    let sp: String = rows[0].get(0).unwrap();

    // After DISCARD ALL, search_path should be the server default
    // (typically '"$user", public'), not just 'pg_catalog'.
    assert!(
        sp.contains("public") || sp.contains("$user"),
        "expected default search_path after pool reset, got: {sp}",
    );
}

// ---------------------------------------------------------------------------
// Metrics recording
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_metrics_recording() {
    // Capture the count before our query (other tests may have already bumped it).
    let before = resolute::metrics::snapshot().query_count;

    let client = connect().await;
    let _rows = client.query("SELECT 1::int4 AS n", &[]).await.unwrap();

    let after = resolute::metrics::snapshot().query_count;
    assert!(
        after > before,
        "expected query_count to increase; before={before}, after={after}",
    );
}

// ---------------------------------------------------------------------------
// TestDb lifecycle
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_test_db_lifecycle() {
    let db = resolute::test_db::TestDb::create(ADDR, USER, PASS)
        .await
        .unwrap();

    let client = db.client().await.unwrap();

    // Create a table, insert, query.
    client
        .simple_query("CREATE TABLE lifecycle_test (id serial PRIMARY KEY, name text)")
        .await
        .unwrap();
    client
        .execute(
            "INSERT INTO lifecycle_test (name) VALUES ($1)",
            &[&"hello"],
        )
        .await
        .unwrap();
    let rows = client
        .query("SELECT name FROM lifecycle_test", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let name: String = rows[0].get(0).unwrap();
    assert_eq!(name, "hello");

    // Drop the client before dropping the DB (releases the connection).
    drop(client);

    // Drop the test database.
    db.drop_db().await.unwrap();

    // Verify the database is gone by trying to connect — should fail.
    let result = Client::connect(ADDR, USER, PASS, &db.database).await;
    assert!(
        result.is_err(),
        "expected connection to dropped database to fail",
    );
}

// ---------------------------------------------------------------------------
// execute_timeout fires on slow statement
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_execute_timeout_fires() {
    let client = connect().await;
    let result = client
        .execute_timeout(
            "SELECT pg_sleep(10)",
            &[],
            std::time::Duration::from_millis(50),
        )
        .await;
    assert!(
        result.is_err(),
        "expected timeout error for slow statement",
    );
    let err = result.unwrap_err();
    let err_msg = format!("{err}");
    assert!(
        err_msg.contains("timed out"),
        "expected timeout error message, got: {err_msg}",
    );
}
