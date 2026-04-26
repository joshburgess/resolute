//! Drive a single PostgreSQL connection at the wire-protocol layer.
//!
//! This example connects to PostgreSQL using only `pg-wired`. It performs a
//! simple unparameterized query, a parameterized query (binary param OID), and
//! a small pipelined transaction. There is no SQL parsing, no type system, and
//! no pool. For the typed API most application code wants, see the `resolute`
//! crate and its examples.
//!
//! Run against the workspace's docker-compose Postgres:
//!
//! ```bash
//! docker compose up -d
//! cargo run -p pg-wired --example raw_query
//! ```
//!
//! Override the connection target with the standard test env vars:
//!
//! ```bash
//! RESOLUTE_TEST_ADDR=127.0.0.1:5432 \
//! RESOLUTE_TEST_USER=alice \
//! RESOLUTE_TEST_PASSWORD=secret \
//! RESOLUTE_TEST_DB=mydb \
//! cargo run -p pg-wired --example raw_query
//! ```

use pg_wired::{AsyncConn, WireConn};

fn env_or<'a>(var: &str, default: &'a str) -> std::borrow::Cow<'a, str> {
    match std::env::var(var) {
        Ok(v) => std::borrow::Cow::Owned(v),
        Err(_) => std::borrow::Cow::Borrowed(default),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = env_or("RESOLUTE_TEST_ADDR", "127.0.0.1:54322");
    let user = env_or("RESOLUTE_TEST_USER", "postgres");
    let pass = env_or("RESOLUTE_TEST_PASSWORD", "postgres");
    let db = env_or("RESOLUTE_TEST_DB", "postgrest_test");

    // 1. Connect. WireConn drives the startup + auth handshake. AsyncConn wraps
    // it with a reader/writer task pair so you can issue queries from any task.
    let wire = WireConn::connect(&addr, &user, &pass, &db).await?;
    let conn = AsyncConn::new(wire);
    println!("connected, backend pid = {}", conn.backend_pid());

    // 2. Simple unparameterized query. exec_query returns RawRow values whose
    // cells are raw bytes in the format requested (text by default). Decoding
    // is the caller's responsibility at this layer.
    let rows = conn
        .exec_query("SELECT 1 AS n, 'hello' AS s", &[], &[])
        .await?;
    let n = std::str::from_utf8(rows[0].cell(0).unwrap())?;
    let s = std::str::from_utf8(rows[0].cell(1).unwrap())?;
    println!("simple query: n = {n}, s = {s}");

    // 3. Parameterized query. Params are passed as Option<&[u8]>; None
    // represents SQL NULL. The OID array tells the server how to interpret
    // each param (here: 25 = text).
    let rows = conn
        .exec_query(
            "SELECT $1::text || ' world'",
            &[Some(b"hello" as &[u8])],
            &[25],
        )
        .await?;
    let v = std::str::from_utf8(rows[0].cell(0).unwrap())?;
    println!("parameterized: {v}");

    // 4. Pipelined transaction. exec_transaction fuses BEGIN, the body, and
    // COMMIT into a single round trip. Useful when you don't need to interleave
    // logic between statements.
    let rows = conn
        .exec_transaction(
            "BEGIN",
            "SELECT count(*)::text FROM pg_catalog.pg_namespace",
            &[],
            &[],
        )
        .await?;
    let count = std::str::from_utf8(rows[0].cell(0).unwrap())?;
    println!("namespaces: {count}");

    Ok(())
}
