//! Named parameters example.
//!
//! Run: cargo run -p stalwart --example named_params
//! Requires: docker compose up -d

use stalwart::{Client, SqlParam};

const ADDR: &str = "127.0.0.1:54322";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::connect(ADDR, "postgres", "postgres", "postgrest_test").await?;

    // Named params in runtime API:
    let rows = client
        .query_named(
            "SELECT id, name FROM api.authors WHERE id = :id",
            &[("id", &1i32 as &dyn SqlParam)],
        )
        .await?;
    let name: String = rows[0].get(1)?;
    println!("Author 1: {name}");

    // Duplicate params — :id used twice, bound once:
    let rows = client
        .query_named(
            "SELECT :val::int4 AS a, :val::int4 + 10 AS b",
            &[("val", &5i32 as &dyn SqlParam)],
        )
        .await?;
    let a: i32 = rows[0].get(0)?;
    let b: i32 = rows[0].get(1)?;
    println!("Duplicate param: a={a}, b={b}");

    // Named params with casts — :value::int4 handled correctly:
    let rows = client
        .query_named(
            "SELECT :value::int4 * 2 AS doubled",
            &[("value", &21i32 as &dyn SqlParam)],
        )
        .await?;
    let doubled: i32 = rows[0].get(0)?;
    println!("Doubled: {doubled}");

    Ok(())
}
