//! Named parameters example.
//!
//! Run: cargo run -p resolute --example named_params
//! Requires: docker compose up -d

use resolute::{params_named, sql, Client};

const ADDR: &str = "127.0.0.1:54322";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::connect(ADDR, "postgres", "postgres", "postgrest_test").await?;

    // Style 1: the params_named! macro.
    let rows = client
        .query_named(
            "SELECT id, name FROM api.authors WHERE id = :id",
            params_named![("id", 1i32)],
        )
        .await?;
    let name: String = rows[0].get(1)?;
    println!("Author 1: {name}");

    // Style 2: raw slice. Duplicate params: :val appears twice, bound once.
    let rows = client
        .query_named(
            "SELECT :val::int4 AS a, :val::int4 + 10 AS b",
            &[("val", &5i32)],
        )
        .await?;
    let a: i32 = rows[0].get(0)?;
    let b: i32 = rows[0].get(1)?;
    println!("Duplicate param: a={a}, b={b}");

    // Style 3: the fluent builder. :value::int4 cast in SQL is handled correctly.
    let doubled: i32 = sql("SELECT :value::int4 * 2 AS doubled")
        .bind_named("value", 21i32)
        .fetch_one(&client)
        .await?
        .get(0)?;
    println!("Doubled: {doubled}");

    Ok(())
}
