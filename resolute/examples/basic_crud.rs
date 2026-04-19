//! Basic CRUD operations with resolute.
//!
//! Run: cargo run -p resolute --example basic_crud
//! Requires: docker compose up -d

use resolute::Client;

const ADDR: &str = "127.0.0.1:54322";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::connect(ADDR, "postgres", "postgres", "postgrest_test").await?;

    // CREATE
    client
        .simple_query(
            "CREATE TABLE IF NOT EXISTS example_users (id serial PRIMARY KEY, name text NOT NULL)",
        )
        .await?;

    // INSERT
    let count = client
        .execute(
            "INSERT INTO example_users (name) VALUES ($1)",
            &[&"Alice".to_string()],
        )
        .await?;
    println!("Inserted {count} row(s)");

    // READ
    let rows = client
        .query("SELECT id, name FROM example_users ORDER BY id", &[])
        .await?;
    for row in &rows {
        let id: i32 = row.get(0)?;
        let name: String = row.get(1)?;
        println!("  {id}: {name}");
    }

    // UPDATE
    let updated = client
        .execute(
            "UPDATE example_users SET name = $1 WHERE name = $2",
            &[&"Bob".to_string(), &"Alice".to_string()],
        )
        .await?;
    println!("Updated {updated} row(s)");

    // DELETE
    client.execute("DELETE FROM example_users", &[]).await?;
    client.simple_query("DROP TABLE example_users").await?;
    println!("Cleanup done");

    Ok(())
}
