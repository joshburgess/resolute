//! Transaction examples: manual, closure-based, and atomic().
//!
//! Run: cargo run -p resolute --example transactions
//! Requires: docker compose up -d

use resolute::{Client, Executor};

const ADDR: &str = "127.0.0.1:54322";

/// A generic function that works with any Executor (Client, Transaction, Pool).
async fn insert_item(
    db: &impl Executor,
    table: &str,
    id: i32,
    name: &str,
) -> Result<(), resolute::TypedError> {
    db.execute(
        &format!("INSERT INTO {table} VALUES ($1, $2)"),
        &[&id, &name.to_string()],
    )
    .await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::connect(ADDR, "postgres", "postgres", "postgrest_test").await?;

    // -- Manual transaction --
    client
        .simple_query("CREATE TEMP TABLE txn_demo (id int, name text)")
        .await?;

    let txn = client.begin().await?;
    insert_item(&txn, "txn_demo", 1, "Alice").await?;
    insert_item(&txn, "txn_demo", 2, "Bob").await?;
    txn.commit().await?;
    println!("Manual transaction committed");

    // -- Closure-based transaction --
    client
        .with_transaction(|db| {
            Box::pin(async move {
                insert_item(db, "txn_demo", 3, "Carol").await?;
                Ok(())
            })
        })
        .await?;
    println!("Closure transaction committed");

    // -- atomic() — works in any context --
    // Called on Client → uses BEGIN/COMMIT:
    client
        .atomic(|db| {
            Box::pin(async move {
                insert_item(db, "txn_demo", 4, "Dave").await?;
                Ok(())
            })
        })
        .await?;
    println!("atomic() on Client committed");

    // Called on Transaction → uses SAVEPOINT:
    let txn = client.begin().await?;
    txn.atomic(|db| {
        Box::pin(async move {
            insert_item(db, "txn_demo", 5, "Eve").await?;
            Ok(())
        })
    })
    .await?;
    txn.commit().await?;
    println!("atomic() on Transaction used savepoint");

    let rows = client
        .query("SELECT count(*)::int4 FROM txn_demo", &[])
        .await?;
    let count: i32 = rows[0].get(0)?;
    println!("Total rows: {count}");

    Ok(())
}
