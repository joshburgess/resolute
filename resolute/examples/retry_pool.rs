//! Retry policy and connection pool example.
//!
//! Run: cargo run -p resolute --example retry_pool
//! Requires: docker compose up -d

use resolute::retry::RetryPolicy;
use resolute::TypedPool;
use std::time::Duration;

const ADDR: &str = "127.0.0.1:54322";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a connection pool.
    let pool = TypedPool::connect(ADDR, "postgres", "postgres", "postgrest_test", 5).await?;
    println!("Pool created with max_size=5");

    // Check out a client and query.
    let client = pool.get().await?;
    let rows = client.query("SELECT 42::int4 AS n", &[]).await?;
    let n: i32 = rows[0].get(0)?;
    println!("Pool query result: {n}");

    // Retry policy with exponential backoff.
    let policy =
        RetryPolicy::new(3, Duration::from_millis(100)).with_max_backoff(Duration::from_secs(5));

    let rows = policy
        .execute(&client, |db| {
            Box::pin(async move { db.query("SELECT 'retry works'::text AS msg", &[]).await })
        })
        .await?;
    let msg: String = rows[0].get(0)?;
    println!("Retry result: {msg}");

    // Pool metrics.
    let metrics = pool.metrics();
    println!("Pool metrics: {:?}", metrics);

    // Application metrics.
    let app_metrics = resolute::metrics::snapshot();
    println!(
        "Query count: {}, errors: {}",
        app_metrics.query_count, app_metrics.query_error_count
    );

    Ok(())
}
