//! Streaming large result sets row-by-row.
//!
//! Run: cargo run -p resolute --example streaming
//! Requires: docker compose up -d

use resolute::Client;
use tokio_stream::StreamExt;

const ADDR: &str = "127.0.0.1:54322";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::connect(ADDR, "postgres", "postgres", "postgrest_test").await?;

    println!("Streaming 10,000 rows...");
    let mut stream = client
        .query_stream("SELECT generate_series(1, 10000)::int4 AS n", &[])
        .await?;

    let mut count = 0u64;
    let mut sum = 0i64;
    while let Some(row) = stream.next().await {
        let row = row?;
        let n: i32 = row.get(0)?;
        sum += n as i64;
        count += 1;
    }

    println!("Processed {count} rows, sum = {sum}");
    // Expected: count=10000, sum=50005000
    assert_eq!(count, 10000);
    assert_eq!(sum, 50005000);
    println!("OK");

    Ok(())
}
