//! COPY protocol for bulk import/export.
//!
//! Run: cargo run -p stalwart --example copy_bulk
//! Requires: docker compose up -d

use stalwart::Client;

const ADDR: &str = "127.0.0.1:54322";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::connect(ADDR, "postgres", "postgres", "postgrest_test").await?;

    // Setup
    client
        .simple_query("CREATE TEMP TABLE bulk_demo (id int, name text, score float8)")
        .await?;

    // COPY IN: bulk import from CSV
    let csv_data = b"1,Alice,95.5\n2,Bob,87.3\n3,Carol,92.1\n4,Dave,88.7\n5,Eve,91.0\n";
    let count = client
        .copy_in(
            "COPY bulk_demo (id, name, score) FROM STDIN WITH (FORMAT csv)",
            csv_data,
        )
        .await?;
    println!("Imported {count} rows via COPY IN");

    // Verify
    let rows = client
        .query("SELECT count(*)::int4, avg(score)::float8 FROM bulk_demo", &[])
        .await?;
    let total: i32 = rows[0].get(0)?;
    let avg: f64 = rows[0].get(1)?;
    println!("Total: {total}, Average score: {avg:.1}");

    // COPY OUT: bulk export to CSV
    let exported = client
        .copy_out("COPY bulk_demo TO STDOUT WITH (FORMAT csv, HEADER)")
        .await?;
    println!("\nExported CSV:\n{}", String::from_utf8_lossy(&exported));

    Ok(())
}
