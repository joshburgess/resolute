//! Database create/drop commands.
//!
//! Connects to the `postgres` maintenance database to issue
//! CREATE DATABASE / DROP DATABASE statements.

use pg_wire::{PgPipeline, WireConn};

/// Create a database if it doesn't already exist.
pub async fn create(database_url: &str) -> Result<(), Box<dyn std::error::Error>> {
    let (user, password, host, port, database) =
        super::parse_pg_uri(database_url).ok_or("Invalid DATABASE_URL")?;
    let addr = format!("{host}:{port}");

    let conn = WireConn::connect(&addr, &user, &password, "postgres").await?;
    let mut pg = PgPipeline::new(conn);

    // Check if the database already exists.
    let (rows, _) = pg
        .simple_query_rows(&format!(
            "SELECT 1 FROM pg_database WHERE datname = '{}'",
            database.replace('\'', "''")
        ))
        .await?;

    if !rows.is_empty() {
        println!("Database '{database}' already exists.");
        return Ok(());
    }

    // CREATE DATABASE cannot run inside a transaction.
    pg.simple_query(&format!(
        "CREATE DATABASE \"{}\"",
        database.replace('"', "\"\"")
    ))
    .await?;

    println!("Created database '{database}'.");
    Ok(())
}

/// Drop a database. With `--force`, terminates active connections first.
pub async fn drop(database_url: &str, force: bool) -> Result<(), Box<dyn std::error::Error>> {
    let (user, password, host, port, database) =
        super::parse_pg_uri(database_url).ok_or("Invalid DATABASE_URL")?;
    let addr = format!("{host}:{port}");

    let conn = WireConn::connect(&addr, &user, &password, "postgres").await?;
    let mut pg = PgPipeline::new(conn);

    if force {
        // Terminate all other sessions connected to the target database.
        pg.simple_query(&format!(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
             WHERE datname = '{}' AND pid != pg_backend_pid()",
            database.replace('\'', "''")
        ))
        .await?;
    }

    pg.simple_query(&format!(
        "DROP DATABASE IF EXISTS \"{}\"",
        database.replace('"', "\"\"")
    ))
    .await?;

    println!("Dropped database '{database}'.");
    Ok(())
}
