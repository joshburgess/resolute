//! Database lifecycle helpers: `CREATE DATABASE` / `DROP DATABASE` against a
//! maintenance connection. Useful for test scaffolding and dev tooling where
//! you don't want a separate `psql` dependency.
//!
//! Connects to the `postgres` maintenance database on the same host/port, so
//! the target database doesn't have to exist yet.

use pg_wired::{PgPipeline, WireConn};

use crate::error::TypedError;

/// Create `database` if it does not already exist. Returns `true` if a new
/// database was created, `false` if it was already present.
pub async fn create_database(database_url: &str) -> Result<bool, TypedError> {
    let (user, password, host, port, database) =
        crate::query::parse_connection_string(database_url)
            .ok_or_else(|| TypedError::Config("invalid database URL".into()))?;
    let addr = format!("{host}:{port}");

    let conn = WireConn::connect(&addr, &user, &password, "postgres").await?;
    let mut pg = PgPipeline::new(conn);

    let (rows, _) = pg
        .simple_query_rows(&format!(
            "SELECT 1 FROM pg_database WHERE datname = '{}'",
            database.replace('\'', "''")
        ))
        .await?;
    if !rows.is_empty() {
        return Ok(false);
    }
    pg.simple_query(&format!(
        "CREATE DATABASE \"{}\"",
        database.replace('"', "\"\"")
    ))
    .await?;
    Ok(true)
}

/// Drop `database`. With `force`, first terminates other sessions connected
/// to the target so the DROP can proceed. Succeeds silently if the database
/// does not exist.
pub async fn drop_database(database_url: &str, force: bool) -> Result<(), TypedError> {
    let (user, password, host, port, database) =
        crate::query::parse_connection_string(database_url)
            .ok_or_else(|| TypedError::Config("invalid database URL".into()))?;
    let addr = format!("{host}:{port}");

    let conn = WireConn::connect(&addr, &user, &password, "postgres").await?;
    let mut pg = PgPipeline::new(conn);

    if force {
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
    Ok(())
}
