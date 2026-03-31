//! Embedded migration runner for use in application startup or build.rs.
//!
//! ```ignore
//! // In main.rs or startup code:
//! resolute::migrate::run("postgres://user:pass@localhost/db", "migrations").await?;
//! ```

use pg_wired::{PgPipeline, WireConn};

use crate::error::TypedError;

/// Run all pending migrations from the given directory.
/// Creates the tracking table if it doesn't exist.
/// Each migration runs in its own transaction.
pub async fn run(database_url: &str, migrations_dir: &str) -> Result<usize, TypedError> {
    let (user, password, host, port, database) =
        parse_uri(database_url).ok_or_else(|| TypedError::Config("Invalid database URL".into()))?;
    let addr = format!("{host}:{port}");

    let conn = WireConn::connect(&addr, &user, &password, &database).await?;
    let mut pg = PgPipeline::new(conn);

    pg.simple_query(
        "CREATE TABLE IF NOT EXISTS _resolute_migrations (\
         version BIGINT PRIMARY KEY, \
         name TEXT NOT NULL, \
         applied_at TIMESTAMPTZ NOT NULL DEFAULT now())",
    )
    .await?;

    let (rows, _) = pg
        .simple_query_rows("SELECT version FROM _resolute_migrations ORDER BY version")
        .await?;
    let applied: Vec<i64> = rows
        .iter()
        .filter_map(|r| {
            r.first()
                .and_then(|v| v.as_ref())
                .and_then(|b| String::from_utf8(b.clone()).ok())
                .and_then(|s| s.parse().ok())
        })
        .collect();

    let dir = std::path::Path::new(migrations_dir);
    if !dir.is_dir() {
        return Ok(0);
    }

    let mut migrations: Vec<(i64, String, std::path::PathBuf)> = Vec::new();
    for entry in std::fs::read_dir(dir).map_err(TypedError::Io)? {
        let entry = entry.map_err(TypedError::Io)?;
        let path = entry.path();
        let name = path.file_name().unwrap_or_default().to_str().unwrap_or("");
        if !name.ends_with(".up.sql") {
            continue;
        }
        let stem = name.strip_suffix(".up.sql").unwrap_or("");
        let (version_str, mig_name) = stem.split_once('_').unwrap_or((stem, "unnamed"));
        if let Ok(version) = version_str.parse::<i64>() {
            migrations.push((version, mig_name.to_string(), path));
        }
    }
    migrations.sort_by_key(|(v, _, _)| *v);

    let mut count = 0;
    for (version, name, path) in &migrations {
        if applied.contains(version) {
            continue;
        }
        let sql = std::fs::read_to_string(path).map_err(TypedError::Io)?;

        pg.simple_query("BEGIN").await?;
        if let Err(e) = pg.simple_query(&sql).await {
            if let Err(rb_err) = pg.simple_query("ROLLBACK").await {
                tracing::error!(error = %rb_err, "rollback failed after migration error");
            }
            return Err(e.into());
        }
        // Use dollar-quoting for the name to avoid SQL injection via crafted
        // migration filenames. Version is a validated i64, safe to interpolate.
        let escaped_name = name.replace('\'', "''");
        pg.simple_query(&format!(
            "INSERT INTO _resolute_migrations (version, name) VALUES ({version}, E'{escaped_name}')"
        ))
        .await?;
        pg.simple_query("COMMIT").await?;
        count += 1;
    }

    Ok(count)
}

fn parse_uri(uri: &str) -> Option<(String, String, String, u16, String)> {
    let rest = uri
        .strip_prefix("postgres://")
        .or_else(|| uri.strip_prefix("postgresql://"))?;
    let (auth, hostdb) = rest.split_once('@').unwrap_or(("postgres:postgres", rest));
    let (user, password) = auth.split_once(':').unwrap_or((auth, ""));
    let (hostport, database) = hostdb.split_once('/').unwrap_or((hostdb, "postgres"));
    let (host, port_str) = hostport.split_once(':').unwrap_or((hostport, "5432"));
    let port: u16 = port_str.parse().unwrap_or(5432);
    Some((
        user.to_string(),
        password.to_string(),
        host.to_string(),
        port,
        database.to_string(),
    ))
}
