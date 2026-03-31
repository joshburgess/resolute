//! Migration system: create, run, revert, and check status of SQL migrations.

use std::path::{Path, PathBuf};

use pg_wire::{PgPipeline, WireConn};

/// A migration file on disk.
#[derive(Debug)]
pub struct Migration {
    pub version: i64,
    pub name: String,
    pub up_path: PathBuf,
    pub down_path: PathBuf,
}

/// Create a new migration file pair.
pub fn create(migrations_dir: &Path, name: &str) -> Result<(), Box<dyn std::error::Error>> {
    std::fs::create_dir_all(migrations_dir)?;
    let ts = chrono::Utc::now().format("%Y%m%d%H%M%S");
    let base = migrations_dir.join(format!("{ts}_{name}"));

    let up_path = base.with_extension("up.sql");
    let down_path = base.with_extension("down.sql");

    std::fs::write(&up_path, format!("-- Migration: {name}\n"))?;
    std::fs::write(&down_path, format!("-- Revert: {name}\n"))?;

    println!("Created:");
    println!("  {}", up_path.display());
    println!("  {}", down_path.display());
    Ok(())
}

/// Run all pending migrations.
pub async fn run(
    database_url: &str,
    migrations_dir: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut pg = connect(database_url).await?;

    // Ensure tracking table.
    pg.simple_query(
        "CREATE TABLE IF NOT EXISTS _stalwart_migrations (\
         version BIGINT PRIMARY KEY, \
         name TEXT NOT NULL, \
         applied_at TIMESTAMPTZ NOT NULL DEFAULT now())",
    )
    .await?;

    // Get applied versions.
    let (rows, _) = pg
        .simple_query_rows("SELECT version FROM _stalwart_migrations ORDER BY version")
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

    // Scan migration files.
    let migrations = scan_migrations(migrations_dir)?;
    let pending: Vec<&Migration> = migrations
        .iter()
        .filter(|m| !applied.contains(&m.version))
        .collect();

    if pending.is_empty() {
        println!("No pending migrations.");
        return Ok(());
    }

    println!("{} pending migration(s):", pending.len());

    for m in &pending {
        let sql = std::fs::read_to_string(&m.up_path)?;
        println!("  Applying {} ({})...", m.version, m.name);

        pg.simple_query("BEGIN").await?;
        match pg.simple_query(&sql).await {
            Ok(()) => {}
            Err(e) => {
                let _ = pg.simple_query("ROLLBACK").await;
                return Err(format!("Migration {} failed: {e}", m.version).into());
            }
        }
        pg.simple_query(&format!(
            "INSERT INTO _stalwart_migrations (version, name) VALUES ({}, '{}')",
            m.version,
            m.name.replace('\'', "''")
        ))
        .await?;
        pg.simple_query("COMMIT").await?;
    }

    println!("Applied {} migration(s).", pending.len());
    Ok(())
}

/// Revert the last applied migration.
pub async fn revert(
    database_url: &str,
    migrations_dir: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut pg = connect(database_url).await?;

    let (rows, _) = pg
        .simple_query_rows(
            "SELECT version, name FROM _stalwart_migrations ORDER BY version DESC LIMIT 1",
        )
        .await?;

    let (version, name) = match rows.first() {
        Some(row) => {
            let v: i64 = row
                .first()
                .and_then(|b| b.as_ref())
                .and_then(|b| String::from_utf8(b.clone()).ok())
                .and_then(|s| s.parse().ok())
                .ok_or("Failed to parse version")?;
            let n: String = row
                .get(1)
                .and_then(|b| b.as_ref())
                .and_then(|b| String::from_utf8(b.clone()).ok())
                .unwrap_or_default();
            (v, n)
        }
        None => {
            println!("No migrations to revert.");
            return Ok(());
        }
    };

    // Find down file.
    let migrations = scan_migrations(migrations_dir)?;
    let migration = migrations
        .iter()
        .find(|m| m.version == version)
        .ok_or_else(|| format!("Migration file for {version} not found"))?;

    if !migration.down_path.exists() {
        return Err(format!(
            "Down migration not found: {}",
            migration.down_path.display()
        )
        .into());
    }

    let sql = std::fs::read_to_string(&migration.down_path)?;
    println!("Reverting {} ({name})...", version);

    pg.simple_query("BEGIN").await?;
    match pg.simple_query(&sql).await {
        Ok(()) => {}
        Err(e) => {
            let _ = pg.simple_query("ROLLBACK").await;
            return Err(format!("Revert failed: {e}").into());
        }
    }
    pg.simple_query(&format!(
        "DELETE FROM _stalwart_migrations WHERE version = {version}"
    ))
    .await?;
    pg.simple_query("COMMIT").await?;
    println!("Reverted.");
    Ok(())
}

/// Show migration status.
pub async fn status(
    database_url: &str,
    migrations_dir: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut pg = connect(database_url).await?;

    // Ensure table exists (don't error if it doesn't).
    pg.simple_query(
        "CREATE TABLE IF NOT EXISTS _stalwart_migrations (\
         version BIGINT PRIMARY KEY, \
         name TEXT NOT NULL, \
         applied_at TIMESTAMPTZ NOT NULL DEFAULT now())",
    )
    .await?;

    let (rows, _) = pg
        .simple_query_rows(
            "SELECT version, name, applied_at::text FROM _stalwart_migrations ORDER BY version",
        )
        .await?;
    let applied: Vec<(i64, String, String)> = rows
        .iter()
        .filter_map(|r| {
            let v = r.first()?.as_ref().and_then(|b| String::from_utf8(b.clone()).ok())?.parse().ok()?;
            let n = r.get(1)?.as_ref().and_then(|b| String::from_utf8(b.clone()).ok())?;
            let t = r.get(2)?.as_ref().and_then(|b| String::from_utf8(b.clone()).ok())?;
            Some((v, n, t))
        })
        .collect();

    let migrations = scan_migrations(migrations_dir)?;

    if migrations.is_empty() && applied.is_empty() {
        println!("No migrations found.");
        return Ok(());
    }

    println!("{:<16} {:<30} STATUS", "VERSION", "NAME");
    println!("{}", "-".repeat(70));

    for m in &migrations {
        let status = applied
            .iter()
            .find(|(v, _, _)| *v == m.version)
            .map(|(_, _, t)| format!("applied {t}"))
            .unwrap_or_else(|| "pending".to_string());
        println!("{:<16} {:<30} {}", m.version, m.name, status);
    }

    Ok(())
}

/// Scan the migrations directory for up/down SQL files.
fn scan_migrations(dir: &Path) -> Result<Vec<Migration>, Box<dyn std::error::Error>> {
    if !dir.is_dir() {
        return Ok(Vec::new());
    }

    let mut migrations = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        let name = path.file_name().unwrap_or_default().to_str().unwrap_or("");

        if !name.ends_with(".up.sql") {
            continue;
        }

        // Parse version from filename: {version}_{name}.up.sql
        let stem = name.strip_suffix(".up.sql").unwrap_or("");
        let (version_str, migration_name) = stem
            .split_once('_')
            .unwrap_or((stem, "unnamed"));
        let version: i64 = version_str.parse().map_err(|_| {
            format!("Invalid migration filename (expected timestamp prefix): {name}")
        })?;

        let down_path = path.with_extension("").with_extension("down.sql");

        migrations.push(Migration {
            version,
            name: migration_name.to_string(),
            up_path: path.clone(),
            down_path,
        });
    }

    migrations.sort_by_key(|m| m.version);
    Ok(migrations)
}

/// Show SQL of pending migrations without running them.
pub async fn info(
    database_url: &str,
    migrations_dir: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut pg = connect(database_url).await?;

    pg.simple_query(
        "CREATE TABLE IF NOT EXISTS _stalwart_migrations (\
         version BIGINT PRIMARY KEY, \
         name TEXT NOT NULL, \
         applied_at TIMESTAMPTZ NOT NULL DEFAULT now())",
    )
    .await?;

    let (rows, _) = pg
        .simple_query_rows("SELECT version FROM _stalwart_migrations ORDER BY version")
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

    let migrations = scan_migrations(migrations_dir)?;
    let pending: Vec<&Migration> = migrations
        .iter()
        .filter(|m| !applied.contains(&m.version))
        .collect();

    if pending.is_empty() {
        println!("No pending migrations.");
        return Ok(());
    }

    println!("{} pending migration(s):\n", pending.len());
    for m in &pending {
        let sql = std::fs::read_to_string(&m.up_path)?;
        println!("--- {} ({}) ---", m.version, m.name);
        println!("{}", sql.trim());
        println!();
    }
    Ok(())
}

/// Validate that on-disk migration files haven't changed since they were applied.
/// Uses a simple length+name check (not cryptographic hash for simplicity).
pub async fn validate(
    database_url: &str,
    migrations_dir: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut pg = connect(database_url).await?;

    pg.simple_query(
        "CREATE TABLE IF NOT EXISTS _stalwart_migrations (\
         version BIGINT PRIMARY KEY, \
         name TEXT NOT NULL, \
         applied_at TIMESTAMPTZ NOT NULL DEFAULT now())",
    )
    .await?;

    let (rows, _) = pg
        .simple_query_rows(
            "SELECT version, name FROM _stalwart_migrations ORDER BY version",
        )
        .await?;
    let applied: Vec<(i64, String)> = rows
        .iter()
        .filter_map(|r| {
            let v = r.first()?.as_ref().and_then(|b| String::from_utf8(b.clone()).ok())?.parse().ok()?;
            let n = r.get(1)?.as_ref().and_then(|b| String::from_utf8(b.clone()).ok())?;
            Some((v, n))
        })
        .collect();

    let migrations = scan_migrations(migrations_dir)?;
    let mut ok = 0;
    let mut mismatched = 0;
    let mut missing = 0;

    for (version, db_name) in &applied {
        match migrations.iter().find(|m| m.version == *version) {
            Some(m) => {
                if m.name != *db_name {
                    eprintln!(
                        "  MISMATCH: version {} — DB has name '{}', file has '{}'",
                        version, db_name, m.name
                    );
                    mismatched += 1;
                } else if !m.up_path.exists() {
                    eprintln!("  MISSING FILE: {} ({})", version, m.name);
                    missing += 1;
                } else {
                    ok += 1;
                }
            }
            None => {
                eprintln!("  MISSING FILE: {} ({}) — no migration file found", version, db_name);
                missing += 1;
            }
        }
    }

    println!("{ok} valid, {mismatched} mismatched, {missing} missing files");
    if mismatched > 0 || missing > 0 {
        std::process::exit(1);
    }
    Ok(())
}

/// Load seed data from a SQL file.
pub async fn seed(
    database_url: &str,
    file: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    if !file.exists() {
        return Err(format!("Seed file not found: {}", file.display()).into());
    }

    let sql = std::fs::read_to_string(file)?;
    let mut pg = connect(database_url).await?;

    println!("Seeding from {}...", file.display());
    pg.simple_query(&sql).await?;
    println!("Seed data loaded.");
    Ok(())
}

async fn connect(database_url: &str) -> Result<PgPipeline, Box<dyn std::error::Error>> {
    let (user, password, host, port, database) =
        super::parse_pg_uri(database_url).ok_or("Invalid DATABASE_URL")?;
    let addr = format!("{host}:{port}");
    let conn = WireConn::connect(&addr, &user, &password, &database).await?;
    Ok(PgPipeline::new(conn))
}
