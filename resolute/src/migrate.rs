//! Migration runner for use in applications, build scripts, and tooling.
//!
//! The primary entry point is [`run`], which applies every pending migration
//! from a directory of `{version}_{name}.up.sql` / `.down.sql` file pairs.
//! Companion helpers (`create`, `revert`, `status`, `info`, `validate`,
//! `seed`) mirror what `resolute-cli migrate ...` exposes: the CLI is a thin
//! presentation layer on top of these functions.
//!
//! ```ignore
//! // In main.rs or startup code:
//! resolute::migrate::run("postgres://user:pass@localhost/db", "migrations").await?;
//! ```

use std::path::{Path, PathBuf};

use pg_wired::{PgPipeline, WireConn};

use crate::error::TypedError;

/// A migration file pair on disk.
#[derive(Debug, Clone)]
pub struct Migration {
    pub version: i64,
    pub name: String,
    pub up_path: PathBuf,
    pub down_path: PathBuf,
}

/// A row from `_resolute_migrations`: a migration that has been applied.
#[derive(Debug, Clone)]
pub struct AppliedMigration {
    pub version: i64,
    pub name: String,
    /// ISO-8601 timestamp text, as returned by PostgreSQL's `timestamptz::text` cast.
    pub applied_at: String,
}

/// Combined view of on-disk files and applied rows, as used by `status`.
#[derive(Debug, Clone)]
pub struct StatusReport {
    /// Migration files found on disk, sorted by version.
    pub files: Vec<Migration>,
    /// Rows present in `_resolute_migrations`, sorted by version.
    pub applied: Vec<AppliedMigration>,
}

/// Result of `validate`: how the on-disk state compares to the tracking table.
#[derive(Debug, Clone, Default)]
pub struct ValidateReport {
    /// Applied migrations whose on-disk name matches the recorded name.
    pub ok: Vec<AppliedMigration>,
    /// Applied migrations where the file name changed since the row was written.
    /// `(recorded, current_file)`.
    pub mismatched: Vec<(AppliedMigration, Migration)>,
    /// Applied migrations whose file has been removed or renamed.
    pub missing: Vec<AppliedMigration>,
}

impl ValidateReport {
    /// `true` when no migrations have drifted from their recorded state.
    pub fn is_clean(&self) -> bool {
        self.mismatched.is_empty() && self.missing.is_empty()
    }
}

/// Create a new `{timestamp}_{name}.up.sql` / `.down.sql` pair under `dir`.
/// Returns the paths to the newly-written up and down files. The timestamp
/// is `YYYYMMDDHHMMSS` in UTC.
///
/// Does not contact a database. Intended for CLI-style invocation.
pub fn create(dir: &Path, name: &str) -> std::io::Result<(PathBuf, PathBuf)> {
    std::fs::create_dir_all(dir)?;
    let ts = utc_timestamp_prefix();
    let base = dir.join(format!("{ts}_{name}"));
    let up_path = base.with_extension("up.sql");
    let down_path = base.with_extension("down.sql");
    std::fs::write(&up_path, format!("-- Migration: {name}\n"))?;
    std::fs::write(&down_path, format!("-- Revert: {name}\n"))?;
    Ok((up_path, down_path))
}

fn utc_timestamp_prefix() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    format_yyyymmddhhmmss(secs)
}

/// Format a UTC unix timestamp as `YYYYMMDDHHMMSS`. Valid for years 1970-9999.
/// Uses the Gregorian calendar (days-since-epoch algorithm), no leap seconds.
fn format_yyyymmddhhmmss(secs: u64) -> String {
    const SECS_PER_MIN: u64 = 60;
    const SECS_PER_HOUR: u64 = SECS_PER_MIN * 60;
    const SECS_PER_DAY: u64 = SECS_PER_HOUR * 24;

    let days = (secs / SECS_PER_DAY) as i64;
    let tod = secs % SECS_PER_DAY;
    let hour = tod / SECS_PER_HOUR;
    let minute = (tod % SECS_PER_HOUR) / SECS_PER_MIN;
    let second = tod % SECS_PER_MIN;

    // Howard Hinnant's civil_from_days (MIT): converts days since 1970-01-01
    // to (y, m, d) in the Gregorian calendar.
    let z = days + 719468;
    let era = z.div_euclid(146097);
    let doe = (z - era * 146097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = y + if m <= 2 { 1 } else { 0 };

    format!("{year:04}{m:02}{d:02}{hour:02}{minute:02}{second:02}")
}

/// Run all pending migrations. Returns the list of migrations newly applied,
/// in the order they ran. Each migration runs in its own transaction.
pub async fn run(
    database_url: &str,
    migrations_dir: impl AsRef<Path>,
) -> Result<Vec<Migration>, TypedError> {
    let mut pg = connect(database_url).await?;
    ensure_tracking_table(&mut pg).await?;

    let applied = read_applied_versions(&mut pg).await?;
    let migrations = scan_migrations(migrations_dir.as_ref())?;

    let mut newly_applied = Vec::new();
    for m in &migrations {
        if applied.contains(&m.version) {
            continue;
        }
        let sql = std::fs::read_to_string(&m.up_path).map_err(TypedError::Io)?;

        tracing::info!(version = m.version, name = %m.name, "applying migration");

        pg.simple_query("BEGIN").await?;
        if let Err(e) = pg.simple_query(&sql).await {
            if let Err(rb_err) = pg.simple_query("ROLLBACK").await {
                tracing::error!(error = %rb_err, "rollback failed after migration error");
            }
            return Err(e.into());
        }
        // `name` comes from the filename. Escape both quote styles and use the
        // E'...' string to make `\` literal too, matching PostgreSQL's default
        // `standard_conforming_strings = on` behavior.
        let escaped = m.name.replace('\\', "\\\\").replace('\'', "''");
        pg.simple_query(&format!(
            "INSERT INTO _resolute_migrations (version, name) VALUES ({}, E'{}')",
            m.version, escaped,
        ))
        .await?;
        pg.simple_query("COMMIT").await?;
        newly_applied.push(m.clone());
    }

    Ok(newly_applied)
}

/// Revert the most recently applied migration. Returns the reverted migration,
/// or `None` when nothing has been applied.
pub async fn revert(
    database_url: &str,
    migrations_dir: impl AsRef<Path>,
) -> Result<Option<Migration>, TypedError> {
    let mut pg = connect(database_url).await?;
    ensure_tracking_table(&mut pg).await?;

    let (rows, _) = pg
        .simple_query_rows(
            "SELECT version, name FROM _resolute_migrations ORDER BY version DESC LIMIT 1",
        )
        .await?;
    let Some(row) = rows.first() else {
        return Ok(None);
    };

    let version: i64 = row
        .first()
        .and_then(|b| b.as_ref())
        .and_then(|b| String::from_utf8(b.clone()).ok())
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| TypedError::Config("failed to parse migration version".into()))?;
    let recorded_name: String = row
        .get(1)
        .and_then(|b| b.as_ref())
        .and_then(|b| String::from_utf8(b.clone()).ok())
        .unwrap_or_default();

    let migrations = scan_migrations(migrations_dir.as_ref())?;
    let migration = migrations
        .iter()
        .find(|m| m.version == version)
        .cloned()
        .ok_or_else(|| {
            TypedError::Config(format!("no migration file found for version {version}"))
        })?;

    if !migration.down_path.exists() {
        return Err(TypedError::Config(format!(
            "down migration missing: {}",
            migration.down_path.display()
        )));
    }

    let sql = std::fs::read_to_string(&migration.down_path).map_err(TypedError::Io)?;
    tracing::info!(version, name = %recorded_name, "reverting migration");

    pg.simple_query("BEGIN").await?;
    if let Err(e) = pg.simple_query(&sql).await {
        if let Err(rb_err) = pg.simple_query("ROLLBACK").await {
            tracing::error!(error = %rb_err, "rollback failed after revert error");
        }
        return Err(e.into());
    }
    pg.simple_query(&format!(
        "DELETE FROM _resolute_migrations WHERE version = {version}"
    ))
    .await?;
    pg.simple_query("COMMIT").await?;

    Ok(Some(migration))
}

/// Snapshot on-disk files + tracking table rows for presentation.
pub async fn status(
    database_url: &str,
    migrations_dir: impl AsRef<Path>,
) -> Result<StatusReport, TypedError> {
    let mut pg = connect(database_url).await?;
    ensure_tracking_table(&mut pg).await?;
    let applied = read_applied(&mut pg).await?;
    let files = scan_migrations(migrations_dir.as_ref())?;
    Ok(StatusReport { files, applied })
}

/// List the pending migrations (ones in `dir` that are not in the tracking table).
pub async fn info(
    database_url: &str,
    migrations_dir: impl AsRef<Path>,
) -> Result<Vec<Migration>, TypedError> {
    let mut pg = connect(database_url).await?;
    ensure_tracking_table(&mut pg).await?;
    let applied = read_applied_versions(&mut pg).await?;
    let files = scan_migrations(migrations_dir.as_ref())?;
    Ok(files
        .into_iter()
        .filter(|m| !applied.contains(&m.version))
        .collect())
}

/// Compare the tracking table to on-disk files and report drift.
pub async fn validate(
    database_url: &str,
    migrations_dir: impl AsRef<Path>,
) -> Result<ValidateReport, TypedError> {
    let mut pg = connect(database_url).await?;
    ensure_tracking_table(&mut pg).await?;
    let applied = read_applied(&mut pg).await?;
    let files = scan_migrations(migrations_dir.as_ref())?;

    let mut report = ValidateReport::default();
    for a in applied {
        match files.iter().find(|m| m.version == a.version) {
            Some(m) if m.name != a.name => {
                report.mismatched.push((a, m.clone()));
            }
            Some(m) if !m.up_path.exists() => {
                report.missing.push(a);
                let _ = m;
            }
            Some(_) => report.ok.push(a),
            None => report.missing.push(a),
        }
    }
    Ok(report)
}

/// Apply a seed SQL file against the target database. Runs as one simple_query.
pub async fn seed(database_url: &str, file: &Path) -> Result<(), TypedError> {
    if !file.exists() {
        return Err(TypedError::Config(format!(
            "seed file not found: {}",
            file.display()
        )));
    }
    let sql = std::fs::read_to_string(file).map_err(TypedError::Io)?;
    let mut pg = connect(database_url).await?;
    pg.simple_query(&sql).await?;
    Ok(())
}

/// Scan `dir` for `{version}_{name}.up.sql` files and return a sorted `Vec`.
/// Directories that don't exist return an empty list rather than erroring.
pub fn scan_migrations(dir: &Path) -> Result<Vec<Migration>, TypedError> {
    if !dir.is_dir() {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    for entry in std::fs::read_dir(dir).map_err(TypedError::Io)? {
        let entry = entry.map_err(TypedError::Io)?;
        let path = entry.path();
        let name = path.file_name().unwrap_or_default().to_str().unwrap_or("");
        if !name.ends_with(".up.sql") {
            continue;
        }
        let stem = name.strip_suffix(".up.sql").unwrap_or("");
        let (version_str, mig_name) = stem.split_once('_').unwrap_or((stem, "unnamed"));
        let version: i64 = version_str.parse().map_err(|_| {
            TypedError::Config(format!(
                "invalid migration filename (expected timestamp prefix): {name}"
            ))
        })?;
        let down_path = path.with_extension("").with_extension("down.sql");
        out.push(Migration {
            version,
            name: mig_name.to_string(),
            up_path: path,
            down_path,
        });
    }
    out.sort_by_key(|m| m.version);
    Ok(out)
}

async fn connect(database_url: &str) -> Result<PgPipeline, TypedError> {
    let (user, password, host, port, database) =
        parse_uri(database_url).ok_or_else(|| TypedError::Config("invalid database URL".into()))?;
    let addr = format!("{host}:{port}");
    let conn = WireConn::connect(&addr, &user, &password, &database).await?;
    Ok(PgPipeline::new(conn))
}

async fn ensure_tracking_table(pg: &mut PgPipeline) -> Result<(), TypedError> {
    pg.simple_query(
        "CREATE TABLE IF NOT EXISTS _resolute_migrations (\
         version BIGINT PRIMARY KEY, \
         name TEXT NOT NULL, \
         applied_at TIMESTAMPTZ NOT NULL DEFAULT now())",
    )
    .await?;
    Ok(())
}

async fn read_applied_versions(pg: &mut PgPipeline) -> Result<Vec<i64>, TypedError> {
    let (rows, _) = pg
        .simple_query_rows("SELECT version FROM _resolute_migrations ORDER BY version")
        .await?;
    Ok(rows
        .iter()
        .filter_map(|r| {
            r.first()
                .and_then(|v| v.as_ref())
                .and_then(|b| String::from_utf8(b.clone()).ok())
                .and_then(|s| s.parse().ok())
        })
        .collect())
}

async fn read_applied(pg: &mut PgPipeline) -> Result<Vec<AppliedMigration>, TypedError> {
    let (rows, _) = pg
        .simple_query_rows(
            "SELECT version, name, applied_at::text FROM _resolute_migrations ORDER BY version",
        )
        .await?;
    Ok(rows
        .iter()
        .filter_map(|r| {
            let v: i64 = r
                .first()?
                .as_ref()
                .and_then(|b| String::from_utf8(b.clone()).ok())?
                .parse()
                .ok()?;
            let n = r
                .get(1)?
                .as_ref()
                .and_then(|b| String::from_utf8(b.clone()).ok())?;
            let t = r
                .get(2)?
                .as_ref()
                .and_then(|b| String::from_utf8(b.clone()).ok())?;
            Some(AppliedMigration {
                version: v,
                name: n,
                applied_at: t,
            })
        })
        .collect())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scan_migrations_sorts_and_rejects_garbage() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("20240101120000_b.up.sql"), "").unwrap();
        std::fs::write(dir.path().join("20240101120000_b.down.sql"), "").unwrap();
        std::fs::write(dir.path().join("20230101120000_a.up.sql"), "").unwrap();
        std::fs::write(dir.path().join("20230101120000_a.down.sql"), "").unwrap();
        std::fs::write(dir.path().join("README.md"), "").unwrap();

        let migrations = scan_migrations(dir.path()).unwrap();
        assert_eq!(migrations.len(), 2);
        assert_eq!(migrations[0].name, "a");
        assert_eq!(migrations[1].name, "b");
        assert!(migrations[0].version < migrations[1].version);
    }

    #[test]
    fn scan_migrations_rejects_non_numeric_prefix() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("notanumber_a.up.sql"), "").unwrap();
        let err = scan_migrations(dir.path()).unwrap_err();
        match err {
            TypedError::Config(msg) => assert!(msg.contains("invalid migration filename")),
            other => panic!("unexpected error kind: {other:?}"),
        }
    }

    #[test]
    fn scan_migrations_empty_when_dir_missing() {
        let tmp = tempfile::tempdir().unwrap();
        let missing = tmp.path().join("does-not-exist");
        assert!(scan_migrations(&missing).unwrap().is_empty());
    }

    #[test]
    fn create_writes_up_and_down_pair() {
        let dir = tempfile::tempdir().unwrap();
        let (up, down) = create(dir.path(), "add_widgets").unwrap();
        assert!(up
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .contains("add_widgets"));
        assert!(up.exists());
        assert!(down.exists());
        assert!(std::fs::read_to_string(&up)
            .unwrap()
            .contains("Migration: add_widgets"));
        assert!(std::fs::read_to_string(&down)
            .unwrap()
            .contains("Revert: add_widgets"));
    }

    #[test]
    fn parse_uri_handles_common_shapes() {
        let (u, p, h, port, db) =
            parse_uri("postgres://alice:secret@example.com:5433/mydb").unwrap();
        assert_eq!(u, "alice");
        assert_eq!(p, "secret");
        assert_eq!(h, "example.com");
        assert_eq!(port, 5433);
        assert_eq!(db, "mydb");

        // No password.
        let (u, p, _, _, _) = parse_uri("postgres://alice@example.com/mydb").unwrap();
        assert_eq!(u, "alice");
        assert_eq!(p, "");

        // Defaults for database.
        let (_, _, _, _, db) = parse_uri("postgres://alice:pw@example.com").unwrap();
        assert_eq!(db, "postgres");
    }

    #[test]
    fn format_yyyymmddhhmmss_spot_checks() {
        assert_eq!(format_yyyymmddhhmmss(0), "19700101000000");
        assert_eq!(format_yyyymmddhhmmss(1704067200), "20240101000000");
        assert_eq!(format_yyyymmddhhmmss(1234567890), "20090213233130");
        // Leap year Feb 29 2020 00:00:00 UTC = 1582934400
        assert_eq!(format_yyyymmddhhmmss(1582934400), "20200229000000");
        // End-of-year rollover: 2023-12-31 23:59:59 UTC = 1704067199
        assert_eq!(format_yyyymmddhhmmss(1704067199), "20231231235959");
    }

    #[test]
    fn validate_report_is_clean_helper() {
        let mut r = ValidateReport::default();
        assert!(r.is_clean());
        r.missing.push(AppliedMigration {
            version: 1,
            name: "x".into(),
            applied_at: "".into(),
        });
        assert!(!r.is_clean());
    }
}
