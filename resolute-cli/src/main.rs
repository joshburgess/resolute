//! CLI tool for resolute: offline cache management and database migrations.
//!
//! Usage:
//!   resolute-cli prepare               # Cache query metadata for offline builds
//!   resolute-cli check                 # Verify cached queries against DB
//!   resolute-cli migrate create <name> # Create a new migration
//!   resolute-cli migrate run           # Run pending migrations
//!   resolute-cli migrate revert        # Revert the last migration
//!   resolute-cli migrate status        # Show migration status

mod database;
mod migrate;

use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

#[derive(Parser)]
#[command(name = "resolute-cli", about = "Offline cache management for resolute query!() macro")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Scan source files for query!() invocations, connect to DB, and cache metadata.
    Prepare {
        /// Database URL (overrides DATABASE_URL env var).
        #[arg(long, env = "DATABASE_URL")]
        database_url: String,
        /// Directory to scan for .rs files (default: current directory).
        #[arg(long, default_value = ".")]
        source_dir: PathBuf,
    },
    /// Verify all cached queries are still valid against the database.
    Check {
        #[arg(long, env = "DATABASE_URL")]
        database_url: String,
    },
    /// Database migration management.
    Migrate {
        #[command(subcommand)]
        action: MigrateAction,
    },
    /// Database lifecycle management (create/drop).
    Database {
        #[command(subcommand)]
        action: DatabaseAction,
    },
}

#[derive(Subcommand)]
enum DatabaseAction {
    /// Create the database if it doesn't exist.
    Create {
        #[arg(long, env = "DATABASE_URL")]
        database_url: String,
    },
    /// Drop the database.
    Drop {
        #[arg(long, env = "DATABASE_URL")]
        database_url: String,
        /// Terminate active connections before dropping.
        #[arg(long)]
        force: bool,
    },
}

#[derive(Subcommand)]
enum MigrateAction {
    /// Create a new migration file pair (up + down).
    Create {
        /// Name of the migration (e.g., "create_users").
        name: String,
        /// Migrations directory.
        #[arg(long, default_value = "migrations")]
        dir: PathBuf,
    },
    /// Run all pending migrations.
    Run {
        #[arg(long, env = "DATABASE_URL")]
        database_url: String,
        #[arg(long, default_value = "migrations")]
        dir: PathBuf,
    },
    /// Revert the last applied migration.
    Revert {
        #[arg(long, env = "DATABASE_URL")]
        database_url: String,
        #[arg(long, default_value = "migrations")]
        dir: PathBuf,
    },
    /// Show which migrations are applied.
    Status {
        #[arg(long, env = "DATABASE_URL")]
        database_url: String,
        #[arg(long, default_value = "migrations")]
        dir: PathBuf,
    },
    /// Show the SQL of pending migrations without running them.
    Info {
        #[arg(long, default_value = "migrations")]
        dir: PathBuf,
        #[arg(long, env = "DATABASE_URL")]
        database_url: String,
    },
    /// Validate migration file checksums against applied migrations.
    Validate {
        #[arg(long, default_value = "migrations")]
        dir: PathBuf,
        #[arg(long, env = "DATABASE_URL")]
        database_url: String,
    },
    /// Load seed data from a SQL file.
    Seed {
        /// Path to the seed SQL file.
        #[arg(long, default_value = "seeds/seed.sql")]
        file: PathBuf,
        #[arg(long, env = "DATABASE_URL")]
        database_url: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CachedColumn {
    name: String,
    type_oid: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CacheEntry {
    sql: String,
    hash: u64,
    param_oids: Vec<u32>,
    columns: Vec<CachedColumn>,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    match cli.command {
        Command::Prepare { database_url, source_dir } => {
            prepare(&database_url, &source_dir).await?;
        }
        Command::Check { database_url } => {
            check(&database_url).await?;
        }
        Command::Migrate { action } => match action {
            MigrateAction::Create { name, dir } => {
                migrate::create(&dir, &name)?;
            }
            MigrateAction::Run { database_url, dir } => {
                migrate::run(&database_url, &dir).await?;
            }
            MigrateAction::Revert { database_url, dir } => {
                migrate::revert(&database_url, &dir).await?;
            }
            MigrateAction::Status { database_url, dir } => {
                migrate::status(&database_url, &dir).await?;
            }
            MigrateAction::Info { dir, database_url } => {
                migrate::info(&database_url, &dir).await?;
            }
            MigrateAction::Validate { dir, database_url } => {
                migrate::validate(&database_url, &dir).await?;
            }
            MigrateAction::Seed { file, database_url } => {
                migrate::seed(&database_url, &file).await?;
            }
        },
        Command::Database { action } => match action {
            DatabaseAction::Create { database_url } => {
                database::create(&database_url).await?;
            }
            DatabaseAction::Drop { database_url, force } => {
                database::drop(&database_url, force).await?;
            }
        },
    }
    Ok(())
}

/// Scan source files for query!() calls, describe each, write cache.
async fn prepare(
    database_url: &str,
    source_dir: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let (user, password, host, port, database) = parse_pg_uri(database_url)
        .ok_or("Invalid DATABASE_URL")?;
    let addr = format!("{host}:{port}");

    // Find all query!() SQL strings in .rs files.
    let queries = scan_source_files(source_dir)?;
    if queries.is_empty() {
        println!("No query!() invocations found.");
        return Ok(());
    }
    println!("Found {} query!() invocations", queries.len());

    // Connect to PG.
    let mut conn = pg_wired::WireConn::connect(&addr, &user, &password, &database).await?;
    println!("Connected to {database}@{host}:{port}");

    // Create .sqlx directory.
    let cache_dir = find_workspace_root(source_dir)
        .unwrap_or_else(|| source_dir.to_path_buf())
        .join(".sqlx");
    std::fs::create_dir_all(&cache_dir)?;

    let mut cached = 0;
    let mut failed = 0;

    for sql in &queries {
        let hash = hash_sql(sql);
        match conn.describe_statement(sql).await {
            Ok((param_oids, fields)) => {
                let entry = CacheEntry {
                    sql: sql.clone(),
                    hash,
                    param_oids,
                    columns: fields
                        .iter()
                        .map(|f| CachedColumn {
                            name: f.name.clone(),
                            type_oid: f.type_oid,
                        })
                        .collect(),
                };
                let path = cache_dir.join(format!("query-{hash:016x}.json"));
                let json = serde_json::to_string_pretty(&entry)?;
                std::fs::write(&path, json)?;
                cached += 1;
            }
            Err(e) => {
                eprintln!("  FAIL: {sql}");
                eprintln!("        {e}");
                failed += 1;
            }
        }
    }

    println!("Cached {cached} queries, {failed} failed");
    if failed > 0 {
        std::process::exit(1);
    }
    Ok(())
}

/// Check all cached queries against the live database.
async fn check(database_url: &str) -> Result<(), Box<dyn std::error::Error>> {
    let (user, password, host, port, database) = parse_pg_uri(database_url)
        .ok_or("Invalid DATABASE_URL")?;
    let addr = format!("{host}:{port}");

    let cache_dir = PathBuf::from(".sqlx");
    if !cache_dir.is_dir() {
        println!("No .sqlx cache directory found. Run `resolute-cli prepare` first.");
        return Ok(());
    }

    let mut conn = pg_wired::WireConn::connect(&addr, &user, &password, &database).await?;
    println!("Connected to {database}@{host}:{port}");

    let mut ok = 0;
    let mut stale = 0;

    for entry in std::fs::read_dir(&cache_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().map(|e| e == "json").unwrap_or(false) {
            let data = std::fs::read_to_string(&path)?;
            let cached: CacheEntry = serde_json::from_str(&data)?;

            match conn.describe_statement(&cached.sql).await {
                Ok((param_oids, fields)) => {
                    let cols: Vec<CachedColumn> = fields
                        .iter()
                        .map(|f| CachedColumn {
                            name: f.name.clone(),
                            type_oid: f.type_oid,
                        })
                        .collect();

                    if param_oids != cached.param_oids || !columns_match(&cols, &cached.columns) {
                        eprintln!("  STALE: {}", cached.sql);
                        stale += 1;
                    } else {
                        ok += 1;
                    }
                }
                Err(e) => {
                    eprintln!("  FAIL: {}", cached.sql);
                    eprintln!("        {e}");
                    stale += 1;
                }
            }
        }
    }

    println!("{ok} queries OK, {stale} stale");
    if stale > 0 {
        println!("Run `resolute-cli prepare` to update the cache.");
        std::process::exit(1);
    }
    Ok(())
}

fn columns_match(a: &[CachedColumn], b: &[CachedColumn]) -> bool {
    a.len() == b.len()
        && a.iter()
            .zip(b.iter())
            .all(|(x, y)| x.name == y.name && x.type_oid == y.type_oid)
}

/// Scan .rs files for `query!("...")` invocations and extract the SQL strings.
fn scan_source_files(dir: &Path) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let mut queries = Vec::new();
    scan_dir(dir, &mut queries)?;
    // Deduplicate.
    queries.sort();
    queries.dedup();
    Ok(queries)
}

fn scan_dir(dir: &Path, queries: &mut Vec<String>) -> Result<(), Box<dyn std::error::Error>> {
    if !dir.is_dir() {
        return Ok(());
    }
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            let name = path.file_name().unwrap_or_default().to_str().unwrap_or("");
            // Skip target, .git, etc.
            if name == "target" || name.starts_with('.') {
                continue;
            }
            scan_dir(&path, queries)?;
        } else if path.extension().map(|e| e == "rs").unwrap_or(false) {
            scan_file(&path, queries)?;
        }
    }
    Ok(())
}

/// Extract SQL strings from `query!("SQL" ...)`, `query_as!(Type, "SQL" ...)`,
/// `query_scalar!("SQL" ...)`, and their `resolute::` prefixed variants.
fn scan_file(path: &Path, queries: &mut Vec<String>) -> Result<(), Box<dyn std::error::Error>> {
    let source = std::fs::read_to_string(path)?;
    // Search for all three macro patterns.
    for pattern in &["query!(", "query_as!(", "query_scalar!(", "query_file!(", "query_file_as!(", "query_file_scalar!("] {
        let mut pos = 0;
        while let Some(idx) = source[pos..].find(pattern) {
            let after_paren = pos + idx + pattern.len();
            let rest = &source[after_paren..];
            let trimmed = rest.trim_start();

            // For query_as!/query_file_as!, skip the type argument and comma first.
            let trimmed = if (*pattern == "query_as!(" || *pattern == "query_file_as!(") && !trimmed.starts_with('"') {
                // Skip to the first comma, then trim again.
                if let Some(comma_pos) = trimmed.find(',') {
                    trimmed[comma_pos + 1..].trim_start()
                } else {
                    pos = after_paren;
                    continue;
                }
            } else {
                trimmed
            };

            if !trimmed.starts_with('"') {
                pos = after_paren;
                continue;
            }
            let actual_start = source.len() - trimmed.len();
            let quote_start = actual_start + 1; // After the `"`
            if let Some(end) = find_string_end(&source, quote_start) {
                let raw = &source[quote_start..end];
                let raw = raw.replace("\\\"", "\"").replace("\\n", "\n").replace("\\\\", "\\");

                // For query_file! variants, raw is a file path — read the SQL.
                let sql = if pattern.starts_with("query_file") {
                    let manifest_dir = path.parent().unwrap_or(Path::new("."));
                    // Walk up to find CARGO_MANIFEST_DIR equivalent.
                    let crate_root = find_crate_root(manifest_dir);
                    let full_path = crate_root.join(&raw);
                    match std::fs::read_to_string(&full_path) {
                        Ok(s) => s.trim().to_string(),
                        Err(_) => {
                            // Try relative to source dir root.
                            raw
                        }
                    }
                } else {
                    raw
                };

                queries.push(sql);
                pos = end + 1;
            } else {
                pos = after_paren;
            }
        }
    }
    Ok(())
}

/// Find the closing `"` of a Rust string literal, handling escapes.
fn find_string_end(source: &str, start: usize) -> Option<usize> {
    let bytes = source.as_bytes();
    let mut i = start;
    while i < bytes.len() {
        if bytes[i] == b'\\' {
            i += 2; // Skip escaped char.
        } else if bytes[i] == b'"' {
            return Some(i);
        } else {
            i += 1;
        }
    }
    None
}

fn hash_sql(sql: &str) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for b in sql.bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

pub(crate) fn parse_pg_uri(uri: &str) -> Option<(String, String, String, u16, String)> {
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

/// Find the crate root (directory containing Cargo.toml) by walking up.
fn find_crate_root(start: &Path) -> PathBuf {
    let mut dir = if start.is_file() {
        start.parent().unwrap_or(Path::new(".")).to_path_buf()
    } else {
        start.to_path_buf()
    };
    loop {
        if dir.join("Cargo.toml").exists() {
            return dir;
        }
        if !dir.pop() {
            return start.to_path_buf();
        }
    }
}

fn find_workspace_root(start: &Path) -> Option<PathBuf> {
    let mut dir = start.to_path_buf();
    if dir.is_file() {
        dir.pop();
    }
    loop {
        let cargo_toml = dir.join("Cargo.toml");
        if cargo_toml.exists() {
            if let Ok(contents) = std::fs::read_to_string(&cargo_toml) {
                if contents.contains("[workspace]") {
                    return Some(dir);
                }
            }
        }
        if !dir.pop() {
            return None;
        }
    }
}
