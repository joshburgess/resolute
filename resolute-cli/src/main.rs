//! CLI tool for resolute: offline cache management and database migrations.
//!
//! Usage:
//!   resolute-cli prepare               # Cache query metadata for offline builds
//!   resolute-cli check                 # Verify cached queries against DB
//!   resolute-cli migrate create NAME   # Create a new migration
//!   resolute-cli migrate run           # Run pending migrations
//!   resolute-cli migrate revert        # Revert the last migration
//!   resolute-cli migrate status        # Show migration status
//!
//! Migration and database-lifecycle operations delegate to
//! `resolute::migrate` and `resolute::admin`; this binary is a thin
//! presentation layer on top of those modules.

use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use syn::visit::Visit;

#[derive(Parser)]
#[command(
    name = "resolute-cli",
    about = "Offline cache management for resolute query!() macro"
)]
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
        /// Directory to start the workspace-root search from (default: current directory).
        #[arg(long, default_value = ".")]
        source_dir: PathBuf,
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

const CACHE_DIR_NAME: &str = ".resolute";

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    match cli.command {
        Command::Prepare {
            database_url,
            source_dir,
        } => {
            prepare(&database_url, &source_dir).await?;
        }
        Command::Check {
            database_url,
            source_dir,
        } => {
            check(&database_url, &source_dir).await?;
        }
        Command::Migrate { action } => run_migrate(action).await?,
        Command::Database { action } => run_database(action).await?,
    }
    Ok(())
}

async fn run_migrate(action: MigrateAction) -> Result<(), Box<dyn std::error::Error>> {
    match action {
        MigrateAction::Create { name, dir } => {
            let (up, down) = resolute::migrate::create(&dir, &name)?;
            println!("Created:");
            println!("  {}", up.display());
            println!("  {}", down.display());
        }
        MigrateAction::Run { database_url, dir } => {
            let applied = resolute::migrate::run(&database_url, &dir).await?;
            if applied.is_empty() {
                println!("No pending migrations.");
            } else {
                println!("{} pending migration(s):", applied.len());
                for m in &applied {
                    println!("  Applied {} ({}).", m.version, m.name);
                }
                println!("Applied {} migration(s).", applied.len());
            }
        }
        MigrateAction::Revert { database_url, dir } => {
            match resolute::migrate::revert(&database_url, &dir).await? {
                Some(m) => println!("Reverted {} ({}).", m.version, m.name),
                None => println!("No migrations to revert."),
            }
        }
        MigrateAction::Status { database_url, dir } => {
            let report = resolute::migrate::status(&database_url, &dir).await?;
            if report.files.is_empty() && report.applied.is_empty() {
                println!("No migrations found.");
                return Ok(());
            }
            println!("{:<16} {:<30} STATUS", "VERSION", "NAME");
            println!("{}", "-".repeat(70));
            for m in &report.files {
                let status = report
                    .applied
                    .iter()
                    .find(|a| a.version == m.version)
                    .map(|a| format!("applied {}", a.applied_at))
                    .unwrap_or_else(|| "pending".to_string());
                println!("{:<16} {:<30} {}", m.version, m.name, status);
            }
        }
        MigrateAction::Info { dir, database_url } => {
            let pending = resolute::migrate::info(&database_url, &dir).await?;
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
        }
        MigrateAction::Validate { dir, database_url } => {
            let report = resolute::migrate::validate(&database_url, &dir).await?;
            for (recorded, file) in &report.mismatched {
                eprintln!(
                    "  MISMATCH: version {} (DB has name '{}', file has '{}')",
                    recorded.version, recorded.name, file.name
                );
            }
            for missing in &report.missing {
                eprintln!("  MISSING FILE: {} ({})", missing.version, missing.name);
            }
            println!(
                "{} valid, {} mismatched, {} missing files",
                report.ok.len(),
                report.mismatched.len(),
                report.missing.len()
            );
            if !report.is_clean() {
                std::process::exit(1);
            }
        }
        MigrateAction::Seed { file, database_url } => {
            println!("Seeding from {}...", file.display());
            resolute::migrate::seed(&database_url, &file).await?;
            println!("Seed data loaded.");
        }
    }
    Ok(())
}

async fn run_database(action: DatabaseAction) -> Result<(), Box<dyn std::error::Error>> {
    match action {
        DatabaseAction::Create { database_url } => {
            let database = database_name(&database_url)?;
            if resolute::admin::create_database(&database_url).await? {
                println!("Created database '{database}'.");
            } else {
                println!("Database '{database}' already exists.");
            }
        }
        DatabaseAction::Drop {
            database_url,
            force,
        } => {
            let database = database_name(&database_url)?;
            resolute::admin::drop_database(&database_url, force).await?;
            println!("Dropped database '{database}'.");
        }
    }
    Ok(())
}

fn database_name(database_url: &str) -> Result<String, Box<dyn std::error::Error>> {
    let (_, _, _, _, database) = parse_pg_uri(database_url).ok_or("Invalid DATABASE_URL")?;
    Ok(database)
}

/// Resolve the cache directory for a given starting point.
///
/// Prefers an existing `.resolute/` walking up from `start`; otherwise places
/// it next to the nearest `Cargo.toml` containing `[workspace]`. Falls back to
/// `start/.resolute` if no workspace root can be found.
fn resolve_cache_dir(start: &Path) -> PathBuf {
    let mut dir = if start.is_file() {
        start.parent().unwrap_or(Path::new(".")).to_path_buf()
    } else {
        start.to_path_buf()
    };

    let search_origin = dir.clone();

    loop {
        let candidate = dir.join(CACHE_DIR_NAME);
        if candidate.is_dir() {
            return candidate;
        }
        if !dir.pop() {
            break;
        }
    }

    if let Some(root) = find_workspace_root(&search_origin) {
        return root.join(CACHE_DIR_NAME);
    }

    search_origin.join(CACHE_DIR_NAME)
}

/// Scan source files for query!() calls, describe each, write cache.
async fn prepare(database_url: &str, source_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let (user, password, host, port, database) =
        parse_pg_uri(database_url).ok_or("Invalid DATABASE_URL")?;
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

    let cache_dir = resolve_cache_dir(source_dir);
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
async fn check(database_url: &str, source_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let (user, password, host, port, database) =
        parse_pg_uri(database_url).ok_or("Invalid DATABASE_URL")?;
    let addr = format!("{host}:{port}");

    let cache_dir = resolve_cache_dir(source_dir);
    if !cache_dir.is_dir() {
        println!(
            "No {CACHE_DIR_NAME} cache directory found (looked up from {}). \
             Run `resolute-cli prepare` first.",
            source_dir.display()
        );
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

/// Walk `dir` recursively, parse each `.rs` file, and collect SQL strings
/// from every `query!`, `query_as!`, `query_scalar!`, and the `query_file*`
/// variants we find in the AST. Skips `target/` and dotfile directories.
fn scan_source_files(dir: &Path) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let mut queries = Vec::new();
    scan_dir(dir, &mut queries)?;
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
            if name == "target" || name.starts_with('.') {
                continue;
            }
            scan_dir(&path, queries)?;
        } else if path.extension().map(|e| e == "rs").unwrap_or(false) {
            scan_file(&path, queries);
        }
    }
    Ok(())
}

/// Parse a single `.rs` file and collect any query macro invocations.
/// Unparseable files are reported to stderr but do not abort the scan.
fn scan_file(path: &Path, queries: &mut Vec<String>) {
    let source = match std::fs::read_to_string(path) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("  warn: cannot read {}: {e}", path.display());
            return;
        }
    };
    let file = match syn::parse_file(&source) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("  warn: cannot parse {}: {e}", path.display());
            return;
        }
    };
    let crate_root = find_crate_root(path);
    let mut visitor = MacroVisitor {
        queries,
        crate_root,
    };
    visitor.visit_file(&file);
}

/// Syn visitor that finds query macro invocations anywhere in the AST,
/// including inside expressions, statements, and nested macro arguments.
struct MacroVisitor<'q> {
    queries: &'q mut Vec<String>,
    crate_root: PathBuf,
}

impl<'ast> Visit<'ast> for MacroVisitor<'_> {
    fn visit_macro(&mut self, mac: &'ast syn::Macro) {
        if let Some(name) = mac.path.segments.last().map(|s| s.ident.to_string()) {
            let tokens = mac.tokens.clone();
            let raw = match name.as_str() {
                "query" | "query_scalar" => parse_first_litstr(tokens.clone()),
                "query_as" => parse_second_litstr_after_type(tokens.clone()),
                "query_file" | "query_file_scalar" => {
                    parse_first_litstr(tokens.clone()).and_then(|p| self.read_query_file(&p))
                }
                "query_file_as" => parse_second_litstr_after_type(tokens.clone())
                    .and_then(|p| self.read_query_file(&p)),
                _ => None,
            };
            if let Some(sql) = raw {
                self.queries.push(sql);
            }
            // Re-visit the macro's inner tokens as expressions so nested
            // `query!` calls (e.g. inside another macro's arguments) are
            // picked up. Silently ignore parse failures — not every macro's
            // body is a valid expression list.
            if let Ok(parsed) = syn::parse2::<ExprList>(mac.tokens.clone()) {
                for expr in &parsed.exprs {
                    self.visit_expr(expr);
                }
            }
        }
        syn::visit::visit_macro(self, mac);
    }
}

impl MacroVisitor<'_> {
    fn read_query_file(&self, rel_path: &str) -> Option<String> {
        let full = self.crate_root.join(rel_path);
        std::fs::read_to_string(&full)
            .map(|s| s.trim().to_string())
            .ok()
    }
}

/// Parser that extracts the first `LitStr` in a macro argument list and
/// discards the rest. Used for `query!("SQL", args...)`.
struct FirstLitStr(String);

impl syn::parse::Parse for FirstLitStr {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let lit: syn::LitStr = input.parse()?;
        let _rest: proc_macro2::TokenStream = input.parse()?;
        Ok(FirstLitStr(lit.value()))
    }
}

/// Parser for `query_as!(TargetType, "SQL", args...)` shape — skips the type
/// and comma, returns the SQL string.
struct SecondLitStr(String);

impl syn::parse::Parse for SecondLitStr {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let _ty: syn::Type = input.parse()?;
        input.parse::<syn::Token![,]>()?;
        let lit: syn::LitStr = input.parse()?;
        let _rest: proc_macro2::TokenStream = input.parse()?;
        Ok(SecondLitStr(lit.value()))
    }
}

/// A comma-separated list of expressions, used for re-visiting macro args.
struct ExprList {
    exprs: Vec<syn::Expr>,
}

impl syn::parse::Parse for ExprList {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let punctuated: syn::punctuated::Punctuated<syn::Expr, syn::Token![,]> =
            syn::punctuated::Punctuated::parse_terminated(input)?;
        Ok(ExprList {
            exprs: punctuated.into_iter().collect(),
        })
    }
}

fn parse_first_litstr(tokens: proc_macro2::TokenStream) -> Option<String> {
    syn::parse2::<FirstLitStr>(tokens).ok().map(|s| s.0)
}

fn parse_second_litstr_after_type(tokens: proc_macro2::TokenStream) -> Option<String> {
    syn::parse2::<SecondLitStr>(tokens).ok().map(|s| s.0)
}

fn hash_sql(sql: &str) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for b in sql.bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

fn parse_pg_uri(uri: &str) -> Option<(String, String, String, u16, String)> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    fn write_crate(dir: &Path, src: &str) -> PathBuf {
        fs::write(
            dir.join("Cargo.toml"),
            "[package]\nname=\"t\"\nversion=\"0.0.0\"\n",
        )
        .unwrap();
        let src_dir = dir.join("src");
        fs::create_dir_all(&src_dir).unwrap();
        let path = src_dir.join("lib.rs");
        fs::write(&path, src).unwrap();
        path
    }

    fn scan(src: &str) -> Vec<String> {
        let dir = tempdir().unwrap();
        let path = write_crate(dir.path(), src);
        let mut queries = Vec::new();
        scan_file(&path, &mut queries);
        queries
    }

    #[test]
    fn scans_basic_query() {
        let queries = scan(r#"fn main() { let _ = query!("SELECT 1"); }"#);
        assert_eq!(queries, vec!["SELECT 1"]);
    }

    #[test]
    fn scans_query_with_args() {
        let queries = scan(r#"fn main() { let _ = query!("SELECT $1::int", 42); }"#);
        assert_eq!(queries, vec!["SELECT $1::int"]);
    }

    #[test]
    fn scans_query_scalar() {
        let queries = scan(r#"fn main() { let _ = query_scalar!("SELECT count(*) FROM t"); }"#);
        assert_eq!(queries, vec!["SELECT count(*) FROM t"]);
    }

    #[test]
    fn query_as_skips_type() {
        let queries =
            scan(r#"fn main() { let _ = query_as!(User, "SELECT id, name FROM users"); }"#);
        assert_eq!(queries, vec!["SELECT id, name FROM users"]);
    }

    #[test]
    fn query_as_with_path_type() {
        let queries = scan(
            r#"fn main() { let _ = query_as!(crate::models::User, "SELECT id FROM users"); }"#,
        );
        assert_eq!(queries, vec!["SELECT id FROM users"]);
    }

    #[test]
    fn path_prefixed_invocation() {
        let queries = scan(r#"fn main() { let _ = resolute::query!("SELECT 2"); }"#);
        assert_eq!(queries, vec!["SELECT 2"]);
    }

    #[test]
    fn cfg_gated_code_is_scanned() {
        let queries = scan(
            r#"#[cfg(feature = "x")]
               fn gated() { let _ = query!("SELECT 3"); }"#,
        );
        assert_eq!(queries, vec!["SELECT 3"]);
    }

    #[test]
    fn nested_macro_arguments() {
        let queries = scan(
            r#"fn main() {
                   println!("{:?}", query!("SELECT nested"));
               }"#,
        );
        assert_eq!(queries, vec!["SELECT nested"]);
    }

    #[test]
    fn multiple_queries_in_one_file() {
        let queries = scan(
            r#"fn a() { let _ = query!("SELECT 1"); }
               fn b() { let _ = query_scalar!("SELECT 2"); }
               fn c() { let _ = query_as!(T, "SELECT 3"); }"#,
        );
        assert_eq!(queries, vec!["SELECT 1", "SELECT 2", "SELECT 3"]);
    }

    #[test]
    fn query_unchecked_is_not_scanned() {
        let queries = scan(r#"fn main() { let _ = query_unchecked!("SELECT skip"); }"#);
        assert!(queries.is_empty(), "got: {:?}", queries);
    }

    #[test]
    fn non_query_macros_are_ignored() {
        let queries = scan(r#"fn main() { println!("not a query"); vec!["x", "y"]; }"#);
        assert!(queries.is_empty(), "got: {:?}", queries);
    }

    #[test]
    fn unparseable_file_is_warned_not_panicked() {
        let dir = tempdir().unwrap();
        fs::write(
            dir.path().join("Cargo.toml"),
            "[package]\nname=\"t\"\nversion=\"0.0.0\"\n",
        )
        .unwrap();
        let src_dir = dir.path().join("src");
        fs::create_dir_all(&src_dir).unwrap();
        let path = src_dir.join("lib.rs");
        fs::write(&path, "fn broken( {").unwrap();
        let mut queries = Vec::new();
        scan_file(&path, &mut queries);
        assert!(queries.is_empty());
    }

    #[test]
    fn query_file_reads_sql_from_disk() {
        let dir = tempdir().unwrap();
        fs::write(
            dir.path().join("Cargo.toml"),
            "[package]\nname=\"t\"\nversion=\"0.0.0\"\n",
        )
        .unwrap();
        let src_dir = dir.path().join("src");
        fs::create_dir_all(&src_dir).unwrap();
        let queries_dir = dir.path().join("queries");
        fs::create_dir_all(&queries_dir).unwrap();
        fs::write(
            queries_dir.join("get_user.sql"),
            "SELECT id FROM users WHERE id = $1\n",
        )
        .unwrap();
        let rs_path = src_dir.join("lib.rs");
        fs::write(
            &rs_path,
            r#"fn main() { let _ = query_file!("queries/get_user.sql", 1); }"#,
        )
        .unwrap();
        let mut queries = Vec::new();
        scan_file(&rs_path, &mut queries);
        assert_eq!(queries, vec!["SELECT id FROM users WHERE id = $1"]);
    }

    #[test]
    fn query_file_as_skips_type_and_reads_disk() {
        let dir = tempdir().unwrap();
        fs::write(
            dir.path().join("Cargo.toml"),
            "[package]\nname=\"t\"\nversion=\"0.0.0\"\n",
        )
        .unwrap();
        let src_dir = dir.path().join("src");
        fs::create_dir_all(&src_dir).unwrap();
        let queries_dir = dir.path().join("queries");
        fs::create_dir_all(&queries_dir).unwrap();
        fs::write(queries_dir.join("list.sql"), "SELECT * FROM t").unwrap();
        let rs_path = src_dir.join("lib.rs");
        fs::write(
            &rs_path,
            r#"fn main() { let _ = query_file_as!(Row, "queries/list.sql"); }"#,
        )
        .unwrap();
        let mut queries = Vec::new();
        scan_file(&rs_path, &mut queries);
        assert_eq!(queries, vec!["SELECT * FROM t"]);
    }

    #[test]
    fn query_file_missing_file_is_skipped() {
        let dir = tempdir().unwrap();
        fs::write(
            dir.path().join("Cargo.toml"),
            "[package]\nname=\"t\"\nversion=\"0.0.0\"\n",
        )
        .unwrap();
        let src_dir = dir.path().join("src");
        fs::create_dir_all(&src_dir).unwrap();
        let rs_path = src_dir.join("lib.rs");
        fs::write(
            &rs_path,
            r#"fn main() { let _ = query_file!("queries/does_not_exist.sql"); }"#,
        )
        .unwrap();
        let mut queries = Vec::new();
        scan_file(&rs_path, &mut queries);
        assert!(queries.is_empty());
    }

    #[test]
    fn raw_string_literal_query() {
        let queries = scan(r##"fn main() { let _ = query!(r#"SELECT "quoted" FROM t"#); }"##);
        assert_eq!(queries, vec![r#"SELECT "quoted" FROM t"#]);
    }
}
