//! Integration tests for stalwart-cli.
//! Requires: docker compose up -d (PostgreSQL on port 54322)

use std::process::Command;

const DB_URL: &str = "postgres://postgres:postgres@127.0.0.1:54322/postgres";

fn cli_bin() -> Command {
    Command::new(env!("CARGO_BIN_EXE_stalwart-cli"))
}

// ---------------------------------------------------------------------------
// Migration: create
// ---------------------------------------------------------------------------

#[test]
fn test_migrate_create() {
    let dir = tempfile::tempdir().unwrap();
    let migrations_dir = dir.path().join("migrations");

    let output = cli_bin()
        .args(["migrate", "create", "add_users", "--dir"])
        .arg(migrations_dir.to_str().unwrap())
        .output()
        .expect("failed to run stalwart-cli");

    assert!(output.status.success(), "exit code: {:?}\nstderr: {}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr));

    // Verify files were created.
    let entries: Vec<_> = std::fs::read_dir(&migrations_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();

    assert_eq!(entries.len(), 2, "expected up + down migration files, got: {:?}", entries);
    assert!(entries.iter().any(|n| n.ends_with("_add_users.up.sql")),
        "missing .up.sql file: {:?}", entries);
    assert!(entries.iter().any(|n| n.ends_with("_add_users.down.sql")),
        "missing .down.sql file: {:?}", entries);
}

// ---------------------------------------------------------------------------
// Migration: run, status, revert
// ---------------------------------------------------------------------------

#[test]
fn test_migrate_run_status_revert() {
    if std::env::var("CI_SKIP_DB_TESTS").is_ok() {
        eprintln!("Skipping DB test: CI_SKIP_DB_TESTS set");
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let migrations_dir = dir.path().join("migrations");
    std::fs::create_dir_all(&migrations_dir).unwrap();

    // Create a migration manually.
    let ts = chrono::Utc::now().format("%Y%m%d%H%M%S");
    std::fs::write(
        migrations_dir.join(format!("{ts}_test_table.up.sql")),
        "CREATE TABLE IF NOT EXISTS __stalwart_cli_test (id int PRIMARY KEY);",
    ).unwrap();
    std::fs::write(
        migrations_dir.join(format!("{ts}_test_table.down.sql")),
        "DROP TABLE IF EXISTS __stalwart_cli_test;",
    ).unwrap();

    // Run migrations.
    let output = cli_bin()
        .args(["migrate", "run", "--database-url", DB_URL, "--dir"])
        .arg(migrations_dir.to_str().unwrap())
        .output()
        .expect("failed to run stalwart-cli migrate run");

    assert!(output.status.success(), "migrate run failed:\n{}",
        String::from_utf8_lossy(&output.stderr));
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Applied"), "expected 'Applied' in output: {stdout}");

    // Check status.
    let output = cli_bin()
        .args(["migrate", "status", "--database-url", DB_URL, "--dir"])
        .arg(migrations_dir.to_str().unwrap())
        .output()
        .expect("failed to run stalwart-cli migrate status");

    assert!(output.status.success(), "migrate status failed:\n{}",
        String::from_utf8_lossy(&output.stderr));
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("applied"), "expected 'applied' in status: {stdout}");

    // Revert.
    let output = cli_bin()
        .args(["migrate", "revert", "--database-url", DB_URL, "--dir"])
        .arg(migrations_dir.to_str().unwrap())
        .output()
        .expect("failed to run stalwart-cli migrate revert");

    assert!(output.status.success(), "migrate revert failed:\n{}",
        String::from_utf8_lossy(&output.stderr));
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Reverted"), "expected 'Reverted' in output: {stdout}");
}

// ---------------------------------------------------------------------------
// Database: create and drop
// ---------------------------------------------------------------------------

#[test]
fn test_database_create_and_drop() {
    if std::env::var("CI_SKIP_DB_TESTS").is_ok() {
        eprintln!("Skipping DB test: CI_SKIP_DB_TESTS set");
        return;
    }

    let test_db_url = "postgres://postgres:postgres@127.0.0.1:54322/__stalwart_cli_test_db";

    // Create database.
    let output = cli_bin()
        .args(["database", "create", "--database-url", test_db_url])
        .output()
        .expect("failed to run stalwart-cli database create");

    assert!(output.status.success(), "database create failed:\n{}",
        String::from_utf8_lossy(&output.stderr));

    // Drop database.
    let output = cli_bin()
        .args(["database", "drop", "--database-url", test_db_url, "--force"])
        .output()
        .expect("failed to run stalwart-cli database drop");

    assert!(output.status.success(), "database drop failed:\n{}",
        String::from_utf8_lossy(&output.stderr));
}

// ---------------------------------------------------------------------------
// Help text
// ---------------------------------------------------------------------------

#[test]
fn test_help_output() {
    let output = cli_bin()
        .arg("--help")
        .output()
        .expect("failed to run stalwart-cli --help");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("stalwart-cli"), "help should mention stalwart-cli");
    assert!(stdout.contains("prepare"), "help should mention prepare");
    assert!(stdout.contains("migrate"), "help should mention migrate");
}

#[test]
fn test_migrate_help() {
    let output = cli_bin()
        .args(["migrate", "--help"])
        .output()
        .expect("failed to run stalwart-cli migrate --help");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("create"), "migrate help should mention create");
    assert!(stdout.contains("run"), "migrate help should mention run");
    assert!(stdout.contains("revert"), "migrate help should mention revert");
    assert!(stdout.contains("status"), "migrate help should mention status");
}
