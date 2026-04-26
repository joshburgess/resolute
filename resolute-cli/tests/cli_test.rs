//! Integration tests for resolute-cli.
//! Requires: docker compose up -d (PostgreSQL on port 54322)

use std::process::Command;

const DB_URL: &str = "postgres://postgres:postgres@127.0.0.1:54322/postgres";

fn cli_bin() -> Command {
    Command::new(env!("CARGO_BIN_EXE_resolute-cli"))
}

/// Create a unique per-test database URL. Tests that mutate the
/// `_resolute_migrations` table must use a fresh database to avoid racing
/// other tests in the same binary (cargo test runs tests in parallel by
/// default, and that table is global to the database).
fn isolated_db_url(label: &str) -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::SeqCst);
    let pid = std::process::id();
    format!("postgres://postgres:postgres@127.0.0.1:54322/__resolute_test_{label}_{pid}_{n}")
}

fn setup_isolated_db(url: &str) {
    // Best-effort drop in case a previous run left it behind.
    let _ = cli_bin()
        .args(["database", "drop", "--database-url", url, "--force"])
        .output();
    let out = cli_bin()
        .args(["database", "create", "--database-url", url])
        .output()
        .expect("failed to create isolated test DB");
    assert!(
        out.status.success(),
        "isolated DB create failed for {url}:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
}

fn teardown_isolated_db(url: &str) {
    let _ = cli_bin()
        .args(["database", "drop", "--database-url", url, "--force"])
        .output();
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
        .expect("failed to run resolute-cli");

    assert!(
        output.status.success(),
        "exit code: {:?}\nstderr: {}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr)
    );

    // Verify files were created.
    let entries: Vec<_> = std::fs::read_dir(&migrations_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();

    assert_eq!(
        entries.len(),
        2,
        "expected up + down migration files, got: {:?}",
        entries
    );
    assert!(
        entries.iter().any(|n| n.ends_with("_add_users.up.sql")),
        "missing .up.sql file: {:?}",
        entries
    );
    assert!(
        entries.iter().any(|n| n.ends_with("_add_users.down.sql")),
        "missing .down.sql file: {:?}",
        entries
    );
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

    let url = isolated_db_url("run_status_revert");
    setup_isolated_db(&url);

    let dir = tempfile::tempdir().unwrap();
    let migrations_dir = dir.path().join("migrations");
    std::fs::create_dir_all(&migrations_dir).unwrap();

    // Create a migration manually. Any 14-digit numeric prefix works.
    let ts = "20240101000000";
    std::fs::write(
        migrations_dir.join(format!("{ts}_test_table.up.sql")),
        "CREATE TABLE IF NOT EXISTS __resolute_cli_test (id int PRIMARY KEY);",
    )
    .unwrap();
    std::fs::write(
        migrations_dir.join(format!("{ts}_test_table.down.sql")),
        "DROP TABLE IF EXISTS __resolute_cli_test;",
    )
    .unwrap();

    // Run migrations.
    let output = cli_bin()
        .args(["migrate", "run", "--database-url", &url, "--dir"])
        .arg(migrations_dir.to_str().unwrap())
        .output()
        .expect("failed to run resolute-cli migrate run");

    assert!(
        output.status.success(),
        "migrate run failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("Applied"),
        "expected 'Applied' in output: {stdout}"
    );

    // Check status.
    let output = cli_bin()
        .args(["migrate", "status", "--database-url", &url, "--dir"])
        .arg(migrations_dir.to_str().unwrap())
        .output()
        .expect("failed to run resolute-cli migrate status");

    assert!(
        output.status.success(),
        "migrate status failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("applied"),
        "expected 'applied' in status: {stdout}"
    );

    // Revert.
    let output = cli_bin()
        .args(["migrate", "revert", "--database-url", &url, "--dir"])
        .arg(migrations_dir.to_str().unwrap())
        .output()
        .expect("failed to run resolute-cli migrate revert");

    assert!(
        output.status.success(),
        "migrate revert failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("Reverted"),
        "expected 'Reverted' in output: {stdout}"
    );

    teardown_isolated_db(&url);
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

    let test_db_url = "postgres://postgres:postgres@127.0.0.1:54322/__resolute_cli_test_db";

    // Create database.
    let output = cli_bin()
        .args(["database", "create", "--database-url", test_db_url])
        .output()
        .expect("failed to run resolute-cli database create");

    assert!(
        output.status.success(),
        "database create failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );

    // Drop database.
    let output = cli_bin()
        .args(["database", "drop", "--database-url", test_db_url, "--force"])
        .output()
        .expect("failed to run resolute-cli database drop");

    assert!(
        output.status.success(),
        "database drop failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

// ---------------------------------------------------------------------------
// Help text
// ---------------------------------------------------------------------------

#[test]
fn test_help_output() {
    let output = cli_bin()
        .arg("--help")
        .output()
        .expect("failed to run resolute-cli --help");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("resolute-cli"),
        "help should mention resolute-cli"
    );
    assert!(stdout.contains("prepare"), "help should mention prepare");
    assert!(stdout.contains("migrate"), "help should mention migrate");
}

#[test]
fn test_migrate_help() {
    let output = cli_bin()
        .args(["migrate", "--help"])
        .output()
        .expect("failed to run resolute-cli migrate --help");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("create"),
        "migrate help should mention create"
    );
    assert!(stdout.contains("run"), "migrate help should mention run");
    assert!(
        stdout.contains("revert"),
        "migrate help should mention revert"
    );
    assert!(
        stdout.contains("status"),
        "migrate help should mention status"
    );
    assert!(
        stdout.contains("validate"),
        "migrate help should mention validate"
    );
    assert!(stdout.contains("info"), "migrate help should mention info");
}

// ---------------------------------------------------------------------------
// Migration: info, validate, error paths
// ---------------------------------------------------------------------------

/// Generate a unique 14-digit migration version to avoid colliding with
/// concurrent tests writing to the shared `_resolute_migrations` table.
fn unique_version() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::SeqCst);
    // 20990000000000 + serial keeps us in the int8 range and far past any
    // realistic real timestamp.
    format!("2099{:010}", n)
}

#[test]
fn test_migrate_info_shows_pending_sql_without_applying() {
    if std::env::var("CI_SKIP_DB_TESTS").is_ok() {
        return;
    }
    let url = isolated_db_url("info");
    setup_isolated_db(&url);

    let dir = tempfile::tempdir().unwrap();
    let migrations_dir = dir.path().join("migrations");
    std::fs::create_dir_all(&migrations_dir).unwrap();

    let ts = unique_version();
    let marker = format!("CREATE TABLE IF NOT EXISTS __resolute_info_marker_{ts} (id int)");
    std::fs::write(
        migrations_dir.join(format!("{ts}_marker.up.sql")),
        format!("{marker};"),
    )
    .unwrap();
    std::fs::write(
        migrations_dir.join(format!("{ts}_marker.down.sql")),
        format!("DROP TABLE IF EXISTS __resolute_info_marker_{ts};"),
    )
    .unwrap();

    let output = cli_bin()
        .args(["migrate", "info", "--database-url", &url, "--dir"])
        .arg(migrations_dir.to_str().unwrap())
        .output()
        .expect("failed to run resolute-cli migrate info");

    assert!(
        output.status.success(),
        "migrate info failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains(&marker),
        "info should print pending SQL: {stdout}"
    );
    // No row should have been inserted into _resolute_migrations.
    let status = cli_bin()
        .args(["migrate", "status", "--database-url", &url, "--dir"])
        .arg(migrations_dir.to_str().unwrap())
        .output()
        .unwrap();
    let status_out = String::from_utf8_lossy(&status.stdout);
    assert!(
        status_out.contains("pending"),
        "migration should still be pending after info: {status_out}"
    );

    teardown_isolated_db(&url);
}

#[test]
fn test_migrate_validate_clean() {
    if std::env::var("CI_SKIP_DB_TESTS").is_ok() {
        return;
    }
    let url = isolated_db_url("validate_clean");
    setup_isolated_db(&url);

    let dir = tempfile::tempdir().unwrap();
    let migrations_dir = dir.path().join("migrations");
    std::fs::create_dir_all(&migrations_dir).unwrap();

    let ts = unique_version();
    let table = format!("__resolute_validate_{ts}");
    std::fs::write(
        migrations_dir.join(format!("{ts}_validate_clean.up.sql")),
        format!("CREATE TABLE IF NOT EXISTS {table} (id int);"),
    )
    .unwrap();
    std::fs::write(
        migrations_dir.join(format!("{ts}_validate_clean.down.sql")),
        format!("DROP TABLE IF EXISTS {table};"),
    )
    .unwrap();

    // Apply.
    let run = cli_bin()
        .args(["migrate", "run", "--database-url", &url, "--dir"])
        .arg(migrations_dir.to_str().unwrap())
        .output()
        .unwrap();
    assert!(run.status.success());

    // Validate. Expect zero exit and "0 mismatched, 0 missing".
    let validate = cli_bin()
        .args(["migrate", "validate", "--database-url", &url, "--dir"])
        .arg(migrations_dir.to_str().unwrap())
        .output()
        .unwrap();
    let v_out = String::from_utf8_lossy(&validate.stdout);
    let v_err = String::from_utf8_lossy(&validate.stderr);
    assert!(
        validate.status.success(),
        "validate should succeed on a clean tree:\nstdout: {v_out}\nstderr: {v_err}"
    );
    assert!(
        v_out.contains("0 mismatched") && v_out.contains("0 missing"),
        "validate should report a clean tree: {v_out}"
    );

    teardown_isolated_db(&url);
}

#[test]
fn test_migrate_validate_detects_missing_file() {
    if std::env::var("CI_SKIP_DB_TESTS").is_ok() {
        return;
    }
    let url = isolated_db_url("validate_missing");
    setup_isolated_db(&url);

    let dir = tempfile::tempdir().unwrap();
    let migrations_dir = dir.path().join("migrations");
    std::fs::create_dir_all(&migrations_dir).unwrap();

    let ts = unique_version();
    let table = format!("__resolute_validate_missing_{ts}");
    let up_path = migrations_dir.join(format!("{ts}_v_missing.up.sql"));
    let down_path = migrations_dir.join(format!("{ts}_v_missing.down.sql"));
    std::fs::write(
        &up_path,
        format!("CREATE TABLE IF NOT EXISTS {table} (id int);"),
    )
    .unwrap();
    std::fs::write(&down_path, format!("DROP TABLE IF EXISTS {table};")).unwrap();

    // Apply.
    let run = cli_bin()
        .args(["migrate", "run", "--database-url", &url, "--dir"])
        .arg(migrations_dir.to_str().unwrap())
        .output()
        .unwrap();
    assert!(run.status.success());

    // Delete the up file so validate sees the migration recorded in DB but
    // missing on disk.
    std::fs::remove_file(&up_path).unwrap();

    let validate = cli_bin()
        .args(["migrate", "validate", "--database-url", &url, "--dir"])
        .arg(migrations_dir.to_str().unwrap())
        .output()
        .unwrap();
    let v_out = String::from_utf8_lossy(&validate.stdout);
    let v_err = String::from_utf8_lossy(&validate.stderr);
    assert!(
        !validate.status.success(),
        "validate should exit non-zero on missing file:\nstdout: {v_out}\nstderr: {v_err}"
    );
    assert!(
        v_err.contains("MISSING FILE") || v_out.contains("missing"),
        "validate should report missing file:\nstdout: {v_out}\nstderr: {v_err}"
    );

    teardown_isolated_db(&url);
}

#[test]
fn test_migrate_run_failing_sql_does_not_record() {
    if std::env::var("CI_SKIP_DB_TESTS").is_ok() {
        return;
    }
    let url = isolated_db_url("failing_sql");
    setup_isolated_db(&url);

    let dir = tempfile::tempdir().unwrap();
    let migrations_dir = dir.path().join("migrations");
    std::fs::create_dir_all(&migrations_dir).unwrap();

    let ts = unique_version();
    std::fs::write(
        migrations_dir.join(format!("{ts}_bad_sql.up.sql")),
        "this is not valid sql at all;",
    )
    .unwrap();
    std::fs::write(
        migrations_dir.join(format!("{ts}_bad_sql.down.sql")),
        "-- noop",
    )
    .unwrap();

    let run = cli_bin()
        .args(["migrate", "run", "--database-url", &url, "--dir"])
        .arg(migrations_dir.to_str().unwrap())
        .output()
        .unwrap();
    assert!(
        !run.status.success(),
        "migrate run should fail on invalid SQL"
    );

    // Status should still show the migration as pending (not applied).
    let status = cli_bin()
        .args(["migrate", "status", "--database-url", &url, "--dir"])
        .arg(migrations_dir.to_str().unwrap())
        .output()
        .unwrap();
    assert!(status.status.success());
    let s_out = String::from_utf8_lossy(&status.stdout);
    assert!(
        s_out.contains("pending"),
        "failed migration should remain pending: {s_out}"
    );

    teardown_isolated_db(&url);
}

// ---------------------------------------------------------------------------
// Database lifecycle: idempotent create
// ---------------------------------------------------------------------------

#[test]
fn test_database_create_is_idempotent() {
    if std::env::var("CI_SKIP_DB_TESTS").is_ok() {
        return;
    }
    let test_db_url = "postgres://postgres:postgres@127.0.0.1:54322/__resolute_idem_test_db";

    // First create: should report Created.
    let first = cli_bin()
        .args(["database", "create", "--database-url", test_db_url])
        .output()
        .unwrap();
    assert!(first.status.success());
    let first_out = String::from_utf8_lossy(&first.stdout);
    assert!(
        first_out.contains("Created"),
        "first create should report Created: {first_out}"
    );

    // Second create: should succeed, but report already exists.
    let second = cli_bin()
        .args(["database", "create", "--database-url", test_db_url])
        .output()
        .unwrap();
    assert!(second.status.success());
    let second_out = String::from_utf8_lossy(&second.stdout);
    assert!(
        second_out.contains("already exists"),
        "second create should report already exists: {second_out}"
    );

    // Cleanup.
    let drop = cli_bin()
        .args(["database", "drop", "--database-url", test_db_url, "--force"])
        .output()
        .unwrap();
    assert!(drop.status.success());
}

// ---------------------------------------------------------------------------
// Offline cache: prepare + check
// ---------------------------------------------------------------------------

#[test]
fn test_prepare_then_check_against_live_db() {
    if std::env::var("CI_SKIP_DB_TESTS").is_ok() {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let crate_dir = dir.path();
    std::fs::write(
        crate_dir.join("Cargo.toml"),
        "[package]\nname=\"prep_test\"\nversion=\"0.0.0\"\nedition=\"2021\"\n",
    )
    .unwrap();
    let src_dir = crate_dir.join("src");
    std::fs::create_dir_all(&src_dir).unwrap();
    std::fs::write(
        src_dir.join("lib.rs"),
        r#"fn _example() { let _ = query!("SELECT 1::int4 AS n"); }"#,
    )
    .unwrap();

    let prepare = cli_bin()
        .args(["prepare", "--database-url", DB_URL, "--source-dir"])
        .arg(crate_dir.to_str().unwrap())
        .output()
        .expect("failed to run resolute-cli prepare");
    assert!(
        prepare.status.success(),
        "prepare failed:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&prepare.stdout),
        String::from_utf8_lossy(&prepare.stderr),
    );
    let cache_dir = crate_dir.join(".resolute");
    assert!(
        cache_dir.is_dir(),
        ".resolute cache dir should exist after prepare"
    );
    let cached_files: Vec<_> = std::fs::read_dir(&cache_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|x| x == "json"))
        .collect();
    assert_eq!(cached_files.len(), 1, "expected 1 cached query json file");

    // check should pass since the cache was just produced from the same DB.
    let check = cli_bin()
        .args(["check", "--database-url", DB_URL, "--source-dir"])
        .arg(crate_dir.to_str().unwrap())
        .output()
        .expect("failed to run resolute-cli check");
    assert!(
        check.status.success(),
        "check failed:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&check.stdout),
        String::from_utf8_lossy(&check.stderr),
    );
    let check_out = String::from_utf8_lossy(&check.stdout);
    assert!(
        check_out.contains("1 queries OK") || check_out.contains("1 queries OK,"),
        "check should report 1 OK: {check_out}"
    );
}
