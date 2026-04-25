//! Test database helper: creates a temporary database for isolated testing.
//!
//! ```ignore
//! #[tokio::test]
//! async fn my_test() {
//!     let db = TestDb::create("127.0.0.1:5432", "postgres", "postgres").await.unwrap();
//!     let client = db.client().await.unwrap();
//!     client.simple_query("CREATE TABLE t (id int)").await.unwrap();
//!     // ... test ...
//!     // database dropped automatically on `db.drop_db().await`
//! }
//! ```

use crate::error::TypedError;
use crate::query::Client;

/// Default `host:port` used when `RESOLUTE_TEST_ADDR` is unset.
pub const DEFAULT_TEST_ADDR: &str = "127.0.0.1:54322";
/// Default role used when `RESOLUTE_TEST_USER` is unset.
pub const DEFAULT_TEST_USER: &str = "postgres";
/// Default password used when `RESOLUTE_TEST_PASSWORD` is unset.
pub const DEFAULT_TEST_PASSWORD: &str = "postgres";
/// Default database used when `RESOLUTE_TEST_DB` is unset.
pub const DEFAULT_TEST_DB: &str = "postgrest_test";

fn cached(slot: &'static std::sync::OnceLock<String>, var: &str, default: &str) -> &'static str {
    slot.get_or_init(|| std::env::var(var).unwrap_or_else(|_| default.to_string()))
        .as_str()
}

/// `host:port` of the test PostgreSQL server.
///
/// Reads `RESOLUTE_TEST_ADDR` once on first call and caches the result. Falls
/// back to [`DEFAULT_TEST_ADDR`]. Use this from integration tests, examples,
/// and benches so a single environment variable can redirect every connection
/// at a different cluster.
pub fn test_addr() -> &'static str {
    static V: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    cached(&V, "RESOLUTE_TEST_ADDR", DEFAULT_TEST_ADDR)
}

/// Test role. Reads `RESOLUTE_TEST_USER`; falls back to [`DEFAULT_TEST_USER`].
pub fn test_user() -> &'static str {
    static V: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    cached(&V, "RESOLUTE_TEST_USER", DEFAULT_TEST_USER)
}

/// Test password. Reads `RESOLUTE_TEST_PASSWORD`; falls back to [`DEFAULT_TEST_PASSWORD`].
pub fn test_password() -> &'static str {
    static V: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    cached(&V, "RESOLUTE_TEST_PASSWORD", DEFAULT_TEST_PASSWORD)
}

/// Test database name. Reads `RESOLUTE_TEST_DB`; falls back to [`DEFAULT_TEST_DB`].
pub fn test_database() -> &'static str {
    static V: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    cached(&V, "RESOLUTE_TEST_DB", DEFAULT_TEST_DB)
}

/// Compose a `postgres://user:password@host/database` URL from the
/// `RESOLUTE_TEST_*` env vars (or defaults). Used by tests and benches that
/// take a libpq-style connection string.
pub fn test_database_url() -> String {
    format!(
        "postgres://{}:{}@{}/{}",
        test_user(),
        test_password(),
        test_addr(),
        test_database()
    )
}

/// A temporary test database that is dropped on cleanup.
///
/// # Examples
///
/// ```ignore
/// #[tokio::test]
/// async fn test_insert() {
///     let db = TestDb::create("127.0.0.1:5432", "postgres", "postgres").await.unwrap();
///     let client = db.client().await.unwrap();
///     client.simple_query("CREATE TABLE items (id serial PRIMARY KEY, name text)").await.unwrap();
///     client.execute("INSERT INTO items (name) VALUES ($1)", &[&"widget"]).await.unwrap();
///     let rows = client.query("SELECT name FROM items", &[]).await.unwrap();
///     assert_eq!(rows.len(), 1);
///     db.drop_db().await.unwrap();
/// }
/// ```
pub struct TestDb {
    /// Connection address (`host:port`) of the underlying PostgreSQL server.
    pub addr: String,
    /// Role used to create and connect to the test database.
    pub user: String,
    /// Password for [`TestDb::user`].
    pub password: String,
    /// The randomly-generated database name.
    pub database: String,
}

impl TestDb {
    /// Create a new temporary database with a random name.
    pub async fn create(addr: &str, user: &str, password: &str) -> Result<Self, TypedError> {
        let database = format!(
            "resolute_test_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );

        // Connect to maintenance DB to create the test database.
        let maint = Client::connect(addr, user, password, "postgres").await?;
        maint
            .simple_query(&format!(
                "CREATE DATABASE \"{}\"",
                database.replace('"', "\"\"")
            ))
            .await?;

        tracing::info!(database = %database, "test database created");

        Ok(Self {
            addr: addr.to_string(),
            user: user.to_string(),
            password: password.to_string(),
            database,
        })
    }

    /// Create a new temporary database and run migrations.
    pub async fn create_with_migrations(
        addr: &str,
        user: &str,
        password: &str,
        migrations_dir: &str,
    ) -> Result<Self, TypedError> {
        let db = Self::create(addr, user, password).await?;
        let url = format!(
            "postgres://{}:{}@{}/{}",
            db.user, db.password, db.addr, db.database
        );
        crate::migrate::run(&url, migrations_dir).await?;
        Ok(db)
    }

    /// Get a client connected to the test database.
    pub async fn client(&self) -> Result<Client, TypedError> {
        Client::connect(&self.addr, &self.user, &self.password, &self.database).await
    }

    /// Drop the test database. Call this in test cleanup.
    pub async fn drop_db(&self) -> Result<(), TypedError> {
        let maint = Client::connect(&self.addr, &self.user, &self.password, "postgres").await?;
        // Terminate other sessions first.
        let _ = maint
            .simple_query(&format!(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
                 WHERE datname = '{}' AND pid != pg_backend_pid()",
                self.database.replace('\'', "''")
            ))
            .await;
        maint
            .simple_query(&format!(
                "DROP DATABASE IF EXISTS \"{}\"",
                self.database.replace('"', "\"\"")
            ))
            .await?;
        tracing::info!(database = %self.database, "test database dropped");
        Ok(())
    }
}
