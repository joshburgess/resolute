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
    /// Connection address.
    pub addr: String,
    pub user: String,
    pub password: String,
    /// The randomly-generated database name.
    pub database: String,
}

impl TestDb {
    /// Create a new temporary database with a random name.
    pub async fn create(
        addr: &str,
        user: &str,
        password: &str,
    ) -> Result<Self, TypedError> {
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
