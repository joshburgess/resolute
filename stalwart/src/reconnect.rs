//! Auto-reconnecting client wrapper.
//!
//! Detects connection loss and re-establishes transparently.
//! Uses `ArcSwap` for lock-free reads — only acquires a lock during reconnection.
//!
//! ```ignore
//! let client = ReconnectingClient::new(
//!     "127.0.0.1:5432", "user", "pass", "mydb",
//!     vec!["SET search_path TO myschema"],
//! ).await?;
//!
//! // If the connection drops, the next query auto-reconnects.
//! let rows = client.query("SELECT 1", &[]).await?;
//! ```

use std::sync::Arc;

use arc_swap::ArcSwap;
use tokio::sync::Mutex;

use crate::encode::SqlParam;
use crate::error::TypedError;
use crate::query::Client;
use crate::row::Row;

/// A client that auto-reconnects on connection failure.
///
/// Reads are lock-free via `ArcSwap`. Reconnection acquires a `Mutex`
/// to prevent thundering herd (only one task reconnects, others wait).
///
/// # Examples
///
/// ```ignore
/// let client = ReconnectingClient::new(
///     "127.0.0.1:5432", "app_user", "secret", "mydb",
///     vec!["SET search_path TO app".into()],
/// ).await?;
///
/// // Queries automatically reconnect if the connection drops:
/// let rows = client.query("SELECT * FROM users LIMIT 10", &[]).await?;
///
/// // Check connection health:
/// assert!(client.is_alive());
/// ```
pub struct ReconnectingClient {
    client: ArcSwap<Client>,
    reconnect_lock: Mutex<()>,
    addr: String,
    user: String,
    password: String,
    database: String,
    init_sql: Vec<String>,
}

impl ReconnectingClient {
    /// Create a new auto-reconnecting client.
    pub async fn new(
        addr: &str,
        user: &str,
        password: &str,
        database: &str,
        init_sql: Vec<String>,
    ) -> Result<Self, TypedError> {
        let client = Self::connect_inner(addr, user, password, database, &init_sql).await?;
        Ok(Self {
            client: ArcSwap::from_pointee(client),
            reconnect_lock: Mutex::new(()),
            addr: addr.to_string(),
            user: user.to_string(),
            password: password.to_string(),
            database: database.to_string(),
            init_sql,
        })
    }

    async fn connect_inner(
        addr: &str,
        user: &str,
        password: &str,
        database: &str,
        init_sql: &[String],
    ) -> Result<Client, TypedError> {
        let init_refs: Vec<&str> = init_sql.iter().map(|s| s.as_str()).collect();
        Client::connect_with_init(addr, user, password, database, &init_refs).await
    }

    /// Reconnect, acquiring the lock to prevent thundering herd.
    async fn reconnect(&self) -> Result<(), TypedError> {
        let _guard = self.reconnect_lock.lock().await;
        // Double-check: another task may have reconnected while we waited.
        if self.client.load().is_alive() {
            return Ok(());
        }
        tracing::info!(addr = %self.addr, database = %self.database, "reconnecting");
        let new_client =
            Self::connect_inner(&self.addr, &self.user, &self.password, &self.database, &self.init_sql)
                .await?;
        self.client.store(Arc::new(new_client));
        Ok(())
    }

    /// Execute a query, auto-reconnecting on connection failure.
    pub async fn query(
        &self,
        sql: &str,
        params: &[&dyn SqlParam],
    ) -> Result<Vec<Row>, TypedError> {
        let client = self.client.load();
        match client.query(sql, params).await {
            Ok(rows) => Ok(rows),
            Err(e) if is_connection_error(&e) => {
                tracing::warn!(error = %e, "connection lost, reconnecting");
                self.reconnect().await?;
                self.client.load().query(sql, params).await
            }
            Err(e) => Err(e),
        }
    }

    /// Execute a statement, auto-reconnecting on connection failure.
    pub async fn execute(
        &self,
        sql: &str,
        params: &[&dyn SqlParam],
    ) -> Result<u64, TypedError> {
        let client = self.client.load();
        match client.execute(sql, params).await {
            Ok(n) => Ok(n),
            Err(e) if is_connection_error(&e) => {
                tracing::warn!(error = %e, "connection lost, reconnecting");
                self.reconnect().await?;
                self.client.load().execute(sql, params).await
            }
            Err(e) => Err(e),
        }
    }

    /// Get the current underlying client (lock-free).
    pub fn client(&self) -> arc_swap::Guard<Arc<Client>> {
        self.client.load()
    }

    /// Check if the current connection is alive.
    pub fn is_alive(&self) -> bool {
        self.client.load().is_alive()
    }
}

fn is_connection_error(e: &TypedError) -> bool {
    match e {
        TypedError::Wire(wire_err) => matches!(
            wire_err.as_ref(),
            pg_wire::PgWireError::Io(_) | pg_wire::PgWireError::ConnectionClosed
        ),
        _ => false,
    }
}
