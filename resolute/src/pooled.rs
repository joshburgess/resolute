//! Pool integration: typed client backed by pg-pool's ConnPool.
//!
//! Connections are reused across checkouts — the `AsyncConn` (with its
//! reader/writer tasks) survives checkout/return cycles.

use std::sync::Arc;

use pg_pool::async_wire::AsyncPoolable;
use pg_pool::{ConnPool, ConnPoolConfig, LifecycleHooks, PoolError, PoolGuard};

use crate::encode::SqlParam;
use crate::error::TypedError;
use crate::row::Row;

/// A pool of typed database connections.
///
/// Connections are `AsyncConn` instances that persist across checkouts.
/// Each checkout returns a `PooledTypedClient` that auto-returns the
/// connection to the pool on drop.
///
/// ```no_run
/// # async fn example() -> Result<(), resolute::TypedError> {
/// use resolute::TypedPool;
/// let pool = TypedPool::connect("127.0.0.1:5432", "user", "pass", "mydb", 10).await?;
/// let client = pool.get().await?;
/// let rows = client.query("SELECT 1::int4 AS n", &[]).await?;
/// # let _ = rows;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct TypedPool {
    pool: Arc<ConnPool<AsyncPoolable>>,
}

impl TypedPool {
    /// Create a new typed pool.
    ///
    /// # Errors
    ///
    /// Returns `PoolError::Connect(PgWireError)` if the initial minimum-size
    /// connections cannot be established.
    pub async fn new(
        config: ConnPoolConfig,
        hooks: LifecycleHooks<AsyncPoolable>,
    ) -> Result<Self, PoolError<pg_wired::PgWireError>> {
        let pool = ConnPool::new(config, hooks).await?;
        Ok(Self { pool })
    }

    /// Connect with sensible defaults.
    ///
    /// # Errors
    ///
    /// Same cases as [`TypedPool::new`].
    pub async fn connect(
        addr: &str,
        user: &str,
        password: &str,
        database: &str,
        max_size: usize,
    ) -> Result<Self, PoolError<pg_wired::PgWireError>> {
        let mut config = ConnPoolConfig::default();
        config.addr = addr.to_string();
        config.user = user.to_string();
        config.password = password.to_string();
        config.database = database.to_string();
        config.max_size = max_size;
        Self::new(config, LifecycleHooks::default()).await
    }

    /// Check out a connection from the pool.
    ///
    /// The returned `PooledTypedClient` implements `Deref<Target = AsyncConn>`
    /// and can be used with all `Executor` trait methods. The connection is
    /// automatically returned to the pool when the client is dropped.
    ///
    /// # Errors
    ///
    /// Returns `TypedError::Pool` wrapping `PoolError::Timeout` if the pool is
    /// at `max_size` and no connection becomes available before the configured
    /// `checkout_timeout`, `PoolError::Connect(PgWireError)` if a new
    /// connection was needed but couldn't be established, or
    /// `PoolError::Draining` / `PoolError::Closed` if the pool is shutting
    /// down.
    pub async fn get(&self) -> Result<PooledTypedClient, TypedError> {
        tracing::debug!("pool checkout");
        crate::metrics::record_pool_checkout();
        let guard = self.pool.get().await.map_err(|e| {
            tracing::warn!(error = %e, "pool checkout failed");
            crate::metrics::record_pool_timeout();
            TypedError::from(e)
        })?;
        Ok(PooledTypedClient { guard })
    }

    /// Pool metrics.
    pub fn metrics(&self) -> pg_pool::PoolMetrics {
        self.pool.metrics()
    }

    /// Pre-populate the pool to a target number of connections.
    /// Avoids cold-start latency on the first requests.
    ///
    /// ```ignore
    /// let pool = TypedPool::connect("...", "user", "pass", "db", 10).await?;
    /// pool.warm_up(5).await;  // pre-create 5 connections
    /// ```
    pub async fn warm_up(&self, target: usize) {
        self.pool.warm_up(target).await;
    }

    /// Drain the pool — all idle connections are closed.
    pub async fn drain(&self) {
        self.pool.drain().await;
    }
}

/// A typed client checked out from the pool.
///
/// Queries go through the pooled `AsyncConn`. When this is dropped,
/// the connection is returned to the pool for reuse.
pub struct PooledTypedClient {
    guard: PoolGuard<AsyncPoolable>,
}

impl std::fmt::Debug for PooledTypedClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PooledTypedClient").finish_non_exhaustive()
    }
}

impl PooledTypedClient {
    /// Access the underlying `AsyncConn` for direct use.
    pub fn conn(&self) -> &pg_wired::AsyncConn {
        self.guard.conn()
    }

    /// Execute a query via the pooled connection.
    pub async fn query(&self, sql: &str, params: &[&dyn SqlParam]) -> Result<Vec<Row>, TypedError> {
        // Build a temporary Client-like wrapper that uses the pooled AsyncConn.
        crate::query::Client::query_on_conn(self.guard.conn(), sql, params).await
    }

    /// Execute a statement via the pooled connection.
    pub async fn execute(&self, sql: &str, params: &[&dyn SqlParam]) -> Result<u64, TypedError> {
        crate::query::Client::execute_on_conn(self.guard.conn(), sql, params).await
    }

    /// Send a simple text query.
    ///
    /// Simple-query SQL is opaque to the driver, so we conservatively flag
    /// the connection state-mutated. This forces a `DISCARD ALL` on
    /// checkout return, which is the safe default for arbitrary user SQL
    /// (could be `SET`, `LISTEN`, `BEGIN`, etc.).
    pub async fn simple_query(&self, sql: &str) -> Result<(), TypedError> {
        self.guard.conn().mark_state_mutated();
        crate::query::Client::simple_query_on_conn(self.guard.conn(), sql).await
    }

    /// Bulk-load data via COPY FROM STDIN.
    pub async fn copy_in(&self, copy_sql: &str, data: &[u8]) -> Result<u64, TypedError> {
        self.guard.conn().mark_state_mutated();
        self.guard
            .conn()
            .copy_in(copy_sql, data)
            .await
            .map_err(|e| TypedError::from(e).with_sql(copy_sql))
    }

    /// Export data via COPY TO STDOUT.
    pub async fn copy_out(&self, copy_sql: &str) -> Result<Vec<u8>, TypedError> {
        self.guard.conn().mark_state_mutated();
        self.guard
            .conn()
            .copy_out(copy_sql)
            .await
            .map_err(|e| TypedError::from(e).with_sql(copy_sql))
    }

    /// Check if the connection is alive.
    pub fn is_alive(&self) -> bool {
        self.guard.conn().is_alive()
    }

    /// Get a `CancelToken` that can be used to cancel an in-flight query on
    /// this connection from another task.
    pub fn cancel_token(&self) -> pg_wired::CancelToken {
        self.guard.conn().cancel_token()
    }

    /// Acquire a session-level advisory lock. Blocks until the lock is granted.
    ///
    /// # Errors
    ///
    /// Returns `TypedError::Wire` if the lock query fails.
    pub async fn advisory_lock(&self, key: i64) -> Result<(), TypedError> {
        // Session-level locks survive across statements and require
        // DISCARD ALL on return to release. Mark dirty so the pool
        // resets the connection.
        self.guard.conn().mark_state_mutated();
        self.execute("SELECT pg_advisory_lock($1)", &[&key]).await?;
        Ok(())
    }

    /// Try to acquire a session-level advisory lock without blocking. Returns
    /// `true` if the lock was acquired, `false` otherwise.
    ///
    /// # Errors
    ///
    /// Returns `TypedError::Wire` if the lock query fails.
    pub async fn try_advisory_lock(&self, key: i64) -> Result<bool, TypedError> {
        // Session-level lock survives across statements; flag dirty so the
        // pool releases it via DISCARD ALL on return.
        self.guard.conn().mark_state_mutated();
        let rows = self
            .query("SELECT pg_try_advisory_lock($1)", &[&key])
            .await?;
        rows[0].get::<bool>(0)
    }

    /// Release a session-level advisory lock previously acquired with
    /// [`advisory_lock`](Self::advisory_lock). Returns `true` if a lock was
    /// released, `false` if no matching lock was held.
    ///
    /// # Errors
    ///
    /// Returns `TypedError::Wire` if the unlock query fails.
    pub async fn advisory_unlock(&self, key: i64) -> Result<bool, TypedError> {
        let rows = self.query("SELECT pg_advisory_unlock($1)", &[&key]).await?;
        rows[0].get::<bool>(0)
    }

    /// Acquire a transaction-scoped advisory lock. Released automatically at
    /// transaction end.
    ///
    /// # Errors
    ///
    /// Returns `TypedError::Wire` if the lock query fails.
    pub async fn advisory_xact_lock(&self, key: i64) -> Result<(), TypedError> {
        self.execute("SELECT pg_advisory_xact_lock($1)", &[&key])
            .await?;
        Ok(())
    }

    /// Try to acquire a transaction-scoped advisory lock without blocking.
    ///
    /// # Errors
    ///
    /// Returns `TypedError::Wire` if the lock query fails.
    pub async fn try_advisory_xact_lock(&self, key: i64) -> Result<bool, TypedError> {
        let rows = self
            .query("SELECT pg_try_advisory_xact_lock($1)", &[&key])
            .await?;
        rows[0].get::<bool>(0)
    }

    /// Begin a transaction on the pooled connection.
    ///
    /// The returned `PooledTransaction` borrows from this client, so the
    /// connection is pinned to this checkout for the transaction's lifetime
    /// and released back to the pool when this `PooledTypedClient` is dropped.
    ///
    /// # Errors
    ///
    /// Returns `TypedError::Wire` if the `BEGIN` statement fails or the
    /// connection is broken.
    pub async fn begin(&self) -> Result<PooledTransaction<'_>, TypedError> {
        self.simple_query("BEGIN").await?;
        Ok(PooledTransaction {
            client: self,
            done: false,
        })
    }

    /// Begin a transaction with a specific isolation level on the pooled
    /// connection. See [`crate::IsolationLevel`] for the available levels.
    ///
    /// # Errors
    ///
    /// Same cases as [`PooledTypedClient::begin`].
    pub async fn begin_with(
        &self,
        level: crate::IsolationLevel,
    ) -> Result<PooledTransaction<'_>, TypedError> {
        let sql = format!("BEGIN ISOLATION LEVEL {}", level.as_sql());
        self.simple_query(&sql).await?;
        Ok(PooledTransaction {
            client: self,
            done: false,
        })
    }
}

/// A transaction scoped to a pooled connection checkout.
///
/// Mirrors [`crate::Transaction`] but borrows from a [`PooledTypedClient`],
/// so the pool guard is held for the transaction's lifetime. See the
/// `Transaction` docs for the drop-behavior contract: dropping without
/// calling `commit()` or `rollback()` logs a warning and relies on
/// PostgreSQL's next-statement auto-rollback rather than sending `ROLLBACK`
/// from the destructor.
pub struct PooledTransaction<'a> {
    client: &'a PooledTypedClient,
    done: bool,
}

impl<'a> std::fmt::Debug for PooledTransaction<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PooledTransaction")
            .field("done", &self.done)
            .finish_non_exhaustive()
    }
}

impl<'a> PooledTransaction<'a> {
    /// Access the underlying [`PooledTypedClient`] this transaction is running on.
    pub fn client(&self) -> &'a PooledTypedClient {
        self.client
    }

    /// Execute a query within the transaction.
    pub async fn query(&self, sql: &str, params: &[&dyn SqlParam]) -> Result<Vec<Row>, TypedError> {
        self.client.query(sql, params).await
    }

    /// Execute a statement within the transaction. Returns affected row count.
    pub async fn execute(&self, sql: &str, params: &[&dyn SqlParam]) -> Result<u64, TypedError> {
        self.client.execute(sql, params).await
    }

    /// Execute a query with named parameters within the transaction.
    pub async fn query_named(
        &self,
        sql: &str,
        params: &[(&str, &dyn SqlParam)],
    ) -> Result<Vec<Row>, TypedError> {
        let (rewritten, names) = crate::named_params::rewrite(sql);
        let ordered = crate::query::resolve_named_params(&names, params)?;
        self.client.query(&rewritten, &ordered).await
    }

    /// Execute a named-param statement within the transaction. Returns affected row count.
    pub async fn execute_named(
        &self,
        sql: &str,
        params: &[(&str, &dyn SqlParam)],
    ) -> Result<u64, TypedError> {
        let (rewritten, names) = crate::named_params::rewrite(sql);
        let ordered = crate::query::resolve_named_params(&names, params)?;
        self.client.execute(&rewritten, &ordered).await
    }

    /// Commit the transaction.
    pub async fn commit(mut self) -> Result<(), TypedError> {
        self.done = true;
        self.client.simple_query("COMMIT").await
    }

    /// Explicitly roll back the transaction.
    pub async fn rollback(mut self) -> Result<(), TypedError> {
        self.done = true;
        self.client.simple_query("ROLLBACK").await
    }
}

impl<'a> Drop for PooledTransaction<'a> {
    fn drop(&mut self) {
        if !self.done && self.client.is_alive() {
            tracing::warn!(
                "PooledTransaction dropped without commit — will auto-rollback on next use"
            );
        }
    }
}
