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
pub struct TypedPool {
    pool: Arc<ConnPool<AsyncPoolable>>,
}

impl TypedPool {
    /// Create a new typed pool.
    pub async fn new(
        config: ConnPoolConfig,
        hooks: LifecycleHooks,
    ) -> Result<Self, PoolError<pg_wire::PgWireError>> {
        let pool = ConnPool::new(config, hooks).await?;
        Ok(Self { pool })
    }

    /// Connect with sensible defaults.
    pub async fn connect(
        addr: &str,
        user: &str,
        password: &str,
        database: &str,
        max_size: usize,
    ) -> Result<Self, PoolError<pg_wire::PgWireError>> {
        let config = ConnPoolConfig {
            addr: addr.to_string(),
            user: user.to_string(),
            password: password.to_string(),
            database: database.to_string(),
            max_size,
            ..Default::default()
        };
        Self::new(config, LifecycleHooks::default()).await
    }

    /// Check out a connection from the pool.
    ///
    /// The returned `PooledTypedClient` implements `Deref<Target = AsyncConn>`
    /// and can be used with all `Executor` trait methods. The connection is
    /// automatically returned to the pool when the client is dropped.
    pub async fn get(&self) -> Result<PooledTypedClient, TypedError> {
        tracing::debug!("pool checkout");
        crate::metrics::record_pool_checkout();
        let guard = self.pool.get().await.map_err(|e| {
            tracing::warn!(error = %e, "pool checkout failed");
            crate::metrics::record_pool_timeout();
            TypedError::Pool(format!("{e}"))
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

impl PooledTypedClient {
    /// Access the underlying `AsyncConn` for direct use.
    pub fn conn(&self) -> &pg_wire::AsyncConn {
        &self.guard.conn().0
    }

    /// Execute a query via the pooled connection.
    pub async fn query(
        &self,
        sql: &str,
        params: &[&dyn SqlParam],
    ) -> Result<Vec<Row>, TypedError> {
        // Build a temporary Client-like wrapper that uses the pooled AsyncConn.
        crate::query::Client::query_on_conn(&self.guard.conn().0, sql, params).await
    }

    /// Execute a statement via the pooled connection.
    pub async fn execute(
        &self,
        sql: &str,
        params: &[&dyn SqlParam],
    ) -> Result<u64, TypedError> {
        crate::query::Client::execute_on_conn(&self.guard.conn().0, sql, params).await
    }

    /// Send a simple text query.
    pub async fn simple_query(&self, sql: &str) -> Result<(), TypedError> {
        crate::query::Client::simple_query_on_conn(&self.guard.conn().0, sql).await
    }

    /// Bulk-load data via COPY FROM STDIN.
    pub async fn copy_in(&self, copy_sql: &str, data: &[u8]) -> Result<u64, TypedError> {
        self.guard
            .conn()
            .0
            .copy_in(copy_sql, data)
            .await
            .map_err(TypedError::from)
    }

    /// Export data via COPY TO STDOUT.
    pub async fn copy_out(&self, copy_sql: &str) -> Result<Vec<u8>, TypedError> {
        self.guard
            .conn()
            .0
            .copy_out(copy_sql)
            .await
            .map_err(TypedError::from)
    }

    /// Check if the connection is alive.
    pub fn is_alive(&self) -> bool {
        self.guard.conn().0.is_alive()
    }
}
