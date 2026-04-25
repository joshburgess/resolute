//! Shared (multiplexed) typed pool: many tasks submit concurrently to a small
//! set of connections through each `AsyncConn`'s writer task.
//!
//! Unlike [`crate::TypedPool`], this pool does not lease a connection
//! exclusively for the duration of a query. Instead, every checkout returns an
//! `Arc<AsyncConn>` selected round-robin from the underlying pool, and the
//! connection's writer task coalesces concurrent submissions into batched
//! writes. There is no semaphore and no waiter queue, so latency under
//! oversubscription is dominated by Postgres throughput rather than checkout
//! contention.
//!
//! When to use which pool:
//!
//! - [`TypedPool`] (exclusive): transactions, advisory locks, `SET LOCAL`,
//!   prepared-statement reuse keyed by name. Anything that requires session
//!   state.
//! - [`SharedTypedPool`] (this type): standalone SELECT / DML statements where
//!   each call is self-contained. Higher concurrent throughput on small pools.
//!
//! Mixing modes on the same physical connection is not supported. Pick one
//! pool per workload.
//!
//! [`TypedPool`]: crate::TypedPool

use std::sync::Arc;

use pg_wired::{AsyncConn, AsyncPool};

use crate::encode::SqlParam;
use crate::error::TypedError;
use crate::row::Row;

/// A shared (multiplexed) pool of typed database connections.
///
/// ```no_run
/// # async fn example() -> Result<(), resolute::TypedError> {
/// use resolute::SharedTypedPool;
/// let pool = SharedTypedPool::connect("127.0.0.1:5432", "user", "pass", "mydb", 4).await?;
/// // 16 concurrent tasks share 4 conns; the writer task batches submissions.
/// let client = pool.get().await;
/// let rows = client.query("SELECT 1::int4", &[]).await?;
/// # let _ = rows;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct SharedTypedPool {
    pool: Arc<AsyncPool>,
}

impl SharedTypedPool {
    /// Connect with `size` underlying connections. Each connection runs its
    /// own writer/reader task and accepts concurrent submissions from any
    /// number of tasks.
    pub async fn connect(
        addr: &str,
        user: &str,
        password: &str,
        database: &str,
        size: usize,
    ) -> Result<Self, pg_wired::PgWireError> {
        let pool = AsyncPool::connect(addr, user, password, database, size).await?;
        Ok(Self { pool })
    }

    /// Pick a live connection round-robin and wrap it for typed access.
    /// Cheap: just an atomic increment plus one Arc clone.
    pub async fn get(&self) -> SharedTypedClient {
        let conn = self.pool.get_async().await;
        SharedTypedClient { conn }
    }

    /// Number of underlying connections in the pool.
    pub fn size(&self) -> usize {
        self.pool.size()
    }

    /// Number of currently alive connections.
    pub async fn alive_count(&self) -> usize {
        self.pool.alive_count().await
    }

    /// Close all connections.
    pub async fn close(&self) -> Result<(), pg_wired::PgWireError> {
        self.pool.close().await
    }
}

/// A typed client over a shared `AsyncConn`. Cheap to clone (one Arc bump).
#[derive(Debug, Clone)]
pub struct SharedTypedClient {
    conn: Arc<AsyncConn>,
}

impl SharedTypedClient {
    /// Access the underlying `AsyncConn`.
    pub fn conn(&self) -> &AsyncConn {
        &self.conn
    }

    /// Execute a query.
    pub async fn query(&self, sql: &str, params: &[&dyn SqlParam]) -> Result<Vec<Row>, TypedError> {
        crate::query::Client::query_on_conn(&self.conn, sql, params).await
    }

    /// Execute a statement.
    pub async fn execute(&self, sql: &str, params: &[&dyn SqlParam]) -> Result<u64, TypedError> {
        crate::query::Client::execute_on_conn(&self.conn, sql, params).await
    }

    /// Send a simple text query.
    pub async fn simple_query(&self, sql: &str) -> Result<(), TypedError> {
        crate::query::Client::simple_query_on_conn(&self.conn, sql).await
    }

    /// Check if the underlying connection is alive.
    pub fn is_alive(&self) -> bool {
        self.conn.is_alive()
    }
}
