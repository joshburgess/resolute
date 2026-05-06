//! Shared (multiplexed) typed pool: many tasks submit concurrently to a small
//! set of connections through each `AsyncConn`'s writer task.
//!
//! Unlike [`crate::ExclusivePool`], this pool does not lease a connection
//! exclusively for the duration of a query. Instead, every checkout returns an
//! `Arc<AsyncConn>` selected round-robin from the underlying pool, and the
//! connection's writer task coalesces concurrent submissions into batched
//! writes. There is no semaphore and no waiter queue, so latency under
//! oversubscription is dominated by Postgres throughput rather than checkout
//! contention.
//!
//! When to use which pool:
//!
//! - [`ExclusivePool`] (exclusive): transactions, advisory locks, `SET LOCAL`,
//!   prepared-statement reuse keyed by name. Anything that requires session
//!   state.
//! - [`SharedPool`] (this type): standalone SELECT / DML statements where
//!   each call is self-contained. Higher concurrent throughput on small pools.
//!
//! Mixing modes on the same physical connection is not supported. Pick one
//! pool per workload.
//!
//! [`ExclusivePool`]: crate::ExclusivePool

use std::sync::Arc;

use pg_wired::protocol::types::RawRow;
use pg_wired::{AsyncConn, AsyncPool};

use crate::encode::SqlParam;
use crate::error::TypedError;
use crate::row::Row;

/// A shared (multiplexed) pool of typed database connections.
///
/// ```no_run
/// # async fn example() -> Result<(), resolute::TypedError> {
/// use resolute::SharedPool;
/// let pool = SharedPool::connect("127.0.0.1:5432", "user", "pass", "mydb", 4).await?;
/// // 16 concurrent tasks share 4 conns; the writer task batches submissions.
/// let client = pool.get().await;
/// let rows = client.query("SELECT 1::int4", &[]).await?;
/// # let _ = rows;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct SharedPool {
    pool: Arc<AsyncPool>,
}

impl SharedPool {
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
    pub async fn get(&self) -> SharedClient {
        let conn = self.pool.get_async().await;
        SharedClient { conn }
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

    /// Execute a setup script + parameterized query as a single pipelined
    /// batch on a round-robin connection.
    ///
    /// `setup_sql` is sent as a simple-query (so it may contain multiple
    /// statements separated by `;`, e.g. `BEGIN; SET LOCAL ROLE foo`).
    /// `query_sql` is sent through the extended protocol with statement
    /// caching, text-format parameters, and text-format results. The whole
    /// batch (setup + query + `COMMIT`) is wrapped in one transaction on
    /// the server and ships in a single coalesced write.
    ///
    /// Many concurrent calls to this method share each connection's writer
    /// task: the writer fuses pending requests into one `write()` syscall
    /// and the reader FIFO-matches responses. Throughput is bounded by
    /// PostgreSQL throughput, not by pool size or checkout contention.
    ///
    /// Returns raw rows in text format. The caller decodes cells via
    /// [`RawRow::cell`]. This intentionally skips per-call Describe to keep
    /// the batch small; if you need typed `Row` decoding with column metadata,
    /// use [`SharedClient::query`] instead.
    pub async fn exec_transaction(
        &self,
        setup_sql: &str,
        query_sql: &str,
        params: &[Option<&[u8]>],
        param_oids: &[u32],
    ) -> Result<Vec<RawRow>, TypedError> {
        self.pool
            .exec_transaction(setup_sql, query_sql, params, param_oids)
            .await
            .map_err(|e| TypedError::from(e).with_sql(query_sql))
    }

    /// Execute a single parameterized query on a round-robin connection.
    /// Like [`SharedPool::exec_transaction`] but without a setup script or
    /// surrounding transaction. Returns raw rows in text format.
    pub async fn exec_query(
        &self,
        sql: &str,
        params: &[Option<&[u8]>],
        param_oids: &[u32],
    ) -> Result<Vec<RawRow>, TypedError> {
        self.pool
            .exec_query(sql, params, param_oids)
            .await
            .map_err(|e| TypedError::from(e).with_sql(sql))
    }
}

/// A typed client over a shared `AsyncConn`. Cheap to clone (one Arc bump).
#[derive(Debug, Clone)]
pub struct SharedClient {
    conn: Arc<AsyncConn>,
}

impl SharedClient {
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
