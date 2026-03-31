//! Pool of AsyncConns for spreading load across multiple PostgreSQL backends.
//!
//! Each AsyncConn maintains its own TCP connection, writer task, and reader task.
//! The pool dispatches requests round-robin across connections using an atomic counter.
//! Dead connections are detected and replaced transparently.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use tokio::sync::RwLock;

use crate::async_conn::AsyncConn;
use crate::connection::WireConn;
use crate::error::PgWireError;

/// Connection configuration for reconnection.
#[derive(Clone)]
pub struct ConnConfig {
    pub addr: String,
    pub user: String,
    pub password: String,
    pub database: String,
}

/// A pool of N AsyncConns for parallel PostgreSQL backend utilization.
/// Detects dead connections and replaces them automatically.
pub struct AsyncPool {
    conns: Vec<RwLock<Arc<AsyncConn>>>,
    config: ConnConfig,
    counter: AtomicUsize,
}

impl AsyncPool {
    /// Create a pool of `size` AsyncConns, each with its own TCP connection.
    pub async fn connect(
        addr: &str,
        user: &str,
        password: &str,
        database: &str,
        size: usize,
    ) -> Result<Arc<Self>, PgWireError> {
        if size == 0 {
            return Err(PgWireError::Protocol("pool size must be >= 1".into()));
        }
        let config = ConnConfig {
            addr: addr.to_string(),
            user: user.to_string(),
            password: password.to_string(),
            database: database.to_string(),
        };

        let mut conns = Vec::with_capacity(size);
        for _ in 0..size {
            let wire = WireConn::connect(addr, user, password, database).await?;
            conns.push(RwLock::new(Arc::new(AsyncConn::new(wire))));
        }

        let pool = Arc::new(Self {
            conns,
            config,
            counter: AtomicUsize::new(0),
        });

        // Spawn background health monitor. Uses a Weak reference so
        // the monitor stops when the pool is dropped.
        {
            let pool_weak = Arc::downgrade(&pool);
            tokio::spawn(async move {
                health_monitor(pool_weak).await;
            });
        }

        Ok(pool)
    }

    /// Get the next alive AsyncConn via round-robin.
    pub async fn get_async(&self) -> Arc<AsyncConn> {
        let len = self.conns.len();
        let start = self.counter.fetch_add(1, Ordering::Relaxed) % len;

        for i in 0..len {
            let idx = (start + i) % len;
            let conn = self.conns[idx].read().await;
            if conn.is_alive() {
                return Arc::clone(&conn);
            }
        }

        // All dead — return first anyway, request will fail and trigger reconnect.
        let conn = self.conns[start % len].read().await;
        Arc::clone(&conn)
    }

    /// Replace a dead connection at the given index.
    async fn reconnect(&self, idx: usize) -> Result<(), PgWireError> {
        let wire = WireConn::connect(
            &self.config.addr,
            &self.config.user,
            &self.config.password,
            &self.config.database,
        )
        .await?;
        let new_conn = Arc::new(AsyncConn::new(wire));

        let mut slot = self.conns[idx].write().await;
        *slot = new_conn;
        tracing::info!("pg-wire: reconnected slot {idx}");
        Ok(())
    }

    /// Number of connections in the pool.
    pub fn size(&self) -> usize {
        self.conns.len()
    }

    /// Number of alive connections.
    pub async fn alive_count(&self) -> usize {
        let mut count = 0;
        for slot in &self.conns {
            let conn = slot.read().await;
            if conn.is_alive() {
                count += 1;
            }
        }
        count
    }

    /// Execute a pipelined transaction on the next available connection.
    pub async fn exec_transaction(
        &self,
        setup_sql: &str,
        query_sql: &str,
        params: &[Option<&[u8]>],
        param_oids: &[u32],
    ) -> Result<Vec<Vec<Option<Vec<u8>>>>, PgWireError> {
        self.get_async()
            .await
            .exec_transaction(setup_sql, query_sql, params, param_oids)
            .await
    }

    /// Execute a parameterized query on the next available connection.
    pub async fn exec_query(
        &self,
        sql: &str,
        params: &[Option<&[u8]>],
        param_oids: &[u32],
    ) -> Result<Vec<Vec<Option<Vec<u8>>>>, PgWireError> {
        self.get_async()
            .await
            .exec_query(sql, params, param_oids)
            .await
    }
}

/// Background task that checks connection health and reconnects dead ones.
/// Stops automatically when the pool is dropped (Weak becomes invalid).
async fn health_monitor(pool_weak: std::sync::Weak<AsyncPool>) {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
    loop {
        interval.tick().await;

        let pool = match pool_weak.upgrade() {
            Some(p) => p,
            None => {
                tracing::debug!("pg-wire: health monitor stopping (pool dropped)");
                return;
            }
        };

        for idx in 0..pool.conns.len() {
            let is_dead = {
                let conn = pool.conns[idx].read().await;
                !conn.is_alive()
            };

            if is_dead {
                tracing::warn!("pg-wire: slot {idx} is dead, reconnecting...");
                match pool.reconnect(idx).await {
                    Ok(()) => {}
                    Err(e) => {
                        tracing::error!("pg-wire: reconnect slot {idx} failed: {e}");
                    }
                }
            }
        }
    }
}
