//! pg-wire AsyncConn integration: implements [`Poolable`] for async connections.
//!
//! Unlike `WirePoolable` which pools raw `WireConn` (requiring a new `AsyncConn`
//! per checkout), this pools `AsyncConn` directly so reader/writer tasks survive
//! across checkout/return cycles.

use crate::Poolable;

/// Poolable wrapper around [`pg_wire::AsyncConn`].
///
/// The `AsyncConn` spawns reader/writer tasks on creation and keeps them
/// running until the connection dies. Pooling `AsyncConn` directly means
/// connections are reused without re-establishing TCP or re-authenticating.
pub struct AsyncPoolable(pub pg_wire::AsyncConn);

impl Poolable for AsyncPoolable {
    type Error = pg_wire::PgWireError;

    async fn connect(
        addr: &str,
        user: &str,
        password: &str,
        database: &str,
    ) -> Result<Self, Self::Error> {
        let wire = pg_wire::WireConn::connect(addr, user, password, database).await?;
        let async_conn = pg_wire::AsyncConn::new(wire);
        Ok(AsyncPoolable(async_conn))
    }

    fn has_pending_data(&self) -> bool {
        !self.0.is_alive()
    }

    /// Reset connection state on return to pool.
    /// Sends DISCARD ALL to clear transactions, SET variables, temp tables,
    /// and prepared statements. If the reset fails, the connection is destroyed.
    async fn reset(&self) -> bool {
        if !self.0.is_alive() {
            return false;
        }
        // DISCARD ALL: resets search_path, temp tables, prepared statements,
        // advisory locks, LISTEN channels, and aborts any open transaction.
        let mut buf = bytes::BytesMut::new();
        pg_wire::protocol::frontend::encode_message(
            &pg_wire::protocol::types::FrontendMsg::Query(b"DISCARD ALL"),
            &mut buf,
        );
        match self.0.submit(buf, pg_wire::ResponseCollector::Drain).await {
            Ok(_) => {
                // DISCARD ALL destroys server-side prepared statements,
                // so clear the client-side cache to stay in sync.
                self.0.clear_statement_cache();
                true
            }
            Err(e) => {
                tracing::warn!(error = %e, "connection reset failed, destroying");
                false
            }
        }
    }
}
