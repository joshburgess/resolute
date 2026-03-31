//! Query cancellation via CancelRequest.
//!
//! PostgreSQL cancellation works by opening a NEW TCP connection to the server
//! and sending a 16-byte CancelRequest message containing the backend PID and
//! secret key from the original connection's BackendKeyData.

use bytes::{BufMut, BytesMut};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

use crate::error::PgWireError;

/// A token that can cancel a running query on a specific backend.
/// Cloneable and Send — can be passed to another task or stored for timeout handling.
#[derive(Debug, Clone)]
pub struct CancelToken {
    pub addr: String,
    pub pid: i32,
    pub secret: i32,
}

impl CancelToken {
    /// Send a cancel request to PostgreSQL.
    ///
    /// Opens a new TCP connection, sends the 16-byte CancelRequest, and closes.
    /// The server will attempt to cancel the current query on the target backend.
    ///
    /// Note: cancellation is best-effort. The query may complete before the cancel
    /// arrives, or the server may not be able to cancel it immediately.
    pub async fn cancel(&self) -> Result<(), PgWireError> {
        let mut stream = TcpStream::connect(&self.addr).await?;

        // CancelRequest: no message type tag.
        // Length(i32) = 16, RequestCode(i32) = 80877102, PID(i32), Secret(i32)
        let mut buf = BytesMut::with_capacity(16);
        buf.put_i32(16);          // total message length
        buf.put_i32(80877102);    // cancel request code (1234 << 16 | 5678)
        buf.put_i32(self.pid);
        buf.put_i32(self.secret);

        stream.write_all(&buf).await?;
        stream.shutdown().await?;
        Ok(())
    }
}
