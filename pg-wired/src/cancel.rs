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
/// Cloneable and Send, can be passed to another task or stored for timeout handling.
///
/// The backend secret is held internally and is not exposed through any
/// accessor; obtain a token from an `AsyncConn` / `WireConn` rather than
/// constructing one with raw fields.
#[derive(Debug, Clone)]
pub struct CancelToken {
    addr: String,
    pid: i32,
    secret: i32,
}

impl CancelToken {
    /// Construct a cancel token from raw parts. Intended for callers that
    /// persisted the three values across processes; most users should get a
    /// token from `AsyncConn::cancel_token()` instead.
    pub fn new(addr: String, pid: i32, secret: i32) -> Self {
        Self { addr, pid, secret }
    }

    /// Server address this token targets.
    pub fn addr(&self) -> &str {
        &self.addr
    }

    /// Backend process ID this token cancels.
    pub fn pid(&self) -> i32 {
        self.pid
    }

    /// Send a cancel request to PostgreSQL.
    ///
    /// Opens a new TCP connection, sends the 16-byte CancelRequest, and closes.
    /// The server will attempt to cancel the current query on the target backend.
    ///
    /// Note: cancellation is best-effort. The query may complete before the cancel
    /// arrives, or the server may not be able to cancel it immediately.
    ///
    /// Times out after 5 seconds to prevent hanging if the server is unreachable.
    pub async fn cancel(&self) -> Result<(), PgWireError> {
        const CANCEL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

        let result = tokio::time::timeout(CANCEL_TIMEOUT, async {
            let mut stream = TcpStream::connect(&self.addr).await?;

            // CancelRequest: no message type tag.
            // Length(i32) = 16, RequestCode(i32) = 80877102, PID(i32), Secret(i32)
            let mut buf = BytesMut::with_capacity(16);
            buf.put_i32(16); // total message length
            buf.put_i32(80877102); // cancel request code (1234 << 16 | 5678)
            buf.put_i32(self.pid);
            buf.put_i32(self.secret);

            stream.write_all(&buf).await?;
            stream.shutdown().await?;
            Ok::<(), PgWireError>(())
        })
        .await;

        match result {
            Ok(inner) => inner,
            Err(_elapsed) => Err(PgWireError::Protocol(
                "cancel request timed out after 5 seconds".into(),
            )),
        }
    }
}
