//! Error type for the `pg-wired` crate.

use crate::protocol::types::PgError;

/// Top-level error type returned by `pg-wired`.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum PgWireError {
    /// Local I/O failure (TCP, TLS, or read/write on the socket).
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// PostgreSQL returned an `ErrorResponse` message. Inspect the wrapped
    /// [`PgError`] for SQLSTATE code and detail fields.
    #[error("PostgreSQL error: {}: {}", .0.code, .0.message)]
    Pg(PgError),

    /// Server response could not be parsed as a valid v3 protocol message,
    /// or the message arrived out of order for the current connection state.
    #[error("Protocol error: {0}")]
    Protocol(String),

    /// The connection was closed mid-operation (peer disconnect, EOF on read,
    /// reader/writer task died).
    #[error("Connection closed")]
    ConnectionClosed,
}
