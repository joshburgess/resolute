use crate::protocol::types::PgError;

#[derive(Debug, thiserror::Error)]
pub enum PgWireError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("PostgreSQL error: {}: {}", .0.code, .0.message)]
    Pg(PgError),

    #[error("Protocol error: {0}")]
    Protocol(String),

    #[error("Connection closed")]
    ConnectionClosed,
}
