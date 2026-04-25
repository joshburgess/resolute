//! Error types for the typed query layer.

/// Top-level error type returned by the typed query layer.
///
/// Most call sites bubble this up via `?`. Match on it to distinguish wire
/// errors (server reported a problem) from decode errors (server response
/// could not be mapped into the requested Rust type) from operational errors
/// (timeouts, pool exhaustion, I/O).
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum TypedError {
    /// The underlying `pg-wired` connection returned a protocol-level error.
    #[error("wire error: {0}")]
    Wire(#[from] Box<pg_wired::PgWireError>),

    /// A row's column value could not be decoded into the requested Rust type.
    #[error("decode error: column {column}: {message}")]
    Decode {
        /// Zero-based column index in the row.
        column: usize,
        /// Decoder-specific failure message.
        message: String,
    },

    /// `Row::get` was called with a column name that does not exist in the row.
    #[error("column not found: {0}")]
    ColumnNotFound(String),

    /// A non-`Option` decode hit a SQL `NULL` in the named column index.
    #[error("unexpected null in column {0}")]
    UnexpectedNull(usize),

    /// `fetch_one` (or similar) expected exactly one row but received a
    /// different count.
    #[error("row count mismatch: expected 1, got {0}")]
    NotExactlyOne(usize),

    /// The decoded column's type OID did not match the type the decoder expects.
    #[error("type mismatch: expected OID {expected}, got {actual}")]
    TypeMismatch {
        /// OID the decoder was registered for.
        expected: u32,
        /// OID the server actually sent.
        actual: u32,
    },

    /// The connection pool returned an error (e.g., timeout waiting for a
    /// connection, exhausted retries on `connect`).
    #[error("pool error: {0}")]
    Pool(#[from] Box<pg_pool::PoolError<pg_wired::PgWireError>>),

    /// The configured query timeout elapsed before the server responded.
    #[error("query timed out after {0:?}")]
    Timeout(std::time::Duration),

    /// A `:name` parameter referenced in SQL was not supplied to
    /// `query_named` / `execute_named`.
    #[error("missing named parameter: :{0}")]
    MissingParam(String),

    /// Local I/O error (rare; usually wrapped into `Wire`).
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Catch-all for misconfiguration (invalid connection string, missing
    /// type registration, etc.).
    #[error("{0}")]
    Config(String),

    /// A wrapper added by `with_sql` that pairs an inner error with the
    /// (possibly truncated) SQL that produced it. Useful for log output.
    #[error("query failed: {source} [SQL: {sql}]")]
    QueryFailed {
        /// The original error.
        source: Box<TypedError>,
        /// The SQL that produced the error (truncated to 200 bytes).
        sql: String,
    },
}

impl From<pg_wired::PgWireError> for TypedError {
    fn from(e: pg_wired::PgWireError) -> Self {
        Self::Wire(Box::new(e))
    }
}

impl From<pg_pool::PoolError<pg_wired::PgWireError>> for TypedError {
    fn from(e: pg_pool::PoolError<pg_wired::PgWireError>) -> Self {
        Self::Pool(Box::new(e))
    }
}

impl TypedError {
    /// Attach SQL context to an error for debugging.
    pub fn with_sql(self, sql: &str) -> Self {
        let truncated = if sql.len() > 200 {
            format!("{}...", &sql[..200])
        } else {
            sql.to_string()
        };
        TypedError::QueryFailed {
            source: Box::new(self),
            sql: truncated,
        }
    }
}
