//! Error types for the typed query layer.

#[derive(Debug, thiserror::Error)]
pub enum TypedError {
    #[error("wire error: {0}")]
    Wire(#[from] Box<pg_wire::PgWireError>),

    #[error("decode error: column {column}: {message}")]
    Decode { column: usize, message: String },

    #[error("column not found: {0}")]
    ColumnNotFound(String),

    #[error("unexpected null in column {0}")]
    UnexpectedNull(usize),

    #[error("row count mismatch: expected 1, got {0}")]
    NotExactlyOne(usize),

    #[error("type mismatch: expected OID {expected}, got {actual}")]
    TypeMismatch { expected: u32, actual: u32 },

    #[error("pool error: {0}")]
    Pool(String),

    #[error("query timed out after {0:?}")]
    Timeout(std::time::Duration),

    #[error("missing named parameter: :{0}")]
    MissingParam(String),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("{0}")]
    Config(String),

    #[error("query failed: {source} [SQL: {sql}]")]
    QueryFailed {
        source: Box<TypedError>,
        sql: String,
    },
}

impl From<pg_wire::PgWireError> for TypedError {
    fn from(e: pg_wire::PgWireError) -> Self {
        Self::Wire(Box::new(e))
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
