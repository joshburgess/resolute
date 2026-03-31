//! Automatic retry for transient database errors.
//!
//! ```ignore
//! use stalwart::retry::RetryPolicy;
//!
//! let policy = RetryPolicy::new(3, Duration::from_millis(100));
//!
//! let rows = policy.execute(&client, |db| Box::pin(async move {
//!     db.query("SELECT * FROM users WHERE id = $1", &[&1i32]).await
//! })).await?;
//! ```

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use crate::error::TypedError;
use crate::executor::Executor;

/// Configurable retry policy for transient database errors.
///
/// # Examples
///
/// ```ignore
/// use std::time::Duration;
/// use stalwart::retry::RetryPolicy;
///
/// // Retry up to 3 times with exponential backoff starting at 100ms:
/// let policy = RetryPolicy::new(3, Duration::from_millis(100))
///     .with_max_backoff(Duration::from_secs(5));
///
/// let rows = policy.execute(&client, |db| Box::pin(async move {
///     db.query("SELECT * FROM orders WHERE status = $1", &[&"pending"]).await
/// })).await?;
/// ```
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    /// Maximum number of retry attempts (not counting the initial attempt).
    pub max_retries: u32,
    /// Initial delay between retries (doubles each attempt).
    pub initial_backoff: Duration,
    /// Maximum delay between retries.
    pub max_backoff: Duration,
}

impl RetryPolicy {
    /// Create a new retry policy.
    pub fn new(max_retries: u32, initial_backoff: Duration) -> Self {
        Self {
            max_retries,
            initial_backoff,
            max_backoff: Duration::from_secs(30),
        }
    }

    /// Set the maximum backoff duration.
    pub fn with_max_backoff(mut self, max: Duration) -> Self {
        self.max_backoff = max;
        self
    }

    /// Execute an operation with retries on transient errors.
    pub async fn execute<'a, T, E: Executor + ?Sized>(
        &self,
        db: &'a E,
        f: impl Fn(&'a E) -> Pin<Box<dyn Future<Output = Result<T, TypedError>> + Send + 'a>>,
    ) -> Result<T, TypedError> {
        let mut last_err = None;
        let mut backoff = self.initial_backoff;

        for attempt in 0..=self.max_retries {
            if attempt > 0 {
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(self.max_backoff);
            }

            match f(db).await {
                Ok(val) => return Ok(val),
                Err(e) => {
                    if is_transient(&e) && attempt < self.max_retries {
                        tracing::warn!(
                            "Transient error on attempt {}/{}: {}",
                            attempt + 1,
                            self.max_retries + 1,
                            e,
                        );
                        last_err = Some(e);
                    } else {
                        return Err(e);
                    }
                }
            }
        }

        Err(last_err.unwrap())
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(30),
        }
    }
}

/// Check if a TypedError is transient (safe to retry).
fn is_transient(err: &TypedError) -> bool {
    match err {
        TypedError::Wire(wire_err) => match wire_err.as_ref() {
            // I/O errors are transient (connection reset, broken pipe).
            pg_wired::PgWireError::Io(_) => true,
            pg_wired::PgWireError::ConnectionClosed => true,
            // PG errors: check the error code.
            pg_wired::PgWireError::Pg(pg_err) => is_transient_pg_code(&pg_err.code),
            _ => false,
        },
        _ => false,
    }
}

/// PostgreSQL error codes that are safe to retry.
fn is_transient_pg_code(code: &str) -> bool {
    matches!(
        code,
        // Class 08 — Connection Exception
        "08000" | "08001" | "08003" | "08004" | "08006" |
        // Class 40 — Transaction Rollback
        "40001" | // serialization_failure
        "40P01" | // deadlock_detected
        // Class 57 — Operator Intervention
        "57P01" | // admin_shutdown
        "57P02" | // crash_shutdown
        "57P03"   // cannot_connect_now
    )
}
