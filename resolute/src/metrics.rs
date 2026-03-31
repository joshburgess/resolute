//! Optional Prometheus metrics for resolute.
//!
//! Enable with the `metrics` feature flag. Tracks query durations,
//! pool utilization, and error counts.
//!
//! ```ignore
//! // Register metrics once at startup:
//! resolute::metrics::register();
//!
//! // Expose via your HTTP server's /metrics endpoint:
//! let output = resolute::metrics::gather();
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// Global query metrics (lock-free atomics).
static QUERY_COUNT: AtomicU64 = AtomicU64::new(0);
static QUERY_ERROR_COUNT: AtomicU64 = AtomicU64::new(0);
static QUERY_DURATION_US_SUM: AtomicU64 = AtomicU64::new(0);
static EXECUTE_COUNT: AtomicU64 = AtomicU64::new(0);
static EXECUTE_ERROR_COUNT: AtomicU64 = AtomicU64::new(0);
static POOL_CHECKOUT_COUNT: AtomicU64 = AtomicU64::new(0);
static POOL_TIMEOUT_COUNT: AtomicU64 = AtomicU64::new(0);

/// Record a successful query with its duration.
pub fn record_query(duration_us: u64) {
    QUERY_COUNT.fetch_add(1, Ordering::Relaxed);
    QUERY_DURATION_US_SUM.fetch_add(duration_us, Ordering::Relaxed);
}

/// Record a failed query.
pub fn record_query_error() {
    QUERY_COUNT.fetch_add(1, Ordering::Relaxed);
    QUERY_ERROR_COUNT.fetch_add(1, Ordering::Relaxed);
}

/// Record a successful execute.
pub fn record_execute() {
    EXECUTE_COUNT.fetch_add(1, Ordering::Relaxed);
}

/// Record a failed execute.
pub fn record_execute_error() {
    EXECUTE_COUNT.fetch_add(1, Ordering::Relaxed);
    EXECUTE_ERROR_COUNT.fetch_add(1, Ordering::Relaxed);
}

/// Record a pool checkout.
pub fn record_pool_checkout() {
    POOL_CHECKOUT_COUNT.fetch_add(1, Ordering::Relaxed);
}

/// Record a pool checkout timeout.
pub fn record_pool_timeout() {
    POOL_TIMEOUT_COUNT.fetch_add(1, Ordering::Relaxed);
}

/// Snapshot of all metrics.
#[derive(Debug, Clone)]
pub struct QueryMetrics {
    pub query_count: u64,
    pub query_error_count: u64,
    pub query_duration_us_sum: u64,
    pub execute_count: u64,
    pub execute_error_count: u64,
    pub pool_checkout_count: u64,
    pub pool_timeout_count: u64,
}

/// Get a snapshot of all metrics.
///
/// # Examples
///
/// ```ignore
/// let metrics = resolute::metrics::snapshot();
/// println!("total queries: {}, errors: {}", metrics.query_count, metrics.query_error_count);
/// ```
pub fn snapshot() -> QueryMetrics {
    QueryMetrics {
        query_count: QUERY_COUNT.load(Ordering::Relaxed),
        query_error_count: QUERY_ERROR_COUNT.load(Ordering::Relaxed),
        query_duration_us_sum: QUERY_DURATION_US_SUM.load(Ordering::Relaxed),
        execute_count: EXECUTE_COUNT.load(Ordering::Relaxed),
        execute_error_count: EXECUTE_ERROR_COUNT.load(Ordering::Relaxed),
        pool_checkout_count: POOL_CHECKOUT_COUNT.load(Ordering::Relaxed),
        pool_timeout_count: POOL_TIMEOUT_COUNT.load(Ordering::Relaxed),
    }
}

/// Render metrics in Prometheus exposition format.
///
/// # Examples
///
/// ```ignore
/// // Expose on an Axum /metrics endpoint:
/// async fn metrics_handler() -> String {
///     resolute::metrics::gather()
/// }
/// ```
pub fn gather() -> String {
    let m = snapshot();
    let avg_us = if m.query_count > 0 {
        m.query_duration_us_sum / m.query_count
    } else {
        0
    };
    format!(
        "# HELP resolute_queries_total Total queries executed.\n\
         # TYPE resolute_queries_total counter\n\
         resolute_queries_total {}\n\
         # HELP resolute_query_errors_total Total query errors.\n\
         # TYPE resolute_query_errors_total counter\n\
         resolute_query_errors_total {}\n\
         # HELP resolute_query_duration_us_avg Average query duration in microseconds.\n\
         # TYPE resolute_query_duration_us_avg gauge\n\
         resolute_query_duration_us_avg {}\n\
         # HELP resolute_executes_total Total execute statements.\n\
         # TYPE resolute_executes_total counter\n\
         resolute_executes_total {}\n\
         # HELP resolute_execute_errors_total Total execute errors.\n\
         # TYPE resolute_execute_errors_total counter\n\
         resolute_execute_errors_total {}\n\
         # HELP resolute_pool_checkouts_total Total pool checkouts.\n\
         # TYPE resolute_pool_checkouts_total counter\n\
         resolute_pool_checkouts_total {}\n\
         # HELP resolute_pool_timeouts_total Total pool checkout timeouts.\n\
         # TYPE resolute_pool_timeouts_total counter\n\
         resolute_pool_timeouts_total {}\n",
        m.query_count,
        m.query_error_count,
        avg_us,
        m.execute_count,
        m.execute_error_count,
        m.pool_checkout_count,
        m.pool_timeout_count,
    )
}

/// Helper: time a block and record.
pub fn timed_query<T>(f: impl FnOnce() -> T) -> T {
    let start = Instant::now();
    let result = f();
    let elapsed = start.elapsed().as_micros() as u64;
    record_query(elapsed);
    result
}
