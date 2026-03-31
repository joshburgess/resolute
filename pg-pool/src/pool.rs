//! Generic connection pool implementation.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, oneshot, Mutex, Notify};

// ---------------------------------------------------------------------------
// Poolable trait
// ---------------------------------------------------------------------------

/// Trait for connection types that can be managed by the pool.
pub trait Poolable: Send + 'static {
    /// The error type for connection operations.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Create a new connection to the database.
    fn connect(
        addr: &str,
        user: &str,
        password: &str,
        database: &str,
    ) -> impl std::future::Future<Output = Result<Self, Self::Error>> + Send
    where
        Self: Sized;

    /// Check if the connection has unconsumed data (is in a corrupted state).
    fn has_pending_data(&self) -> bool;

    /// Reset the connection to a clean state before returning to the pool.
    /// Implementations should send `DISCARD ALL` or equivalent to clear
    /// session state (transactions, SET variables, temp tables, prepared statements).
    /// Returns false if the reset failed and the connection should be destroyed.
    fn reset(&self) -> impl std::future::Future<Output = bool> + Send {
        async { true } // default: no-op (backward compatible)
    }
}

// ---------------------------------------------------------------------------
// Pool error
// ---------------------------------------------------------------------------

/// Errors returned by pool operations.
#[derive(Debug)]
pub enum PoolError<E: std::error::Error> {
    /// Connection creation failed.
    Connect(E),
    /// Pool is draining (shutting down).
    Draining,
    /// Checkout timed out waiting for an available connection.
    Timeout,
    /// Pool is closed.
    Closed,
    /// Pool is at maximum capacity.
    AtCapacity,
}

impl<E: std::error::Error> std::fmt::Display for PoolError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Connect(e) => write!(f, "connection error: {e}"),
            Self::Draining => write!(f, "pool is draining"),
            Self::Timeout => write!(f, "checkout timeout"),
            Self::Closed => write!(f, "pool closed"),
            Self::AtCapacity => write!(f, "pool at max capacity"),
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for PoolError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Connect(e) => Some(e),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Connection pool configuration.
#[derive(Clone, Debug)]
pub struct ConnPoolConfig {
    /// Address (host:port).
    pub addr: String,
    /// User.
    pub user: String,
    /// Password.
    pub password: String,
    /// Database.
    pub database: String,
    /// Minimum idle connections to maintain.
    pub min_idle: usize,
    /// Maximum total connections.
    pub max_size: usize,
    /// Maximum lifetime per connection (with jitter applied).
    pub max_lifetime: Duration,
    /// Jitter range for max_lifetime (± this value).
    pub max_lifetime_jitter: Duration,
    /// Timeout waiting for a connection from the pool.
    pub checkout_timeout: Duration,
    /// How often to run the maintenance task.
    pub maintenance_interval: Duration,
    /// Whether to verify connections on checkout.
    pub test_on_checkout: bool,
}

impl Default for ConnPoolConfig {
    fn default() -> Self {
        Self {
            addr: String::new(),
            user: String::new(),
            password: String::new(),
            database: String::new(),
            min_idle: 1,
            max_size: 10,
            max_lifetime: Duration::from_secs(30 * 60),
            max_lifetime_jitter: Duration::from_secs(60),
            checkout_timeout: Duration::from_secs(5),
            maintenance_interval: Duration::from_secs(10),
            test_on_checkout: true,
        }
    }
}

// ---------------------------------------------------------------------------
// Lifecycle hooks
// ---------------------------------------------------------------------------

/// Lifecycle hook callbacks. All hooks are optional.
#[derive(Default)]
pub struct LifecycleHooks {
    pub on_create: Option<Box<dyn Fn() + Send + Sync>>,
    pub on_checkout: Option<Box<dyn Fn() + Send + Sync>>,
    pub on_checkin: Option<Box<dyn Fn() + Send + Sync>>,
    pub on_destroy: Option<Box<dyn Fn() + Send + Sync>>,
}

// ---------------------------------------------------------------------------
// Pool metrics
// ---------------------------------------------------------------------------

/// Snapshot of pool metrics.
#[derive(Debug, Clone)]
pub struct PoolMetrics {
    pub total: usize,
    pub idle: usize,
    pub in_use: usize,
    pub waiters: usize,
    pub total_checkouts: u64,
    pub total_created: u64,
    pub total_destroyed: u64,
    pub total_timeouts: u64,
}

// ---------------------------------------------------------------------------
// Internal types
// ---------------------------------------------------------------------------

struct IdleConn<C> {
    conn: C,
    expires_at: Instant,
}

struct Waiter<C> {
    tx: oneshot::Sender<C>,
}

// ---------------------------------------------------------------------------
// ConnPool
// ---------------------------------------------------------------------------

/// Production-grade connection pool, generic over connection type `C`.
pub struct ConnPool<C: Poolable> {
    config: ConnPoolConfig,
    hooks: LifecycleHooks,
    idle: Mutex<VecDeque<IdleConn<C>>>,
    waiters: Mutex<VecDeque<Waiter<C>>>,
    total_count: AtomicUsize,
    in_use_count: AtomicUsize,
    total_checkouts: AtomicU64,
    total_created: AtomicU64,
    total_destroyed: AtomicU64,
    total_timeouts: AtomicU64,
    draining: AtomicBool,
    drain_complete: Notify,
    shutdown_tx: mpsc::Sender<()>,
}

impl<C: Poolable> ConnPool<C> {
    /// Create a new connection pool and spawn the maintenance task.
    pub async fn new(
        config: ConnPoolConfig,
        hooks: LifecycleHooks,
    ) -> Result<Arc<Self>, PoolError<C::Error>> {
        let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

        let pool = Arc::new(Self {
            config: config.clone(),
            hooks,
            idle: Mutex::new(VecDeque::with_capacity(config.max_size)),
            waiters: Mutex::new(VecDeque::new()),
            total_count: AtomicUsize::new(0),
            in_use_count: AtomicUsize::new(0),
            total_checkouts: AtomicU64::new(0),
            total_created: AtomicU64::new(0),
            total_destroyed: AtomicU64::new(0),
            total_timeouts: AtomicU64::new(0),
            draining: AtomicBool::new(false),
            drain_complete: Notify::new(),
            shutdown_tx,
        });

        for _ in 0..config.min_idle {
            match pool.create_connection().await {
                Ok(idle_conn) => {
                    pool.idle.lock().await.push_back(idle_conn);
                    pool.total_count.fetch_add(1, Ordering::Relaxed);
                }
                Err(e) => {
                    tracing::warn!("Failed to pre-fill connection: {e}");
                }
            }
        }

        {
            let pool_ref = Arc::clone(&pool);
            tokio::spawn(maintenance_task(pool_ref, shutdown_rx));
        }

        Ok(pool)
    }

    /// Check out a connection from the pool.
    pub async fn get(self: &Arc<Self>) -> Result<PoolGuard<C>, PoolError<C::Error>> {
        if self.draining.load(Ordering::Relaxed) {
            return Err(PoolError::Draining);
        }

        if let Some(conn) = self.try_get_idle().await {
            self.in_use_count.fetch_add(1, Ordering::Relaxed);
            self.total_checkouts.fetch_add(1, Ordering::Relaxed);
            if let Some(ref hook) = self.hooks.on_checkout {
                hook();
            }
            return Ok(PoolGuard {
                conn: Some(conn),
                pool: Arc::clone(self),
            });
        }

        if self.total_count.load(Ordering::Relaxed) < self.config.max_size {
            match self.create_and_track().await {
                Ok(conn) => {
                    self.in_use_count.fetch_add(1, Ordering::Relaxed);
                    self.total_checkouts.fetch_add(1, Ordering::Relaxed);
                    if let Some(ref hook) = self.hooks.on_checkout {
                        hook();
                    }
                    return Ok(PoolGuard {
                        conn: Some(conn),
                        pool: Arc::clone(self),
                    });
                }
                Err(e) => {
                    tracing::warn!("Failed to create new connection: {e}");
                }
            }
        }

        let (tx, rx) = oneshot::channel();
        {
            let mut waiters = self.waiters.lock().await;
            waiters.push_back(Waiter { tx });
        }

        match tokio::time::timeout(self.config.checkout_timeout, rx).await {
            Ok(Ok(conn)) => {
                self.in_use_count.fetch_add(1, Ordering::Relaxed);
                self.total_checkouts.fetch_add(1, Ordering::Relaxed);
                if let Some(ref hook) = self.hooks.on_checkout {
                    hook();
                }
                Ok(PoolGuard {
                    conn: Some(conn),
                    pool: Arc::clone(self),
                })
            }
            Ok(Err(_)) => Err(PoolError::Closed),
            Err(_) => {
                self.total_timeouts.fetch_add(1, Ordering::Relaxed);
                Err(PoolError::Timeout)
            }
        }
    }

    async fn try_get_idle(&self) -> Option<C> {
        let mut idle = self.idle.lock().await;
        while let Some(entry) = idle.pop_front() {
            if Instant::now() >= entry.expires_at {
                self.destroy_conn_stats();
                if let Some(ref hook) = self.hooks.on_destroy {
                    hook();
                }
                continue;
            }
            if self.config.test_on_checkout && entry.conn.has_pending_data() {
                self.destroy_conn_stats();
                if let Some(ref hook) = self.hooks.on_destroy {
                    hook();
                }
                continue;
            }
            return Some(entry.conn);
        }
        None
    }

    async fn create_connection(&self) -> Result<IdleConn<C>, C::Error> {
        let conn = C::connect(
            &self.config.addr,
            &self.config.user,
            &self.config.password,
            &self.config.database,
        )
        .await?;

        self.total_created.fetch_add(1, Ordering::Relaxed);
        if let Some(ref hook) = self.hooks.on_create {
            hook();
        }

        let jitter = jittered_duration(self.config.max_lifetime, self.config.max_lifetime_jitter);
        Ok(IdleConn {
            conn,
            expires_at: Instant::now() + jitter,
        })
    }

    async fn create_and_track(&self) -> Result<C, PoolError<C::Error>> {
        let prev = self.total_count.fetch_add(1, Ordering::Relaxed);
        if prev >= self.config.max_size {
            self.total_count.fetch_sub(1, Ordering::Relaxed);
            return Err(PoolError::AtCapacity);
        }

        match self.create_connection().await {
            Ok(idle_conn) => Ok(idle_conn.conn),
            Err(e) => {
                self.total_count.fetch_sub(1, Ordering::Relaxed);
                Err(PoolError::Connect(e))
            }
        }
    }

    fn return_conn(pool: Arc<Self>, conn: C) {
        tokio::spawn(async move {
            pool.return_conn_async(conn).await;
        });
    }

    async fn return_conn_async(&self, conn: C) {
        if conn.has_pending_data() {
            self.in_use_count.fetch_sub(1, Ordering::Relaxed);
            self.destroy_conn_stats();
            if let Some(ref hook) = self.hooks.on_destroy {
                hook();
            }
            self.maybe_notify_drain();
            return;
        }

        // Reset connection state (DISCARD ALL) to prevent dirty state leaking.
        if !conn.reset().await {
            self.in_use_count.fetch_sub(1, Ordering::Relaxed);
            self.destroy_conn_stats();
            if let Some(ref hook) = self.hooks.on_destroy {
                hook();
            }
            self.maybe_notify_drain();
            return;
        }

        if let Some(ref hook) = self.hooks.on_checkin {
            hook();
        }

        self.in_use_count.fetch_sub(1, Ordering::Relaxed);

        if self.draining.load(Ordering::Relaxed) {
            self.destroy_conn_stats();
            if let Some(ref hook) = self.hooks.on_destroy {
                hook();
            }
            self.maybe_notify_drain();
            return;
        }

        let mut conn = conn;
        {
            let mut waiters = self.waiters.lock().await;
            while let Some(waiter) = waiters.pop_front() {
                match waiter.tx.send(conn) {
                    Ok(()) => return,
                    Err(returned_conn) => {
                        conn = returned_conn;
                        continue;
                    }
                }
            }
        }

        let jitter = jittered_duration(self.config.max_lifetime, self.config.max_lifetime_jitter);
        let mut idle = self.idle.lock().await;
        idle.push_back(IdleConn {
            conn,
            expires_at: Instant::now() + jitter,
        });
    }

    fn maybe_notify_drain(&self) {
        if self.draining.load(Ordering::Relaxed)
            && self.total_count.load(Ordering::Relaxed) == 0
        {
            self.drain_complete.notify_one();
        }
    }

    fn destroy_conn_stats(&self) {
        self.total_count.fetch_sub(1, Ordering::Relaxed);
        self.total_destroyed.fetch_add(1, Ordering::Relaxed);
    }

    /// Get a snapshot of pool metrics.
    pub fn metrics(&self) -> PoolMetrics {
        let total = self.total_count.load(Ordering::Relaxed);
        let in_use = self.in_use_count.load(Ordering::Relaxed);
        PoolMetrics {
            total,
            idle: total.saturating_sub(in_use),
            in_use,
            waiters: 0,
            total_checkouts: self.total_checkouts.load(Ordering::Relaxed),
            total_created: self.total_created.load(Ordering::Relaxed),
            total_destroyed: self.total_destroyed.load(Ordering::Relaxed),
            total_timeouts: self.total_timeouts.load(Ordering::Relaxed),
        }
    }

    /// Pre-populate the pool to a target number of idle connections.
    /// Useful for warming up on startup to avoid first-request latency.
    pub async fn warm_up(&self, target: usize) {
        let current = self.metrics().total;
        let to_create = target.saturating_sub(current).min(self.config.max_size - current);
        let mut created = 0;
        for _ in 0..to_create {
            match self.create_connection().await {
                Ok(idle_conn) => {
                    self.idle.lock().await.push_back(idle_conn);
                    self.total_count.fetch_add(1, Ordering::Relaxed);
                    created += 1;
                }
                Err(e) => {
                    tracing::warn!("warm_up: failed to create connection: {e}");
                    break;
                }
            }
        }
        if created > 0 {
            tracing::info!(created, target, "pool warm-up complete");
        }
    }

    /// Initiate graceful drain.
    pub async fn drain(&self) {
        self.draining.store(true, Ordering::Relaxed);

        {
            let mut idle = self.idle.lock().await;
            let count = idle.len();
            idle.clear();
            self.total_count.fetch_sub(count, Ordering::Relaxed);
            self.total_destroyed.fetch_add(count as u64, Ordering::Relaxed);
            if count > 0 {
                if let Some(ref hook) = self.hooks.on_destroy {
                    for _ in 0..count {
                        hook();
                    }
                }
            }
        }

        {
            let mut waiters = self.waiters.lock().await;
            waiters.clear();
        }

        loop {
            let notified = self.drain_complete.notified();
            if self.total_count.load(Ordering::Relaxed) == 0 {
                break;
            }
            notified.await;
        }

        let _ = self.shutdown_tx.send(()).await;
        tracing::info!("Connection pool drained");
    }

    /// Current pool status string.
    pub fn status(&self) -> String {
        let m = self.metrics();
        format!(
            "pool: total={} idle={} in_use={} created={} destroyed={} timeouts={}",
            m.total, m.idle, m.in_use, m.total_created, m.total_destroyed, m.total_timeouts
        )
    }
}

// ---------------------------------------------------------------------------
// PoolGuard
// ---------------------------------------------------------------------------

/// A checked-out connection that returns itself to the pool on drop.
pub struct PoolGuard<C: Poolable> {
    conn: Option<C>,
    pool: Arc<ConnPool<C>>,
}

impl<C: Poolable> PoolGuard<C> {
    pub fn conn(&self) -> &C {
        self.conn.as_ref().unwrap()
    }

    pub fn conn_mut(&mut self) -> &mut C {
        self.conn.as_mut().unwrap()
    }

    /// Take ownership of the connection, removing it from the pool.
    pub fn take(mut self) -> C {
        let conn = self.conn.take().unwrap();
        self.pool.in_use_count.fetch_sub(1, Ordering::Relaxed);
        self.pool.total_count.fetch_sub(1, Ordering::Relaxed);
        conn
    }
}

impl<C: Poolable> Drop for PoolGuard<C> {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            ConnPool::return_conn(Arc::clone(&self.pool), conn);
        }
    }
}

impl<C: Poolable> std::ops::Deref for PoolGuard<C> {
    type Target = C;
    fn deref(&self) -> &Self::Target {
        self.conn.as_ref().unwrap()
    }
}

impl<C: Poolable> std::ops::DerefMut for PoolGuard<C> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.conn.as_mut().unwrap()
    }
}

// ---------------------------------------------------------------------------
// Maintenance task
// ---------------------------------------------------------------------------

async fn maintenance_task<C: Poolable>(pool: Arc<ConnPool<C>>, mut shutdown_rx: mpsc::Receiver<()>) {
    let mut interval = tokio::time::interval(pool.config.maintenance_interval);
    interval.tick().await;
    loop {
        tokio::select! {
            _ = interval.tick() => {}
            _ = shutdown_rx.recv() => {
                tracing::debug!("Maintenance task shutting down");
                return;
            }
        }

        if pool.draining.load(Ordering::Relaxed) {
            return;
        }

        {
            let mut idle = pool.idle.lock().await;
            let now = Instant::now();
            let before = idle.len();
            idle.retain(|entry| now < entry.expires_at);
            let evicted = before - idle.len();
            if evicted > 0 {
                pool.total_count.fetch_sub(evicted, Ordering::Relaxed);
                pool.total_destroyed.fetch_add(evicted as u64, Ordering::Relaxed);
                tracing::debug!("Evicted {evicted} expired connections");
            }
        }

        let total = pool.total_count.load(Ordering::Relaxed);
        let in_use = pool.in_use_count.load(Ordering::Relaxed);
        let current_idle = total.saturating_sub(in_use);

        if current_idle < pool.config.min_idle && total < pool.config.max_size {
            let to_create = (pool.config.min_idle - current_idle)
                .min(pool.config.max_size - total);
            for _ in 0..to_create {
                match pool.create_and_track().await {
                    Ok(conn) => {
                        let jitter = jittered_duration(
                            pool.config.max_lifetime,
                            pool.config.max_lifetime_jitter,
                        );
                        let mut idle = pool.idle.lock().await;
                        idle.push_back(IdleConn {
                            conn,
                            expires_at: Instant::now() + jitter,
                        });
                    }
                    Err(_) => break,
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn jittered_duration(base: Duration, jitter: Duration) -> Duration {
    if jitter.is_zero() {
        return base;
    }
    let jitter_ms = jitter.as_millis() as u64;
    let offset = fastrand_u64() % (jitter_ms * 2 + 1);
    let jittered = base.as_millis() as i128 + offset as i128 - jitter_ms as i128;
    Duration::from_millis(jittered.max(1) as u64)
}

fn fastrand_u64() -> u64 {
    use std::cell::Cell;
    thread_local! {
        static STATE: Cell<u64> = Cell::new(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64
        );
    }
    STATE.with(|s| {
        let mut x = s.get();
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        if x == 0 { x = 1; }
        s.set(x);
        x
    })
}
