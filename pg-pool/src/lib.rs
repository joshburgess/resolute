//! Production-grade async connection pool.
//!
//! Generic over any connection type that implements [`Poolable`].
//! Features: waiter queue with dead-waiter skipping, jittered max-life,
//! health checks on checkout, lifecycle hooks, graceful drain, and metrics.
//!
//! # Example with pg-wired (requires `wire` feature)
//!
//! ```ignore
//! use pg_pool::{ConnPool, ConnPoolConfig, LifecycleHooks};
//! use pg_pool::wire::WirePoolable;
//!
//! let mut config = ConnPoolConfig::default();
//! config.addr = "127.0.0.1:5432".into();
//! config.user = "postgres".into();
//! config.password = "postgres".into();
//! config.database = "mydb".into();
//!
//! let pool = ConnPool::<WirePoolable>::new(config, LifecycleHooks::default()).await?;
//! ```

#[cfg(feature = "wire")]
pub mod async_wire;
mod pool;
#[cfg(feature = "wire")]
pub mod wire;

pub use pool::{
    ConnPool, ConnPoolConfig, LifecycleHooks, PoolError, PoolGuard, PoolMetrics, Poolable,
};
