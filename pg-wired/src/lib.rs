//! Minimal async PostgreSQL wire protocol v3 client.
//!
//! `pg-wired` is the lowest-level crate in the resolute workspace. It speaks
//! the PostgreSQL frontend/backend protocol directly over a [`tokio`] socket,
//! with no SQL parsing, no type system, and no high-level query API. If you
//! want a typed, derive-driven query layer, use the [`resolute`] crate instead;
//! this crate is the foundation it builds on.
//!
//! # Protocol features
//!
//! - **Startup, authentication.** Cleartext, MD5, and SCRAM-SHA-256 (including
//!   channel binding when the connection is TLS-wrapped).
//! - **Extended query protocol.** `Parse` / `Bind` / `Describe` / `Execute` /
//!   `Sync` with statement caching at the connection layer.
//! - **Pipelining.** Multiple round trips fused into a single write via
//!   [`PgPipeline`], with FIFO response matching.
//! - **Streaming.** Backpressure-aware row streaming for large result sets.
//! - **`COPY` in / out.** Both CSV and binary formats.
//! - **`LISTEN` / `NOTIFY`.** Bounded notification channel with a dropped-
//!   notification counter.
//! - **Cancellation.** Out-of-band cancel via [`CancelToken`] using the
//!   server-issued backend key.
//! - **TLS.** Optional, gated behind the `tls` feature; uses `rustls`.
//!
//! # Where to start
//!
//! - [`AsyncConn`] is the main entry point: a single PostgreSQL connection
//!   wrapped around the protocol state machine.
//! - [`AsyncPool`] is a thin pool over [`AsyncConn`].
//! - [`PgPipeline`] batches multiple operations into one round trip.
//! - [`tls::TlsMode`] selects between plaintext and TLS connections.
//!
//! # Stability
//!
//! The public surface is intended to be stable, but `pg-wired` is primarily
//! consumed by `resolute` and its sibling crates. Most application code should
//! prefer the typed [`resolute`] surface.
//!
//! [`resolute`]: https://docs.rs/resolute
//! [`tokio`]: https://docs.rs/tokio

#![deny(missing_docs)]

pub mod async_conn;
pub mod async_pool;
pub mod cancel;
pub mod connection;
pub mod error;
pub mod pipeline;
#[doc(hidden)]
pub mod protocol;
mod scram;
pub mod tls;

pub use async_conn::{AsyncConn, PipelineResponse, ResponseCollector};
pub use async_pool::AsyncPool;
pub use cancel::CancelToken;
pub use connection::WireConn;
pub use error::PgWireError;
pub use pipeline::PgPipeline;
pub use protocol::types::{FormatCode, Oid, PgError};
#[cfg(feature = "tls")]
pub use tls::TlsConfig;
pub use tls::TlsMode;
