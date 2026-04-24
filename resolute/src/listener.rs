//! PostgreSQL LISTEN/NOTIFY support.
//!
//! Provides a typed notification listener that uses a dedicated connection.
//! When the connection drops, the listener transparently reconnects with
//! exponential backoff (50ms to 30s with full jitter), re-issues every
//! LISTEN it had active, and surfaces a `ListenerEvent::Reconnected` so
//! callers can react (reconcile state, backfill missed events, etc.).

use std::time::Duration;

use pg_wired::protocol::types::BackendMsg;
use pg_wired::{PgPipeline, PgWireError, WireConn};
use rand::Rng;

use crate::error::TypedError;

/// A notification received from PostgreSQL LISTEN/NOTIFY.
#[derive(Debug, Clone)]
pub struct Notification {
    /// PID of the notifying backend.
    pub pid: i32,
    /// Channel name.
    pub channel: String,
    /// Payload string.
    pub payload: String,
}

/// An event yielded from [`PgListener::recv_event`].
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum ListenerEvent {
    /// A notification arrived on a subscribed channel.
    Notification(Notification),
    /// The underlying connection dropped and was re-established. All
    /// previously-active `LISTEN` subscriptions have been re-issued before
    /// this event is delivered. Notifications published during the outage
    /// are lost (PostgreSQL does not buffer them); the caller should
    /// reconcile any state that depended on them.
    Reconnected,
}

/// A LISTEN/NOTIFY listener on a dedicated connection.
///
/// Automatically reconnects with exponential backoff when the connection
/// drops. Use [`recv`](Self::recv) for the simple case (notifications only)
/// or [`recv_event`](Self::recv_event) if you need to observe reconnects.
pub struct PgListener {
    pipeline: PgPipeline,
    channels: Vec<String>,
    addr: String,
    user: String,
    password: String,
    database: String,
}

impl PgListener {
    /// Connect and create a listener.
    pub async fn connect(
        addr: &str,
        user: &str,
        password: &str,
        database: &str,
    ) -> Result<Self, TypedError> {
        let conn = WireConn::connect(addr, user, password, database).await?;
        Ok(Self {
            pipeline: PgPipeline::new(conn),
            channels: Vec::new(),
            addr: addr.to_string(),
            user: user.to_string(),
            password: password.to_string(),
            database: database.to_string(),
        })
    }

    /// Subscribe to a channel.
    pub async fn listen(&mut self, channel: &str) -> Result<(), TypedError> {
        let quoted = quote_ident(channel);
        self.pipeline
            .simple_query(&format!("LISTEN {quoted}"))
            .await?;
        if !self.channels.iter().any(|c| c == channel) {
            self.channels.push(channel.to_string());
        }
        Ok(())
    }

    /// Unsubscribe from a channel.
    pub async fn unlisten(&mut self, channel: &str) -> Result<(), TypedError> {
        let quoted = quote_ident(channel);
        self.pipeline
            .simple_query(&format!("UNLISTEN {quoted}"))
            .await?;
        self.channels.retain(|c| c != channel);
        Ok(())
    }

    /// Unsubscribe from all channels.
    pub async fn unlisten_all(&mut self) -> Result<(), TypedError> {
        self.pipeline.simple_query("UNLISTEN *").await?;
        self.channels.clear();
        Ok(())
    }

    /// Wait for the next notification. Transparently reconnects on connection
    /// errors, swallowing `Reconnected` events. Use
    /// [`recv_event`](Self::recv_event) to observe them.
    pub async fn recv(&mut self) -> Result<Notification, TypedError> {
        loop {
            match self.recv_event().await? {
                ListenerEvent::Notification(n) => return Ok(n),
                ListenerEvent::Reconnected => continue,
            }
        }
    }

    /// Wait for the next listener event. On a dropped connection this
    /// reconnects (with backoff) and re-issues every active `LISTEN` before
    /// returning `ListenerEvent::Reconnected`.
    pub async fn recv_event(&mut self) -> Result<ListenerEvent, TypedError> {
        loop {
            match self.pipeline.conn().recv_msg().await {
                Ok(BackendMsg::NotificationResponse {
                    pid,
                    channel,
                    payload,
                }) => {
                    return Ok(ListenerEvent::Notification(Notification {
                        pid,
                        channel,
                        payload,
                    }));
                }
                Ok(_) => {
                    // Skip ParameterStatus, NoticeResponse, etc.
                }
                Err(e) if is_disconnect(&e) => {
                    tracing::warn!(error = %e, "listener connection dropped; reconnecting");
                    self.reconnect_with_backoff().await;
                    return Ok(ListenerEvent::Reconnected);
                }
                Err(e) => return Err(e.into()),
            }
        }
    }

    /// The channels this listener is subscribed to.
    pub fn channels(&self) -> &[String] {
        &self.channels
    }

    /// Backend process ID the listener is currently connected as. Changes
    /// across a reconnect; use this to target the backend from another
    /// session (e.g., `pg_terminate_backend` in tests).
    pub fn backend_pid(&self) -> i32 {
        self.pipeline.conn_ref().pid()
    }

    /// Reconnect with unbounded exponential backoff (50ms to 30s, full jitter)
    /// and re-issue every active LISTEN before returning. If any re-LISTEN
    /// fails, drops the connection and retries the whole dance.
    async fn reconnect_with_backoff(&mut self) {
        const INITIAL_MS: u64 = 50;
        const MAX_MS: u64 = 30_000;

        let mut delay_ms: u64 = INITIAL_MS;
        loop {
            let sleep_ms = jitter(delay_ms);
            tokio::time::sleep(Duration::from_millis(sleep_ms)).await;

            match WireConn::connect(&self.addr, &self.user, &self.password, &self.database).await {
                Ok(conn) => {
                    let mut pipeline = PgPipeline::new(conn);
                    let mut all_relistened = true;
                    for channel in &self.channels {
                        let quoted = quote_ident(channel);
                        if let Err(e) = pipeline.simple_query(&format!("LISTEN {quoted}")).await {
                            tracing::warn!(
                                channel = %channel,
                                error = %e,
                                "re-LISTEN failed after reconnect; retrying full reconnect",
                            );
                            all_relistened = false;
                            break;
                        }
                    }
                    if all_relistened {
                        self.pipeline = pipeline;
                        return;
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        delay_ms = sleep_ms,
                        "listener reconnect failed; backing off",
                    );
                }
            }
            delay_ms = delay_ms.saturating_mul(2).min(MAX_MS);
        }
    }
}

fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

fn jitter(max_ms: u64) -> u64 {
    if max_ms == 0 {
        return 0;
    }
    let mut rng = rand::rng();
    rng.random_range(0..=max_ms)
}

fn is_disconnect(e: &PgWireError) -> bool {
    match e {
        PgWireError::ConnectionClosed => true,
        PgWireError::Io(io) => matches!(
            io.kind(),
            std::io::ErrorKind::UnexpectedEof
                | std::io::ErrorKind::BrokenPipe
                | std::io::ErrorKind::ConnectionReset
                | std::io::ErrorKind::ConnectionAborted
                | std::io::ErrorKind::NotConnected
                | std::io::ErrorKind::TimedOut
        ),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quote_ident_doubles_embedded_quotes() {
        assert_eq!(quote_ident("chan"), "\"chan\"");
        assert_eq!(quote_ident("a\"b"), "\"a\"\"b\"");
    }

    #[test]
    fn jitter_stays_in_bounds() {
        for ceiling in [1u64, 10, 50, 30_000] {
            for _ in 0..32 {
                let v = jitter(ceiling);
                assert!(v <= ceiling, "jitter({ceiling}) produced {v}");
            }
        }
    }

    #[test]
    fn jitter_of_zero_is_zero() {
        assert_eq!(jitter(0), 0);
    }
}
