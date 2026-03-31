//! PostgreSQL LISTEN/NOTIFY support.
//!
//! Provides a typed notification listener that uses a dedicated connection.

use pg_wire::protocol::types::BackendMsg;
use pg_wire::{PgPipeline, WireConn};

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

/// A LISTEN/NOTIFY listener on a dedicated connection.
pub struct PgListener {
    pipeline: PgPipeline,
    channels: Vec<String>,
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
        })
    }

    /// Subscribe to a channel.
    pub async fn listen(&mut self, channel: &str) -> Result<(), TypedError> {
        let quoted = format!("\"{}\"", channel.replace('"', "\"\""));
        self.pipeline
            .simple_query(&format!("LISTEN {quoted}"))
            .await?;
        self.channels.push(channel.to_string());
        Ok(())
    }

    /// Unsubscribe from a channel.
    pub async fn unlisten(&mut self, channel: &str) -> Result<(), TypedError> {
        let quoted = format!("\"{}\"", channel.replace('"', "\"\""));
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

    /// Wait for the next notification. Blocks until one arrives.
    pub async fn recv(&mut self) -> Result<Notification, TypedError> {
        loop {
            let msg = self.pipeline.conn().recv_msg().await?;
            if let BackendMsg::NotificationResponse {
                pid,
                channel,
                payload,
            } = msg
            {
                return Ok(Notification {
                    pid,
                    channel,
                    payload,
                });
            }
            // Skip other async messages (ParameterStatus, NoticeResponse, etc.)
        }
    }

    /// The channels this listener is subscribed to.
    pub fn channels(&self) -> &[String] {
        &self.channels
    }
}
