use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::error::PgWireError;
use crate::protocol::backend;
use crate::protocol::frontend;
use crate::protocol::types::{BackendMsg, FrontendMsg};
use crate::scram::ScramClient;
use crate::tls::MaybeTlsStream;

/// Raw PostgreSQL wire connection.
/// Handles TCP I/O, buffered reading, and authentication.
pub struct WireConn {
    pub(crate) stream: MaybeTlsStream,
    recv_buf: BytesMut,
    pub pid: i32,
    pub secret: i32,
}

const RECV_BUF_SIZE: usize = 32 * 1024; // 32KB recv buffer

impl WireConn {
    /// Check if the connection has unconsumed data in the receive buffer.
    pub fn has_pending_data(&self) -> bool {
        !self.recv_buf.is_empty()
    }

    /// Connect to PostgreSQL and perform authentication.
    pub async fn connect(
        addr: &str,
        user: &str,
        password: &str,
        database: &str,
    ) -> Result<Self, PgWireError> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;

        // TLS negotiation (if feature enabled).
        #[cfg(feature = "tls")]
        let stream = {
            let hostname = addr.split(':').next().unwrap_or(addr);
            crate::tls::negotiate_tls(stream, hostname).await?
        };
        #[cfg(not(feature = "tls"))]
        let stream = MaybeTlsStream::Plain(stream);

        let mut conn = WireConn {
            stream,
            recv_buf: BytesMut::with_capacity(RECV_BUF_SIZE),
            pid: 0,
            secret: 0,
        };

        // Send startup message.
        let mut buf = BytesMut::new();
        frontend::encode_startup(user, database, &mut buf);
        conn.send_raw(&buf).await?;

        // Authentication loop.
        loop {
            let msg = conn.recv_msg().await?;
            match msg {
                BackendMsg::AuthenticationOk => {}
                BackendMsg::AuthenticationCleartextPassword => {
                    let mut buf = BytesMut::new();
                    frontend::encode_password(password.as_bytes(), &mut buf);
                    conn.send_raw(&buf).await?;
                }
                BackendMsg::AuthenticationMd5Password { salt } => {
                    let hash = frontend::md5_password(user, password, &salt);
                    let mut buf = BytesMut::new();
                    frontend::encode_password(&hash, &mut buf);
                    conn.send_raw(&buf).await?;
                }
                BackendMsg::AuthenticationSASL { mechanisms } => {
                    if !mechanisms.iter().any(|m| m == "SCRAM-SHA-256") {
                        return Err(PgWireError::Protocol(
                            format!("No supported SASL mechanism: {:?}", mechanisms),
                        ));
                    }
                    // Use channel binding if TLS is active.
                    let cb = crate::scram::ChannelBinding::None;
                    let (scram, client_first) = ScramClient::new(password, cb);
                    let mut buf = BytesMut::new();
                    frontend::encode_message(
                        &FrontendMsg::SASLInitialResponse {
                            mechanism: b"SCRAM-SHA-256",
                            data: &client_first,
                        },
                        &mut buf,
                    );
                    conn.send_raw(&buf).await?;

                    // Wait for server-first.
                    let server_first = loop {
                        match conn.recv_msg().await? {
                            BackendMsg::AuthenticationSASLContinue { data } => break data,
                            BackendMsg::ErrorResponse { fields } => {
                                return Err(PgWireError::Pg(fields));
                            }
                            _ => {}
                        }
                    };

                    let client_final = scram
                        .process_server_first(&server_first)
                        .map_err(PgWireError::Protocol)?;
                    let mut buf = BytesMut::new();
                    frontend::encode_message(
                        &FrontendMsg::SASLResponse(&client_final),
                        &mut buf,
                    );
                    conn.send_raw(&buf).await?;

                    // Wait for server-final + AuthenticationOk.
                    loop {
                        match conn.recv_msg().await? {
                            BackendMsg::AuthenticationSASLFinal { .. } => {}
                            BackendMsg::AuthenticationOk => break,
                            BackendMsg::ErrorResponse { fields } => {
                                return Err(PgWireError::Pg(fields));
                            }
                            _ => {}
                        }
                    }
                }
                BackendMsg::ParameterStatus { .. } => {} // Collect if needed
                BackendMsg::BackendKeyData { pid, secret } => {
                    conn.pid = pid;
                    conn.secret = secret;
                }
                BackendMsg::ReadyForQuery { .. } => break,
                BackendMsg::ErrorResponse { fields } => {
                    return Err(PgWireError::Pg(fields));
                }
                BackendMsg::NoticeResponse { .. } => {}
                other => {
                    tracing::debug!("Startup: ignoring {:?}", other);
                }
            }
        }

        Ok(conn)
    }

    /// Send a raw buffer to the server (one write syscall).
    pub async fn send_raw(&mut self, buf: &[u8]) -> Result<(), PgWireError> {
        self.stream.write_all(buf).await?;
        Ok(())
    }

    /// Read one complete backend message from the connection.
    /// Uses an internal buffer to minimize read() syscalls.
    pub async fn recv_msg(&mut self) -> Result<BackendMsg, PgWireError> {
        loop {
            // Try to parse a message from the buffer.
            if let Some(msg) = backend::parse_message(&mut self.recv_buf)
                .map_err(PgWireError::Protocol)?
            {
                return Ok(msg);
            }

            // Not enough data — read more from the socket.
            let n = self.stream.read_buf(&mut self.recv_buf).await?;
            if n == 0 {
                return Err(PgWireError::ConnectionClosed);
            }
        }
    }

    /// Receive messages until ReadyForQuery, collecting DataRows.
    /// Returns (rows, command_tag).
    pub async fn collect_rows(&mut self) -> Result<(Vec<Vec<Option<Vec<u8>>>>, String), PgWireError> {
        let mut rows = Vec::new();
        let mut tag = String::new();

        loop {
            let msg = self.recv_msg().await?;
            match msg {
                BackendMsg::DataRow { columns } => {
                    tracing::trace!("collect_rows: DataRow with {} cols", columns.len());
                    rows.push(columns);
                }
                BackendMsg::CommandComplete { tag: t } => tag = t,
                BackendMsg::ReadyForQuery { .. } => return Ok((rows, tag)),
                BackendMsg::ParseComplete | BackendMsg::BindComplete | BackendMsg::NoData => {}
                BackendMsg::RowDescription { .. } => {}
                BackendMsg::ErrorResponse { fields } => {
                    // Drain until ReadyForQuery.
                    self.drain_until_ready().await?;
                    return Err(PgWireError::Pg(fields));
                }
                BackendMsg::NoticeResponse { .. } => {}
                BackendMsg::EmptyQueryResponse => {}
                _ => {}
            }
        }
    }

    /// Describe a SQL statement: sends Parse + Describe Statement + Sync,
    /// returns (parameter type OIDs, column field descriptions).
    /// Used by compile-time query checking macros.
    pub async fn describe_statement(
        &mut self,
        sql: &str,
    ) -> Result<(Vec<u32>, Vec<crate::protocol::types::FieldDescription>), PgWireError> {
        use crate::protocol::frontend;
        use crate::protocol::types::FrontendMsg;
        let mut buf = bytes::BytesMut::with_capacity(256);

        // Parse (unnamed statement).
        frontend::encode_message(
            &FrontendMsg::Parse {
                name: b"",
                sql: sql.as_bytes(),
                param_oids: &[],
            },
            &mut buf,
        );
        // Describe statement.
        frontend::encode_message(
            &FrontendMsg::Describe {
                kind: b'S',
                name: b"",
            },
            &mut buf,
        );
        // Sync.
        frontend::encode_message(&FrontendMsg::Sync, &mut buf);

        self.send_raw(&buf).await?;

        let mut param_oids = Vec::new();
        let mut fields = Vec::new();

        loop {
            let msg = self.recv_msg().await?;
            match msg {
                BackendMsg::ParseComplete => {}
                BackendMsg::ParameterDescription { type_oids } => {
                    param_oids = type_oids;
                }
                BackendMsg::RowDescription { fields: f } => {
                    fields = f;
                }
                BackendMsg::NoData => {} // query returns no rows
                BackendMsg::ReadyForQuery { .. } => {
                    return Ok((param_oids, fields));
                }
                BackendMsg::ErrorResponse { fields } => {
                    self.drain_until_ready().await?;
                    return Err(PgWireError::Pg(fields));
                }
                _ => {}
            }
        }
    }

    /// Drain messages until ReadyForQuery (error recovery).
    pub async fn drain_until_ready(&mut self) -> Result<(), PgWireError> {
        loop {
            let msg = self.recv_msg().await?;
            if matches!(msg, BackendMsg::ReadyForQuery { .. }) {
                return Ok(());
            }
            // ErrorResponse inside a simple query — absorb it, keep draining.
            if let BackendMsg::ErrorResponse { ref fields } = msg {
                tracing::warn!("Error in drain: {}: {}", fields.code, fields.message);
            }
        }
    }
}
