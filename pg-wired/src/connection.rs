//! Synchronous-style PostgreSQL connection driving the v3 wire protocol on
//! a single owned [`tokio::net::TcpStream`]. Use [`crate::AsyncConn`] for the
//! shared, multi-task connection wrapper most callers want.

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::error::PgWireError;
use crate::protocol::backend;
use crate::protocol::frontend;
use crate::protocol::types::{BackendMsg, FrontendMsg, RawRow};
use crate::scram::ScramClient;
use crate::tls::{MaybeTlsStream, TlsMode};

/// Raw PostgreSQL wire connection.
/// Handles TCP I/O, buffered reading, and authentication.
pub struct WireConn {
    pub(crate) stream: MaybeTlsStream,
    recv_buf: BytesMut,
    pub(crate) pid: i32,
    pub(crate) secret: i32,
    /// Server parameters reported via `ParameterStatus` during startup.
    ///
    /// PostgreSQL reports a small set of GUCs it considers useful to clients:
    /// typically `server_version`, `server_encoding`, `client_encoding`,
    /// `application_name`, `is_superuser`, `session_authorization`,
    /// `DateStyle`, `IntervalStyle`, `TimeZone`, `integer_datetimes`, and
    /// `standard_conforming_strings`. The exact set depends on the server
    /// version and its `GUC_REPORT` configuration.
    ///
    /// This map is populated once on connect and is not kept in sync when
    /// the server emits later `ParameterStatus` messages (for example after
    /// `SET TimeZone = ...`). Treat these values as startup defaults, not a
    /// live view of the session state.
    pub params: std::collections::HashMap<String, String>,
    /// Authentication mechanism the server selected during startup.
    ///
    /// One of `"trust"`, `"cleartext"`, `"md5"`, `"SCRAM-SHA-256"`, or
    /// `"SCRAM-SHA-256-PLUS"`. Useful for tests that need to verify channel
    /// binding actually fired and for operational logging.
    pub(crate) auth_mechanism: &'static str,
}

impl WireConn {
    /// Backend process ID assigned by the server. Useful for logging and for
    /// building a cancel token. The secret key that pairs with this PID is
    /// intentionally not exposed; use `cancel_token()` to obtain a token that
    /// can send a cancel request.
    pub fn pid(&self) -> i32 {
        self.pid
    }

    /// Authentication mechanism the server selected during startup.
    ///
    /// Returns one of `"trust"`, `"cleartext"`, `"md5"`, `"SCRAM-SHA-256"`,
    /// or `"SCRAM-SHA-256-PLUS"`. `"SCRAM-SHA-256-PLUS"` confirms that
    /// `tls-server-end-point` channel binding was negotiated.
    pub fn auth_mechanism(&self) -> &'static str {
        self.auth_mechanism
    }
}

impl std::fmt::Debug for WireConn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WireConn")
            .field("pid", &self.pid)
            .field("params", &self.params)
            .finish_non_exhaustive()
    }
}

const RECV_BUF_SIZE: usize = 32 * 1024; // 32KB recv buffer

impl WireConn {
    /// Choose the best SCRAM mechanism based on TLS state and server support.
    /// Returns (ChannelBinding, mechanism_name_bytes).
    #[allow(clippy::result_large_err)]
    fn choose_scram_mechanism(
        &self,
        mechanisms: &[String],
    ) -> Result<(crate::scram::ChannelBinding, &'static [u8], &'static str), PgWireError> {
        // If TLS is active and server supports SCRAM-SHA-256-PLUS, use channel binding.
        #[cfg(feature = "tls")]
        if let MaybeTlsStream::Tls(ref tls) = self.stream {
            if mechanisms.iter().any(|m| m == "SCRAM-SHA-256-PLUS") {
                if let Some(certs) = tls.get_ref().1.peer_certificates() {
                    if let Some(cert) = certs.first() {
                        use sha2::{Digest, Sha256};
                        let hash = Sha256::digest(cert.as_ref()).to_vec();
                        return Ok((
                            crate::scram::ChannelBinding::TlsServerEndPoint(hash),
                            b"SCRAM-SHA-256-PLUS",
                            "SCRAM-SHA-256-PLUS",
                        ));
                    }
                }
            }
        }

        // Fall back to plain SCRAM-SHA-256.
        if mechanisms.iter().any(|m| m == "SCRAM-SHA-256") {
            Ok((
                crate::scram::ChannelBinding::None,
                b"SCRAM-SHA-256",
                "SCRAM-SHA-256",
            ))
        } else {
            Err(PgWireError::Protocol(format!(
                "No supported SASL mechanism: {:?}",
                mechanisms
            )))
        }
    }

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
        Self::connect_with_options(addr, user, password, database, &[], TlsMode::default()).await
    }

    /// Connect with additional startup parameters.
    ///
    /// Parameters are sent in the startup message and appear in `pg_stat_activity`.
    /// Common parameters: `application_name`, `client_encoding`, `options`.
    ///
    /// ```no_run
    /// # async fn _doctest() -> Result<(), Box<dyn std::error::Error>> {
    /// use pg_wired::WireConn;
    /// let _conn = WireConn::connect_with_params(
    ///     "127.0.0.1:5432", "user", "pass", "mydb",
    ///     &[("application_name", "my-service")],
    /// ).await?;
    /// # Ok(()) }
    /// ```
    pub async fn connect_with_params(
        addr: &str,
        user: &str,
        password: &str,
        database: &str,
        startup_params: &[(&str, &str)],
    ) -> Result<Self, PgWireError> {
        Self::connect_with_options(
            addr,
            user,
            password,
            database,
            startup_params,
            TlsMode::default(),
        )
        .await
    }

    /// Connect with startup parameters and an explicit TLS mode.
    ///
    /// Uses the system root trust store (`webpki-roots`) for certificate
    /// verification. To override the trust store, supply a client
    /// certificate, or otherwise customize TLS, use
    /// [`Self::connect_with_tls_config`].
    pub async fn connect_with_options(
        addr: &str,
        user: &str,
        password: &str,
        database: &str,
        startup_params: &[(&str, &str)],
        tls_mode: TlsMode,
    ) -> Result<Self, PgWireError> {
        #[cfg(feature = "tls")]
        {
            Self::connect_with_tls_config(
                addr,
                user,
                password,
                database,
                startup_params,
                tls_mode,
                &crate::tls::TlsConfig::default(),
            )
            .await
        }
        #[cfg(not(feature = "tls"))]
        {
            Self::connect_inner(addr, user, password, database, startup_params, tls_mode).await
        }
    }

    /// Connect with startup parameters, an explicit TLS mode, and a custom
    /// TLS configuration (custom trust roots and/or a client certificate).
    ///
    /// Only available when the `tls` feature is enabled.
    #[cfg(feature = "tls")]
    pub async fn connect_with_tls_config(
        addr: &str,
        user: &str,
        password: &str,
        database: &str,
        startup_params: &[(&str, &str)],
        tls_mode: TlsMode,
        tls_config: &crate::tls::TlsConfig,
    ) -> Result<Self, PgWireError> {
        Self::connect_inner(
            addr,
            user,
            password,
            database,
            startup_params,
            tls_mode,
            tls_config,
        )
        .await
    }

    #[cfg(feature = "tls")]
    async fn connect_inner(
        addr: &str,
        user: &str,
        password: &str,
        database: &str,
        startup_params: &[(&str, &str)],
        tls_mode: TlsMode,
        tls_config: &crate::tls::TlsConfig,
    ) -> Result<Self, PgWireError> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;

        let socket = socket2::SockRef::from(&stream);
        let keepalive = socket2::TcpKeepalive::new()
            .with_time(std::time::Duration::from_secs(60))
            .with_interval(std::time::Duration::from_secs(15));
        let _ = socket.set_tcp_keepalive(&keepalive);

        let hostname = parse_hostname(addr);
        let stream =
            crate::tls::negotiate_tls_with_config(stream, &hostname, tls_config, tls_mode).await?;

        Self::finish_startup(stream, user, password, database, startup_params).await
    }

    #[cfg(not(feature = "tls"))]
    async fn connect_inner(
        addr: &str,
        user: &str,
        password: &str,
        database: &str,
        startup_params: &[(&str, &str)],
        tls_mode: TlsMode,
    ) -> Result<Self, PgWireError> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;

        let socket = socket2::SockRef::from(&stream);
        let keepalive = socket2::TcpKeepalive::new()
            .with_time(std::time::Duration::from_secs(60))
            .with_interval(std::time::Duration::from_secs(15));
        let _ = socket.set_tcp_keepalive(&keepalive);

        if tls_mode == TlsMode::Require {
            return Err(PgWireError::Protocol(
                "sslmode=require but pg-wired was built without the `tls` feature".into(),
            ));
        }
        let stream = MaybeTlsStream::Plain(stream);

        Self::finish_startup(stream, user, password, database, startup_params).await
    }

    async fn finish_startup(
        stream: MaybeTlsStream,
        user: &str,
        password: &str,
        database: &str,
        startup_params: &[(&str, &str)],
    ) -> Result<Self, PgWireError> {
        let mut conn = WireConn {
            stream,
            recv_buf: BytesMut::with_capacity(RECV_BUF_SIZE),
            pid: 0,
            secret: 0,
            params: std::collections::HashMap::new(),
            // Default to "trust": if the server sends AuthenticationOk without
            // a prior challenge, no real auth method ran.
            auth_mechanism: "trust",
        };

        // Send startup message with optional extra parameters.
        let mut buf = BytesMut::new();
        frontend::encode_startup_with_params(user, database, startup_params, &mut buf);
        conn.send_raw(&buf).await?;

        // Authentication loop.
        loop {
            let msg = conn.recv_msg().await?;
            match msg {
                BackendMsg::AuthenticationOk => {}
                BackendMsg::AuthenticationCleartextPassword => {
                    conn.auth_mechanism = "cleartext";
                    let mut buf = BytesMut::new();
                    frontend::encode_password(password.as_bytes(), &mut buf);
                    conn.send_raw(&buf).await?;
                }
                BackendMsg::AuthenticationMd5Password { salt } => {
                    conn.auth_mechanism = "md5";
                    let hash = frontend::md5_password(user, password, &salt);
                    let mut buf = BytesMut::new();
                    frontend::encode_password(&hash, &mut buf);
                    conn.send_raw(&buf).await?;
                }
                BackendMsg::AuthenticationSASL { mechanisms } => {
                    // Prefer SCRAM-SHA-256-PLUS (with channel binding) when TLS is active.
                    let (cb, mechanism, name) = conn.choose_scram_mechanism(&mechanisms)?;
                    conn.auth_mechanism = name;
                    let (scram, client_first) = ScramClient::new(password, cb);
                    let mut buf = BytesMut::new();
                    frontend::encode_message(
                        &FrontendMsg::SASLInitialResponse {
                            mechanism,
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
                    frontend::encode_message(&FrontendMsg::SASLResponse(&client_final), &mut buf);
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
                BackendMsg::ParameterStatus { name, value } => {
                    tracing::debug!(name = %name, value = %value, "server parameter");
                    conn.params.insert(name, value);
                }
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
            if let Some(msg) =
                backend::parse_message(&mut self.recv_buf).map_err(PgWireError::Protocol)?
            {
                return Ok(msg);
            }

            // Not enough data — read more from the socket.
            let n = self.stream.read_buf(&mut self.recv_buf).await?;
            if n == 0 {
                // EOF — try to parse any remaining buffered data before giving up.
                if let Some(msg) =
                    backend::parse_message(&mut self.recv_buf).map_err(PgWireError::Protocol)?
                {
                    return Ok(msg);
                }
                return Err(PgWireError::ConnectionClosed);
            }
        }
    }

    /// Receive messages until ReadyForQuery, collecting DataRows.
    /// Returns (rows, command_tag).
    pub async fn collect_rows(&mut self) -> Result<(Vec<RawRow>, String), PgWireError> {
        let mut rows = Vec::new();
        let mut tag = String::new();

        loop {
            let msg = self.recv_msg().await?;
            match msg {
                BackendMsg::DataRow(row) => {
                    tracing::trace!("collect_rows: DataRow with {} cols", row.len());
                    rows.push(row);
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
    /// Also attempts to parse any remaining data in the receive buffer
    /// before declaring the connection closed.
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

#[cfg(any(feature = "tls", test))]
/// Extract hostname from an address string, handling IPv6 bracket notation.
/// Examples: "localhost:5432" → "localhost", "[::1]:5432" → "::1", "host" → "host"
fn parse_hostname(addr: &str) -> String {
    if addr.starts_with('[') {
        // IPv6 bracket notation: [::1]:5432
        if let Some(end) = addr.find(']') {
            return addr[1..end].to_string();
        }
    }
    // IPv4 or hostname: host:port
    addr.split(':').next().unwrap_or(addr).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_hostname_ipv4() {
        assert_eq!(parse_hostname("127.0.0.1:5432"), "127.0.0.1");
    }

    #[test]
    fn test_parse_hostname_name() {
        assert_eq!(parse_hostname("localhost:5432"), "localhost");
    }

    #[test]
    fn test_parse_hostname_ipv6() {
        assert_eq!(parse_hostname("[::1]:5432"), "::1");
    }

    #[test]
    fn test_parse_hostname_ipv6_full() {
        assert_eq!(parse_hostname("[2001:db8::1]:5432"), "2001:db8::1");
    }

    #[test]
    fn test_parse_hostname_no_port() {
        assert_eq!(parse_hostname("myhost"), "myhost");
    }
}
