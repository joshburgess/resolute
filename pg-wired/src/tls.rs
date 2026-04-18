//! TLS support for PostgreSQL wire connections.
//!
//! When the `tls` feature is enabled, connections can negotiate SSL/TLS
//! with the PostgreSQL server using rustls.

use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;

/// A stream that is either plain TCP or TLS-wrapped TCP.
pub enum MaybeTlsStream {
    Plain(TcpStream),
    #[cfg(feature = "tls")]
    Tls(tokio_rustls::client::TlsStream<TcpStream>),
}

/// How to handle TLS negotiation with the server.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TlsMode {
    /// Do not send SSLRequest. Use plain TCP.
    Disable,
    /// Send SSLRequest. Upgrade if the server agrees (`S`), fall back to
    /// plain TCP if the server refuses (`N`). Default.
    Prefer,
    /// Send SSLRequest. Error out if the server refuses.
    Require,
}

impl Default for TlsMode {
    fn default() -> Self {
        TlsMode::Prefer
    }
}

impl AsyncRead for MaybeTlsStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            MaybeTlsStream::Plain(s) => Pin::new(s).poll_read(cx, buf),
            #[cfg(feature = "tls")]
            MaybeTlsStream::Tls(s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for MaybeTlsStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            MaybeTlsStream::Plain(s) => Pin::new(s).poll_write(cx, buf),
            #[cfg(feature = "tls")]
            MaybeTlsStream::Tls(s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            MaybeTlsStream::Plain(s) => Pin::new(s).poll_flush(cx),
            #[cfg(feature = "tls")]
            MaybeTlsStream::Tls(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            MaybeTlsStream::Plain(s) => Pin::new(s).poll_shutdown(cx),
            #[cfg(feature = "tls")]
            MaybeTlsStream::Tls(s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}

impl MaybeTlsStream {
    /// Get the peer address of the underlying TCP stream.
    pub fn peer_addr(&self) -> std::io::Result<std::net::SocketAddr> {
        match self {
            MaybeTlsStream::Plain(s) => s.peer_addr(),
            #[cfg(feature = "tls")]
            MaybeTlsStream::Tls(s) => s.get_ref().0.peer_addr(),
        }
    }
}

/// TLS configuration for PostgreSQL connections.
#[cfg(feature = "tls")]
pub struct TlsConfig {
    /// Custom root CA certificates. If empty, uses system root CAs.
    pub root_certs: Vec<Vec<u8>>,
    /// Client certificate and private key for mTLS (optional).
    pub client_cert: Option<(Vec<Vec<u8>>, Vec<u8>)>,
}

#[cfg(feature = "tls")]
impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            root_certs: Vec::new(),
            client_cert: None,
        }
    }
}

/// Negotiate TLS with default configuration (system root CAs, no client cert).
///
/// Uses `TlsMode::Prefer`: sends SSLRequest, upgrades on `S`, falls back to
/// plain TCP on `N`.
#[cfg(feature = "tls")]
pub async fn negotiate_tls(
    stream: TcpStream,
    hostname: &str,
) -> Result<MaybeTlsStream, crate::error::PgWireError> {
    negotiate_tls_with_config(stream, hostname, &TlsConfig::default(), TlsMode::Prefer).await
}

/// Negotiate TLS with custom configuration (custom CAs, client certs).
///
/// Behavior depends on `mode`:
/// - `Disable`: skip SSLRequest, return plain stream.
/// - `Prefer`: send SSLRequest, upgrade on `S`, fall back on `N`.
/// - `Require`: send SSLRequest, error if server responds `N`.
#[cfg(feature = "tls")]
pub async fn negotiate_tls_with_config(
    mut stream: TcpStream,
    hostname: &str,
    config: &TlsConfig,
    mode: TlsMode,
) -> Result<MaybeTlsStream, crate::error::PgWireError> {
    use bytes::{BufMut, BytesMut};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    if mode == TlsMode::Disable {
        return Ok(MaybeTlsStream::Plain(stream));
    }

    // Send SSLRequest: length=8, code=80877103
    let mut buf = BytesMut::with_capacity(8);
    buf.put_i32(8);
    buf.put_i32(80877103); // SSL request code (1234 << 16 | 5679)
    stream.write_all(&buf).await?;

    // Read 1-byte response.
    let mut response = [0u8; 1];
    stream.read_exact(&mut response).await?;

    match response[0] {
        b'S' => {
            // Server supports SSL — upgrade.
            let mut root_store = rustls::RootCertStore::empty();
            if config.root_certs.is_empty() {
                // Use system root CAs.
                root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
            } else {
                // Use custom root CAs.
                for cert_der in &config.root_certs {
                    root_store
                        .add(rustls_pki_types::CertificateDer::from(cert_der.clone()))
                        .map_err(|e| crate::error::PgWireError::Protocol(
                            format!("invalid root certificate: {e}"),
                        ))?;
                }
            }

            let tls_config = if let Some((ref cert_chain, ref key_der)) = config.client_cert {
                // mTLS: client certificate authentication.
                let certs: Vec<rustls_pki_types::CertificateDer<'static>> = cert_chain
                    .iter()
                    .map(|c| rustls_pki_types::CertificateDer::from(c.clone()))
                    .collect();
                let key = rustls_pki_types::PrivateKeyDer::try_from(key_der.clone())
                    .map_err(|e| crate::error::PgWireError::Protocol(
                        format!("invalid client private key: {e}"),
                    ))?;
                rustls::ClientConfig::builder()
                    .with_root_certificates(root_store)
                    .with_client_auth_cert(certs, key)
                    .map_err(|e| crate::error::PgWireError::Protocol(
                        format!("TLS client auth config error: {e}"),
                    ))?
            } else {
                rustls::ClientConfig::builder()
                    .with_root_certificates(root_store)
                    .with_no_client_auth()
            };

            let connector = tokio_rustls::TlsConnector::from(std::sync::Arc::new(tls_config));
            let server_name = rustls_pki_types::ServerName::try_from(hostname.to_string())
                .map_err(|e| crate::error::PgWireError::Protocol(format!("invalid hostname: {e}")))?;

            let tls_stream = connector.connect(server_name, stream).await?;
            Ok(MaybeTlsStream::Tls(tls_stream))
        }
        b'N' => {
            if mode == TlsMode::Require {
                return Err(crate::error::PgWireError::Protocol(
                    "server does not support TLS but sslmode=require".to_string(),
                ));
            }
            // Prefer mode: server doesn't support SSL, continue with plain TCP.
            Ok(MaybeTlsStream::Plain(stream))
        }
        other => Err(crate::error::PgWireError::Protocol(format!(
            "unexpected SSL response: {}",
            other as char
        ))),
    }
}
