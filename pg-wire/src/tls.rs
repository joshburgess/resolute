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

/// Negotiate TLS with the PostgreSQL server.
///
/// Sends SSLRequest. If the server responds `S`, upgrades the connection
/// using rustls. If `N`, returns the plain stream.
#[cfg(feature = "tls")]
pub async fn negotiate_tls(
    mut stream: TcpStream,
    hostname: &str,
) -> Result<MaybeTlsStream, crate::error::PgWireError> {
    use bytes::{BufMut, BytesMut};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

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
            root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

            let config = rustls::ClientConfig::builder()
                .with_root_certificates(root_store)
                .with_no_client_auth();

            let connector = tokio_rustls::TlsConnector::from(std::sync::Arc::new(config));
            let server_name = rustls_pki_types::ServerName::try_from(hostname.to_string())
                .map_err(|e| crate::error::PgWireError::Protocol(format!("invalid hostname: {e}")))?;

            let tls_stream = connector.connect(server_name, stream).await?;
            Ok(MaybeTlsStream::Tls(tls_stream))
        }
        b'N' => {
            // Server doesn't support SSL — continue with plain TCP.
            Ok(MaybeTlsStream::Plain(stream))
        }
        other => Err(crate::error::PgWireError::Protocol(format!(
            "unexpected SSL response: {}",
            other as char
        ))),
    }
}
