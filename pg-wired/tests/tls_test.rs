//! Real TLS handshake test.
//!
//! Skipped unless `RESOLUTE_TLS_TEST_ADDR` is set to a Postgres listening on
//! TLS, in which case `RESOLUTE_TLS_TEST_CA_DER` must point at the DER-encoded
//! root CA. CI sets both. Locally see `docs/CONTRIBUTING.md`.

#![cfg(feature = "tls")]

use pg_wired::{PgPipeline, TlsConfig, TlsMode, WireConn};

mod test_env;

fn tls_addr() -> Option<String> {
    std::env::var("RESOLUTE_TLS_TEST_ADDR").ok()
}

fn ca_der_path() -> Option<String> {
    std::env::var("RESOLUTE_TLS_TEST_CA_DER").ok()
}

#[tokio::test]
async fn tls_connect_with_custom_ca_succeeds() {
    let Some(addr) = tls_addr() else {
        eprintln!("RESOLUTE_TLS_TEST_ADDR not set, skipping tls_connect_with_custom_ca_succeeds");
        return;
    };
    let ca_path =
        ca_der_path().expect("RESOLUTE_TLS_TEST_ADDR is set but RESOLUTE_TLS_TEST_CA_DER is not");
    let ca_der = std::fs::read(&ca_path).expect("read CA DER file");

    let mut tls_config = TlsConfig::default();
    tls_config.root_certs.push(ca_der);

    let conn = WireConn::connect_with_tls_config(
        &addr,
        test_env::user(),
        test_env::pass(),
        test_env::db(),
        &[],
        TlsMode::Require,
        &tls_config,
    )
    .await
    .expect("connect over TLS with custom root CA");

    // SCRAM-SHA-256-PLUS must win over plain SCRAM-SHA-256 whenever TLS is
    // active and the server advertises both. Anything else is a silent
    // channel-binding downgrade.
    assert_eq!(
        conn.auth_mechanism(),
        "SCRAM-SHA-256-PLUS",
        "expected channel binding (SCRAM-SHA-256-PLUS) over TLS"
    );

    let mut pg = PgPipeline::new(conn);
    pg.simple_query("SELECT 1").await.expect("SELECT 1 over TLS");
}

#[tokio::test]
async fn plain_scram_used_without_tls() {
    let conn = WireConn::connect(
        test_env::addr(),
        test_env::user(),
        test_env::pass(),
        test_env::db(),
    )
    .await
    .expect("connect over plain TCP");

    assert_eq!(
        conn.auth_mechanism(),
        "SCRAM-SHA-256",
        "expected plain SCRAM-SHA-256 without TLS"
    );
}

#[tokio::test]
async fn tls_require_against_plaintext_server_fails() {
    if tls_addr().is_some() {
        // The TLS-only Postgres in CI rejects plaintext startup, but the
        // *negative* assertion here targets the plain compose Postgres on
        // RESOLUTE_TEST_ADDR, which is unaffected.
    }

    let result = WireConn::connect_with_tls_config(
        test_env::addr(),
        test_env::user(),
        test_env::pass(),
        test_env::db(),
        &[],
        TlsMode::Require,
        &TlsConfig::default(),
    )
    .await;

    assert!(
        result.is_err(),
        "TlsMode::Require against plaintext server must error"
    );
}
