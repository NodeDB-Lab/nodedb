// SPDX-License-Identifier: BUSL-1.1

//! TLS policy enforcement over a real handshake.
//!
//! The policy's decision table is unit-tested in
//! `control::security::tls_policy`; what needs a socket is the *capture* half:
//! that the negotiated version can actually be read back off an accepted
//! connection, and that the value the listener carries forward is the one the
//! policy then rules on. This test performs a genuine rustls handshake against
//! a self-signed certificate, builds the same `ConnStream` the native, RESP,
//! and ILP listeners build, and runs the captured value through the same guard
//! those listeners call after authentication.

use std::sync::Arc;

use rcgen::generate_simple_self_signed;
use tempfile::TempDir;
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::TlsConnector;
use tokio_rustls::rustls::ClientConfig;
use tokio_rustls::rustls::pki_types::ServerName;

use nodedb::bootstrap::tls::build_tls_acceptor;
use nodedb::bridge::dispatch::Dispatcher;
use nodedb::config::auth::AuthConfig;
use nodedb::config::server::TlsSettings;
use nodedb::control::security::identity::{AuthMethod, AuthenticatedIdentity, DatabaseSet, Role};
use nodedb::control::security::tls_policy::{TlsPolicyConfig, TlsVersion, TransportSecurity};
use nodedb::control::server::conn_stream::ConnStream;
use nodedb::control::server::session_auth::check_transport_security;
use nodedb::control::state::SharedState;
use nodedb::types::TenantId;
use nodedb::wal::WalManager;

// ── Fixtures ──────────────────────────────────────────────────────────────

fn generate_self_signed_cert(dir: &TempDir) -> (std::path::PathBuf, std::path::PathBuf) {
    let certified = generate_simple_self_signed(vec!["localhost".to_string()])
        .expect("self-signed cert generation failed");

    let cert_path = dir.path().join("test.crt");
    let key_path = dir.path().join("test.key");
    std::fs::write(&cert_path, certified.cert.pem().as_bytes()).expect("write cert");
    std::fs::write(&key_path, certified.signing_key.serialize_pem().as_bytes()).expect("write key");

    (cert_path, key_path)
}

fn tls_settings(cert_path: std::path::PathBuf, key_path: std::path::PathBuf) -> TlsSettings {
    TlsSettings {
        cert_path,
        key_path,
        cert_reload_interval_secs: None,
        native: true,
        pgwire: true,
        http: true,
        resp: true,
        ilp: true,
    }
}

/// A `ServerCertVerifier` that accepts any certificate — self-signed test
/// certificates only.
#[derive(Debug)]
struct NoVerification;

impl tokio_rustls::rustls::client::danger::ServerCertVerifier for NoVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &tokio_rustls::rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[tokio_rustls::rustls::pki_types::CertificateDer<'_>],
        _server_name: &tokio_rustls::rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: tokio_rustls::rustls::pki_types::UnixTime,
    ) -> Result<tokio_rustls::rustls::client::danger::ServerCertVerified, tokio_rustls::rustls::Error>
    {
        Ok(tokio_rustls::rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &tokio_rustls::rustls::pki_types::CertificateDer<'_>,
        _dss: &tokio_rustls::rustls::DigitallySignedStruct,
    ) -> Result<
        tokio_rustls::rustls::client::danger::HandshakeSignatureValid,
        tokio_rustls::rustls::Error,
    > {
        Ok(tokio_rustls::rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &tokio_rustls::rustls::pki_types::CertificateDer<'_>,
        _dss: &tokio_rustls::rustls::DigitallySignedStruct,
    ) -> Result<
        tokio_rustls::rustls::client::danger::HandshakeSignatureValid,
        tokio_rustls::rustls::Error,
    > {
        Ok(tokio_rustls::rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<tokio_rustls::rustls::SignatureScheme> {
        use tokio_rustls::rustls::SignatureScheme;
        vec![
            SignatureScheme::ECDSA_NISTP256_SHA256,
            SignatureScheme::ECDSA_NISTP384_SHA384,
            SignatureScheme::RSA_PSS_SHA256,
            SignatureScheme::RSA_PSS_SHA384,
            SignatureScheme::RSA_PSS_SHA512,
            SignatureScheme::RSA_PKCS1_SHA256,
            SignatureScheme::RSA_PKCS1_SHA384,
            SignatureScheme::RSA_PKCS1_SHA512,
            SignatureScheme::ED25519,
        ]
    }
}

fn client_connector() -> TlsConnector {
    let config = ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(NoVerification))
        .with_no_client_auth();
    TlsConnector::from(Arc::new(config))
}

/// Open a catalog-backed `SharedState` from a server config whose `[auth]`
/// section carries `tls_policy`, exactly as an operator's config file would.
fn open_state(
    dir: &TempDir,
    tls_policy: Option<TlsPolicyConfig>,
) -> nodedb::Result<Arc<SharedState>> {
    let wal = Arc::new(
        WalManager::open_for_testing(&dir.path().join("tls-policy.wal")).expect("open test WAL"),
    );
    let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
    let auth_config = AuthConfig {
        tls_policy,
        ..AuthConfig::default()
    };
    SharedState::open(
        nodedb::control::state::DataPlaneHandles {
            dispatcher,
            quiesce: nodedb::bridge::quiesce::CollectionQuiesce::new(),
            array_catalog: nodedb::control::array_catalog::ArrayCatalog::handle(),
            system_metrics: std::sync::Arc::new(nodedb::control::metrics::SystemMetrics::new()),
        },
        wal,
        &dir.path().join("system.redb"),
        nodedb::control::state::OpenConfig::strict(&auth_config),
        nodedb_types::config::TuningConfig::default(),
        false,
        nodedb::data::executor::core_loop::test_governor(),
    )
}

fn state_with_policy(dir: &TempDir, config: TlsPolicyConfig) -> Arc<SharedState> {
    open_state(dir, Some(config)).expect("shared state opens")
}

fn regular_identity() -> AuthenticatedIdentity {
    AuthenticatedIdentity::new_regular(
        7701,
        "tls-policy-user",
        TenantId::new(1),
        AuthMethod::ScramSha256,
        vec![Role::ReadWrite],
        None,
        DatabaseSet::All,
    )
}

/// Accept one connection, TLS-wrap it exactly as the listeners do, and return
/// what `ConnStream` reports about the negotiated transport.
async fn captured_tls_transport() -> TransportSecurity {
    let _ = tokio_rustls::rustls::crypto::aws_lc_rs::default_provider().install_default();

    let dir = TempDir::new().expect("tmpdir");
    let (cert_path, key_path) = generate_self_signed_cert(&dir);
    let acceptor = build_tls_acceptor(&tls_settings(cert_path, key_path)).expect("TLS acceptor");

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local addr");

    let client = tokio::spawn(async move {
        let socket = TcpStream::connect(addr).await.expect("client connect");
        let name = ServerName::try_from("localhost").expect("server name");
        let stream = client_connector()
            .connect(name, socket)
            .await
            .expect("client handshake");
        // Hold the connection open until the server side has inspected it.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        drop(stream);
    });

    let (socket, _peer) = listener.accept().await.expect("accept");
    let tls_stream = acceptor.accept(socket).await.expect("server handshake");
    let conn = ConnStream::tls(tls_stream);
    let captured = conn.transport_security();

    client.await.expect("client task");
    captured
}

// ── Capture ───────────────────────────────────────────────────────────────

/// The capture helper reads a real negotiated version, not a placeholder.
#[tokio::test]
async fn a_real_handshake_is_captured_with_its_negotiated_version() {
    let captured = captured_tls_transport().await;

    assert_eq!(
        captured,
        TransportSecurity::Tls(TlsVersion::Tls1_3),
        "rustls negotiates TLS 1.3 with the default provider, and the capture \
         helper must report exactly that"
    );
    assert!(captured.is_encrypted());
}

#[tokio::test]
async fn a_plain_connection_is_captured_as_cleartext() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local addr");
    let client = tokio::spawn(async move {
        let socket = TcpStream::connect(addr).await.expect("client connect");
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        drop(socket);
    });

    let (socket, _peer) = listener.accept().await.expect("accept");
    let conn = ConnStream::plain(socket);

    assert_eq!(conn.transport_security(), TransportSecurity::Cleartext);
    assert!(!conn.transport_security().is_encrypted());
    client.await.expect("client task");
}

// ── Enforcement over the captured value ───────────────────────────────────

/// The captured value drives the guard: the same TLS 1.3 connection passes a
/// `min_tls_version = "1.3"` policy and would fail one that demanded more than
/// it negotiated — proving the version reaches the comparison rather than
/// only the error message.
#[tokio::test]
async fn the_captured_version_is_what_the_policy_rules_on() {
    let captured = captured_tls_transport().await;
    let identity = regular_identity();
    let dir = TempDir::new().expect("tmpdir");

    let strict = state_with_policy(
        &dir,
        TlsPolicyConfig {
            enabled: true,
            min_tls_version: "1.3".into(),
            reject_cleartext: true,
        },
    );
    check_transport_security(&strict, &identity, captured, "127.0.0.1:5432")
        .expect("a TLS 1.3 connection clears a 1.3 minimum");

    // The same policy refuses the plaintext connection the other listeners
    // would have produced on the same server.
    let refused = check_transport_security(
        &strict,
        &identity,
        TransportSecurity::Cleartext,
        "127.0.0.1:5432",
    );
    assert!(
        refused.is_err(),
        "reject_cleartext must refuse a plaintext connection"
    );

    // ...and with enforcement off, both are admitted.
    let dir_off = TempDir::new().expect("tmpdir");
    let permissive = state_with_policy(&dir_off, TlsPolicyConfig::default());
    check_transport_security(&permissive, &identity, captured, "127.0.0.1:5432")
        .expect("the default policy refuses nothing");
    check_transport_security(
        &permissive,
        &identity,
        TransportSecurity::Cleartext,
        "127.0.0.1:5432",
    )
    .expect("the default policy admits plaintext");
}

// ── Configuration ─────────────────────────────────────────────────────────

/// `[auth.tls_policy]` from the server config reaches the policy the guard
/// consults — the knob is not inert.
#[test]
fn tls_policy_config_from_server_config_reaches_the_guard() {
    let identity = regular_identity();

    let strict_dir = TempDir::new().expect("tempdir");
    let strict = state_with_policy(
        &strict_dir,
        TlsPolicyConfig {
            enabled: true,
            min_tls_version: "1.3".into(),
            reject_cleartext: true,
        },
    );
    assert!(
        check_transport_security(
            &strict,
            &identity,
            TransportSecurity::Tls(TlsVersion::Tls1_2),
            "127.0.0.1:5432",
        )
        .is_err(),
        "a configured 1.3 minimum must refuse a TLS 1.2 connection"
    );
    assert!(
        check_transport_security(
            &strict,
            &identity,
            TransportSecurity::Cleartext,
            "127.0.0.1:5432",
        )
        .is_err(),
        "a configured reject_cleartext must refuse a plaintext connection"
    );

    // An absent section leaves enforcement off, so the same two connections
    // are admitted.
    let absent_dir = TempDir::new().expect("tempdir");
    let absent = open_state(&absent_dir, None).expect("shared state opens");
    check_transport_security(
        &absent,
        &identity,
        TransportSecurity::Tls(TlsVersion::Tls1_2),
        "127.0.0.1:5432",
    )
    .expect("an absent section enforces nothing");
    check_transport_security(
        &absent,
        &identity,
        TransportSecurity::Cleartext,
        "127.0.0.1:5432",
    )
    .expect("an absent section admits plaintext");
}

/// An unparseable `min_tls_version` fails startup rather than silently
/// defaulting to a version the operator never asked for.
#[test]
fn an_unparseable_min_tls_version_fails_startup() {
    let dir = TempDir::new().expect("tempdir");
    let result = open_state(
        &dir,
        Some(TlsPolicyConfig {
            enabled: true,
            min_tls_version: "1.2+".into(),
            reject_cleartext: false,
        }),
    );

    assert!(
        matches!(result, Err(nodedb::Error::Config { .. })),
        "a server must not start with a TLS minimum it cannot interpret"
    );
}
