//! QUIC and TLS configuration for Raft RPCs.

use std::sync::Arc;
use std::time::Duration;

use crate::error::{ClusterError, Result};

/// ALPN protocol identifier for NodeDB Raft RPCs.
pub const ALPN_NODEDB_RAFT: &[u8] = b"nodedb-raft/1";

/// SNI hostname used for QUIC connections between NodeDB nodes.
pub const SNI_HOSTNAME: &str = "nodedb";

/// Default RPC timeout.
pub const DEFAULT_RPC_TIMEOUT: Duration = Duration::from_secs(5);

/// Transport config tuned for Raft RPCs in a datacenter.
pub fn raft_transport_config() -> quinn::TransportConfig {
    let mut config = quinn::TransportConfig::default();
    // Raft RPCs use bidi streams: one per request-response pair.
    config.max_concurrent_bidi_streams(quinn::VarInt::from_u32(256));
    // Also allow uni streams for future migration/snapshot streaming.
    config.max_concurrent_uni_streams(quinn::VarInt::from_u32(256));
    // Datacenter tuning: generous windows, low RTT estimate.
    config.receive_window(quinn::VarInt::from_u32(16 * 1024 * 1024));
    config.send_window(16 * 1024 * 1024);
    config.stream_receive_window(quinn::VarInt::from_u32(4 * 1024 * 1024));
    config.initial_rtt(Duration::from_micros(100));
    config.keep_alive_interval(Some(Duration::from_secs(5)));
    config.max_idle_timeout(Some(
        Duration::from_secs(30)
            .try_into()
            .expect("30s fits IdleTimeout"),
    ));
    config
}

/// Build a QUIC server config with self-signed TLS (dev/bootstrap mode).
///
/// Production clusters use mTLS via [`nexar::transport::tls::ClusterCa`].
pub fn make_raft_server_config() -> Result<quinn::ServerConfig> {
    let (cert, key) = nexar::transport::tls::generate_self_signed_cert().map_err(|e| {
        ClusterError::Transport {
            detail: format!("generate cert: {e}"),
        }
    })?;

    let provider = rustls::crypto::ring::default_provider();
    let mut tls_config = rustls::ServerConfig::builder_with_provider(Arc::new(provider))
        .with_safe_default_protocol_versions()
        .map_err(|e| ClusterError::Transport {
            detail: format!("server TLS protocol versions: {e}"),
        })?
        .with_no_client_auth()
        .with_single_cert(vec![cert], key)
        .map_err(|e| ClusterError::Transport {
            detail: format!("server TLS config: {e}"),
        })?;

    tls_config.alpn_protocols = vec![ALPN_NODEDB_RAFT.to_vec()];

    let quic_crypto = quinn::crypto::rustls::QuicServerConfig::try_from(Arc::new(tls_config))
        .map_err(|e| ClusterError::Transport {
            detail: format!("QUIC server config: {e}"),
        })?;

    let mut server_config = quinn::ServerConfig::with_crypto(Arc::new(quic_crypto));
    server_config.transport_config(Arc::new(raft_transport_config()));
    Ok(server_config)
}

/// Build a QUIC client config that skips server verification (dev/bootstrap mode).
///
/// Production clusters use mTLS via [`nexar::transport::tls::make_client_config_mtls`].
pub fn make_raft_client_config() -> Result<quinn::ClientConfig> {
    let provider = rustls::crypto::ring::default_provider();
    let mut tls_config = rustls::ClientConfig::builder_with_provider(Arc::new(provider))
        .with_safe_default_protocol_versions()
        .map_err(|e| ClusterError::Transport {
            detail: format!("client TLS protocol versions: {e}"),
        })?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(SkipServerVerification))
        .with_no_client_auth();

    tls_config.alpn_protocols = vec![ALPN_NODEDB_RAFT.to_vec()];

    let quic_crypto = quinn::crypto::rustls::QuicClientConfig::try_from(Arc::new(tls_config))
        .map_err(|e| ClusterError::Transport {
            detail: format!("QUIC client config: {e}"),
        })?;

    let mut client_config = quinn::ClientConfig::new(Arc::new(quic_crypto));
    client_config.transport_config(Arc::new(raft_transport_config()));
    Ok(client_config)
}

/// TLS credentials for a node (used for mTLS in production).
pub struct TlsCredentials {
    pub cert: rustls::pki_types::CertificateDer<'static>,
    pub key: rustls::pki_types::PrivateKeyDer<'static>,
    pub ca_cert: rustls::pki_types::CertificateDer<'static>,
}

/// Build a QUIC server config with mutual TLS (production mode).
///
/// Requires connecting clients to present a certificate signed by the cluster CA.
pub fn make_raft_server_config_mtls(creds: &TlsCredentials) -> Result<quinn::ServerConfig> {
    let mut root_store = rustls::RootCertStore::empty();
    root_store
        .add(creds.ca_cert.clone())
        .map_err(|e| ClusterError::Transport {
            detail: format!("add CA to root store: {e}"),
        })?;

    let client_verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(root_store))
        .build()
        .map_err(|e| ClusterError::Transport {
            detail: format!("build client verifier: {e}"),
        })?;

    let provider = rustls::crypto::ring::default_provider();
    let mut tls_config = rustls::ServerConfig::builder_with_provider(Arc::new(provider))
        .with_safe_default_protocol_versions()
        .map_err(|e| ClusterError::Transport {
            detail: format!("server TLS protocol versions: {e}"),
        })?
        .with_client_cert_verifier(client_verifier)
        .with_single_cert(vec![creds.cert.clone()], creds.key.clone_key())
        .map_err(|e| ClusterError::Transport {
            detail: format!("mTLS server config: {e}"),
        })?;

    tls_config.alpn_protocols = vec![ALPN_NODEDB_RAFT.to_vec()];

    let quic_crypto = quinn::crypto::rustls::QuicServerConfig::try_from(Arc::new(tls_config))
        .map_err(|e| ClusterError::Transport {
            detail: format!("QUIC mTLS server config: {e}"),
        })?;

    let mut server_config = quinn::ServerConfig::with_crypto(Arc::new(quic_crypto));
    server_config.transport_config(Arc::new(raft_transport_config()));
    Ok(server_config)
}

/// Build a QUIC client config with mutual TLS (production mode).
///
/// Verifies server cert and presents client cert, both signed by cluster CA.
pub fn make_raft_client_config_mtls(creds: &TlsCredentials) -> Result<quinn::ClientConfig> {
    let mut root_store = rustls::RootCertStore::empty();
    root_store
        .add(creds.ca_cert.clone())
        .map_err(|e| ClusterError::Transport {
            detail: format!("add CA to root store: {e}"),
        })?;

    let provider = rustls::crypto::ring::default_provider();
    let mut tls_config = rustls::ClientConfig::builder_with_provider(Arc::new(provider))
        .with_safe_default_protocol_versions()
        .map_err(|e| ClusterError::Transport {
            detail: format!("client TLS protocol versions: {e}"),
        })?
        .with_root_certificates(root_store)
        .with_client_auth_cert(vec![creds.cert.clone()], creds.key.clone_key())
        .map_err(|e| ClusterError::Transport {
            detail: format!("mTLS client config: {e}"),
        })?;

    tls_config.alpn_protocols = vec![ALPN_NODEDB_RAFT.to_vec()];

    let quic_crypto = quinn::crypto::rustls::QuicClientConfig::try_from(Arc::new(tls_config))
        .map_err(|e| ClusterError::Transport {
            detail: format!("QUIC mTLS client config: {e}"),
        })?;

    let mut client_config = quinn::ClientConfig::new(Arc::new(quic_crypto));
    client_config.transport_config(Arc::new(raft_transport_config()));
    Ok(client_config)
}

/// Generate a cluster CA and issue a node certificate.
///
/// Called during bootstrap. The CA cert is stored in the catalog and
/// distributed to joining nodes via the JoinResponse.
pub fn generate_node_credentials(
    node_san: &str,
) -> Result<(nexar::transport::tls::ClusterCa, TlsCredentials)> {
    let ca = nexar::transport::tls::ClusterCa::generate().map_err(|e| ClusterError::Transport {
        detail: format!("generate cluster CA: {e}"),
    })?;
    let ca_cert = ca.cert_der();
    let (cert, key) = ca
        .issue_cert(node_san)
        .map_err(|e| ClusterError::Transport {
            detail: format!("issue node cert: {e}"),
        })?;
    Ok((ca, TlsCredentials { cert, key, ca_cert }))
}

/// Certificate verifier that accepts any server certificate (dev/bootstrap only).
#[derive(Debug)]
struct SkipServerVerification;

impl rustls::client::danger::ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> std::result::Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::CryptoProvider::get_default()
            .map(|p| p.signature_verification_algorithms.supported_schemes())
            .unwrap_or_else(|| {
                rustls::crypto::ring::default_provider()
                    .signature_verification_algorithms
                    .supported_schemes()
            })
    }
}
