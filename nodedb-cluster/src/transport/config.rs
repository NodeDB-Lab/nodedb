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

    let mut tls_config = rustls::ServerConfig::builder()
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
    let mut tls_config = rustls::ClientConfig::builder()
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
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}
