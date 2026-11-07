// SPDX-License-Identifier: BUSL-1.1

//! Who can open a mirror link: mTLS with a pinned client key.
//!
//! The source trusts one CA for mirror clients, and on top of that pins each
//! client's key by name in the `[mirror.clients]` allowlist. A certificate the
//! CA signed for any other key fails the TLS handshake, so an unlisted client
//! reaches no stream, and no log read can run for it.
//!
//! This listener has no insecure mode. The allowlist names a client by its key,
//! so a connection with no certificate has no identity to check.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tracing::warn;

use crate::error::{ClusterError, Result};
use crate::transport::config::{TlsCredentials, ensure_rustls_crypto_provider};
use crate::transport::peer_identity_verifier::spki_pin_from_cert_der;

/// ALPN of the mirror link. It keeps a raft client off this listener, and a
/// mirror client off the raft listener.
pub const ALPN_NODEDB_MIRROR: &[u8] = b"nodedb-mirror/1";

/// A connection with nothing to do closes after this. A client's keep-alive
/// spans its long polls.
const IDLE_TIMEOUT: Duration = Duration::from_secs(60);
/// How often a client proves it lives.
const KEEP_ALIVE: Duration = Duration::from_secs(15);
/// How many requests one client runs at a time: one per group it tails, plus
/// an ack and a hello.
const MAX_BIDI_STREAMS: u32 = 256;

/// One allowed client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MirrorClient {
    /// The client's name in `[mirror.clients]`.
    pub name: String,
    /// SHA-256 of the client's SubjectPublicKeyInfo.
    pub spki: [u8; 32],
    /// The source databases the client can follow, by name.
    pub databases: Vec<String>,
}

impl MirrorClient {
    /// Whether the client can follow `database`.
    pub fn allows(&self, database: &str) -> bool {
        self.databases.iter().any(|allowed| allowed == database)
    }
}

/// Every allowed client, by key.
#[derive(Debug, Clone, Default)]
pub struct MirrorAllowlist {
    by_spki: HashMap<[u8; 32], Arc<MirrorClient>>,
}

impl MirrorAllowlist {
    pub fn new(clients: impl IntoIterator<Item = MirrorClient>) -> Self {
        Self {
            by_spki: clients
                .into_iter()
                .map(|client| (client.spki, Arc::new(client)))
                .collect(),
        }
    }

    /// The client whose leaf certificate is `cert_der`. A certificate that
    /// does not parse names no client.
    pub fn client_of(&self, cert_der: &[u8]) -> Option<Arc<MirrorClient>> {
        let spki = spki_pin_from_cert_der(cert_der).ok()?;
        self.by_spki.get(&spki).cloned()
    }

    /// Whether the allowlist names nobody.
    pub fn is_empty(&self) -> bool {
        self.by_spki.is_empty()
    }
}

/// The source's client verifier: the CA's chain check first, then the key pin.
struct AllowlistVerifier {
    inner: Arc<dyn rustls::server::danger::ClientCertVerifier>,
    allowlist: Arc<MirrorAllowlist>,
}

impl std::fmt::Debug for AllowlistVerifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AllowlistVerifier").finish_non_exhaustive()
    }
}

impl rustls::server::danger::ClientCertVerifier for AllowlistVerifier {
    fn root_hint_subjects(&self) -> &[rustls::DistinguishedName] {
        self.inner.root_hint_subjects()
    }

    fn verify_client_cert(
        &self,
        end_entity: &rustls::pki_types::CertificateDer<'_>,
        intermediates: &[rustls::pki_types::CertificateDer<'_>],
        now: rustls::pki_types::UnixTime,
    ) -> std::result::Result<rustls::server::danger::ClientCertVerified, rustls::Error> {
        let verified = self
            .inner
            .verify_client_cert(end_entity, intermediates, now)?;
        if self.allowlist.client_of(end_entity.as_ref()).is_none() {
            warn!("mirror listener: a client certificate's key is not in [mirror.clients]");
            return Err(rustls::Error::InvalidCertificate(
                rustls::CertificateError::ApplicationVerificationFailure,
            ));
        }
        Ok(verified)
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}

fn config_error(step: &str) -> impl FnOnce(String) -> ClusterError + '_ {
    move |e| ClusterError::Transport {
        detail: format!("mirror TLS config: {step}: {e}"),
    }
}

/// The trust store holding `ca` alone: this link's peer CA, never the cluster's
/// own by default.
fn roots_of(ca: &rustls::pki_types::CertificateDer<'static>) -> Result<rustls::RootCertStore> {
    let mut roots = rustls::RootCertStore::empty();
    roots
        .add(ca.clone())
        .map_err(|e| config_error("add the peer CA")(e.to_string()))?;
    Ok(roots)
}

fn transport_config(keep_alive: Option<Duration>) -> Result<quinn::TransportConfig> {
    let idle = quinn::IdleTimeout::try_from(IDLE_TIMEOUT)
        .map_err(|e| config_error("idle timeout")(e.to_string()))?;
    let mut config = quinn::TransportConfig::default();
    config.max_concurrent_bidi_streams(quinn::VarInt::from_u32(MAX_BIDI_STREAMS));
    config.max_concurrent_uni_streams(quinn::VarInt::from_u32(0));
    config.max_idle_timeout(Some(idle));
    config.keep_alive_interval(keep_alive);
    Ok(config)
}

/// The source's QUIC config: `creds` names this node, `creds.ca_cert` is the
/// CA of mirror clients, and `allowlist` pins each client's key.
pub fn make_mirror_server_config(
    creds: &TlsCredentials,
    allowlist: Arc<MirrorAllowlist>,
) -> Result<quinn::ServerConfig> {
    ensure_rustls_crypto_provider();
    let inner = rustls::server::WebPkiClientVerifier::builder(Arc::new(roots_of(&creds.ca_cert)?))
        .build()
        .map_err(|e| config_error("client verifier")(e.to_string()))?;
    let verifier = Arc::new(AllowlistVerifier { inner, allowlist });
    let provider = rustls::crypto::ring::default_provider();
    let mut tls = rustls::ServerConfig::builder_with_provider(Arc::new(provider))
        .with_safe_default_protocol_versions()
        .map_err(|e| config_error("protocol versions")(e.to_string()))?
        .with_client_cert_verifier(verifier)
        .with_single_cert(vec![creds.cert.clone()], creds.key.clone_key())
        .map_err(|e| config_error("server certificate")(e.to_string()))?;
    tls.alpn_protocols = vec![ALPN_NODEDB_MIRROR.to_vec()];
    let crypto = quinn::crypto::rustls::QuicServerConfig::try_from(Arc::new(tls))
        .map_err(|e| config_error("QUIC server crypto")(e.to_string()))?;
    let mut config = quinn::ServerConfig::with_crypto(Arc::new(crypto));
    config.transport_config(Arc::new(transport_config(None)?));
    Ok(config)
}

/// The mirror's QUIC config: `creds` names this client, and `creds.ca_cert` is
/// the source's CA, pinned per source.
pub fn make_mirror_client_config(creds: &TlsCredentials) -> Result<quinn::ClientConfig> {
    ensure_rustls_crypto_provider();
    let provider = rustls::crypto::ring::default_provider();
    let mut tls = rustls::ClientConfig::builder_with_provider(Arc::new(provider))
        .with_safe_default_protocol_versions()
        .map_err(|e| config_error("protocol versions")(e.to_string()))?
        .with_root_certificates(roots_of(&creds.ca_cert)?)
        .with_client_auth_cert(vec![creds.cert.clone()], creds.key.clone_key())
        .map_err(|e| config_error("client certificate")(e.to_string()))?;
    tls.alpn_protocols = vec![ALPN_NODEDB_MIRROR.to_vec()];
    let crypto = quinn::crypto::rustls::QuicClientConfig::try_from(Arc::new(tls))
        .map_err(|e| config_error("QUIC client crypto")(e.to_string()))?;
    let mut config = quinn::ClientConfig::new(Arc::new(crypto));
    config.transport_config(Arc::new(transport_config(Some(KEEP_ALIVE))?));
    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::config::generate_node_credentials;

    fn client(spki: [u8; 32]) -> MirrorClient {
        MirrorClient {
            name: "dr".to_string(),
            spki,
            databases: vec!["orders".to_string()],
        }
    }

    #[test]
    fn a_listed_key_names_its_client_and_any_other_names_nobody() {
        let (_ca, listed) = generate_node_credentials("listed").expect("creds");
        let (_other_ca, other) = generate_node_credentials("other").expect("creds");
        let allowlist = MirrorAllowlist::new([client(listed.spki_pin)]);
        let named = allowlist.client_of(listed.cert.as_ref()).expect("listed");
        assert_eq!(named.name, "dr");
        assert!(allowlist.client_of(other.cert.as_ref()).is_none());
        assert!(allowlist.client_of(b"not a certificate").is_none());
    }

    #[test]
    fn a_client_follows_only_its_named_databases() {
        let entry = client([0; 32]);
        assert!(entry.allows("orders"));
        assert!(!entry.allows("orders2"));
        assert!(!entry.allows(""));
    }
}
