//! mTLS configuration for inter-node and client transport.
//!
//! TDD §14.1: Inter-node transport MUST use mTLS.
//! TDD §14.2: Encryption in transit: TLS 1.3 minimum.
//! Keys MUST be rotatable without full-cluster downtime.

use std::path::PathBuf;

/// TLS configuration for a node.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TlsConfig {
    /// Path to the node's X.509 certificate (PEM).
    pub cert_path: PathBuf,
    /// Path to the node's private key (PEM).
    pub key_path: PathBuf,
    /// Path to the CA certificate chain for verifying peers (PEM).
    pub ca_cert_path: PathBuf,
    /// Minimum TLS version (default: TLS 1.3).
    pub min_tls_version: TlsVersion,
    /// Whether to require client certificates (mTLS). Default: true for inter-node.
    pub require_client_cert: bool,
    /// Certificate reload interval (seconds). Enables hot rotation.
    pub cert_reload_interval_secs: u64,
    /// Path to CRL (Certificate Revocation List) file in PEM format.
    /// When set, revoked certificates will be rejected during mTLS handshake.
    #[serde(default)]
    pub crl_path: Option<PathBuf>,
}

/// Supported TLS versions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum TlsVersion {
    Tls12,
    Tls13,
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            cert_path: PathBuf::from("/etc/nodedb/tls/node.crt"),
            key_path: PathBuf::from("/etc/nodedb/tls/node.key"),
            ca_cert_path: PathBuf::from("/etc/nodedb/tls/ca.crt"),
            min_tls_version: TlsVersion::Tls13,
            require_client_cert: true,
            cert_reload_interval_secs: 3600,
            crl_path: None,
        }
    }
}

/// Validated TLS identity extracted from a verified mTLS connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerIdentity {
    /// Common Name (CN) from the peer's certificate.
    pub common_name: String,
    /// Subject Alternative Names (SANs).
    pub san_dns: Vec<String>,
    /// Whether this is an inter-node peer (vs. client).
    pub is_node: bool,
}

/// Result of certificate validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CertValidation {
    /// Certificate is valid and trusted.
    Valid(PeerIdentity),
    /// Certificate is expired.
    Expired { cn: String, expired_at: String },
    /// Certificate is not signed by a trusted CA.
    UntrustedCa,
    /// No client certificate presented (required for mTLS).
    NoCertificate,
    /// Certificate is revoked.
    Revoked { cn: String },
}

impl CertValidation {
    pub fn is_valid(&self) -> bool {
        matches!(self, CertValidation::Valid(_))
    }

    pub fn identity(&self) -> Option<&PeerIdentity> {
        match self {
            CertValidation::Valid(id) => Some(id),
            _ => None,
        }
    }
}

/// Certificate rotation state.
#[derive(Debug, Clone)]
pub struct CertRotationState {
    /// When the current certificate was loaded.
    pub loaded_at_us: u64,
    /// Serial number of the current certificate.
    pub serial: String,
    /// Number of successful rotations.
    pub rotations: u64,
    /// Last rotation error, if any.
    pub last_error: Option<String>,
}

impl CertRotationState {
    pub fn new(serial: String) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;
        Self {
            loaded_at_us: now,
            serial,
            rotations: 0,
            last_error: None,
        }
    }

    /// Record a successful rotation.
    pub fn rotated(&mut self, new_serial: String) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;
        self.loaded_at_us = now;
        self.serial = new_serial;
        self.rotations += 1;
        self.last_error = None;
    }

    /// Record a rotation failure.
    pub fn rotation_failed(&mut self, error: String) {
        self.last_error = Some(error);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tls_config_defaults() {
        let cfg = TlsConfig::default();
        assert_eq!(cfg.min_tls_version, TlsVersion::Tls13);
        assert!(cfg.require_client_cert);
        assert_eq!(cfg.cert_reload_interval_secs, 3600);
    }

    #[test]
    fn cert_validation_states() {
        let valid = CertValidation::Valid(PeerIdentity {
            common_name: "node-1.nodedb.local".into(),
            san_dns: vec!["node-1.nodedb.local".into()],
            is_node: true,
        });
        assert!(valid.is_valid());
        assert_eq!(valid.identity().unwrap().common_name, "node-1.nodedb.local");

        let expired = CertValidation::Expired {
            cn: "node-2".into(),
            expired_at: "2024-01-01".into(),
        };
        assert!(!expired.is_valid());
        assert!(expired.identity().is_none());

        assert!(!CertValidation::UntrustedCa.is_valid());
        assert!(!CertValidation::NoCertificate.is_valid());
    }

    #[test]
    fn cert_rotation_lifecycle() {
        let mut state = CertRotationState::new("SERIAL-001".into());
        assert_eq!(state.rotations, 0);
        assert!(state.last_error.is_none());

        state.rotated("SERIAL-002".into());
        assert_eq!(state.rotations, 1);
        assert_eq!(state.serial, "SERIAL-002");

        state.rotation_failed("file not found".into());
        assert_eq!(state.last_error.as_deref(), Some("file not found"));

        state.rotated("SERIAL-003".into());
        assert_eq!(state.rotations, 2);
        assert!(state.last_error.is_none());
    }

    #[test]
    fn peer_identity_node_vs_client() {
        let node = PeerIdentity {
            common_name: "node-1".into(),
            san_dns: vec![],
            is_node: true,
        };
        assert!(node.is_node);

        let client = PeerIdentity {
            common_name: "app-client".into(),
            san_dns: vec![],
            is_node: false,
        };
        assert!(!client.is_node);
    }
}
