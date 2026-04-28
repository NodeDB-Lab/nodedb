//! Authenticated join bundle: wraps the raw cred bytes sent to a joiner
//! with an HMAC-SHA256 MAC so a MitM cannot substitute their own CA even
//! if they intercept the token.
//!
//! MAC key derivation: `HMAC-SHA256(cluster_secret XOR token_hash)` over
//! the bundle bytes. This binds the bundle to both the cluster secret and
//! the specific token, so a replayed bundle from a different session or a
//! bundle signed under a different secret is rejected.
//!
//! The joiner calls [`verify_bundle`] before installing any cert material.

use hmac::{Hmac, Mac};
use sha2::Sha256;

use crate::wire_version::WireVersion;

/// Authenticated wrapper around the raw join bundle bytes.
#[derive(Debug, Clone)]
pub struct AuthenticatedJoinBundle {
    /// Wire protocol version (currently [`WireVersion::CURRENT`]).
    pub version: WireVersion,
    /// Serialised bundle bytes (MessagePack of `BootstrapCredsResponse`
    /// or any opaque byte payload the caller provides).
    pub bundle: Vec<u8>,
    /// HMAC-SHA256 of `bundle` keyed on `derive_mac_key(cluster_secret, token_hash)`.
    pub mac: [u8; 32],
}

/// Error from bundle operations.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BundleError {
    #[error("bundle MAC verification failed")]
    MacMismatch,
    #[error("hmac key length invalid")]
    HmacKeyLength,
    #[error("bundle version mismatch: expected {expected:?}, got {got:?}")]
    VersionMismatch {
        expected: WireVersion,
        got: WireVersion,
    },
}

/// Derive the MAC key as `cluster_secret XOR token_hash`.
///
/// Both inputs are exactly 32 bytes. The XOR ensures neither value alone
/// is sufficient to forge a MAC — an attacker who has the cluster secret
/// but not the token (or vice versa) cannot produce a valid MAC.
pub fn derive_mac_key(cluster_secret: &[u8; 32], token_hash: &[u8; 32]) -> [u8; 32] {
    let mut key = [0u8; 32];
    for i in 0..32 {
        key[i] = cluster_secret[i] ^ token_hash[i];
    }
    key
}

/// Wrap `bundle_bytes` in an `AuthenticatedJoinBundle`.
pub fn seal_bundle(
    bundle_bytes: Vec<u8>,
    cluster_secret: &[u8; 32],
    token_hash: &[u8; 32],
) -> Result<AuthenticatedJoinBundle, BundleError> {
    let key = derive_mac_key(cluster_secret, token_hash);
    let mut mac = <Hmac<Sha256>>::new_from_slice(&key).map_err(|_| BundleError::HmacKeyLength)?;
    mac.update(&bundle_bytes);
    let tag: [u8; 32] = mac.finalize().into_bytes().into();
    Ok(AuthenticatedJoinBundle {
        version: WireVersion::CURRENT,
        bundle: bundle_bytes,
        mac: tag,
    })
}

/// Verify the MAC on `sealed` and return the inner bundle bytes.
///
/// The comparison is constant-time via `hmac::Mac::verify_slice`.
pub fn open_bundle<'a>(
    sealed: &'a AuthenticatedJoinBundle,
    cluster_secret: &[u8; 32],
    token_hash: &[u8; 32],
) -> Result<&'a [u8], BundleError> {
    if sealed.version != WireVersion::CURRENT {
        return Err(BundleError::VersionMismatch {
            expected: WireVersion::CURRENT,
            got: sealed.version,
        });
    }
    let key = derive_mac_key(cluster_secret, token_hash);
    let mut mac = <Hmac<Sha256>>::new_from_slice(&key).map_err(|_| BundleError::HmacKeyLength)?;
    mac.update(&sealed.bundle);
    mac.verify_slice(&sealed.mac)
        .map_err(|_| BundleError::MacMismatch)?;
    Ok(&sealed.bundle)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seal_and_open_roundtrip() {
        let secret = [0x1Au8; 32];
        let token_hash = [0x2Bu8; 32];
        let payload = b"hello cluster".to_vec();
        let sealed = seal_bundle(payload.clone(), &secret, &token_hash).unwrap();
        let out = open_bundle(&sealed, &secret, &token_hash).unwrap();
        assert_eq!(out, payload.as_slice());
    }

    #[test]
    fn tampered_bundle_rejected() {
        let secret = [0x3Cu8; 32];
        let token_hash = [0x4Du8; 32];
        let mut sealed = seal_bundle(b"real payload".to_vec(), &secret, &token_hash).unwrap();
        // Flip first byte of bundle
        sealed.bundle[0] ^= 0xFF;
        assert_eq!(
            open_bundle(&sealed, &secret, &token_hash).unwrap_err(),
            BundleError::MacMismatch
        );
    }

    #[test]
    fn wrong_cluster_secret_rejected() {
        let secret = [0x5Eu8; 32];
        let wrong = [0x6Fu8; 32];
        let token_hash = [0x70u8; 32];
        let sealed = seal_bundle(b"payload".to_vec(), &secret, &token_hash).unwrap();
        assert_eq!(
            open_bundle(&sealed, &wrong, &token_hash).unwrap_err(),
            BundleError::MacMismatch
        );
    }

    #[test]
    fn wrong_token_hash_rejected() {
        let secret = [0x81u8; 32];
        let token_hash = [0x92u8; 32];
        let wrong_hash = [0xA3u8; 32];
        let sealed = seal_bundle(b"payload".to_vec(), &secret, &token_hash).unwrap();
        assert_eq!(
            open_bundle(&sealed, &secret, &wrong_hash).unwrap_err(),
            BundleError::MacMismatch
        );
    }

    #[test]
    fn version_mismatch_rejected() {
        let secret = [0xB4u8; 32];
        let token_hash = [0xC5u8; 32];
        let mut sealed = seal_bundle(b"payload".to_vec(), &secret, &token_hash).unwrap();
        sealed.version = WireVersion::V1;
        assert!(matches!(
            open_bundle(&sealed, &secret, &token_hash).unwrap_err(),
            BundleError::VersionMismatch { .. }
        ));
    }

    #[test]
    fn derive_mac_key_xors_both_inputs() {
        let secret = [0xFFu8; 32];
        let hash = [0x0Fu8; 32];
        let key = derive_mac_key(&secret, &hash);
        // 0xFF ^ 0x0F = 0xF0
        assert!(key.iter().all(|&b| b == 0xF0));
    }
}
