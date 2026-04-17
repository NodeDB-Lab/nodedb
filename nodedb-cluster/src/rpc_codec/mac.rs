//! Symmetric MAC key + HMAC-SHA256 primitive for the authenticated Raft
//! envelope.
//!
//! # Trust model
//!
//! The MAC key is a cluster-wide 32-byte shared secret generated at
//! bootstrap and distributed to joining nodes out of band (via the mTLS
//! join RPC, L.4). Every legitimate cluster member holds the same key;
//! outside parties do not.
//!
//! # What the MAC buys
//!
//! Frame replay protection: a frame captured off the wire (or replayed
//! within a compromised TLS session, or across sessions that share the
//! same transport identity) cannot be modified or re-sent by a party
//! that lacks the cluster key. Combined with the per-peer monotonic
//! sequence in the envelope, every frame is consumed at most once.
//!
//! # What the MAC does NOT buy
//!
//! If a node's credentials leak wholesale (its mTLS key **and** its
//! copy of the cluster secret), the attacker can impersonate that node
//! in full. The MAC is defence-in-depth, not a substitute for
//! compromising a node.

use hmac::{Hmac, Mac};
use sha2::Sha256;

use crate::error::{ClusterError, Result};

/// Length of the MAC tag in bytes. HMAC-SHA256 produces 32 bytes.
pub const MAC_LEN: usize = 32;

/// Cluster-wide symmetric MAC key.
///
/// The key is 32 bytes — HMAC-SHA256's natural block-equivalent input.
/// All legitimate cluster members hold the same value.
#[derive(Clone)]
pub struct MacKey([u8; MAC_LEN]);

impl MacKey {
    /// Construct from raw bytes.
    pub fn from_bytes(bytes: [u8; MAC_LEN]) -> Self {
        Self(bytes)
    }

    /// A cryptographically random fresh key. Use at cluster bootstrap.
    pub fn random() -> Self {
        use rand::RngCore;
        let mut out = [0u8; MAC_LEN];
        rand::rng().fill_bytes(&mut out);
        Self(out)
    }

    /// All-zero sentinel key used only by the `Insecure` transport mode.
    /// When the key is zero the MAC verification is decorative — the
    /// insecure mode already trusts any network peer.
    pub fn zero() -> Self {
        Self([0u8; MAC_LEN])
    }

    /// Raw bytes. Used for persistence only; callers must treat the
    /// return value as key material.
    pub fn as_bytes(&self) -> &[u8; MAC_LEN] {
        &self.0
    }

    /// Whether this key is the all-zero sentinel. Insecure transports
    /// skip replay-detection telemetry accordingly.
    pub fn is_zero(&self) -> bool {
        self.0 == [0u8; MAC_LEN]
    }
}

/// Redacted `Debug` — never print key bytes.
impl std::fmt::Debug for MacKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_zero() {
            write!(f, "MacKey(zero)")
        } else {
            write!(f, "MacKey(<redacted>)")
        }
    }
}

/// Compute HMAC-SHA256 over `data` with `key`. Length is fixed at
/// [`MAC_LEN`].
pub fn compute_hmac(key: &MacKey, data: &[u8]) -> [u8; MAC_LEN] {
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(key.as_bytes())
        .expect("HMAC-SHA256 accepts any key length");
    mac.update(data);
    let out = mac.finalize().into_bytes();
    let mut tag = [0u8; MAC_LEN];
    tag.copy_from_slice(&out);
    tag
}

/// Constant-time verify `tag` against HMAC-SHA256 over `data` with `key`.
/// Returns `Err` on mismatch.
pub fn verify_hmac(key: &MacKey, data: &[u8], tag: &[u8; MAC_LEN]) -> Result<()> {
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(key.as_bytes())
        .expect("HMAC-SHA256 accepts any key length");
    mac.update(data);
    mac.verify_slice(tag).map_err(|_| ClusterError::Codec {
        detail: "frame MAC verification failed".into(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hmac_roundtrip() {
        let key = MacKey::from_bytes([7u8; MAC_LEN]);
        let tag = compute_hmac(&key, b"hello world");
        verify_hmac(&key, b"hello world", &tag).unwrap();
    }

    #[test]
    fn hmac_rejects_tampered_data() {
        let key = MacKey::from_bytes([7u8; MAC_LEN]);
        let tag = compute_hmac(&key, b"hello world");
        let err = verify_hmac(&key, b"hello WORLD", &tag).unwrap_err();
        assert!(err.to_string().contains("MAC verification failed"));
    }

    #[test]
    fn hmac_rejects_wrong_key() {
        let k1 = MacKey::from_bytes([1u8; MAC_LEN]);
        let k2 = MacKey::from_bytes([2u8; MAC_LEN]);
        let tag = compute_hmac(&k1, b"msg");
        assert!(verify_hmac(&k2, b"msg", &tag).is_err());
    }

    #[test]
    fn debug_redacts_key() {
        let k = MacKey::from_bytes([0xAA; MAC_LEN]);
        let s = format!("{k:?}");
        assert!(!s.contains("aa"), "debug leaked key bytes: {s}");
        assert!(s.contains("redacted"));
    }

    #[test]
    fn random_keys_differ() {
        let k1 = MacKey::random();
        let k2 = MacKey::random();
        assert_ne!(k1.as_bytes(), k2.as_bytes());
    }

    #[test]
    fn zero_key_reports_zero() {
        assert!(MacKey::zero().is_zero());
        assert!(!MacKey::random().is_zero());
    }
}
