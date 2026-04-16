//! WAL payload encryption using AES-256-GCM.
//!
//! Design:
//! - Header stays plaintext (needed for recovery scanning — magic, lsn, tenant_id)
//! - Payload is encrypted before CRC computation
//! - CRC covers the ciphertext (detects corruption of encrypted data)
//! - Nonce = `[4-byte random epoch][8-byte LSN]` — epoch is generated per WAL
//!   lifetime to prevent nonce reuse after snapshot restore or WAL truncation
//! - Additional Authenticated Data (AAD) = header bytes (binds ciphertext to its header)
//!
//! On-disk format for encrypted payload:
//! ```text
//! [header(30B plaintext)] [ciphertext(payload_len bytes)] [auth_tag(16B)]
//! ```
//! `payload_len` includes the 16-byte auth tag.

use aes_gcm::Aes256Gcm;
use aes_gcm::aead::{Aead, KeyInit};

use crate::error::{Result, WalError};
use crate::record::HEADER_SIZE;

/// AES-256-GCM key with a random per-lifetime epoch for nonce disambiguation.
///
/// The epoch is generated randomly at construction time. Each WAL lifetime
/// (process start, snapshot restore, segment creation) gets a fresh epoch,
/// ensuring that nonces are never reused even if LSNs restart from 1.
#[derive(Clone)]
pub struct WalEncryptionKey {
    cipher: Aes256Gcm,
    /// Random 4-byte epoch: occupies the high 4 bytes of the 12-byte nonce.
    /// Disambiguates nonces across WAL lifetimes with the same key.
    epoch: [u8; 4],
}

impl WalEncryptionKey {
    /// Create from a 32-byte key with a fresh random epoch.
    pub fn from_bytes(key: &[u8; 32]) -> Self {
        let mut epoch = [0u8; 4];
        getrandom::fill(&mut epoch).expect("getrandom failed");
        Self {
            cipher: Aes256Gcm::new(key.into()),
            epoch,
        }
    }

    /// Load key from a file (must contain exactly 32 bytes).
    pub fn from_file(path: &std::path::Path) -> Result<Self> {
        let key_bytes = std::fs::read(path).map_err(WalError::Io)?;
        if key_bytes.len() != 32 {
            return Err(WalError::EncryptionError {
                detail: format!(
                    "encryption key must be exactly 32 bytes, got {}",
                    key_bytes.len()
                ),
            });
        }
        let mut key = [0u8; 32];
        key.copy_from_slice(&key_bytes);
        Ok(Self::from_bytes(&key))
    }

    /// Encrypt a payload. Returns ciphertext + auth_tag (16 bytes appended).
    ///
    /// - `lsn`: used to derive a deterministic nonce
    /// - `header_bytes`: used as AAD (additional authenticated data)
    /// - `plaintext`: the payload to encrypt
    pub fn encrypt(
        &self,
        lsn: u64,
        header_bytes: &[u8; HEADER_SIZE],
        plaintext: &[u8],
    ) -> Result<Vec<u8>> {
        let nonce = lsn_to_nonce(&self.epoch, lsn);
        self.cipher
            .encrypt(
                &nonce,
                aes_gcm::aead::Payload {
                    msg: plaintext,
                    aad: header_bytes,
                },
            )
            .map_err(|_| WalError::EncryptionError {
                detail: "AES-256-GCM encryption failed".into(),
            })
    }

    /// The random epoch for this key instance.
    pub fn epoch(&self) -> &[u8; 4] {
        &self.epoch
    }

    /// Decrypt a payload. Input is ciphertext + auth_tag (16 bytes at end).
    ///
    /// - `epoch`: the epoch that was used during encryption (from the segment header)
    /// - `lsn`: must match the LSN used during encryption
    /// - `header_bytes`: must match the header used during encryption (AAD)
    /// - `ciphertext`: the encrypted payload (includes 16-byte auth tag)
    pub fn decrypt(
        &self,
        epoch: &[u8; 4],
        lsn: u64,
        header_bytes: &[u8; HEADER_SIZE],
        ciphertext: &[u8],
    ) -> Result<Vec<u8>> {
        let nonce = lsn_to_nonce(epoch, lsn);
        self.cipher
            .decrypt(
                &nonce,
                aes_gcm::aead::Payload {
                    msg: ciphertext,
                    aad: header_bytes,
                },
            )
            .map_err(|_| WalError::EncryptionError {
                detail: "AES-256-GCM decryption failed (corrupted or wrong key)".into(),
            })
    }
}

/// Key ring supporting dual-key reads for seamless key rotation.
///
/// During rotation: new writes use the current key, reads try current
/// then fall back to previous. Once all old data is re-encrypted,
/// the previous key is removed.
#[derive(Clone)]
pub struct KeyRing {
    current: WalEncryptionKey,
    previous: Option<WalEncryptionKey>,
}

impl KeyRing {
    /// Create a key ring with only the current key.
    pub fn new(current: WalEncryptionKey) -> Self {
        Self {
            current,
            previous: None,
        }
    }

    /// Create a key ring with current + previous key (for rotation).
    pub fn with_previous(current: WalEncryptionKey, previous: WalEncryptionKey) -> Self {
        Self {
            current,
            previous: Some(previous),
        }
    }

    /// Encrypt using the current key.
    pub fn encrypt(
        &self,
        lsn: u64,
        header_bytes: &[u8; HEADER_SIZE],
        plaintext: &[u8],
    ) -> Result<Vec<u8>> {
        self.current.encrypt(lsn, header_bytes, plaintext)
    }

    /// Decrypt: try current key first, then previous (if set).
    ///
    /// `epoch` is the encryption epoch stored in the WAL segment header.
    /// This enables seamless key rotation — old data encrypted with the
    /// previous key can still be read while new data uses the current key.
    pub fn decrypt(
        &self,
        epoch: &[u8; 4],
        lsn: u64,
        header_bytes: &[u8; HEADER_SIZE],
        ciphertext: &[u8],
    ) -> Result<Vec<u8>> {
        match (
            self.current.decrypt(epoch, lsn, header_bytes, ciphertext),
            self.previous.as_ref(),
        ) {
            (Ok(plaintext), _) => Ok(plaintext),
            (Err(_), Some(prev)) => prev.decrypt(epoch, lsn, header_bytes, ciphertext),
            (Err(e), None) => Err(e),
        }
    }

    /// Get the current key (for encryption operations).
    pub fn current(&self) -> &WalEncryptionKey {
        &self.current
    }

    /// Whether a previous key is present (rotation in progress).
    pub fn has_previous(&self) -> bool {
        self.previous.is_some()
    }

    /// Remove the previous key (rotation complete).
    pub fn clear_previous(&mut self) {
        self.previous = None;
    }
}

/// AES-256-GCM auth tag size in bytes.
pub const AUTH_TAG_SIZE: usize = 16;

/// Derive a 12-byte nonce from an epoch and LSN.
///
/// AES-256-GCM requires a 96-bit (12 byte) nonce that must never repeat
/// for the same key. Layout: `[4-byte random epoch][8-byte LSN]`.
/// The epoch is generated randomly per WAL lifetime, so even if LSNs
/// restart from 1 after a snapshot restore, the nonces remain unique.
fn lsn_to_nonce(epoch: &[u8; 4], lsn: u64) -> aes_gcm::Nonce<aes_gcm::aead::consts::U12> {
    let mut nonce_bytes = [0u8; 12];
    nonce_bytes[..4].copy_from_slice(epoch);
    nonce_bytes[4..12].copy_from_slice(&lsn.to_le_bytes());
    nonce_bytes.into()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_key() -> WalEncryptionKey {
        WalEncryptionKey::from_bytes(&[0x42u8; 32])
    }

    fn test_header(lsn: u64) -> [u8; HEADER_SIZE] {
        let mut h = [0u8; HEADER_SIZE];
        h[8..16].copy_from_slice(&lsn.to_le_bytes());
        h
    }

    #[test]
    fn encrypt_decrypt_roundtrip() {
        let key = test_key();
        let epoch = *key.epoch();
        let header = test_header(1);
        let plaintext = b"hello nodedb encryption";

        let ciphertext = key.encrypt(1, &header, plaintext).unwrap();
        assert_ne!(&ciphertext[..plaintext.len()], plaintext);
        assert_eq!(ciphertext.len(), plaintext.len() + AUTH_TAG_SIZE);

        let decrypted = key.decrypt(&epoch, 1, &header, &ciphertext).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn wrong_key_fails() {
        let key1 = WalEncryptionKey::from_bytes(&[0x01; 32]);
        let epoch1 = *key1.epoch();
        let key2 = WalEncryptionKey::from_bytes(&[0x02; 32]);
        let header = test_header(1);

        let ciphertext = key1.encrypt(1, &header, b"secret").unwrap();
        assert!(key2.decrypt(&epoch1, 1, &header, &ciphertext).is_err());
    }

    #[test]
    fn wrong_lsn_fails() {
        let key = test_key();
        let epoch = *key.epoch();
        let header = test_header(1);

        let ciphertext = key.encrypt(1, &header, b"secret").unwrap();
        // Different LSN = different nonce = decryption fails.
        assert!(key.decrypt(&epoch, 2, &header, &ciphertext).is_err());
    }

    #[test]
    fn tampered_ciphertext_fails() {
        let key = test_key();
        let epoch = *key.epoch();
        let header = test_header(1);

        let mut ciphertext = key.encrypt(1, &header, b"secret").unwrap();
        ciphertext[0] ^= 0xFF;
        assert!(key.decrypt(&epoch, 1, &header, &ciphertext).is_err());
    }

    #[test]
    fn tampered_header_fails() {
        let key = test_key();
        let epoch = *key.epoch();
        let header1 = test_header(1);

        let ciphertext = key.encrypt(1, &header1, b"secret").unwrap();

        // Tamper the AAD (header).
        let mut header2 = header1;
        header2[0] = 0xFF;
        assert!(key.decrypt(&epoch, 1, &header2, &ciphertext).is_err());
    }

    #[test]
    fn empty_payload() {
        let key = test_key();
        let epoch = *key.epoch();
        let header = test_header(1);

        let ciphertext = key.encrypt(1, &header, b"").unwrap();
        assert_eq!(ciphertext.len(), AUTH_TAG_SIZE); // Just the tag.

        let decrypted = key.decrypt(&epoch, 1, &header, &ciphertext).unwrap();
        assert!(decrypted.is_empty());
    }

    #[test]
    fn different_lsns_produce_different_ciphertext() {
        let key = test_key();
        let plaintext = b"same payload";

        let ct1 = key.encrypt(1, &test_header(1), plaintext).unwrap();
        let ct2 = key.encrypt(2, &test_header(2), plaintext).unwrap();
        assert_ne!(ct1, ct2);
    }

    #[test]
    fn same_lsn_different_wal_lifetimes_produce_different_ciphertext() {
        // Simulate two WAL lifetimes: same key bytes, same LSN=1, but
        // separate WalEncryptionKey instances (each gets a fresh random epoch).
        // This models: write at LSN=1, wipe WAL, restart with same key,
        // write at LSN=1 again. The two ciphertexts must differ.
        let key_bytes = [0x42u8; 32];
        let key1 = WalEncryptionKey::from_bytes(&key_bytes);
        let key2 = WalEncryptionKey::from_bytes(&key_bytes);
        let header = test_header(1);
        let pt = b"same plaintext in two wal lifetimes";

        let ct1 = key1.encrypt(1, &header, pt).unwrap();
        let ct2 = key2.encrypt(1, &header, pt).unwrap();

        // SPEC: different WAL lifetimes (different epochs) must produce
        // different ciphertext even with the same key bytes and LSN.
        assert_ne!(
            ct1, ct2,
            "nonce reuse: same (key_bytes, lsn) must not produce identical ciphertext across WAL lifetimes"
        );
    }
}
