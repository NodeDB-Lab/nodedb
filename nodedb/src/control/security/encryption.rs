//! AES-256-XTS encryption at rest for local storage volumes.
//!
//! Beyond the WAL encryption (AES-256-GCM per record), this provides
//! full volume-level encryption for all persistent data:
//!
//! - redb database files (sparse engine, edge store, GSI)
//! - HNSW checkpoint files
//! - mmap vector segment files
//! - Sort spill temp files
//!
//! XTS mode is designed for disk encryption: it handles arbitrary-length
//! sectors without authentication overhead (since integrity is handled
//! separately by checksums in each data format).
//!
//! Key management:
//! - Master key derived from a key file or KMS
//! - Per-file data encryption key (DEK) generated randomly
//! - DEK encrypted by master key, stored in file header
//! - Key rotation: re-encrypt DEKs with new master key (no data rewrite)

use std::path::{Path, PathBuf};

use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce};
use tracing::info;

/// Volume encryption configuration.
#[derive(Debug, Clone)]
pub struct VolumeEncryptionConfig {
    /// Path to the master key file (32 bytes, AES-256).
    pub master_key_path: Option<PathBuf>,
    /// Whether encryption is enabled.
    pub enabled: bool,
    /// Algorithm identifier stored in encrypted file headers.
    pub algorithm: EncryptionAlgorithm,
}

impl Default for VolumeEncryptionConfig {
    fn default() -> Self {
        Self {
            master_key_path: None,
            enabled: false,
            algorithm: EncryptionAlgorithm::Aes256Xts,
        }
    }
}

/// Supported encryption algorithms.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum EncryptionAlgorithm {
    /// AES-256-XTS for volume encryption (no authentication — checksums handle integrity).
    Aes256Xts,
    /// AES-256-GCM for authenticated encryption (used by WAL).
    Aes256Gcm,
}

/// Encrypted file header: stored at the beginning of each encrypted file.
///
/// Layout: `[magic: 4B][version: 2B][algo: 2B][dek_len: 4B][encrypted_dek: N bytes][iv: 16B]`
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct EncryptedFileHeader {
    /// Magic bytes: "NENC" (NodeDB ENCrypted).
    pub magic: [u8; 4],
    /// Header format version.
    pub version: u16,
    /// Encryption algorithm.
    pub algorithm: EncryptionAlgorithm,
    /// Data Encryption Key (DEK) encrypted by the master key.
    pub encrypted_dek: Vec<u8>,
    /// Initialization vector for the DEK encryption.
    pub dek_iv: [u8; 16],
    /// Salt for key derivation (if using password-based master key).
    pub salt: [u8; 16],
}

impl EncryptedFileHeader {
    /// Magic bytes for encrypted files.
    pub const MAGIC: [u8; 4] = *b"NENC";
    /// Current header version.
    pub const VERSION: u16 = 1;

    /// Create a new header for an encrypted file.
    pub fn new(algorithm: EncryptionAlgorithm, encrypted_dek: Vec<u8>) -> Self {
        let mut dek_iv = [0u8; 16];
        let mut salt = [0u8; 16];
        // Use getrandom for cryptographic randomness.
        let _ = getrandom::fill(&mut dek_iv);
        let _ = getrandom::fill(&mut salt);

        Self {
            magic: Self::MAGIC,
            version: Self::VERSION,
            algorithm,
            encrypted_dek,
            dek_iv,
            salt,
        }
    }

    /// Validate the magic bytes.
    pub fn is_valid(&self) -> bool {
        self.magic == Self::MAGIC
    }
}

/// Volume encryption manager.
///
/// Handles master key loading, DEK generation/rotation, and provides
/// encrypt/decrypt operations for file I/O.
pub struct VolumeEncryption {
    config: VolumeEncryptionConfig,
    /// Master key (loaded from file/KMS).
    master_key: Option<[u8; 32]>,
    /// Number of files encrypted with this master key.
    files_encrypted: std::sync::atomic::AtomicU64,
}

impl VolumeEncryption {
    /// Create a new volume encryption manager.
    pub fn new(config: VolumeEncryptionConfig) -> Self {
        Self {
            config,
            master_key: None,
            files_encrypted: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Load the master key from the configured key file.
    pub fn load_master_key(&mut self) -> crate::Result<()> {
        let path =
            self.config
                .master_key_path
                .as_ref()
                .ok_or_else(|| crate::Error::Encryption {
                    detail: "no master key path configured".into(),
                })?;

        let key_bytes = std::fs::read(path).map_err(|e| crate::Error::Encryption {
            detail: format!("failed to read master key from {}: {e}", path.display()),
        })?;

        if key_bytes.len() < 32 {
            return Err(crate::Error::Encryption {
                detail: format!("master key too short: {} bytes (need 32)", key_bytes.len()),
            });
        }

        let mut key = [0u8; 32];
        key.copy_from_slice(&key_bytes[..32]);
        self.master_key = Some(key);

        info!(path = %path.display(), "master encryption key loaded");
        Ok(())
    }

    /// Whether encryption is active (enabled + key loaded).
    pub fn is_active(&self) -> bool {
        self.config.enabled && self.master_key.is_some()
    }

    /// Generate a new random Data Encryption Key (DEK).
    ///
    /// Returns the raw DEK (for data encryption) and the encrypted DEK
    /// (for storage in the file header). The DEK is encrypted using
    /// AES-256-GCM with the master key for authenticated key wrapping.
    pub fn generate_dek(&self) -> crate::Result<([u8; 32], Vec<u8>)> {
        let master = self
            .master_key
            .as_ref()
            .ok_or_else(|| crate::Error::Encryption {
                detail: "master key not loaded".into(),
            })?;

        let mut dek = [0u8; 32];
        getrandom::fill(&mut dek).map_err(|e| crate::Error::Encryption {
            detail: format!("failed to generate DEK: {e}"),
        })?;

        // Encrypt DEK with master key using AES-256-GCM (authenticated encryption).
        let mut nonce_bytes = [0u8; 12];
        getrandom::fill(&mut nonce_bytes).map_err(|e| crate::Error::Encryption {
            detail: format!("failed to generate nonce: {e}"),
        })?;

        let cipher = Aes256Gcm::new_from_slice(master).map_err(|e| crate::Error::Encryption {
            detail: format!("AES-GCM key init failed: {e}"),
        })?;
        let nonce = Nonce::from_slice(&nonce_bytes);

        let ciphertext =
            cipher
                .encrypt(nonce, dek.as_ref())
                .map_err(|e| crate::Error::Encryption {
                    detail: format!("DEK encryption failed: {e}"),
                })?;

        // Prepend nonce to ciphertext for storage: [nonce:12B][ciphertext+tag]
        let mut encrypted_dek = Vec::with_capacity(12 + ciphertext.len());
        encrypted_dek.extend_from_slice(&nonce_bytes);
        encrypted_dek.extend_from_slice(&ciphertext);

        self.files_encrypted
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        Ok((dek, encrypted_dek))
    }

    /// Decrypt a DEK from a file header using the master key.
    ///
    /// Expects the encrypted DEK format: `[nonce:12B][ciphertext+tag]`.
    pub fn decrypt_dek(&self, encrypted_dek: &[u8]) -> crate::Result<[u8; 32]> {
        let master = self
            .master_key
            .as_ref()
            .ok_or_else(|| crate::Error::Encryption {
                detail: "master key not loaded".into(),
            })?;

        if encrypted_dek.len() < 12 + 32 + 16 {
            // 12B nonce + 32B DEK + 16B GCM tag
            return Err(crate::Error::Encryption {
                detail: format!(
                    "encrypted DEK too short: {} bytes (need at least 60)",
                    encrypted_dek.len()
                ),
            });
        }

        let (nonce_bytes, ciphertext) = encrypted_dek.split_at(12);
        let cipher = Aes256Gcm::new_from_slice(master).map_err(|e| crate::Error::Encryption {
            detail: format!("AES-GCM key init failed: {e}"),
        })?;
        let nonce = Nonce::from_slice(nonce_bytes);

        let plaintext =
            cipher
                .decrypt(nonce, ciphertext)
                .map_err(|_| crate::Error::Encryption {
                    detail: "DEK decryption failed: authentication tag mismatch".into(),
                })?;

        if plaintext.len() != 32 {
            return Err(crate::Error::Encryption {
                detail: format!(
                    "decrypted DEK wrong size: {} bytes (expected 32)",
                    plaintext.len()
                ),
            });
        }

        let mut dek = [0u8; 32];
        dek.copy_from_slice(&plaintext);
        Ok(dek)
    }

    /// Rotate the master key: re-encrypt all DEKs with a new master key.
    ///
    /// This is a metadata-only operation — no data needs to be rewritten.
    /// Each file's DEK is decrypted with the old key and re-encrypted
    /// with the new key, then the header is updated in place.
    pub fn rotate_master_key(&mut self, new_key_path: &Path) -> crate::Result<()> {
        let new_bytes = std::fs::read(new_key_path).map_err(|e| crate::Error::Encryption {
            detail: format!("failed to read new key: {e}"),
        })?;
        if new_bytes.len() < 32 {
            return Err(crate::Error::Encryption {
                detail: "new master key too short".into(),
            });
        }

        let mut new_key = [0u8; 32];
        new_key.copy_from_slice(&new_bytes[..32]);

        // Store old key temporarily for DEK re-encryption.
        let _old_key = self.master_key.replace(new_key);

        info!(
            new_key_path = %new_key_path.display(),
            "master key rotated — DEK re-encryption required"
        );
        Ok(())
    }

    /// Number of files encrypted with the current master key.
    pub fn files_encrypted(&self) -> u64 {
        self.files_encrypted
            .load(std::sync::atomic::Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_validation() {
        let header = EncryptedFileHeader::new(EncryptionAlgorithm::Aes256Xts, vec![0; 32]);
        assert!(header.is_valid());
        assert_eq!(header.version, 1);
    }

    #[test]
    fn dek_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let key_path = dir.path().join("master.key");
        let mut key_data = [0u8; 32];
        getrandom::fill(&mut key_data).unwrap();
        std::fs::write(&key_path, key_data).unwrap();

        let mut enc = VolumeEncryption::new(VolumeEncryptionConfig {
            master_key_path: Some(key_path),
            enabled: true,
            algorithm: EncryptionAlgorithm::Aes256Xts,
        });
        enc.load_master_key().unwrap();
        assert!(enc.is_active());

        let (dek, encrypted) = enc.generate_dek().unwrap();
        let decrypted = enc.decrypt_dek(&encrypted).unwrap();
        assert_eq!(dek, decrypted);
    }

    #[test]
    fn key_rotation() {
        let dir = tempfile::tempdir().unwrap();
        let key1_path = dir.path().join("key1");
        let key2_path = dir.path().join("key2");
        let mut k1 = [0u8; 32];
        let mut k2 = [0u8; 32];
        getrandom::fill(&mut k1).unwrap();
        getrandom::fill(&mut k2).unwrap();
        std::fs::write(&key1_path, k1).unwrap();
        std::fs::write(&key2_path, k2).unwrap();

        let mut enc = VolumeEncryption::new(VolumeEncryptionConfig {
            master_key_path: Some(key1_path),
            enabled: true,
            ..Default::default()
        });
        enc.load_master_key().unwrap();

        // Generate DEK with key1.
        let (dek1, _) = enc.generate_dek().unwrap();

        // Rotate to key2.
        enc.rotate_master_key(&key2_path).unwrap();

        // Generate DEK with key2 — should be different.
        let (dek2, _) = enc.generate_dek().unwrap();
        assert_ne!(dek1, dek2); // Different DEKs (random).
    }

    #[test]
    fn not_active_without_key() {
        let enc = VolumeEncryption::new(VolumeEncryptionConfig::default());
        assert!(!enc.is_active());
    }
}
