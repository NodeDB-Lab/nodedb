// SPDX-License-Identifier: BUSL-1.1

//! The key that seals `CREATE CREDENTIAL` secrets at rest, read from the
//! `[credential_encryption] key_path` file.
//!
//! Every node of a cluster reads the same key file, so a secret one node
//! seals opens on every node. The key file passes the same checks as the
//! WAL key: no symlink, a regular file, mode `0400` or `0600`, owned by the
//! server's user.

use std::path::Path;

use zeroize::Zeroizing;

use crate::control::security::keystore::key_file_security::check_key_file;

/// Bytes in the key.
pub const KEK_LEN: usize = 32;

/// The credential key. `Debug` never prints it.
#[derive(Clone)]
pub struct CredentialKek(Zeroizing<[u8; KEK_LEN]>);

impl CredentialKek {
    /// The key `bytes`.
    pub fn from_bytes(bytes: [u8; KEK_LEN]) -> Self {
        Self(Zeroizing::new(bytes))
    }

    /// Read the key file at `path`: exactly 32 bytes.
    pub fn read(path: &Path) -> crate::Result<Self> {
        check_key_file(path)?;
        let raw = Zeroizing::new(std::fs::read(path).map_err(|e| crate::Error::Encryption {
            detail: format!("cannot read credential key file {}: {e}", path.display()),
        })?);
        let bytes: [u8; KEK_LEN] =
            raw.as_slice()
                .try_into()
                .map_err(|_| crate::Error::Encryption {
                    detail: format!(
                        "credential key file {} holds {} bytes, expected {KEK_LEN}; generate one \
                         with `head -c 32 /dev/urandom`",
                        path.display(),
                        raw.len()
                    ),
                })?;
        Ok(Self::from_bytes(bytes))
    }

    /// The key bytes.
    pub(super) fn bytes(&self) -> &[u8; KEK_LEN] {
        &self.0
    }

    /// Whether `other` is this same key.
    pub fn same_bytes(&self, other: &[u8; KEK_LEN]) -> bool {
        self.bytes() == other
    }

    /// Whether `wal_key` is this same key, wherever the WAL key came from.
    ///
    /// The WAL key never leaves its type, so this compares a subkey both derive.
    pub fn same_key_as_wal(
        &self,
        wal_key: &nodedb_wal::crypto::WalEncryptionKey,
    ) -> crate::Result<bool> {
        let domain = b"nodedb-credential-kek-reuse-check";
        let candidate = nodedb_wal::crypto::WalEncryptionKey::with_epoch(self.bytes(), [0u8; 4]);
        let own = candidate.derive_subkey(domain).map_err(crate::Error::Wal)?;
        let wal = wal_key.derive_subkey(domain).map_err(crate::Error::Wal)?;
        Ok(own == wal)
    }
}

impl std::fmt::Debug for CredentialKek {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CredentialKek(<redacted>)")
    }
}
