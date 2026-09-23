// SPDX-License-Identifier: BUSL-1.1

use std::fs::{self, OpenOptions};
use std::io::Write as _;
use std::path::{Path, PathBuf};

use aes_gcm::aead::{Aead as _, KeyInit as _, Payload};
use aes_gcm::{Aes256Gcm, Nonce};
use tracing::info;

use super::core::WalManager;

impl WalManager {
    /// Open with encryption key loaded from a file.
    pub fn open_encrypted(
        path: &Path,
        use_direct_io: bool,
        key_path: &Path,
    ) -> crate::Result<Self> {
        let key =
            nodedb_wal::crypto::WalEncryptionKey::from_file(key_path).map_err(crate::Error::Wal)?;
        let ring = nodedb_wal::crypto::KeyRing::new(key);
        let mut mgr = Self::open(path, use_direct_io)?;
        {
            let mut wal = mgr.wal.lock().unwrap_or_else(|p| p.into_inner());
            wal.configure_encryption_ring(ring.clone())
                .map_err(crate::Error::Wal)?;
        }
        mgr.crdt_signing_root = Some(load_or_create_signing_root(path, &ring)?);
        mgr.encryption_ring = Some(ring);
        info!(key_path = %key_path.display(), "WAL encryption enabled");
        Ok(mgr)
    }

    /// Open with key rotation: current key + previous key for dual-key reads.
    ///
    /// New writes use `current_key_path`. Reads try current first, then previous.
    /// Once all old WAL segments are compacted, remove the previous key.
    pub fn open_encrypted_rotating(
        path: &Path,
        use_direct_io: bool,
        current_key_path: &Path,
        previous_key_path: &Path,
    ) -> crate::Result<Self> {
        let current = nodedb_wal::crypto::WalEncryptionKey::from_file(current_key_path)
            .map_err(crate::Error::Wal)?;
        let previous = nodedb_wal::crypto::WalEncryptionKey::from_file(previous_key_path)
            .map_err(crate::Error::Wal)?;
        let ring = nodedb_wal::crypto::KeyRing::with_previous(current, previous);
        let mut mgr = Self::open(path, use_direct_io)?;
        {
            let mut wal = mgr.wal.lock().unwrap_or_else(|p| p.into_inner());
            wal.configure_encryption_ring(ring.clone())
                .map_err(crate::Error::Wal)?;
        }
        let signing_root = load_or_create_signing_root(path, &ring)?;
        // Rewrap with the current key so the next current+previous rotation can
        // recover the same root even after the oldest WAL key is retired.
        write_signing_root(path, ring.current(), &signing_root)?;
        mgr.crdt_signing_root = Some(signing_root);
        mgr.encryption_ring = Some(ring);
        info!(
            current_key = %current_key_path.display(),
            previous_key = %previous_key_path.display(),
            "WAL encryption enabled with key rotation"
        );
        Ok(mgr)
    }

    /// Rotate the encryption key at runtime without downtime.
    ///
    /// The new key becomes the current key for all future writes.
    /// The old current key becomes the previous key for dual-key reads.
    /// Returns an error if the WAL has already written records to the active
    /// segment — in that case, roll to a new segment first.
    pub fn rotate_key(&mut self, new_key_path: &Path) -> crate::Result<()> {
        let new_key = nodedb_wal::crypto::WalEncryptionKey::from_file(new_key_path)
            .map_err(crate::Error::Wal)?;

        let new_ring = {
            let mut wal = self.wal.lock().unwrap_or_else(|p| p.into_inner());
            let new_ring = if let Some(ring) = wal.encryption_ring() {
                nodedb_wal::crypto::KeyRing::with_previous(new_key.clone(), ring.current().clone())
            } else {
                nodedb_wal::crypto::KeyRing::new(new_key.clone())
            };
            wal.rotate_encryption_ring(new_ring.clone())
                .map_err(crate::Error::Wal)?;
            new_ring
        };
        if let Some(root) = self.crdt_signing_root {
            write_signing_root(&self.wal_dir, &new_key, &root)?;
        }
        self.encryption_ring = Some(new_ring);
        info!(new_key = %new_key_path.display(), "WAL encryption key rotated");
        Ok(())
    }

    /// Get the current encryption key (if configured). Used for backup encryption.
    pub fn encryption_key(&self) -> Option<&nodedb_wal::crypto::WalEncryptionKey> {
        self.encryption_ring.as_ref().map(|r| r.current())
    }

    /// Get the key ring (if configured). Used for dual-key decryption during replay.
    pub fn encryption_ring(&self) -> Option<&nodedb_wal::crypto::KeyRing> {
        self.encryption_ring.as_ref()
    }

    /// Set the encryption key ring. All subsequent records will be encrypted.
    ///
    /// Must be called before any records are written to the active segment.
    pub fn set_encryption_ring(&mut self, ring: nodedb_wal::crypto::KeyRing) -> crate::Result<()> {
        let signing_root = load_or_create_signing_root(&self.wal_dir, &ring)?;
        write_signing_root(&self.wal_dir, ring.current(), &signing_root)?;
        let mut wal = self.wal.lock().unwrap_or_else(|p| p.into_inner());
        wal.configure_encryption_ring(ring.clone())
            .map_err(crate::Error::Wal)?;
        self.crdt_signing_root = Some(signing_root);
        self.encryption_ring = Some(ring);
        Ok(())
    }
}

const SIGNING_ROOT_FILE: &str = "crdt_signing_root.enc";
const SIGNING_ROOT_VERSION: u8 = 1;
const SIGNING_ROOT_AAD: &[u8] = b"nodedb-crdt-signing-root-v1";

fn signing_root_path(wal_dir: &Path) -> PathBuf {
    wal_dir.join(SIGNING_ROOT_FILE)
}

fn root_cipher(key: &nodedb_wal::crypto::WalEncryptionKey) -> crate::Result<Aes256Gcm> {
    let wrapping_key = key
        .derive_subkey(b"nodedb-crdt-signing-root-wrap-v1")
        .map_err(crate::Error::Wal)?;
    Aes256Gcm::new_from_slice(&wrapping_key).map_err(|_| crate::Error::Storage {
        engine: "wal".into(),
        detail: "invalid CRDT signing-root wrapping key".into(),
    })
}

fn load_or_create_signing_root(
    wal_dir: &Path,
    ring: &nodedb_wal::crypto::KeyRing,
) -> crate::Result<[u8; 32]> {
    let path = signing_root_path(wal_dir);
    match fs::read(&path) {
        Ok(encoded) => decrypt_signing_root(&encoded, ring),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let mut root = [0u8; 32];
            getrandom::fill(&mut root).map_err(|error| crate::Error::Storage {
                engine: "wal".into(),
                detail: format!("generate CRDT signing root: {error}"),
            })?;
            write_signing_root(wal_dir, ring.current(), &root)?;
            Ok(root)
        }
        Err(error) => Err(crate::Error::Storage {
            engine: "wal".into(),
            detail: format!("read {}: {error}", path.display()),
        }),
    }
}

fn decrypt_signing_root(
    encoded: &[u8],
    ring: &nodedb_wal::crypto::KeyRing,
) -> crate::Result<[u8; 32]> {
    if encoded.len() < 13 || encoded[0] != SIGNING_ROOT_VERSION {
        return Err(crate::Error::Storage {
            engine: "wal".into(),
            detail: "invalid CRDT signing-root envelope".into(),
        });
    }
    let nonce_bytes: [u8; 12] = encoded[1..13]
        .try_into()
        .map_err(|_| crate::Error::Storage {
            engine: "wal".into(),
            detail: "invalid CRDT signing-root nonce".into(),
        })?;
    let nonce = Nonce::from(nonce_bytes);
    for key in std::iter::once(ring.current()).chain(ring.previous()) {
        let cipher = root_cipher(key)?;
        if let Ok(plaintext) = cipher.decrypt(
            &nonce,
            Payload {
                msg: &encoded[13..],
                aad: SIGNING_ROOT_AAD,
            },
        ) {
            return plaintext.try_into().map_err(|_| crate::Error::Storage {
                engine: "wal".into(),
                detail: "invalid CRDT signing-root plaintext length".into(),
            });
        }
    }
    Err(crate::Error::Storage {
        engine: "wal".into(),
        detail: "CRDT signing-root envelope authentication failed".into(),
    })
}

fn write_signing_root(
    wal_dir: &Path,
    key: &nodedb_wal::crypto::WalEncryptionKey,
    root: &[u8; 32],
) -> crate::Result<()> {
    let mut nonce_bytes = [0u8; 12];
    getrandom::fill(&mut nonce_bytes).map_err(|error| crate::Error::Storage {
        engine: "wal".into(),
        detail: format!("generate CRDT signing-root nonce: {error}"),
    })?;
    let nonce = Nonce::from(nonce_bytes);
    let ciphertext = root_cipher(key)?
        .encrypt(
            &nonce,
            Payload {
                msg: root,
                aad: SIGNING_ROOT_AAD,
            },
        )
        .map_err(|_| crate::Error::Storage {
            engine: "wal".into(),
            detail: "encrypt CRDT signing root".into(),
        })?;
    let path = signing_root_path(wal_dir);
    let temporary = wal_dir.join(format!(".{SIGNING_ROOT_FILE}.{}.tmp", std::process::id()));
    let mut options = OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    let mut file = options
        .open(&temporary)
        .map_err(|error| crate::Error::Storage {
            engine: "wal".into(),
            detail: format!("create {}: {error}", temporary.display()),
        })?;
    file.write_all(&[SIGNING_ROOT_VERSION])
        .and_then(|()| file.write_all(&nonce_bytes))
        .and_then(|()| file.write_all(&ciphertext))
        .and_then(|()| file.sync_all())
        .map_err(|error| crate::Error::Storage {
            engine: "wal".into(),
            detail: format!("write {}: {error}", temporary.display()),
        })?;
    fs::rename(&temporary, &path).map_err(|error| crate::Error::Storage {
        engine: "wal".into(),
        detail: format!("replace {}: {error}", path.display()),
    })?;
    #[cfg(unix)]
    fs::File::open(wal_dir)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| crate::Error::Storage {
            engine: "wal".into(),
            detail: format!("sync {}: {error}", wal_dir.display()),
        })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{DatabaseId, TenantId, VShardId};
    use crate::wal::manager::NO_APPLY_KEY;

    #[test]
    fn crdt_signing_root_survives_chained_runtime_rotation_and_restart() {
        fn write_key(path: &std::path::Path, byte: u8) {
            std::fs::write(path, [byte; 32]).unwrap();
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt as _;
                std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
            }
        }

        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal_dir");
        let key_a = dir.path().join("key-a");
        let key_b = dir.path().join("key-b");
        let key_c = dir.path().join("key-c");
        write_key(&key_a, 0x11);
        write_key(&key_b, 0x22);
        write_key(&key_c, 0x33);

        let mut wal = WalManager::open_encrypted(&wal_dir, false, &key_a).unwrap();
        let stable_root = wal.crdt_signing_root().unwrap().unwrap();
        wal.appender(NO_APPLY_KEY)
            .append_put(
                TenantId::new(1),
                VShardId::new(0),
                DatabaseId::DEFAULT,
                b"a",
            )
            .unwrap();
        wal.rotate_key(&key_b).unwrap();
        drop(wal);

        let mut wal = WalManager::open_encrypted_rotating(&wal_dir, false, &key_b, &key_a).unwrap();
        assert_eq!(wal.crdt_signing_root().unwrap(), Some(stable_root));
        wal.appender(NO_APPLY_KEY)
            .append_put(
                TenantId::new(1),
                VShardId::new(0),
                DatabaseId::DEFAULT,
                b"b",
            )
            .unwrap();
        wal.rotate_key(&key_c).unwrap();
        drop(wal);

        let wal = WalManager::open_encrypted_rotating(&wal_dir, false, &key_c, &key_b).unwrap();
        assert_eq!(wal.crdt_signing_root().unwrap(), Some(stable_root));
    }

    #[test]
    fn set_encryption_ring_rewraps_root_across_chained_transitions() {
        fn write_key(path: &std::path::Path, byte: u8) {
            std::fs::write(path, [byte; 32]).unwrap();
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt as _;
                std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
            }
        }

        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal_dir");
        let key_a_path = dir.path().join("key-a");
        let key_b_path = dir.path().join("key-b");
        let key_c_path = dir.path().join("key-c");
        write_key(&key_a_path, 0x41);
        write_key(&key_b_path, 0x42);
        write_key(&key_c_path, 0x43);
        let key_a = nodedb_wal::crypto::WalEncryptionKey::from_file(&key_a_path).unwrap();
        let key_b = nodedb_wal::crypto::WalEncryptionKey::from_file(&key_b_path).unwrap();
        let key_c = nodedb_wal::crypto::WalEncryptionKey::from_file(&key_c_path).unwrap();

        let mut wal = WalManager::open_for_testing(&wal_dir).unwrap();
        wal.set_encryption_ring(nodedb_wal::crypto::KeyRing::new(key_a.clone()))
            .unwrap();
        let root = wal.crdt_signing_root().unwrap();
        wal.appender(NO_APPLY_KEY)
            .append_put(
                TenantId::new(1),
                VShardId::new(0),
                DatabaseId::DEFAULT,
                b"a",
            )
            .unwrap();
        wal.set_encryption_ring(nodedb_wal::crypto::KeyRing::with_previous(
            key_b.clone(),
            key_a,
        ))
        .unwrap();
        assert_eq!(wal.crdt_signing_root().unwrap(), root);
        wal.appender(NO_APPLY_KEY)
            .append_put(
                TenantId::new(1),
                VShardId::new(0),
                DatabaseId::DEFAULT,
                b"b",
            )
            .unwrap();
        wal.set_encryption_ring(nodedb_wal::crypto::KeyRing::with_previous(key_c, key_b))
            .unwrap();
        assert_eq!(wal.crdt_signing_root().unwrap(), root);
        drop(wal);

        let wal =
            WalManager::open_encrypted_rotating(&wal_dir, false, &key_c_path, &key_b_path).unwrap();
        assert_eq!(wal.crdt_signing_root().unwrap(), root);
    }
}
