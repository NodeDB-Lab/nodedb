// SPDX-License-Identifier: BUSL-1.1

//! Per-user HMAC material for externally synchronized CRDT deltas.
//!
//! Keys are derived from a WAL-wrapped root and never persisted: the catalog
//! keeps only a non-secret fingerprint that binds it to that root, so a swapped
//! or missing root fails closed on restart instead of silently deriving
//! different keys.

use sha2::{Digest as _, Sha256};

use crate::control::security::catalog::types::{SystemCatalog, catalog_err};

/// Non-secret fingerprint that binds the catalog to its WAL-wrapped signing
/// root and makes a missing/incorrect root fail closed on restart.
pub const CRDT_SIGNING_ROOT_METADATA: redb::TableDefinition<&str, &[u8]> =
    redb::TableDefinition::new("_system.crdt_signing_root_metadata");

impl SystemCatalog {
    /// Install the at-rest-protected root used to derive per-user signing
    /// keys.
    pub fn configure_crdt_signing_root(&self, root: Option<[u8; 32]>) -> crate::Result<()> {
        use redb::ReadableTable as _;

        if let Some(root) = root {
            let fingerprint: [u8; 32] = Sha256::digest(root).into();
            let txn = self
                .db
                .begin_write()
                .map_err(|e| catalog_err("crdt signing-root metadata write txn", e))?;
            {
                let mut table = txn
                    .open_table(CRDT_SIGNING_ROOT_METADATA)
                    .map_err(|e| catalog_err("open crdt signing-root metadata", e))?;
                if let Some(stored) = table
                    .get("fingerprint")
                    .map_err(|e| catalog_err("read crdt signing-root fingerprint", e))?
                    && stored.value() != fingerprint
                {
                    return Err(crate::Error::Config {
                        detail: "WAL-wrapped CRDT signing root does not match the durable catalog fingerprint".into(),
                    });
                }
                table
                    .insert("fingerprint", fingerprint.as_slice())
                    .map_err(|e| catalog_err("persist crdt signing-root fingerprint", e))?;
            }
            txn.commit()
                .map_err(|e| catalog_err("crdt signing-root metadata commit", e))?;
        }
        *self
            .crdt_signing_root
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = root;
        Ok(())
    }

    fn derive_crdt_signing_key(&self, tenant_id: u64, user_id: u64) -> crate::Result<[u8; 32]> {
        let root = self
            .crdt_signing_root
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .ok_or_else(|| crate::Error::Config {
                detail: "CRDT signing root unavailable; enable WAL encryption".into(),
            })?;
        let mut context = [0u8; 16];
        context[..8].copy_from_slice(&tenant_id.to_le_bytes());
        context[8..].copy_from_slice(&user_id.to_le_bytes());
        let hkdf = hkdf::Hkdf::<sha2::Sha256>::new(Some(b"nodedb-crdt-user-key-v1"), &root);
        let mut key = [0u8; 32];
        hkdf.expand(&context, &mut key)
            .map_err(|_| crate::Error::Internal {
                detail: "CRDT signing key derivation failed".into(),
            })?;
        Ok(key)
    }

    /// Derive a stable per-user key without persisting it in the catalog.
    pub fn get_or_create_crdt_signing_key(
        &self,
        tenant_id: u64,
        user_id: u64,
    ) -> crate::Result<[u8; 32]> {
        self.derive_crdt_signing_key(tenant_id, user_id)
    }

    /// Derive the existing per-user key when the secure root is configured.
    pub fn get_crdt_signing_key(
        &self,
        tenant_id: u64,
        user_id: u64,
    ) -> crate::Result<Option<[u8; 32]>> {
        self.derive_crdt_signing_key(tenant_id, user_id).map(Some)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crdt_signing_key_is_stable_and_tenant_user_scoped() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("system.redb");
        let root = [0x5a; 32];
        let first = {
            let cat = SystemCatalog::open(&path).unwrap();
            cat.configure_crdt_signing_root(Some(root)).unwrap();
            let first = cat.get_or_create_crdt_signing_key(5, 9).unwrap();
            assert_eq!(cat.get_or_create_crdt_signing_key(5, 9).unwrap(), first);
            assert_ne!(cat.get_or_create_crdt_signing_key(5, 10).unwrap(), first);
            first
        };
        let cat = SystemCatalog::open(&path).unwrap();
        cat.configure_crdt_signing_root(Some(root)).unwrap();
        assert_eq!(cat.get_crdt_signing_key(5, 9).unwrap(), Some(first));
    }

    #[test]
    fn signing_root_fingerprint_rejects_silent_root_replacement() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("system.redb");
        {
            let catalog = SystemCatalog::open(&path).unwrap();
            catalog
                .configure_crdt_signing_root(Some([0x11; 32]))
                .unwrap();
        }
        let catalog = SystemCatalog::open(&path).unwrap();
        let error = catalog
            .configure_crdt_signing_root(Some([0x22; 32]))
            .unwrap_err();
        assert!(matches!(error, crate::Error::Config { .. }));
    }
}
