// SPDX-License-Identifier: BUSL-1.1

//! `CREATE CREDENTIAL` rows in the system catalog.
//!
//! `_system.credentials` keys on `"{tenant_id}:{name}"` and holds one
//! MessagePack `StoredCredential` per credential. The secret inside is
//! sealed, so the table never holds plaintext.

use redb::{ReadableDatabase, ReadableTable, TableDefinition};

use super::types::{SystemCatalog, catalog_err};
use crate::control::security::source_credential::StoredCredential;

/// Table: `"{tenant_id}:{name}"` -> MessagePack `StoredCredential`.
pub(super) const CREDENTIALS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("_system.credentials");

/// The row key of credential `name` of `tenant_id`.
fn credential_key(tenant_id: u64, name: &str) -> String {
    format!("{tenant_id}:{name}")
}

impl SystemCatalog {
    /// Store `credential`, replacing a row of the same name.
    pub fn put_credential(&self, credential: &StoredCredential) -> crate::Result<()> {
        let key = credential_key(credential.tenant_id, &credential.name);
        let bytes = zerompk::to_msgpack_vec(credential)
            .map_err(|e| catalog_err("serialize credential", e))?;
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("credential write txn", e))?;
        {
            let mut table = txn
                .open_table(CREDENTIALS)
                .map_err(|e| catalog_err("open credentials", e))?;
            table
                .insert(key.as_str(), bytes.as_slice())
                .map_err(|e| catalog_err("insert credential", e))?;
        }
        txn.commit()
            .map_err(|e| catalog_err("credential commit", e))
    }

    /// Remove credential `name` of `tenant_id`. Returns whether it existed.
    pub fn delete_credential(&self, tenant_id: u64, name: &str) -> crate::Result<bool> {
        let key = credential_key(tenant_id, name);
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("credential write txn", e))?;
        let removed = {
            let mut table = txn
                .open_table(CREDENTIALS)
                .map_err(|e| catalog_err("open credentials", e))?;
            table
                .remove(key.as_str())
                .map_err(|e| catalog_err("remove credential", e))?
                .is_some()
        };
        txn.commit()
            .map_err(|e| catalog_err("credential commit", e))?;
        Ok(removed)
    }

    /// Credential `name` of `tenant_id`.
    pub fn get_credential(
        &self,
        tenant_id: u64,
        name: &str,
    ) -> crate::Result<Option<StoredCredential>> {
        let key = credential_key(tenant_id, name);
        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("credential read txn", e))?;
        let table = txn
            .open_table(CREDENTIALS)
            .map_err(|e| catalog_err("open credentials", e))?;
        let Some(row) = table
            .get(key.as_str())
            .map_err(|e| catalog_err("read credential", e))?
        else {
            return Ok(None);
        };
        zerompk::from_msgpack(row.value())
            .map(Some)
            .map_err(|e| catalog_err("decode credential", e))
    }

    /// Every credential of `tenant_id`, in name order.
    pub fn list_credentials(&self, tenant_id: u64) -> crate::Result<Vec<StoredCredential>> {
        let prefix = format!("{tenant_id}:");
        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("credential read txn", e))?;
        let table = txn
            .open_table(CREDENTIALS)
            .map_err(|e| catalog_err("open credentials", e))?;
        let mut out = Vec::new();
        for row in table
            .range(prefix.as_str()..)
            .map_err(|e| catalog_err("range credentials", e))?
        {
            let (key, value) = row.map_err(|e| catalog_err("read credential", e))?;
            if !key.value().starts_with(&prefix) {
                break;
            }
            let credential: StoredCredential = zerompk::from_msgpack(value.value())
                .map_err(|e| catalog_err("decode credential", e))?;
            out.push(credential);
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::source_credential::CredentialKind;

    fn row(tenant_id: u64, name: &str) -> StoredCredential {
        StoredCredential {
            tenant_id,
            name: name.into(),
            kind: CredentialKind::Bearer,
            sealed: vec![1, 2, 3],
            owner: "nodedb".into(),
            created_at_ms: 1,
        }
    }

    #[test]
    fn rows_round_trip_per_tenant() {
        let dir = tempfile::tempdir().expect("tempdir");
        let catalog = SystemCatalog::open(&dir.path().join("system.redb")).expect("catalog");
        catalog.put_credential(&row(1, "b")).expect("put");
        catalog.put_credential(&row(1, "a")).expect("put");
        catalog.put_credential(&row(12, "c")).expect("put");
        let names: Vec<String> = catalog
            .list_credentials(1)
            .expect("list")
            .into_iter()
            .map(|c| c.name)
            .collect();
        assert_eq!(names, vec!["a".to_string(), "b".to_string()]);
        assert_eq!(
            catalog.get_credential(1, "a").expect("get"),
            Some(row(1, "a"))
        );
        assert!(catalog.delete_credential(1, "a").expect("delete"));
        assert!(!catalog.delete_credential(1, "a").expect("delete"));
        assert_eq!(catalog.get_credential(1, "a").expect("get"), None);
    }
}
