//! RLS policy persistence in the system catalog.
//!
//! `RlsPolicy` (the runtime shape) carries `Option<RlsPredicate>` and
//! `DenyMode`, both of which are serde-only and don't fit zerompk's
//! `ToMessagePack` derive. `StoredRlsPolicy` flattens those parts into
//! JSON strings (via sonic_rs) so the whole record can be msgpack-encoded
//! by zerompk like every other catalog row.
//!
//! Conversions: [`StoredRlsPolicy::from_runtime`] for serialization,
//! [`StoredRlsPolicy::to_runtime`] for replay on apply / boot.

use redb::TableDefinition;

use crate::control::security::deny::DenyMode;
use crate::control::security::predicate::{PolicyMode, RlsPredicate};
use crate::control::security::rls::{PolicyType, RlsPolicy};

use super::types::{SystemCatalog, catalog_err};

/// Table: `"{tenant_id}:{collection}:{policy_name}"` → MessagePack
/// `StoredRlsPolicy`.
pub(super) const RLS_POLICIES: TableDefinition<&str, &[u8]> =
    TableDefinition::new("_system.rls_policies");

/// Catalog-shape RLS policy. JSON strings are sonic_rs-encoded
/// versions of the runtime types so zerompk can derive the encoder.
#[derive(zerompk::ToMessagePack, zerompk::FromMessagePack, Debug, Clone)]
pub struct StoredRlsPolicy {
    pub tenant_id: u32,
    pub collection: String,
    pub name: String,
    /// 0 = Read, 1 = Write, 2 = All.
    pub policy_type_tag: u8,
    pub legacy_predicate: Vec<u8>,
    /// JSON-serialized `Option<RlsPredicate>`. Empty string = None.
    pub compiled_predicate_json: String,
    /// 0 = Permissive, 1 = Restrictive.
    pub mode_tag: u8,
    /// JSON-serialized `DenyMode`.
    pub on_deny_json: String,
    pub enabled: bool,
    pub created_by: String,
    pub created_at: u64,
}

impl StoredRlsPolicy {
    pub fn from_runtime(p: &RlsPolicy) -> crate::Result<Self> {
        let compiled_predicate_json = match &p.compiled_predicate {
            Some(rp) => sonic_rs::to_string(rp).map_err(|e| catalog_err("ser compiled rls", e))?,
            None => String::new(),
        };
        let on_deny_json =
            sonic_rs::to_string(&p.on_deny).map_err(|e| catalog_err("ser deny mode", e))?;
        Ok(Self {
            tenant_id: p.tenant_id,
            collection: p.collection.clone(),
            name: p.name.clone(),
            policy_type_tag: match p.policy_type {
                PolicyType::Read => 0,
                PolicyType::Write => 1,
                PolicyType::All => 2,
            },
            legacy_predicate: p.predicate.clone(),
            compiled_predicate_json,
            mode_tag: match p.mode {
                PolicyMode::Permissive => 0,
                PolicyMode::Restrictive => 1,
            },
            on_deny_json,
            enabled: p.enabled,
            created_by: p.created_by.clone(),
            created_at: p.created_at,
        })
    }

    pub fn to_runtime(&self) -> crate::Result<RlsPolicy> {
        let policy_type = match self.policy_type_tag {
            0 => PolicyType::Read,
            1 => PolicyType::Write,
            2 => PolicyType::All,
            other => {
                return Err(catalog_err(
                    "deser rls",
                    format!("invalid policy_type_tag {other}"),
                ));
            }
        };
        let mode = match self.mode_tag {
            0 => PolicyMode::Permissive,
            1 => PolicyMode::Restrictive,
            other => {
                return Err(catalog_err(
                    "deser rls",
                    format!("invalid mode_tag {other}"),
                ));
            }
        };
        let compiled_predicate: Option<RlsPredicate> = if self.compiled_predicate_json.is_empty() {
            None
        } else {
            Some(
                sonic_rs::from_str(&self.compiled_predicate_json)
                    .map_err(|e| catalog_err("deser compiled rls", e))?,
            )
        };
        let on_deny: DenyMode = sonic_rs::from_str(&self.on_deny_json)
            .map_err(|e| catalog_err("deser deny mode", e))?;
        Ok(RlsPolicy {
            name: self.name.clone(),
            collection: self.collection.clone(),
            tenant_id: self.tenant_id,
            policy_type,
            predicate: self.legacy_predicate.clone(),
            compiled_predicate,
            mode,
            on_deny,
            enabled: self.enabled,
            created_by: self.created_by.clone(),
            created_at: self.created_at,
        })
    }

    fn redb_key(&self) -> String {
        rls_key(self.tenant_id, &self.collection, &self.name)
    }
}

fn rls_key(tenant_id: u32, collection: &str, policy_name: &str) -> String {
    format!("{tenant_id}:{collection}:{policy_name}")
}

impl SystemCatalog {
    /// Insert or overwrite an RLS policy record.
    pub fn put_rls_policy(&self, stored: &StoredRlsPolicy) -> crate::Result<()> {
        let key = stored.redb_key();
        let bytes = zerompk::to_msgpack_vec(stored).map_err(|e| catalog_err("ser rls", e))?;
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        {
            let mut table = write_txn
                .open_table(RLS_POLICIES)
                .map_err(|e| catalog_err("open rls_policies", e))?;
            table
                .insert(key.as_str(), bytes.as_slice())
                .map_err(|e| catalog_err("insert rls", e))?;
        }
        write_txn.commit().map_err(|e| catalog_err("commit rls", e))
    }

    /// Delete an RLS policy. Returns `true` if a row was removed.
    pub fn delete_rls_policy(
        &self,
        tenant_id: u32,
        collection: &str,
        policy_name: &str,
    ) -> crate::Result<bool> {
        let key = rls_key(tenant_id, collection, policy_name);
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        let existed;
        {
            let mut table = write_txn
                .open_table(RLS_POLICIES)
                .map_err(|e| catalog_err("open rls_policies", e))?;
            existed = table
                .remove(key.as_str())
                .map_err(|e| catalog_err("remove rls", e))?
                .is_some();
        }
        write_txn
            .commit()
            .map_err(|e| catalog_err("commit rls", e))?;
        Ok(existed)
    }

    /// Read a single RLS policy by full key.
    pub fn get_rls_policy(
        &self,
        tenant_id: u32,
        collection: &str,
        policy_name: &str,
    ) -> crate::Result<Option<StoredRlsPolicy>> {
        let key = rls_key(tenant_id, collection, policy_name);
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(RLS_POLICIES)
            .map_err(|e| catalog_err("open rls_policies", e))?;
        match table.get(key.as_str()) {
            Ok(Some(value)) => {
                let s: StoredRlsPolicy = zerompk::from_msgpack(value.value())
                    .map_err(|e| catalog_err("deser rls", e))?;
                Ok(Some(s))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(catalog_err("get rls", e)),
        }
    }

    /// Load every RLS policy across every tenant. Used by boot replay.
    pub fn load_all_rls_policies(&self) -> crate::Result<Vec<StoredRlsPolicy>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(RLS_POLICIES)
            .map_err(|e| catalog_err("open rls_policies", e))?;
        let mut out = Vec::new();
        for entry in table
            .range::<&str>(..)
            .map_err(|e| catalog_err("range rls_policies", e))?
        {
            let (_, value) = entry.map_err(|e| catalog_err("read rls", e))?;
            let s: StoredRlsPolicy =
                zerompk::from_msgpack(value.value()).map_err(|e| catalog_err("deser rls", e))?;
            out.push(s);
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_catalog() -> SystemCatalog {
        let dir = tempfile::tempdir().unwrap();
        SystemCatalog::open(&dir.path().join("system.redb")).unwrap()
    }

    fn sample_policy(tenant_id: u32, collection: &str, name: &str) -> RlsPolicy {
        RlsPolicy {
            name: name.into(),
            collection: collection.into(),
            tenant_id,
            policy_type: PolicyType::Read,
            predicate: Vec::new(),
            compiled_predicate: None,
            mode: PolicyMode::Permissive,
            on_deny: DenyMode::default(),
            enabled: true,
            created_by: "admin".into(),
            created_at: 1234,
        }
    }

    #[test]
    fn put_get_delete_roundtrip() {
        let catalog = make_catalog();
        let runtime = sample_policy(1, "users", "p1");
        let stored = StoredRlsPolicy::from_runtime(&runtime).unwrap();
        catalog.put_rls_policy(&stored).unwrap();

        let loaded = catalog.get_rls_policy(1, "users", "p1").unwrap().unwrap();
        let runtime2 = loaded.to_runtime().unwrap();
        assert_eq!(runtime2.name, "p1");
        assert_eq!(runtime2.collection, "users");
        assert!(matches!(runtime2.policy_type, PolicyType::Read));

        assert!(catalog.delete_rls_policy(1, "users", "p1").unwrap());
        assert!(catalog.get_rls_policy(1, "users", "p1").unwrap().is_none());
    }

    #[test]
    fn load_all_returns_every_tenant() {
        let catalog = make_catalog();
        for (tenant, name) in [(1, "a"), (1, "b"), (2, "c")] {
            let stored = StoredRlsPolicy::from_runtime(&sample_policy(tenant, "x", name)).unwrap();
            catalog.put_rls_policy(&stored).unwrap();
        }
        let all = catalog.load_all_rls_policies().unwrap();
        assert_eq!(all.len(), 3);
    }
}
