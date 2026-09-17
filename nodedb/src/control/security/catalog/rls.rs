// SPDX-License-Identifier: BUSL-1.1

//! RLS policy persistence in the system catalog.
//!
//! `StoredRlsPolicy` is the catalog row: the policy's `USING` text, its
//! database, and the scalar settings. The compiled predicate is never
//! persisted. Every reader recompiles the text against the collection's
//! current declared columns through [`StoredRlsPolicy::to_runtime`], so a
//! literal compared against a `TIMESTAMP` column is typed the same way at
//! boot, on Raft apply, and after a schema change as it was at
//! `CREATE RLS POLICY`. `DenyMode` is serde-only and is carried as a
//! sonic_rs JSON string so the row can derive zerompk like every other
//! catalog row.
//!
//! Conversions: [`StoredRlsPolicy::from_runtime`] for serialization,
//! [`StoredRlsPolicy::to_runtime`] for replay on apply / boot, and
//! [`StoredRlsPolicy::rehydrate`] for the load paths that must install a
//! policy whatever happens: a row that cannot be compiled installs as a
//! restrictive deny-all so it can never widen access.

use redb::{ReadableDatabase, ReadableTable, TableDefinition};
use tracing::error;

use crate::control::security::deny::DenyMode;
use crate::control::security::predicate::{PolicyMode, RlsPredicate};
use crate::control::security::rls::compile::{compile_policy_predicate, declared_columns};
use crate::control::security::rls::{PolicyType, RlsPolicy};
use crate::types::DatabaseId;

use super::types::{SystemCatalog, catalog_err};

/// Table: `"{tenant_id}:{collection}:{policy_name}"` → MessagePack
/// `StoredRlsPolicy`.
pub(super) const RLS_POLICIES: TableDefinition<&str, &[u8]> =
    TableDefinition::new("_system.rls_policies");

/// Catalog-shape RLS policy.
///
/// Map-encoded (`#[msgpack(map)]`) so a field can carry `#[msgpack(default)]`.
#[derive(zerompk::ToMessagePack, zerompk::FromMessagePack, Debug, Clone)]
#[msgpack(map)]
pub struct StoredRlsPolicy {
    pub tenant_id: u64,
    /// Database the policy's collection lives in. With `display_collection`
    /// it names the catalog collection whose declared columns type the
    /// predicate's literals.
    pub database_id: u64,
    /// Qualified with the owning database ID — the lookup key. Never
    /// shown to a user; see `display_collection`.
    pub collection: String,
    /// The collection name as the user wrote it, unqualified.
    /// Falls back to `collection` when empty.
    #[msgpack(default)]
    pub display_collection: String,
    pub name: String,
    /// 0 = Read, 1 = Write, 2 = All.
    pub policy_type_tag: u8,
    /// The `USING (...)` text as written. Empty = no row filter (vacuous).
    pub predicate_text: String,
    /// 0 = Permissive, 1 = Restrictive.
    pub mode_tag: u8,
    /// JSON-serialized `DenyMode`.
    pub on_deny_json: String,
    pub enabled: bool,
    pub created_by: String,
    pub created_at: u64,
}

impl StoredRlsPolicy {
    /// The catalog row for `p`, whose predicate was compiled from
    /// `predicate_text` against the collection in `database_id`.
    pub fn from_runtime(
        p: &RlsPolicy,
        database_id: DatabaseId,
        predicate_text: &str,
    ) -> crate::Result<Self> {
        let on_deny_json =
            sonic_rs::to_string(&p.on_deny).map_err(|e| catalog_err("ser deny mode", e))?;
        Ok(Self {
            tenant_id: p.tenant_id,
            database_id: database_id.as_u64(),
            collection: p.collection.clone(),
            display_collection: p.display_collection.clone(),
            name: p.name.clone(),
            policy_type_tag: match p.policy_type {
                PolicyType::Read => 0,
                PolicyType::Write => 1,
                PolicyType::All => 2,
            },
            predicate_text: predicate_text.to_string(),
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

    /// The runtime policy, with `predicate_text` compiled against the
    /// collection's current declared columns.
    ///
    /// Errors when a tag is invalid, the deny mode does not decode, the
    /// collection is absent from `catalog`, or the text does not compile
    /// against its columns.
    pub fn to_runtime(&self, catalog: &SystemCatalog) -> crate::Result<RlsPolicy> {
        let policy_type = self.policy_type()?;
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
        let on_deny: DenyMode = sonic_rs::from_str(&self.on_deny_json)
            .map_err(|e| catalog_err("deser deny mode", e))?;
        let compiled_predicate = if self.predicate_text.is_empty() {
            None
        } else {
            let columns = declared_columns(
                catalog,
                DatabaseId::new(self.database_id),
                self.tenant_id,
                self.display_name(),
            )?;
            Some(compile_policy_predicate(&self.predicate_text, &columns)?)
        };
        Ok(RlsPolicy {
            name: self.name.clone(),
            collection: self.collection.clone(),
            display_collection: self.display_name().to_string(),
            tenant_id: self.tenant_id,
            policy_type,
            compiled_predicate,
            mode,
            on_deny,
            enabled: self.enabled,
            created_by: self.created_by.clone(),
            created_at: self.created_at,
        })
    }

    /// [`Self::to_runtime`], or a restrictive deny-all policy under the same
    /// key when the row cannot be compiled.
    ///
    /// For the load paths that install every stored row: a policy that
    /// cannot be typed against its collection must still govern the
    /// collection, and the only safe form is one that admits no row. The
    /// error is logged at `error` level with the policy's key.
    pub fn rehydrate(&self, catalog: &SystemCatalog) -> RlsPolicy {
        match self.to_runtime(catalog) {
            Ok(policy) => policy,
            Err(e) => {
                error!(
                    policy = %self.name,
                    collection = %self.collection,
                    tenant = self.tenant_id,
                    error = %e,
                    "RLS policy cannot be compiled; installed as restrictive deny-all"
                );
                self.deny_all()
            }
        }
    }

    /// The restrictive deny-all form of this row: same key, same enabled
    /// flag, a predicate no row passes, AND-combined with every other policy.
    fn deny_all(&self) -> RlsPolicy {
        RlsPolicy {
            name: self.name.clone(),
            collection: self.collection.clone(),
            display_collection: self.display_name().to_string(),
            tenant_id: self.tenant_id,
            // An invalid tag cannot say which path the policy governs, so
            // the deny-all governs both.
            policy_type: self.policy_type().unwrap_or(PolicyType::All),
            compiled_predicate: Some(RlsPredicate::AlwaysFalse),
            mode: PolicyMode::Restrictive,
            on_deny: DenyMode::default(),
            enabled: self.enabled,
            created_by: self.created_by.clone(),
            created_at: self.created_at,
        }
    }

    fn policy_type(&self) -> crate::Result<PolicyType> {
        match self.policy_type_tag {
            0 => Ok(PolicyType::Read),
            1 => Ok(PolicyType::Write),
            2 => Ok(PolicyType::All),
            other => Err(catalog_err(
                "deser rls",
                format!("invalid policy_type_tag {other}"),
            )),
        }
    }

    /// The unqualified collection name, falling back to `collection`.
    fn display_name(&self) -> &str {
        if self.display_collection.is_empty() {
            &self.collection
        } else {
            &self.display_collection
        }
    }

    fn redb_key(&self) -> String {
        rls_key(self.tenant_id, &self.collection, &self.name)
    }
}

fn rls_key(tenant_id: u64, collection: &str, policy_name: &str) -> String {
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
        tenant_id: u64,
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
        tenant_id: u64,
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

    /// Every RLS policy attached to one collection of one tenant.
    ///
    /// `collection` is the database-qualified name the key carries. The scan
    /// is bounded to that collection's key prefix.
    pub fn list_rls_policies_for_collection(
        &self,
        tenant_id: u64,
        collection: &str,
    ) -> crate::Result<Vec<StoredRlsPolicy>> {
        let prefix = format!("{tenant_id}:{collection}:");
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(RLS_POLICIES)
            .map_err(|e| catalog_err("open rls_policies", e))?;
        let mut out = Vec::new();
        for entry in table
            .range(prefix.as_str()..)
            .map_err(|e| catalog_err("range rls_policies", e))?
        {
            let (key, value) = entry.map_err(|e| catalog_err("read rls", e))?;
            if !key.value().starts_with(prefix.as_str()) {
                break;
            }
            let s: StoredRlsPolicy =
                zerompk::from_msgpack(value.value()).map_err(|e| catalog_err("deser rls", e))?;
            out.push(s);
        }
        Ok(out)
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
            .range(..)
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

    fn sample_policy(tenant_id: u64, collection: &str, name: &str) -> RlsPolicy {
        RlsPolicy {
            name: name.into(),
            collection: collection.into(),
            display_collection: collection.into(),
            tenant_id,
            policy_type: PolicyType::Read,
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
        let stored = StoredRlsPolicy::from_runtime(&runtime, DatabaseId::DEFAULT, "").unwrap();
        catalog.put_rls_policy(&stored).unwrap();

        let loaded = catalog.get_rls_policy(1, "users", "p1").unwrap().unwrap();
        let runtime2 = loaded.to_runtime(&catalog).unwrap();
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
            let stored = StoredRlsPolicy::from_runtime(
                &sample_policy(tenant, "x", name),
                DatabaseId::DEFAULT,
                "",
            )
            .unwrap();
            catalog.put_rls_policy(&stored).unwrap();
        }
        let all = catalog.load_all_rls_policies().unwrap();
        assert_eq!(all.len(), 3);
    }

    /// A row whose collection is absent from the catalog cannot be typed;
    /// `rehydrate` installs it as a restrictive deny-all under the same key
    /// instead of dropping it or admitting every row.
    #[test]
    fn rehydrate_without_the_collection_installs_a_restrictive_deny_all() {
        let catalog = make_catalog();
        let mut runtime = sample_policy(1, "ghost", "p1");
        runtime.mode = PolicyMode::Permissive;
        let stored =
            StoredRlsPolicy::from_runtime(&runtime, DatabaseId::DEFAULT, "owner = $auth.id")
                .unwrap();

        assert!(stored.to_runtime(&catalog).is_err());
        let installed = stored.rehydrate(&catalog);
        assert_eq!(installed.name, "p1");
        assert_eq!(installed.collection, "ghost");
        assert_eq!(installed.mode, PolicyMode::Restrictive);
        assert!(matches!(
            installed.compiled_predicate,
            Some(RlsPredicate::AlwaysFalse)
        ));
    }
}
