// SPDX-License-Identifier: BUSL-1.1

//! Redaction policy persistence in the system catalog.
//!
//! `RedactionPolicy` (the runtime shape) carries `Vec<RedactionRule>`, and
//! `RedactionRule::mode` is a `RedactionMode` enum with a `Mask(String)`
//! payload — serde-only shapes that don't fit zerompk's `ToMessagePack`
//! derive. `StoredRedactionPolicy` flattens the rule list into a single
//! JSON string (via sonic_rs) so the whole record can be msgpack-encoded
//! by zerompk like every other catalog row.
//!
//! Conversions: [`StoredRedactionPolicy::from_runtime`] for serialization,
//! [`StoredRedactionPolicy::to_runtime`] for replay on apply / boot.

use redb::{ReadableDatabase, ReadableTable, TableDefinition};

use crate::control::security::redaction::{RedactionPolicy, RedactionRule};

use super::types::{SystemCatalog, catalog_err};

/// Table: `"{tenant_id}:{collection}:{for_role}"` → MessagePack
/// `StoredRedactionPolicy`.
pub(super) const REDACTION_POLICIES: TableDefinition<&str, &[u8]> =
    TableDefinition::new("_system.redaction_policies");

/// Catalog-shape redaction policy. `rules_json` is the sonic_rs-encoded
/// version of the runtime `Vec<RedactionRule>` so zerompk can derive the
/// encoder for the rest of the record.
#[derive(zerompk::ToMessagePack, zerompk::FromMessagePack, Debug, Clone)]
#[msgpack(map)]
pub struct StoredRedactionPolicy {
    pub tenant_id: u64,
    /// `db_qualified` collection — the storage/lookup key. Never shown to a
    /// user; see `display_collection`.
    pub collection: String,
    /// The collection name as the user wrote it, unqualified. Display-only.
    /// Never empty.
    pub display_collection: String,
    pub for_role: String,
    pub name: String,
    /// JSON-serialized `Vec<RedactionRule>`.
    pub rules_json: String,
}

impl StoredRedactionPolicy {
    pub fn from_runtime(p: &RedactionPolicy) -> crate::Result<Self> {
        let rules_json =
            sonic_rs::to_string(&p.rules).map_err(|e| catalog_err("ser redaction rules", e))?;
        Ok(Self {
            tenant_id: p.tenant_id,
            collection: p.collection.clone(),
            display_collection: p.display_collection.clone(),
            for_role: p.for_role.clone(),
            name: p.name.clone(),
            rules_json,
        })
    }

    pub fn to_runtime(&self) -> crate::Result<RedactionPolicy> {
        let rules: Vec<RedactionRule> = sonic_rs::from_str(&self.rules_json)
            .map_err(|e| catalog_err("deser redaction rules", e))?;
        // Every writer stores the display name, so an empty one marks a
        // malformed row.
        if self.display_collection.is_empty() {
            return Err(catalog_err(
                "deser redaction",
                format!(
                    "policy '{}' on '{}' has an empty display_collection",
                    self.name, self.collection
                ),
            ));
        }
        Ok(RedactionPolicy {
            name: self.name.clone(),
            tenant_id: self.tenant_id,
            collection: self.collection.clone(),
            display_collection: self.display_collection.clone(),
            for_role: self.for_role.clone(),
            rules,
        })
    }

    fn redb_key(&self) -> String {
        redaction_key(self.tenant_id, &self.collection, &self.for_role)
    }
}

fn redaction_key(tenant_id: u64, collection: &str, for_role: &str) -> String {
    format!("{tenant_id}:{collection}:{for_role}")
}

impl SystemCatalog {
    /// Insert or overwrite a redaction policy record.
    pub fn put_redaction_policy(&self, stored: &StoredRedactionPolicy) -> crate::Result<()> {
        let key = stored.redb_key();
        let bytes = zerompk::to_msgpack_vec(stored).map_err(|e| catalog_err("ser redaction", e))?;
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        {
            let mut table = write_txn
                .open_table(REDACTION_POLICIES)
                .map_err(|e| catalog_err("open redaction_policies", e))?;
            table
                .insert(key.as_str(), bytes.as_slice())
                .map_err(|e| catalog_err("insert redaction", e))?;
        }
        write_txn
            .commit()
            .map_err(|e| catalog_err("commit redaction", e))
    }

    /// Delete a redaction policy. Returns `true` if a row was removed.
    pub fn delete_redaction_policy(
        &self,
        tenant_id: u64,
        collection: &str,
        for_role: &str,
    ) -> crate::Result<bool> {
        let key = redaction_key(tenant_id, collection, for_role);
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        let existed;
        {
            let mut table = write_txn
                .open_table(REDACTION_POLICIES)
                .map_err(|e| catalog_err("open redaction_policies", e))?;
            existed = table
                .remove(key.as_str())
                .map_err(|e| catalog_err("remove redaction", e))?
                .is_some();
        }
        write_txn
            .commit()
            .map_err(|e| catalog_err("commit redaction", e))?;
        Ok(existed)
    }

    /// Read a single redaction policy by full key.
    pub fn get_redaction_policy(
        &self,
        tenant_id: u64,
        collection: &str,
        for_role: &str,
    ) -> crate::Result<Option<StoredRedactionPolicy>> {
        let key = redaction_key(tenant_id, collection, for_role);
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(REDACTION_POLICIES)
            .map_err(|e| catalog_err("open redaction_policies", e))?;
        match table.get(key.as_str()) {
            Ok(Some(value)) => {
                let s: StoredRedactionPolicy = zerompk::from_msgpack(value.value())
                    .map_err(|e| catalog_err("deser redaction", e))?;
                Ok(Some(s))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(catalog_err("get redaction", e)),
        }
    }

    /// Every redaction policy attached to one collection of one tenant.
    ///
    /// `collection` is the database-qualified name the key carries. The scan
    /// is bounded to that collection's key prefix.
    pub fn list_redaction_policies_for_collection(
        &self,
        tenant_id: u64,
        collection: &str,
    ) -> crate::Result<Vec<StoredRedactionPolicy>> {
        let prefix = format!("{tenant_id}:{collection}:");
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(REDACTION_POLICIES)
            .map_err(|e| catalog_err("open redaction_policies", e))?;
        let mut out = Vec::new();
        for entry in table
            .range(prefix.as_str()..)
            .map_err(|e| catalog_err("range redaction_policies", e))?
        {
            let (key, value) = entry.map_err(|e| catalog_err("read redaction", e))?;
            if !key.value().starts_with(prefix.as_str()) {
                break;
            }
            let s: StoredRedactionPolicy = zerompk::from_msgpack(value.value())
                .map_err(|e| catalog_err("deser redaction", e))?;
            out.push(s);
        }
        Ok(out)
    }

    /// Load every redaction policy across every tenant. Used by boot replay.
    pub fn load_all_redaction_policies(&self) -> crate::Result<Vec<StoredRedactionPolicy>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(REDACTION_POLICIES)
            .map_err(|e| catalog_err("open redaction_policies", e))?;
        let mut out = Vec::new();
        for entry in table
            .range(..)
            .map_err(|e| catalog_err("range redaction_policies", e))?
        {
            let (_, value) = entry.map_err(|e| catalog_err("read redaction", e))?;
            let s: StoredRedactionPolicy = zerompk::from_msgpack(value.value())
                .map_err(|e| catalog_err("deser redaction", e))?;
            out.push(s);
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::redaction::RedactionMode;

    fn make_catalog() -> SystemCatalog {
        let dir = tempfile::tempdir().unwrap();
        SystemCatalog::open(&dir.path().join("system.redb")).unwrap()
    }

    fn sample_policy(tenant_id: u64, collection: &str, for_role: &str) -> RedactionPolicy {
        RedactionPolicy {
            name: format!("policy_{for_role}"),
            tenant_id,
            collection: collection.into(),
            display_collection: collection.into(),
            for_role: for_role.into(),
            rules: vec![
                RedactionRule {
                    field: "email".into(),
                    mode: RedactionMode::Mask("***@***.com".into()),
                },
                RedactionRule {
                    field: "ssn".into(),
                    mode: RedactionMode::Hash,
                },
                RedactionRule {
                    field: "notes".into(),
                    mode: RedactionMode::Null,
                },
            ],
        }
    }

    /// A row without a display name is malformed and does not rehydrate.
    #[test]
    fn a_row_without_display_name_is_refused() {
        let mut stored =
            StoredRedactionPolicy::from_runtime(&sample_policy(1, "users", "support")).unwrap();
        stored.display_collection.clear();
        assert!(stored.to_runtime().is_err());
    }

    #[test]
    fn put_get_delete_roundtrip() {
        let catalog = make_catalog();
        let runtime = sample_policy(1, "users", "support");
        let stored = StoredRedactionPolicy::from_runtime(&runtime).unwrap();
        catalog.put_redaction_policy(&stored).unwrap();

        let loaded = catalog
            .get_redaction_policy(1, "users", "support")
            .unwrap()
            .unwrap();
        let runtime2 = loaded.to_runtime().unwrap();
        assert_eq!(runtime2.name, "policy_support");
        assert_eq!(runtime2.collection, "users");
        assert_eq!(runtime2.for_role, "support");
        assert_eq!(runtime2.rules.len(), 3);

        assert!(matches!(
            &runtime2.rules[0].mode,
            RedactionMode::Mask(mask) if mask == "***@***.com"
        ));
        assert_eq!(runtime2.rules[0].field, "email");
        assert!(matches!(runtime2.rules[1].mode, RedactionMode::Hash));
        assert_eq!(runtime2.rules[1].field, "ssn");
        assert!(matches!(runtime2.rules[2].mode, RedactionMode::Null));
        assert_eq!(runtime2.rules[2].field, "notes");

        assert!(
            catalog
                .delete_redaction_policy(1, "users", "support")
                .unwrap()
        );
        assert!(
            catalog
                .get_redaction_policy(1, "users", "support")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn load_all_returns_every_tenant() {
        let catalog = make_catalog();
        for (tenant, role) in [(1, "support"), (1, "analyst"), (2, "support")] {
            let stored =
                StoredRedactionPolicy::from_runtime(&sample_policy(tenant, "x", role)).unwrap();
            catalog.put_redaction_policy(&stored).unwrap();
        }
        let all = catalog.load_all_redaction_policies().unwrap();
        assert_eq!(all.len(), 3);
    }

    #[test]
    fn distinct_roles_coexist_and_delete_independently() {
        // Two policies differing only in `for_role` on the same
        // (tenant, collection) must be independently addressable — this
        // pins the 3-part key shape.
        let catalog = make_catalog();
        let support =
            StoredRedactionPolicy::from_runtime(&sample_policy(1, "users", "support")).unwrap();
        let analyst =
            StoredRedactionPolicy::from_runtime(&sample_policy(1, "users", "analyst")).unwrap();
        catalog.put_redaction_policy(&support).unwrap();
        catalog.put_redaction_policy(&analyst).unwrap();

        assert!(
            catalog
                .get_redaction_policy(1, "users", "support")
                .unwrap()
                .is_some()
        );
        assert!(
            catalog
                .get_redaction_policy(1, "users", "analyst")
                .unwrap()
                .is_some()
        );

        assert!(
            catalog
                .delete_redaction_policy(1, "users", "support")
                .unwrap()
        );
        assert!(
            catalog
                .get_redaction_policy(1, "users", "support")
                .unwrap()
                .is_none()
        );
        assert!(
            catalog
                .get_redaction_policy(1, "users", "analyst")
                .unwrap()
                .is_some()
        );
    }
}
