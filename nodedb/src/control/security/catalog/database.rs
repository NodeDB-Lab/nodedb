// SPDX-License-Identifier: BUSL-1.1

//! Database catalog ops for `_system.databases`, `_system.databases_by_name`,
//! and `_system.database_hwm`.
//!
//! Every method that modifies both the forward (`DATABASES`) and reverse
//! (`DATABASES_BY_NAME`) tables does so in a single redb write txn so the
//! two directions can never drift.

use nodedb_types::DatabaseId;
use redb::{ReadableDatabase, ReadableTable, ReadableTableMetadata};

use super::database_types::DatabaseDescriptor;
use super::types::{DATABASE_HWM, DATABASES, DATABASES_BY_NAME, SystemCatalog, catalog_err};

/// Singleton row key for the hwm table.
const HWM_KEY: &str = "global";

impl SystemCatalog {
    // ── database_hwm ──────────────────────────────────────────────────────

    /// Persist the database allocator high-watermark.
    pub fn put_database_hwm(&self, hwm: u64) -> crate::Result<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("database_hwm write txn", e))?;
        {
            let mut table = txn
                .open_table(DATABASE_HWM)
                .map_err(|e| catalog_err("open database_hwm", e))?;
            table
                .insert(HWM_KEY, hwm)
                .map_err(|e| catalog_err("insert database_hwm", e))?;
        }
        txn.commit()
            .map_err(|e| catalog_err("database_hwm commit", e))
    }

    /// Load the persisted database hwm, or `0` if none recorded yet.
    pub fn get_database_hwm(&self) -> crate::Result<u64> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("database_hwm read txn", e))?;
        let table = txn
            .open_table(DATABASE_HWM)
            .map_err(|e| catalog_err("open database_hwm", e))?;
        match table
            .get(HWM_KEY)
            .map_err(|e| catalog_err("get database_hwm", e))?
        {
            Some(v) => Ok(v.value()),
            None => Ok(0),
        }
    }

    // ── databases + databases_by_name ──────────────────────────────────────

    /// Insert or overwrite a database descriptor. Writes both the forward
    /// (`DATABASES`) and reverse (`DATABASES_BY_NAME`) rows in one txn.
    ///
    /// When renaming (calling with a descriptor whose `name` has changed),
    /// the old name's reverse row is removed inside the same txn.
    pub fn put_database(&self, descriptor: &DatabaseDescriptor) -> crate::Result<()> {
        let bytes = zerompk::to_msgpack_vec(descriptor)
            .map_err(|e| catalog_err("serialize DatabaseDescriptor", e))?;
        let db_id = descriptor.id.as_u64();

        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("databases write txn", e))?;
        {
            // Read the old name (if any) to remove the stale reverse row.
            let old_name: Option<String> = {
                let read_table = txn
                    .open_table(DATABASES)
                    .map_err(|e| catalog_err("open databases for old-name read", e))?;
                if let Some(existing) = read_table
                    .get(db_id)
                    .map_err(|e| catalog_err("get databases for old-name read", e))?
                {
                    let existing_desc: DatabaseDescriptor = zerompk::from_msgpack(existing.value())
                        .map_err(|e| catalog_err("deser old DatabaseDescriptor", e))?;
                    if existing_desc.name != descriptor.name {
                        Some(existing_desc.name)
                    } else {
                        None
                    }
                } else {
                    None
                }
            };

            let mut fwd = txn
                .open_table(DATABASES)
                .map_err(|e| catalog_err("open databases fwd", e))?;
            fwd.insert(db_id, bytes.as_slice())
                .map_err(|e| catalog_err("insert databases", e))?;

            let mut rev = txn
                .open_table(DATABASES_BY_NAME)
                .map_err(|e| catalog_err("open databases_by_name", e))?;
            // Remove stale reverse row if name changed.
            if let Some(old) = old_name {
                rev.remove(old.as_str())
                    .map_err(|e| catalog_err("remove stale databases_by_name", e))?;
            }
            rev.insert(descriptor.name.as_str(), db_id)
                .map_err(|e| catalog_err("insert databases_by_name", e))?;
        }
        txn.commit().map_err(|e| catalog_err("databases commit", e))
    }

    /// Get a database descriptor by id. Returns `None` if not found.
    pub fn get_database(&self, id: DatabaseId) -> crate::Result<Option<DatabaseDescriptor>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("databases read txn", e))?;
        let table = txn
            .open_table(DATABASES)
            .map_err(|e| catalog_err("open databases", e))?;
        match table
            .get(id.as_u64())
            .map_err(|e| catalog_err("get databases", e))?
        {
            Some(v) => {
                let desc: DatabaseDescriptor = zerompk::from_msgpack(v.value())
                    .map_err(|e| catalog_err("deser DatabaseDescriptor", e))?;
                Ok(Some(desc))
            }
            None => Ok(None),
        }
    }

    /// Resolve a database name to its `DatabaseId`. Returns `None` if
    /// no database with that name exists.
    pub fn get_database_id_by_name(&self, name: &str) -> crate::Result<Option<DatabaseId>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("databases_by_name read txn", e))?;
        let table = txn
            .open_table(DATABASES_BY_NAME)
            .map_err(|e| catalog_err("open databases_by_name", e))?;
        match table
            .get(name)
            .map_err(|e| catalog_err("get databases_by_name", e))?
        {
            Some(v) => Ok(Some(DatabaseId::new(v.value()))),
            None => Ok(None),
        }
    }

    /// Resolve a database id to its name. Returns `None` if not found.
    pub fn get_database_name_by_id(&self, id: DatabaseId) -> crate::Result<Option<String>> {
        match self.get_database(id)? {
            Some(desc) => Ok(Some(desc.name)),
            None => Ok(None),
        }
    }

    /// Remove a database descriptor and its reverse-lookup row.
    /// Idempotent: removing a missing database succeeds silently.
    pub fn delete_database(&self, id: DatabaseId) -> crate::Result<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("databases delete txn", e))?;
        {
            let mut fwd = txn
                .open_table(DATABASES)
                .map_err(|e| catalog_err("open databases for delete", e))?;
            let removed = fwd
                .remove(id.as_u64())
                .map_err(|e| catalog_err("remove databases", e))?;
            if let Some(v) = removed {
                let desc: DatabaseDescriptor = zerompk::from_msgpack(v.value())
                    .map_err(|e| catalog_err("deser databases for delete", e))?;
                let mut rev = txn
                    .open_table(DATABASES_BY_NAME)
                    .map_err(|e| catalog_err("open databases_by_name for delete", e))?;
                rev.remove(desc.name.as_str())
                    .map_err(|e| catalog_err("remove databases_by_name", e))?;
            }
        }
        txn.commit()
            .map_err(|e| catalog_err("databases delete commit", e))
    }

    /// Return `true` if the `_system.databases` table is empty. Used by
    /// the bootstrap path to decide whether to insert `DatabaseId(0) = "default"`.
    pub fn databases_is_empty(&self) -> crate::Result<bool> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("databases_is_empty read txn", e))?;
        let table = txn
            .open_table(DATABASES)
            .map_err(|e| catalog_err("open databases_is_empty", e))?;
        let empty = table
            .is_empty()
            .map_err(|e| catalog_err("databases is_empty", e))?;
        Ok(empty)
    }

    /// Bootstrap: if `_system.databases` is empty, insert `DatabaseId(0) = "default"`.
    /// Idempotent: if already populated, this is a no-op.
    pub fn bootstrap_default_database(&self) -> crate::Result<()> {
        if !self.databases_is_empty()? {
            return Ok(());
        }
        self.put_database(&DatabaseDescriptor::default_db())
    }

    /// List all databases. Used by `SHOW DATABASES` and migration.
    pub fn list_databases(&self) -> crate::Result<Vec<DatabaseDescriptor>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("list_databases read txn", e))?;
        list_databases_in(&txn)
    }

    /// Calculate the real storage size in bytes used by a database across all catalog tables.
    ///
    /// Sums the persisted key and value byte lengths for all entries belonging to `db_id`
    /// across collections, surrogate indexes, topics, streams, materialized views,
    /// policies, procedures, triggers, functions, and database descriptors.
    pub fn database_storage_bytes(&self, db_id: DatabaseId) -> crate::Result<u64> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn for database_storage_bytes", e))?;

        let id = db_id.as_u64();
        let mut total_bytes: u64 = 0;

        // 1. Collections: (database_id: u64, "{tenant_id}:{name}") -> msgpack
        if let Ok(table) = read_txn.open_table(super::types::COLLECTIONS) {
            if let Ok(range) = table.iter() {
                for entry in range {
                    if let Ok((k, v)) = entry {
                        let (db, name) = k.value();
                        if db == id {
                            total_bytes = total_bytes
                                .saturating_add(8 + name.len() as u64 + v.value().len() as u64);
                        }
                    }
                }
            }
        }

        // 2. Surrogate PKs: (database_id: u64, tenant_id: u64, collection, encoded_pk) -> u32
        if let Ok(table) = read_txn.open_table(super::types::SURROGATE_PK_V3) {
            if let Ok(range) = table.iter() {
                for entry in range {
                    if let Ok((k, _v)) = entry {
                        let (db, _tid, coll, pk) = k.value();
                        if db == id {
                            total_bytes = total_bytes
                                .saturating_add(16 + coll.len() as u64 + pk.len() as u64 + 4);
                        }
                    }
                }
            }
        }

        // 3. Surrogate PK Rev: (database_id: u64, tenant_id: u64, collection, surrogate: u32) -> encoded pk
        if let Ok(table) = read_txn.open_table(super::types::SURROGATE_PK_REV_V3) {
            if let Ok(range) = table.iter() {
                for entry in range {
                    if let Ok((k, v)) = entry {
                        let (db, _tid, coll, _surr) = k.value();
                        if db == id {
                            total_bytes = total_bytes
                                .saturating_add(20 + coll.len() as u64 + v.value().len() as u64);
                        }
                    }
                }
            }
        }

        // 4. Topic messages: [database_id: be u64]...
        if let Ok(table) = read_txn.open_table(super::types::TOPIC_MESSAGES) {
            let be_bytes = id.to_be_bytes();
            if let Ok(range) = table.iter() {
                for entry in range {
                    if let Ok((k, v)) = entry {
                        let k_bytes = k.value();
                        if k_bytes.starts_with(&be_bytes) {
                            total_bytes = total_bytes
                                .saturating_add(k_bytes.len() as u64 + v.value().len() as u64);
                        }
                    }
                }
            }
        }

        // 5. Ep Topics: "v2/{database_id}/{tenant_id}..."
        if let Ok(table) = read_txn.open_table(super::types::TOPICS_EP) {
            let prefix = format!("v2/{id}/");
            if let Ok(range) = table.iter() {
                for entry in range {
                    if let Ok((k, v)) = entry {
                        let k_str = k.value();
                        if k_str.starts_with(&prefix) {
                            total_bytes = total_bytes
                                .saturating_add(k_str.len() as u64 + v.value().len() as u64);
                        }
                    }
                }
            }
        }

        // 6. Change streams: "v2/{database_id}/..."
        if let Ok(table) = read_txn.open_table(super::types::CHANGE_STREAMS) {
            let prefix = format!("v2/{id}/");
            if let Ok(range) = table.iter() {
                for entry in range {
                    if let Ok((k, v)) = entry {
                        let k_str = k.value();
                        if k_str.starts_with(&prefix) {
                            total_bytes = total_bytes
                                .saturating_add(k_str.len() as u64 + v.value().len() as u64);
                        }
                    }
                }
            }
        }

        // 7. Consumer groups: "v2:{database_id}:..."
        if let Ok(table) = read_txn.open_table(super::types::CONSUMER_GROUPS) {
            let prefix = format!("v2:{id}:");
            if let Ok(range) = table.iter() {
                for entry in range {
                    if let Ok((k, v)) = entry {
                        let k_str = k.value();
                        if k_str.starts_with(&prefix) {
                            total_bytes = total_bytes
                                .saturating_add(k_str.len() as u64 + v.value().len() as u64);
                        }
                    }
                }
            }
        }

        // 8. Streaming MVs: "v2:{database_id}:..."
        if let Ok(table) = read_txn.open_table(super::types::STREAMING_MVS) {
            let prefix = format!("v2:{id}:");
            if let Ok(range) = table.iter() {
                for entry in range {
                    if let Ok((k, v)) = entry {
                        let k_str = k.value();
                        if k_str.starts_with(&prefix) {
                            total_bytes = total_bytes
                                .saturating_add(k_str.len() as u64 + v.value().len() as u64);
                        }
                    }
                }
            }
        }

        // 9. Continuous Aggregates: (database_id: u64, "{tenant_id}:{name}")
        if let Ok(table) = read_txn.open_table(super::types::CONTINUOUS_AGGREGATES) {
            if let Ok(range) = table.iter() {
                for entry in range {
                    if let Ok((k, v)) = entry {
                        let (db, name) = k.value();
                        if db == id {
                            total_bytes = total_bytes
                                .saturating_add(8 + name.len() as u64 + v.value().len() as u64);
                        }
                    }
                }
            }
        }

        // 10. Retention policies: (database_id: u64, "{tenant_id}:{policy_name}")
        if let Ok(table) = read_txn.open_table(super::types::RETENTION_POLICIES) {
            if let Ok(range) = table.iter() {
                for entry in range {
                    if let Ok((k, v)) = entry {
                        let (db, name) = k.value();
                        if db == id {
                            total_bytes = total_bytes
                                .saturating_add(8 + name.len() as u64 + v.value().len() as u64);
                        }
                    }
                }
            }
        }

        // 11. Alert rules: (database_id: u64, "{tenant_id}:{alert_name}")
        if let Ok(table) = read_txn.open_table(super::types::ALERT_RULES) {
            if let Ok(range) = table.iter() {
                for entry in range {
                    if let Ok((k, v)) = entry {
                        let (db, name) = k.value();
                        if db == id {
                            total_bytes = total_bytes
                                .saturating_add(8 + name.len() as u64 + v.value().len() as u64);
                        }
                    }
                }
            }
        }

        // 12. Synonym groups: "{database_id}:{tenant_id}:{group_name}"
        if let Ok(table) = read_txn.open_table(super::types::SYNONYM_GROUPS) {
            let prefix = format!("{id}:");
            if let Ok(range) = table.iter() {
                for entry in range {
                    if let Ok((k, v)) = entry {
                        let k_str = k.value();
                        if k_str.starts_with(&prefix) {
                            total_bytes = total_bytes
                                .saturating_add(k_str.len() as u64 + v.value().len() as u64);
                        }
                    }
                }
            }
        }

        // 13. Column stats: "{database_id}:{tenant_id}:{collection}:{column}"
        if let Ok(table) = read_txn.open_table(super::types::COLUMN_STATS) {
            let prefix = format!("{id}:");
            if let Ok(range) = table.iter() {
                for entry in range {
                    if let Ok((k, v)) = entry {
                        let k_str = k.value();
                        if k_str.starts_with(&prefix) {
                            total_bytes = total_bytes
                                .saturating_add(k_str.len() as u64 + v.value().len() as u64);
                        }
                    }
                }
            }
        }

        // 14. Database descriptor: `database_id (u64)`
        if let Ok(table) = read_txn.open_table(DATABASES) {
            if let Ok(Some(v)) = table.get(id) {
                total_bytes = total_bytes.saturating_add(8 + v.value().len() as u64);
            }
        }

        // 15. Functions, triggers, procedures, arrays: "v2:{tenant_id}:{database_id}:{name}" or "\0v2:..."
        let mid_pattern = format!(":{id}:");
        for table_def in [
            super::types::FUNCTIONS,
            super::types::TRIGGERS,
            super::types::PROCEDURES,
            super::types::ARRAYS,
            super::types::DEPENDENCIES,
        ] {
            if let Ok(table) = read_txn.open_table(table_def) {
                if let Ok(range) = table.iter() {
                    for entry in range {
                        if let Ok((k, v)) = entry {
                            let k_str = k.value();
                            if k_str.contains(&mid_pattern) {
                                total_bytes = total_bytes
                                    .saturating_add(k_str.len() as u64 + v.value().len() as u64);
                            }
                        }
                    }
                }
            }
        }

        Ok(total_bytes)
    }
}

/// Body of [`SystemCatalog::list_databases`], over an already-open read
/// transaction so the read-only catalog handle can reuse it verbatim.
pub(super) fn list_databases_in(
    txn: &redb::ReadTransaction,
) -> crate::Result<Vec<DatabaseDescriptor>> {
    let table = txn
        .open_table(DATABASES)
        .map_err(|e| catalog_err("open databases list", e))?;
    let mut out = Vec::new();
    let iter = table.iter().map_err(|e| catalog_err("iter databases", e))?;
    for row in iter {
        let (_, v) = row.map_err(|e| catalog_err("iter databases row", e))?;
        let desc: DatabaseDescriptor = zerompk::from_msgpack(v.value())
            .map_err(|e| catalog_err("deser list_databases row", e))?;
        out.push(desc);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn open_catalog() -> (tempfile::TempDir, SystemCatalog) {
        let dir = tempfile::tempdir().unwrap();
        let cat = SystemCatalog::open(&dir.path().join("system.redb")).unwrap();
        (dir, cat)
    }

    #[test]
    fn hwm_fresh_is_zero() {
        let (_dir, cat) = open_catalog();
        assert_eq!(cat.get_database_hwm().unwrap(), 0);
    }

    #[test]
    fn hwm_put_get_roundtrip() {
        let (_dir, cat) = open_catalog();
        cat.put_database_hwm(1024).unwrap();
        assert_eq!(cat.get_database_hwm().unwrap(), 1024);
        cat.put_database_hwm(9999).unwrap();
        assert_eq!(cat.get_database_hwm().unwrap(), 9999);
    }

    /// Opening a catalog must leave the default database resolvable.
    ///
    /// Every fail-closed database lookup — the native handshake among them —
    /// refuses a session whose database is absent, so a freshly opened catalog
    /// that had not bootstrapped answered every new native session with
    /// `3D000 selected database does not exist`.
    #[test]
    fn open_bootstraps_the_default_database() {
        let (_dir, cat) = open_catalog();
        assert!(!cat.databases_is_empty().unwrap());
        let desc = cat.get_database(DatabaseId::DEFAULT).unwrap().unwrap();
        assert_eq!(desc.name, "default");
        assert_eq!(desc.id, DatabaseId::DEFAULT);
    }

    #[test]
    fn bootstrap_inserts_default_into_an_empty_catalog() {
        let dir = tempfile::tempdir().unwrap();
        let cat = SystemCatalog::open(&dir.path().join("system.redb")).unwrap();
        cat.delete_database(DatabaseId::DEFAULT).unwrap();
        assert!(cat.databases_is_empty().unwrap());

        cat.bootstrap_default_database().unwrap();
        assert!(!cat.databases_is_empty().unwrap());
        let desc = cat.get_database(DatabaseId::DEFAULT).unwrap().unwrap();
        assert_eq!(desc.name, "default");
        assert_eq!(desc.id, DatabaseId::DEFAULT);
    }

    #[test]
    fn bootstrap_is_idempotent() {
        let (_dir, cat) = open_catalog();
        cat.bootstrap_default_database().unwrap();
        cat.bootstrap_default_database().unwrap();
        let dbs = cat.list_databases().unwrap();
        assert_eq!(dbs.len(), 1);
    }

    #[test]
    fn put_then_get_roundtrip() {
        let (_dir, cat) = open_catalog();
        cat.bootstrap_default_database().unwrap();
        let desc = cat.get_database(DatabaseId::DEFAULT).unwrap().unwrap();
        assert_eq!(desc.name, "default");
        let by_name = cat.get_database_id_by_name("default").unwrap();
        assert_eq!(by_name, Some(DatabaseId::DEFAULT));
    }

    #[test]
    fn missing_returns_none() {
        let (_dir, cat) = open_catalog();
        assert!(cat.get_database(DatabaseId::new(999)).unwrap().is_none());
        assert!(cat.get_database_id_by_name("ghost").unwrap().is_none());
    }

    #[test]
    fn rename_updates_reverse_lookup() {
        let (_dir, cat) = open_catalog();
        cat.bootstrap_default_database().unwrap();
        let mut desc = cat.get_database(DatabaseId::DEFAULT).unwrap().unwrap();
        desc.name = "renamed_default".to_string();
        cat.put_database(&desc).unwrap();

        assert!(cat.get_database_id_by_name("default").unwrap().is_none());
        assert_eq!(
            cat.get_database_id_by_name("renamed_default").unwrap(),
            Some(DatabaseId::DEFAULT)
        );
    }

    #[test]
    fn delete_is_idempotent() {
        let (_dir, cat) = open_catalog();
        cat.bootstrap_default_database().unwrap();
        cat.delete_database(DatabaseId::DEFAULT).unwrap();
        assert!(cat.get_database(DatabaseId::DEFAULT).unwrap().is_none());
        assert!(cat.get_database_id_by_name("default").unwrap().is_none());
        // second delete is a no-op
        cat.delete_database(DatabaseId::DEFAULT).unwrap();
    }

    #[test]
    fn database_storage_bytes_reflects_actual_usage() {
        let (_dir, cat) = open_catalog();
        cat.bootstrap_default_database().unwrap();
        let initial_bytes = cat.database_storage_bytes(DatabaseId::DEFAULT).unwrap();
        assert!(
            initial_bytes > 0,
            "bootstrapped default database descriptor must contribute non-zero bytes"
        );

        let mut coll = super::super::collection::StoredCollection::new(1, "users", "admin");
        coll.database_id = DatabaseId::DEFAULT;
        cat.put_collection(DatabaseId::DEFAULT, &coll).unwrap();

        let after_coll_bytes = cat.database_storage_bytes(DatabaseId::DEFAULT).unwrap();
        assert!(
            after_coll_bytes > initial_bytes,
            "adding collection must increase database_storage_bytes: {after_coll_bytes} > {initial_bytes}"
        );

        // Unrelated database reports 0 bytes
        assert_eq!(cat.database_storage_bytes(DatabaseId::new(999)).unwrap(), 0);
    }
}
