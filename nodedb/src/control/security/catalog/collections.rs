// SPDX-License-Identifier: BUSL-1.1

//! Collection metadata operations for the system catalog.
//!
//! The storage key is `(database_id: u64, "{tenant_id}:{name}")`.
//! The inner key preserves the legacy `"{tenant_id}:{name}"` encoding so
//! existing catalog-resolver call sites only need to add `database_id`
//! (always `DatabaseId::DEFAULT` in Tier 1).
//!
//! ## Migration
//!
//! On first boot against pre-migration storage, `migrate_collections()`
//! reads all rows from `_system.collections` (the legacy bare-String-keyed
//! table) and rewrites them under `_system.collections_v2` with
//! `DatabaseId::DEFAULT` prepended. The migration is idempotent: if the
//! v2 table already has rows the migration skips; if the legacy table is
//! absent or empty it is also a no-op.

use nodedb_types::DatabaseId;
use nodedb_types::columnar::schema::{TS_SYSTEM, TS_VALID_FROM, TS_VALID_UNTIL};
use redb::{ReadableDatabase, ReadableTable, ReadableTableMetadata};

use super::types::{COLLECTIONS, COLLECTIONS_LEGACY, StoredCollection, SystemCatalog, catalog_err};

/// Union inferred ingest fields into a collection's schema projection.
///
/// Existing field order and types win; only absent inferred names are
/// appended. Bitemporal collections always expose their reserved BIGINT
/// fields exactly once. Returns `true` when the projection changed.
///
/// Deliberately pure: a collection descriptor is replicated catalog state, so
/// the merged record has to reach storage through the replicated metadata path
/// (see `catalog_entry::persist_collection`) rather than a local write. Mutating
/// the persisted record in place would leave this node's copy at descriptor
/// version N no longer byte-equal to the replicated entry at version N, and
/// replaying that entry after a restart would wedge the metadata applier.
pub fn merge_inferred_fields(
    collection: &mut StoredCollection,
    inferred_fields: &[(String, String)],
) -> bool {
    let mut changed = false;
    for (field, field_type) in inferred_fields {
        // Reserved bitemporal columns are schema-owned. Never let an ingest
        // projection supply their type or add a duplicate; normalization below
        // owns them entirely.
        if collection.bitemporal
            && [TS_SYSTEM, TS_VALID_FROM, TS_VALID_UNTIL].contains(&field.as_str())
        {
            continue;
        }
        if !collection
            .fields
            .iter()
            .any(|(existing, _)| existing == field)
        {
            collection.fields.push((field.clone(), field_type.clone()));
            changed = true;
        }
    }
    if collection.bitemporal {
        for reserved in [TS_SYSTEM, TS_VALID_FROM, TS_VALID_UNTIL] {
            let mut retained = false;
            let before = collection.fields.len();
            collection.fields.retain_mut(|(field, field_type)| {
                if field != reserved {
                    return true;
                }
                if retained {
                    return false;
                }
                retained = true;
                if field_type != "BIGINT" {
                    *field_type = "BIGINT".to_owned();
                    changed = true;
                }
                true
            });
            changed |= collection.fields.len() != before;
            if !retained {
                collection
                    .fields
                    .push((reserved.to_owned(), "BIGINT".to_owned()));
                changed = true;
            }
        }
    }
    changed
}

impl SystemCatalog {
    /// Store a collection record.
    pub fn put_collection(
        &self,
        database_id: DatabaseId,
        coll: &StoredCollection,
    ) -> crate::Result<()> {
        #[cfg(test)]
        if self
            .fail_next_collection_write
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(catalog_err(
                "insert collection",
                "injected collection write failure",
            ));
        }
        let inner_key = format!("{}:{}", coll.tenant_id, coll.name);
        let bytes =
            zerompk::to_msgpack_vec(coll).map_err(|e| catalog_err("serialize collection", e))?;
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        {
            let mut table = write_txn
                .open_table(COLLECTIONS)
                .map_err(|e| catalog_err("open collections", e))?;
            table
                .insert((database_id.as_u64(), inner_key.as_str()), bytes.as_slice())
                .map_err(|e| catalog_err("insert collection", e))?;
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))
    }

    /// Insert a collection only when its catalog key is absent.
    ///
    /// The existence check and insert share one redb write transaction, so
    /// concurrent direct-mode schema announcements cannot overwrite the first
    /// winner after both observed absence.
    pub fn put_collection_if_absent(
        &self,
        database_id: DatabaseId,
        coll: &StoredCollection,
    ) -> crate::Result<bool> {
        let inner_key = format!("{}:{}", coll.tenant_id, coll.name);
        let bytes =
            zerompk::to_msgpack_vec(coll).map_err(|e| catalog_err("serialize collection", e))?;
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        let inserted = {
            let mut table = write_txn
                .open_table(COLLECTIONS)
                .map_err(|e| catalog_err("open collections", e))?;
            if table
                .get((database_id.as_u64(), inner_key.as_str()))
                .map_err(|e| catalog_err("get collection", e))?
                .is_some()
            {
                false
            } else {
                table
                    .insert((database_id.as_u64(), inner_key.as_str()), bytes.as_slice())
                    .map_err(|e| catalog_err("insert collection", e))?;
                true
            }
        };
        write_txn.commit().map_err(|e| catalog_err("commit", e))?;
        Ok(inserted)
    }

    /// Load all collections for a tenant within a database, with the calling
    /// connection's buffered transactional DDL merged in.
    pub fn load_collections_for_tenant(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
    ) -> crate::Result<Vec<StoredCollection>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let committed = load_collections_for_tenant_in(&read_txn, database_id, tenant_id)?;
        Ok(crate::control::catalog_overlay::resolve_tenant_collections(
            database_id,
            tenant_id,
            committed,
        ))
    }

    /// Load every soft-deleted collection across all tenants within a database.
    pub fn load_dropped_collections(
        &self,
        database_id: DatabaseId,
    ) -> crate::Result<Vec<StoredCollection>> {
        self.scan_collections_filtered(database_id, |c| !c.is_active)
    }

    /// Load all collections across all tenants within a database.
    pub fn load_all_collections(
        &self,
        database_id: DatabaseId,
    ) -> crate::Result<Vec<StoredCollection>> {
        self.scan_collections_filtered(database_id, |_| true)
    }

    /// Load every collection in every database.
    pub fn load_all_collections_across_databases(&self) -> crate::Result<Vec<StoredCollection>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(COLLECTIONS)
            .map_err(|e| catalog_err("open collections", e))?;
        let mut collections = Vec::new();
        for entry in table
            .iter()
            .map_err(|e| catalog_err("iterate collections", e))?
        {
            let (_, value) = entry.map_err(|e| catalog_err("read collection", e))?;
            collections.push(
                zerompk::from_msgpack(value.value())
                    .map_err(|e| catalog_err("deser collection", e))?,
            );
        }
        Ok(collections)
    }

    fn scan_collections_filtered(
        &self,
        database_id: DatabaseId,
        predicate: impl Fn(&StoredCollection) -> bool,
    ) -> crate::Result<Vec<StoredCollection>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        scan_collections_filtered_in(&read_txn, database_id, predicate)
    }

    /// Hard-delete a collection row. Returns `true` if a row was
    /// removed, `false` if the row was already absent (idempotent).
    pub fn delete_collection(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        name: &str,
    ) -> crate::Result<bool> {
        let inner_key = format!("{tenant_id}:{name}");
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        let removed;
        {
            let mut table = write_txn
                .open_table(COLLECTIONS)
                .map_err(|e| catalog_err("open collections", e))?;
            removed = table
                .remove((database_id.as_u64(), inner_key.as_str()))
                .map_err(|e| catalog_err("remove collection", e))?
                .is_some();
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))?;
        Ok(removed)
    }

    /// Get a single collection by database_id + tenant_id + name.
    ///
    /// DDL the calling connection has buffered in an open transaction shadows
    /// the committed row, so a transaction resolves names against its own
    /// uncommitted `CREATE` / `ALTER` / `DROP`. Every other session, and every
    /// caller outside a connection scope, reads committed state only.
    pub fn get_collection(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        name: &str,
    ) -> crate::Result<Option<StoredCollection>> {
        let committed = self.get_committed_collection(database_id, tenant_id, name)?;
        Ok(crate::control::catalog_overlay::resolve_collection(
            database_id,
            tenant_id,
            name,
            committed,
        ))
    }

    /// `collection`'s DDL-declared `PRIMARY KEY` column name, if any.
    ///
    /// The resolved `primary_key` a plan carries cannot answer this:
    /// schemaless, columnar, and spatial collections resolve it to `id` by
    /// convention with nothing declared. This reads `declared_primary_key`,
    /// set only by the `PRIMARY KEY` keyword itself, naming the column it
    /// applied `NOT NULL` to. A catalog miss reads as not declared.
    pub fn declared_primary_key(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        name: &str,
    ) -> crate::Result<Option<String>> {
        Ok(self
            .get_collection(database_id, tenant_id, name)?
            .and_then(|c| c.declared_primary_key))
    }

    /// Committed-only read, bypassing the transaction DDL overlay. The
    /// descriptor stamper reads through this: a version derived from an
    /// uncommitted overlay row would stamp two entries at the same version.
    pub fn get_committed_collection(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        name: &str,
    ) -> crate::Result<Option<StoredCollection>> {
        let inner_key = format!("{tenant_id}:{name}");
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(COLLECTIONS)
            .map_err(|e| catalog_err("open collections", e))?;
        match table.get((database_id.as_u64(), inner_key.as_str())) {
            Ok(Some(value)) => {
                let coll: StoredCollection = zerompk::from_msgpack(value.value())
                    .map_err(|e| catalog_err("deser collection", e))?;
                Ok(Some(coll))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(catalog_err("get collection", e)),
        }
    }

    /// Idempotent migration: reads all rows from the legacy
    /// `_system.collections` table (bare `"{tenant_id}:{name}"` key) and
    /// rewrites them under `_system.collections_v2` with
    /// `DatabaseId::DEFAULT` prepended.
    ///
    /// Safe to call on:
    /// - Fresh boot: legacy table absent or empty → no-op.
    /// - Pre-migration boot: legacy rows present → migrated to v2.
    /// - Already-migrated boot: v2 rows already exist → no-op (skips if
    ///   v2 table is non-empty; any duplicate put is an idempotent
    ///   overwrite because the key+value are identical).
    pub fn migrate_collections(&self) -> crate::Result<()> {
        // Check legacy table existence and emptiness.
        let legacy_rows: Vec<(String, Vec<u8>)> = {
            let txn = self
                .db
                .begin_read()
                .map_err(|e| catalog_err("migrate_collections read txn", e))?;
            match txn.open_table(COLLECTIONS_LEGACY) {
                Ok(table) => {
                    let iter = table
                        .iter()
                        .map_err(|e| catalog_err("migrate_collections iter", e))?;
                    let mut rows = Vec::new();
                    for row in iter {
                        let (k, v) = row.map_err(|e| catalog_err("migrate_collections row", e))?;
                        rows.push((k.value().to_string(), v.value().to_vec()));
                    }
                    rows
                }
                Err(_) => Vec::new(), // legacy table does not exist yet
            }
        };

        if legacy_rows.is_empty() {
            return Ok(());
        }

        // Check if v2 is already populated (already-migrated boot).
        let v2_empty = {
            let txn = self
                .db
                .begin_read()
                .map_err(|e| catalog_err("migrate_collections v2 check txn", e))?;
            match txn.open_table(COLLECTIONS) {
                Ok(table) => table
                    .is_empty()
                    .map_err(|e| catalog_err("migrate_collections v2 is_empty", e))?,
                Err(_) => true,
            }
        };
        if !v2_empty {
            // Already migrated — idempotent no-op.
            return Ok(());
        }

        // Write all legacy rows into v2 under DatabaseId::DEFAULT.
        let db_id = DatabaseId::DEFAULT.as_u64();
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("migrate_collections write txn", e))?;
        {
            let mut table = write_txn
                .open_table(COLLECTIONS)
                .map_err(|e| catalog_err("migrate_collections open v2", e))?;
            for (inner_key, bytes) in &legacy_rows {
                table
                    .insert((db_id, inner_key.as_str()), bytes.as_slice())
                    .map_err(|e| catalog_err("migrate_collections insert v2", e))?;
            }
        }
        write_txn
            .commit()
            .map_err(|e| catalog_err("migrate_collections commit", e))
    }
}

/// Body of [`SystemCatalog::load_collections_for_tenant`], over an already-open
/// read transaction so the read-only catalog handle can reuse it verbatim.
pub(super) fn load_collections_for_tenant_in(
    read_txn: &redb::ReadTransaction,
    database_id: DatabaseId,
    tenant_id: u64,
) -> crate::Result<Vec<StoredCollection>> {
    let prefix = format!("{tenant_id}:");
    let db_id = database_id.as_u64();
    let table = read_txn
        .open_table(COLLECTIONS)
        .map_err(|e| catalog_err("open collections", e))?;
    let mut colls = Vec::new();
    // Range over all entries with matching database_id prefix.
    let range_start = (db_id, "");
    let range_end = (db_id + 1, "");
    for entry in table
        .range(range_start..range_end)
        .map_err(|e| catalog_err("range collections", e))?
    {
        let (key, value) = entry.map_err(|e| catalog_err("read collection", e))?;
        let (_, inner) = key.value();
        if inner.starts_with(&prefix) {
            let coll: StoredCollection = zerompk::from_msgpack(value.value())
                .map_err(|e| catalog_err("deser collection", e))?;
            if coll.is_active {
                colls.push(coll);
            }
        }
    }
    Ok(colls)
}

/// Body of [`SystemCatalog::load_all_collections`] / `scan_collections_filtered`,
/// over an already-open read transaction so the read-only catalog handle can
/// reuse it verbatim.
pub(super) fn scan_collections_filtered_in(
    read_txn: &redb::ReadTransaction,
    database_id: DatabaseId,
    predicate: impl Fn(&StoredCollection) -> bool,
) -> crate::Result<Vec<StoredCollection>> {
    let db_id = database_id.as_u64();
    let table = read_txn
        .open_table(COLLECTIONS)
        .map_err(|e| catalog_err("open collections", e))?;
    let mut colls = Vec::new();
    let range_start = (db_id, "");
    let range_end = (db_id + 1, "");
    for entry in table
        .range(range_start..range_end)
        .map_err(|e| catalog_err("range collections filter", e))?
    {
        let (_, value) = entry.map_err(|e| catalog_err("read collection", e))?;
        let coll: StoredCollection =
            zerompk::from_msgpack(value.value()).map_err(|e| catalog_err("deser collection", e))?;
        if predicate(&coll) {
            colls.push(coll);
        }
    }
    Ok(colls)
}

#[cfg(test)]
mod tests {
    use nodedb_types::CollectionType;

    use super::*;
    use crate::control::security::catalog::types::{COLLECTIONS_LEGACY, StoredCollection};

    fn open_catalog() -> (tempfile::TempDir, SystemCatalog) {
        let dir = tempfile::tempdir().unwrap();
        let cat = SystemCatalog::open(&dir.path().join("system.redb")).unwrap();
        (dir, cat)
    }

    fn make_coll(tenant_id: u64, name: &str) -> StoredCollection {
        let mut c = StoredCollection::new(tenant_id, name, "admin");
        c.collection_type = CollectionType::document();
        c
    }

    #[test]
    fn put_get_roundtrip() {
        let (_dir, cat) = open_catalog();
        let coll = make_coll(1, "users");
        cat.put_collection(DatabaseId::DEFAULT, &coll).unwrap();
        let fetched = cat.get_collection(DatabaseId::DEFAULT, 1, "users").unwrap();
        assert!(fetched.is_some());
        assert_eq!(fetched.unwrap().name, "users");
    }

    #[test]
    fn missing_returns_none() {
        let (_dir, cat) = open_catalog();
        assert!(
            cat.get_collection(DatabaseId::DEFAULT, 1, "ghost")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn delete_is_idempotent() {
        let (_dir, cat) = open_catalog();
        cat.put_collection(DatabaseId::DEFAULT, &make_coll(1, "users"))
            .unwrap();
        assert!(
            cat.delete_collection(DatabaseId::DEFAULT, 1, "users")
                .unwrap()
        );
        assert!(
            !cat.delete_collection(DatabaseId::DEFAULT, 1, "users")
                .unwrap()
        );
    }

    #[test]
    fn merge_inferred_fields_unions_disjoint_updates_without_reordering() {
        let mut coll = make_coll(1, "metrics");
        coll.fields = vec![("existing".to_owned(), "VARCHAR".to_owned())];

        assert!(merge_inferred_fields(
            &mut coll,
            &[("first".to_owned(), "BIGINT".to_owned())]
        ));
        assert!(merge_inferred_fields(
            &mut coll,
            &[("second".to_owned(), "FLOAT".to_owned())]
        ));
        // A known name never re-types an existing column, and reports no change.
        assert!(!merge_inferred_fields(
            &mut coll,
            &[("first".to_owned(), "BOOLEAN".to_owned())]
        ));

        assert_eq!(
            coll.fields,
            vec![
                ("existing".to_owned(), "VARCHAR".to_owned()),
                ("first".to_owned(), "BIGINT".to_owned()),
                ("second".to_owned(), "FLOAT".to_owned()),
            ]
        );
    }

    #[test]
    fn merge_inferred_fields_adds_bitemporal_reserved_fields_once() {
        let mut coll = make_coll(1, "audit");
        coll.bitemporal = true;
        coll.fields = vec![(TS_SYSTEM.to_owned(), "BIGINT".to_owned())];

        assert!(merge_inferred_fields(
            &mut coll,
            &[("value".to_owned(), "FLOAT".to_owned())]
        ));
        assert!(!merge_inferred_fields(&mut coll, &[]));
        for reserved in [TS_SYSTEM, TS_VALID_FROM, TS_VALID_UNTIL] {
            assert_eq!(
                coll.fields
                    .iter()
                    .filter(|(field, _)| field == reserved)
                    .count(),
                1
            );
        }
    }

    #[test]
    fn merge_inferred_fields_normalizes_bitemporal_reserved_types_and_duplicates() {
        let mut coll = make_coll(1, "audit-normalized");
        coll.bitemporal = true;
        coll.fields = vec![
            (TS_SYSTEM.to_owned(), "VARCHAR".to_owned()),
            (TS_SYSTEM.to_owned(), "FLOAT".to_owned()),
            ("value".to_owned(), "FLOAT".to_owned()),
        ];

        assert!(merge_inferred_fields(
            &mut coll,
            &[
                (TS_VALID_FROM.to_owned(), "VARCHAR".to_owned()),
                (TS_VALID_UNTIL.to_owned(), "BOOLEAN".to_owned()),
            ]
        ));

        for reserved in [TS_SYSTEM, TS_VALID_FROM, TS_VALID_UNTIL] {
            assert_eq!(
                coll.fields
                    .iter()
                    .filter(|(field, _)| field == reserved)
                    .count(),
                1
            );
            assert_eq!(
                coll.fields.iter().find(|(field, _)| field == reserved),
                Some(&(reserved.to_owned(), "BIGINT".to_owned()))
            );
        }
    }

    #[test]
    fn load_for_tenant_filters_correctly() {
        let (_dir, cat) = open_catalog();
        cat.put_collection(DatabaseId::DEFAULT, &make_coll(1, "a"))
            .unwrap();
        cat.put_collection(DatabaseId::DEFAULT, &make_coll(1, "b"))
            .unwrap();
        cat.put_collection(DatabaseId::DEFAULT, &make_coll(2, "c"))
            .unwrap();
        let t1 = cat
            .load_collections_for_tenant(DatabaseId::DEFAULT, 1)
            .unwrap();
        assert_eq!(t1.len(), 2);
        let t2 = cat
            .load_collections_for_tenant(DatabaseId::DEFAULT, 2)
            .unwrap();
        assert_eq!(t2.len(), 1);
    }

    // ── Migration tests ──────────────────────────────────────────────────

    /// Helper: write a legacy (bare string key) row directly so we can
    /// test the migration without going through put_collection.
    fn insert_legacy_row(cat: &SystemCatalog, coll: &StoredCollection) {
        let key = format!("{}:{}", coll.tenant_id, coll.name);
        let bytes = zerompk::to_msgpack_vec(coll).unwrap();
        let txn = cat.db.begin_write().unwrap();
        {
            let mut table = txn.open_table(COLLECTIONS_LEGACY).unwrap();
            table.insert(key.as_str(), bytes.as_slice()).unwrap();
        }
        txn.commit().unwrap();
    }

    #[test]
    fn fresh_boot_migration_is_noop() {
        let (_dir, cat) = open_catalog();
        // No legacy rows → migration is a no-op.
        cat.migrate_collections().unwrap();
        assert!(
            cat.load_all_collections(DatabaseId::DEFAULT)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn pre_migration_boot_migrates_all_rows() {
        let (_dir, cat) = open_catalog();
        let coll1 = make_coll(1, "widgets");
        let coll2 = make_coll(2, "orders");
        insert_legacy_row(&cat, &coll1);
        insert_legacy_row(&cat, &coll2);

        cat.migrate_collections().unwrap();

        let w = cat
            .get_collection(DatabaseId::DEFAULT, 1, "widgets")
            .unwrap();
        assert!(w.is_some(), "widgets must be accessible after migration");
        let o = cat
            .get_collection(DatabaseId::DEFAULT, 2, "orders")
            .unwrap();
        assert!(o.is_some(), "orders must be accessible after migration");
    }

    #[test]
    fn already_migrated_boot_is_idempotent() {
        let (_dir, cat) = open_catalog();
        // Write a v2 row directly (simulating already-migrated).
        cat.put_collection(DatabaseId::DEFAULT, &make_coll(1, "existing"))
            .unwrap();

        // Also insert a legacy row that would conflict if re-migrated.
        let coll_legacy = make_coll(1, "existing");
        insert_legacy_row(&cat, &coll_legacy);

        // Migration should be a no-op (v2 non-empty).
        cat.migrate_collections().unwrap();

        let all = cat.load_all_collections(DatabaseId::DEFAULT).unwrap();
        assert_eq!(all.len(), 1, "should still be 1 row, not duplicated");
    }
}
