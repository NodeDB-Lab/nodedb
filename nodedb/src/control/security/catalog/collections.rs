// SPDX-License-Identifier: BUSL-1.1

//! Collection metadata operations for the system catalog.
//!
//! The storage key is `(database_id: u64, "{tenant_id}:{name}")`.

use nodedb_types::DatabaseId;
use nodedb_types::columnar::schema::{TS_SYSTEM, TS_VALID_FROM, TS_VALID_UNTIL};
use redb::{ReadableDatabase, ReadableTable};

use super::types::{COLLECTIONS, StoredCollection, SystemCatalog, catalog_err};

/// Union inferred ingest fields into a collection's schema projection.
///
/// Existing field order and types win; only absent inferred names are
/// appended. Bitemporal collections always expose their reserved BIGINT
/// fields exactly once. Returns `true` when the projection changed.
///
/// `time_column` is the column an ingest inferred for the row time. A
/// timeseries collection whose declared time key names one of its fields
/// stores the row time in that field. Its projection therefore never gains
/// the inferred time column. Every other collection takes it as a field.
///
/// Deliberately pure: a collection descriptor is replicated catalog state, so
/// the merged record has to reach storage through the replicated metadata path
/// (see `catalog_entry::persist_collection`) rather than a local write. Mutating
/// the persisted record in place leaves this node's copy at descriptor
/// version N no longer byte-equal to the replicated entry at version N, and
/// replaying that entry after a restart wedges the metadata applier.
pub fn merge_inferred_fields(
    collection: &mut StoredCollection,
    time_column: Option<&(String, String)>,
    inferred_fields: &[(String, String)],
) -> bool {
    let time_column = time_column.filter(|_| !declares_time_key_field(collection));
    let mut changed = false;
    for (field, field_type) in time_column.into_iter().chain(inferred_fields) {
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

/// Whether a timeseries collection's declared time key names one of its
/// fields. The Data Plane then builds its memtable from that declaration and
/// writes each row's time into that field.
fn declares_time_key_field(collection: &StoredCollection) -> bool {
    let nodedb_types::CollectionType::Columnar(nodedb_types::ColumnarProfile::Timeseries {
        time_key,
        ..
    }) = &collection.collection_type
    else {
        return false;
    };
    collection.fields.iter().any(|(field, _)| field == time_key)
}

impl SystemCatalog {
    /// Store a collection record. A record with no incarnation is refused
    /// with [`crate::Error::CollectionUnstamped`].
    pub fn put_collection(
        &self,
        database_id: DatabaseId,
        coll: &StoredCollection,
    ) -> crate::Result<()> {
        #[cfg(test)]
        if self
            .faults
            .fail_next_collection_write
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(catalog_err(
                "insert collection",
                "injected collection write failure",
            ));
        }
        super::collection_incarnation::require_incarnation(database_id, coll)?;
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
        write_txn.commit().map_err(|e| catalog_err("commit", e))?;
        self.event_defs.install(database_id, coll);
        Ok(())
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
        super::collection_incarnation::require_incarnation(database_id, coll)?;
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
        if inserted {
            self.event_defs.install(database_id, coll);
        }
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
        if removed {
            self.event_defs.remove(database_id, tenant_id, name);
        }
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
    /// uncommitted overlay row stamps two entries at the same version.
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
    use crate::control::security::catalog::types::StoredCollection;

    fn open_catalog() -> (tempfile::TempDir, SystemCatalog) {
        let dir = tempfile::tempdir().unwrap();
        let cat = SystemCatalog::open(&dir.path().join("system.redb")).unwrap();
        (dir, cat)
    }

    fn make_coll(tenant_id: u64, name: &str) -> StoredCollection {
        let mut c = StoredCollection::stamped_for_test(tenant_id, name, "admin");
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

    fn with_event(tenant_id: u64, name: &str) -> StoredCollection {
        let mut c = make_coll(tenant_id, name);
        c.event_defs = vec![super::super::collection_constraints::EventDefinition {
            name: "ev".into(),
            collection: name.into(),
            when_condition: "INSERT".into(),
            then_action: "SELECT 1".into(),
        }];
        c
    }

    #[test]
    fn committed_writes_keep_the_event_index_in_step() {
        let (_dir, cat) = open_catalog();
        let db = DatabaseId::DEFAULT;
        cat.put_collection(db, &with_event(1, "orders")).unwrap();
        assert_eq!(
            cat.event_definitions(db, 1, "orders").map(|d| d.len()),
            Some(1)
        );

        cat.put_collection(db, &make_coll(1, "orders")).unwrap();
        assert!(cat.event_definitions(db, 1, "orders").is_none());

        cat.put_collection(db, &with_event(1, "orders")).unwrap();
        assert!(cat.delete_collection(db, 1, "orders").unwrap());
        assert!(cat.event_definitions(db, 1, "orders").is_none());
    }

    #[test]
    fn a_skipped_insert_leaves_the_event_index_unchanged() {
        let (_dir, cat) = open_catalog();
        let db = DatabaseId::DEFAULT;
        cat.put_collection(db, &make_coll(1, "orders")).unwrap();
        assert!(
            !cat.put_collection_if_absent(db, &with_event(1, "orders"))
                .unwrap()
        );
        assert!(cat.event_definitions(db, 1, "orders").is_none());
    }

    #[test]
    fn a_failed_write_leaves_the_event_index_unchanged() {
        let (_dir, cat) = open_catalog();
        let db = DatabaseId::DEFAULT;
        cat.fail_next_collection_write_for_test();
        assert!(cat.put_collection(db, &with_event(1, "orders")).is_err());
        assert!(cat.event_definitions(db, 1, "orders").is_none());
    }

    #[test]
    fn reopening_the_catalog_loads_the_event_index() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("system.redb");
        {
            let cat = SystemCatalog::open(&path).unwrap();
            cat.put_collection(DatabaseId::DEFAULT, &with_event(1, "orders"))
                .unwrap();
        }
        let cat = SystemCatalog::open(&path).unwrap();
        assert_eq!(
            cat.event_definitions(DatabaseId::DEFAULT, 1, "orders")
                .map(|d| d.len()),
            Some(1)
        );
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
            None,
            &[("first".to_owned(), "BIGINT".to_owned())]
        ));
        assert!(merge_inferred_fields(
            &mut coll,
            None,
            &[("second".to_owned(), "FLOAT".to_owned())]
        ));
        // A known name never re-types an existing column, and reports no change.
        assert!(!merge_inferred_fields(
            &mut coll,
            None,
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

    fn ilp_time_column() -> (String, String) {
        ("timestamp".to_owned(), "TIMESTAMP".to_owned())
    }

    /// A collection declared with `ts BIGINT TIME_KEY` stores the ILP line
    /// time in `ts`. The inferred `timestamp` column must not reach its
    /// projection, or every flush proposes a new descriptor version.
    #[test]
    fn merge_skips_the_inferred_time_column_for_a_declared_time_key() {
        let mut coll = make_coll(1, "crash_ilp_ts_bulk");
        coll.collection_type = CollectionType::timeseries("ts", "1h");
        coll.fields = vec![
            ("ts".to_owned(), "BIGINT TIME_KEY".to_owned()),
            ("value".to_owned(), "BIGINT".to_owned()),
        ];

        assert!(
            !merge_inferred_fields(
                &mut coll,
                Some(&ilp_time_column()),
                &[("value".to_owned(), "BIGINT".to_owned())]
            ),
            "an ILP batch carrying only declared fields changes nothing"
        );
        assert!(
            merge_inferred_fields(
                &mut coll,
                Some(&ilp_time_column()),
                &[
                    ("host".to_owned(), "VARCHAR".to_owned()),
                    ("value".to_owned(), "BIGINT".to_owned()),
                    ("load".to_owned(), "FLOAT".to_owned()),
                ]
            ),
            "a new tag and a new field still reach the projection"
        );
        assert_eq!(
            coll.fields,
            vec![
                ("ts".to_owned(), "BIGINT TIME_KEY".to_owned()),
                ("value".to_owned(), "BIGINT".to_owned()),
                ("host".to_owned(), "VARCHAR".to_owned()),
                ("load".to_owned(), "FLOAT".to_owned()),
            ]
        );
    }

    /// A field literally called `timestamp` is a field, not the line time.
    /// It reaches the projection of a collection with a declared time key.
    #[test]
    fn merge_keeps_a_field_named_timestamp_for_a_declared_time_key() {
        let mut coll = make_coll(1, "metrics");
        coll.collection_type = CollectionType::timeseries("ts", "1h");
        coll.fields = vec![("ts".to_owned(), "TIMESTAMP".to_owned())];

        assert!(merge_inferred_fields(
            &mut coll,
            Some(&ilp_time_column()),
            &[("timestamp".to_owned(), "BIGINT".to_owned())]
        ));
        assert_eq!(
            coll.fields,
            vec![
                ("ts".to_owned(), "TIMESTAMP".to_owned()),
                ("timestamp".to_owned(), "BIGINT".to_owned()),
            ]
        );
    }

    /// With no declared time key among its fields, the Data Plane infers the
    /// schema and stores the line time under the inferred name. The
    /// projection follows it.
    #[test]
    fn merge_adds_the_inferred_time_column_without_a_declared_time_key() {
        let mut undeclared = make_coll(1, "events");
        assert!(merge_inferred_fields(
            &mut undeclared,
            Some(&ilp_time_column()),
            &[("value".to_owned(), "FLOAT".to_owned())]
        ));
        assert_eq!(
            undeclared.fields,
            vec![ilp_time_column(), ("value".to_owned(), "FLOAT".to_owned())]
        );

        // A time key absent from the field list resolves no declaration, so
        // the Data Plane infers here too.
        let mut unresolved = make_coll(1, "cpu");
        unresolved.collection_type = CollectionType::timeseries("ts", "1h");
        assert!(merge_inferred_fields(
            &mut unresolved,
            Some(&ilp_time_column()),
            &[]
        ));
        assert_eq!(unresolved.fields, vec![ilp_time_column()]);
    }

    #[test]
    fn merge_inferred_fields_adds_bitemporal_reserved_fields_once() {
        let mut coll = make_coll(1, "audit");
        coll.bitemporal = true;
        coll.fields = vec![(TS_SYSTEM.to_owned(), "BIGINT".to_owned())];

        assert!(merge_inferred_fields(
            &mut coll,
            None,
            &[("value".to_owned(), "FLOAT".to_owned())]
        ));
        assert!(!merge_inferred_fields(&mut coll, None, &[]));
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
            None,
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
}
