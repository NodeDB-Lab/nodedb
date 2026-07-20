// SPDX-License-Identifier: BUSL-1.1

//! Apply `RecordWalTombstone` catalog entries to `SystemCatalog` redb.

use tracing::warn;

use crate::control::security::catalog::SystemCatalog;

pub fn record(
    database_id: u64,
    tenant_id: u64,
    collection: &str,
    purge_lsn: u64,
    catalog: &SystemCatalog,
) {
    if let Err(e) = catalog.record_wal_tombstone(database_id, tenant_id, collection, purge_lsn) {
        warn!(
            database_id,
            tenant_id,
            collection = %collection,
            purge_lsn,
            error = %e,
            "catalog_entry: record_wal_tombstone failed"
        );
    }
}

#[cfg(test)]
mod tests {
    use crate::control::catalog_entry::apply::apply_to;
    use crate::control::catalog_entry::entry::CatalogEntry;
    use crate::control::security::credential::CredentialStore;
    use std::sync::Arc;

    fn make_catalog() -> (Arc<CredentialStore>, tempfile::TempDir) {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let store = Arc::new(CredentialStore::open(&tmp.path().join("system.redb")).expect("open"));
        (store, tmp)
    }

    #[test]
    fn record_wal_tombstone_entry_applies_and_is_monotone() {
        let (store, _tmp) = make_catalog();
        let catalog = store.catalog();

        // Apply via the top-level apply_to path (entry → apply_to_inner → wal_tombstone::record).
        let entry = CatalogEntry::RecordWalTombstone {
            database_id: 7,
            tenant_id: 1,
            collection: "users".into(),
            purge_lsn: 100,
        };
        apply_to(&entry, catalog);

        let set = catalog.load_wal_tombstones().expect("load");
        assert_eq!(
            set.purge_lsn(7, 1, "users"),
            Some(100),
            "initial tombstone not recorded"
        );

        // Lower purge_lsn must not regress the stored value (monotone).
        let entry_lower = CatalogEntry::RecordWalTombstone {
            database_id: 7,
            tenant_id: 1,
            collection: "users".into(),
            purge_lsn: 50,
        };
        apply_to(&entry_lower, catalog);
        let set = catalog.load_wal_tombstones().expect("load after lower");
        assert_eq!(
            set.purge_lsn(7, 1, "users"),
            Some(100),
            "lower purge_lsn must not regress stored tombstone"
        );

        // Higher purge_lsn must raise the stored value (monotone raise).
        let entry_higher = CatalogEntry::RecordWalTombstone {
            database_id: 7,
            tenant_id: 1,
            collection: "users".into(),
            purge_lsn: 200,
        };
        apply_to(&entry_higher, catalog);
        let set = catalog.load_wal_tombstones().expect("load after higher");
        assert_eq!(
            set.purge_lsn(7, 1, "users"),
            Some(200),
            "higher purge_lsn must raise stored tombstone"
        );
    }
}
