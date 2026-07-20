// SPDX-License-Identifier: BUSL-1.1

use std::sync::{Arc, RwLock};

use tokio::sync::broadcast;

use nodedb_cluster::{MetadataApplier, MetadataCache, MetadataEntry, encode_entry};
use nodedb_types::DatabaseId;

use crate::control::catalog_entry;
use crate::control::catalog_entry::CatalogEntry;
use crate::control::security::catalog::StoredCollection;
use crate::control::security::credential::CredentialStore;

use super::types::MetadataCommitApplier;

fn make_applier() -> (
    MetadataCommitApplier,
    Arc<RwLock<MetadataCache>>,
    Arc<CredentialStore>,
    tempfile::TempDir,
) {
    let tmp = tempfile::tempdir().expect("tmpdir");
    let credentials =
        Arc::new(CredentialStore::open(&tmp.path().join("system.redb")).expect("open"));
    let cache = Arc::new(RwLock::new(MetadataCache::new()));
    let (tx, _rx) = broadcast::channel(16);
    let applier = MetadataCommitApplier::new(cache.clone(), tx, credentials.clone());
    (applier, cache, credentials, tmp)
}

fn put_collection_entry(name: &str) -> MetadataEntry {
    let stored = StoredCollection::new(7, name, "tester");
    let catalog_entry = CatalogEntry::PutCollection(Box::new(stored));
    MetadataEntry::CatalogDdl {
        payload: catalog_entry::encode(&catalog_entry).unwrap(),
    }
}

#[test]
fn apply_put_collection_writes_through_to_redb() {
    let (applier, cache, credentials, _tmp) = make_applier();
    let bytes = encode_entry(&put_collection_entry("orders")).unwrap();
    assert_eq!(applier.apply(&[(11, bytes)]), 11);

    let cache_guard = cache.read().unwrap();
    assert_eq!(cache_guard.applied_index, 11);
    assert_eq!(cache_guard.catalog_entries_applied, 1);
    drop(cache_guard);

    let loaded = credentials
        .catalog()
        .get_collection(DatabaseId::DEFAULT, 7, "orders")
        .unwrap()
        .expect("present");
    assert_eq!(loaded.name, "orders");
    assert_eq!(loaded.owner, "tester");
}

#[test]
fn apply_deactivate_preserves_record() {
    let (applier, _cache, credentials, _tmp) = make_applier();

    // Seed.
    applier.apply(&[(1, encode_entry(&put_collection_entry("archived")).unwrap())]);

    let drop_entry = MetadataEntry::CatalogDdl {
        payload: catalog_entry::encode(&CatalogEntry::DeactivateCollection {
            database_id: 0,
            tenant_id: 7,
            name: "archived".into(),
        })
        .unwrap(),
    };
    applier.apply(&[(2, encode_entry(&drop_entry).unwrap())]);

    let loaded = credentials
        .catalog()
        .get_collection(DatabaseId::DEFAULT, 7, "archived")
        .unwrap()
        .expect("preserved");
    assert!(!loaded.is_active);
}

#[test]
fn apply_empty_batch_is_noop() {
    let (applier, _cache, _credentials, _tmp) = make_applier();
    assert_eq!(applier.apply(&[]), 0);
}

#[test]
fn apply_noop_entry_advances_cache_watermark() {
    let (applier, cache, _credentials, _tmp) = make_applier();
    // A committed Raft no-op (empty payload) at index 1 — the shape of every
    // group's first entry on a fresh single-node start. It mutates nothing, but
    // the cache watermark must advance in lockstep with the Raft applied index
    // the tick loop takes from the return value; otherwise the startup
    // applied-index sanity check reads a spurious gap and fails the boot.
    assert_eq!(applier.apply(&[(1, Vec::new())]), 1);
    assert_eq!(cache.read().unwrap().applied_index, 1);
    assert_eq!(
        cache.read().unwrap().catalog_entries_applied,
        0,
        "a no-op applies no catalog entry"
    );
}
