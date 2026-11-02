// SPDX-License-Identifier: BUSL-1.1

//! Apply a committed collection write only to the collection incarnation its
//! proposer planned against.
//!
//! The proposer stamps, for each collection the write names, the incarnation
//! its catalog held. On this replica:
//!
//! - The collection still holds that incarnation: the write applies.
//! - The collection is gone, or holds another incarnation: the write is
//!   superseded. It concludes with a final refusal counted as durable, and
//!   changes nothing. This covers a purge followed by a same-name create, and
//!   the source of a MOVE TENANT, whose cutover re-issued every row into the
//!   target before it moved the catalog row. Applying the write to the target
//!   applies it twice.
//! - The write names no incarnation (`Hlc::ZERO`): it applies by key alone.
//!   Only a write can be unstamped. Every committed collection row holds a
//!   non-zero incarnation.
//!
//! The decision needs no WAL tombstone, so it holds after tombstone GC.
//!
//! An array the write names follows the same rule against the array mirror,
//! under the array incarnation's own gate.
//!
//! The write routes and reaches its core's queue under each collection's
//! gate held shared. A purge or a MOVE TENANT reclaim of the key holds the
//! gate exclusive, so a routed write is on its core before the reclaim.

use nodedb_types::{CollectionKey, Hlc};

use nodedb_cluster::calvin::types::IncarnationKind;

use crate::control::array_catalog::ArrayCatalog;
use crate::control::array_catalog::named_incarnation::{
    array_gate_key, array_holds_incarnation, read_mirror,
};
use crate::control::security::catalog::SystemCatalog;
use crate::control::state::SharedState;
use crate::control::wal_replication::CollectionIncarnation;
use crate::control::write_gate::{self, GateKey, SharedGate};
use crate::types::{DatabaseId, TenantId};

/// Where a committed collection write goes.
pub(super) enum CollectionRoute {
    /// The write applies. The gates stay held until it is on its core.
    Apply(Vec<SharedGate>),
    /// A collection the write names no longer holds its incarnation.
    Superseded,
}

fn key_of<'a>(database_id: DatabaseId, named: &'a str) -> CollectionKey<'a> {
    CollectionKey::from_qualified_str(database_id, named)
        .unwrap_or_else(|_| CollectionKey::from_bare(database_id, named))
}

/// Route a write of `tenant_id` in `database_id` that names `named`. A
/// collection takes its collection gate. An array takes its own
/// incarnation's gate, never a same-name collection's.
pub(super) async fn route(
    state: &SharedState,
    tenant_id: u64,
    database_id: DatabaseId,
    named: &[CollectionIncarnation],
) -> crate::Result<CollectionRoute> {
    if named.is_empty() {
        return Ok(CollectionRoute::Apply(Vec::new()));
    }
    let mut keys = Vec::with_capacity(named.len());
    {
        let mirror = read_mirror(&state.array_catalog);
        for entry in named {
            match entry.kind {
                IncarnationKind::Collection => {
                    let key = key_of(database_id, &entry.collection);
                    keys.push(GateKey::Collection {
                        database_id: key.database_id().as_u64(),
                        tenant_id,
                        name: key.name().to_string(),
                    });
                }
                IncarnationKind::Array => match array_gate_key(
                    &mirror,
                    TenantId::new(tenant_id),
                    database_id,
                    &entry.collection,
                    entry.incarnation,
                ) {
                    Some(key) => keys.push(key),
                    // An unstamped write of an array this replica does not
                    // hold has nothing to write into.
                    None => return Ok(CollectionRoute::Superseded),
                },
            }
        }
    }
    let gates = write_gate::shared_all(keys).await;
    let arrays = read_mirror(&state.array_catalog);
    if applies(
        state.credentials.catalog(),
        &arrays,
        tenant_id,
        database_id,
        named,
    )? {
        Ok(CollectionRoute::Apply(gates))
    } else {
        Ok(CollectionRoute::Superseded)
    }
}

/// Whether every collection and array of `named` still holds the
/// incarnation its proposer stamped, in this node's committed catalog and
/// array mirror.
pub(super) fn applies(
    catalog: &SystemCatalog,
    arrays: &ArrayCatalog,
    tenant_id: u64,
    database_id: DatabaseId,
    named: &[CollectionIncarnation],
) -> crate::Result<bool> {
    for entry in named {
        let holds = match entry.kind {
            // An array that moved or is gone holds nothing this write can
            // mutate.
            IncarnationKind::Array => array_holds_incarnation(
                arrays,
                TenantId::new(tenant_id),
                database_id,
                &entry.collection,
                entry.incarnation,
            ),
            IncarnationKind::Collection if entry.incarnation == Hlc::ZERO => true,
            // Refusing a moved collection's write is correct: the cutover
            // drained the source and re-issued every row into the target.
            IncarnationKind::Collection => catalog.holds_incarnation(
                database_id,
                tenant_id,
                &entry.collection,
                entry.incarnation,
            )?,
        };
        if !holds {
            return Ok(false);
        }
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::control::catalog_entry::CatalogEntry;
    use crate::control::catalog_entry::descriptor_stamp::stamp;
    use crate::control::security::catalog::StoredCollection;
    use crate::control::security::credential::CredentialStore;
    use nodedb_types::HlcClock;

    fn open() -> (Arc<CredentialStore>, tempfile::TempDir) {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let store = Arc::new(CredentialStore::open(&tmp.path().join("system.redb")).expect("open"));
        (store, tmp)
    }

    /// Stamp and write a put of `row`, as the metadata applier does.
    fn put(catalog: &SystemCatalog, clock: &HlcClock, row: StoredCollection) -> StoredCollection {
        let CatalogEntry::PutCollection(stamped) =
            stamp(CatalogEntry::PutCollection(Box::new(row)), clock, catalog).expect("stamp")
        else {
            panic!("a collection put stamps as a collection put");
        };
        catalog
            .put_collection(DatabaseId::DEFAULT, &stamped)
            .expect("write the row");
        *stamped
    }

    fn naming(incarnation: Hlc) -> Vec<CollectionIncarnation> {
        vec![CollectionIncarnation {
            collection: "orders".into(),
            incarnation,
            kind: IncarnationKind::Collection,
        }]
    }

    /// A write planned against a purged incarnation never lands on a
    /// same-name recreate, with no WAL tombstone left to fence it. A redo
    /// entry names its collections the same way and is refused alike. An
    /// ALTER keeps the incarnation, so a write planned before it applies.
    #[test]
    fn a_stale_write_never_lands_on_a_recreated_collection() {
        let (store, _tmp) = open();
        let catalog = store.catalog();
        let clock = HlcClock::new();

        let first = put(catalog, &clock, StoredCollection::new(1, "orders", "admin"));
        assert_ne!(
            first.incarnation,
            Hlc::ZERO,
            "a create names an incarnation"
        );
        let stale = naming(first.incarnation);
        assert!(
            applies(
                catalog,
                &ArrayCatalog::new(),
                1,
                DatabaseId::DEFAULT,
                &stale
            )
            .expect("route")
        );

        let altered = put(catalog, &clock, first.clone());
        assert_eq!(altered.incarnation, first.incarnation, "ALTER keeps it");
        assert!(
            applies(
                catalog,
                &ArrayCatalog::new(),
                1,
                DatabaseId::DEFAULT,
                &stale
            )
            .expect("route")
        );

        // Purge, with the tombstone already collected: only the row goes.
        catalog
            .delete_collection(DatabaseId::DEFAULT, 1, "orders")
            .expect("purge the row");
        assert!(
            !applies(
                catalog,
                &ArrayCatalog::new(),
                1,
                DatabaseId::DEFAULT,
                &stale
            )
            .expect("route")
        );

        let recreated = put(catalog, &clock, StoredCollection::new(1, "orders", "admin"));
        assert_ne!(recreated.incarnation, first.incarnation);
        assert!(
            !applies(
                catalog,
                &ArrayCatalog::new(),
                1,
                DatabaseId::DEFAULT,
                &stale
            )
            .expect("route"),
            "the stale write is superseded"
        );

        // A redo entry that writes the collection among others routes on every
        // collection it names.
        let redo = |incarnation: Hlc| {
            vec![
                CollectionIncarnation {
                    collection: "orders".into(),
                    incarnation,
                    kind: IncarnationKind::Collection,
                },
                CollectionIncarnation {
                    collection: "ledger".into(),
                    incarnation: Hlc::ZERO,
                    kind: IncarnationKind::Collection,
                },
            ]
        };
        assert!(
            !applies(
                catalog,
                &ArrayCatalog::new(),
                1,
                DatabaseId::DEFAULT,
                &redo(first.incarnation)
            )
            .expect("route"),
            "the stale redo entry is superseded"
        );
        assert!(
            applies(
                catalog,
                &ArrayCatalog::new(),
                1,
                DatabaseId::DEFAULT,
                &redo(recreated.incarnation)
            )
            .expect("route")
        );
    }

    #[test]
    fn an_unstamped_write_applies_by_key() {
        let (store, _tmp) = open();
        assert!(
            applies(
                store.catalog(),
                &ArrayCatalog::new(),
                1,
                DatabaseId::DEFAULT,
                &naming(Hlc::ZERO)
            )
            .expect("route")
        );
    }

    /// A restored array names itself by the array catalog. It applies while
    /// the array holds the planned incarnation, whatever a same-name
    /// collection holds, and is superseded once the array is recreated.
    #[test]
    fn an_array_routes_by_its_own_incarnation() {
        let (store, _tmp) = open();
        let catalog = store.catalog();
        let clock = HlcClock::new();
        // A same-name collection with its own incarnation.
        put(catalog, &clock, StoredCollection::new(1, "grid", "admin"));

        let planned = Hlc::new(9_200, 0);
        let mirror = |incarnation: Hlc| {
            let mut mirror = ArrayCatalog::new();
            mirror
                .register(crate::control::array_catalog::ArrayCatalogEntry {
                    array_id: nodedb_array::types::ArrayId::in_database(
                        TenantId::new(1),
                        DatabaseId::DEFAULT,
                        "grid",
                    ),
                    name: "grid".into(),
                    schema_msgpack: vec![0x90],
                    schema_hash: 7,
                    created_at_ms: 0,
                    prefix_bits: 8,
                    audit_retain_ms: None,
                    minimum_audit_retain_ms: None,
                    modification_hlc: incarnation,
                    incarnation,
                })
                .expect("register");
            mirror
        };
        let array = vec![CollectionIncarnation {
            collection: "grid".into(),
            incarnation: planned,
            kind: IncarnationKind::Array,
        }];
        assert!(
            applies(catalog, &mirror(planned), 1, DatabaseId::DEFAULT, &array).expect("route"),
            "the collection's incarnation does not decide the array's write"
        );
        assert!(
            !applies(
                catalog,
                &mirror(Hlc::new(9_300, 0)),
                1,
                DatabaseId::DEFAULT,
                &array
            )
            .expect("route"),
            "a recreated array supersedes the write"
        );
    }
}
