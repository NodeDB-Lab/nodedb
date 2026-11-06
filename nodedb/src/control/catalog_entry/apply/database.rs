// SPDX-License-Identifier: BUSL-1.1

//! Apply database catalog entries to `SystemCatalog` redb.

use crate::control::security::catalog::auth_types::object_type;
use crate::control::security::catalog::database_types::{DatabaseDescriptor, ParentCloneRef};
use crate::control::security::catalog::{SystemCatalog, catalog_err};
use nodedb_types::{DatabaseId, Hlc};

/// Apply a `PutDatabase` entry — upsert the descriptor into
/// `_system.databases` and `_system.databases_by_name`.
///
/// A promoted mirror keeps its cut state. A replica whose data group lags the
/// promotion still decides each held entry from that row.
pub fn put(descriptor: &DatabaseDescriptor, catalog: &SystemCatalog) -> crate::Result<()> {
    catalog.put_database(descriptor).map_err(|e| {
        catalog_err(
            &format!(
                "put_database '{}' (database {})",
                descriptor.name,
                descriptor.id.as_u64()
            ),
            e,
        )
    })
}

/// Apply a `DeleteDatabase` entry — remove the descriptor, its
/// reverse-lookup row, the quota rows of the dropped scope, its mirror cut
/// state, and its mirror Calvin positions.
pub fn delete(db_id: u64, catalog: &SystemCatalog) -> crate::Result<()> {
    let id = DatabaseId::new(db_id);
    catalog
        .delete_database(id)
        .map_err(|e| catalog_err(&format!("delete_database (database {db_id})"), e))?;
    catalog
        .delete_mirror_cut_state(id)
        .map_err(|e| catalog_err(&format!("delete_mirror_cut_state (database {db_id})"), e))?;
    catalog
        .delete_mirror_calvin_applied(db_id)
        .map_err(|e| catalog_err(&format!("delete mirror calvin (database {db_id})"), e))?;
    catalog
        .mark_mirror_pins_source_dropped(db_id)
        .map_err(|e| catalog_err(&format!("stop mirror pins (database {db_id})"), e))?;
    // A stale quota row keeps consuming the sum-of-quotas ceiling.
    super::quota::purge_database_scope(db_id, catalog)
}

/// Apply a `PutDatabaseGrant` entry.
pub fn put_grant(
    db_id: u64,
    user_id: u64,
    privilege: &str,
    catalog: &SystemCatalog,
) -> crate::Result<()> {
    catalog
        .put_database_grant(DatabaseId::new(db_id), user_id, privilege)
        .map_err(|e| {
            catalog_err(
                &format!("put_database_grant '{privilege}' (database {db_id}, user {user_id})"),
                e,
            )
        })
}

/// Apply a `CloneDatabase` entry — write the target descriptor, update the
/// clone lineage table, and stamp every source collection into the target
/// database with `cloned_from` set so the SQL planner can resolve queries
/// against the clone without a source-side lookup at plan time.
///
/// Every step raises on failure: a half-stamped clone answers queries this
/// node's peers answer differently.
///
/// The lineage edge is written last. `descriptor_validate` reads it as the
/// mark of a completed clone, so a replay after an interrupted apply re-runs
/// the whole clone.
///
/// Every shadow takes `incarnation`, the fresh one the proposer stamped.
pub fn clone_apply(
    target_descriptor: &DatabaseDescriptor,
    source_db_id: u64,
    incarnation: Hlc,
    catalog: &SystemCatalog,
) -> crate::Result<()> {
    if incarnation == Hlc::ZERO {
        return Err(crate::Error::Internal {
            detail: format!(
                "clone_database '{}' (database {}) carries no shadow incarnation; \
                 the proposer stamps every clone entry",
                target_descriptor.name,
                target_descriptor.id.as_u64()
            ),
        });
    }
    let child = target_descriptor.id;
    let source = DatabaseId::new(source_db_id);
    catalog.put_database(target_descriptor).map_err(|e| {
        catalog_err(
            &format!(
                "clone_database descriptor write of '{}' (database {})",
                target_descriptor.name,
                child.as_u64()
            ),
            e,
        )
    })?;
    if let Some(parent_clone) = &target_descriptor.parent_clone {
        stamp_shadows(
            target_descriptor,
            parent_clone,
            source,
            incarnation,
            catalog,
        )?;
    }
    catalog.add_clone_child(source, child).map_err(|e| {
        catalog_err(
            &format!(
                "clone_database lineage edge (source {source_db_id}, child {})",
                child.as_u64()
            ),
            e,
        )
    })
}

/// Write a shadow descriptor for every active source collection into the
/// child, then copy the source's other database-scoped catalog rows.
fn stamp_shadows(
    target_descriptor: &DatabaseDescriptor,
    parent_clone: &ParentCloneRef,
    source: DatabaseId,
    incarnation: Hlc,
    catalog: &SystemCatalog,
) -> crate::Result<()> {
    let child = target_descriptor.id;
    let source_db_id = source.as_u64();
    let as_of_lsn = nodedb_types::Lsn::new(parent_clone.as_of_lsn);
    let clone_created_at = nodedb_types::Lsn::new(target_descriptor.created_at_lsn);
    let kv_surrogate_ceiling = parent_clone.kv_surrogate_ceiling;

    // Enumerate every active collection in the source database and write a
    // shadow descriptor into the target database so the SQL planner can
    // resolve collection names without knowing about clone indirection.
    //
    // Each shadow collection carries `cloned_from` pointing back to the
    // source, so the read/write planner applies CoW delegation at dispatch
    // time. The engines never see this field.
    //
    // We enumerate all tenants visible in the source by walking every
    // collection row under the source database_id. The tenant_id is encoded
    // in the inner key prefix, so we collect it from the row itself.
    let source_colls = catalog.load_all_collections(source).map_err(|e| {
        catalog_err(
            &format!("clone_database enumeration of source database {source_db_id}"),
            e,
        )
    })?;

    for mut coll in source_colls.into_iter().filter(|c| c.is_active) {
        coll.database_id = child;
        coll.cloned_from = Some(nodedb_types::CloneOrigin {
            source_database: source,
            source_collection: coll.name.clone(),
            as_of_lsn,
            clone_created_at,
            kv_surrogate_ceiling,
        });
        coll.clone_status = nodedb_types::CloneStatus::Shadowed;
        // A shadow is a new collection under a new key: its first version, and
        // an incarnation of its own, identical on every replica.
        coll.descriptor_version = 1;
        coll.incarnation = incarnation;
        coll.modification_hlc = incarnation;
        catalog.put_collection(child, &coll).map_err(|e| {
            catalog_err(
                &format!(
                    "clone_database shadow stamp of '{}' into database {}",
                    coll.name,
                    child.as_u64()
                ),
                e,
            )
        })?;
        super::owner::put_parent_owner(
            object_type::COLLECTION,
            child.as_u64(),
            coll.tenant_id,
            &coll.name,
            &coll.owner,
            catalog,
        )?;
    }

    // The shadow descriptors resolve the names. The rest of the source's
    // database-scoped catalog rows make the clone answer like the source.
    crate::control::clone::copy_database_metadata(catalog, source, child)
}

/// Apply a `DeleteDatabaseGrant` entry.
pub fn delete_grant(
    db_id: u64,
    user_id: u64,
    privilege: &str,
    catalog: &SystemCatalog,
) -> crate::Result<()> {
    catalog
        .delete_database_grant(DatabaseId::new(db_id), user_id, privilege)
        .map_err(|e| {
            catalog_err(
                &format!("delete_database_grant '{privilege}' (database {db_id}, user {user_id})"),
                e,
            )
        })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nodedb_types::MirrorStatus;

    use super::*;
    use crate::control::catalog_entry::apply::apply_to;
    use crate::control::catalog_entry::entry::CatalogEntry;
    use crate::control::security::catalog::StoredCollection;
    use crate::control::security::catalog::database_types::{DatabaseStatus, ParentCloneRef};
    use crate::control::security::credential::store::CredentialStore;

    /// The incarnation the proposer stamps on the test clone.
    const SHADOW: Hlc = Hlc::new(1_000_000, 0);

    fn open_catalog() -> (Arc<CredentialStore>, tempfile::TempDir) {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let store = Arc::new(
            CredentialStore::open(&tmp.path().join("system.redb")).expect("open credential store"),
        );
        (store, tmp)
    }

    fn clone_descriptor(source: DatabaseId, child: DatabaseId) -> DatabaseDescriptor {
        DatabaseDescriptor {
            id: child,
            name: "clone_target".into(),
            status: DatabaseStatus::Cloning,
            created_at_lsn: 20,
            quota_ref: 0,
            parent_clone: Some(ParentCloneRef {
                source_db_id: source,
                as_of_lsn: 10,
                as_of_ms: 0,
                kv_surrogate_ceiling: None,
            }),
            mirror_origin: None,
            audit_dml: nodedb_types::AuditDmlMode::None,
            idle_session_timeout_secs: 0,
        }
    }

    /// A shadow-stamp failure aborts the whole clone. Finishing the remaining
    /// collections leaves this node answering queries its peers cannot.
    #[test]
    fn clone_apply_raises_instead_of_stamping_the_rest() {
        let (credentials, _tmp) = open_catalog();
        let catalog = credentials.catalog();
        let source = DatabaseId::new(1);
        let child = DatabaseId::new(2);
        for name in ["orders", "invoices"] {
            let mut coll = StoredCollection::stamped_for_test(5, name, "cloner");
            coll.database_id = source;
            apply_to(&CatalogEntry::PutCollection(Box::new(coll)), catalog)
                .expect("seed source collection");
        }

        catalog.fail_next_collection_write_for_test();
        let error = clone_apply(
            &clone_descriptor(source, child),
            source.as_u64(),
            SHADOW,
            catalog,
        )
        .expect_err("a failed shadow stamp must raise");
        assert!(error.to_string().contains("clone_database"), "{error}");

        let stamped = catalog.load_all_collections(child).expect("load target");
        assert!(
            stamped.is_empty(),
            "a raised clone leaves no partially stamped target: {stamped:?}"
        );
        // No lineage edge, so a replay re-runs the clone instead of skipping it.
        assert!(
            catalog
                .get_clone_children(source)
                .expect("read lineage")
                .is_empty()
        );
    }

    /// A completed clone leaves the lineage edge that marks it applied.
    #[test]
    fn clone_apply_writes_the_lineage_edge() {
        let (credentials, _tmp) = open_catalog();
        let catalog = credentials.catalog();
        let source = DatabaseId::new(1);
        let child = DatabaseId::new(2);
        clone_apply(
            &clone_descriptor(source, child),
            source.as_u64(),
            SHADOW,
            catalog,
        )
        .expect("clone applies");
        assert_eq!(
            catalog.get_clone_children(source).expect("read lineage"),
            vec![child]
        );
    }

    /// A shadow is a new collection: it takes the clone's own incarnation and
    /// its first descriptor version, never the source's.
    #[test]
    fn a_shadow_takes_a_fresh_incarnation() {
        let (credentials, _tmp) = open_catalog();
        let catalog = credentials.catalog();
        let source = DatabaseId::new(1);
        let child = DatabaseId::new(2);
        let mut coll = StoredCollection::stamped_for_test(5, "orders", "cloner");
        coll.database_id = source;
        catalog.put_collection(source, &coll).expect("seed source");
        let source_row = catalog
            .get_committed_collection(source, 5, "orders")
            .expect("read source")
            .expect("source row");

        clone_apply(
            &clone_descriptor(source, child),
            source.as_u64(),
            SHADOW,
            catalog,
        )
        .expect("clone applies");

        let shadow = catalog
            .get_committed_collection(child, 5, "orders")
            .expect("read shadow")
            .expect("shadow row");
        assert_eq!(shadow.incarnation, SHADOW);
        assert_ne!(shadow.incarnation, source_row.incarnation);
        assert_eq!(shadow.descriptor_version, 1);
    }

    /// An unstamped clone entry is refused before it writes anything.
    #[test]
    fn an_unstamped_clone_is_refused() {
        let (credentials, _tmp) = open_catalog();
        let catalog = credentials.catalog();
        let source = DatabaseId::new(1);
        let child = DatabaseId::new(2);
        assert!(
            clone_apply(
                &clone_descriptor(source, child),
                source.as_u64(),
                Hlc::ZERO,
                catalog,
            )
            .is_err()
        );
        assert!(catalog.get_database(child).expect("read").is_none());
    }

    fn seed_cut_state(catalog: &SystemCatalog, db: DatabaseId) {
        let state = nodedb_types::MirrorCutState {
            ready_cut: 5,
            applied_cut: 5,
            applied_cut_hlc_ns: 7,
            cap_ordinal: 7,
            positions: Vec::new(),
            epochs: Vec::new(),
            fence_epoch: 0,
        };
        catalog
            .raise_mirror_cut_state(db, &state)
            .expect("seed cut state");
    }

    fn mirror_descriptor(db: DatabaseId, status: MirrorStatus) -> DatabaseDescriptor {
        DatabaseDescriptor {
            id: db,
            name: "mirror_db".to_string(),
            status: DatabaseStatus::Mirroring,
            created_at_lsn: 0,
            quota_ref: 0,
            parent_clone: None,
            mirror_origin: Some(nodedb_types::MirrorOrigin {
                source_cluster: "source".to_string(),
                source_database: db,
                source_database_name: "orders".to_string(),
                mode: nodedb_types::MirrorMode::Async,
                status,
            }),
            audit_dml: nodedb_types::AuditDmlMode::None,
            idle_session_timeout_secs: 0,
        }
    }

    /// `DeleteDatabase` removes the cut state on every node that applies it,
    /// and a replay of it is a no-op.
    #[test]
    fn delete_removes_the_cut_state_and_replays_cleanly() {
        let (credentials, _tmp) = open_catalog();
        let catalog = credentials.catalog();
        let db = DatabaseId::new(1030);
        seed_cut_state(catalog, db);
        let entry = CatalogEntry::DeleteDatabase { db_id: db.as_u64() };
        for _ in 0..2 {
            apply_to(&entry, catalog).expect("delete database applies");
            assert!(catalog.get_mirror_cut_state(db).expect("read").is_none());
        }
    }

    /// A promoted mirror keeps its cut state: a replica behind the promotion
    /// decides its held entries from the row.
    #[test]
    fn a_promotion_keeps_the_cut_state() {
        let (credentials, _tmp) = open_catalog();
        let catalog = credentials.catalog();
        let db = DatabaseId::new(1031);
        seed_cut_state(catalog, db);
        let entry =
            CatalogEntry::PutDatabase(Box::new(mirror_descriptor(db, MirrorStatus::Promoted)));
        apply_to(&entry, catalog).expect("promotion applies");
        assert!(catalog.get_mirror_cut_state(db).expect("read").is_some());
    }

    #[test]
    fn a_following_descriptor_keeps_the_cut_state() {
        let (credentials, _tmp) = open_catalog();
        let catalog = credentials.catalog();
        let db = DatabaseId::new(1032);
        seed_cut_state(catalog, db);
        let entry =
            CatalogEntry::PutDatabase(Box::new(mirror_descriptor(db, MirrorStatus::Following)));
        apply_to(&entry, catalog).expect("descriptor applies");
        assert!(catalog.get_mirror_cut_state(db).expect("read").is_some());
    }
}
