// SPDX-License-Identifier: BUSL-1.1

//! Post-apply document redo for Data-Plane write-sets.
//!
//! Some write handlers mint no autocommit WAL redo of their own, so a
//! vector-index rebuild at startup would resurrect a stale embedding. The Data
//! Plane carries surrogate + post-image in [`Response::write_set`]; the
//! Control Plane mints the durable redo here.

use crate::bridge::envelope::{PhysicalPlan, Response, Status, WriteSetEntry};
use crate::engine::document::store::StorageKey;
use crate::types::{DatabaseId, Lsn, TenantId, VShardId};
use crate::wal::manager::WalManager;
use nodedb_physical::physical_plan::DocumentOp;
use nodedb_types::Surrogate;

use super::document::{encode_document_delete_record, encode_document_put_record};

/// Return `Some(collection)` when `plan`'s durable redo is minted *after* the
/// Data Plane applies it, from [`Response::write_set`]. `None` when durability
/// is owned elsewhere. `PointPut`/`PointInsert`/`PointDelete` qualify only when
/// a materialized-sum binding writes an unjournaled target row.
pub fn plan_post_apply_redo(plan: &PhysicalPlan) -> Option<String> {
    if let PhysicalPlan::Document(DocumentOp::PointUpdate { collection, .. }) = plan {
        Some(collection.to_string())
    } else if let PhysicalPlan::Document(DocumentOp::PointPut { collection, .. }) = plan {
        Some(collection.to_string())
    } else if let PhysicalPlan::Document(DocumentOp::PointInsert { collection, .. }) = plan {
        Some(collection.to_string())
    } else if let PhysicalPlan::Document(DocumentOp::PointDelete { collection, .. }) = plan {
        Some(collection.to_string())
    } else if let PhysicalPlan::Document(DocumentOp::Upsert { collection, .. }) = plan {
        Some(collection.to_string())
    } else if let PhysicalPlan::Document(DocumentOp::BulkUpdate { collection, .. }) = plan {
        Some(collection.to_string())
    } else if let PhysicalPlan::Document(DocumentOp::BulkDelete { collection, .. }) = plan {
        Some(collection.to_string())
    } else if let PhysicalPlan::Document(DocumentOp::BatchInsert { collection, .. }) = plan {
        Some(collection.to_string())
    } else if let PhysicalPlan::Document(DocumentOp::Truncate { collection, .. }) = plan {
        Some(collection.to_string())
    } else if let PhysicalPlan::Document(DocumentOp::UpdateFromJoin {
        target_collection, ..
    }) = plan
    {
        Some(target_collection.to_string())
    } else if let PhysicalPlan::Document(DocumentOp::ApplyBalanceDelta { collection, .. }) = plan {
        // Journals nothing on the pre-dispatch path; without this redo, a WAL-only
        // restart replays source rows and leaves the total as it stood before.
        Some(collection.to_string())
    } else {
        None
    }
}

/// Append a document redo record for each write-set entry, returning the last
/// allocated LSN. Each entry is keyed by `StorageKey::for_surrogate(surrogate)`
/// so replay keys on the same identity. Called under the write-admission guard.
pub fn append_write_set_redo(
    wal: &WalManager,
    tenant_id: TenantId,
    vshard_id: VShardId,
    database_id: DatabaseId,
    collection: &str,
    write_set: &[WriteSetEntry],
) -> crate::Result<Option<Lsn>> {
    let mut last: Option<Lsn> = None;
    for entry in write_set {
        let entry_collection = entry.collection.as_deref().unwrap_or(collection);
        let doc_id = StorageKey::for_surrogate(Surrogate::new(entry.surrogate)).to_string();
        // A cross-collection entry homes to a different vShard, so it's re-derived
        // per entry rather than reusing the caller-hoisted `vshard_id`.
        let entry_vshard_id = match &entry.collection {
            Some(c) => VShardId::from_collection_in_database(database_id, c),
            None => vshard_id,
        };
        let lsn = if entry.is_delete {
            let record = encode_document_delete_record(entry_collection, &doc_id, entry.surrogate)?;
            wal.append_delete(tenant_id, entry_vshard_id, database_id, &record)?
        } else {
            let record = encode_document_put_record(
                entry_collection,
                &doc_id,
                &entry.value,
                entry.surrogate,
            )?;
            wal.append_put(tenant_id, entry_vshard_id, database_id, &record)?
        };
        last = Some(lsn);
    }
    Ok(last)
}

/// Mint the post-apply redo for a `dispatch_local` response built outside the
/// autocommit funnel's own redo minting. No-op when not `Ok` or write-set is empty.
pub fn mint_dispatch_local_redo(
    wal: &WalManager,
    tenant_id: TenantId,
    database_id: DatabaseId,
    collection: &str,
    resp: &Response,
) -> crate::Result<()> {
    if resp.status != Status::Ok || resp.write_set.is_empty() {
        return Ok(());
    }
    let vshard_id = VShardId::from_collection_in_database(database_id, collection);
    append_write_set_redo(
        wal,
        tenant_id,
        vshard_id,
        database_id,
        collection,
        &resp.write_set,
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_physical::physical_plan::ReturningSpec;
    use nodedb_types::QualifiedCollection;
    use nodedb_types::sync::wire::SyncProvenance;

    fn open_wal(dir: &std::path::Path) -> WalManager {
        WalManager::open_for_testing(&dir.join("test.wal")).expect("open wal")
    }

    fn last_record_of_type(
        wal: &WalManager,
        record_type: nodedb_wal::record::RecordType,
    ) -> nodedb_wal::WalRecord {
        wal.sync().expect("sync wal");
        wal.replay()
            .expect("read wal")
            .into_iter()
            .rfind(|r| {
                nodedb_wal::record::RecordType::from_raw(r.logical_record_type())
                    == Some(record_type)
            })
            .expect("expected record of this type")
    }

    #[test]
    fn point_update_is_post_apply_redo() {
        let plan = PhysicalPlan::Document(DocumentOp::PointUpdate {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "docs"),
            document_id: "d1".to_string(),
            surrogate: Surrogate::new(1),
            pk_bytes: Vec::new(),
            updates: Vec::new(),
            returning: None::<ReturningSpec>,
            rls_filters: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            resolved_sum_targets: Vec::new(),
            declared_primary_key: None,
        });
        assert_eq!(plan_post_apply_redo(&plan).as_deref(), Some("docs"));
    }

    #[test]
    fn bulk_update_is_post_apply_redo() {
        let plan = PhysicalPlan::Document(DocumentOp::BulkUpdate {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "docs"),
            filters: Vec::new(),
            updates: Vec::new(),
            returning: None::<ReturningSpec>,
            ollp_predicted_surrogates: None,
            ollp_predicted_edges: None,
            rls_filters: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            resolved_sum_targets: Vec::new(),
            declared_primary_key: None,
        });
        assert_eq!(plan_post_apply_redo(&plan).as_deref(), Some("docs"));
    }

    #[test]
    fn update_from_join_is_post_apply_redo() {
        let plan = PhysicalPlan::Document(DocumentOp::UpdateFromJoin {
            target_collection: QualifiedCollection::new(DatabaseId::DEFAULT, "docs"),
            source_collection: QualifiedCollection::new(DatabaseId::DEFAULT, "src"),
            source_alias: "s".to_string(),
            target_join_col: "sku".to_string(),
            source_join_col: "sku".to_string(),
            updates: Vec::new(),
            target_filters: Vec::new(),
            returning: None::<ReturningSpec>,
            source_rows: None,
            rls_filters: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            resolved_sum_targets: Vec::new(),
            declared_primary_key: None,
        });
        assert_eq!(plan_post_apply_redo(&plan).as_deref(), Some("docs"));
    }

    #[test]
    fn bulk_delete_is_post_apply_redo() {
        let plan = PhysicalPlan::Document(DocumentOp::BulkDelete {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "docs"),
            filters: Vec::new(),
            returning: None::<ReturningSpec>,
            ollp_predicted_surrogates: None,
            ollp_predicted_edges: None,
            rls_filters: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            resolved_sum_targets: Vec::new(),
            declared_primary_key: None,
        });
        assert_eq!(plan_post_apply_redo(&plan).as_deref(), Some("docs"));
    }

    #[test]
    fn batch_insert_is_post_apply_redo() {
        let plan = PhysicalPlan::Document(DocumentOp::BatchInsert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "docs"),
            documents: Vec::new(),
            surrogates: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
            deferred_sum_targets: Vec::new(),
        });
        assert_eq!(plan_post_apply_redo(&plan).as_deref(), Some("docs"));
    }

    #[test]
    fn point_get_is_not_post_apply_redo() {
        let plan = PhysicalPlan::Document(DocumentOp::PointGet {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "docs"),
            document_id: "d1".to_string(),
            surrogate: Surrogate::ZERO,
            pk_bytes: Vec::new(),
            rls_filters: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
        });
        assert!(plan_post_apply_redo(&plan).is_none());
    }

    #[test]
    fn write_set_put_appends_replayable_put_record() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = open_wal(dir.path());
        let entries = vec![WriteSetEntry {
            surrogate: 9,
            is_delete: false,
            value: vec![1, 2, 3],
            collection: None,
        }];

        let lsn = append_write_set_redo(
            &wal,
            TenantId::new(1),
            VShardId::new(0),
            DatabaseId::DEFAULT,
            "docs",
            &entries,
        )
        .expect("append");
        assert!(
            lsn.is_some(),
            "a put write-set entry must append a redo LSN"
        );

        // Byte-shape must match the redo replay decoder's PUT tuple.
        let record = last_record_of_type(&wal, nodedb_wal::record::RecordType::Put);
        let (collection, document_id, value, _prov, surrogate) =
            zerompk::from_msgpack::<(String, String, Vec<u8>, Option<SyncProvenance>, u32)>(
                &record.payload,
            )
            .expect("decode put payload");
        assert_eq!(collection, "docs");
        assert_eq!(
            document_id,
            StorageKey::for_surrogate(Surrogate::new(9)).to_string()
        );
        assert_eq!(value, vec![1, 2, 3]);
        assert_eq!(surrogate, 9);
    }

    #[test]
    fn write_set_delete_appends_replayable_delete_record() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = open_wal(dir.path());
        let entries = vec![WriteSetEntry {
            surrogate: 9,
            is_delete: true,
            value: Vec::new(),
            collection: None,
        }];

        append_write_set_redo(
            &wal,
            TenantId::new(1),
            VShardId::new(0),
            DatabaseId::DEFAULT,
            "docs",
            &entries,
        )
        .expect("append");

        let record = last_record_of_type(&wal, nodedb_wal::record::RecordType::Delete);
        let (collection, document_id, _prov, surrogate) =
            zerompk::from_msgpack::<(String, String, Option<SyncProvenance>, u32)>(&record.payload)
                .expect("decode delete payload");
        assert_eq!(collection, "docs");
        assert_eq!(
            document_id,
            StorageKey::for_surrogate(Surrogate::new(9)).to_string()
        );
        assert_eq!(surrogate, 9);
    }

    #[test]
    fn empty_write_set_appends_nothing() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = open_wal(dir.path());
        let lsn = append_write_set_redo(
            &wal,
            TenantId::new(1),
            VShardId::new(0),
            DatabaseId::DEFAULT,
            "docs",
            &[],
        )
        .expect("append");
        assert!(lsn.is_none(), "empty write-set must append no record");
    }
}
