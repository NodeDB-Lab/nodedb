// SPDX-License-Identifier: BUSL-1.1

//! WAL append dispatch for `PhysicalPlan::Document(DocumentOp)`.

#![deny(clippy::wildcard_enum_match_arm)]

use nodedb_physical::physical_plan::DocumentOp;

use crate::types::{DatabaseId, Lsn, TenantId, VShardId};
use crate::wal::manager::WalAppender;

/// Encode the unstamped PUT record of a point write's pre-dispatch or
/// opening forward record: `(collection, document_id, value,
/// Option<SyncProvenance>, surrogate)`. The apply decides the version key,
/// and its stamped write-set image replaces this record on replay. No other
/// producer writes this form.
fn encode_forward_put_record(
    collection: &str,
    document_id: &str,
    value: &[u8],
    surrogate: u32,
) -> crate::Result<Vec<u8>> {
    let prov: Option<nodedb_types::sync::wire::SyncProvenance> = None;
    zerompk::to_msgpack_vec(&(collection, document_id, value, prov, surrogate)).map_err(|e| {
        crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("wal document put: {e}"),
        }
    })
}

/// Encode a stamped document DELETE redo record: `(collection, document_id,
/// Option<SyncProvenance>, surrogate, system_from)`. The surrogate keys the
/// redb storage row. `system_from` is the tombstone's system-time ordinal.
///
/// `document_id` is the row's client identity, as for the PUT record.
pub(crate) fn encode_document_delete_record(
    collection: &str,
    document_id: &str,
    surrogate: u32,
    system_from: i64,
) -> crate::Result<Vec<u8>> {
    let prov: Option<nodedb_types::sync::wire::SyncProvenance> = None;
    zerompk::to_msgpack_vec(&(collection, document_id, prov, surrogate, system_from)).map_err(|e| {
        crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("wal document delete: {e}"),
        }
    })
}

/// Encode the unstamped DELETE record of a point delete's pre-dispatch or
/// opening forward record: `(collection, document_id,
/// Option<SyncProvenance>, surrogate)`. No other producer writes this form.
fn encode_forward_delete_record(
    collection: &str,
    document_id: &str,
    surrogate: u32,
) -> crate::Result<Vec<u8>> {
    let prov: Option<nodedb_types::sync::wire::SyncProvenance> = None;
    zerompk::to_msgpack_vec(&(collection, document_id, prov, surrogate)).map_err(|e| {
        crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("wal document delete: {e}"),
        }
    })
}

/// The forward sub-record of a point write whose apply always reports rows
/// beyond its own: a delete, whose node cascade and sum folds land after it,
/// and a put or insert that folds materialized-sum targets. The funnel
/// journals such a forward record as the opening record of the write's
/// group, so a restore that lacks the parts drops the forward record too.
/// `None` for every other op.
pub(super) fn opening_forward_sub_record(
    op: &DocumentOp,
) -> crate::Result<Option<crate::wal::RedoSubRecord>> {
    use nodedb_wal::record::RecordType;
    if let DocumentOp::PointDelete {
        collection,
        document_id,
        surrogate: Some(surrogate),
        ..
    } = op
    {
        return Ok(Some(crate::wal::RedoSubRecord {
            record_type: RecordType::Delete as u32,
            payload: encode_forward_delete_record(
                collection.as_str(),
                document_id.as_str(),
                surrogate.as_u32(),
            )?,
        }));
    }
    if let DocumentOp::PointPut {
        collection,
        document_id,
        value,
        surrogate,
        resolved_sum_targets,
        ..
    }
    | DocumentOp::PointInsert {
        collection,
        document_id,
        value,
        surrogate,
        resolved_sum_targets,
        ..
    } = op
        && !resolved_sum_targets.is_empty()
    {
        return Ok(Some(crate::wal::RedoSubRecord {
            record_type: RecordType::Put as u32,
            payload: encode_forward_put_record(
                collection.as_str(),
                document_id.as_str(),
                value,
                surrogate.as_u32(),
            )?,
        }));
    }
    Ok(None)
}

/// Append the pre-dispatch WAL record for a `DocumentOp`: the allocated LSN
/// for the point writes whose plan carries the row's post-image, `None`
/// otherwise. Every other document write journals the rows it stored after
/// apply, from `Response::write_set` (see `write_set_redo`). Exhaustive so a
/// new variant can't silently skip durability.
pub(super) fn wal_append_document_op(
    wal: WalAppender<'_>,
    tenant_id: TenantId,
    vshard_id: VShardId,
    database_id: DatabaseId,
    op: &DocumentOp,
) -> crate::Result<Option<Lsn>> {
    let appended = match op {
        DocumentOp::PointPut {
            collection,
            document_id,
            value,
            surrogate,
            pk_bytes: _,
            // Projection is answered from the Data Plane's response, not the journal.
            returning: _,
            rls_filters: _,
            rls_write_check: _,
            // Plan-time materialized-sum resolution is not part of the applied record.
            resolved_sum_targets: _,
        } => {
            // A versioned row's stamp is decided at apply, so its write set
            // carries the stamped image and cancels this record.
            let entry = encode_forward_put_record(
                collection.as_str(),
                document_id.as_str(),
                value,
                surrogate.as_u32(),
            )?;
            Some(wal.append_put(tenant_id, vshard_id, database_id, &entry)?)
        }
        DocumentOp::PointInsert {
            collection,
            document_id,
            value,
            if_absent: _,
            surrogate,
            returning: _,
            rls_filters: _,
            rls_write_check: _,
            // See `PointPut`.
            resolved_sum_targets: _,
            deferred_sum_targets: _,
        } => {
            // An `if_absent` insert that finds the row writes nothing: its
            // write set cancels this record, as it does for a versioned row.
            let entry = encode_forward_put_record(
                collection.as_str(),
                document_id.as_str(),
                value,
                surrogate.as_u32(),
            )?;
            Some(wal.append_put(tenant_id, vshard_id, database_id, &entry)?)
        }
        // A delete of a key unbound in its database removes no row, so it
        // has no durable effect to journal.
        DocumentOp::PointDelete {
            surrogate: None, ..
        } => None,
        DocumentOp::PointDelete {
            collection,
            document_id,
            surrogate: Some(surrogate),
            ..
        } => {
            // The surrogate keys the row, its secondary indexes and its vector
            // nodes on replay.
            let entry = encode_forward_delete_record(
                collection.as_str(),
                document_id.as_str(),
                surrogate.as_u32(),
            )?;
            Some(wal.append_delete(tenant_id, vshard_id, database_id, &entry)?)
        }
        // NotAWrite — reads / query ops / DDL that produces no engine mutation here
        DocumentOp::ResolveWrite(_)
        | DocumentOp::PointGet { .. }
        | DocumentOp::Scan { .. }
        | DocumentOp::RangeScan { .. }
        | DocumentOp::IndexLookup { .. }
        | DocumentOp::IndexedFetch { .. }
        | DocumentOp::EstimateCount { .. }
        | DocumentOp::MaterializeScan { .. } => None,
        // The rows these write are decided at apply: an update's post-image,
        // a predicate's row set, an upsert's branch, a balance fold. The Data
        // Plane reports every stored row in `Response::write_set`, and the
        // post-apply redo journals each one.
        DocumentOp::PointUpdate { .. }
        | DocumentOp::Upsert { .. }
        | DocumentOp::BatchUpsert { .. }
        | DocumentOp::BatchInsert { .. }
        | DocumentOp::BulkUpdate { .. }
        | DocumentOp::BulkDelete { .. }
        | DocumentOp::Merge { .. }
        | DocumentOp::UpdateFromJoin { .. }
        | DocumentOp::Truncate { .. }
        | DocumentOp::ApplyBalanceDelta { .. }
        | DocumentOp::ResolvedWrite { .. } => None,
        // Resolved on the Control Plane into `BatchInsert` pages; the Data
        // Plane refuses it.
        DocumentOp::InsertSelect { .. } => None,
        // Collection config: the catalog holds it.
        DocumentOp::Register { .. } => None,
        // A secondary index's build and purge journal themselves. Replay runs
        // each at its LSN, against the rows stored below it.
        DocumentOp::BackfillIndex {
            collection,
            path,
            is_array,
            unique,
            case_insensitive,
            predicate,
            owner,
            fence,
        } => {
            let record = crate::wal::DocumentIndexBuildRecord {
                collection: collection.as_str().to_owned(),
                path: path.clone(),
                is_array: *is_array,
                unique: *unique,
                case_insensitive: *case_insensitive,
                predicate: predicate.clone(),
                owner: owner.clone(),
                fence: *fence,
            };
            Some(wal.append_document_index_build(
                tenant_id,
                vshard_id,
                database_id,
                &record.to_bytes()?,
            )?)
        }
        DocumentOp::DropIndex {
            collection,
            field,
            owner,
            fence,
        } => {
            let record = crate::wal::DocumentIndexRetireRecord {
                collection: collection.as_str().to_owned(),
                field: field.clone(),
                owner: owner.clone(),
                fence: *fence,
            };
            Some(wal.append_document_index_retire(
                tenant_id,
                vshard_id,
                database_id,
                &record.to_bytes()?,
            )?)
        }
    };
    Ok(appended)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::manager::WalManager;
    use nodedb_physical::physical_plan::PhysicalPlan;
    use nodedb_types::{QualifiedCollection, Surrogate};

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
    fn point_put_appends_put_record() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = open_wal(dir.path());
        let plan = PhysicalPlan::Document(DocumentOp::PointPut {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "users"),
            document_id: "u1".to_string(),
            value: vec![1, 2, 3],
            surrogate: Surrogate::new(5),
            pk_bytes: vec![],
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
        });

        let outcome = super::super::wal_append_if_write(
            &wal,
            TenantId::new(1),
            VShardId::new(0),
            DatabaseId::DEFAULT,
            &plan,
        )
        .expect("append");
        assert!(outcome.lsn.is_some(), "PointPut must produce a durable LSN");

        let record = last_record_of_type(&wal, nodedb_wal::record::RecordType::Put);
        let (collection, document_id, _value, _prov, surrogate) = zerompk::from_msgpack::<(
            String,
            String,
            Vec<u8>,
            Option<nodedb_types::sync::wire::SyncProvenance>,
            u32,
        )>(&record.payload)
        .expect("decode point put payload");
        assert_eq!(collection, "users");
        assert_eq!(document_id, "u1");
        assert_eq!(surrogate, 5);
    }

    #[test]
    fn point_delete_appends_delete_record() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = open_wal(dir.path());
        let plan = PhysicalPlan::Document(DocumentOp::PointDelete {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "users"),
            document_id: "u1".to_string(),
            surrogate: Some(Surrogate::new(5)),
            pk_bytes: vec![],
            returning: None,
            rls_filters: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            resolved_sum_targets: Vec::new(),
        });

        let outcome = super::super::wal_append_if_write(
            &wal,
            TenantId::new(1),
            VShardId::new(0),
            DatabaseId::DEFAULT,
            &plan,
        )
        .expect("append");
        assert!(
            outcome.lsn.is_some(),
            "PointDelete must produce a durable LSN"
        );
        let _ = last_record_of_type(&wal, nodedb_wal::record::RecordType::Delete);
    }

    #[test]
    fn index_build_appends_its_own_record() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = open_wal(dir.path());
        let plan = PhysicalPlan::Document(DocumentOp::BackfillIndex {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "users"),
            path: "$.email".to_string(),
            is_array: false,
            unique: true,
            case_insensitive: false,
            predicate: Some("active = true".to_string()),
            owner: nodedb_physical::physical_plan::DocIndexOwner::Named("by_email".into()),
            fence: nodedb_physical::physical_plan::DocIndexFence {
                incarnation: nodedb_types::Hlc::new(5, 0),
                descriptor_version: 3,
            },
        });
        let outcome = super::super::wal_append_if_write(
            &wal,
            TenantId::new(1),
            VShardId::new(0),
            DatabaseId::DEFAULT,
            &plan,
        )
        .expect("append");
        assert!(outcome.lsn.is_some(), "an index build journals a record");
        let record = last_record_of_type(&wal, nodedb_wal::record::RecordType::DocumentIndexBuild);
        let decoded =
            crate::wal::DocumentIndexBuildRecord::from_bytes(&record.payload).expect("decode");
        assert_eq!(decoded.collection, "users");
        assert_eq!(decoded.path, "$.email");
        assert!(decoded.unique);
        assert_eq!(decoded.predicate.as_deref(), Some("active = true"));
        assert_eq!(
            decoded.owner,
            nodedb_physical::physical_plan::DocIndexOwner::Named("by_email".into())
        );
        assert_eq!(decoded.fence.descriptor_version, 3);
    }

    #[test]
    fn index_purge_appends_its_own_record() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = open_wal(dir.path());
        let plan = PhysicalPlan::Document(DocumentOp::DropIndex {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "users"),
            field: "$.email".to_string(),
            owner: nodedb_physical::physical_plan::DocIndexOwner::Declared,
            fence: nodedb_physical::physical_plan::DocIndexFence::default(),
        });
        let outcome = super::super::wal_append_if_write(
            &wal,
            TenantId::new(1),
            VShardId::new(0),
            DatabaseId::DEFAULT,
            &plan,
        )
        .expect("append");
        assert!(outcome.lsn.is_some(), "an index purge journals a record");
        let record = last_record_of_type(&wal, nodedb_wal::record::RecordType::DocumentIndexRetire);
        let decoded =
            crate::wal::DocumentIndexRetireRecord::from_bytes(&record.payload).expect("decode");
        assert_eq!(decoded.collection, "users");
        assert_eq!(decoded.field, "$.email");
        assert_eq!(
            decoded.owner,
            nodedb_physical::physical_plan::DocIndexOwner::Declared
        );
    }

    #[test]
    fn read_op_appends_nothing() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = open_wal(dir.path());
        let plan = PhysicalPlan::Document(DocumentOp::EstimateCount {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "users"),
            field: "id".to_string(),
        });

        let outcome = super::super::wal_append_if_write(
            &wal,
            TenantId::new(1),
            VShardId::new(0),
            DatabaseId::DEFAULT,
            &plan,
        )
        .expect("append");
        assert!(outcome.lsn.is_none(), "read op must produce no durable LSN");
    }
}
