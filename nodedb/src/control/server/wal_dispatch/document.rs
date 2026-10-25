// SPDX-License-Identifier: BUSL-1.1

//! WAL append dispatch for `PhysicalPlan::Document(DocumentOp)`.

#![deny(clippy::wildcard_enum_match_arm)]

use nodedb_physical::physical_plan::DocumentOp;

use crate::bridge::envelope::RowVersion;
use crate::types::{DatabaseId, Lsn, TenantId, VShardId};
use crate::wal::manager::WalAppender;

/// Encode a document PUT redo record: `(collection, document_id, value,
/// Option<SyncProvenance>, surrogate)`, with `(sys_from, valid_from_ms,
/// valid_until_ms)` appended when `version` is `Some`; `sys_from` is the
/// version's system-time ordinal. Must match
/// `wal_replay_redo_document`'s decode.
///
/// `document_id` is the row's client identity. The Event Plane replay reads
/// it back verbatim as the event's row id.
pub(crate) fn encode_document_put_record(
    collection: &str,
    document_id: &str,
    value: &[u8],
    surrogate: u32,
    version: Option<RowVersion>,
) -> crate::Result<Vec<u8>> {
    let prov: Option<nodedb_types::sync::wire::SyncProvenance> = None;
    match version {
        Some(v) => zerompk::to_msgpack_vec(&(
            collection,
            document_id,
            value,
            prov,
            surrogate,
            v.sys_from,
            v.valid_from_ms,
            v.valid_until_ms,
        )),
        None => zerompk::to_msgpack_vec(&(collection, document_id, value, prov, surrogate)),
    }
    .map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("wal document put: {e}"),
    })
}

/// Encode a document DELETE redo record: `(collection, document_id,
/// Option<SyncProvenance>, surrogate)` — surrogate keys the redb storage row —
/// with the tombstone's system-time ordinal appended when `system_from` is
/// `Some`.
///
/// `document_id` is the row's client identity, as for the PUT record.
pub(crate) fn encode_document_delete_record(
    collection: &str,
    document_id: &str,
    surrogate: u32,
    system_from: Option<i64>,
) -> crate::Result<Vec<u8>> {
    let prov: Option<nodedb_types::sync::wire::SyncProvenance> = None;
    match system_from {
        Some(sys) => zerompk::to_msgpack_vec(&(collection, document_id, prov, surrogate, sys)),
        None => zerompk::to_msgpack_vec(&(collection, document_id, prov, surrogate)),
    }
    .map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("wal document delete: {e}"),
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
            payload: encode_document_delete_record(
                collection.as_str(),
                document_id.as_str(),
                surrogate.as_u32(),
                None,
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
            payload: encode_document_put_record(
                collection.as_str(),
                document_id.as_str(),
                value,
                surrogate.as_u32(),
                None,
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
            // Plan-time materialized-sum resolution is not part of the applied record.
            resolved_sum_targets: _,
        } => {
            // A versioned row's stamp is decided at apply, so its write set
            // carries the stamped image and cancels this record.
            let entry = encode_document_put_record(
                collection.as_str(),
                document_id.as_str(),
                value,
                surrogate.as_u32(),
                None,
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
            // See `PointPut`.
            resolved_sum_targets: _,
            deferred_sum_targets: _,
        } => {
            // An `if_absent` insert that finds the row writes nothing: its
            // write set cancels this record, as it does for a versioned row.
            let entry = encode_document_put_record(
                collection.as_str(),
                document_id.as_str(),
                value,
                surrogate.as_u32(),
                None,
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
            let entry = encode_document_delete_record(
                collection.as_str(),
                document_id.as_str(),
                surrogate.as_u32(),
                None,
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
        // Collection config and derived index entries: the catalog holds the
        // config, and the index entries derive from the journalled rows.
        DocumentOp::Register { .. }
        | DocumentOp::DropIndex { .. }
        | DocumentOp::BackfillIndex { .. } => None,
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
