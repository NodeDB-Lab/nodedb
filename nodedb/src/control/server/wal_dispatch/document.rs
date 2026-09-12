// SPDX-License-Identifier: BUSL-1.1

//! WAL append dispatch for `PhysicalPlan::Document(DocumentOp)`.

#![deny(clippy::wildcard_enum_match_arm)]

use nodedb_physical::physical_plan::DocumentOp;

use crate::types::{DatabaseId, Lsn, TenantId, VShardId};
use crate::wal::manager::WalManager;

/// Encode a document PUT redo record: `(collection, document_id, value,
/// Option<SyncProvenance>, surrogate)`. Must match `wal_replay_redo_document`'s decode.
///
/// `document_id` is the row's client identity. The Event Plane replay reads
/// it back verbatim as the event's row id.
pub(crate) fn encode_document_put_record(
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

/// Encode a document DELETE redo record: `(collection, document_id,
/// Option<SyncProvenance>, surrogate)` — surrogate keys the redb storage row.
///
/// `document_id` is the row's client identity, as for the PUT record.
pub(crate) fn encode_document_delete_record(
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

/// Append the WAL record for a `DocumentOp`: the allocated LSN for point-write
/// variants, `None` otherwise. Exhaustive so a new variant can't silently skip durability.
pub(super) fn wal_append_document_op(
    wal: &WalManager,
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
            let entry = encode_document_put_record(
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
            // See `PointPut`.
            resolved_sum_targets: _,
            deferred_sum_targets: _,
        } => {
            let entry = encode_document_put_record(
                collection.as_str(),
                document_id.as_str(),
                value,
                surrogate.as_u32(),
            )?;
            Some(wal.append_put(tenant_id, vshard_id, database_id, &entry)?)
        }
        DocumentOp::PointDelete {
            collection,
            document_id,
            surrogate,
            ..
        } => {
            // 4-tuple keys secondary vector-index removal by surrogate on restart —
            // a 3-tuple would leave the deleted embedding to resurrect.
            let entry = encode_document_delete_record(
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
        | DocumentOp::MaterializeScan { .. }
        // Durability comes from the post-apply write-set redo, which re-derives its vShard.
        | DocumentOp::ApplyBalanceDelta { .. }
        // Durable as the committed Raft entry; per-row redo shapes can't express a mutation list.
        | DocumentOp::ResolvedWrite { .. } => None,
        // Row is redb-synchronous-durable; secondary-vector-index restart fidelity
        // would need an apply-time per-row Put/Delete record — tracked, not built here.
        DocumentOp::PointUpdate { .. }
        | DocumentOp::Upsert { .. }
        | DocumentOp::BatchInsert { .. }
        | DocumentOp::InsertSelect { .. }
        | DocumentOp::BulkUpdate { .. }
        | DocumentOp::BulkDelete { .. }
        | DocumentOp::Merge { .. }
        | DocumentOp::UpdateFromJoin { .. } => None,
        // Row deletion is redb-durable; per-row HNSW cleanup is carried in
        // `Response::write_set` and minted as a post-apply `Delete` redo.
        DocumentOp::Truncate { .. } => None,
        // DurableElsewhere — index state is catalog + redb durable
        DocumentOp::Register { .. }
        | DocumentOp::DropIndex { .. }
        | DocumentOp::BackfillIndex { .. } => None,
    };
    Ok(appended)
}

#[cfg(test)]
mod tests {
    use super::*;
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
            surrogate: Surrogate::new(5),
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
