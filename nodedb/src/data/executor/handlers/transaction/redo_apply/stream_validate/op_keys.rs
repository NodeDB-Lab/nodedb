// SPDX-License-Identifier: BUSL-1.1

//! The fence keys of state a sub-record's install writes beside its
//! collection: the sync high-water mark a sync frame advances, and the
//! graph state of the record's tenant.
//!
//! The record's plan names neither, so a yielding install arms them from
//! the walk. The walk reaches every sub-record before the install's first
//! batch. Each slice arms what it walked before the core runs another task.
//! So no task touches the state between its sub-record's walk and the
//! install.

use std::collections::BTreeSet;

use nodedb_types::sync::wire::SyncProvenance;
use nodedb_wal::record::{
    FtsDeletePayload, FtsIndexPayload, RecordType, SpatialDeletePayload, SpatialPutPayload,
};

use crate::data::executor::core_loop::install_fence::FenceKey;
use crate::engine::array::wal::{decode_delete_with_version, decode_put_with_version};
use crate::types::{DatabaseId, TenantId};
use crate::wal::RedoSubRecord;

use super::super::sub_ops::label_op;
use super::super::write_keys::edge_write;

/// A single vector insert: `(collection, vector, dim, field, surrogate,
/// provenance)`.
type VectorPutTuple = (String, Vec<f32>, usize, String, u32, Option<SyncProvenance>);
/// A vector delete: `(collection, surrogate, field, provenance)`.
type VectorDeleteTuple = (String, Option<u32>, String, Option<SyncProvenance>);

/// Add to `keys` the keys of the state the install of `sub` writes beside
/// its collection. `db` and `tenant` are the installing request's.
pub(super) fn add_sub_record_keys(
    sub: &RedoSubRecord,
    db: DatabaseId,
    tenant: TenantId,
    keys: &mut BTreeSet<FenceKey>,
) {
    keys.extend(
        sub_record_provenance(sub)
            .as_ref()
            .and_then(FenceKey::sync_stream),
    );
    // The graph key joins once. The edge and label decode runs only until
    // a record has joined it.
    let graph = FenceKey::Graph { db, tenant };
    if !keys.contains(&graph) && writes_graph(sub) {
        keys.insert(graph);
    }
}

/// Whether the install of `sub` writes the graph state of its tenant: an
/// edge version, an edge cut, or a node-label delta.
fn writes_graph(sub: &RedoSubRecord) -> bool {
    RecordType::from_raw(sub.record_type) == Some(RecordType::GraphEdgeCut)
        || label_op(sub).is_some()
        || edge_write(sub).is_some()
}

/// The provenance of the sync frame `sub` holds, as the committed-redo
/// arm that advances its mark reads it. `None` for a sub-record whose arm
/// advances no mark.
fn sub_record_provenance(sub: &RedoSubRecord) -> Option<SyncProvenance> {
    let record_type = RecordType::from_raw(sub.record_type)?;
    let payload = sub.payload.as_slice();
    match record_type {
        RecordType::VectorPut => zerompk::from_msgpack::<VectorPutTuple>(payload)
            .ok()
            .and_then(|(.., provenance)| provenance),
        RecordType::VectorDelete => zerompk::from_msgpack::<VectorDeleteTuple>(payload)
            .ok()
            .and_then(|(.., provenance)| provenance),
        RecordType::FtsIndex => FtsIndexPayload::from_bytes(payload)
            .ok()
            .map(|payload| payload.provenance),
        RecordType::FtsDelete => FtsDeletePayload::from_bytes(payload)
            .ok()
            .map(|payload| payload.provenance),
        RecordType::SpatialPut => SpatialPutPayload::from_bytes(payload)
            .ok()
            .map(|payload| payload.provenance),
        RecordType::SpatialDelete => SpatialDeletePayload::from_bytes(payload)
            .ok()
            .map(|payload| payload.provenance),
        // A restored cell carries no provenance. Its arm tries it first.
        RecordType::ArrayPut => match crate::wal::ArrayRestoreRedo::decode(payload) {
            Some(_) => None,
            None => decode_put_with_version(payload)
                .ok()
                .and_then(|payload| payload.provenance),
        },
        RecordType::ArrayDelete => decode_delete_with_version(payload)
            .ok()
            .and_then(|payload| payload.provenance),
        // A committed redo carries columnar rows as images, which advance
        // no mark. Only a timeseries ingest does.
        RecordType::TimeseriesBatch => crate::wal::decode_batch_record(payload)
            .ok()
            .filter(|record| record.kind == "timeseries")
            .and_then(|record| record.provenance),
        // A committed record never carries a raw CRDT delta, the one CRDT
        // record with provenance: the validate refuses it.
        RecordType::Noop
        | RecordType::Put
        | RecordType::Delete
        | RecordType::VectorParams
        | RecordType::VectorIndexDrop
        | RecordType::VectorDirectUpsert
        | RecordType::VectorDirectDelete
        | RecordType::VectorDirectUpdate
        | RecordType::VectorDirectTruncate
        | RecordType::ColumnarTruncate
        | RecordType::TimeseriesTruncate
        | RecordType::VectorResolvedDirectWrite
        | RecordType::SparseVectorPut
        | RecordType::SparseVectorDelete
        | RecordType::MultiVectorPut
        | RecordType::MultiVectorDelete
        | RecordType::CrdtDelta
        | RecordType::CrdtListOp
        | RecordType::CrdtDocOp
        | RecordType::LogBatch
        | RecordType::ArrayFlush
        | RecordType::Transaction
        | RecordType::TransactionRedo
        | RecordType::Checkpoint
        | RecordType::CollectionTombstoned
        | RecordType::TimeAnchor
        | RecordType::TemporalPurge
        | RecordType::SurrogateAlloc
        | RecordType::SurrogateBindBatch
        | RecordType::GraphNodeLabelSet
        | RecordType::GraphNodeLabelRemove
        | RecordType::WriteAborted
        | RecordType::ProposalApplied
        | RecordType::ChangePosition
        | RecordType::RestorePoint
        | RecordType::GraphEdgeCut
        | RecordType::SnapshotInstalled
        | RecordType::WriteGroup
        | RecordType::RedoChunk
        | RecordType::RedoStreamRef
        | RecordType::RedoBoundary
        | RecordType::DocumentIndexBuild
        | RecordType::DocumentIndexRetire
        | RecordType::SyncSeqAdvance => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::server::wal_dispatch::encode_vector_put_payload;
    use nodedb_types::Surrogate;

    fn provenance(producer_id: u64) -> SyncProvenance {
        SyncProvenance {
            producer_id,
            epoch: 1,
            stream_id: 42,
            seq: 1,
        }
    }

    fn vector_put(provenance: Option<&SyncProvenance>) -> RedoSubRecord {
        let payload =
            encode_vector_put_payload("v", &[1.0, 2.0], 2, "", Surrogate::new(9), provenance)
                .expect("encode vector put");
        RedoSubRecord {
            record_type: RecordType::VectorPut as u32,
            payload,
        }
    }

    fn keys_of(sub: &RedoSubRecord) -> BTreeSet<FenceKey> {
        let mut keys = BTreeSet::new();
        add_sub_record_keys(sub, DatabaseId::DEFAULT, TenantId::new(1), &mut keys);
        keys
    }

    #[test]
    fn a_sync_frame_keys_its_stream() {
        let keys = keys_of(&vector_put(Some(&provenance(7))));
        assert_eq!(
            keys.into_iter().collect::<Vec<_>>(),
            vec![FenceKey::SyncStream {
                producer_id: 7,
                stream_id: 42,
            }]
        );
    }

    #[test]
    fn a_frame_of_producer_zero_and_a_plain_write_key_nothing() {
        assert!(keys_of(&vector_put(Some(&provenance(0)))).is_empty());
        assert!(keys_of(&vector_put(None)).is_empty());
    }

    #[test]
    fn an_edge_put_keys_the_graph_of_its_tenant() {
        let edge = RedoSubRecord {
            record_type: RecordType::Put as u32,
            payload: zerompk::to_msgpack_vec(&crate::wal::EdgePutRedo {
                collection: "knows".into(),
                src_id: "a".into(),
                label: "KNOWS".into(),
                dst_id: "b".into(),
                properties: Vec::new(),
                src_surrogate: 1,
                dst_surrogate: 2,
                system_from: Some(5),
                applied: None,
                valid_from_ms: None,
                valid_until_ms: None,
            })
            .expect("encode edge put"),
        };
        assert_eq!(
            keys_of(&edge).into_iter().collect::<Vec<_>>(),
            vec![FenceKey::Graph {
                db: DatabaseId::DEFAULT,
                tenant: TenantId::new(1),
            }]
        );
    }

    #[test]
    fn a_document_put_keys_nothing_beside_its_collection() {
        let body = nodedb_types::json_to_msgpack(&serde_json::json!({"n": 1})).expect("body");
        let put = super::super::super::test_commit::doc_put_sub_record("docs", "d1", &body, 1);
        assert!(keys_of(&put).is_empty());
    }
}
