// SPDX-License-Identifier: BUSL-1.1

//! The state one committed sub-record's install writes beside its
//! collection: the sync high-water mark a sync frame advances, and the
//! graph state of the record's tenant.
//!
//! The proposer reads these to declare a stream marker's keys. The Data
//! Plane reads them to key what its install walks. Both read the same
//! bytes through these functions, so the declared keys cover the walk.

use nodedb_types::sync::wire::SyncProvenance;
use nodedb_wal::record::{
    FtsDeletePayload, FtsIndexPayload, RecordType, SpatialDeletePayload, SpatialPutPayload,
};

use crate::engine::array::wal::{decode_delete_with_version, decode_put_with_version};
use crate::wal::{ArrayRestoreRedo, EdgeDeleteRedo, EdgePutRedo, NodeLabelRedo, RedoSubRecord};

/// A single vector insert: `(collection, vector, dim, field, surrogate,
/// provenance)`.
type VectorPutTuple = (String, Vec<f32>, usize, String, u32, Option<SyncProvenance>);
/// A vector delete: `(collection, surrogate, field, provenance)`.
type VectorDeleteTuple = (String, Option<u32>, String, Option<SyncProvenance>);

/// Whether the install of `sub` writes the graph state of its tenant: an
/// edge version, an edge cut, or a node-label delta.
pub fn sub_record_writes_graph(sub: &RedoSubRecord) -> bool {
    let record_type = RecordType::from_raw(sub.record_type);
    let payload = sub.payload.as_slice();
    record_type == Some(RecordType::GraphEdgeCut)
        || (matches!(
            record_type,
            Some(RecordType::GraphNodeLabelSet | RecordType::GraphNodeLabelRemove)
        ) && NodeLabelRedo::decode(payload).is_ok())
        || (record_type == Some(RecordType::Put)
            && zerompk::from_msgpack::<EdgePutRedo>(payload).is_ok())
        || (record_type == Some(RecordType::Delete)
            && zerompk::from_msgpack::<EdgeDeleteRedo>(payload).is_ok())
}

/// The provenance of the sync frame `sub` holds, as the committed-redo
/// arm that advances its mark reads it. `None` for a sub-record whose arm
/// advances no mark.
pub fn sub_record_provenance(sub: &RedoSubRecord) -> Option<SyncProvenance> {
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
        RecordType::ArrayPut => match ArrayRestoreRedo::decode(payload) {
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
