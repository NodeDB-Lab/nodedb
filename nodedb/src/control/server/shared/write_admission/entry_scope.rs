// SPDX-License-Identifier: BUSL-1.1

//! The lock scope the gate takes for one replicated entry.

use nodedb_types::QualifiedCollection;

use crate::control::wal_replication::ReplicatedWrite;

/// What the gate locks for one replicated entry.
pub(super) enum EntryScope {
    /// The entry writes no row, or its Calvin scheduler holds its locks.
    Exempt,
    /// The entry writes these collections whole.
    Collections(Vec<String>),
    /// The entry carries a plan the gate locks as it locks a local write.
    Plan,
}

/// The lock scope of `write`, an entry of `database_id` that names
/// `incarnations`.
pub(super) fn entry_scope(
    write: &ReplicatedWrite,
    incarnations: &[crate::control::wal_replication::CollectionIncarnation],
    database_id: crate::types::DatabaseId,
) -> EntryScope {
    match write {
        // Calvin bookkeeping, a backup cut, a mirror seal, a topic message, a
        // surrogate binding and an array schema write no row.
        ReplicatedWrite::CalvinReadResult { .. }
        | ReplicatedWrite::CalvinReadTimeout { .. }
        | ReplicatedWrite::CutBarrier { .. }
        | ReplicatedWrite::MirrorSeal { .. }
        | ReplicatedWrite::TopicPublish { .. }
        | ReplicatedWrite::SurrogateBind { .. }
        | ReplicatedWrite::ArraySchema { .. } => EntryScope::Exempt,
        // A Calvin transaction's redo carries its sequencer stamp, and its
        // scheduler holds its locks. A session transaction's redo writes
        // every collection and array it names. Each lock takes the name its
        // writers lock: a collection's plan name, an array's qualified name.
        ReplicatedWrite::TransactionRedo { calvin, .. } => {
            if calvin.is_some() {
                EntryScope::Exempt
            } else {
                EntryScope::Collections(
                    incarnations
                        .iter()
                        .map(|named| named.collection.clone())
                        .collect(),
                )
            }
        }
        // A redo chunk and an abandon write no row: the stream's final entry
        // takes the locks.
        ReplicatedWrite::RedoChunk { .. } | ReplicatedWrite::RedoAbandon { .. } => {
            EntryScope::Exempt
        }
        ReplicatedWrite::ArrayOp { array, .. } => EntryScope::Collections(vec![
            QualifiedCollection::new(database_id, array)
                .as_str()
                .to_owned(),
        ]),
        // Each rewrites or removes a collection whole.
        ReplicatedWrite::CollectionConvert { collection, .. } => EntryScope::Collections(vec![
            QualifiedCollection::new(database_id, collection)
                .as_str()
                .to_owned(),
        ]),
        ReplicatedWrite::TenantPurge { collections, .. } => EntryScope::Collections(
            collections
                .iter()
                .map(|name| {
                    QualifiedCollection::new(database_id, name)
                        .as_str()
                        .to_owned()
                })
                .collect(),
        ),
        ReplicatedWrite::TimeseriesAggregateImage { collection, .. } => {
            EntryScope::Collections(vec![collection.clone()])
        }
        ReplicatedWrite::RetentionDrop { op, .. } => match op.object() {
            Some(object) => EntryScope::Collections(vec![
                QualifiedCollection::new(database_id, object)
                    .as_str()
                    .to_owned(),
            ]),
            None => EntryScope::Exempt,
        },
        ReplicatedWrite::PointPut { .. }
        | ReplicatedWrite::PointInsert { .. }
        | ReplicatedWrite::PointDelete { .. }
        | ReplicatedWrite::PointUpdate { .. }
        | ReplicatedWrite::DocUpsert { .. }
        | ReplicatedWrite::DocBatchUpsert { .. }
        | ReplicatedWrite::DocBatchInsert { .. }
        | ReplicatedWrite::VectorInsert { .. }
        | ReplicatedWrite::VectorBatchInsert { .. }
        | ReplicatedWrite::VectorDelete { .. }
        | ReplicatedWrite::SetVectorParams { .. }
        | ReplicatedWrite::SparseInsert { .. }
        | ReplicatedWrite::SparseDelete { .. }
        | ReplicatedWrite::MultiVectorInsert { .. }
        | ReplicatedWrite::MultiVectorDelete { .. }
        | ReplicatedWrite::DeleteBySurrogate { .. }
        | ReplicatedWrite::DirectUpsert { .. }
        | ReplicatedWrite::CrdtApply { .. }
        | ReplicatedWrite::ColumnarIngest { .. }
        | ReplicatedWrite::TimeseriesIngest { .. }
        | ReplicatedWrite::FtsIndex { .. }
        | ReplicatedWrite::FtsDelete { .. }
        | ReplicatedWrite::SpatialInsert { .. }
        | ReplicatedWrite::SpatialDelete { .. }
        | ReplicatedWrite::EdgePut { .. }
        | ReplicatedWrite::EdgeDelete { .. }
        | ReplicatedWrite::SetNodeLabels { .. }
        | ReplicatedWrite::RemoveNodeLabels { .. }
        | ReplicatedWrite::EdgePutBatch { .. }
        | ReplicatedWrite::EdgeDeleteBatch { .. }
        | ReplicatedWrite::KvPut { .. }
        | ReplicatedWrite::KvDelete { .. }
        | ReplicatedWrite::KvInsert { .. }
        | ReplicatedWrite::KvInsertOnConflictUpdate { .. }
        | ReplicatedWrite::KvBatchPut { .. }
        | ReplicatedWrite::KvExpire { .. }
        | ReplicatedWrite::KvPersist { .. }
        | ReplicatedWrite::KvIncr { .. }
        | ReplicatedWrite::KvIncrFloat { .. }
        | ReplicatedWrite::KvCas { .. }
        | ReplicatedWrite::KvGetSet { .. }
        | ReplicatedWrite::KvRegisterSortedIndex { .. }
        | ReplicatedWrite::KvDropSortedIndex { .. }
        | ReplicatedWrite::KvRegisterIndex { .. }
        | ReplicatedWrite::KvDropIndex { .. }
        | ReplicatedWrite::KvFieldSet { .. }
        | ReplicatedWrite::KvTransfer { .. }
        | ReplicatedWrite::KvTransferItem { .. }
        | ReplicatedWrite::BulkDml { .. }
        | ReplicatedWrite::ColumnarBulkDml { .. }
        | ReplicatedWrite::CrdtImportCollection { .. }
        | ReplicatedWrite::CrdtListInsert { .. }
        | ReplicatedWrite::CrdtListDelete { .. }
        | ReplicatedWrite::CrdtListMove { .. }
        | ReplicatedWrite::CrdtDocUpsert { .. }
        | ReplicatedWrite::CrdtDocDelete { .. }
        | ReplicatedWrite::DocTruncate { .. }
        | ReplicatedWrite::KvTruncate { .. }
        | ReplicatedWrite::ConstraintChange { .. }
        | ReplicatedWrite::ArrayCellPut { .. }
        | ReplicatedWrite::ArrayCellDelete { .. }
        | ReplicatedWrite::CrdtApplyFenced { .. }
        | ReplicatedWrite::CrdtApplyAuthenticated { .. }
        | ReplicatedWrite::DropVectorIndex { .. }
        | ReplicatedWrite::ApplyBalanceDelta { .. }
        | ReplicatedWrite::ColumnarBulkDmlResolved { .. }
        | ReplicatedWrite::ColumnarTruncate { .. }
        | ReplicatedWrite::TimeseriesTruncate { .. }
        | ReplicatedWrite::TimeseriesAggregateRefresh { .. }
        | ReplicatedWrite::KvResolvedWrite { .. }
        | ReplicatedWrite::DocumentResolvedWrite { .. }
        | ReplicatedWrite::VectorDirectDelete { .. }
        | ReplicatedWrite::VectorDirectTruncate { .. }
        | ReplicatedWrite::VectorDirectUpdate { .. }
        | ReplicatedWrite::VectorResolvedDirectWrite { .. }
        | ReplicatedWrite::MergeApply { .. }
        | ReplicatedWrite::UpdateFromJoinApply { .. } => EntryScope::Plan,
    }
}
