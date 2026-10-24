// SPDX-License-Identifier: BUSL-1.1

//! How many system-time ordinals a replicated write stamps.

#![deny(clippy::wildcard_enum_match_arm)]

use super::replicated_write::ReplicatedWrite;

impl ReplicatedWrite {
    /// The run of system-time ordinals the data group's leader reserves for
    /// this write, `None` for a write that lands no system-time version.
    ///
    /// - A document write stamps one ordinal: every version it lands takes
    ///   it, as one statement takes one system time.
    /// - A CRDT write stamps one: the rows it materializes into a bitemporal
    ///   collection take it.
    /// - A columnar insert stamps one ordinal per row, so two versions of a
    ///   key in one batch stay distinct. A columnar UPDATE stamps one: its
    ///   new versions take it. A columnar DELETE stamps one: the tombstone
    ///   versions that close its keys in a bitemporal collection take it. A
    ///   columnar TRUNCATE stamps one, for the same reason.
    /// - A timeseries ingest stamps one: its rows' ingest stamp.
    ///
    /// Index engines (KV, vector, FTS, spatial) carry no system time. Edge
    /// and label writes run as Calvin transactions and take the
    /// transaction's ordinal. A transaction redo carries the stamp of every
    /// version it installs.
    ///
    /// The match is exhaustive, so a new variant decides by name whether it
    /// stamps system time.
    pub fn system_ordinal_span(&self) -> Option<usize> {
        match self {
            Self::ColumnarIngest {
                payload,
                surrogates,
                ..
            } => Some(columnar_rows(payload, surrogates.len())),
            Self::ColumnarBulkDml { .. }
            | Self::ColumnarBulkDmlResolved { .. }
            | Self::ColumnarTruncate { .. } => Some(1),
            Self::PointPut { .. }
            | Self::PointInsert { .. }
            | Self::PointDelete { .. }
            | Self::PointUpdate { .. }
            | Self::DocUpsert { .. }
            | Self::DocBatchUpsert { .. }
            | Self::DocBatchInsert { .. }
            | Self::DocTruncate { .. }
            | Self::BulkDml { .. }
            | Self::ApplyBalanceDelta { .. }
            | Self::DocumentResolvedWrite { .. }
            | Self::MergeApply { .. }
            | Self::UpdateFromJoinApply { .. }
            | Self::CrdtApply { .. }
            | Self::CrdtApplyFenced { .. }
            | Self::CrdtApplyAuthenticated { .. }
            | Self::CrdtImportCollection { .. }
            | Self::CrdtListInsert { .. }
            | Self::CrdtListDelete { .. }
            | Self::CrdtListMove { .. }
            | Self::CrdtDocUpsert { .. }
            | Self::CrdtDocDelete { .. }
            | Self::TimeseriesIngest { .. } => Some(1),
            Self::CalvinReadResult { .. }
            | Self::CalvinReadTimeout { .. }
            | Self::CutBarrier { .. }
            | Self::TopicPublish { .. }
            | Self::SurrogateBind { .. }
            | Self::ArraySchema { .. }
            | Self::TransactionRedo { .. }
            | Self::RedoChunk { .. }
            | Self::RedoAbandon { .. }
            | Self::ArrayOp { .. }
            | Self::ArrayCellPut { .. }
            | Self::ArrayCellDelete { .. }
            | Self::VectorInsert { .. }
            | Self::VectorBatchInsert { .. }
            | Self::VectorDelete { .. }
            | Self::SetVectorParams { .. }
            | Self::SparseInsert { .. }
            | Self::SparseDelete { .. }
            | Self::MultiVectorInsert { .. }
            | Self::MultiVectorDelete { .. }
            | Self::DeleteBySurrogate { .. }
            | Self::DirectUpsert { .. }
            | Self::DropVectorIndex { .. }
            | Self::VectorDirectDelete { .. }
            | Self::VectorDirectTruncate { .. }
            | Self::VectorDirectUpdate { .. }
            | Self::VectorResolvedDirectWrite { .. }
            | Self::FtsIndex { .. }
            | Self::FtsDelete { .. }
            | Self::SpatialInsert { .. }
            | Self::SpatialDelete { .. }
            | Self::EdgePut { .. }
            | Self::EdgeDelete { .. }
            | Self::SetNodeLabels { .. }
            | Self::RemoveNodeLabels { .. }
            | Self::EdgePutBatch { .. }
            | Self::EdgeDeleteBatch { .. }
            | Self::KvPut { .. }
            | Self::KvDelete { .. }
            | Self::KvInsert { .. }
            | Self::KvInsertOnConflictUpdate { .. }
            | Self::KvBatchPut { .. }
            | Self::KvExpire { .. }
            | Self::KvPersist { .. }
            | Self::KvIncr { .. }
            | Self::KvIncrFloat { .. }
            | Self::KvCas { .. }
            | Self::KvGetSet { .. }
            | Self::KvRegisterSortedIndex { .. }
            | Self::KvDropSortedIndex { .. }
            | Self::KvRegisterIndex { .. }
            | Self::KvDropIndex { .. }
            | Self::KvFieldSet { .. }
            | Self::KvTransfer { .. }
            | Self::KvTransferItem { .. }
            | Self::KvTruncate { .. }
            | Self::KvResolvedWrite { .. }
            | Self::ConstraintChange { .. }
            | Self::TimeseriesTruncate { .. }
            | Self::TimeseriesAggregateRefresh { .. } => None,
        }
    }
}

/// The rows of a columnar insert payload: one per surrogate when the write
/// carries them, else the length of the MessagePack row array, else one row
/// (the payload is a single row object).
pub(crate) fn columnar_rows(payload: &[u8], surrogates: usize) -> usize {
    if surrogates > 0 {
        return surrogates;
    }
    nodedb_query::msgpack_scan::array_header(payload, 0).map_or(1, |(len, _)| len)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rows(n: usize) -> Vec<u8> {
        let row = nodedb_types::Value::Object(std::collections::HashMap::new());
        nodedb_types::value_to_msgpack(&nodedb_types::Value::Array(vec![row; n]))
            .expect("encode rows")
    }

    #[test]
    fn a_columnar_insert_spans_one_ordinal_per_row() {
        assert_eq!(columnar_rows(&rows(3), 0), 3);
        assert_eq!(columnar_rows(&rows(1), 4), 4, "surrogates count the rows");
    }

    #[test]
    fn a_document_write_spans_one_and_a_kv_write_none() {
        let put = ReplicatedWrite::PointPut {
            collection: "c".into(),
            document_id: "d".into(),
            value: Vec::new(),
            surrogate: 1,
            resolved_sum_targets: Vec::new(),
            resolved_sum_target_bindings: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
        };
        assert_eq!(put.system_ordinal_span(), Some(1));
        let barrier = ReplicatedWrite::CutBarrier {
            hlc: 1,
            restore_point: 0,
            capture: None,
        };
        assert_eq!(barrier.system_ordinal_span(), None);
    }

    /// A columnar DELETE spans one ordinal, as an UPDATE does: the
    /// tombstones that close its keys take it.
    #[test]
    fn a_columnar_delete_spans_one() {
        let delete = ReplicatedWrite::ColumnarBulkDml {
            collection: "c".into(),
            filters: Vec::new(),
            is_update: false,
            updates: Vec::new(),
        };
        assert_eq!(delete.system_ordinal_span(), Some(1));
        let resolved = ReplicatedWrite::ColumnarBulkDmlResolved {
            collection: "c".into(),
            is_update: false,
            rows: Vec::new(),
        };
        assert_eq!(resolved.system_ordinal_span(), Some(1));
    }
}
