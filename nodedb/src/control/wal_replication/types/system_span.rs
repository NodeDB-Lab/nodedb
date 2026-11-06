// SPDX-License-Identifier: BUSL-1.1

//! How many system-time ordinals a replicated write stamps.

#![deny(clippy::wildcard_enum_match_arm)]

use super::replicated_write::ReplicatedWrite;

impl ReplicatedWrite {
    /// The run of system-time ordinals the data group's leader reserves for
    /// this write, `None` for a write that lands no row version.
    ///
    /// - A columnar insert stamps one ordinal per row, so two versions of a
    ///   key in one batch stay distinct.
    /// - Every other write of rows stamps one ordinal: every version it
    ///   lands takes it, as one statement takes one system time. That holds
    ///   for document, CRDT, columnar UPDATE / DELETE / TRUNCATE, timeseries,
    ///   KV, array, vector, FTS, spatial, and edge writes, and for a
    ///   transaction redo, whose KV rows, timeseries batches, and array cells
    ///   take it. A session redo's stamped versions take at least it
    ///   (`wal::redo::stamp_floor`).
    /// - A write that lands no row version (a schema or index change, a
    ///   barrier, a topic message, a surrogate bind, a Calvin read relay, a
    ///   redo chunk the final redo entry installs) reserves none.
    ///
    /// Every replica stamps the entry's ordinal, so every replica, and every
    /// replay of every replica's records, writes the same version keys.
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
            | Self::ColumnarTruncate { .. }
            | Self::PointPut { .. }
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
            | Self::TimeseriesIngest { .. }
            | Self::TimeseriesTruncate { .. }
            | Self::TimeseriesAggregateRefresh { .. }
            | Self::TransactionRedo { .. }
            | Self::ArrayOp { .. }
            | Self::ArrayCellPut { .. }
            | Self::ArrayCellDelete { .. }
            | Self::VectorInsert { .. }
            | Self::VectorBatchInsert { .. }
            | Self::VectorDelete { .. }
            | Self::SparseInsert { .. }
            | Self::SparseDelete { .. }
            | Self::MultiVectorInsert { .. }
            | Self::MultiVectorDelete { .. }
            | Self::DeleteBySurrogate { .. }
            | Self::DirectUpsert { .. }
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
            | Self::KvFieldSet { .. }
            | Self::KvTransfer { .. }
            | Self::KvTransferItem { .. }
            | Self::KvTruncate { .. }
            | Self::KvResolvedWrite { .. } => Some(1),
            Self::CalvinReadResult { .. }
            | Self::CalvinReadTimeout { .. }
            | Self::CutBarrier { .. }
            | Self::MirrorSeal { .. }
            | Self::TopicPublish { .. }
            | Self::SurrogateBind { .. }
            | Self::ArraySchema { .. }
            | Self::RedoChunk { .. }
            | Self::RedoAbandon { .. }
            | Self::SetVectorParams { .. }
            | Self::DropVectorIndex { .. }
            | Self::KvRegisterSortedIndex { .. }
            | Self::KvDropSortedIndex { .. }
            | Self::KvRegisterIndex { .. }
            | Self::KvDropIndex { .. }
            | Self::ConstraintChange { .. } => None,
        }
    }
}

impl ReplicatedWrite {
    /// The ordinal the leader's clock must pass before it reserves this
    /// write's run, `0` for none.
    ///
    /// A TTL reap ends the reaped version at the last ordinal of its expiry
    /// millisecond. The leader folds that ordinal first, so the write's own
    /// ordinal, and every later write's, lies above the version's end.
    pub fn ordinal_floor(&self) -> i64 {
        let Self::KvResolvedWrite { mutations, .. } = self else {
            return 0;
        };
        mutations
            .iter()
            .filter_map(|mutation| match mutation {
                super::KvResolvedMutationWire::Reap { expire_at_ms, .. } => {
                    Some(nodedb_types::ms_to_ordinal_upper(
                        i64::try_from(*expire_at_ms).unwrap_or(i64::MAX),
                    ))
                }
                super::KvResolvedMutationWire::Put { .. }
                | super::KvResolvedMutationWire::Rewrite { .. }
                | super::KvResolvedMutationWire::Delete { .. }
                | super::KvResolvedMutationWire::Expire { .. }
                | super::KvResolvedMutationWire::Persist { .. } => None,
            })
            .max()
            .unwrap_or(0)
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
    fn a_document_write_spans_one_and_a_barrier_none() {
        let put = ReplicatedWrite::PointPut {
            collection: "c".into(),
            document_id: "d".into(),
            value: Vec::new(),
            surrogate: 1,
            resolved_sum_targets: Vec::new(),
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

    /// A KV write and an array cell write span one ordinal: every replica
    /// stamps their versions at the entry's ordinal.
    #[test]
    fn a_kv_and_an_array_cell_write_span_one() {
        let delete = ReplicatedWrite::KvDelete {
            collection: "c".into(),
            keys: vec![b"k".to_vec()],
            returning: None,
            rls_filters: Vec::new(),
            provenance: None,
        };
        assert_eq!(delete.system_ordinal_span(), Some(1));
        let cell = ReplicatedWrite::ArrayCellDelete {
            array: "a".into(),
            coords_msgpack: Vec::new(),
            provenance: None,
            incarnation: nodedb_types::Hlc::default(),
        };
        assert_eq!(cell.system_ordinal_span(), Some(1));
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

    /// A reap's floor is the last ordinal of its expiry millisecond. Any
    /// other write carries none.
    #[test]
    fn a_reap_floors_the_leader_clock_at_its_expiry_instant() {
        let reap = ReplicatedWrite::KvResolvedWrite {
            mutations: vec![super::super::KvResolvedMutationWire::Reap {
                collection: "c".into(),
                key: b"k".to_vec(),
                expire_at_ms: 1_500,
            }],
            response_payload: Vec::new(),
            resolved_now_ms: 2_000,
        };
        assert_eq!(
            reap.ordinal_floor(),
            nodedb_types::ms_to_ordinal_upper(1_500)
        );
        let delete = ReplicatedWrite::KvDelete {
            collection: "c".into(),
            keys: vec![b"k".to_vec()],
            returning: None,
            rls_filters: Vec::new(),
            provenance: None,
        };
        assert_eq!(delete.ordinal_floor(), 0);
    }
}
