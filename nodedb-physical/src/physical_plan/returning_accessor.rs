// SPDX-License-Identifier: Apache-2.0

//! The `RETURNING` slot of a write plan, with the collection it writes.
//!
//! Every write op that can answer rows owns one `returning` slot. The
//! accessors here read it without the caller matching each engine. One
//! exhaustive match lists every op, so a new op variant fails to compile
//! until it is placed with the slotted ops or the slotless ones.

use nodedb_types::{QualifiedCollection, ReturningSpec};

use super::{
    ArrayOp, ClusterArrayOp, ClusterEventOp, ColumnarOp, CrdtOp, DocumentOp, GraphOp, KvOp, MetaOp,
    PhysicalPlan, QueryOp, SpatialOp, TextOp, TimeseriesOp, VectorOp,
};

/// The one exhaustive match over every plan variant. It binds the target
/// collection to `$c` and the slot to `$r`, and yields `Some($out)` for a
/// slotted op and `None` otherwise. Binding modes follow the scrutinee, so
/// the same match serves a shared and a mutable borrow.
macro_rules! match_returning_slot {
    ($plan:expr, $c:ident, $r:ident => $out:expr) => {
        match $plan {
            PhysicalPlan::Document(
                DocumentOp::PointInsert { $c, $r, .. }
                | DocumentOp::PointPut { $c, $r, .. }
                | DocumentOp::BatchInsert { $c, $r, .. }
                | DocumentOp::Upsert { $c, $r, .. }
                | DocumentOp::BatchUpsert { $c, $r, .. }
                | DocumentOp::PointUpdate { $c, $r, .. }
                | DocumentOp::BulkUpdate { $c, $r, .. }
                | DocumentOp::PointDelete { $c, $r, .. }
                | DocumentOp::BulkDelete { $c, $r, .. }
                | DocumentOp::UpdateFromJoin {
                    target_collection: $c,
                    $r,
                    ..
                }
                | DocumentOp::Merge {
                    target_collection: $c,
                    $r,
                    ..
                }
                | DocumentOp::InsertSelect {
                    target_collection: $c,
                    $r,
                    ..
                },
            )
            | PhysicalPlan::Kv(
                KvOp::Insert { $c, $r, .. }
                | KvOp::InsertIfAbsent { $c, $r, .. }
                | KvOp::InsertOnConflictUpdate { $c, $r, .. }
                | KvOp::Put { $c, $r, .. }
                | KvOp::BatchPut { $c, $r, .. }
                | KvOp::FieldSet { $c, $r, .. }
                | KvOp::PredicateUpdate { $c, $r, .. }
                | KvOp::Delete { $c, $r, .. }
                | KvOp::PredicateDelete { $c, $r, .. },
            )
            | PhysicalPlan::Columnar(ColumnarOp::Insert { $c, $r, .. })
            | PhysicalPlan::Timeseries(TimeseriesOp::Ingest { $c, $r, .. })
            | PhysicalPlan::Vector(
                VectorOp::DirectUpsert { $c, $r, .. }
                | VectorOp::DirectInsert { $c, $r, .. }
                | VectorOp::DirectInsertIfAbsent { $c, $r, .. }
                | VectorOp::DirectBatchWrite { $c, $r, .. }
                | VectorOp::DirectDelete { $c, $r, .. }
                | VectorOp::DirectUpdate { $c, $r, .. },
            )
            | PhysicalPlan::Crdt(
                CrdtOp::DocUpsert { $c, $r, .. } | CrdtOp::DocDelete { $c, $r, .. },
            ) => Some($out),
            // Every variant with no `returning` slot, listed so a new variant
            // forces a decision here instead of inheriting silence.
            PhysicalPlan::Document(
                DocumentOp::PointGet { .. }
                | DocumentOp::Scan { .. }
                | DocumentOp::RangeScan { .. }
                | DocumentOp::Register { .. }
                | DocumentOp::IndexLookup { .. }
                | DocumentOp::IndexedFetch { .. }
                | DocumentOp::DropIndex { .. }
                | DocumentOp::BackfillIndex { .. }
                | DocumentOp::Truncate { .. }
                | DocumentOp::EstimateCount { .. }
                | DocumentOp::MaterializeScan { .. }
                | DocumentOp::ApplyBalanceDelta { .. }
                | DocumentOp::ResolveWrite(_)
                | DocumentOp::ResolvedWrite { .. },
            )
            | PhysicalPlan::Kv(
                KvOp::Get { .. }
                | KvOp::Scan { .. }
                | KvOp::Expire { .. }
                | KvOp::Persist { .. }
                | KvOp::GetTtl { .. }
                | KvOp::BatchGet { .. }
                | KvOp::RegisterIndex { .. }
                | KvOp::DropIndex { .. }
                | KvOp::FieldGet { .. }
                | KvOp::Truncate { .. }
                | KvOp::Incr { .. }
                | KvOp::IncrFloat { .. }
                | KvOp::Cas { .. }
                | KvOp::GetSet { .. }
                | KvOp::Transfer { .. }
                | KvOp::TransferItem { .. }
                | KvOp::RegisterSortedIndex { .. }
                | KvOp::DropSortedIndex { .. }
                | KvOp::SortedIndexRank { .. }
                | KvOp::SortedIndexTopK { .. }
                | KvOp::SortedIndexRange { .. }
                | KvOp::SortedIndexCount { .. }
                | KvOp::SortedIndexScore { .. }
                | KvOp::SortedIndexTxnRead { .. }
                | KvOp::MaterializeScan { .. }
                | KvOp::ResolveWrite(_)
                | KvOp::ResolvedWrite { .. },
            )
            | PhysicalPlan::Vector(
                VectorOp::Search { .. }
                | VectorOp::Insert { .. }
                | VectorOp::BatchInsert { .. }
                | VectorOp::MultiSearch { .. }
                | VectorOp::Delete { .. }
                | VectorOp::DeleteBySurrogate { .. }
                | VectorOp::SetParams { .. }
                | VectorOp::DropIndex { .. }
                | VectorOp::QueryStats { .. }
                | VectorOp::Seal { .. }
                | VectorOp::CompactIndex { .. }
                | VectorOp::Rebuild { .. }
                | VectorOp::SparseInsert { .. }
                | VectorOp::SparseSearch { .. }
                | VectorOp::SparseDelete { .. }
                | VectorOp::MultiVectorInsert { .. }
                | VectorOp::MultiVectorDelete { .. }
                | VectorOp::MultiVectorScoreSearch { .. }
                | VectorOp::DirectTruncate { .. }
                | VectorOp::ResolveDirectWrite(_)
                | VectorOp::ResolvedDirectWrite { .. },
            )
            | PhysicalPlan::Graph(
                GraphOp::EdgePut { .. }
                | GraphOp::EdgePutBatch { .. }
                | GraphOp::EdgeDelete { .. }
                | GraphOp::EdgeDeleteBatch { .. }
                | GraphOp::Hop { .. }
                | GraphOp::Neighbors { .. }
                | GraphOp::NeighborsMulti { .. }
                | GraphOp::Path { .. }
                | GraphOp::Subgraph { .. }
                | GraphOp::RagFusion { .. }
                | GraphOp::Algo { .. }
                | GraphOp::Match { .. }
                | GraphOp::MatchContinuation { .. }
                | GraphOp::MatchVarLenResume { .. }
                | GraphOp::SetNodeLabels { .. }
                | GraphOp::RemoveNodeLabels { .. }
                | GraphOp::TemporalNeighbors { .. }
                | GraphOp::Stats { .. }
                | GraphOp::ResolveEdgeDelete(_)
                | GraphOp::BspSuperstep(_)
                | GraphOp::WccSuperstep(_)
                | GraphOp::NodeEdgeGuard { .. }
                | GraphOp::NodePresenceGuard { .. }
                | GraphOp::TruncateEdges { .. }
                | GraphOp::NodePresenceRead { .. },
            )
            | PhysicalPlan::Text(
                TextOp::Search { .. }
                | TextOp::BM25ScoreScan { .. }
                | TextOp::PhraseSearch { .. }
                | TextOp::HybridSearch { .. }
                | TextOp::FtsIndexDoc { .. }
                | TextOp::FtsDeleteDoc { .. }
                | TextOp::HybridSearchTriple { .. }
                | TextOp::SetTextConfig { .. },
            )
            | PhysicalPlan::Columnar(
                ColumnarOp::Scan { .. }
                | ColumnarOp::Update { .. }
                | ColumnarOp::Delete { .. }
                | ColumnarOp::ResolvedUpdate { .. }
                | ColumnarOp::ResolvedDelete { .. }
                | ColumnarOp::ResolveDml { .. }
                | ColumnarOp::MaterializeScan { .. }
                | ColumnarOp::Truncate { .. },
            )
            | PhysicalPlan::Timeseries(
                TimeseriesOp::Scan { .. }
                | TimeseriesOp::ResolveIngest(_)
                | TimeseriesOp::Truncate { .. }
                | TimeseriesOp::RefreshAggregate { .. }
                | TimeseriesOp::Partitions { .. }
                | TimeseriesOp::ReadAggregate { .. },
            )
            | PhysicalPlan::Spatial(
                SpatialOp::Insert { .. } | SpatialOp::Delete { .. } | SpatialOp::Scan { .. },
            )
            | PhysicalPlan::Crdt(
                CrdtOp::Read { .. }
                | CrdtOp::Apply { .. }
                | CrdtOp::ApplyAuthenticated { .. }
                | CrdtOp::ImportSnapshot { .. }
                | CrdtOp::SetConstraints { .. }
                | CrdtOp::DropConstraints { .. }
                | CrdtOp::ReadConstraints { .. }
                | CrdtOp::SetPolicy { .. }
                | CrdtOp::GetPolicy { .. }
                | CrdtOp::ReadAtVersion { .. }
                | CrdtOp::GetVersionVector { .. }
                | CrdtOp::ExportDelta { .. }
                | CrdtOp::RestoreToVersion { .. }
                | CrdtOp::CompactAtVersion { .. }
                | CrdtOp::ListInsert { .. }
                | CrdtOp::ListDelete { .. }
                | CrdtOp::ListMove { .. }
                | CrdtOp::PreviewApply { .. },
            )
            | PhysicalPlan::Query(
                QueryOp::ProviderScan { .. }
                | QueryOp::PostProcess { .. }
                | QueryOp::SetOp { .. }
                | QueryOp::Aggregate { .. }
                | QueryOp::PartialAggregate { .. }
                | QueryOp::PartialAggregateState { .. }
                | QueryOp::HashJoin { .. }
                | QueryOp::ShuffleJoinConsume { .. }
                | QueryOp::ShuffleAggregateConsume { .. }
                | QueryOp::NestedLoopJoin { .. }
                | QueryOp::SortMergeJoin { .. }
                | QueryOp::FacetCounts { .. }
                | QueryOp::RecursiveScan { .. }
                | QueryOp::RecursiveValue { .. }
                | QueryOp::LateralTopK { .. }
                | QueryOp::LateralLoop { .. }
                | QueryOp::Exchange(_),
            )
            | PhysicalPlan::Meta(
                MetaOp::WalAppend { .. }
                | MetaOp::Cancel { .. }
                | MetaOp::TransactionBatch { .. }
                | MetaOp::CreateSnapshot
                | MetaOp::Compact
                | MetaOp::Checkpoint
                | MetaOp::RegisterContinuousAggregate { .. }
                | MetaOp::InstallAggregateImage { .. }
                | MetaOp::UnregisterContinuousAggregate { .. }
                | MetaOp::ListContinuousAggregates
                | MetaOp::ContinuousAggregatesRegistered
                | MetaOp::AwaitContinuousAggregateRefresh { .. }
                | MetaOp::ConvertCollection { .. }
                | MetaOp::CreateTenantSnapshot { .. }
                | MetaOp::RestoreTenantSnapshot { .. }
                | MetaOp::PurgeDepartedVShards { .. }
                | MetaOp::PurgeVersionsBelow { .. }
                | MetaOp::KvDueExpiries { .. }
                | MetaOp::ListTenantCollections { .. }
                | MetaOp::RefreshGraphCount { .. }
                | MetaOp::QueryStorageBytes { .. }
                | MetaOp::PurgeTenant { .. }
                | MetaOp::UnregisterCollection { .. }
                | MetaOp::UnregisterMaterializedView { .. }
                | MetaOp::QueryCollectionSize { .. }
                | MetaOp::EnforceTimeseriesRetention { .. }
                | MetaOp::TemporalPurgeEdgeStore { .. }
                | MetaOp::TemporalPurgeDocumentStrict { .. }
                | MetaOp::TemporalPurgeColumnar { .. }
                | MetaOp::TemporalPurgeCrdt { .. }
                | MetaOp::TemporalPurgeArray { .. }
                | MetaOp::AlterArray { .. }
                | MetaOp::ApplyContinuousAggRetention { .. }
                | MetaOp::QueryAggregateWatermark { .. }
                | MetaOp::QueryLastValues { .. }
                | MetaOp::QueryLastValue { .. }
                | MetaOp::VerifyHashChain { .. }
                | MetaOp::CalvinExecuteStatic { .. }
                | MetaOp::CalvinExecutePassive { .. }
                | MetaOp::CalvinExecuteActive { .. }
                | MetaOp::RebuildIndex { .. }
                | MetaOp::PutSynonymGroup { .. }
                | MetaOp::DeleteSynonymGroup { .. }
                | MetaOp::SetCustomCrsSet { .. }
                | MetaOp::StageWrite { .. }
                | MetaOp::DropTxnOverlay { .. }
                | MetaOp::MarkSavepoint { .. }
                | MetaOp::RollbackToSavepoint { .. }
                | MetaOp::CalvinDrop { .. }
                | MetaOp::ResolveTxnPart { .. }
                | MetaOp::CalvinResolve { .. }
                | MetaOp::ApplyTransactionRedo { .. }
                | MetaOp::RestoreRedo(_),
            )
            | PhysicalPlan::Array(
                ArrayOp::OpenArray { .. }
                | ArrayOp::Put { .. }
                | ArrayOp::Delete { .. }
                | ArrayOp::Slice { .. }
                | ArrayOp::Aggregate { .. }
                | ArrayOp::Elementwise { .. }
                | ArrayOp::Flush { .. }
                | ArrayOp::Compact { .. }
                | ArrayOp::SurrogateBitmapScan { .. }
                | ArrayOp::DropArray { .. }
                | ArrayOp::RekeyArray { .. }
                | ArrayOp::PurgeArrayDrop { .. },
            )
            | PhysicalPlan::ClusterArray(
                ClusterArrayOp::Slice { .. }
                | ClusterArrayOp::Agg { .. }
                | ClusterArrayOp::Elementwise { .. }
                | ClusterArrayOp::Put { .. }
                | ClusterArrayOp::Delete { .. }
                | ClusterArrayOp::Flush { .. }
                | ClusterArrayOp::Compact { .. },
            )
            | PhysicalPlan::ClusterEvent(
                ClusterEventOp::ConsumeStream { .. }
                | ClusterEventOp::TenantWriteMarks { .. }
                | ClusterEventOp::SurrogateBinds { .. }
                | ClusterEventOp::MetadataApplied { .. }
                | ClusterEventOp::SurrogateHolders { .. },
            ) => None,
        }
    };
}

impl PhysicalPlan {
    /// The collection this write plan targets and its `returning` slot.
    /// `None` for a plan whose op has no slot. The slot itself is `None`
    /// until a `RETURNING` clause fills it.
    pub fn returning_slot(&self) -> Option<(&QualifiedCollection, &Option<ReturningSpec>)> {
        match_returning_slot!(self, collection, returning => (collection, returning))
    }

    /// The `returning` slot of this write plan, for filling it. `None` for a
    /// plan whose op has no slot.
    pub fn returning_slot_mut(&mut self) -> Option<&mut Option<ReturningSpec>> {
        match_returning_slot!(self, collection, returning => {
            let _ = collection;
            returning
        })
    }

    /// The collection this write plan targets and the `RETURNING` spec it
    /// carries. `None` for a plan with no `returning` slot, or a slot left
    /// empty.
    pub fn returning_target(&self) -> Option<(&QualifiedCollection, &ReturningSpec)> {
        let (collection, returning) = self.returning_slot()?;
        returning.as_ref().map(|spec| (collection, spec))
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::{DatabaseId, ReturningColumns};

    use super::*;

    fn insert(returning: Option<ReturningSpec>) -> PhysicalPlan {
        PhysicalPlan::Kv(KvOp::Insert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "accounts"),
            key: b"k".to_vec(),
            value: Vec::new(),
            ttl_ms: 0,
            surrogate: nodedb_types::Surrogate::new(1),
            returning,
            rls_filters: Vec::new(),
        })
    }

    #[test]
    fn a_filled_slot_names_its_collection_and_spec() {
        let spec = ReturningSpec {
            columns: ReturningColumns::Star,
            precommit: None,
        };
        let plan = insert(Some(spec.clone()));
        let (collection, found) = plan.returning_target().expect("slot is filled");
        assert_eq!(collection.as_str(), "accounts");
        assert_eq!(found, &spec);
    }

    #[test]
    fn an_empty_slot_names_nothing() {
        assert!(insert(None).returning_target().is_none());
    }
}
