// SPDX-License-Identifier: BUSL-1.1

//! The identity a read observes, for the transaction read-set.
//!
//! [`ReadKey::Predicate`] validates the whole collection at commit. It is
//! the widest observation, so it is never unsound. A narrower key is chosen
//! only where the plan proves which rows it read. Every operation family is
//! matched variant by variant, so a new variant fails to compile until it is
//! decided.

use crate::bridge::envelope::PhysicalPlan;
use crate::control::server::shared::session::read_set::{ReadKey, ReadPoint};
use nodedb_physical::physical_plan::{
    ArrayOp, ClusterArrayOp, ClusterEventOp, ColumnarOp, CrdtOp, DocumentOp, GraphOp, KvOp, MetaOp,
    QueryOp, SpatialOp, TextOp, TimeseriesOp, VectorOp,
};

/// Classify a read plan's observed identity for the transaction read-set. A
/// `PointGet` miss degrades to [`ReadKey::Predicate`] (can't catch a phantom
/// INSERT); a KV miss keeps `Point` — its key IS the future write's key.
pub(crate) fn read_key_of(plan: &PhysicalPlan, found: bool) -> ReadKey {
    match plan {
        PhysicalPlan::Document(op) => document_read_key(op, found),
        PhysicalPlan::Kv(op) => kv_read_key(op),
        PhysicalPlan::Vector(op) => vector_read_key(op),
        PhysicalPlan::Graph(op) => graph_read_key(op),
        PhysicalPlan::Text(op) => text_read_key(op),
        PhysicalPlan::Columnar(op) => columnar_read_key(op),
        PhysicalPlan::Timeseries(op) => timeseries_read_key(op),
        PhysicalPlan::Spatial(op) => spatial_read_key(op),
        PhysicalPlan::Crdt(op) => crdt_read_key(op),
        PhysicalPlan::Query(op) => query_read_key(op),
        PhysicalPlan::Meta(op) => meta_read_key(op),
        PhysicalPlan::Array(op) => array_read_key(op),
        PhysicalPlan::ClusterArray(op) => cluster_array_read_key(op),
        PhysicalPlan::ClusterEvent(op) => cluster_event_read_key(op),
    }
}

fn document_read_key(op: &DocumentOp, found: bool) -> ReadKey {
    match op {
        DocumentOp::PointGet { surrogate, .. } => match surrogate {
            Some(surrogate) if found => ReadKey::Point {
                repr: ReadPoint::Surrogate(surrogate.as_u32()),
            },
            // A miss, or a read with no bound surrogate, observes the
            // predicate: a later insert of the key must still conflict.
            Some(_) | None => ReadKey::Predicate,
        },
        // Observation is confined to the indexed dimension; `filters` (the
        // residual compound predicate) is ignored — validating the indexed
        // dimension is sound.
        DocumentOp::IndexedFetch { path, value, .. }
        | DocumentOp::IndexLookup { path, value, .. } => ReadKey::IndexEq {
            field: path.clone(),
            value: value.clone(),
        },
        // Bound bytes interpreted as UTF-8, same as the scan. One-sided
        // ranges leave the absent bound `None`.
        DocumentOp::RangeScan {
            field,
            lower,
            upper,
            ..
        } => ReadKey::IndexRange {
            field: field.clone(),
            lo: lower
                .as_ref()
                .map(|b| String::from_utf8_lossy(b).into_owned()),
            hi: upper
                .as_ref()
                .map(|b| String::from_utf8_lossy(b).into_owned()),
        },
        DocumentOp::PointPut { .. }
        | DocumentOp::PointInsert { .. }
        | DocumentOp::PointDelete { .. }
        | DocumentOp::PointUpdate { .. }
        | DocumentOp::Scan { .. }
        | DocumentOp::BatchInsert { .. }
        | DocumentOp::Register { .. }
        | DocumentOp::DropIndex { .. }
        | DocumentOp::BackfillIndex { .. }
        | DocumentOp::Truncate { .. }
        | DocumentOp::EstimateCount { .. }
        | DocumentOp::InsertSelect { .. }
        | DocumentOp::Upsert { .. }
        | DocumentOp::BatchUpsert { .. }
        | DocumentOp::UpdateFromJoin { .. }
        | DocumentOp::BulkUpdate { .. }
        | DocumentOp::BulkDelete { .. }
        | DocumentOp::Merge { .. }
        | DocumentOp::MaterializeScan { .. }
        | DocumentOp::ApplyBalanceDelta { .. }
        | DocumentOp::ResolveWrite(_)
        | DocumentOp::ResolvedWrite { .. } => ReadKey::Predicate,
    }
}

fn kv_read_key(op: &KvOp) -> ReadKey {
    match op {
        KvOp::Get { key, .. } | KvOp::FieldGet { key, .. } => ReadKey::Point {
            repr: ReadPoint::KvKey(key.clone().into_boxed_slice()),
        },
        KvOp::Put { .. }
        | KvOp::Insert { .. }
        | KvOp::InsertIfAbsent { .. }
        | KvOp::InsertOnConflictUpdate { .. }
        | KvOp::Delete { .. }
        | KvOp::Scan { .. }
        | KvOp::Expire { .. }
        | KvOp::Persist { .. }
        | KvOp::GetTtl { .. }
        | KvOp::BatchGet { .. }
        | KvOp::BatchPut { .. }
        | KvOp::RegisterIndex { .. }
        | KvOp::DropIndex { .. }
        | KvOp::FieldSet { .. }
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
        | KvOp::ResolvedWrite { .. }
        | KvOp::PredicateUpdate { .. }
        | KvOp::PredicateDelete { .. } => ReadKey::Predicate,
    }
}

fn vector_read_key(op: &VectorOp) -> ReadKey {
    match op {
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
        | VectorOp::DirectUpsert { .. }
        | VectorOp::DirectInsert { .. }
        | VectorOp::DirectInsertIfAbsent { .. }
        | VectorOp::DirectBatchWrite { .. }
        | VectorOp::DirectDelete { .. }
        | VectorOp::DirectTruncate { .. }
        | VectorOp::DirectUpdate { .. }
        | VectorOp::ResolveDirectWrite(_)
        | VectorOp::ResolvedDirectWrite { .. } => ReadKey::Predicate,
    }
}

fn graph_read_key(op: &GraphOp) -> ReadKey {
    match op {
        GraphOp::EdgePut { .. }
        | GraphOp::EdgePutBatch { .. }
        | GraphOp::EdgeDelete { .. }
        | GraphOp::ResolveEdgeDelete(_)
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
        | GraphOp::BspSuperstep(_)
        | GraphOp::WccSuperstep(_)
        | GraphOp::SetNodeLabels { .. }
        | GraphOp::RemoveNodeLabels { .. }
        | GraphOp::TemporalNeighbors { .. }
        | GraphOp::Stats { .. }
        | GraphOp::NodeEdgeGuard { .. }
        | GraphOp::NodePresenceGuard { .. }
        | GraphOp::TruncateEdges { .. }
        | GraphOp::NodePresenceRead { .. } => ReadKey::Predicate,
    }
}

fn text_read_key(op: &TextOp) -> ReadKey {
    match op {
        TextOp::Search { .. }
        | TextOp::BM25ScoreScan { .. }
        | TextOp::PhraseSearch { .. }
        | TextOp::HybridSearch { .. }
        | TextOp::FtsIndexDoc { .. }
        | TextOp::FtsDeleteDoc { .. }
        | TextOp::HybridSearchTriple { .. }
        | TextOp::SetTextConfig { .. } => ReadKey::Predicate,
    }
}

fn columnar_read_key(op: &ColumnarOp) -> ReadKey {
    match op {
        ColumnarOp::Scan { .. }
        | ColumnarOp::Insert { .. }
        | ColumnarOp::Update { .. }
        | ColumnarOp::Delete { .. }
        | ColumnarOp::ResolvedUpdate { .. }
        | ColumnarOp::ResolvedDelete { .. }
        | ColumnarOp::ResolveDml { .. }
        | ColumnarOp::MaterializeScan { .. }
        | ColumnarOp::Truncate { .. } => ReadKey::Predicate,
    }
}

fn timeseries_read_key(op: &TimeseriesOp) -> ReadKey {
    match op {
        TimeseriesOp::Scan { .. }
        | TimeseriesOp::Ingest { .. }
        | TimeseriesOp::ResolveIngest(_)
        | TimeseriesOp::Truncate { .. }
        | TimeseriesOp::RefreshAggregate { .. }
        | TimeseriesOp::Partitions { .. }
        | TimeseriesOp::ReadAggregate { .. } => ReadKey::Predicate,
    }
}

fn spatial_read_key(op: &SpatialOp) -> ReadKey {
    match op {
        SpatialOp::Insert { .. } | SpatialOp::Delete { .. } | SpatialOp::Scan { .. } => {
            ReadKey::Predicate
        }
    }
}

fn crdt_read_key(op: &CrdtOp) -> ReadKey {
    match op {
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
        | CrdtOp::DocUpsert { .. }
        | CrdtOp::DocDelete { .. }
        | CrdtOp::PreviewApply { .. } => ReadKey::Predicate,
    }
}

fn query_read_key(op: &QueryOp) -> ReadKey {
    match op {
        QueryOp::Exchange(_)
        | QueryOp::ProviderScan { .. }
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
        | QueryOp::LateralLoop { .. } => ReadKey::Predicate,
    }
}

fn meta_read_key(op: &MetaOp) -> ReadKey {
    match op {
        MetaOp::WalAppend { .. }
        | MetaOp::Cancel { .. }
        | MetaOp::TransactionBatch { .. }
        | MetaOp::CreateSnapshot
        | MetaOp::Compact
        | MetaOp::Checkpoint
        | MetaOp::RegisterContinuousAggregate { .. }
        | MetaOp::UnregisterContinuousAggregate { .. }
        | MetaOp::ListContinuousAggregates
        | MetaOp::ContinuousAggregatesRegistered
        | MetaOp::AwaitContinuousAggregateRefresh { .. }
        | MetaOp::ConvertCollection { .. }
        | MetaOp::CreateTenantSnapshot { .. }
        | MetaOp::RestoreTenantSnapshot { .. }
        | MetaOp::PurgeDepartedVShards { .. }
        | MetaOp::PurgeTenant { .. }
        | MetaOp::UnregisterCollection { .. }
        | MetaOp::UnregisterMaterializedView { .. }
        | MetaOp::QueryCollectionSize { .. }
        | MetaOp::VerifyHashChain { .. }
        | MetaOp::EnforceTimeseriesRetention { .. }
        | MetaOp::TemporalPurgeEdgeStore { .. }
        | MetaOp::TemporalPurgeDocumentStrict { .. }
        | MetaOp::TemporalPurgeColumnar { .. }
        | MetaOp::TemporalPurgeCrdt { .. }
        | MetaOp::TemporalPurgeArray { .. }
        | MetaOp::AlterArray { .. }
        | MetaOp::ApplyContinuousAggRetention
        | MetaOp::QueryAggregateWatermark { .. }
        | MetaOp::QueryLastValues { .. }
        | MetaOp::QueryLastValue { .. }
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
        | MetaOp::ResolveTxn { .. }
        | MetaOp::CalvinResolve { .. }
        | MetaOp::ApplyTransactionRedo { .. }
        | MetaOp::RestoreRedo(_)
        | MetaOp::HomeVersions { .. } => ReadKey::Predicate,
    }
}

fn array_read_key(op: &ArrayOp) -> ReadKey {
    match op {
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
        | ArrayOp::PurgeArrayDrop { .. } => ReadKey::Predicate,
    }
}

fn cluster_array_read_key(op: &ClusterArrayOp) -> ReadKey {
    match op {
        ClusterArrayOp::Slice { .. }
        | ClusterArrayOp::Agg { .. }
        | ClusterArrayOp::Elementwise { .. }
        | ClusterArrayOp::Put { .. }
        | ClusterArrayOp::Delete { .. }
        | ClusterArrayOp::Flush { .. }
        | ClusterArrayOp::Compact { .. } => ReadKey::Predicate,
    }
}

fn cluster_event_read_key(op: &ClusterEventOp) -> ReadKey {
    match op {
        ClusterEventOp::ConsumeStream { .. }
        | ClusterEventOp::TenantWriteMarks { .. }
        | ClusterEventOp::SurrogateBinds { .. }
        | ClusterEventOp::MetadataApplied { .. }
        | ClusterEventOp::SurrogateHolders { .. } => ReadKey::Predicate,
    }
}
