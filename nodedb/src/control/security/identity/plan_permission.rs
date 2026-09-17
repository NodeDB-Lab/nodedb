// SPDX-License-Identifier: BUSL-1.1

//! `PhysicalPlan` → `Permission` mapping.
//!
//! The match must remain fully exhaustive. Adding a new `PhysicalPlan`
//! variant must produce a compile error here so the security tier is
//! intentionally decided rather than defaulted.

#![deny(clippy::wildcard_enum_match_arm)]

use super::permission::Permission;

/// Map a PhysicalPlan to the Permission required to execute it.
pub fn required_permission(plan: &crate::bridge::envelope::PhysicalPlan) -> Permission {
    use crate::bridge::envelope::PhysicalPlan;
    use nodedb_physical::physical_plan::{
        ArrayOp, ColumnarOp, CrdtOp, DocumentOp, GraphOp, KvOp, MetaOp, QueryOp, SpatialOp, TextOp,
        TimeseriesOp, VectorOp,
    };
    match plan {
        // Read operations.
        PhysicalPlan::Document(
            DocumentOp::PointGet { .. }
            | DocumentOp::RangeScan { .. }
            | DocumentOp::Scan { .. }
            | DocumentOp::IndexLookup { .. }
            | DocumentOp::IndexedFetch { .. }
            | DocumentOp::EstimateCount { .. }
            | DocumentOp::MaterializeScan { .. }
            // Read-only: reports what the wrapped write would apply; that write is authorized separately.
            | DocumentOp::ResolveWrite(_),
        ) => Permission::Read,

        PhysicalPlan::Vector(
            VectorOp::Search { .. }
            | VectorOp::MultiSearch { .. }
            | VectorOp::QueryStats { .. }
            | VectorOp::SparseSearch { .. }
            | VectorOp::MultiVectorScoreSearch { .. },
        ) => Permission::Read,

        PhysicalPlan::Crdt(
            CrdtOp::Read { .. }
            | CrdtOp::PreviewApply { .. }
            | CrdtOp::ReadAtVersion { .. }
            | CrdtOp::GetVersionVector { .. }
            | CrdtOp::ReadConstraints { .. }
            | CrdtOp::ExportDelta { .. },
        ) => Permission::Read,

        PhysicalPlan::Graph(
            // Read-only: decides what a governed delete would do; that delete is authorized separately.
            GraphOp::ResolveEdgeDelete(_)
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
            | GraphOp::TemporalNeighbors { .. }
            | GraphOp::TemporalAlgorithm { .. }
            | GraphOp::BspSuperstep(_)
            | GraphOp::WccSuperstep(_)
            | GraphOp::Stats { .. },
        ) => Permission::Read,

        PhysicalPlan::Query(
            QueryOp::Aggregate { .. }
            | QueryOp::HashJoin { .. }
            | QueryOp::PartialAggregate { .. }
            | QueryOp::PartialAggregateState { .. }
            | QueryOp::NestedLoopJoin { .. }
            | QueryOp::SortMergeJoin { .. }
            | QueryOp::RecursiveScan { .. }
            | QueryOp::RecursiveValue { .. }
            | QueryOp::FacetCounts { .. }
            | QueryOp::LateralTopK { .. }
            | QueryOp::LateralLoop { .. }
            | QueryOp::ShuffleJoinConsume { .. }
            | QueryOp::ShuffleAggregateConsume { .. }
            | QueryOp::ProviderScan { .. },
        ) => Permission::Read,

        // An Exchange node's required permission is its child's permission;
        // it only redistributes rows produced by the wrapped plan.
        PhysicalPlan::Query(QueryOp::Exchange(op)) => required_permission(&op.child),

        // PostProcess only reshapes child rows; permission is the child's — recurse, don't assume Read.
        PhysicalPlan::Query(QueryOp::PostProcess { input, .. }) => required_permission(input),

        // SetOp merges N child relations; it requires the strictest permission
        // any branch requires. An empty input list is unreachable from the
        // converter and maps to Read, the weakest tier.
        PhysicalPlan::Query(QueryOp::SetOp { inputs, .. }) => inputs
            .iter()
            .map(required_permission)
            .fold(Permission::Read, strictest),

        PhysicalPlan::Text(
            TextOp::Search { .. }
            | TextOp::BM25ScoreScan { .. }
            | TextOp::HybridSearch { .. }
            | TextOp::HybridSearchTriple { .. }
            | TextOp::PhraseSearch { .. },
        ) => Permission::Read,

        PhysicalPlan::Text(TextOp::FtsIndexDoc { .. } | TextOp::FtsDeleteDoc { .. }) => {
            Permission::Write
        }

        PhysicalPlan::Text(TextOp::SetTextConfig { .. }) => Permission::Alter,

        PhysicalPlan::Spatial(SpatialOp::Scan { .. }) => Permission::Read,
        PhysicalPlan::Spatial(SpatialOp::Insert { .. } | SpatialOp::Delete { .. }) => {
            Permission::Write
        }

        PhysicalPlan::Columnar(ColumnarOp::Scan { .. } | ColumnarOp::MaterializeScan { .. }) => {
            Permission::Read
        }

        // Read-only: reports what a governed ingest would store; that ingest is authorized separately.
        PhysicalPlan::Timeseries(TimeseriesOp::Scan { .. } | TimeseriesOp::ResolveIngest(_)) => {
            Permission::Read
        }

        // Write operations.
        PhysicalPlan::Crdt(
            CrdtOp::Apply { .. }
            | CrdtOp::ApplyAuthenticated { .. }
            | CrdtOp::ImportSnapshot { .. }
            | CrdtOp::SetConstraints { .. }
            | CrdtOp::DropConstraints { .. }
            | CrdtOp::RestoreToVersion { .. }
            | CrdtOp::ListInsert { .. }
            | CrdtOp::ListDelete { .. }
            | CrdtOp::ListMove { .. }
            | CrdtOp::DocUpsert { .. }
            | CrdtOp::DocDelete { .. },
        ) => Permission::Write,

        PhysicalPlan::Vector(
            VectorOp::Insert { .. }
            | VectorOp::BatchInsert { .. }
            | VectorOp::Delete { .. }
            | VectorOp::DeleteBySurrogate { .. }
            | VectorOp::SparseInsert { .. }
            | VectorOp::SparseDelete { .. }
            | VectorOp::MultiVectorInsert { .. }
            | VectorOp::MultiVectorDelete { .. }
            | VectorOp::DirectUpsert { .. },
        ) => Permission::Write,

        PhysicalPlan::Document(
            DocumentOp::BatchInsert { .. }
            | DocumentOp::PointPut { .. }
            | DocumentOp::PointInsert { .. }
            | DocumentOp::PointDelete { .. }
            | DocumentOp::PointUpdate { .. }
            | DocumentOp::BulkUpdate { .. }
            | DocumentOp::BulkDelete { .. }
            | DocumentOp::UpdateFromJoin { .. }
            | DocumentOp::Upsert { .. }
            | DocumentOp::InsertSelect { .. }
            | DocumentOp::Truncate { .. }
            | DocumentOp::Merge { .. }
            // Never client-issued: planner appends it after the causing statement is authorized.
            | DocumentOp::ApplyBalanceDelta { .. }
            // Never client-issued: write-resolve orchestrator builds it post-authorization.
            | DocumentOp::ResolvedWrite { .. },
        ) => Permission::Write,

        PhysicalPlan::Graph(
            GraphOp::EdgePut { .. }
            | GraphOp::EdgePutBatch { .. }
            | GraphOp::EdgeDelete { .. }
            | GraphOp::EdgeDeleteBatch { .. }
            | GraphOp::SetNodeLabels { .. }
            | GraphOp::RemoveNodeLabels { .. },
        ) => Permission::Write,

        PhysicalPlan::Meta(MetaOp::WalAppend { .. }) => Permission::Write,

        PhysicalPlan::Columnar(
            ColumnarOp::Insert { .. }
            | ColumnarOp::Update { .. }
            | ColumnarOp::Delete { .. }
            | ColumnarOp::ResolvedUpdate { .. }
            | ColumnarOp::ResolvedDelete { .. }
            // Requires the same Write the predicate it resolves requires, though it isn't
            // itself write-class for admission/replication (see `write_class::columnar_is_write`).
            | ColumnarOp::ResolveDml { .. },
        ) => Permission::Write,

        PhysicalPlan::Timeseries(TimeseriesOp::Ingest { .. }) => Permission::Write,

        // Transaction batch: requires write (contains writes).
        PhysicalPlan::Meta(MetaOp::TransactionBatch { .. }) => Permission::Write,

        // DDL / schema changes.
        PhysicalPlan::Document(
            DocumentOp::Register { .. }
            | DocumentOp::DropIndex { .. }
            | DocumentOp::BackfillIndex { .. },
        ) => Permission::Alter,

        PhysicalPlan::Vector(VectorOp::DropIndex { .. }) => Permission::Alter,

        PhysicalPlan::Crdt(CrdtOp::SetPolicy { .. } | CrdtOp::CompactAtVersion { .. }) => {
            Permission::Alter
        }

        PhysicalPlan::Crdt(CrdtOp::GetPolicy { .. }) => Permission::Read,

        PhysicalPlan::Meta(
            MetaOp::RegisterContinuousAggregate { .. }
            | MetaOp::UnregisterContinuousAggregate { .. }
            | MetaOp::ListContinuousAggregates
            | MetaOp::ConvertCollection { .. },
        ) => Permission::Alter,

        PhysicalPlan::Vector(
            VectorOp::SetParams { .. }
            | VectorOp::Seal { .. }
            | VectorOp::CompactIndex { .. }
            | VectorOp::Rebuild { .. },
        ) => Permission::Alter,

        // Control operations.
        PhysicalPlan::Meta(MetaOp::Cancel { .. }) => Permission::Admin,

        // System-level operations: require admin.
        PhysicalPlan::Meta(
            MetaOp::CreateSnapshot
            | MetaOp::Compact
            | MetaOp::Checkpoint
            | MetaOp::CreateTenantSnapshot { .. }
            | MetaOp::RestoreTenantSnapshot { .. }
            | MetaOp::UnregisterCollection { .. }
            | MetaOp::UnregisterMaterializedView { .. }
            | MetaOp::QueryCollectionSize { .. }
            | MetaOp::AlterArray { .. }
            | MetaOp::RebuildIndex { .. }
            | MetaOp::RenameCollection { .. }
            | MetaOp::DropTxnOverlay { .. },
        ) => Permission::Admin,

        // Staging a write, savepoint mark/rollback, and COMMIT redo resolution all mutate the overlay.
        PhysicalPlan::Meta(
            MetaOp::StageWrite { .. }
            | MetaOp::MarkSavepoint { .. }
            | MetaOp::RollbackToSavepoint { .. }
            | MetaOp::ResolveTxn { .. },
        ) => Permission::Write,

        // Mirrors `ResolveTxn`: Calvin scheduler's commit path, treated as Write though it doesn't mutate base state.
        PhysicalPlan::Meta(MetaOp::CalvinResolve { .. }) => Permission::Write,

        // KV engine: read operations.
        PhysicalPlan::Kv(
            KvOp::Get { .. }
            | KvOp::GetTtl { .. }
            | KvOp::Scan { .. }
            | KvOp::MaterializeScan { .. }
            | KvOp::BatchGet { .. }
            | KvOp::FieldGet { .. }
            | KvOp::SortedIndexRank { .. }
            | KvOp::SortedIndexTopK { .. }
            | KvOp::SortedIndexRange { .. }
            | KvOp::SortedIndexCount { .. }
            | KvOp::SortedIndexScore { .. }
            // Read-only: reports what a governed write would apply; that write is authorized separately.
            | KvOp::ResolveWrite(_),
        ) => Permission::Read,

        // KV engine: write operations.
        PhysicalPlan::Kv(
            KvOp::Put { .. }
            | KvOp::Insert { .. }
            | KvOp::InsertIfAbsent { .. }
            | KvOp::InsertOnConflictUpdate { .. }
            | KvOp::Delete { .. }
            | KvOp::Expire { .. }
            | KvOp::Persist { .. }
            | KvOp::BatchPut { .. }
            | KvOp::RegisterIndex { .. }
            | KvOp::DropIndex { .. }
            | KvOp::FieldSet { .. }
            | KvOp::Truncate { .. }
            | KvOp::Incr { .. }
            | KvOp::IncrFloat { .. }
            | KvOp::Cas { .. }
            | KvOp::GetSet { .. }
            | KvOp::RegisterSortedIndex { .. }
            | KvOp::DropSortedIndex { .. }
            | KvOp::Transfer { .. }
            | KvOp::TransferItem { .. }
            | KvOp::ResolvedWrite { .. }
            | KvOp::PredicateUpdate { .. }
            | KvOp::PredicateDelete { .. },
        ) => Permission::Write,

        // Tenant purge requires superuser (checked at DDL level); map to Write.
        PhysicalPlan::Meta(MetaOp::PurgeTenant { .. }) => Permission::Write,

        // Retention enforcement is admin-level (invoked by background tasks).
        PhysicalPlan::Meta(
            MetaOp::EnforceTimeseriesRetention { .. }
            | MetaOp::ApplyContinuousAggRetention
            | MetaOp::TemporalPurgeEdgeStore { .. }
            | MetaOp::TemporalPurgeDocumentStrict { .. }
            | MetaOp::TemporalPurgeColumnar { .. }
            | MetaOp::TemporalPurgeCrdt { .. }
            | MetaOp::TemporalPurgeArray { .. },
        ) => Permission::Admin,

        // Watermark query is admin-level (invoked by enforcement loop).
        PhysicalPlan::Meta(MetaOp::QueryAggregateWatermark { .. }) => Permission::Admin,

        // Last-value cache queries are read operations.
        PhysicalPlan::Meta(MetaOp::QueryLastValues { .. } | MetaOp::QueryLastValue { .. }) => {
            Permission::Read
        }

        // Array engine: query ops are reads, put/delete are writes, OpenArray is DDL, flush/compact are admin.
        PhysicalPlan::Array(
            ArrayOp::Slice { .. }
            | ArrayOp::SurrogateBitmapScan { .. }
            | ArrayOp::Project { .. }
            | ArrayOp::Aggregate { .. }
            | ArrayOp::Elementwise { .. },
        ) => Permission::Read,
        PhysicalPlan::Array(ArrayOp::Put { .. } | ArrayOp::Delete { .. }) => Permission::Write,
        PhysicalPlan::Array(ArrayOp::OpenArray { .. }) => Permission::Alter,
        PhysicalPlan::Array(
            ArrayOp::Flush { .. }
            | ArrayOp::Compact { .. }
            | ArrayOp::DropArray { .. }
            | ArrayOp::RestoreArrayDrop { .. }
            | ArrayOp::PurgeArrayDrop { .. },
        ) => Permission::Admin,

        // ClusterArray mirrors the local ArrayOp permission model.
        PhysicalPlan::ClusterArray(
            nodedb_physical::physical_plan::ClusterArrayOp::Slice { .. }
            | nodedb_physical::physical_plan::ClusterArrayOp::Agg { .. },
        ) => Permission::Read,
        PhysicalPlan::ClusterArray(
            nodedb_physical::physical_plan::ClusterArrayOp::Put { .. }
            | nodedb_physical::physical_plan::ClusterArrayOp::Delete { .. },
        ) => Permission::Write,
        PhysicalPlan::ClusterEvent(_) => Permission::Read,

        // Calvin cross-shard execution batches: dispatched internally, treated as Write.
        PhysicalPlan::Meta(
            MetaOp::CalvinExecuteStatic { .. }
            | MetaOp::CalvinExecutePassive { .. }
            | MetaOp::CalvinExecuteActive { .. }
            | MetaOp::RecordCalvinWriteVersions { .. }
            | MetaOp::CalvinFlush { .. }
            | MetaOp::CalvinDrop { .. },
        ) => Permission::Write,

        // Synonym group DDL: Alter permission (same tier as CREATE/DROP other DDL objects).
        PhysicalPlan::Meta(MetaOp::PutSynonymGroup { .. } | MetaOp::DeleteSynonymGroup { .. }) => {
            Permission::Alter
        }
    }
}

/// The stricter of two permissions under the tier order used to fold a
/// multi-input node. Exhaustive so a new `Permission` variant forces a
/// placement here.
fn strictest(a: Permission, b: Permission) -> Permission {
    if strictness_rank(b) > strictness_rank(a) {
        b
    } else {
        a
    }
}

/// Tier order from weakest to strictest. Read-class tiers come first, then
/// write, then schema, then cluster-wide control.
fn strictness_rank(permission: Permission) -> u8 {
    match permission {
        Permission::Read => 0,
        Permission::Monitor => 1,
        Permission::Execute => 2,
        Permission::Write => 3,
        Permission::Create => 4,
        Permission::Drop => 5,
        Permission::Alter => 6,
        Permission::Backup => 7,
        Permission::Admin => 8,
    }
}
