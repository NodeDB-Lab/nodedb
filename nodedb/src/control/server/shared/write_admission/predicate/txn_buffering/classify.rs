// SPDX-License-Identifier: BUSL-1.1

//! The classification table itself: one exhaustive arm per physical-plan
//! variant. The contract it implements — which variants deliberately diverge
//! from the `to_replicated_entry` oracle, and why — is documented on
//! [`super`].

use crate::bridge::envelope::PhysicalPlan;
use nodedb_physical::physical_plan::{
    ArrayOp, ClusterArrayOp, ColumnarOp, CrdtOp, DocumentOp, GraphOp, KvOp, MetaOp, QueryOp,
    SpatialOp, TextOp, TimeseriesOp, VectorOp,
};

/// Whether an in-transaction statement's plan must be buffered for COMMIT-time
/// replay (`true`) or may execute as a read (`false`). Mirrors
/// `to_replicated_entry(..).is_some()`, except the flipped variants below.
pub fn plan_requires_txn_buffering(plan: &PhysicalPlan) -> bool {
    match plan {
        // ---- Document: encoded (buffered) ----
        PhysicalPlan::Document(
            DocumentOp::PointPut { .. }
            | DocumentOp::PointInsert { .. }
            | DocumentOp::PointDelete { .. }
            | DocumentOp::PointUpdate { .. }
            | DocumentOp::Upsert { .. }
            | DocumentOp::InsertSelect { .. },
        ) => true,

        // Encoded only when there's no predicted surrogate/edge set to verify against;
        // when either is `Some`, it routes via Calvin and classifies as a read.
        PhysicalPlan::Document(DocumentOp::BulkDelete {
            ollp_predicted_surrogates: None,
            ollp_predicted_edges: None,
            ..
        }) => true,
        PhysicalPlan::Document(DocumentOp::BulkDelete { .. }) => false,
        PhysicalPlan::Document(DocumentOp::BulkUpdate {
            ollp_predicted_surrogates: None,
            ollp_predicted_edges: None,
            ..
        }) => true,
        PhysicalPlan::Document(DocumentOp::BulkUpdate { .. }) => false,

        // ---- Document: reads / DDL, not encoded ----
        PhysicalPlan::Document(
            DocumentOp::PointGet { .. }
            | DocumentOp::Scan { .. }
            | DocumentOp::RangeScan { .. }
            | DocumentOp::Register { .. }
            | DocumentOp::IndexLookup { .. }
            | DocumentOp::IndexedFetch { .. }
            | DocumentOp::DropIndex { .. }
            | DocumentOp::BackfillIndex { .. }
            | DocumentOp::EstimateCount { .. }
            | DocumentOp::MaterializeScan { .. }
            // Read-only classification pass issued by the Control-Plane expander itself.
            | DocumentOp::ResolveWrite(_)
            // Appended by the planner after statement admission as its own task.
            | DocumentOp::ApplyBalanceDelta { .. }
            // Built by write-resolve on the autocommit path, proposed straight through Raft.
            | DocumentOp::ResolvedWrite { .. },
        ) => false,

        // `Merge`/`UpdateFromJoin` never reach this — staged as point ops instead.
        // `BatchInsert` replays via `exec_tx_passthrough` (oracle divergence, see module doc).
        PhysicalPlan::Document(
            DocumentOp::BatchInsert { .. }
            | DocumentOp::Merge { .. }
            | DocumentOp::UpdateFromJoin { .. },
        ) => true,

        // `Truncate` stays write-but-unbuffered: unverified whether the passthrough
        // executes it correctly at COMMIT replay, so ROLLBACK still doesn't undo it.
        PhysicalPlan::Document(DocumentOp::Truncate { .. }) => false,

        // ---- Vector: encoded (buffered) ----
        PhysicalPlan::Vector(
            VectorOp::Insert { .. } | VectorOp::BatchInsert { .. } | VectorOp::Delete { .. },
        ) => true,
        // `SetParams` is `Permission::Alter`, but `to_replicated_entry` encodes it —
        // classified as a write despite the DDL-like permission tier.
        PhysicalPlan::Vector(VectorOp::SetParams { .. }) => true,
        // `DropIndex` replicates the same way as `SetParams`.
        PhysicalPlan::Vector(VectorOp::DropIndex { .. }) => true,

        // ---- Vector: reads, not encoded ----
        PhysicalPlan::Vector(
            VectorOp::Search { .. }
            | VectorOp::MultiSearch { .. }
            | VectorOp::QueryStats { .. }
            | VectorOp::SparseSearch { .. }
            | VectorOp::MultiVectorScoreSearch { .. },
        ) => false,

        // `to_replicated_entry` encodes all six (`encode/vector.rs`) — a plain
        // oracle-matching write, not a divergence. See `vector_variants_match_oracle`.
        PhysicalPlan::Vector(
            VectorOp::DeleteBySurrogate { .. }
            | VectorOp::SparseInsert { .. }
            | VectorOp::SparseDelete { .. }
            | VectorOp::MultiVectorInsert { .. }
            | VectorOp::MultiVectorDelete { .. }
            | VectorOp::DirectUpsert { .. },
        ) => true,

        // DDL/Alter, not encoded.
        PhysicalPlan::Vector(
            VectorOp::Seal { .. } | VectorOp::CompactIndex { .. } | VectorOp::Rebuild { .. },
        ) => false,

        // ---- Crdt: encoded (buffered) ----
        // Raw delta applies need preview admission before proposal; `route_in_tx_write` rejects them.
        PhysicalPlan::Crdt(
            CrdtOp::ImportSnapshot { .. }
            | CrdtOp::ListInsert { .. }
            | CrdtOp::ListDelete { .. }
            | CrdtOp::ListMove { .. }
            | CrdtOp::DocUpsert { .. }
            | CrdtOp::DocDelete { .. },
        ) => true,

        // ---- Crdt: raw Apply is rejected in transactions; reads are not encoded ----
        PhysicalPlan::Crdt(
            CrdtOp::Apply { .. }
            | CrdtOp::ApplyAuthenticated { .. }
            | CrdtOp::Read { .. }
            | CrdtOp::PreviewApply { .. }
            | CrdtOp::ReadConstraints { .. }
            | CrdtOp::GetPolicy { .. }
            | CrdtOp::ReadAtVersion { .. }
            | CrdtOp::GetVersionVector { .. }
            | CrdtOp::ExportDelta { .. },
        ) => false,

        // No encoder arm here (see module doc); reaches `exec_tx_passthrough` at COMMIT
        // with no reject arm, at the cost of RYOW loss + the no-undo gap.
        PhysicalPlan::Crdt(
            CrdtOp::SetConstraints { .. }
            | CrdtOp::DropConstraints { .. }
            | CrdtOp::RestoreToVersion { .. },
        ) => true,

        // DDL/Alter, not encoded.
        PhysicalPlan::Crdt(CrdtOp::SetPolicy { .. } | CrdtOp::CompactAtVersion { .. }) => false,

        // ---- Graph: encoded (buffered) ----
        PhysicalPlan::Graph(
            GraphOp::EdgePut { .. }
            | GraphOp::EdgeDelete { .. }
            | GraphOp::SetNodeLabels { .. }
            | GraphOp::RemoveNodeLabels { .. }
            | GraphOp::EdgePutBatch { .. }
            | GraphOp::EdgeDeleteBatch { .. },
        ) => true,

        // ---- Graph: reads, not encoded ----
        PhysicalPlan::Graph(
            // Resolve pass is read-only; `to_replicated_entry` reports `None`.
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
            | GraphOp::BspSuperstep(_)
            | GraphOp::WccSuperstep(_)
            | GraphOp::TemporalNeighbors { .. }
            | GraphOp::TemporalAlgorithm { .. }
            | GraphOp::Stats { .. },
        ) => false,

        // ---- Kv: encoded (buffered) ----
        PhysicalPlan::Kv(
            KvOp::Put { .. }
            | KvOp::Delete { .. }
            | KvOp::Insert { .. }
            | KvOp::InsertIfAbsent { .. }
            | KvOp::InsertOnConflictUpdate { .. }
            | KvOp::BatchPut { .. }
            | KvOp::Incr { .. }
            | KvOp::IncrFloat { .. }
            | KvOp::Cas { .. }
            | KvOp::GetSet { .. }
            | KvOp::RegisterSortedIndex { .. }
            | KvOp::DropSortedIndex { .. }
            | KvOp::FieldSet { .. }
            | KvOp::Transfer { .. }
            | KvOp::TransferItem { .. },
        ) => true,
        // `Expire`/`Persist` are encoded (buffered), but `execute_tx_kv` rejects
        // them inside a `TransactionBatch` — a COMMIT replay fails there regardless.
        PhysicalPlan::Kv(KvOp::Expire { .. } | KvOp::Persist { .. }) => true,

        // ---- Kv: reads, not encoded ----
        PhysicalPlan::Kv(
            KvOp::Get { .. }
            | KvOp::Scan { .. }
            | KvOp::GetTtl { .. }
            | KvOp::BatchGet { .. }
            | KvOp::FieldGet { .. }
            | KvOp::SortedIndexRank { .. }
            | KvOp::SortedIndexTopK { .. }
            | KvOp::SortedIndexRange { .. }
            | KvOp::SortedIndexCount { .. }
            | KvOp::SortedIndexScore { .. }
            | KvOp::MaterializeScan { .. }
            // Read-only: reports what a governed write would apply; encodes nothing.
            | KvOp::ResolveWrite(_),
        ) => false,

        // ---- Kv: index / DDL / truncate — encoded, but autocommit-only ----
        // Inverse divergence: encoded, but transaction resolve rejects them.
        PhysicalPlan::Kv(
            KvOp::Truncate { .. } | KvOp::RegisterIndex { .. } | KvOp::DropIndex { .. },
        ) => false,

        // ---- Kv: resolved write — encoded, but autocommit-only ----
        // Same inverse divergence: encoded, but transaction resolve rejects it.
        PhysicalPlan::Kv(KvOp::ResolvedWrite { .. }) => false,

        // ---- Kv: predicate DML — encoded (buffered) ----
        // Encoded; COMMIT-time resolve refuses them — autocommit is the supported path.
        PhysicalPlan::Kv(KvOp::PredicateUpdate { .. } | KvOp::PredicateDelete { .. }) => true,

        // ---- Columnar: encoded (buffered) ----
        PhysicalPlan::Columnar(
            ColumnarOp::Insert { .. }
            | ColumnarOp::Delete { .. }
            | ColumnarOp::Update { .. }
            | ColumnarOp::ResolvedUpdate { .. }
            | ColumnarOp::ResolvedDelete { .. },
        ) => true,
        // ---- Columnar: reads, not encoded ----
        // `ResolveDml` mirrors the oracle: `to_replicated_entry` returns `None` for it.
        PhysicalPlan::Columnar(
            ColumnarOp::Scan { .. }
                | ColumnarOp::MaterializeScan { .. }
                | ColumnarOp::ResolveDml { .. },
        ) => false,

        // ---- Timeseries ----
        PhysicalPlan::Timeseries(TimeseriesOp::Ingest { .. }) => true,
        // Resolve pass is read-only; `to_replicated_entry` reports `None`.
        PhysicalPlan::Timeseries(TimeseriesOp::Scan { .. } | TimeseriesOp::ResolveIngest(_)) => {
            false
        }

        // ---- Text: encoded (buffered) ----
        PhysicalPlan::Text(TextOp::FtsIndexDoc { .. } | TextOp::FtsDeleteDoc { .. }) => true,
        // ---- Text: reads / DDL, not encoded ----
        PhysicalPlan::Text(
            TextOp::Search { .. }
            | TextOp::BM25ScoreScan { .. }
            | TextOp::PhraseSearch { .. }
            | TextOp::HybridSearch { .. }
            | TextOp::HybridSearchTriple { .. }
            | TextOp::SetTextConfig { .. },
        ) => false,

        // ---- Spatial: encoded (buffered) ----
        PhysicalPlan::Spatial(SpatialOp::Insert { .. } | SpatialOp::Delete { .. }) => true,
        // ---- Spatial: reads, not encoded ----
        PhysicalPlan::Spatial(SpatialOp::Scan { .. }) => false,

        // ---- Query: coordinator-side data movement / joins / aggregates — all reads.
        PhysicalPlan::Query(
            QueryOp::Exchange(_)
            | QueryOp::ProviderScan { .. }
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
            | QueryOp::PostProcess { .. },
        ) => false,

        // ---- Meta: control / maintenance ops — internal orchestration, never a client `task.plan`.
        PhysicalPlan::Meta(
            MetaOp::WalAppend { .. }
            | MetaOp::Cancel { .. }
            | MetaOp::TransactionBatch { .. }
            | MetaOp::CreateSnapshot
            | MetaOp::Compact
            | MetaOp::Checkpoint
            | MetaOp::RegisterContinuousAggregate { .. }
            | MetaOp::UnregisterContinuousAggregate { .. }
            | MetaOp::ListContinuousAggregates
            | MetaOp::ConvertCollection { .. }
            | MetaOp::CreateTenantSnapshot { .. }
            | MetaOp::RestoreTenantSnapshot { .. }
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
            | MetaOp::RenameCollection { .. }
            | MetaOp::StageWrite { .. }
            | MetaOp::DropTxnOverlay { .. }
            | MetaOp::MarkSavepoint { .. }
            | MetaOp::RollbackToSavepoint { .. }
            | MetaOp::RecordCalvinWriteVersions { .. }
            | MetaOp::CalvinFlush { .. }
            | MetaOp::CalvinDrop { .. }
            | MetaOp::CalvinResolve { .. }
            | MetaOp::ResolveTxn { .. },
        ) => false,

        // ---- Array reads / DDL / Flush: `to_replicated_entry` returns `None` — matches oracle.
        PhysicalPlan::Array(
            ArrayOp::OpenArray { .. }
            | ArrayOp::Slice { .. }
            | ArrayOp::Project { .. }
            | ArrayOp::Aggregate { .. }
            | ArrayOp::Elementwise { .. }
            | ArrayOp::Flush { .. }
            | ArrayOp::Compact { .. }
            | ArrayOp::SurrogateBitmapScan { .. }
            | ArrayOp::DropArray { .. }
            | ArrayOp::RestoreArrayDrop { .. }
            | ArrayOp::PurgeArrayDrop { .. },
        ) => false,
        // Buffered and encoded — matches oracle. `to_replicated_entry` emits
        // `ArrayCellPut`/`ArrayCellDelete`, at the cost of RYOW loss + the no-undo gap.
        PhysicalPlan::Array(ArrayOp::Put { .. } | ArrayOp::Delete { .. }) => true,

        // ---- ClusterArray: coordinator-only, never touched by `to_replicated_entry`.
        PhysicalPlan::ClusterArray(ClusterArrayOp::Slice { .. } | ClusterArrayOp::Agg { .. }) => {
            false
        }
        // Buffered but unencoded: `session::txn_expand` reshapes the routing
        // wrapper into per-shard `ArrayOp::Put`/`Delete` before it enters the
        // buffer, so COMMIT replays those and never the wrapper itself.
        PhysicalPlan::ClusterArray(ClusterArrayOp::Put { .. } | ClusterArrayOp::Delete { .. }) => {
            true
        }
        PhysicalPlan::ClusterEvent(_) => false,
    }
}

/// Equivalence regression: `plan_requires_txn_buffering` must match
/// `to_replicated_entry(..).is_some()` for every leaf variant except the
/// flipped set (pinned separately via `assert_buffered_but_unencoded`).
#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::wal_replication::ReplicableWrite;
    use nodedb_array::types::ArrayId;
    use nodedb_graph::{AlgoParams, Direction, GraphAlgorithm, GraphTraversalOptions};
    use nodedb_physical::physical_plan::{
        ArrayBinaryOp, ArrayReducer, BatchEdge, BspSuperstepPlan, ColumnarInsertIntent,
        EnforcementOptions, ExchangeMode, ExchangeOp, OllpPredictedEdge, SpatialPredicate,
        StorageMode, WccSuperstepPlan,
    };
    use nodedb_types::calvin::{
        EngineKeySet, EngineTag, PassiveReadKey, ReadKeyIdent, SortedVec, VersionedReadEntry,
    };
    use nodedb_types::geometry::Geometry;
    use nodedb_types::id::{RequestId, TxnId, VShardId};
    use nodedb_types::timeseries::continuous_agg::{ContinuousAggregateDef, RefreshPolicy};
    use nodedb_types::vector_distance::DistanceMetric;
    use nodedb_types::{
        DatabaseId, Lsn, QualifiedCollection, Surrogate, SystemTimeScope, TenantId,
        VectorAnnOptions,
    };
    use std::collections::BTreeMap;

    fn tenant() -> TenantId {
        TenantId::new(1)
    }
    fn db() -> DatabaseId {
        DatabaseId::DEFAULT
    }
    fn vshard() -> VShardId {
        VShardId::new(0)
    }

    /// A trivial read plan used as filler wherever a `Box<PhysicalPlan>`
    /// child is required but its content is irrelevant to the classification
    /// under test.
    fn trivial_read_plan() -> PhysicalPlan {
        PhysicalPlan::Kv(KvOp::Get {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, ""),
            key: Vec::new(),
            rls_filters: Vec::new(),
            surrogate_ceiling: None,
        })
    }

    /// Decide + encode in one call, matching the pre-`ReplicableWrite` shape
    /// these oracle assertions were written against.
    fn to_replicated_entry(
        tenant_id: TenantId,
        database_id: DatabaseId,
        vshard_id: VShardId,
        plan: &PhysicalPlan,
    ) -> crate::Result<Option<crate::control::wal_replication::ReplicatedEntry>> {
        let write = ReplicableWrite::decide_for_replication(plan)?;
        crate::control::wal_replication::to_replicated_entry(
            tenant_id,
            database_id,
            vshard_id,
            &write,
        )
    }

    fn assert_matches_oracle(plan: &PhysicalPlan) {
        let expected = to_replicated_entry(tenant(), db(), vshard(), plan)
            .expect("encode must not error for this fixture")
            .is_some();
        let actual = plan_requires_txn_buffering(plan);
        assert_eq!(
            actual, expected,
            "plan_requires_txn_buffering disagrees with to_replicated_entry for {plan:?}"
        );
    }

    /// Pin the deliberate divergence for a flipped variant (module doc): buffered
    /// `true` here while `to_replicated_entry` still has no encoder arm.
    fn assert_buffered_but_unencoded(plan: &PhysicalPlan) {
        assert!(
            plan_requires_txn_buffering(plan),
            "expected {plan:?} to require txn buffering"
        );
        assert!(
            to_replicated_entry(tenant(), db(), vshard(), plan)
                .expect("encode must not error for this fixture")
                .is_none(),
            "expected {plan:?} to still have no WAL encoder arm (encoder omission is a separate, undone unit)"
        );
    }

    /// Pin the inverse divergence (module doc): classified `false` since it's
    /// rejected in an explicit transaction, while `to_replicated_entry` returns
    /// `Some` since it replicates normally autocommit.
    fn assert_encoded_but_not_buffered(plan: &PhysicalPlan) {
        assert!(
            !plan_requires_txn_buffering(plan),
            "expected {plan:?} to not require txn buffering (autocommit-only)"
        );
        assert!(
            to_replicated_entry(tenant(), db(), vshard(), plan)
                .expect("encode must not error for this fixture")
                .is_some(),
            "expected {plan:?} to have a WAL encoder arm"
        );
    }

    #[test]
    fn document_variants_match_oracle() {
        let plans = vec![
            PhysicalPlan::Document(DocumentOp::PointGet {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                document_id: "d".into(),
                surrogate: Surrogate::ZERO,
                pk_bytes: Vec::new(),
                rls_filters: Vec::new(),
                system_time: SystemTimeScope::Current,
                valid_at_ms: None,
            }),
            PhysicalPlan::Document(DocumentOp::PointPut {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                document_id: "d".into(),
                value: Vec::new(),
                surrogate: Surrogate::ZERO,
                pk_bytes: Vec::new(),
                returning: None,
                rls_filters: Vec::new(),
                resolved_sum_targets: Vec::new(),
            }),
            PhysicalPlan::Document(DocumentOp::PointInsert {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                document_id: "d".into(),
                value: Vec::new(),
                if_absent: false,
                surrogate: Surrogate::ZERO,
                returning: None,
                rls_filters: Vec::new(),
                resolved_sum_targets: Vec::new(),
                deferred_sum_targets: Vec::new(),
            }),
            PhysicalPlan::Document(DocumentOp::PointDelete {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                document_id: "d".into(),
                surrogate: Surrogate::ZERO,
                pk_bytes: Vec::new(),
                returning: None,
                rls_filters: Vec::new(),
                rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
                resolved_sum_targets: Vec::new(),
            }),
            PhysicalPlan::Document(DocumentOp::PointUpdate {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                document_id: "d".into(),
                surrogate: Surrogate::ZERO,
                pk_bytes: Vec::new(),
                updates: Vec::new(),
                returning: None,
                rls_filters: Vec::new(),
                rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
                resolved_sum_targets: Vec::new(),
                declared_primary_key: None,
            }),
            PhysicalPlan::Document(DocumentOp::Scan {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                limit: 0,
                offset: 0,
                sort_keys: Vec::new(),
                filters: Vec::new(),
                distinct: false,
                projection: Vec::new(),
                computed_columns: Vec::new(),
                window_functions: Vec::new(),
                system_time: SystemTimeScope::Current,
                valid_at_ms: None,
                prefilter: None,
            }),
            PhysicalPlan::Document(DocumentOp::RangeScan {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                field: "f".into(),
                lower: None,
                upper: None,
                limit: 0,
                rls_filters: Vec::new(),
            }),
            PhysicalPlan::Document(DocumentOp::Register {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                indexes: Vec::new(),
                crdt_enabled: false,
                storage_mode: StorageMode::default(),
                enforcement: Box::new(EnforcementOptions::default()),
                bitemporal: false,
                conflict_policy: None,
                timeseries: None,
                vector_primary: None,
            }),
            PhysicalPlan::Document(DocumentOp::IndexLookup {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                path: "$.f".into(),
                value: "v".into(),
            }),
            PhysicalPlan::Document(DocumentOp::IndexedFetch {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                path: "$.f".into(),
                value: "v".into(),
                filters: Vec::new(),
                projection: Vec::new(),
                limit: 0,
                offset: 0,
            }),
            PhysicalPlan::Document(DocumentOp::DropIndex {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                field: "f".into(),
            }),
            PhysicalPlan::Document(DocumentOp::BackfillIndex {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                path: "$.f".into(),
                is_array: false,
                unique: false,
                case_insensitive: false,
                predicate: None,
            }),
            PhysicalPlan::Document(DocumentOp::EstimateCount {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                field: "f".into(),
            }),
            PhysicalPlan::Document(DocumentOp::InsertSelect {
                target_collection: QualifiedCollection::new(DatabaseId::DEFAULT, "t"),
                source_collection: QualifiedCollection::new(DatabaseId::DEFAULT, "s"),
                source_filters: Vec::new(),
                source_limit: 0,
                column_map: Vec::new(),
            }),
            PhysicalPlan::Document(DocumentOp::Upsert {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                document_id: "d".into(),
                value: Vec::new(),
                on_conflict_updates: Vec::new(),
                surrogate: Surrogate::ZERO,
                rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
                returning: None,
                rls_filters: Vec::new(),
                resolved_sum_targets: Vec::new(),
            }),
            // BulkUpdate / BulkDelete: non-OLLP (both None) — the buffered case.
            PhysicalPlan::Document(DocumentOp::BulkUpdate {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                filters: Vec::new(),
                updates: Vec::new(),
                returning: None,
                ollp_predicted_surrogates: None,
                ollp_predicted_edges: None,
                rls_filters: Vec::new(),
                rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
                resolved_sum_targets: Vec::new(),
                declared_primary_key: None,
            }),
            PhysicalPlan::Document(DocumentOp::BulkDelete {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                filters: Vec::new(),
                returning: None,
                ollp_predicted_surrogates: None,
                ollp_predicted_edges: None,
                rls_filters: Vec::new(),
                rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
                resolved_sum_targets: Vec::new(),
            }),
            // BulkUpdate / BulkDelete: OLLP surrogate set present — the
            // Calvin-routed, not-buffered case.
            PhysicalPlan::Document(DocumentOp::BulkUpdate {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                filters: Vec::new(),
                updates: Vec::new(),
                returning: None,
                ollp_predicted_surrogates: Some(vec![1, 2]),
                ollp_predicted_edges: None,
                rls_filters: Vec::new(),
                rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
                resolved_sum_targets: Vec::new(),
                declared_primary_key: None,
            }),
            PhysicalPlan::Document(DocumentOp::BulkDelete {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                filters: Vec::new(),
                returning: None,
                ollp_predicted_surrogates: Some(vec![1, 2]),
                ollp_predicted_edges: None,
                rls_filters: Vec::new(),
                rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
                resolved_sum_targets: Vec::new(),
            }),
            // BulkUpdate / BulkDelete: OLLP edge set present, surrogates None —
            // the other half of the `Some` guard.
            PhysicalPlan::Document(DocumentOp::BulkUpdate {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                filters: Vec::new(),
                updates: Vec::new(),
                returning: None,
                ollp_predicted_surrogates: None,
                ollp_predicted_edges: Some(vec![OllpPredictedEdge {
                    surrogate: 1,
                    from: "a".into(),
                    to: "b".into(),
                    label: None,
                }]),
                rls_filters: Vec::new(),
                rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
                resolved_sum_targets: Vec::new(),
                declared_primary_key: None,
            }),
            PhysicalPlan::Document(DocumentOp::BulkDelete {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                filters: Vec::new(),
                returning: None,
                ollp_predicted_surrogates: None,
                ollp_predicted_edges: Some(vec![OllpPredictedEdge {
                    surrogate: 1,
                    from: "a".into(),
                    to: "b".into(),
                    label: None,
                }]),
                rls_filters: Vec::new(),
                rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
                resolved_sum_targets: Vec::new(),
            }),
            PhysicalPlan::Document(DocumentOp::MaterializeScan {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                cursor: Vec::new(),
                count: 0,
                system_as_of_ms: None,
            }),
            // Predicate `true` and encoder `Some` agree for `BatchInsert`.
            PhysicalPlan::Document(DocumentOp::BatchInsert {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                documents: Vec::new(),
                surrogates: Vec::new(),
                returning: None,
                rls_filters: Vec::new(),
                resolved_sum_targets: Vec::new(),
                deferred_sum_targets: Vec::new(),
            }),
        ];
        for p in &plans {
            assert_matches_oracle(p);
        }
    }

    #[test]
    fn vector_variants_match_oracle() {
        let plans = vec![
            PhysicalPlan::Vector(VectorOp::Search {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                query_vector: Vec::new(),
                top_k: 0,
                ef_search: 0,
                metric: DistanceMetric::Cosine,
                filter_bitmap: None,
                field_name: String::new(),
                rls_filters: Vec::new(),
                inline_prefilter_plan: None,
                ann_options: VectorAnnOptions::default(),
                skip_payload_fetch: false,
                payload_filters: Vec::new(),
            }),
            PhysicalPlan::Vector(VectorOp::Insert {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                vector: Vec::new(),
                dim: 0,
                field_name: String::new(),
                surrogate: Surrogate::ZERO,
                pk_bytes: None,
                provenance: None,
            }),
            PhysicalPlan::Vector(VectorOp::BatchInsert {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                vectors: Vec::new(),
                dim: 0,
                surrogates: Vec::new(),
            }),
            PhysicalPlan::Vector(VectorOp::MultiSearch {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                query_vector: Vec::new(),
                top_k: 0,
                ef_search: 0,
                filter_bitmap: None,
                rls_filters: Vec::new(),
            }),
            PhysicalPlan::Vector(VectorOp::Delete {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                vector_id: 0,
            }),
            PhysicalPlan::Vector(VectorOp::SetParams {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                field_name: String::new(),
                dim: 0,
                m: 0,
                ef_construction: 0,
                metric: String::new(),
                index_type: String::new(),
                pq_m: 0,
                ivf_cells: 0,
                ivf_nprobe: 0,
            }),
            PhysicalPlan::Vector(VectorOp::QueryStats {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                field_name: String::new(),
            }),
            PhysicalPlan::Vector(VectorOp::Seal {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                field_name: String::new(),
            }),
            PhysicalPlan::Vector(VectorOp::CompactIndex {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                field_name: String::new(),
            }),
            PhysicalPlan::Vector(VectorOp::Rebuild {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                field_name: String::new(),
                m: 0,
                m0: 0,
                ef_construction: 0,
            }),
            PhysicalPlan::Vector(VectorOp::SparseSearch {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                field_name: String::new(),
                query_entries: Vec::new(),
                top_k: 0,
            }),
            PhysicalPlan::Vector(VectorOp::MultiVectorScoreSearch {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                field_name: String::new(),
                query_vector: Vec::new(),
                top_k: 0,
                ef_search: 0,
                mode: String::new(),
            }),
            // Encoder covers these six, so predicate and encoder agree — oracle-matching set.
            PhysicalPlan::Vector(VectorOp::DeleteBySurrogate {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                surrogate: Surrogate::ZERO,
                field_name: String::new(),
                provenance: None,
            }),
            PhysicalPlan::Vector(VectorOp::SparseInsert {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                field_name: String::new(),
                doc_id: "d".into(),
                entries: Vec::new(),
            }),
            PhysicalPlan::Vector(VectorOp::SparseDelete {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                field_name: String::new(),
                doc_id: "d".into(),
            }),
            PhysicalPlan::Vector(VectorOp::MultiVectorInsert {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                field_name: String::new(),
                document_surrogate: Surrogate::ZERO,
                vectors: Vec::new(),
                count: 0,
                dim: 0,
            }),
            PhysicalPlan::Vector(VectorOp::MultiVectorDelete {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                field_name: String::new(),
                document_surrogate: Surrogate::ZERO,
            }),
            PhysicalPlan::Vector(VectorOp::DirectUpsert {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                field: String::new(),
                surrogate: Surrogate::ZERO,
                vector: Vec::new(),
                payload: Vec::new(),
                quantization: Default::default(),
                storage_dtype: Default::default(),
                payload_indexes: Vec::new(),
                returning: None,
                rls_filters: Vec::new(),
            }),
        ];
        for p in &plans {
            assert_matches_oracle(p);
        }
    }

    #[test]
    fn raw_crdt_apply_is_not_transaction_bufferable() {
        let plan = PhysicalPlan::Crdt(CrdtOp::Apply {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
            document_id: "d".into(),
            delta: Vec::new(),
            peer_id: 0,
            mutation_id: 0,
            surrogate: Surrogate::ZERO,
            provenance: None,
            constraint_version_required: 0,
            expected_frontier_digest: None,
        });
        assert!(!plan_requires_txn_buffering(&plan));
    }

    #[test]
    fn crdt_variants_match_oracle() {
        let plans = vec![
            PhysicalPlan::Crdt(CrdtOp::Read {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                document_id: "d".into(),
            }),
            PhysicalPlan::Crdt(CrdtOp::ImportSnapshot {
                tenant_id: 1,
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                bytes: Vec::new(),
            }),
            PhysicalPlan::Crdt(CrdtOp::ReadConstraints {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
            }),
            PhysicalPlan::Crdt(CrdtOp::SetPolicy {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                policy_json: "{}".into(),
            }),
            PhysicalPlan::Crdt(CrdtOp::GetPolicy {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
            }),
            PhysicalPlan::Crdt(CrdtOp::ReadAtVersion {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                document_id: "d".into(),
                version_vector_json: "{}".into(),
            }),
            PhysicalPlan::Crdt(CrdtOp::GetVersionVector {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
            }),
            PhysicalPlan::Crdt(CrdtOp::ExportDelta {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                from_version_json: "{}".into(),
            }),
            PhysicalPlan::Crdt(CrdtOp::CompactAtVersion {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                target_version_json: "{}".into(),
            }),
            PhysicalPlan::Crdt(CrdtOp::ListInsert {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                document_id: "d".into(),
                list_path: "$.l".into(),
                index: 0,
                fields_json: "{}".into(),
                surrogate: Surrogate::ZERO,
            }),
            PhysicalPlan::Crdt(CrdtOp::ListDelete {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                document_id: "d".into(),
                list_path: "$.l".into(),
                index: 0,
                surrogate: Surrogate::ZERO,
            }),
            PhysicalPlan::Crdt(CrdtOp::ListMove {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                document_id: "d".into(),
                list_path: "$.l".into(),
                from_index: 0,
                to_index: 1,
                surrogate: Surrogate::ZERO,
            }),
            // `to_replicated_entry` has encoder arms for these — predicate and encoder agree.
            PhysicalPlan::Crdt(CrdtOp::SetConstraints {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                constraint_version: 0,
                constraints: Vec::new(),
            }),
            PhysicalPlan::Crdt(CrdtOp::DropConstraints {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                constraint_version: 0,
            }),
            PhysicalPlan::Crdt(CrdtOp::DocUpsert {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                document_id: "d".into(),
                fields_json: "{}".into(),
                surrogate: Surrogate::ZERO,
                partial: false,
                returning: None,
                rls_filters: Vec::new(),
            }),
            PhysicalPlan::Crdt(CrdtOp::DocDelete {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                document_id: "d".into(),
                surrogate: Surrogate::ZERO,
                returning: None,
                rls_filters: Vec::new(),
            }),
        ];
        for p in &plans {
            assert_matches_oracle(p);
        }
    }

    #[test]
    fn graph_variants_match_oracle() {
        let plans = vec![
            PhysicalPlan::Graph(GraphOp::EdgePut {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                src_id: "a".into(),
                label: "L".into(),
                dst_id: "b".into(),
                properties: Vec::new(),
                src_surrogate: Surrogate::ZERO,
                dst_surrogate: Surrogate::ZERO,
            }),
            PhysicalPlan::Graph(GraphOp::EdgePutBatch {
                edges: Vec::<BatchEdge>::new(),
            }),
            PhysicalPlan::Graph(GraphOp::EdgeDelete {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                src_id: "a".into(),
                label: "L".into(),
                dst_id: "b".into(),
                src_surrogate: Surrogate::ZERO,
                dst_surrogate: Surrogate::ZERO,
                rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            }),
            PhysicalPlan::Graph(GraphOp::EdgeDeleteBatch {
                edges: Vec::<BatchEdge>::new(),
            }),
            PhysicalPlan::Graph(GraphOp::Hop {
                start_nodes: Vec::new(),
                edge_label: None,
                direction: Direction::Out,
                depth: 0,
                options: GraphTraversalOptions::default(),
                rls_filters: Vec::new(),
                frontier_bitmap: None,
                collection: None,
            }),
            PhysicalPlan::Graph(GraphOp::Neighbors {
                node_id: "n".into(),
                edge_label: None,
                direction: Direction::Out,
                rls_filters: Vec::new(),
                collection: None,
            }),
            PhysicalPlan::Graph(GraphOp::NeighborsMulti {
                node_ids: Vec::new(),
                edge_label: None,
                direction: Direction::Out,
                max_results: 0,
                rls_filters: Vec::new(),
                collection: None,
            }),
            PhysicalPlan::Graph(GraphOp::Path {
                src: "a".into(),
                dst: "b".into(),
                edge_label: None,
                max_depth: 0,
                options: GraphTraversalOptions::default(),
                rls_filters: Vec::new(),
                frontier_bitmap: None,
                collection: None,
            }),
            PhysicalPlan::Graph(GraphOp::Subgraph {
                start_nodes: Vec::new(),
                edge_label: None,
                depth: 0,
                options: GraphTraversalOptions::default(),
                rls_filters: Vec::new(),
                collection: None,
            }),
            PhysicalPlan::Graph(GraphOp::RagFusion {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                query_vector: Vec::new(),
                vector_top_k: 0,
                edge_label: None,
                direction: Direction::Out,
                expansion_depth: 0,
                final_top_k: 0,
                rrf_k: (0.0, 0.0),
                rrf_k_triple: None,
                vector_field: String::new(),
                options: GraphTraversalOptions::default(),
                bm25_query: None,
                bm25_field: None,
            }),
            PhysicalPlan::Graph(GraphOp::Algo {
                algorithm: GraphAlgorithm::PageRank,
                params: AlgoParams::default(),
            }),
            PhysicalPlan::Graph(GraphOp::Match {
                query: Vec::new(),
                frontier_bitmap: None,
                cluster_mode: false,
            }),
            PhysicalPlan::Graph(GraphOp::MatchContinuation {
                query: Vec::new(),
                resume_triple_idx: 0,
                partial_row: Vec::new(),
                source_node: "n".into(),
                source_binding: "b".into(),
            }),
            PhysicalPlan::Graph(GraphOp::MatchVarLenResume {
                query: Vec::new(),
                resume: Vec::new(),
            }),
            PhysicalPlan::Graph(GraphOp::BspSuperstep(Box::new(BspSuperstepPlan {
                algorithm: GraphAlgorithm::PageRank,
                params: AlgoParams::default(),
                superstep: 0,
                global_n: 0,
                owned_vshards: Vec::new(),
                incoming_contributions: Vec::new(),
                rank_seed: Vec::new(),
                global_dangling: 0.0,
                personalization_sum: 0.0,
            }))),
            PhysicalPlan::Graph(GraphOp::WccSuperstep(Box::new(WccSuperstepPlan {
                params: AlgoParams::default(),
                owned_vshards: Vec::new(),
            }))),
            PhysicalPlan::Graph(GraphOp::SetNodeLabels {
                node_id: "n".into(),
                labels: Vec::new(),
            }),
            PhysicalPlan::Graph(GraphOp::RemoveNodeLabels {
                node_id: "n".into(),
                labels: Vec::new(),
            }),
            PhysicalPlan::Graph(GraphOp::TemporalNeighbors {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                node_id: "n".into(),
                edge_label: None,
                direction: Direction::Out,
                system_time: SystemTimeScope::Current,
                valid_at_ms: None,
                rls_filters: Vec::new(),
            }),
            PhysicalPlan::Graph(GraphOp::TemporalAlgorithm {
                algorithm: GraphAlgorithm::PageRank,
                params: AlgoParams::default(),
                system_time: SystemTimeScope::Current,
            }),
            PhysicalPlan::Graph(GraphOp::Stats {
                collection: None,
                as_of: None,
            }),
        ];
        for p in &plans {
            assert_matches_oracle(p);
        }
    }

    #[test]
    fn kv_variants_match_oracle() {
        let plans = vec![
            PhysicalPlan::Kv(KvOp::Get {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                key: Vec::new(),
                rls_filters: Vec::new(),
                surrogate_ceiling: None,
            }),
            PhysicalPlan::Kv(KvOp::Put {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                key: Vec::new(),
                value: Vec::new(),
                ttl_ms: 0,
                surrogate: Surrogate::ZERO,
                returning: None,
                rls_filters: Vec::new(),
            }),
            PhysicalPlan::Kv(KvOp::Insert {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                key: Vec::new(),
                value: Vec::new(),
                ttl_ms: 0,
                surrogate: Surrogate::ZERO,
                returning: None,
                rls_filters: Vec::new(),
            }),
            PhysicalPlan::Kv(KvOp::InsertIfAbsent {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                key: Vec::new(),
                value: Vec::new(),
                ttl_ms: 0,
                surrogate: Surrogate::ZERO,
                returning: None,
                rls_filters: Vec::new(),
            }),
            PhysicalPlan::Kv(KvOp::InsertOnConflictUpdate {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                key: Vec::new(),
                value: Vec::new(),
                ttl_ms: 0,
                updates: Vec::new(),
                surrogate: Surrogate::ZERO,
                rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
                returning: None,
                rls_filters: Vec::new(),
            }),
            PhysicalPlan::Kv(KvOp::Delete {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                keys: Vec::new(),
                rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            }),
            PhysicalPlan::Kv(KvOp::PredicateUpdate {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                filters: Vec::new(),
                updates: Vec::new(),
                rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            }),
            PhysicalPlan::Kv(KvOp::PredicateDelete {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                filters: Vec::new(),
                rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            }),
            PhysicalPlan::Kv(KvOp::Scan {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                cursor: Vec::new(),
                count: 0,
                filters: Vec::new(),
                match_pattern: None,
                sort_keys: Vec::new(),
                surrogate_ceiling: None,
            }),
            PhysicalPlan::Kv(KvOp::Expire {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                key: Vec::new(),
                ttl_ms: 0,
                rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            }),
            PhysicalPlan::Kv(KvOp::Persist {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                key: Vec::new(),
                rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            }),
            PhysicalPlan::Kv(KvOp::GetTtl {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                key: Vec::new(),
            }),
            PhysicalPlan::Kv(KvOp::BatchGet {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                keys: Vec::new(),
                rls_filters: Vec::new(),
            }),
            PhysicalPlan::Kv(KvOp::BatchPut {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                entries: Vec::new(),
                ttl_ms: 0,
                surrogates: Vec::new(),
                returning: None,
                rls_filters: Vec::new(),
            }),
            PhysicalPlan::Kv(KvOp::FieldGet {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                key: Vec::new(),
                fields: Vec::new(),
                rls_filters: Vec::new(),
            }),
            PhysicalPlan::Kv(KvOp::FieldSet {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                key: Vec::new(),
                updates: Vec::new(),
                surrogate: Surrogate::ZERO,
                rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            }),
            PhysicalPlan::Kv(KvOp::Incr {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                key: Vec::new(),
                delta: 0,
                ttl_ms: 0,
                surrogate: Surrogate::ZERO,
                rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            }),
            PhysicalPlan::Kv(KvOp::IncrFloat {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                key: Vec::new(),
                delta: 0.0,
                surrogate: Surrogate::ZERO,
                rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            }),
            PhysicalPlan::Kv(KvOp::Cas {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                key: Vec::new(),
                expected: Vec::new(),
                new_value: Vec::new(),
                surrogate: Surrogate::ZERO,
                rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            }),
            PhysicalPlan::Kv(KvOp::GetSet {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                key: Vec::new(),
                new_value: Vec::new(),
                surrogate: Surrogate::ZERO,
                rls_filters: Vec::new(),
                rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            }),
            PhysicalPlan::Kv(KvOp::Transfer {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                source_key: Vec::new(),
                dest_key: Vec::new(),
                field: "f".into(),
                amount: 0.0,
                debit_surrogate: Surrogate::ZERO,
                credit_surrogate: Surrogate::ZERO,
                rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            }),
            PhysicalPlan::Kv(KvOp::TransferItem {
                source_collection: QualifiedCollection::new(DatabaseId::DEFAULT, "s"),
                dest_collection: QualifiedCollection::new(DatabaseId::DEFAULT, "d"),
                item_key: Vec::new(),
                dest_key: Vec::new(),
                surrogate: Surrogate::ZERO,
                source_rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
                dest_rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            }),
            PhysicalPlan::Kv(KvOp::RegisterSortedIndex {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                index_name: "i".into(),
                sort_columns: Vec::new(),
                key_column: "k".into(),
                window_type: "none".into(),
                window_timestamp_column: String::new(),
                window_start_ms: 0,
                window_end_ms: 0,
            }),
            PhysicalPlan::Kv(KvOp::DropSortedIndex {
                index_name: "i".into(),
            }),
            PhysicalPlan::Kv(KvOp::SortedIndexRank {
                index_name: "i".into(),
                primary_key: Vec::new(),
            }),
            PhysicalPlan::Kv(KvOp::SortedIndexTopK {
                index_name: "i".into(),
                k: 0,
            }),
            PhysicalPlan::Kv(KvOp::SortedIndexRange {
                index_name: "i".into(),
                score_min: None,
                score_max: None,
            }),
            PhysicalPlan::Kv(KvOp::SortedIndexCount {
                index_name: "i".into(),
            }),
            PhysicalPlan::Kv(KvOp::SortedIndexScore {
                index_name: "i".into(),
                primary_key: Vec::new(),
            }),
            PhysicalPlan::Kv(KvOp::MaterializeScan {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                cursor: Vec::new(),
                count: 0,
            }),
        ];
        for p in &plans {
            assert_matches_oracle(p);
        }
    }

    #[test]
    fn columnar_timeseries_spatial_variants_match_oracle() {
        let plans = vec![
            PhysicalPlan::Columnar(ColumnarOp::Scan {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                projection: Vec::new(),
                limit: 0,
                filters: Vec::new(),
                rls_filters: Vec::new(),
                sort_keys: Vec::new(),
                system_time: SystemTimeScope::Current,
                valid_at_ms: None,
                prefilter: None,
                computed_columns: Vec::new(),
            }),
            PhysicalPlan::Columnar(ColumnarOp::Insert {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                payload: Vec::new(),
                format: "json".into(),
                intent: ColumnarInsertIntent::Insert,
                on_conflict_updates: Vec::new(),
                surrogates: Vec::new(),
                schema_bytes: Vec::new(),
                provenance: None,
                wal_lsn: None,
                rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
                returning: None,
                rls_filters: Vec::new(),
            }),
            PhysicalPlan::Columnar(ColumnarOp::Update {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                filters: Vec::new(),
                updates: Vec::new(),
                rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            }),
            PhysicalPlan::Columnar(ColumnarOp::Delete {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                filters: Vec::new(),
                rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            }),
            PhysicalPlan::Columnar(ColumnarOp::ResolvedUpdate {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                rows: Vec::new(),
                rls_write_check: nodedb_types::RlsWriteCheck::decided_earlier_in_request(),
            }),
            PhysicalPlan::Columnar(ColumnarOp::ResolvedDelete {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                pks: Vec::new(),
                rls_write_check: nodedb_types::RlsWriteCheck::decided_earlier_in_request(),
            }),
            PhysicalPlan::Columnar(ColumnarOp::MaterializeScan {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                cursor: Vec::new(),
                count: 0,
                system_as_of_ms: None,
            }),
            PhysicalPlan::Timeseries(TimeseriesOp::Scan {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                time_range: (0, i64::MAX),
                projection: Vec::new(),
                limit: 0,
                filters: Vec::new(),
                sort_keys: Vec::new(),
                bucket_interval_ms: 0,
                group_by: Vec::new(),
                aggregates: Vec::new(),
                gap_fill: String::new(),
                computed_columns: Vec::new(),
                rls_filters: Vec::new(),
                system_time: SystemTimeScope::Current,
                valid_at_ms: None,
            }),
            PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                payload: Vec::new(),
                format: "ilp".into(),
                wal_lsn: None,
                surrogates: Vec::new(),
                provenance: None,
                rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
                returning: None,
                rls_filters: Vec::new(),
            }),
            PhysicalPlan::Spatial(SpatialOp::Insert {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                field: "f".into(),
                surrogate: Surrogate::ZERO,
                geometry: Geometry::point(0.0, 0.0),
                provenance: None,
            }),
            PhysicalPlan::Spatial(SpatialOp::Delete {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                field: "f".into(),
                surrogate: Surrogate::ZERO,
                provenance: None,
            }),
            PhysicalPlan::Spatial(SpatialOp::Scan {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                field: "f".into(),
                predicate: SpatialPredicate::Contains,
                query_geometry: Geometry::point(0.0, 0.0),
                distance_meters: 0.0,
                attribute_filters: Vec::new(),
                limit: 0,
                projection: Vec::new(),
                rls_filters: Vec::new(),
                prefilter: None,
            }),
        ];
        for p in &plans {
            assert_matches_oracle(p);
        }
    }

    #[test]
    fn text_variants_match_oracle() {
        let plans = vec![
            PhysicalPlan::Text(TextOp::Search {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                query: "q".into(),
                top_k: 0,
                fuzzy: false,
                prefilter: None,
                rls_filters: Vec::new(),
            }),
            PhysicalPlan::Text(TextOp::BM25ScoreScan {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                query: "q".into(),
                score_alias: "s".into(),
                fuzzy: false,
            }),
            PhysicalPlan::Text(TextOp::PhraseSearch {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                terms: Vec::new(),
                top_k: 0,
                prefilter: None,
            }),
            PhysicalPlan::Text(TextOp::HybridSearch {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                query_vector: Vec::new(),
                query_text: "q".into(),
                top_k: 0,
                ef_search: 0,
                fuzzy: false,
                vector_weight: 0.0,
                filter_bitmap: None,
                rls_filters: Vec::new(),
                score_alias: None,
            }),
            PhysicalPlan::Text(TextOp::FtsIndexDoc {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                surrogate: Surrogate::ZERO,
                text: "t".into(),
                provenance: None,
            }),
            PhysicalPlan::Text(TextOp::FtsDeleteDoc {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                surrogate: Surrogate::ZERO,
                provenance: None,
            }),
            PhysicalPlan::Text(TextOp::HybridSearchTriple {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                query_vector: Vec::new(),
                query_text: "q".into(),
                graph_seed_id: "n".into(),
                graph_depth: 0,
                graph_edge_label: None,
                top_k: 0,
                ef_search: 0,
                fuzzy: false,
                rrf_k: (0.0, 0.0, 0.0),
                filter_bitmap: None,
                rls_filters: Vec::new(),
                score_alias: None,
            }),
            PhysicalPlan::Text(TextOp::SetTextConfig {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                analyzer_name: Some("standard".into()),
                fuzzy_default: None,
            }),
        ];
        for p in &plans {
            assert_matches_oracle(p);
        }
    }

    #[test]
    fn query_variants_match_oracle() {
        let plans = vec![
            PhysicalPlan::Query(QueryOp::Exchange(ExchangeOp {
                child: Box::new(trivial_read_plan()),
                mode: ExchangeMode::Broadcast,
            })),
            PhysicalPlan::Query(QueryOp::ProviderScan {
                provider: None,
                rows: Vec::new(),
                filters: Vec::new(),
                projection: Vec::new(),
                computed_columns: Vec::new(),
                window_functions: Vec::new(),
                sort_keys: Vec::new(),
                limit: None,
                offset: 0,
                distinct: false,
            }),
            PhysicalPlan::Query(QueryOp::Aggregate {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                input: None,
                group_by: Vec::new(),
                aggregates: Vec::new(),
                filters: Vec::new(),
                having: Vec::new(),
                limit: 0,
                sub_group_by: Vec::new(),
                sub_aggregates: Vec::new(),
                grouping_sets: Vec::new(),
                sort_keys: Vec::new(),
            }),
            PhysicalPlan::Query(QueryOp::PartialAggregate {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                group_by: Vec::new(),
                aggregates: Vec::new(),
                filters: Vec::new(),
            }),
            PhysicalPlan::Query(QueryOp::PartialAggregateState {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                input: None,
                group_by: Vec::new(),
                aggregates: Vec::new(),
                filters: Vec::new(),
            }),
            PhysicalPlan::Query(QueryOp::HashJoin {
                left_collection: QualifiedCollection::new(DatabaseId::DEFAULT, "l"),
                right_collection: QualifiedCollection::new(DatabaseId::DEFAULT, "r"),
                left_alias: None,
                right_alias: None,
                on: Vec::new(),
                join_type: "inner".into(),
                limit: 0,
                post_group_by: Vec::new(),
                post_aggregates: Vec::new(),
                projection: Vec::new(),
                computed_projection: Vec::new(),
                join_filters: Vec::new(),
                post_filters: Vec::new(),
                left_input: None,
                right_input: None,
                left_bitmap: None,
                right_bitmap: None,
                left_rls_filters: Vec::new(),
                right_rls_filters: Vec::new(),
                left_scan_filters: Vec::new(),
                right_scan_filters: Vec::new(),
            }),
            PhysicalPlan::Query(QueryOp::ShuffleJoinConsume {
                build_path: String::new(),
                probe_path: String::new(),
                on: Vec::new(),
                join_type: "inner".into(),
                limit: 0,
                probe_qualifier: String::new(),
                index_qualifier: String::new(),
            }),
            PhysicalPlan::Query(QueryOp::ShuffleAggregateConsume {
                state_path: String::new(),
                group_by: Vec::new(),
                aggregates: Vec::new(),
                having: Vec::new(),
                limit: 0,
                sort_keys: Vec::new(),
            }),
            PhysicalPlan::Query(QueryOp::NestedLoopJoin {
                left_collection: QualifiedCollection::new(DatabaseId::DEFAULT, "l"),
                right_collection: QualifiedCollection::new(DatabaseId::DEFAULT, "r"),
                condition: Vec::new(),
                join_type: "inner".into(),
                limit: 0,
                left_rls_filters: Vec::new(),
                right_rls_filters: Vec::new(),
            }),
            PhysicalPlan::Query(QueryOp::SortMergeJoin {
                left_collection: QualifiedCollection::new(DatabaseId::DEFAULT, "l"),
                right_collection: QualifiedCollection::new(DatabaseId::DEFAULT, "r"),
                on: Vec::new(),
                join_type: "inner".into(),
                limit: 0,
                pre_sorted: false,
                left_rls_filters: Vec::new(),
                right_rls_filters: Vec::new(),
            }),
            PhysicalPlan::Query(QueryOp::FacetCounts {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                filters: Vec::new(),
                fields: Vec::new(),
                limit_per_facet: 0,
            }),
            PhysicalPlan::Query(QueryOp::RecursiveScan {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                base_filters: Vec::new(),
                recursive_filters: Vec::new(),
                join_link: None,
                max_iterations: 0,
                distinct: false,
                limit: 0,
            }),
            PhysicalPlan::Query(QueryOp::RecursiveValue {
                cte_name: "cte".into(),
                columns: Vec::new(),
                init_exprs: Vec::new(),
                step_exprs: Vec::new(),
                condition: None,
                max_depth: 0,
                distinct: false,
            }),
            PhysicalPlan::Query(QueryOp::LateralTopK {
                outer_plan: Box::new(trivial_read_plan()),
                outer_alias: "o".into(),
                inner_collection: QualifiedCollection::new(DatabaseId::DEFAULT, "i"),
                inner_filters: Vec::new(),
                inner_order_by: Vec::new(),
                inner_limit: 0,
                correlation_keys: Vec::new(),
                lateral_alias: "l".into(),
                projection: Vec::new(),
                left_join: false,
            }),
            PhysicalPlan::Query(QueryOp::LateralLoop {
                outer_plan: Box::new(trivial_read_plan()),
                outer_alias: "o".into(),
                inner_collection: QualifiedCollection::new(DatabaseId::DEFAULT, "i"),
                inner_filters: Vec::new(),
                correlation_predicates: Vec::new(),
                lateral_alias: "l".into(),
                projection: Vec::new(),
                left_join: false,
                outer_row_cap: 0,
            }),
        ];
        for p in &plans {
            assert_matches_oracle(p);
        }
    }

    #[test]
    fn meta_variants_match_oracle() {
        let plans = vec![
            PhysicalPlan::Meta(MetaOp::WalAppend {
                payload: Vec::new(),
            }),
            PhysicalPlan::Meta(MetaOp::Cancel {
                target_request_id: RequestId::new(1),
            }),
            PhysicalPlan::Meta(MetaOp::TransactionBatch {
                plans: Vec::new(),
                txn_id: None,
            }),
            PhysicalPlan::Meta(MetaOp::CreateSnapshot),
            PhysicalPlan::Meta(MetaOp::Compact),
            PhysicalPlan::Meta(MetaOp::Checkpoint),
            PhysicalPlan::Meta(MetaOp::RegisterContinuousAggregate {
                def: ContinuousAggregateDef {
                    database_id: 0,
                    name: "agg".into(),
                    source: "src".into(),
                    bucket_interval: "1m".into(),
                    bucket_interval_ms: 60_000,
                    group_by: Vec::new(),
                    aggregates: Vec::new(),
                    refresh_policy: RefreshPolicy::OnFlush,
                    retention_period_ms: 0,
                    stale: false,
                },
            }),
            PhysicalPlan::Meta(MetaOp::UnregisterContinuousAggregate { name: "agg".into() }),
            PhysicalPlan::Meta(MetaOp::ListContinuousAggregates),
            PhysicalPlan::Meta(MetaOp::ConvertCollection {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                target_type: "kv".into(),
                schema_json: "{}".into(),
            }),
            PhysicalPlan::Meta(MetaOp::CreateTenantSnapshot { tenant_id: 1 }),
            PhysicalPlan::Meta(MetaOp::RestoreTenantSnapshot {
                tenant_id: 1,
                snapshot: Vec::new(),
                replace_mode: false,
                clear_vshards: Vec::new(),
                collections_to_clear: Vec::new(),
            }),
            PhysicalPlan::Meta(MetaOp::PurgeTenant { tenant_id: 1 }),
            PhysicalPlan::Meta(MetaOp::UnregisterCollection {
                tenant_id: 1,
                name: "c".into(),
                purge_lsn: 0,
                reclaim_l1_files: true,
            }),
            PhysicalPlan::Meta(MetaOp::UnregisterMaterializedView {
                tenant_id: 1,
                name: "mv".into(),
            }),
            PhysicalPlan::Meta(MetaOp::QueryCollectionSize {
                tenant_id: 1,
                name: "c".into(),
            }),
            PhysicalPlan::Meta(MetaOp::EnforceTimeseriesRetention {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                max_age_ms: 0,
            }),
            PhysicalPlan::Meta(MetaOp::TemporalPurgeEdgeStore {
                tenant_id: 1,
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                cutoff_system_ms: 0,
            }),
            PhysicalPlan::Meta(MetaOp::TemporalPurgeDocumentStrict {
                tenant_id: 1,
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                cutoff_system_ms: 0,
            }),
            PhysicalPlan::Meta(MetaOp::TemporalPurgeColumnar {
                tenant_id: 1,
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                cutoff_system_ms: 0,
            }),
            PhysicalPlan::Meta(MetaOp::TemporalPurgeCrdt {
                tenant_id: 1,
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                cutoff_system_ms: 0,
            }),
            PhysicalPlan::Meta(MetaOp::TemporalPurgeArray {
                tenant_id: 0,
                array_id: "a".into(),
                cutoff_system_ms: 0,
            }),
            PhysicalPlan::Meta(MetaOp::AlterArray {
                array_id: "a".into(),
                audit_retain_ms: None,
                minimum_audit_retain_ms: None,
            }),
            PhysicalPlan::Meta(MetaOp::ApplyContinuousAggRetention),
            PhysicalPlan::Meta(MetaOp::QueryAggregateWatermark {
                aggregate_name: "agg".into(),
            }),
            PhysicalPlan::Meta(MetaOp::QueryLastValues {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
            }),
            PhysicalPlan::Meta(MetaOp::QueryLastValue {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                series_id: 0,
            }),
            PhysicalPlan::Meta(MetaOp::CalvinExecuteStatic {
                epoch: 0,
                position: 0,
                tenant_id: tenant(),
                plans: Vec::new(),
                epoch_system_ms: 0,
                is_group_leader: false,
                versioned_reads: vec![VersionedReadEntry {
                    engine: EngineTag::Kv,
                    collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c").to_string(),
                    key: ReadKeyIdent::Predicate,
                    read_lsn: Lsn::ZERO,
                }],
            }),
            PhysicalPlan::Meta(MetaOp::CalvinExecutePassive {
                epoch: 0,
                position: 0,
                tenant_id: tenant(),
                keys_to_read: vec![PassiveReadKey {
                    engine_key: EngineKeySet::Kv {
                        collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c").to_string(),
                        keys: SortedVec::new(Vec::new()),
                    },
                }],
            }),
            PhysicalPlan::Meta(MetaOp::CalvinExecuteActive {
                epoch: 0,
                position: 0,
                tenant_id: tenant(),
                plans: Vec::new(),
                injected_reads: BTreeMap::new(),
                epoch_system_ms: 0,
                is_group_leader: false,
            }),
            PhysicalPlan::Meta(MetaOp::RebuildIndex {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                index_name: None,
                concurrent: false,
            }),
            PhysicalPlan::Meta(MetaOp::PutSynonymGroup {
                tenant_id: 1,
                record_json: "{}".into(),
            }),
            PhysicalPlan::Meta(MetaOp::DeleteSynonymGroup {
                tenant_id: 1,
                name: "syn".into(),
            }),
            PhysicalPlan::Meta(MetaOp::RenameCollection {
                tenant_id: 1,
                old_database_id: 0,
                new_database_id: 1,
                old_collection: QualifiedCollection::new(DatabaseId::DEFAULT, "old"),
                new_collection: QualifiedCollection::new(DatabaseId::DEFAULT, "new"),
            }),
            PhysicalPlan::Meta(MetaOp::StageWrite {
                plan: Box::new(trivial_read_plan()),
            }),
            PhysicalPlan::Meta(MetaOp::DropTxnOverlay {
                txn_id: TxnId::new(1),
            }),
            PhysicalPlan::Meta(MetaOp::MarkSavepoint {
                txn_id: TxnId::new(1),
            }),
            PhysicalPlan::Meta(MetaOp::RollbackToSavepoint {
                txn_id: TxnId::new(1),
                value_marker: 0,
                graph_marker: 0,
            }),
            PhysicalPlan::Meta(MetaOp::RecordCalvinWriteVersions {
                tenant_id: tenant(),
                plans: Vec::new(),
                epoch: 0,
                position: 0,
            }),
            PhysicalPlan::Meta(MetaOp::CalvinFlush {
                epoch: 0,
                position: 0,
            }),
            PhysicalPlan::Meta(MetaOp::CalvinDrop {
                epoch: 0,
                position: 0,
            }),
            PhysicalPlan::Meta(MetaOp::ResolveTxn {
                txn_id: TxnId::new(1),
                plans: Vec::new(),
            }),
        ];
        for p in &plans {
            assert_matches_oracle(p);
        }
    }

    #[test]
    fn array_and_cluster_array_variants_match_oracle() {
        let array_id = ArrayId::new(tenant(), "a");
        let plans = vec![
            PhysicalPlan::Array(ArrayOp::OpenArray {
                array_id: array_id.clone(),
                schema_msgpack: Vec::new(),
                schema_hash: 0,
                prefix_bits: 0,
                audit_retain_ms: None,
                minimum_audit_retain_ms: None,
            }),
            PhysicalPlan::Array(ArrayOp::Slice {
                array_id: array_id.clone(),
                slice_msgpack: Vec::new(),
                attr_projection: Vec::new(),
                limit: 0,
                cell_filter: None,
                hilbert_range: None,
                system_time: SystemTimeScope::Current,
                valid_at_ms: None,
            }),
            PhysicalPlan::Array(ArrayOp::Project {
                array_id: array_id.clone(),
                attr_indices: Vec::new(),
            }),
            PhysicalPlan::Array(ArrayOp::Aggregate {
                array_id: array_id.clone(),
                attr_idx: 0,
                reducer: ArrayReducer::Sum,
                group_by_dim: -1,
                cell_filter: None,
                return_partial: false,
                hilbert_range: None,
                system_as_of: None,
                valid_at_ms: None,
            }),
            PhysicalPlan::Array(ArrayOp::Elementwise {
                left: array_id.clone(),
                right: array_id.clone(),
                op: ArrayBinaryOp::Add,
                attr_idx: 0,
                cell_filter: None,
            }),
            PhysicalPlan::Array(ArrayOp::Flush {
                array_id: array_id.clone(),
                wal_lsn: 0,
            }),
            PhysicalPlan::Array(ArrayOp::Compact {
                array_id: array_id.clone(),
                audit_retain_ms: None,
            }),
            PhysicalPlan::Array(ArrayOp::SurrogateBitmapScan {
                array_id: array_id.clone(),
                slice_msgpack: Vec::new(),
            }),
            PhysicalPlan::Array(ArrayOp::DropArray {
                array_id: array_id.clone(),
            }),
            // `ArrayOp::Put`/`Delete` match the oracle: both buffered and encoded
            // (Raft-native `ArrayCellPut`/`ArrayCellDelete`).
            PhysicalPlan::Array(ArrayOp::Put {
                array_id: array_id.clone(),
                cells_msgpack: Vec::new(),
                wal_lsn: 0,
                provenance: None,
            }),
            PhysicalPlan::Array(ArrayOp::Delete {
                array_id: array_id.clone(),
                coords_msgpack: Vec::new(),
                wal_lsn: 0,
                provenance: None,
            }),
            PhysicalPlan::ClusterArray(ClusterArrayOp::Slice {
                array_id: array_id.clone(),
                slice_msgpack: Vec::new(),
                attr_projection: Vec::new(),
                limit: 0,
                slice_hilbert_ranges: Vec::new(),
                prefix_bits: 0,
                system_time: SystemTimeScope::Current,
                valid_at_ms: None,
            }),
            PhysicalPlan::ClusterArray(ClusterArrayOp::Agg {
                array_id: array_id.clone(),
                attr_idx: 0,
                reducer_msgpack: Vec::new(),
                group_by_dim: -1,
                slice_hilbert_ranges: Vec::new(),
                prefix_bits: 0,
                system_as_of: None,
                valid_at_ms: None,
            }),
            // `ClusterArrayOp::Put`/`Delete` are flipped (buffered, unencoded)
            // and pinned by `flipped_variants_are_buffered_and_unencoded`.
        ];
        for p in &plans {
            assert_matches_oracle(p);
        }
    }

    /// Pin the oracle divergence for flipped variants (Document, Crdt,
    /// ClusterArray): classified `true` while `to_replicated_entry` has no
    /// encoder arm.
    #[test]
    fn flipped_variants_are_buffered_and_unencoded() {
        let plans = vec![
            PhysicalPlan::Document(DocumentOp::Merge {
                target_collection: QualifiedCollection::new(DatabaseId::DEFAULT, "t"),
                source_collection: QualifiedCollection::new(DatabaseId::DEFAULT, "s"),
                source_alias: "s".into(),
                target_join_col: "id".into(),
                source_join_col: "id".into(),
                clauses: Vec::new(),
                returning: None,
                resolved_inserts: None,
                source_rows: None,
                rls_filters: Vec::new(),
                rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
                resolved_sum_targets: Vec::new(),
                declared_primary_key: None,
            }),
            PhysicalPlan::Document(DocumentOp::UpdateFromJoin {
                target_collection: QualifiedCollection::new(DatabaseId::DEFAULT, "t"),
                source_collection: QualifiedCollection::new(DatabaseId::DEFAULT, "s"),
                source_alias: "s".into(),
                target_join_col: "id".into(),
                source_join_col: "id".into(),
                updates: Vec::new(),
                target_filters: Vec::new(),
                returning: None,
                source_rows: None,
                rls_filters: Vec::new(),
                rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
                resolved_sum_targets: Vec::new(),
                declared_primary_key: None,
            }),
            PhysicalPlan::Crdt(CrdtOp::RestoreToVersion {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                document_id: "d".into(),
                target_version_json: "{}".into(),
                surrogate: Surrogate::ZERO,
            }),
            // `session::txn_expand` reshapes these into per-shard `ArrayOp`
            // writes before they enter the buffer, so the wrapper itself needs
            // no encoder arm.
            PhysicalPlan::ClusterArray(ClusterArrayOp::Put {
                array_id: ArrayId::new(tenant(), "a"),
                array_id_msgpack: Vec::new(),
                cells: Vec::new(),
                wal_lsn: 0,
                prefix_bits: 0,
            }),
            PhysicalPlan::ClusterArray(ClusterArrayOp::Delete {
                array_id: ArrayId::new(tenant(), "a"),
                array_id_msgpack: Vec::new(),
                coords: Vec::new(),
                wal_lsn: 0,
                prefix_bits: 0,
            }),
        ];
        for p in &plans {
            assert_buffered_but_unencoded(p);
        }
    }

    /// Pin the inverse divergence: truncate and KV index/DDL ops are autocommit-only
    /// (resolve rejects them, so buffering is never needed), while `to_replicated_entry`
    /// encodes them all.
    #[test]
    fn truncate_and_index_variants_are_encoded_but_not_buffered() {
        let plans = vec![
            PhysicalPlan::Document(DocumentOp::Truncate {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                restart_identity: false,
                resolved_sum_targets: Vec::new(),
            }),
            PhysicalPlan::Kv(KvOp::Truncate {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
            }),
            PhysicalPlan::Kv(KvOp::RegisterIndex {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                field: "f".into(),
                field_position: 0,
                backfill: false,
            }),
            PhysicalPlan::Kv(KvOp::DropIndex {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                field: "f".into(),
            }),
        ];
        for p in &plans {
            assert_encoded_but_not_buffered(p);
        }
    }
}
