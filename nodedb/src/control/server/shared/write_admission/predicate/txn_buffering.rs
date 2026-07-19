// SPDX-License-Identifier: BUSL-1.1

//! `plan_requires_txn_buffering`: is this in-transaction statement a write
//! that must be buffered until COMMIT, or a read that executes immediately?
//!
//! `route_in_tx_write` (`control/server/shared/session/staging_gate.rs`) used
//! to answer this by calling `to_replicated_entry(..).is_some()` — but that
//! function is a WAL/Raft ENCODER, not a classifier: it ends in a catch-all
//! `_ => None`, so any `PhysicalPlan` variant it has no encoder arm for is
//! silently treated as a read (executed immediately, visible before COMMIT,
//! and NOT rolled back by ROLLBACK).
//!
//! `plan_requires_txn_buffering` reproduces `to_replicated_entry(..).is_some()`
//! variant-for-variant as a compile-time-exhaustive match, so a new
//! `PhysicalPlan` variant forces an explicit staging decision instead of
//! silently inheriting a wrong default. For most arms this is a
//! behavior-preserving reclassification, not a bug fix: every remaining
//! `false` arm below that is semantically a write (`required_permission` says
//! `Write`) carries a comment stating that fact about TODAY's behavior, and
//! fixing those is a separate, later change.
//!
//! One documented set of arms is the exception:
//! `DocumentOp::{Merge, UpdateFromJoin}` and `CrdtOp::RestoreToVersion`
//! classify `true` here even though `to_replicated_entry` has no encoder arm
//! for any of them — a deliberate divergence from the oracle (see the
//! equivalence test below, which pins the divergence explicitly rather than
//! papering over it).
//! `ArrayOp::{Put, Delete}` used to be in this same exception list, but
//! `to_replicated_entry` now has encoder arms for both (see
//! `control/wal_replication/encode/entry_array.rs::array_write`, which emits
//! the Raft-native `ArrayCellPut` / `ArrayCellDelete` cluster-write variants),
//! so they no longer diverge from the oracle and were moved into
//! `array_and_cluster_array_variants_match_oracle` below.
//! `VectorOp::{DeleteBySurrogate, SparseInsert, SparseDelete,
//! MultiVectorInsert, MultiVectorDelete, DirectUpsert}` used to be in this
//! same exception list, but `to_replicated_entry` now has encoder arms for
//! all six (see `control/wal_replication/encode/vector.rs::encode`), so they
//! no longer diverge from the oracle and were moved to
//! `vector_variants_match_oracle` below.
//! `CrdtOp::{ListInsert, ListDelete, ListMove}` similarly used to be in this
//! exception list, but `to_replicated_entry` now has encoder arms for all
//! three (see `control/wal_replication/encode/crdt.rs::encode`), so they no
//! longer diverge from the oracle and were moved into
//! `crdt_variants_match_oracle` below.
//! `DocumentOp::BatchInsert` and `CrdtOp::{SetConstraints, DropConstraints}`
//! were also in this exception list, but `to_replicated_entry` now has
//! encoder arms for all three (see
//! `control/wal_replication/encode/document.rs::encode` and
//! `control/wal_replication/encode/crdt.rs::encode`), so they too no longer
//! diverge from the oracle and were moved into `document_variants_match_oracle`
//! / `crdt_variants_match_oracle` below. Without buffering,
//! each of these executed immediately against base state inside an explicit
//! transaction, was visible before COMMIT, and survived ROLLBACK: a
//! correctness bug, not a classification nuance. Closing it costs two
//! deliberate, documented trade-offs:
//!
//! 1. RYOW LOSS: a `Buffered` plan does not stage into the per-transaction
//!    overlay, so a read later in the SAME transaction does not observe the
//!    write until COMMIT. This matches how bulk Document DML already behaved
//!    in a transaction before it was staged.
//! 2. NO-UNDO GAP (pre-existing, not fixed here): every flipped variant
//!    reaches `exec_tx_passthrough`
//!    (`data/executor/handlers/transaction/sub_plan_write.rs`) at COMMIT,
//!    which pushes no `UndoEntry`. If a sibling sub-plan fails later in the
//!    same COMMIT batch, these writes cannot be reversed by
//!    `rollback_undo_log`. Spatial, Text, and bulk Document writes already
//!    ride this exact path.
//!
//! A second, inverse divergence exists in the opposite direction:
//! `DocumentOp::Truncate` and `KvOp::{Truncate, RegisterIndex, DropIndex}`
//! classify `false` here (not buffered) even though `to_replicated_entry` has
//! an encoder arm for each and returns `Some`. This is not a bug in either
//! function: every one of them is autocommit-only — `resolve/entry.rs`
//! (`data/executor/handlers/transaction/resolve/entry.rs:329-334` for the Kv
//! index/DDL/truncate arm, `:394-400` for the Document arm) rejects them with
//! `PlanError` when they appear inside an explicit transaction, so they are
//! never routed through `plan_requires_txn_buffering` for staging in practice.
//! They only ever reach `to_replicated_entry` via the autocommit path, where
//! they replicate normally. Pinned by
//! `truncate_and_index_variants_are_encoded_but_not_buffered` below via
//! `assert_encoded_but_not_buffered` — the inverse of
//! `assert_buffered_but_unencoded` — and correspondingly excluded from
//! `kv_variants_match_oracle`.

#![deny(clippy::wildcard_enum_match_arm)]

use crate::bridge::envelope::PhysicalPlan;
use nodedb_physical::physical_plan::{
    ArrayOp, ClusterArrayOp, ColumnarOp, CrdtOp, DocumentOp, GraphOp, KvOp, MetaOp, QueryOp,
    SpatialOp, TextOp, TimeseriesOp, VectorOp,
};

/// Whether an in-transaction statement's plan must be buffered for
/// COMMIT-time replay (`true`) or may execute immediately as a read
/// (`false`).
///
/// Mirrors `to_replicated_entry(..).is_some()`
/// (`control/wal_replication/encode/entry.rs`) variant-for-variant —
/// including the two payload-conditional `DocumentOp` bulk variants, whose
/// OLLP-predicted-surrogate/edge fields the WAL encoder inspects before
/// deciding whether to encode — EXCEPT for the documented set of
/// write-but-was-unbuffered variants flipped to `true` below (see module
/// doc): those return `true` here even though the oracle returns `false`.
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

        // OLLP-predicted bulk plans: encoded ONLY when the executor has no
        // predicted surrogate/edge set to verify against (the static-set,
        // non-OLLP path). When either is `Some`, the plan routes via Calvin
        // instead and is classified as a read here, exactly like
        // `to_replicated_entry`.
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
            | DocumentOp::MaterializeScan { .. },
        ) => false,

        // Buffered. Neither `Merge` nor `UpdateFromJoin` reaches this predicate in
        // a transaction: both are intercepted BEFORE `route_in_tx_write` and
        // resolved + staged as concrete point ops at STATEMENT time by
        // `control::server::shared::session::expander_stage` (so they commit
        // indexed, replicated, undo-tracked point writes, not the passthrough);
        // these arms remain only for the autocommit-classification callers and
        // exhaustiveness. `BatchInsert` still replays through
        // `exec_tx_passthrough`
        // (`data/executor/handlers/transaction/sub_plan.rs:165`) at COMMIT with
        // no reject arm: `to_replicated_entry` has no encoder arm for it (a
        // deliberate divergence from the oracle, see module doc). Buffering
        // closes the prior atomicity gap (statement used to execute immediately
        // and survive ROLLBACK) at the cost of RYOW loss + the no-undo gap
        // (module doc) for `BatchInsert`.
        PhysicalPlan::Document(
            DocumentOp::BatchInsert { .. }
            | DocumentOp::Merge { .. }
            | DocumentOp::UpdateFromJoin { .. },
        ) => true,

        // `Truncate` stays write-but-unbuffered: it is unverified whether
        // `exec_tx_document`'s passthrough executes it correctly at COMMIT
        // replay time, so a ROLLBACK inside an explicit transaction still
        // does not undo it today. Not flipped in this change.
        PhysicalPlan::Document(DocumentOp::Truncate { .. }) => false,

        // ---- Vector: encoded (buffered) ----
        PhysicalPlan::Vector(
            VectorOp::Insert { .. } | VectorOp::BatchInsert { .. } | VectorOp::Delete { .. },
        ) => true,
        // `SetParams` is `Permission::Alter` (DDL-shaped), but
        // `to_replicated_entry` DOES encode it — it must be classified as a
        // write here despite the DDL-like permission tier.
        PhysicalPlan::Vector(VectorOp::SetParams { .. }) => true,

        // ---- Vector: reads, not encoded ----
        PhysicalPlan::Vector(
            VectorOp::Search { .. }
            | VectorOp::MultiSearch { .. }
            | VectorOp::QueryStats { .. }
            | VectorOp::SparseSearch { .. }
            | VectorOp::MultiVectorScoreSearch { .. },
        ) => false,

        // Buffered AND now encoded: `to_replicated_entry` used to have no
        // encoder arm for these (the same divergence pattern documented for
        // the remaining flipped variants in the module doc), but it now
        // encodes all six (`control/wal_replication/encode/vector.rs`), so
        // this is a plain oracle-matching write classification, not a
        // divergence — see `vector_variants_match_oracle` below.
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
        PhysicalPlan::Crdt(
            CrdtOp::Apply { .. }
            | CrdtOp::ImportSnapshot { .. }
            | CrdtOp::ListInsert { .. }
            | CrdtOp::ListDelete { .. }
            | CrdtOp::ListMove { .. }
            | CrdtOp::DocUpsert { .. }
            | CrdtOp::DocDelete { .. },
        ) => true,

        // ---- Crdt: reads, not encoded ----
        PhysicalPlan::Crdt(
            CrdtOp::Read { .. }
            | CrdtOp::ReadConstraints { .. }
            | CrdtOp::GetPolicy { .. }
            | CrdtOp::ReadAtVersion { .. }
            | CrdtOp::GetVersionVector { .. }
            | CrdtOp::ExportDelta { .. },
        ) => false,

        // Buffered: `to_replicated_entry` has no encoder arm for these (a
        // deliberate divergence from the oracle, see module doc), but each
        // reaches `exec_tx_passthrough`
        // (`data/executor/handlers/transaction/sub_plan.rs:286`) at COMMIT
        // with no reject arm. Buffering closes the prior atomicity gap
        // (statement used to execute immediately and survive ROLLBACK) at
        // the cost of RYOW loss + the no-undo gap (module doc). For
        // `SetConstraints` / `DropConstraints` specifically, RYOW loss is
        // more than cosmetic: a same-transaction `CrdtOp::Apply` validated
        // against `constraint_version_required` now validates against the
        // OLD installed constraint version until COMMIT installs the new one.
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
            GraphOp::Hop { .. }
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
        // `Expire` / `Persist` ARE encoded (buffered) today, but the staged
        // path's own executor (`execute_tx_kv`,
        // `data/executor/handlers/transaction/sub_plan_kv_ops.rs:50-60`)
        // rejects them inside a `TransactionBatch` — a COMMIT that replays a
        // buffered `Expire`/`Persist` fails there regardless of this
        // predicate. Not fixed here.
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
            | KvOp::MaterializeScan { .. },
        ) => false,

        // ---- Kv: index / DDL / truncate — encoded, but autocommit-only ----
        // A deliberate inverse divergence from the oracle: `to_replicated_entry`
        // encodes all three (they replicate normally when executed autocommit),
        // but transaction resolve rejects them outright
        // (`data/executor/handlers/transaction/resolve/entry.rs:329-334`: "kv
        // index/DDL/truncate op is not supported in transaction resolve"), so
        // they are never stageable into the overlay and never need buffering.
        // Pinned by `truncate_and_index_variants_are_encoded_but_not_buffered`.
        PhysicalPlan::Kv(
            KvOp::Truncate { .. } | KvOp::RegisterIndex { .. } | KvOp::DropIndex { .. },
        ) => false,

        // ---- Columnar: encoded (buffered) ----
        PhysicalPlan::Columnar(
            ColumnarOp::Insert { .. } | ColumnarOp::Delete { .. } | ColumnarOp::Update { .. },
        ) => true,
        // ---- Columnar: reads, not encoded ----
        PhysicalPlan::Columnar(ColumnarOp::Scan { .. } | ColumnarOp::MaterializeScan { .. }) => {
            false
        }

        // ---- Timeseries ----
        PhysicalPlan::Timeseries(TimeseriesOp::Ingest { .. }) => true,
        PhysicalPlan::Timeseries(TimeseriesOp::Scan { .. }) => false,

        // ---- Text: encoded (buffered) ----
        PhysicalPlan::Text(TextOp::FtsIndexDoc { .. } | TextOp::FtsDeleteDoc { .. }) => true,
        // ---- Text: reads / DDL, not encoded ----
        PhysicalPlan::Text(
            TextOp::Search { .. }
            | TextOp::BM25ScoreScan { .. }
            | TextOp::PhraseSearch { .. }
            | TextOp::HybridSearch { .. }
            | TextOp::HybridSearchTriple { .. }
            | TextOp::SetAnalyzer { .. },
        ) => false,

        // ---- Spatial: encoded (buffered) ----
        PhysicalPlan::Spatial(SpatialOp::Insert { .. } | SpatialOp::Delete { .. }) => true,
        // ---- Spatial: reads, not encoded ----
        PhysicalPlan::Spatial(SpatialOp::Scan { .. }) => false,

        // ---- Query: coordinator-side data movement / joins / aggregates.
        // `to_replicated_entry` never matches `PhysicalPlan::Query(_)` — every
        // variant falls to its `_ => None` tail, so every `QueryOp` is a read
        // here. None require Write permission, so there is no
        // write-but-unbuffered case in this engine.
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
            | QueryOp::LateralLoop { .. },
        ) => false,

        // ---- Meta: control / maintenance / internal-mechanism ops.
        // `to_replicated_entry` never matches `PhysicalPlan::Meta(_)` at all,
        // so every `MetaOp` is a read here — including the ones that require
        // Write permission (`WalAppend`, `TransactionBatch`, `PurgeTenant`,
        // `StageWrite`, `MarkSavepoint`, `RollbackToSavepoint`, `ResolveTxn`,
        // the `CalvinExecute*` / `CalvinFlush` / `CalvinDrop` / `CalvinResolve`
        // family, and `RecordCalvinWriteVersions`). None of these ever appear as a
        // client statement's `task.plan` at this call site — they are
        // internal orchestration plans the Calvin scheduler and the COMMIT
        // path dispatch directly — so this is today's behavior, not a gap
        // this predicate introduces.
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

        // ---- Array reads / DDL / Flush: `to_replicated_entry` returns `None`
        // for these (not a write), so they classify `false` — matching the
        // oracle.
        PhysicalPlan::Array(
            ArrayOp::OpenArray { .. }
            | ArrayOp::Slice { .. }
            | ArrayOp::Project { .. }
            | ArrayOp::Aggregate { .. }
            | ArrayOp::Elementwise { .. }
            | ArrayOp::Flush { .. }
            | ArrayOp::Compact { .. }
            | ArrayOp::SurrogateBitmapScan { .. }
            | ArrayOp::DropArray { .. },
        ) => false,
        // Buffered AND encoded — matches the oracle. `to_replicated_entry` now
        // emits the Raft-native `ArrayCellPut` / `ArrayCellDelete` for these
        // (`encode/entry_array.rs`), so they replicate to the shard's data
        // group. Buffering also closes the in-transaction atomicity gap
        // (statement used to execute immediately and survive ROLLBACK) at the
        // cost of RYOW loss + the no-undo gap (module doc).
        PhysicalPlan::Array(ArrayOp::Put { .. } | ArrayOp::Delete { .. }) => true,

        // ---- ClusterArray: coordinator-only, never dispatched to the Data
        // Plane and never touched by `to_replicated_entry`.
        PhysicalPlan::ClusterArray(ClusterArrayOp::Slice { .. } | ClusterArrayOp::Agg { .. }) => {
            false
        }
        // Write-but-unbuffered: Write permission, no encoder arm today, and
        // NOT flipped in this change. Unlike `ArrayOp`, `ClusterArrayOp` is a
        // Control-Plane-only construct (`control/cluster/array_cluster_exec/
        // executor.rs` RPC-fans-out to per-shard `ArrayOp`); the Data Plane's
        // `DataPlaneVisitor::cluster_array`
        // (`data/executor/dispatch/visitor.rs:82-84`) is a hard
        // `unreachable!()`. Buffering these would route them through
        // `MetaOp::TransactionBatch` -> `exec_tx_passthrough` ->
        // `self.execute(..)` at COMMIT, which reaches that `unreachable!()`
        // and panics. Left `false` deliberately.
        PhysicalPlan::ClusterArray(ClusterArrayOp::Put { .. } | ClusterArrayOp::Delete { .. }) => {
            false
        }
    }
}

/// Equivalence regression: `plan_requires_txn_buffering` must return exactly
/// what `to_replicated_entry(..).is_some()` returns, for a representative
/// instance of every leaf op variant across every engine EXCEPT the flipped
/// set documented in the module doc (those are pinned separately by
/// `flipped_variants_are_buffered_and_unencoded` below, via
/// `assert_buffered_but_unencoded`). This is the safety net for retiring the
/// ad-hoc oracle over the remaining variants — if a future edit to either
/// this predicate or the WAL encoder drifts the two apart, one of these
/// assertions fails.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::wal_replication::to_replicated_entry;
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
    use nodedb_types::{DatabaseId, Lsn, Surrogate, SystemTimeScope, TenantId, VectorAnnOptions};
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
            collection: String::new(),
            key: Vec::new(),
            rls_filters: Vec::new(),
            surrogate_ceiling: None,
        })
    }

    fn assert_matches_oracle(plan: &PhysicalPlan) {
        let expected = to_replicated_entry(tenant(), db(), vshard(), plan).is_some();
        let actual = plan_requires_txn_buffering(plan);
        assert_eq!(
            actual, expected,
            "plan_requires_txn_buffering disagrees with to_replicated_entry for {plan:?}"
        );
    }

    /// Pin the deliberate divergence for a flipped variant (module doc): the
    /// predicate now classifies it `true` (buffered — the atomicity fix),
    /// while `to_replicated_entry` still has no encoder arm for it and
    /// returns `None` (a separate, documented encoder omission, not fixed
    /// here). Asserting both makes the divergence from `assert_matches_oracle`
    /// intentional and visible instead of a silent drift.
    fn assert_buffered_but_unencoded(plan: &PhysicalPlan) {
        assert!(
            plan_requires_txn_buffering(plan),
            "expected {plan:?} to require txn buffering"
        );
        assert!(
            to_replicated_entry(tenant(), db(), vshard(), plan).is_none(),
            "expected {plan:?} to still have no WAL encoder arm (encoder omission is a separate, undone unit)"
        );
    }

    /// Pin the inverse divergence (module doc) for an autocommit-only
    /// Truncate variant: the predicate classifies it `false` (not
    /// buffered) because it is rejected inside an explicit transaction
    /// (`resolve/entry.rs`) and so never reaches this predicate for staging
    /// in practice, while `to_replicated_entry` returns `Some` because it
    /// replicates normally when executed autocommit. The inverse of
    /// `assert_buffered_but_unencoded`.
    fn assert_encoded_but_not_buffered(plan: &PhysicalPlan) {
        assert!(
            !plan_requires_txn_buffering(plan),
            "expected {plan:?} to not require txn buffering (autocommit-only)"
        );
        assert!(
            to_replicated_entry(tenant(), db(), vshard(), plan).is_some(),
            "expected {plan:?} to have a WAL encoder arm"
        );
    }

    #[test]
    fn document_variants_match_oracle() {
        let plans = vec![
            PhysicalPlan::Document(DocumentOp::PointGet {
                collection: "c".into(),
                document_id: "d".into(),
                surrogate: Surrogate::ZERO,
                pk_bytes: Vec::new(),
                rls_filters: Vec::new(),
                system_time: SystemTimeScope::Current,
                valid_at_ms: None,
            }),
            PhysicalPlan::Document(DocumentOp::PointPut {
                collection: "c".into(),
                document_id: "d".into(),
                value: Vec::new(),
                surrogate: Surrogate::ZERO,
                pk_bytes: Vec::new(),
            }),
            PhysicalPlan::Document(DocumentOp::PointInsert {
                collection: "c".into(),
                document_id: "d".into(),
                value: Vec::new(),
                if_absent: false,
                surrogate: Surrogate::ZERO,
            }),
            PhysicalPlan::Document(DocumentOp::PointDelete {
                collection: "c".into(),
                document_id: "d".into(),
                surrogate: Surrogate::ZERO,
                pk_bytes: Vec::new(),
                returning: None,
            }),
            PhysicalPlan::Document(DocumentOp::PointUpdate {
                collection: "c".into(),
                document_id: "d".into(),
                surrogate: Surrogate::ZERO,
                pk_bytes: Vec::new(),
                updates: Vec::new(),
                returning: None,
            }),
            PhysicalPlan::Document(DocumentOp::Scan {
                collection: "c".into(),
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
                collection: "c".into(),
                field: "f".into(),
                lower: None,
                upper: None,
                limit: 0,
            }),
            PhysicalPlan::Document(DocumentOp::Register {
                collection: "c".into(),
                indexes: Vec::new(),
                crdt_enabled: false,
                storage_mode: StorageMode::default(),
                enforcement: Box::new(EnforcementOptions::default()),
                bitemporal: false,
                conflict_policy: None,
            }),
            PhysicalPlan::Document(DocumentOp::IndexLookup {
                collection: "c".into(),
                path: "$.f".into(),
                value: "v".into(),
            }),
            PhysicalPlan::Document(DocumentOp::IndexedFetch {
                collection: "c".into(),
                path: "$.f".into(),
                value: "v".into(),
                filters: Vec::new(),
                projection: Vec::new(),
                limit: 0,
                offset: 0,
            }),
            PhysicalPlan::Document(DocumentOp::DropIndex {
                collection: "c".into(),
                field: "f".into(),
            }),
            PhysicalPlan::Document(DocumentOp::BackfillIndex {
                collection: "c".into(),
                path: "$.f".into(),
                is_array: false,
                unique: false,
                case_insensitive: false,
                predicate: None,
            }),
            PhysicalPlan::Document(DocumentOp::EstimateCount {
                collection: "c".into(),
                field: "f".into(),
            }),
            PhysicalPlan::Document(DocumentOp::InsertSelect {
                target_collection: "t".into(),
                source_collection: "s".into(),
                source_filters: Vec::new(),
                source_limit: 0,
            }),
            PhysicalPlan::Document(DocumentOp::Upsert {
                collection: "c".into(),
                document_id: "d".into(),
                value: Vec::new(),
                on_conflict_updates: Vec::new(),
                surrogate: Surrogate::ZERO,
            }),
            // BulkUpdate / BulkDelete: non-OLLP (both None) — the buffered case.
            PhysicalPlan::Document(DocumentOp::BulkUpdate {
                collection: "c".into(),
                filters: Vec::new(),
                updates: Vec::new(),
                returning: None,
                ollp_predicted_surrogates: None,
                ollp_predicted_edges: None,
            }),
            PhysicalPlan::Document(DocumentOp::BulkDelete {
                collection: "c".into(),
                filters: Vec::new(),
                returning: None,
                ollp_predicted_surrogates: None,
                ollp_predicted_edges: None,
            }),
            // BulkUpdate / BulkDelete: OLLP surrogate set present — the
            // Calvin-routed, not-buffered case.
            PhysicalPlan::Document(DocumentOp::BulkUpdate {
                collection: "c".into(),
                filters: Vec::new(),
                updates: Vec::new(),
                returning: None,
                ollp_predicted_surrogates: Some(vec![1, 2]),
                ollp_predicted_edges: None,
            }),
            PhysicalPlan::Document(DocumentOp::BulkDelete {
                collection: "c".into(),
                filters: Vec::new(),
                returning: None,
                ollp_predicted_surrogates: Some(vec![1, 2]),
                ollp_predicted_edges: None,
            }),
            // BulkUpdate / BulkDelete: OLLP edge set present, surrogates None —
            // the other half of the `Some` guard.
            PhysicalPlan::Document(DocumentOp::BulkUpdate {
                collection: "c".into(),
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
            }),
            PhysicalPlan::Document(DocumentOp::BulkDelete {
                collection: "c".into(),
                filters: Vec::new(),
                returning: None,
                ollp_predicted_surrogates: None,
                ollp_predicted_edges: Some(vec![OllpPredictedEdge {
                    surrogate: 1,
                    from: "a".into(),
                    to: "b".into(),
                    label: None,
                }]),
            }),
            PhysicalPlan::Document(DocumentOp::MaterializeScan {
                collection: "c".into(),
                cursor: Vec::new(),
                count: 0,
                system_as_of_ms: None,
            }),
            // `to_replicated_entry` now has an encoder arm for `BatchInsert`
            // (used to be in the flipped/unencoded exception list, see module
            // doc), so predicate `true` and encoder `Some` agree here.
            PhysicalPlan::Document(DocumentOp::BatchInsert {
                collection: "c".into(),
                documents: Vec::new(),
                surrogates: Vec::new(),
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
                collection: "c".into(),
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
                collection: "c".into(),
                vector: Vec::new(),
                dim: 0,
                field_name: String::new(),
                surrogate: Surrogate::ZERO,
                pk_bytes: None,
                provenance: None,
            }),
            PhysicalPlan::Vector(VectorOp::BatchInsert {
                collection: "c".into(),
                vectors: Vec::new(),
                dim: 0,
                surrogates: Vec::new(),
            }),
            PhysicalPlan::Vector(VectorOp::MultiSearch {
                collection: "c".into(),
                query_vector: Vec::new(),
                top_k: 0,
                ef_search: 0,
                filter_bitmap: None,
                rls_filters: Vec::new(),
            }),
            PhysicalPlan::Vector(VectorOp::Delete {
                collection: "c".into(),
                vector_id: 0,
            }),
            PhysicalPlan::Vector(VectorOp::SetParams {
                collection: "c".into(),
                field_name: String::new(),
                m: 0,
                ef_construction: 0,
                metric: String::new(),
                index_type: String::new(),
                pq_m: 0,
                ivf_cells: 0,
                ivf_nprobe: 0,
            }),
            PhysicalPlan::Vector(VectorOp::QueryStats {
                collection: "c".into(),
                field_name: String::new(),
            }),
            PhysicalPlan::Vector(VectorOp::Seal {
                collection: "c".into(),
                field_name: String::new(),
            }),
            PhysicalPlan::Vector(VectorOp::CompactIndex {
                collection: "c".into(),
                field_name: String::new(),
            }),
            PhysicalPlan::Vector(VectorOp::Rebuild {
                collection: "c".into(),
                field_name: String::new(),
                m: 0,
                m0: 0,
                ef_construction: 0,
            }),
            PhysicalPlan::Vector(VectorOp::SparseSearch {
                collection: "c".into(),
                field_name: String::new(),
                query_entries: Vec::new(),
                top_k: 0,
            }),
            PhysicalPlan::Vector(VectorOp::MultiVectorScoreSearch {
                collection: "c".into(),
                field_name: String::new(),
                query_vector: Vec::new(),
                top_k: 0,
                ef_search: 0,
                mode: String::new(),
            }),
            // These six used to have no `to_replicated_entry` encoder arm
            // (pinned separately by `flipped_variants_are_buffered_and_unencoded`);
            // now that the encoder covers them, predicate `true` and encoder
            // `Some` agree again, so they belong in the oracle-matching set.
            PhysicalPlan::Vector(VectorOp::DeleteBySurrogate {
                collection: "c".into(),
                surrogate: Surrogate::ZERO,
                field_name: String::new(),
                provenance: None,
            }),
            PhysicalPlan::Vector(VectorOp::SparseInsert {
                collection: "c".into(),
                field_name: String::new(),
                doc_id: "d".into(),
                entries: Vec::new(),
            }),
            PhysicalPlan::Vector(VectorOp::SparseDelete {
                collection: "c".into(),
                field_name: String::new(),
                doc_id: "d".into(),
            }),
            PhysicalPlan::Vector(VectorOp::MultiVectorInsert {
                collection: "c".into(),
                field_name: String::new(),
                document_surrogate: Surrogate::ZERO,
                vectors: Vec::new(),
                count: 0,
                dim: 0,
            }),
            PhysicalPlan::Vector(VectorOp::MultiVectorDelete {
                collection: "c".into(),
                field_name: String::new(),
                document_surrogate: Surrogate::ZERO,
            }),
            PhysicalPlan::Vector(VectorOp::DirectUpsert {
                collection: "c".into(),
                field: String::new(),
                surrogate: Surrogate::ZERO,
                vector: Vec::new(),
                payload: Vec::new(),
                quantization: Default::default(),
                storage_dtype: Default::default(),
                payload_indexes: Vec::new(),
            }),
        ];
        for p in &plans {
            assert_matches_oracle(p);
        }
    }

    #[test]
    fn crdt_variants_match_oracle() {
        let plans = vec![
            PhysicalPlan::Crdt(CrdtOp::Read {
                collection: "c".into(),
                document_id: "d".into(),
            }),
            PhysicalPlan::Crdt(CrdtOp::Apply {
                collection: "c".into(),
                document_id: "d".into(),
                delta: Vec::new(),
                peer_id: 0,
                mutation_id: 0,
                surrogate: Surrogate::ZERO,
                provenance: None,
                constraint_version_required: 0,
            }),
            PhysicalPlan::Crdt(CrdtOp::ImportSnapshot {
                tenant_id: 1,
                collection: "c".into(),
                bytes: Vec::new(),
            }),
            PhysicalPlan::Crdt(CrdtOp::ReadConstraints {
                collection: "c".into(),
            }),
            PhysicalPlan::Crdt(CrdtOp::SetPolicy {
                collection: "c".into(),
                policy_json: "{}".into(),
            }),
            PhysicalPlan::Crdt(CrdtOp::GetPolicy {
                collection: "c".into(),
            }),
            PhysicalPlan::Crdt(CrdtOp::ReadAtVersion {
                collection: "c".into(),
                document_id: "d".into(),
                version_vector_json: "{}".into(),
            }),
            PhysicalPlan::Crdt(CrdtOp::GetVersionVector {
                collection: "c".into(),
            }),
            PhysicalPlan::Crdt(CrdtOp::ExportDelta {
                collection: "c".into(),
                from_version_json: "{}".into(),
            }),
            PhysicalPlan::Crdt(CrdtOp::CompactAtVersion {
                collection: "c".into(),
                target_version_json: "{}".into(),
            }),
            PhysicalPlan::Crdt(CrdtOp::ListInsert {
                collection: "c".into(),
                document_id: "d".into(),
                list_path: "$.l".into(),
                index: 0,
                fields_json: "{}".into(),
                surrogate: Surrogate::ZERO,
            }),
            PhysicalPlan::Crdt(CrdtOp::ListDelete {
                collection: "c".into(),
                document_id: "d".into(),
                list_path: "$.l".into(),
                index: 0,
                surrogate: Surrogate::ZERO,
            }),
            PhysicalPlan::Crdt(CrdtOp::ListMove {
                collection: "c".into(),
                document_id: "d".into(),
                list_path: "$.l".into(),
                from_index: 0,
                to_index: 1,
                surrogate: Surrogate::ZERO,
            }),
            // `to_replicated_entry` now has encoder arms for `SetConstraints`
            // / `DropConstraints` (used to be in the flipped/unencoded
            // exception list, see module doc), so predicate `true` and
            // encoder `Some` agree here.
            PhysicalPlan::Crdt(CrdtOp::SetConstraints {
                collection: "c".into(),
                constraint_version: 0,
                constraints: Vec::new(),
            }),
            PhysicalPlan::Crdt(CrdtOp::DropConstraints {
                collection: "c".into(),
                constraint_version: 0,
            }),
            PhysicalPlan::Crdt(CrdtOp::DocUpsert {
                collection: "c".into(),
                document_id: "d".into(),
                fields_json: "{}".into(),
                surrogate: Surrogate::ZERO,
                partial: false,
                returning: None,
            }),
            PhysicalPlan::Crdt(CrdtOp::DocDelete {
                collection: "c".into(),
                document_id: "d".into(),
                surrogate: Surrogate::ZERO,
                returning: None,
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
                collection: "c".into(),
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
                collection: "c".into(),
                src_id: "a".into(),
                label: "L".into(),
                dst_id: "b".into(),
                src_surrogate: Surrogate::ZERO,
                dst_surrogate: Surrogate::ZERO,
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
            }),
            PhysicalPlan::Graph(GraphOp::Neighbors {
                node_id: "n".into(),
                edge_label: None,
                direction: Direction::Out,
                rls_filters: Vec::new(),
            }),
            PhysicalPlan::Graph(GraphOp::NeighborsMulti {
                node_ids: Vec::new(),
                edge_label: None,
                direction: Direction::Out,
                max_results: 0,
                rls_filters: Vec::new(),
            }),
            PhysicalPlan::Graph(GraphOp::Path {
                src: "a".into(),
                dst: "b".into(),
                edge_label: None,
                max_depth: 0,
                options: GraphTraversalOptions::default(),
                rls_filters: Vec::new(),
                frontier_bitmap: None,
            }),
            PhysicalPlan::Graph(GraphOp::Subgraph {
                start_nodes: Vec::new(),
                edge_label: None,
                depth: 0,
                options: GraphTraversalOptions::default(),
                rls_filters: Vec::new(),
            }),
            PhysicalPlan::Graph(GraphOp::RagFusion {
                collection: "c".into(),
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
                collection: "c".into(),
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
                collection: "c".into(),
                key: Vec::new(),
                rls_filters: Vec::new(),
                surrogate_ceiling: None,
            }),
            PhysicalPlan::Kv(KvOp::Put {
                collection: "c".into(),
                key: Vec::new(),
                value: Vec::new(),
                ttl_ms: 0,
                surrogate: Surrogate::ZERO,
            }),
            PhysicalPlan::Kv(KvOp::Insert {
                collection: "c".into(),
                key: Vec::new(),
                value: Vec::new(),
                ttl_ms: 0,
                surrogate: Surrogate::ZERO,
            }),
            PhysicalPlan::Kv(KvOp::InsertIfAbsent {
                collection: "c".into(),
                key: Vec::new(),
                value: Vec::new(),
                ttl_ms: 0,
                surrogate: Surrogate::ZERO,
            }),
            PhysicalPlan::Kv(KvOp::InsertOnConflictUpdate {
                collection: "c".into(),
                key: Vec::new(),
                value: Vec::new(),
                ttl_ms: 0,
                updates: Vec::new(),
                surrogate: Surrogate::ZERO,
            }),
            PhysicalPlan::Kv(KvOp::Delete {
                collection: "c".into(),
                keys: Vec::new(),
            }),
            PhysicalPlan::Kv(KvOp::Scan {
                collection: "c".into(),
                cursor: Vec::new(),
                count: 0,
                filters: Vec::new(),
                match_pattern: None,
                sort_keys: Vec::new(),
                surrogate_ceiling: None,
            }),
            PhysicalPlan::Kv(KvOp::Expire {
                collection: "c".into(),
                key: Vec::new(),
                ttl_ms: 0,
            }),
            PhysicalPlan::Kv(KvOp::Persist {
                collection: "c".into(),
                key: Vec::new(),
            }),
            PhysicalPlan::Kv(KvOp::GetTtl {
                collection: "c".into(),
                key: Vec::new(),
            }),
            PhysicalPlan::Kv(KvOp::BatchGet {
                collection: "c".into(),
                keys: Vec::new(),
            }),
            PhysicalPlan::Kv(KvOp::BatchPut {
                collection: "c".into(),
                entries: Vec::new(),
                ttl_ms: 0,
                surrogates: Vec::new(),
            }),
            PhysicalPlan::Kv(KvOp::FieldGet {
                collection: "c".into(),
                key: Vec::new(),
                fields: Vec::new(),
            }),
            PhysicalPlan::Kv(KvOp::FieldSet {
                collection: "c".into(),
                key: Vec::new(),
                updates: Vec::new(),
                surrogate: Surrogate::ZERO,
            }),
            PhysicalPlan::Kv(KvOp::Incr {
                collection: "c".into(),
                key: Vec::new(),
                delta: 0,
                ttl_ms: 0,
                surrogate: Surrogate::ZERO,
            }),
            PhysicalPlan::Kv(KvOp::IncrFloat {
                collection: "c".into(),
                key: Vec::new(),
                delta: 0.0,
                surrogate: Surrogate::ZERO,
            }),
            PhysicalPlan::Kv(KvOp::Cas {
                collection: "c".into(),
                key: Vec::new(),
                expected: Vec::new(),
                new_value: Vec::new(),
                surrogate: Surrogate::ZERO,
            }),
            PhysicalPlan::Kv(KvOp::GetSet {
                collection: "c".into(),
                key: Vec::new(),
                new_value: Vec::new(),
                surrogate: Surrogate::ZERO,
            }),
            PhysicalPlan::Kv(KvOp::Transfer {
                collection: "c".into(),
                source_key: Vec::new(),
                dest_key: Vec::new(),
                field: "f".into(),
                amount: 0.0,
                debit_surrogate: Surrogate::ZERO,
                credit_surrogate: Surrogate::ZERO,
            }),
            PhysicalPlan::Kv(KvOp::TransferItem {
                source_collection: "s".into(),
                dest_collection: "d".into(),
                item_key: Vec::new(),
                dest_key: Vec::new(),
                surrogate: Surrogate::ZERO,
            }),
            PhysicalPlan::Kv(KvOp::RegisterSortedIndex {
                collection: "c".into(),
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
                collection: "c".into(),
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
                collection: "c".into(),
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
                collection: "c".into(),
                payload: Vec::new(),
                format: "json".into(),
                intent: ColumnarInsertIntent::Insert,
                on_conflict_updates: Vec::new(),
                surrogates: Vec::new(),
                schema_bytes: Vec::new(),
                provenance: None,
                wal_lsn: None,
            }),
            PhysicalPlan::Columnar(ColumnarOp::Update {
                collection: "c".into(),
                filters: Vec::new(),
                updates: Vec::new(),
            }),
            PhysicalPlan::Columnar(ColumnarOp::Delete {
                collection: "c".into(),
                filters: Vec::new(),
            }),
            PhysicalPlan::Columnar(ColumnarOp::MaterializeScan {
                collection: "c".into(),
                cursor: Vec::new(),
                count: 0,
                system_as_of_ms: None,
            }),
            PhysicalPlan::Timeseries(TimeseriesOp::Scan {
                collection: "c".into(),
                time_range: (0, i64::MAX),
                projection: Vec::new(),
                limit: 0,
                filters: Vec::new(),
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
                collection: "c".into(),
                payload: Vec::new(),
                format: "ilp".into(),
                wal_lsn: None,
                surrogates: Vec::new(),
                provenance: None,
            }),
            PhysicalPlan::Spatial(SpatialOp::Insert {
                collection: "c".into(),
                field: "f".into(),
                surrogate: Surrogate::ZERO,
                geometry: Geometry::point(0.0, 0.0),
                provenance: None,
            }),
            PhysicalPlan::Spatial(SpatialOp::Delete {
                collection: "c".into(),
                field: "f".into(),
                surrogate: Surrogate::ZERO,
                provenance: None,
            }),
            PhysicalPlan::Spatial(SpatialOp::Scan {
                collection: "c".into(),
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
                collection: "c".into(),
                query: "q".into(),
                top_k: 0,
                fuzzy: false,
                prefilter: None,
                rls_filters: Vec::new(),
            }),
            PhysicalPlan::Text(TextOp::BM25ScoreScan {
                collection: "c".into(),
                query: "q".into(),
                score_alias: "s".into(),
                fuzzy: false,
            }),
            PhysicalPlan::Text(TextOp::PhraseSearch {
                collection: "c".into(),
                terms: Vec::new(),
                top_k: 0,
                prefilter: None,
            }),
            PhysicalPlan::Text(TextOp::HybridSearch {
                collection: "c".into(),
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
                collection: "c".into(),
                surrogate: Surrogate::ZERO,
                text: "t".into(),
                provenance: None,
            }),
            PhysicalPlan::Text(TextOp::FtsDeleteDoc {
                collection: "c".into(),
                surrogate: Surrogate::ZERO,
                provenance: None,
            }),
            PhysicalPlan::Text(TextOp::HybridSearchTriple {
                collection: "c".into(),
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
            PhysicalPlan::Text(TextOp::SetAnalyzer {
                collection: "c".into(),
                analyzer_name: "standard".into(),
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
                sort_keys: Vec::new(),
                limit: None,
                offset: 0,
                distinct: false,
            }),
            PhysicalPlan::Query(QueryOp::Aggregate {
                collection: "c".into(),
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
                collection: "c".into(),
                group_by: Vec::new(),
                aggregates: Vec::new(),
                filters: Vec::new(),
            }),
            PhysicalPlan::Query(QueryOp::PartialAggregateState {
                collection: "c".into(),
                input: None,
                group_by: Vec::new(),
                aggregates: Vec::new(),
                filters: Vec::new(),
            }),
            PhysicalPlan::Query(QueryOp::HashJoin {
                left_collection: "l".into(),
                right_collection: "r".into(),
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
                left_collection: "l".into(),
                right_collection: "r".into(),
                condition: Vec::new(),
                join_type: "inner".into(),
                limit: 0,
            }),
            PhysicalPlan::Query(QueryOp::SortMergeJoin {
                left_collection: "l".into(),
                right_collection: "r".into(),
                on: Vec::new(),
                join_type: "inner".into(),
                limit: 0,
                pre_sorted: false,
            }),
            PhysicalPlan::Query(QueryOp::FacetCounts {
                collection: "c".into(),
                filters: Vec::new(),
                fields: Vec::new(),
                limit_per_facet: 0,
            }),
            PhysicalPlan::Query(QueryOp::RecursiveScan {
                collection: "c".into(),
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
                inner_collection: "i".into(),
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
                inner_collection: "i".into(),
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
                collection: "c".into(),
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
                collection: "c".into(),
                max_age_ms: 0,
            }),
            PhysicalPlan::Meta(MetaOp::TemporalPurgeEdgeStore {
                tenant_id: 1,
                collection: "c".into(),
                cutoff_system_ms: 0,
            }),
            PhysicalPlan::Meta(MetaOp::TemporalPurgeDocumentStrict {
                tenant_id: 1,
                collection: "c".into(),
                cutoff_system_ms: 0,
            }),
            PhysicalPlan::Meta(MetaOp::TemporalPurgeColumnar {
                tenant_id: 1,
                collection: "c".into(),
                cutoff_system_ms: 0,
            }),
            PhysicalPlan::Meta(MetaOp::TemporalPurgeCrdt {
                tenant_id: 1,
                collection: "c".into(),
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
                collection: "c".into(),
            }),
            PhysicalPlan::Meta(MetaOp::QueryLastValue {
                collection: "c".into(),
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
                    collection: "c".into(),
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
                        collection: "c".into(),
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
                collection: "c".into(),
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
                old_collection: "old".into(),
                new_collection: "new".into(),
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
            // `ArrayOp::Put` / `Delete` now MATCH the oracle: both are buffered
            // (`plan_requires_txn_buffering == true`, the atomicity fix) AND
            // encoded (`to_replicated_entry` returns `Some` — the Raft-native
            // `ArrayCellPut` / `ArrayCellDelete` cluster-write path), so they
            // belong here rather than in `flipped_variants_are_buffered_and_unencoded`.
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
            // `ClusterArrayOp::Put` / `Delete` are NOT flipped (see their arm
            // above): buffering them would panic at COMMIT via
            // `DataPlaneVisitor::cluster_array`'s `unreachable!()`. They stay
            // in the equivalence set, both sides `false`.
            PhysicalPlan::ClusterArray(ClusterArrayOp::Put {
                array_id: array_id.clone(),
                array_id_msgpack: Vec::new(),
                cells: Vec::new(),
                wal_lsn: 0,
                prefix_bits: 0,
            }),
            PhysicalPlan::ClusterArray(ClusterArrayOp::Delete {
                array_id: array_id.clone(),
                array_id_msgpack: Vec::new(),
                coords: Vec::new(),
                wal_lsn: 0,
                prefix_bits: 0,
            }),
        ];
        for p in &plans {
            assert_matches_oracle(p);
        }
    }

    /// Pin the deliberate oracle divergence (module doc) for every remaining
    /// flipped variant across the three still-affected engines (Document,
    /// Crdt, Array): each classifies `true` (buffered — closes the
    /// atomicity gap) while `to_replicated_entry` still has no encoder arm
    /// and returns `None` (a separate, undone encoder-omission unit).
    /// `VectorOp`'s six formerly-flipped variants are no longer here — see
    /// `vector_variants_match_oracle`. `DocumentOp::BatchInsert` and
    /// `CrdtOp::{SetConstraints, DropConstraints}` are no longer here either
    /// — see `document_variants_match_oracle` / `crdt_variants_match_oracle`.
    #[test]
    fn flipped_variants_are_buffered_and_unencoded() {
        let plans = vec![
            PhysicalPlan::Document(DocumentOp::Merge {
                target_collection: "t".into(),
                source_collection: "s".into(),
                source_alias: "s".into(),
                target_join_col: "id".into(),
                source_join_col: "id".into(),
                clauses: Vec::new(),
                returning: None,
                resolve_only: false,
                resolved_inserts: None,
                source_rows: None,
            }),
            PhysicalPlan::Document(DocumentOp::UpdateFromJoin {
                target_collection: "t".into(),
                source_collection: "s".into(),
                source_alias: "s".into(),
                target_join_col: "id".into(),
                source_join_col: "id".into(),
                updates: Vec::new(),
                target_filters: Vec::new(),
                returning: None,
                resolve_only: false,
                source_rows: None,
            }),
            PhysicalPlan::Crdt(CrdtOp::RestoreToVersion {
                collection: "c".into(),
                document_id: "d".into(),
                target_version_json: "{}".into(),
                surrogate: Surrogate::ZERO,
            }),
        ];
        for p in &plans {
            assert_buffered_but_unencoded(p);
        }
    }

    /// Pin the inverse divergence (module doc): truncate and KV index/DDL ops
    /// are autocommit-only — transaction resolve rejects them outright
    /// (`data/executor/handlers/transaction/resolve/entry.rs:329-334` for the
    /// KV arm, `:394-400` for the document arm), so
    /// `plan_requires_txn_buffering` never needs to buffer them — while
    /// `to_replicated_entry` encodes them all, since they replicate normally
    /// when executed autocommit. The inverse of
    /// `flipped_variants_are_buffered_and_unencoded`.
    ///
    /// These are deliberately excluded from `kv_variants_match_oracle`: they
    /// are the KV variants that legitimately disagree with the oracle, and
    /// pinning them here keeps that disagreement explicit rather than letting
    /// the equivalence test fail on an intentional divergence.
    #[test]
    fn truncate_and_index_variants_are_encoded_but_not_buffered() {
        let plans = vec![
            PhysicalPlan::Document(DocumentOp::Truncate {
                collection: "c".into(),
                restart_identity: false,
            }),
            PhysicalPlan::Kv(KvOp::Truncate {
                collection: "c".into(),
            }),
            PhysicalPlan::Kv(KvOp::RegisterIndex {
                collection: "c".into(),
                field: "f".into(),
                field_position: 0,
                backfill: false,
            }),
            PhysicalPlan::Kv(KvOp::DropIndex {
                collection: "c".into(),
                field: "f".into(),
            }),
        ];
        for p in &plans {
            assert_encoded_but_not_buffered(p);
        }
    }
}
