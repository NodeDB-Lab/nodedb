// SPDX-License-Identifier: BUSL-1.1

//! Records the per-core last-write-LSN version for every key a committed
//! transaction batch wrote. Runs once, after commit, over the buffered
//! sub-plans, covering both the fast-path commit and every Calvin apply.
//! One WAL LSN applies to every key in the batch.

use crate::bridge::envelope::PhysicalPlan;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::write_index::KeyRepr;
use crate::data::executor::task::ExecutionTask;
use crate::types::{Lsn, TenantId};
use nodedb_physical::physical_plan::{
    ColumnarOp, CrdtOp, DocumentOp, GraphOp, SpatialOp, TextOp, TimeseriesOp, VectorOp,
    VectorWriteTargets,
};
use nodedb_types::Surrogate;

impl CoreLoop {
    /// Record the version of every key written by a committed transaction
    /// batch. No-op with no WAL LSN. Per-key engines record `KeyRepr`;
    /// engines with internal per-key identity record only the collection floor.
    pub(in crate::data::executor) fn record_batch_write_versions(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        plans: &[PhysicalPlan],
    ) {
        let Some(lsn) = task.wal_lsn() else {
            return;
        };
        let db = task.request.database_id;
        let tenant = TenantId::new(tid);
        for plan in plans {
            self.record_plan_write_version(db, tenant, plan, lsn);
        }
    }

    fn record_plan_write_version(
        &mut self,
        db: crate::types::DatabaseId,
        tenant: TenantId,
        plan: &PhysicalPlan,
        lsn: Lsn,
    ) {
        match plan {
            PhysicalPlan::Document(op) => self.record_document_version(db, tenant, op, lsn),
            PhysicalPlan::Vector(op) => self.record_vector_version(db, tenant, op, lsn),
            PhysicalPlan::Graph(op) => self.record_graph_version(db, tenant, op, lsn),
            PhysicalPlan::Kv(op) => self.record_kv_version(db, tenant, op, lsn),
            // Collection-floor engines: per-key identity is engine-internal.
            PhysicalPlan::Columnar(op) => {
                let coll = match op {
                    ColumnarOp::Insert { collection, .. }
                    | ColumnarOp::Update { collection, .. }
                    | ColumnarOp::Delete { collection, .. }
                    | ColumnarOp::ResolvedUpdate { collection, .. }
                    | ColumnarOp::ResolvedDelete { collection, .. } => Some(collection.as_str()),
                    // Read-only: nothing written, no version to record.
                    ColumnarOp::Scan { .. } | ColumnarOp::MaterializeScan { .. } => None,
                    // Resolve pass reads what a governed write depends on;
                    // the write itself lands as ResolvedUpdate/ResolvedDelete.
                    ColumnarOp::ResolveDml { .. } => None,
                };
                if let Some(c) = coll {
                    self.note_write_lsn(db, tenant, c, None, lsn);
                }
            }
            PhysicalPlan::Timeseries(TimeseriesOp::Ingest { collection, .. }) => {
                self.note_write_lsn(db, tenant, collection.as_str(), None, lsn);
            }
            PhysicalPlan::Spatial(op) => {
                let coll = match op {
                    SpatialOp::Insert { collection, .. } | SpatialOp::Delete { collection, .. } => {
                        Some(collection.as_str())
                    }
                    // Read-only: nothing written, no version to record.
                    SpatialOp::Scan { .. } => None,
                };
                if let Some(c) = coll {
                    self.note_write_lsn(db, tenant, c, None, lsn);
                }
            }
            PhysicalPlan::Text(op) => {
                let coll = match op {
                    TextOp::FtsIndexDoc { collection, .. }
                    | TextOp::FtsDeleteDoc { collection, .. } => Some(collection.as_str()),
                    // Read-only queries: nothing written, no version to record.
                    TextOp::Search { .. }
                    | TextOp::BM25ScoreScan { .. }
                    | TextOp::PhraseSearch { .. }
                    | TextOp::HybridSearch { .. }
                    | TextOp::HybridSearchTriple { .. } => None,
                    // Analyzer/config DDL: no key written.
                    TextOp::SetTextConfig { .. } => None,
                };
                if let Some(c) = coll {
                    self.note_write_lsn(db, tenant, c, None, lsn);
                }
            }
            PhysicalPlan::Crdt(CrdtOp::Apply { collection, .. })
            | PhysicalPlan::Crdt(CrdtOp::DocUpsert { collection, .. })
            | PhysicalPlan::Crdt(CrdtOp::DocDelete { collection, .. }) => {
                self.note_write_lsn(db, tenant, collection.as_str(), None, lsn);
            }
            // No per-key/collection version recorded for reads, control ops, or
            // engines not (yet) part of the funnel (array is keyed by tile).
            PhysicalPlan::Timeseries(_)
            | PhysicalPlan::Crdt(_)
            | PhysicalPlan::Query(_)
            | PhysicalPlan::Meta(_)
            | PhysicalPlan::Array(_)
            | PhysicalPlan::ClusterArray(_)
            | PhysicalPlan::ClusterEvent(_) => {}
        }
    }

    fn record_document_version(
        &mut self,
        db: crate::types::DatabaseId,
        tenant: TenantId,
        op: &DocumentOp,
        lsn: Lsn,
    ) {
        let (collection, surrogate) = match op {
            DocumentOp::PointPut {
                collection,
                surrogate,
                ..
            }
            | DocumentOp::PointInsert {
                collection,
                surrogate,
                ..
            }
            | DocumentOp::PointDelete {
                collection,
                surrogate,
                ..
            } => (collection.as_str(), *surrogate),
            // Whole-collection mutation: key set is every row, so only the
            // collection floor applies.
            DocumentOp::Truncate { collection, .. } => {
                self.note_write_lsn(db, tenant, collection.as_str(), None, lsn);
                return;
            }
            // No per-key version recorded: reads, DDL/index ops, and
            // multi-row ops that carry no single surrogate.
            DocumentOp::PointGet { .. }
            | DocumentOp::PointUpdate { .. }
            | DocumentOp::Scan { .. }
            | DocumentOp::BatchInsert { .. }
            | DocumentOp::RangeScan { .. }
            | DocumentOp::Register { .. }
            | DocumentOp::IndexLookup { .. }
            | DocumentOp::IndexedFetch { .. }
            | DocumentOp::DropIndex { .. }
            | DocumentOp::BackfillIndex { .. }
            | DocumentOp::EstimateCount { .. }
            | DocumentOp::InsertSelect { .. }
            | DocumentOp::Upsert { .. }
            | DocumentOp::UpdateFromJoin { .. }
            | DocumentOp::BulkUpdate { .. }
            | DocumentOp::BulkDelete { .. }
            | DocumentOp::Merge { .. }
            | DocumentOp::MaterializeScan { .. }
            | DocumentOp::ApplyBalanceDelta { .. }
            | DocumentOp::ResolveWrite(_)
            | DocumentOp::ResolvedWrite { .. } => return,
        };
        self.note_write_lsn(
            db,
            tenant,
            collection,
            Some(KeyRepr::Surrogate(surrogate.as_u32())),
            lsn,
        );
    }

    /// Records the version of a vector-engine write within a committed
    /// transaction batch. Surrogate-carrying ops record `KeyRepr::Surrogate`
    /// per surrogate; sparse ops (`doc_id`-keyed) and `Delete` (`vector_id`
    /// is the internal HNSW node id, not the surrogate) record the
    /// collection floor only.
    fn record_vector_version(
        &mut self,
        db: crate::types::DatabaseId,
        tenant: TenantId,
        op: &VectorOp,
        lsn: Lsn,
    ) {
        match op {
            VectorOp::Insert {
                collection,
                surrogate,
                ..
            }
            | VectorOp::DirectUpsert {
                collection,
                surrogate,
                ..
            }
            | VectorOp::DirectInsert {
                collection,
                surrogate,
                ..
            }
            | VectorOp::DirectInsertIfAbsent {
                collection,
                surrogate,
                ..
            }
            | VectorOp::DeleteBySurrogate {
                collection,
                surrogate,
                ..
            } => {
                self.note_write_lsn(
                    db,
                    tenant,
                    collection.as_str(),
                    Some(KeyRepr::Surrogate(surrogate.as_u32())),
                    lsn,
                );
            }
            VectorOp::MultiVectorInsert {
                collection,
                document_surrogate,
                ..
            }
            | VectorOp::MultiVectorDelete {
                collection,
                document_surrogate,
                ..
            } => {
                self.note_write_lsn(
                    db,
                    tenant,
                    collection.as_str(),
                    Some(KeyRepr::Surrogate(document_surrogate.as_u32())),
                    lsn,
                );
            }
            VectorOp::BatchInsert {
                collection,
                surrogates,
                ..
            } => {
                let mut any_surrogate_recorded = false;
                for s in surrogates {
                    if *s != Surrogate::ZERO {
                        self.note_write_lsn(
                            db,
                            tenant,
                            collection.as_str(),
                            Some(KeyRepr::Surrogate(s.as_u32())),
                            lsn,
                        );
                        any_surrogate_recorded = true;
                    }
                }
                if !any_surrogate_recorded {
                    self.note_write_lsn(db, tenant, collection.as_str(), None, lsn);
                }
            }
            // A point-targeted delete or update names its surrogates; a
            // predicate-targeted one resolves them on apply, so it records the
            // collection floor only.
            VectorOp::DirectDelete {
                collection,
                targets,
                ..
            }
            | VectorOp::DirectUpdate {
                collection,
                targets,
                ..
            } => match targets {
                VectorWriteTargets::Surrogates(surrogates) if !surrogates.is_empty() => {
                    for s in surrogates {
                        self.note_write_lsn(
                            db,
                            tenant,
                            collection.as_str(),
                            Some(KeyRepr::Surrogate(s.as_u32())),
                            lsn,
                        );
                    }
                }
                VectorWriteTargets::Surrogates(_) | VectorWriteTargets::Predicate(_) => {
                    self.note_write_lsn(db, tenant, collection.as_str(), None, lsn);
                }
            },
            // Every resolved mutation names its surrogate.
            VectorOp::ResolvedDirectWrite {
                collection,
                mutations,
                ..
            } => {
                for mutation in mutations {
                    self.note_write_lsn(
                        db,
                        tenant,
                        collection.as_str(),
                        Some(KeyRepr::Surrogate(mutation.surrogate().as_u32())),
                        lsn,
                    );
                }
                if mutations.is_empty() {
                    self.note_write_lsn(db, tenant, collection.as_str(), None, lsn);
                }
            }
            // Sparse (doc_id-keyed) and `Delete` (vector_id isn't the
            // surrogate): collection floor only.
            VectorOp::SparseInsert { collection, .. }
            | VectorOp::SparseDelete { collection, .. }
            | VectorOp::Delete { collection, .. } => {
                self.note_write_lsn(db, tenant, collection.as_str(), None, lsn);
            }
            // Read / config / query ops: nothing written, no version to record.
            // The resolve pass reads what a governed write depends on.
            VectorOp::ResolveDirectWrite(_)
            | VectorOp::Search { .. }
            | VectorOp::MultiSearch { .. }
            | VectorOp::SetParams { .. }
            | VectorOp::DropIndex { .. }
            | VectorOp::QueryStats { .. }
            | VectorOp::Seal { .. }
            | VectorOp::CompactIndex { .. }
            | VectorOp::Rebuild { .. }
            | VectorOp::SparseSearch { .. }
            | VectorOp::MultiVectorScoreSearch { .. } => {}
        }
    }

    fn record_graph_version(
        &mut self,
        db: crate::types::DatabaseId,
        tenant: TenantId,
        op: &GraphOp,
        lsn: Lsn,
    ) {
        match op {
            GraphOp::EdgePut {
                collection,
                src_id,
                label,
                dst_id,
                ..
            }
            | GraphOp::EdgeDelete {
                collection,
                src_id,
                label,
                dst_id,
                ..
            } => {
                self.note_write_lsn(
                    db,
                    tenant,
                    collection.as_str(),
                    Some(KeyRepr::Edge {
                        src: Box::from(src_id.as_str()),
                        label: Box::from(label.as_str()),
                        dst: Box::from(dst_id.as_str()),
                    }),
                    lsn,
                );
            }
            GraphOp::EdgePutBatch { edges } | GraphOp::EdgeDeleteBatch { edges } => {
                for edge in edges {
                    self.note_write_lsn(
                        db,
                        tenant,
                        edge.collection.as_str(),
                        Some(KeyRepr::Edge {
                            src: Box::from(edge.src_id.as_str()),
                            label: Box::from(edge.label.as_str()),
                            dst: Box::from(edge.dst_id.as_str()),
                        }),
                        lsn,
                    );
                }
            }
            // Resolve pass reads what a governed write depends on; the
            // decided delete lands as EdgeDelete once proposed.
            GraphOp::ResolveEdgeDelete(_) => {}
            // Node-label mutations carry no collection/surrogate context, so
            // no key version is recorded for them.
            GraphOp::SetNodeLabels { .. } | GraphOp::RemoveNodeLabels { .. } => {}
            // Read-only traversals, pattern matches, and stats: nothing
            // written, no version to record.
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
            | GraphOp::TemporalNeighbors { .. }
            | GraphOp::TemporalAlgorithm { .. }
            | GraphOp::Stats { .. } => {}
            // Distributed algorithm supersteps: compute passes over an
            // in-memory CSR snapshot, no key written.
            GraphOp::BspSuperstep(_) | GraphOp::WccSuperstep(_) => {}
        }
    }
}
