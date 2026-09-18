// SPDX-License-Identifier: BUSL-1.1

//! Deciding whether a planned DML task can carry a `RETURNING` spec, and
//! attaching it to the plan variants that can.
//!
//! Every engine carries the clause on its insert op — document (schemaless
//! and strict), key-value, columnar, spatial, timeseries, and vector-primary
//! each own a `returning` slot paired with an `rls_filters` read gate, so the
//! statement returns the stored post-image bounded by the read policy. The
//! one plan shape with nowhere to carry it is `INSERT ... SELECT`, refused
//! rather than silently dropped.

use nodedb_physical::physical_plan::{
    ArrayOp, ClusterArrayOp, ClusterEventOp, ColumnarOp, CrdtOp, DocumentOp, GraphOp, KvOp, MetaOp,
    QueryOp, ReturningSpec, SpatialOp, TextOp, TimeseriesOp, VectorOp,
};
use nodedb_physical::physical_task::PhysicalTask;

use crate::Error;
use crate::bridge::envelope::PhysicalPlan;

/// Attach `spec` to every planned task, refusing any insert shape that has
/// nowhere to carry it rather than dropping the clause in silence.
pub fn attach_returning_spec(
    tasks: &mut [PhysicalTask],
    spec: &ReturningSpec,
) -> crate::Result<()> {
    for task in tasks.iter_mut() {
        refuse_unprojectable_insert_returning(&task.plan)?;
        inject_returning_spec(&mut task.plan, spec.clone());
    }
    Ok(())
}

/// Refuse an `INSERT ... RETURNING` whose plan SHAPE has nowhere to carry the
/// clause.
///
/// Every engine carries it on its insert op — document (schemaless and
/// strict), key-value, columnar, spatial, timeseries, and vector-primary each
/// own a `returning` slot paired with an `rls_filters` read gate, so the
/// statement returns the STORED post-image bounded by the read policy. What
/// remains here is not an engine gap but a plan-shape one: `INSERT ... SELECT`
/// never reaches the Data Plane as a single insert op, so there is no slot on
/// it for the clause to ride in, whatever engine it targets.
///
/// Refusing is the honest answer. Silently dropping the clause answers a
/// statement that asked for rows with a bare command tag, and nothing anywhere
/// says the request was discarded.
///
/// Runs against the plan rather than the statement text: the expansion that
/// removes the slot is a planning decision, not a syntactic one.
pub fn refuse_unprojectable_insert_returning(plan: &PhysicalPlan) -> Result<(), Error> {
    let unsupported = match plan {
        // `INSERT ... SELECT` never reaches the Data Plane as this op: it is
        // expanded on the Control Plane into fresh-surrogate insert tasks whose
        // rows the expander, not the plan, decides — so there is no slot on
        // this plan for the clause to ride in.
        PhysicalPlan::Document(DocumentOp::InsertSelect { .. }) => "INSERT ... SELECT",
        // Exchange wraps an unresolved child; judge the child.
        PhysicalPlan::Query(QueryOp::Exchange(op)) => {
            return refuse_unprojectable_insert_returning(&op.child);
        }
        // Everything else either carries the clause already or is not an
        // insert. Enumerated per engine rather than via a catch-all so a new
        // `PhysicalPlan` variant forces a decision instead of silently
        // inheriting "supported" and dropping the clause.
        PhysicalPlan::Document(_)
        | PhysicalPlan::Kv(_)
        | PhysicalPlan::Vector(_)
        | PhysicalPlan::Graph(_)
        | PhysicalPlan::Text(_)
        | PhysicalPlan::Columnar(_)
        | PhysicalPlan::Timeseries(_)
        | PhysicalPlan::Spatial(_)
        | PhysicalPlan::Crdt(_)
        | PhysicalPlan::Query(_)
        | PhysicalPlan::Meta(_)
        | PhysicalPlan::Array(_)
        | PhysicalPlan::ClusterArray(_)
        | PhysicalPlan::ClusterEvent(_) => return Ok(()),
    };
    Err(Error::BadRequest {
        detail: format!(
            "RETURNING is not supported on {unsupported}; it is supported on every engine's \
             direct INSERT — document collections (schemaless and strict), key-value, \
             columnar, spatial, timeseries, and vector-primary collections — and on UPDATE, \
             DELETE, and MERGE. Follow the insert with a SELECT on the inserted key to read \
             the stored rows."
        ),
    })
}

/// The error a row-returning write must fail with when an open transaction
/// buffers or stages it instead of executing it.
///
/// Both in-transaction routes are structurally unable to answer the clause, and
/// for different reasons — which is why this refuses rather than returning an
/// empty row set:
///
/// - A **buffered** write performs no engine work at all until COMMIT, so at
///   statement time there is no stored row to project. Nothing could be
///   returned however the response were shaped.
/// - A **staged** write does touch the transaction overlay, but every staging
///   handler answers with an affected-count payload; the one payload-bearing
///   staged outcome is reserved for the atomic key-value ops that compute a
///   value. No staged write carries a row image back.
///
/// COMMIT then answers with a single tag for the whole transaction, so the rows
/// cannot be surfaced later either. Reporting success with no rows is the exact
/// silence this clause exists to remove, so the statement is refused and says
/// which limitation it hit. Verb-agnostic on purpose: it fires for any plan the
/// shaper classifies as row-returning, so INSERT, UPSERT, UPDATE, DELETE and
/// MERGE all behave identically inside a transaction.
pub fn in_transaction_returning_unsupported() -> Error {
    Error::BadRequest {
        detail: "RETURNING is not supported inside an explicit transaction: the write is staged \
                 or buffered until COMMIT, so it has no stored row to project at this point. Run \
                 the statement in autocommit, or follow the write with a SELECT after COMMIT."
            .to_string(),
    }
}

/// Inject a RETURNING spec into a DML physical plan variant.
///
/// Only `PointInsert`, `PointPut`, `BatchInsert`, `Upsert`, `PointUpdate`,
/// `BulkUpdate`, `PointDelete`, `BulkDelete`, `UpdateFromJoin`, `Merge`, the KV
/// `Insert` / `InsertIfAbsent` / `InsertOnConflictUpdate` / `Put` / `BatchPut`
/// / `FieldSet` / `PredicateUpdate` / `Delete` / `PredicateDelete` ops, the
/// columnar `Insert`, the timeseries `Ingest`, the vector `DirectUpsert`, and
/// the CRDT `DocUpsert` / `DocDelete` ops are affected. Every other variant is
/// left unchanged — an insert shape among them has already been refused by
/// [`refuse_unprojectable_insert_returning`], which runs first.
pub fn inject_returning_spec(plan: &mut PhysicalPlan, spec: ReturningSpec) {
    match plan {
        PhysicalPlan::Document(DocumentOp::PointInsert { returning, .. }) => {
            *returning = Some(spec);
        }
        PhysicalPlan::Kv(KvOp::Insert { returning, .. }) => {
            *returning = Some(spec);
        }
        PhysicalPlan::Kv(KvOp::InsertIfAbsent { returning, .. }) => {
            *returning = Some(spec);
        }
        PhysicalPlan::Kv(KvOp::InsertOnConflictUpdate { returning, .. }) => {
            *returning = Some(spec);
        }
        PhysicalPlan::Kv(KvOp::Put { returning, .. }) => {
            *returning = Some(spec);
        }
        PhysicalPlan::Kv(KvOp::BatchPut { returning, .. }) => {
            *returning = Some(spec);
        }
        PhysicalPlan::Kv(KvOp::FieldSet { returning, .. }) => {
            *returning = Some(spec);
        }
        PhysicalPlan::Kv(KvOp::PredicateUpdate { returning, .. }) => {
            *returning = Some(spec);
        }
        PhysicalPlan::Kv(KvOp::Delete { returning, .. }) => {
            *returning = Some(spec);
        }
        PhysicalPlan::Kv(KvOp::PredicateDelete { returning, .. }) => {
            *returning = Some(spec);
        }
        PhysicalPlan::Columnar(ColumnarOp::Insert { returning, .. }) => {
            *returning = Some(spec);
        }
        PhysicalPlan::Timeseries(TimeseriesOp::Ingest { returning, .. }) => {
            *returning = Some(spec);
        }
        PhysicalPlan::Vector(VectorOp::DirectUpsert { returning, .. }) => {
            *returning = Some(spec);
        }
        PhysicalPlan::Document(DocumentOp::PointPut { returning, .. }) => {
            *returning = Some(spec);
        }
        PhysicalPlan::Document(DocumentOp::BatchInsert { returning, .. }) => {
            *returning = Some(spec);
        }
        PhysicalPlan::Document(DocumentOp::Upsert { returning, .. }) => {
            *returning = Some(spec);
        }
        PhysicalPlan::Document(DocumentOp::PointUpdate { returning, .. }) => {
            *returning = Some(spec);
        }
        PhysicalPlan::Document(DocumentOp::BulkUpdate { returning, .. }) => {
            *returning = Some(spec);
        }
        PhysicalPlan::Document(DocumentOp::PointDelete { returning, .. }) => {
            *returning = Some(spec);
        }
        PhysicalPlan::Document(DocumentOp::BulkDelete { returning, .. }) => {
            *returning = Some(spec);
        }
        PhysicalPlan::Document(DocumentOp::UpdateFromJoin { returning, .. }) => {
            *returning = Some(spec);
        }
        PhysicalPlan::Document(DocumentOp::Merge { returning, .. }) => {
            *returning = Some(spec);
        }
        PhysicalPlan::Crdt(CrdtOp::DocUpsert { returning, .. }) => {
            *returning = Some(spec);
        }
        PhysicalPlan::Crdt(CrdtOp::DocDelete { returning, .. }) => {
            *returning = Some(spec);
        }
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
            | DocumentOp::InsertSelect { .. }
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
            | VectorOp::MultiVectorScoreSearch { .. },
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
            | GraphOp::TemporalAlgorithm { .. }
            | GraphOp::Stats { .. }
            | GraphOp::ResolveEdgeDelete(_)
            | GraphOp::BspSuperstep(_)
            | GraphOp::WccSuperstep(_),
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
            | ColumnarOp::MaterializeScan { .. },
        )
        | PhysicalPlan::Timeseries(TimeseriesOp::Scan { .. } | TimeseriesOp::ResolveIngest(_))
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
            | MetaOp::ResolveTxn { .. }
            | MetaOp::CalvinResolve { .. },
        )
        | PhysicalPlan::Array(
            ArrayOp::OpenArray { .. }
            | ArrayOp::Put { .. }
            | ArrayOp::Delete { .. }
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
        )
        | PhysicalPlan::ClusterArray(
            ClusterArrayOp::Slice { .. }
            | ClusterArrayOp::Agg { .. }
            | ClusterArrayOp::Put { .. }
            | ClusterArrayOp::Delete { .. },
        )
        | PhysicalPlan::ClusterEvent(
            ClusterEventOp::ConsumeStream { .. } | ClusterEventOp::PublishTopic { .. },
        ) => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::{DatabaseId, QualifiedCollection};

    /// An insert shape with no `returning` slot is refused at the plan, naming
    /// the shape and where the clause IS honored. Silently dropping it left
    /// the caller with a command tag for a statement that asked for rows.
    #[test]
    fn an_insert_plan_with_no_returning_slot_is_refused() {
        let plan = PhysicalPlan::Document(DocumentOp::InsertSelect {
            target_collection: QualifiedCollection::new(DatabaseId::DEFAULT, "dst"),
            source_collection: QualifiedCollection::new(DatabaseId::DEFAULT, "src"),
            source_filters: Vec::new(),
            source_limit: 0,
            column_map: Vec::new(),
        });
        let detail = refuse_unprojectable_insert_returning(&plan)
            .expect_err("an INSERT ... SELECT cannot carry the clause")
            .to_string();
        assert!(
            detail.contains("INSERT ... SELECT") && detail.contains("document"),
            "the refusal must name the plan shape and where it IS supported; got {detail}"
        );
    }

    /// A vector-primary upsert carries the clause, so the same gate admits
    /// it. Pinned beside the refusal above for the same reason the columnar and
    /// timeseries cases are: an engine dropped from the refusal without gaining
    /// the slot silently drops the clause, and only asserting both halves
    /// catches that.
    #[test]
    fn a_vector_primary_upsert_plan_is_admitted() {
        let plan = PhysicalPlan::Vector(VectorOp::DirectUpsert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "vectors"),
            field: "emb".into(),
            surrogate: nodedb_types::Surrogate::ZERO,
            vector: Vec::new(),
            payload: Vec::new(),
            quantization: nodedb_types::VectorQuantization::None,
            storage_dtype: nodedb_types::VectorStorageDtype::F32,
            payload_indexes: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
        });
        assert!(refuse_unprojectable_insert_returning(&plan).is_ok());
    }

    /// A timeseries ingest carries the clause, so the same gate admits it.
    #[test]
    fn a_timeseries_ingest_plan_is_admitted() {
        let plan = PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "metrics"),
            payload: Vec::new(),
            format: "ilp".into(),
            wal_lsn: None,
            surrogates: Vec::new(),
            provenance: None,
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            returning: None,
            rls_filters: Vec::new(),
        });
        assert!(refuse_unprojectable_insert_returning(&plan).is_ok());
    }

    /// A columnar insert carries the clause, so the same gate admits it.
    /// This is the assertion that fails if the columnar arm is ever restored
    /// to the refusal while the op keeps its `returning` slot — the
    /// combination that silently drops the clause.
    #[test]
    fn a_columnar_insert_plan_is_admitted() {
        let plan = PhysicalPlan::Columnar(ColumnarOp::Insert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "metrics"),
            payload: Vec::new(),
            format: "msgpack".into(),
            intent: nodedb_physical::physical_plan::ColumnarInsertIntent::Insert,
            on_conflict_updates: Vec::new(),
            surrogates: Vec::new(),
            schema_bytes: Vec::new(),
            provenance: None,
            wal_lsn: None,
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            returning: None,
            rls_filters: Vec::new(),
        });
        assert!(refuse_unprojectable_insert_returning(&plan).is_ok());
    }

    /// A document insert carries the clause, so the same gate admits it.
    #[test]
    fn a_document_insert_plan_is_admitted() {
        let plan = PhysicalPlan::Document(DocumentOp::PointInsert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "items"),
            document_id: "a".into(),
            value: Vec::new(),
            if_absent: false,
            surrogate: nodedb_types::Surrogate::ZERO,
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
            deferred_sum_targets: Vec::new(),
        });
        assert!(refuse_unprojectable_insert_returning(&plan).is_ok());
    }
}
