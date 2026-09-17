// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral plan classification types.
//!
//! These operate purely on `PhysicalPlan` and carry no pgwire wire types,
//! so they are shared across any protocol-specific response shaper.

use crate::bridge::envelope::PhysicalPlan;
use nodedb_physical::physical_plan::{
    ColumnarOp, CrdtOp, DocumentOp, GraphOp, KvOp, QueryOp, SpatialOp, TextOp, TimeseriesOp,
    VectorOp,
};

#[derive(Debug, Clone, Copy)]
pub enum PlanKind {
    SingleDocument,
    MultiRow,
    /// Array slice result — decoded via `ArraySliceResponse` to surface the
    /// `truncated_before_horizon` flag as a pgwire NOTICE when set.
    ArraySlice,
    Execution,
    /// DML operation that returns affected row count.
    /// The tag name is used in the pgwire `CommandComplete` message (e.g., "UPDATE", "DELETE").
    DmlResult(&'static str),
    /// DML with RETURNING clause — payload is a `RowsPayload` (msgpack).
    /// Decoded into one pgwire field per column.
    ReturningRows,
}

pub fn describe_plan(plan: &PhysicalPlan) -> PlanKind {
    match plan {
        PhysicalPlan::Crdt(CrdtOp::DocUpsert {
            returning: Some(_), ..
        })
        | PhysicalPlan::Crdt(CrdtOp::DocDelete {
            returning: Some(_), ..
        }) => PlanKind::ReturningRows,

        // A CRDT delete can legitimately remove nothing, so its count must render
        // as a DML count from the write's own response, not a document-shaped read.
        PhysicalPlan::Crdt(CrdtOp::DocDelete { .. }) => DmlResult("DELETE"),

        PhysicalPlan::Document(DocumentOp::PointGet { .. })
        | PhysicalPlan::Crdt(CrdtOp::Read { .. })
        | PhysicalPlan::Crdt(CrdtOp::GetPolicy { .. })
        | PhysicalPlan::Crdt(CrdtOp::DocUpsert { .. }) => PlanKind::SingleDocument,

        PhysicalPlan::Vector(VectorOp::Search { .. })
        | PhysicalPlan::Vector(VectorOp::MultiSearch { .. })
        | PhysicalPlan::Vector(VectorOp::MultiVectorScoreSearch { .. })
        | PhysicalPlan::Vector(VectorOp::SparseSearch { .. })
        | PhysicalPlan::Document(DocumentOp::RangeScan { .. })
        | PhysicalPlan::Graph(GraphOp::Hop { .. })
        | PhysicalPlan::Graph(GraphOp::Neighbors { .. })
        | PhysicalPlan::Graph(GraphOp::Path { .. })
        | PhysicalPlan::Graph(GraphOp::Subgraph { .. })
        | PhysicalPlan::Graph(GraphOp::RagFusion { .. })
        | PhysicalPlan::Document(DocumentOp::Scan { .. })
        | PhysicalPlan::Document(DocumentOp::IndexedFetch { .. })
        | PhysicalPlan::Columnar(ColumnarOp::Scan { .. })
        | PhysicalPlan::Timeseries(TimeseriesOp::Scan { .. })
        | PhysicalPlan::Spatial(SpatialOp::Scan { .. })
        | PhysicalPlan::Kv(KvOp::Scan { .. })
        | PhysicalPlan::Kv(KvOp::BatchGet { .. })
        | PhysicalPlan::Query(QueryOp::Aggregate { .. })
        | PhysicalPlan::Query(QueryOp::FacetCounts { .. })
        | PhysicalPlan::Query(QueryOp::HashJoin { .. })
        | PhysicalPlan::Query(QueryOp::RecursiveScan { .. })
        | PhysicalPlan::Query(QueryOp::RecursiveValue { .. })
        | PhysicalPlan::Query(QueryOp::LateralTopK { .. })
        | PhysicalPlan::Query(QueryOp::LateralLoop { .. })
        | PhysicalPlan::Graph(GraphOp::Algo { .. })
        | PhysicalPlan::Graph(GraphOp::Match { .. })
        | PhysicalPlan::Graph(GraphOp::MatchContinuation { .. })
        | PhysicalPlan::Graph(GraphOp::MatchVarLenResume { .. })
        | PhysicalPlan::Graph(GraphOp::BspSuperstep(_))
        | PhysicalPlan::Graph(GraphOp::WccSuperstep(_))
        | PhysicalPlan::Text(TextOp::Search { .. })
        | PhysicalPlan::Text(TextOp::PhraseSearch { .. })
        | PhysicalPlan::Text(TextOp::HybridSearch { .. })
        | PhysicalPlan::Text(TextOp::HybridSearchTriple { .. })
        | PhysicalPlan::Text(TextOp::BM25ScoreScan { .. })
        | PhysicalPlan::Text(TextOp::FtsIndexDoc { .. })
        | PhysicalPlan::Text(TextOp::FtsDeleteDoc { .. }) => PlanKind::MultiRow,

        // Opaque execution results: config write, index teardown status.
        PhysicalPlan::Text(TextOp::SetTextConfig { .. })
        | PhysicalPlan::Vector(VectorOp::DropIndex { .. })
        // Internal typed zerompk value, never a client row — decoded by the
        // admission caller as `CrdtPreviewResult`.
        | PhysicalPlan::Crdt(CrdtOp::PreviewApply { .. }) => PlanKind::Execution,

        PhysicalPlan::Kv(KvOp::Get { .. }) | PhysicalPlan::Kv(KvOp::FieldGet { .. }) => {
            PlanKind::SingleDocument
        }

        // Constant/catalog-scan expressions compile to ProviderScan; route MultiRow
        // so each array element streams as its own pgwire row.
        PhysicalPlan::Query(QueryOp::ProviderScan { .. }) => PlanKind::MultiRow,

        // Exchange means the plan wasn't yet resolved — recurse into the child.
        PhysicalPlan::Query(QueryOp::Exchange(op)) => describe_plan(&op.child),

        // PostProcess reshapes a multi-row subquery; its kind is the child's.
        PhysicalPlan::Query(QueryOp::PostProcess { input, .. }) => describe_plan(input),

        // SetOp resolves to a ProviderScan of merged rows; route MultiRow so
        // each row streams as its own pgwire row.
        PhysicalPlan::Query(QueryOp::SetOp { .. }) => PlanKind::MultiRow,

        // An insert with a projection returns real stored rows and must be decoded
        // and redacted, else it silently leaks unredacted rows like `Merge` did.
        PhysicalPlan::Kv(
            KvOp::Insert {
                returning: Some(_), ..
            }
            | KvOp::InsertIfAbsent {
                returning: Some(_), ..
            }
            | KvOp::InsertOnConflictUpdate {
                returning: Some(_), ..
            }
            | KvOp::Put {
                returning: Some(_), ..
            }
            | KvOp::BatchPut {
                returning: Some(_), ..
            },
        )
        | PhysicalPlan::Document(DocumentOp::PointPut {
            returning: Some(_), ..
        })
        | PhysicalPlan::Document(DocumentOp::PointInsert {
            returning: Some(_), ..
        })
        | PhysicalPlan::Document(DocumentOp::BatchInsert {
            returning: Some(_), ..
        })
        | PhysicalPlan::Columnar(ColumnarOp::Insert {
            returning: Some(_), ..
        })
        | PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
            returning: Some(_), ..
        })
        | PhysicalPlan::Vector(VectorOp::DirectUpsert {
            returning: Some(_), ..
        }) => PlanKind::ReturningRows,

        // `PointInsert`/`InsertIfAbsent`: `ON CONFLICT DO NOTHING` makes them
        // no-op-capable, so the count must come from the write's response.
        PhysicalPlan::Document(DocumentOp::PointPut { .. })
        | PhysicalPlan::Document(DocumentOp::PointInsert { .. })
        | PhysicalPlan::Document(DocumentOp::BatchInsert { .. })
        | PhysicalPlan::Kv(KvOp::InsertIfAbsent { .. })
        | PhysicalPlan::Columnar(ColumnarOp::Insert { .. }) => DmlResult("INSERT"),

        PhysicalPlan::Document(DocumentOp::PointUpdate {
            returning: Some(_), ..
        })
        | PhysicalPlan::Document(DocumentOp::BulkUpdate {
            returning: Some(_), ..
        }) => PlanKind::ReturningRows,
        PhysicalPlan::Document(DocumentOp::PointUpdate { .. })
        | PhysicalPlan::Document(DocumentOp::BulkUpdate { .. }) => DmlResult("UPDATE"),

        PhysicalPlan::Document(DocumentOp::PointDelete {
            returning: Some(_), ..
        })
        | PhysicalPlan::Document(DocumentOp::BulkDelete {
            returning: Some(_), ..
        }) => PlanKind::ReturningRows,
        PhysicalPlan::Document(DocumentOp::PointDelete { .. })
        | PhysicalPlan::Document(DocumentOp::BulkDelete { .. }) => DmlResult("DELETE"),

        PhysicalPlan::Document(DocumentOp::UpdateFromJoin {
            returning: Some(_), ..
        }) => PlanKind::ReturningRows,
        PhysicalPlan::Document(DocumentOp::UpdateFromJoin { .. }) => DmlResult("UPDATE"),

        // A MERGE with a projection returns real target rows and must be decoded
        // and redacted, else it falls through to unredacted `Execution` passthrough.
        PhysicalPlan::Document(DocumentOp::Merge {
            returning: Some(_), ..
        }) => PlanKind::ReturningRows,
        // Postgres tags a plain MERGE `MERGE <rows-affected>`, matching the staged path.
        PhysicalPlan::Document(DocumentOp::Merge { .. }) => DmlResult("MERGE"),

        PhysicalPlan::Document(DocumentOp::Truncate { .. }) => DmlResult("TRUNCATE"),

        // KV delete/truncate count the keys removed — `Execution` would discard that.
        PhysicalPlan::Kv(KvOp::Delete { .. }) | PhysicalPlan::Kv(KvOp::PredicateDelete { .. }) => {
            DmlResult("DELETE")
        }
        // Reports `{"affected": n}` — `Execution` would discard that count.
        PhysicalPlan::Kv(KvOp::PredicateUpdate { .. }) => DmlResult("UPDATE"),
        PhysicalPlan::Kv(KvOp::Truncate { .. }) => DmlResult("TRUNCATE"),

        PhysicalPlan::Document(DocumentOp::InsertSelect { .. }) => DmlResult("INSERT"),

        PhysicalPlan::Document(DocumentOp::Upsert {
            returning: Some(_), ..
        }) => PlanKind::ReturningRows,
        PhysicalPlan::Document(DocumentOp::Upsert { .. }) => DmlResult("UPSERT"),

        // Array read/maintenance ops produce a JSON-array payload; route to the
        // multi-row decoder so each row streams as its own pgwire field.
        PhysicalPlan::Array(nodedb_physical::physical_plan::ArrayOp::Slice { .. }) => {
            PlanKind::ArraySlice
        }
        PhysicalPlan::Array(nodedb_physical::physical_plan::ArrayOp::Project { .. })
        | PhysicalPlan::Array(nodedb_physical::physical_plan::ArrayOp::Aggregate { .. })
        | PhysicalPlan::Array(nodedb_physical::physical_plan::ArrayOp::Elementwise { .. }) => {
            PlanKind::MultiRow
        }
        // Flush/Compact return status JSON — route SingleDocument.
        PhysicalPlan::Array(nodedb_physical::physical_plan::ArrayOp::Flush { .. })
        | PhysicalPlan::Array(nodedb_physical::physical_plan::ArrayOp::Compact { .. }) => {
            PlanKind::SingleDocument
        }

        // Vector write/config ops carry no row payload. Enumerated explicitly (not a
        // `Vector(_)` wildcard) so a future read op can't silently strand its hits.
        PhysicalPlan::Vector(VectorOp::Insert { .. })
        | PhysicalPlan::Vector(VectorOp::BatchInsert { .. })
        | PhysicalPlan::Vector(VectorOp::Delete { .. })
        | PhysicalPlan::Vector(VectorOp::DeleteBySurrogate { .. })
        | PhysicalPlan::Vector(VectorOp::SetParams { .. })
        | PhysicalPlan::Vector(VectorOp::QueryStats { .. })
        | PhysicalPlan::Vector(VectorOp::Seal { .. })
        | PhysicalPlan::Vector(VectorOp::CompactIndex { .. })
        | PhysicalPlan::Vector(VectorOp::Rebuild { .. })
        | PhysicalPlan::Vector(VectorOp::SparseInsert { .. })
        | PhysicalPlan::Vector(VectorOp::SparseDelete { .. })
        | PhysicalPlan::Vector(VectorOp::MultiVectorInsert { .. })
        | PhysicalPlan::Vector(VectorOp::MultiVectorDelete { .. })
        | PhysicalPlan::Vector(VectorOp::DirectUpsert { .. }) => PlanKind::Execution,

        // Document ops with no row payload. Enumerated explicitly, not a `Document(_)`
        // wildcard — that let `Merge` default to unredacted passthrough.
        PhysicalPlan::Document(DocumentOp::Register { .. })
        | PhysicalPlan::Document(DocumentOp::IndexLookup { .. })
        | PhysicalPlan::Document(DocumentOp::DropIndex { .. })
        | PhysicalPlan::Document(DocumentOp::BackfillIndex { .. })
        | PhysicalPlan::Document(DocumentOp::EstimateCount { .. })
        | PhysicalPlan::Document(DocumentOp::MaterializeScan { .. })
        // Read-only resolve: payload is the internal classification tuple, never a client row.
        | PhysicalPlan::Document(DocumentOp::ResolveWrite(_))
        // A derived balance write answers no client — reports an affected count only.
        | PhysicalPlan::Document(DocumentOp::ApplyBalanceDelta { .. })
        // Never reaches this classifier: write-resolve returns the response itself,
        // shaped from the intercepted plan whose `returning` slot decides.
        | PhysicalPlan::Document(DocumentOp::ResolvedWrite { .. })

        // Default: opaque execution result. Exhaustive so a new variant forces a decision.
        | PhysicalPlan::Graph(_)
        | PhysicalPlan::Kv(_)
        | PhysicalPlan::Columnar(_)
        | PhysicalPlan::Timeseries(_)
        | PhysicalPlan::Spatial(_)
        | PhysicalPlan::Crdt(_)
        | PhysicalPlan::Query(_)
        | PhysicalPlan::Meta(_)
        | PhysicalPlan::Array(_)
        | PhysicalPlan::ClusterArray(_)
        | PhysicalPlan::ClusterEvent(_) => PlanKind::Execution,
    }
}

// Bring the variant into scope for brevity in match arms above.
use PlanKind::DmlResult;

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::{DatabaseId, QualifiedCollection};

    #[test]
    fn crdt_preview_is_an_opaque_execution_plan() {
        let plan = PhysicalPlan::Crdt(CrdtOp::PreviewApply {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "tasks"),
            document_id: "task-1".to_string(),
            delta: vec![0x92, 0x01],
        });

        assert!(matches!(describe_plan(&plan), PlanKind::Execution));
    }

    fn merge_plan(
        returning: Option<nodedb_physical::physical_plan::ReturningSpec>,
    ) -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::Merge {
            target_collection: QualifiedCollection::new(DatabaseId::DEFAULT, "target"),
            source_collection: QualifiedCollection::new(DatabaseId::DEFAULT, "source"),
            source_alias: "s".to_string(),
            target_join_col: "id".to_string(),
            source_join_col: "id".to_string(),
            clauses: Vec::new(),
            returning,
            resolved_inserts: None,
            source_rows: None,
            rls_filters: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            resolved_sum_targets: Vec::new(),
            declared_primary_key: None,
        })
    }

    /// A `MERGE ... RETURNING` payload is real target rows — `Execution` would
    /// pass them unredacted.
    #[test]
    fn merge_with_returning_is_returning_rows() {
        use nodedb_physical::physical_plan::{ReturningColumns, ReturningSpec};

        let plan = merge_plan(Some(ReturningSpec {
            columns: ReturningColumns::Star,
        }));

        assert!(matches!(describe_plan(&plan), PlanKind::ReturningRows));
    }

    /// Every insert-family op with a projection must classify row-returning,
    /// else it leaks unredacted like the MERGE case above.
    #[test]
    fn inserts_with_returning_are_returning_rows() {
        use nodedb_physical::physical_plan::{ReturningColumns, ReturningSpec};

        let spec = || {
            Some(ReturningSpec {
                columns: ReturningColumns::Star,
            })
        };
        let plans = [
            PhysicalPlan::Document(DocumentOp::PointInsert {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                document_id: "d".into(),
                value: Vec::new(),
                if_absent: false,
                surrogate: nodedb_types::Surrogate::ZERO,
                returning: spec(),
                rls_filters: Vec::new(),
                resolved_sum_targets: Vec::new(),
                deferred_sum_targets: Vec::new(),
            }),
            PhysicalPlan::Document(DocumentOp::PointPut {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                document_id: "d".into(),
                value: Vec::new(),
                surrogate: nodedb_types::Surrogate::ZERO,
                pk_bytes: Vec::new(),
                returning: spec(),
                rls_filters: Vec::new(),
                resolved_sum_targets: Vec::new(),
            }),
            PhysicalPlan::Document(DocumentOp::BatchInsert {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                documents: Vec::new(),
                surrogates: Vec::new(),
                returning: spec(),
                rls_filters: Vec::new(),
                resolved_sum_targets: Vec::new(),
                deferred_sum_targets: Vec::new(),
            }),
            PhysicalPlan::Document(DocumentOp::Upsert {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                document_id: "d".into(),
                value: Vec::new(),
                on_conflict_updates: Vec::new(),
                surrogate: nodedb_types::Surrogate::ZERO,
                rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
                returning: spec(),
                rls_filters: Vec::new(),
                resolved_sum_targets: Vec::new(),
            }),
        ];
        for plan in &plans {
            assert!(
                matches!(describe_plan(plan), PlanKind::ReturningRows),
                "{plan:?} must shape as rows"
            );
        }
    }

    /// Every KV insert-family op that can carry a projection must classify as
    /// row-returning too — the same passthrough leak, one engine over.
    #[test]
    fn kv_inserts_with_returning_are_returning_rows() {
        use nodedb_physical::physical_plan::{KvOp, ReturningColumns, ReturningSpec};

        let spec = || {
            Some(ReturningSpec {
                columns: ReturningColumns::Star,
            })
        };
        let plans = [
            PhysicalPlan::Kv(KvOp::Insert {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                key: b"k".to_vec(),
                value: Vec::new(),
                ttl_ms: 0,
                surrogate: nodedb_types::Surrogate::ZERO,
                returning: spec(),
                rls_filters: Vec::new(),
            }),
            PhysicalPlan::Kv(KvOp::InsertIfAbsent {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                key: b"k".to_vec(),
                value: Vec::new(),
                ttl_ms: 0,
                surrogate: nodedb_types::Surrogate::ZERO,
                returning: spec(),
                rls_filters: Vec::new(),
            }),
            PhysicalPlan::Kv(KvOp::InsertOnConflictUpdate {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                key: b"k".to_vec(),
                value: Vec::new(),
                ttl_ms: 0,
                updates: Vec::new(),
                surrogate: nodedb_types::Surrogate::ZERO,
                rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
                returning: spec(),
                rls_filters: Vec::new(),
            }),
            PhysicalPlan::Kv(KvOp::Put {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                key: b"k".to_vec(),
                value: Vec::new(),
                ttl_ms: 0,
                surrogate: nodedb_types::Surrogate::ZERO,
                returning: spec(),
                rls_filters: Vec::new(),
            }),
            PhysicalPlan::Kv(KvOp::BatchPut {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                entries: Vec::new(),
                ttl_ms: 0,
                surrogates: Vec::new(),
                returning: spec(),
                rls_filters: Vec::new(),
            }),
        ];
        for plan in &plans {
            assert!(
                matches!(describe_plan(plan), PlanKind::ReturningRows),
                "{plan:?} must shape as rows"
            );
        }
    }

    /// A plain MERGE reports its affected count under the Postgres `MERGE` tag,
    /// not an opaque `OK`.
    #[test]
    fn merge_without_returning_is_a_dml_result() {
        assert!(matches!(
            describe_plan(&merge_plan(None)),
            PlanKind::DmlResult("MERGE")
        ));
    }
}
