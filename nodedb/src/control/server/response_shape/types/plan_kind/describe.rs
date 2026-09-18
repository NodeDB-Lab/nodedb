// SPDX-License-Identifier: BUSL-1.1

//! `describe_plan`: the one entry point that maps a `PhysicalPlan` to the
//! response shape it produces. Each engine's `*Op` enum is classified in its
//! own file, exhaustively, so a new op is a compile error until it decides.

use crate::bridge::envelope::PhysicalPlan;

use super::array::describe_array;
use super::columnar_family::{describe_columnar, describe_spatial, describe_timeseries};
use super::crdt::describe_crdt;
use super::document::describe_document;
use super::graph::describe_graph;
use super::kind::PlanKind;
use super::kv::describe_kv;
use super::query::describe_query;
use super::search::{describe_text, describe_vector};

pub fn describe_plan(plan: &PhysicalPlan) -> PlanKind {
    match plan {
        PhysicalPlan::Document(op) => describe_document(op),
        PhysicalPlan::Kv(op) => describe_kv(op),
        PhysicalPlan::Crdt(op) => describe_crdt(op),
        PhysicalPlan::Graph(op) => describe_graph(op),
        PhysicalPlan::Vector(op) => describe_vector(op),
        PhysicalPlan::Text(op) => describe_text(op),
        PhysicalPlan::Columnar(op) => describe_columnar(op),
        PhysicalPlan::Timeseries(op) => describe_timeseries(op),
        PhysicalPlan::Spatial(op) => describe_spatial(op),
        PhysicalPlan::Array(op) => describe_array(op),
        PhysicalPlan::Query(op) => describe_query(op),

        // Control-plane catalog, session and cluster ops. No `MetaOp` is a
        // client DML: none reports a row count, each answers its own caller.
        PhysicalPlan::Meta(_)
        // Never dispatched through the plan-shaping path: the pgwire cluster
        // array router classifies these itself (`routing/cluster_array.rs`).
        | PhysicalPlan::ClusterArray(_)
        // Event-plane forwarding, answered by its own dispatcher.
        | PhysicalPlan::ClusterEvent(_) => PlanKind::Execution,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_physical::physical_plan::{
        ArrayOp, CrdtOp, CrdtWriteVerb, DocumentOp, KvOp, TimeseriesOp,
    };
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
        use nodedb_physical::physical_plan::{ReturningColumns, ReturningSpec};

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

    fn kv_put() -> PhysicalPlan {
        PhysicalPlan::Kv(KvOp::Put {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
            key: b"k".to_vec(),
            value: Vec::new(),
            ttl_ms: 0,
            surrogate: nodedb_types::Surrogate::ZERO,
            returning: None,
            rls_filters: Vec::new(),
        })
    }

    /// `KvOp::Put` is the SQL `UPSERT` statement: it tags `UPSERT n`, the
    /// same as `DocumentOp::Upsert`, the staged path and the Calvin fold.
    #[test]
    fn kv_put_is_the_upsert_dml_result() {
        assert!(matches!(
            describe_plan(&kv_put()),
            PlanKind::DmlResult("UPSERT")
        ));
    }

    /// `KvOp::InsertOnConflictUpdate` resolves insert-vs-update at apply time;
    /// the tag must follow the verb the handler reports.
    #[test]
    fn kv_insert_on_conflict_update_is_decided_by_op() {
        let plan = PhysicalPlan::Kv(KvOp::InsertOnConflictUpdate {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
            key: b"k".to_vec(),
            value: Vec::new(),
            ttl_ms: 0,
            updates: Vec::new(),
            surrogate: nodedb_types::Surrogate::ZERO,
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            returning: None,
            rls_filters: Vec::new(),
        });
        assert!(matches!(describe_plan(&plan), PlanKind::DmlResultByOp));
    }

    /// A timeseries ingest reports `{"accepted": n}` under the `INSERT` tag,
    /// not an opaque `OK`.
    #[test]
    fn timeseries_ingest_is_an_insert_dml_result() {
        let plan = PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "metrics"),
            payload: Vec::new(),
            format: "ilp".to_string(),
            wal_lsn: None,
            surrogates: Vec::new(),
            provenance: None,
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            returning: None,
            rls_filters: Vec::new(),
        });
        assert!(matches!(
            describe_plan(&plan),
            PlanKind::DmlResult("INSERT")
        ));
    }

    fn crdt_doc_upsert(verb: CrdtWriteVerb) -> PhysicalPlan {
        PhysicalPlan::Crdt(CrdtOp::DocUpsert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "notes"),
            document_id: "d1".into(),
            fields_json: "{}".into(),
            surrogate: nodedb_types::Surrogate::ZERO,
            partial: matches!(verb, CrdtWriteVerb::Update),
            verb,
            returning: None,
            rls_filters: Vec::new(),
        })
    }

    /// INSERT, UPSERT and UPDATE all lower to `CrdtOp::DocUpsert`; the tag
    /// follows the statement verb carried on the op.
    #[test]
    fn crdt_doc_upsert_tags_by_verb() {
        assert!(matches!(
            describe_plan(&crdt_doc_upsert(CrdtWriteVerb::Insert)),
            PlanKind::DmlResult("INSERT")
        ));
        assert!(matches!(
            describe_plan(&crdt_doc_upsert(CrdtWriteVerb::Upsert)),
            PlanKind::DmlResult("UPSERT")
        ));
        assert!(matches!(
            describe_plan(&crdt_doc_upsert(CrdtWriteVerb::Update)),
            PlanKind::DmlResult("UPDATE")
        ));
    }

    /// `INSERT INTO ARRAY` reports `{"inserted": n}` under the `INSERT` tag.
    #[test]
    fn array_put_is_an_insert_dml_result() {
        let plan = PhysicalPlan::Array(ArrayOp::Put {
            array_id: nodedb_array::types::ArrayId::new(nodedb_types::TenantId::new(1), "genome"),
            cells_msgpack: Vec::new(),
            wal_lsn: 0,
            provenance: None,
        });
        assert!(matches!(
            describe_plan(&plan),
            PlanKind::DmlResult("INSERT")
        ));
    }
}
