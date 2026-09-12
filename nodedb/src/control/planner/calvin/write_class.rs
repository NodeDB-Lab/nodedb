// SPDX-License-Identifier: BUSL-1.1

//! Exhaustive, compile-enforced classification: does a `PhysicalPlan` mutate
//! base state in a way that needs a Calvin write-key / lock?
//!
//! The chokepoint `classify_dispatch` and `build_static_tx_class` use to
//! decide write-key-set membership. Mirrors `plan_vshard`: every op in the
//! eight write-capable engines is matched explicitly `true`/`false` (no
//! wildcard), so a new op variant is a compile error here. Text/Spatial/
//! Query/Meta stay one blanket `false` arm each (`NotAWrite` in
//! `plan_vshard`), still exhaustive over `PhysicalPlan`.
//!
//! Does NOT delegate to `plan_is_write` (`Permission::Write`): several
//! `Permission::Write` variants carry no vshard to lock in `plan_vshard`
//! (index-metadata ops, cross-collection writes like `Merge`/`TransferItem`,
//! Text/Spatial write ops, most `MetaOp` writes) and would misclassify as
//! Calvin writes, turning a routing gap into an aborted transaction.

#![deny(clippy::wildcard_enum_match_arm)]

use nodedb_physical::physical_plan::{
    ArrayOp, ColumnarOp, CrdtOp, DocumentOp, GraphOp, KvOp, PhysicalPlan, TimeseriesOp, VectorOp,
};

fn document_is_write(op: &DocumentOp) -> bool {
    match op {
        DocumentOp::PointPut { .. }
        | DocumentOp::PointInsert { .. }
        | DocumentOp::PointDelete { .. }
        | DocumentOp::PointUpdate { .. }
        | DocumentOp::BatchInsert { .. }
        | DocumentOp::InsertSelect { .. }
        | DocumentOp::Upsert { .. }
        | DocumentOp::BulkUpdate { .. }
        | DocumentOp::BulkDelete { .. }
        // `UpdateFromJoin` is `Unroutable` in `plan_vshard` (no enforced
        // co-location) but is already classified `true` here.
        | DocumentOp::UpdateFromJoin { .. }
        | DocumentOp::Truncate { .. }
        // Without this in the write-key set, the pair classifies as
        // single-shard and the source write commits without the balance.
        | DocumentOp::ApplyBalanceDelta { .. }
        // Mutates the rows its mutation list names, like any other write.
        | DocumentOp::ResolvedWrite { .. } => true,
        // Read-only: reports what the wrapped write would apply, mutates nothing.
        DocumentOp::ResolveWrite(_)
        | DocumentOp::PointGet { .. }
        | DocumentOp::Scan { .. }
        | DocumentOp::RangeScan { .. }
        | DocumentOp::IndexLookup { .. }
        | DocumentOp::IndexedFetch { .. }
        | DocumentOp::EstimateCount { .. }
        | DocumentOp::MaterializeScan { .. }
        | DocumentOp::Register { .. }
        | DocumentOp::DropIndex { .. }
        | DocumentOp::BackfillIndex { .. }
        // `Merge` is `Permission::Write` but `Unroutable` in `plan_vshard`
        // (no enforced co-location) — kept `false` so it never enters the
        // write-key set with no vshard to lock on.
        | DocumentOp::Merge { .. } => false,
    }
}

/// Whether `plan` is a DERIVED side effect (a `GraphOp` edge write mirroring
/// a document, or an [`DocumentOp::ApplyBalanceDelta`] whose target differs
/// from the source's vShard) rather than the user's own write.
///
/// Both are real writes that must enter Calvin's write-key set — what they
/// must NOT do is answer the client: a derived participant's response
/// describes a row the statement never named, so shaping `CommandComplete`
/// from it would report the wrong count. Named once here rather than as an
/// inline negation, after the balance write (modelled on the implicit graph
/// edge) failed to inherit an ad hoc `!matches!` check and raced the source
/// write to deposit the statement's response.
pub fn is_derived_side_effect(plan: &PhysicalPlan) -> bool {
    match plan {
        PhysicalPlan::Graph(_) => true,
        PhysicalPlan::Document(DocumentOp::ApplyBalanceDelta { .. }) => true,
        PhysicalPlan::Document(_)
        | PhysicalPlan::Kv(_)
        | PhysicalPlan::Vector(_)
        | PhysicalPlan::Timeseries(_)
        | PhysicalPlan::Columnar(_)
        | PhysicalPlan::Crdt(_)
        | PhysicalPlan::Array(_)
        | PhysicalPlan::Text(_)
        | PhysicalPlan::Spatial(_)
        | PhysicalPlan::Query(_)
        | PhysicalPlan::Meta(_)
        | PhysicalPlan::ClusterArray(_)
        | PhysicalPlan::ClusterEvent(_) => false,
    }
}

fn kv_is_write(op: &KvOp) -> bool {
    match op {
        KvOp::Put { .. }
        | KvOp::Insert { .. }
        | KvOp::InsertIfAbsent { .. }
        | KvOp::InsertOnConflictUpdate { .. }
        | KvOp::Delete { .. }
        | KvOp::BatchPut { .. }
        | KvOp::Expire { .. }
        | KvOp::Persist { .. }
        | KvOp::FieldSet { .. }
        | KvOp::Truncate { .. }
        | KvOp::Incr { .. }
        | KvOp::IncrFloat { .. }
        | KvOp::Cas { .. }
        | KvOp::GetSet { .. }
        | KvOp::Transfer { .. }
        // Predicate DML mutates the rows a scan selects, homed on the one
        // collection it names — a single-vshard write like `Truncate`.
        | KvOp::PredicateUpdate { .. }
        | KvOp::PredicateDelete { .. } => true,
        KvOp::Get { .. }
        | KvOp::Scan { .. }
        | KvOp::GetTtl { .. }
        | KvOp::BatchGet { .. }
        | KvOp::FieldGet { .. }
        | KvOp::MaterializeScan { .. }
        // `SortedIndexRank`/`TopK`/`Range`/`Count`/`Score` are `Permission::Read`
        // (query-only) despite the `SortedIndex*` naming.
        | KvOp::SortedIndexRank { .. }
        | KvOp::SortedIndexTopK { .. }
        | KvOp::SortedIndexRange { .. }
        | KvOp::SortedIndexCount { .. }
        | KvOp::SortedIndexScore { .. }
        // Read-only: reports what a governed write would apply, mutates
        // nothing, and is `NotAWrite` in `plan_vshard`.
        | KvOp::ResolveWrite(_)
        // `Permission::Write` but `NotAWrite` in `plan_vshard` — index
        // metadata registration, not key-value state; no vshard to lock on.
        | KvOp::RegisterIndex { .. }
        | KvOp::DropIndex { .. }
        | KvOp::RegisterSortedIndex { .. }
        | KvOp::DropSortedIndex { .. }
        // `Permission::Write` but `Unroutable` in `plan_vshard`
        // (cross-collection write, no enforced co-location).
        | KvOp::TransferItem { .. }
        // `Permission::Write` but `Unroutable` in `plan_vshard` — its
        // mutations may span two collections, so no single vshard to lock on.
        | KvOp::ResolvedWrite { .. } => false,
    }
}

fn vector_is_write(op: &VectorOp) -> bool {
    match op {
        VectorOp::Insert { .. }
        | VectorOp::BatchInsert { .. }
        | VectorOp::Delete { .. }
        | VectorOp::DeleteBySurrogate { .. }
        | VectorOp::SparseInsert { .. }
        | VectorOp::SparseDelete { .. }
        | VectorOp::MultiVectorInsert { .. }
        | VectorOp::MultiVectorDelete { .. }
        | VectorOp::DirectUpsert { .. } => true,
        VectorOp::Search { .. }
        | VectorOp::MultiSearch { .. }
        | VectorOp::QueryStats { .. }
        | VectorOp::SparseSearch { .. }
        | VectorOp::MultiVectorScoreSearch { .. }
        // `SetParams`/`DropIndex`/`Seal`/`CompactIndex`/`Rebuild` are
        // `Permission::Alter` (not `Write` at all) and `NotAWrite` in
        // `plan_vshard`.
        | VectorOp::SetParams { .. }
        | VectorOp::DropIndex { .. }
        | VectorOp::Seal { .. }
        | VectorOp::CompactIndex { .. }
        | VectorOp::Rebuild { .. } => false,
    }
}

fn graph_is_write(op: &GraphOp) -> bool {
    match op {
        GraphOp::EdgePut { .. }
        | GraphOp::EdgePutBatch { .. }
        | GraphOp::EdgeDelete { .. }
        | GraphOp::EdgeDeleteBatch { .. }
        | GraphOp::SetNodeLabels { .. }
        | GraphOp::RemoveNodeLabels { .. } => true,
        // The resolve pass writes nothing; the delete it decides is proposed
        // separately by the write-resolve orchestrator.
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
        | GraphOp::Stats { .. } => false,
    }
}

fn timeseries_is_write(op: &TimeseriesOp) -> bool {
    match op {
        TimeseriesOp::Ingest { .. } => true,
        // The resolve pass writes nothing; the ingest it reports is proposed
        // separately by the write-resolve orchestrator.
        TimeseriesOp::ResolveIngest(_) | TimeseriesOp::Scan { .. } => false,
    }
}

fn columnar_is_write(op: &ColumnarOp) -> bool {
    match op {
        ColumnarOp::Insert { .. }
        | ColumnarOp::Update { .. }
        | ColumnarOp::Delete { .. }
        | ColumnarOp::ResolvedUpdate { .. }
        | ColumnarOp::ResolvedDelete { .. } => true,
        // Read-only: decides the write policy but mutates nothing, so no
        // vshard lock to take.
        ColumnarOp::Scan { .. }
        | ColumnarOp::MaterializeScan { .. }
        | ColumnarOp::ResolveDml { .. } => false,
    }
}

fn crdt_is_write(op: &CrdtOp) -> bool {
    match op {
        CrdtOp::Apply { .. } | CrdtOp::ApplyAuthenticated { .. }
        | CrdtOp::ListInsert { .. }
        | CrdtOp::ListDelete { .. }
        | CrdtOp::ListMove { .. }
        | CrdtOp::DocUpsert { .. }
        | CrdtOp::DocDelete { .. }
        | CrdtOp::SetConstraints { .. }
        | CrdtOp::DropConstraints { .. }
        | CrdtOp::RestoreToVersion { .. }
        | CrdtOp::ImportSnapshot { .. } => true,
        CrdtOp::Read { .. }
        | CrdtOp::PreviewApply { .. }
        | CrdtOp::ReadConstraints { .. }
        | CrdtOp::GetPolicy { .. }
        | CrdtOp::ReadAtVersion { .. }
        | CrdtOp::GetVersionVector { .. }
        | CrdtOp::ExportDelta { .. }
        // `SetPolicy`/`CompactAtVersion` are `Permission::Alter` (not `Write`
        // at all), same pattern as `VectorOp::SetParams`/`Seal` above.
        | CrdtOp::SetPolicy { .. }
        | CrdtOp::CompactAtVersion { .. } => false,
    }
}

fn array_is_write(op: &ArrayOp) -> bool {
    match op {
        // `Put`/`Delete`/`Flush` are `Unroutable` in `plan_vshard` (tile→vshard
        // needs catalog tile_extents not present on the plan) but are
        // already classified `true` here.
        ArrayOp::Put { .. } | ArrayOp::Delete { .. } | ArrayOp::Flush { .. } => true,
        ArrayOp::OpenArray { .. }
        | ArrayOp::Compact { .. }
        | ArrayOp::DropArray { .. }
        | ArrayOp::RestoreArrayDrop { .. }
        | ArrayOp::PurgeArrayDrop { .. }
        | ArrayOp::Slice { .. }
        | ArrayOp::Project { .. }
        | ArrayOp::Aggregate { .. }
        | ArrayOp::Elementwise { .. }
        | ArrayOp::SurrogateBitmapScan { .. } => false,
    }
}

/// Returns `true` if the plan is a write operation that must be classified
/// into Calvin's write-key set.
///
/// Centralizing this avoids scattered `match` arms when new write variants
/// are added. Reads, scans, and query operators return `false`.
pub fn is_write_plan(plan: &PhysicalPlan) -> bool {
    match plan {
        PhysicalPlan::Document(op) => document_is_write(op),
        PhysicalPlan::Kv(op) => kv_is_write(op),
        PhysicalPlan::Vector(op) => vector_is_write(op),
        PhysicalPlan::Graph(op) => graph_is_write(op),
        PhysicalPlan::Timeseries(op) => timeseries_is_write(op),
        PhysicalPlan::Columnar(op) => columnar_is_write(op),
        PhysicalPlan::Crdt(op) => crdt_is_write(op),
        PhysicalPlan::Array(op) => array_is_write(op),
        // Reads, scans, queries, meta, spatial, text: none of these
        // families carry a Calvin-lockable write in `plan_vshard`.
        PhysicalPlan::Spatial(_)
        | PhysicalPlan::Text(_)
        | PhysicalPlan::Query(_)
        | PhysicalPlan::Meta(_)
        | PhysicalPlan::ClusterArray(_)
        | PhysicalPlan::ClusterEvent(_) => false,
    }
}

#[cfg(test)]
mod tests {
    //! Unit tests for the newly-widened write variants in `is_write_plan`.
    //!
    //! Each test below corresponds to a variant that `plan_vshard`
    //! (`control/cluster/calvin/scheduler/driver/core/routing.rs`) confirms is
    //! `Vshards`-routable, and that `required_permission`
    //! (`control/security/identity/plan_permission.rs`) confirms is
    //! `Permission::Write` — the two pieces of evidence this widening rests on.

    use super::*;
    use nodedb_physical::physical_plan::{BatchEdge, CrdtOp, DocumentOp, GraphOp, KvOp, VectorOp};
    use nodedb_types::{
        DatabaseId, PayloadIndexKind, QualifiedCollection, Surrogate, VectorQuantization,
        VectorStorageDtype,
    };

    // ── CrdtOp ──────────────────────────────────────────────────────────────────

    #[test]
    fn is_write_plan_true_for_crdt_apply() {
        let plan = PhysicalPlan::Crdt(CrdtOp::Apply {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "docs"),
            document_id: "id1".to_owned(),
            delta: Vec::new(),
            peer_id: 0,
            mutation_id: 0,
            surrogate: Surrogate::ZERO,
            provenance: None,
            constraint_version_required: 0,
            expected_frontier_digest: None,
        });
        assert!(is_write_plan(&plan), "CrdtOp::Apply must be a write");
    }

    #[test]
    fn is_write_plan_true_for_crdt_set_constraints() {
        let plan = PhysicalPlan::Crdt(CrdtOp::SetConstraints {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "docs"),
            constraint_version: 1,
            constraints: Vec::new(),
        });
        assert!(
            is_write_plan(&plan),
            "CrdtOp::SetConstraints must be a write"
        );
    }

    #[test]
    fn is_write_plan_true_for_crdt_drop_constraints() {
        let plan = PhysicalPlan::Crdt(CrdtOp::DropConstraints {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "docs"),
            constraint_version: 1,
        });
        assert!(
            is_write_plan(&plan),
            "CrdtOp::DropConstraints must be a write"
        );
    }

    #[test]
    fn is_write_plan_true_for_crdt_restore_to_version() {
        let plan = PhysicalPlan::Crdt(CrdtOp::RestoreToVersion {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "docs"),
            document_id: "id1".to_owned(),
            target_version_json: "{}".to_owned(),
            surrogate: Surrogate::new(1),
        });
        assert!(
            is_write_plan(&plan),
            "CrdtOp::RestoreToVersion must be a write"
        );
    }

    #[test]
    fn is_write_plan_true_for_crdt_import_snapshot() {
        let plan = PhysicalPlan::Crdt(CrdtOp::ImportSnapshot {
            tenant_id: 1,
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "docs"),
            bytes: Vec::new(),
        });
        assert!(
            is_write_plan(&plan),
            "CrdtOp::ImportSnapshot must be a write"
        );
    }

    // ── DocumentOp ────────────────────────────────────────────────────────────

    #[test]
    fn is_write_plan_true_for_document_truncate() {
        let plan = PhysicalPlan::Document(DocumentOp::Truncate {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "docs"),
            restart_identity: false,
            resolved_sum_targets: Vec::new(),
            declared_primary_key: None,
        });
        assert!(is_write_plan(&plan), "DocumentOp::Truncate must be a write");
    }

    // ── KvOp ─────────────────────────────────────────────────────────────────

    #[test]
    fn is_write_plan_true_for_kv_truncate() {
        let plan = PhysicalPlan::Kv(KvOp::Truncate {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "cache"),
        });
        assert!(is_write_plan(&plan), "KvOp::Truncate must be a write");
    }

    #[test]
    fn is_write_plan_true_for_kv_expire() {
        let plan = PhysicalPlan::Kv(KvOp::Expire {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "cache"),
            key: b"k".to_vec(),
            ttl_ms: 1000,
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
        });
        assert!(is_write_plan(&plan), "KvOp::Expire must be a write");
    }

    #[test]
    fn is_write_plan_true_for_kv_persist() {
        let plan = PhysicalPlan::Kv(KvOp::Persist {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "cache"),
            key: b"k".to_vec(),
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
        });
        assert!(is_write_plan(&plan), "KvOp::Persist must be a write");
    }

    #[test]
    fn is_write_plan_true_for_kv_field_set() {
        let plan = PhysicalPlan::Kv(KvOp::FieldSet {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "cache"),
            key: b"k".to_vec(),
            updates: vec![("field".to_owned(), b"v".to_vec())],
            surrogate: Surrogate::new(1),
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
        });
        assert!(is_write_plan(&plan), "KvOp::FieldSet must be a write");
    }

    #[test]
    fn is_write_plan_true_for_kv_incr() {
        let plan = PhysicalPlan::Kv(KvOp::Incr {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "cache"),
            key: b"k".to_vec(),
            delta: 1,
            ttl_ms: 0,
            surrogate: Surrogate::new(1),
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
        });
        assert!(is_write_plan(&plan), "KvOp::Incr must be a write");
    }

    #[test]
    fn is_write_plan_true_for_kv_incr_float() {
        let plan = PhysicalPlan::Kv(KvOp::IncrFloat {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "cache"),
            key: b"k".to_vec(),
            delta: 1.5,
            surrogate: Surrogate::new(1),
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
        });
        assert!(is_write_plan(&plan), "KvOp::IncrFloat must be a write");
    }

    #[test]
    fn is_write_plan_true_for_kv_cas() {
        let plan = PhysicalPlan::Kv(KvOp::Cas {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "cache"),
            key: b"k".to_vec(),
            expected: b"old".to_vec(),
            new_value: b"new".to_vec(),
            surrogate: Surrogate::new(1),
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
        });
        assert!(is_write_plan(&plan), "KvOp::Cas must be a write");
    }

    #[test]
    fn is_write_plan_true_for_kv_get_set() {
        let plan = PhysicalPlan::Kv(KvOp::GetSet {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "cache"),
            key: b"k".to_vec(),
            new_value: b"new".to_vec(),
            surrogate: Surrogate::new(1),
            rls_filters: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
        });
        assert!(is_write_plan(&plan), "KvOp::GetSet must be a write");
    }

    #[test]
    fn is_write_plan_true_for_kv_transfer() {
        let plan = PhysicalPlan::Kv(KvOp::Transfer {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "accounts"),
            source_key: b"a".to_vec(),
            dest_key: b"b".to_vec(),
            field: "balance".to_owned(),
            amount: 10.0,
            debit_surrogate: Surrogate::new(1),
            credit_surrogate: Surrogate::new(2),
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
        });
        assert!(is_write_plan(&plan), "KvOp::Transfer must be a write");
    }

    // ── VectorOp ─────────────────────────────────────────────────────────────

    #[test]
    fn is_write_plan_true_for_vector_multi_vector_delete() {
        let plan = PhysicalPlan::Vector(VectorOp::MultiVectorDelete {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "vecs"),
            field_name: "colbert".to_owned(),
            document_surrogate: Surrogate::new(2),
        });
        assert!(
            is_write_plan(&plan),
            "VectorOp::MultiVectorDelete must be a write"
        );
    }

    #[test]
    fn is_write_plan_true_for_vector_direct_upsert() {
        let plan = PhysicalPlan::Vector(VectorOp::DirectUpsert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "vecs"),
            field: "emb".to_owned(),
            surrogate: Surrogate::new(3),
            vector: vec![0.5, 0.6],
            payload: vec![1, 2, 3],
            quantization: VectorQuantization::None,
            storage_dtype: VectorStorageDtype::F32,
            payload_indexes: vec![("tenant_id".to_owned(), PayloadIndexKind::Equality)],
            returning: None,
            rls_filters: Vec::new(),
        });
        assert!(
            is_write_plan(&plan),
            "VectorOp::DirectUpsert must be a write"
        );
    }

    // ── GraphOp ──────────────────────────────────────────────────────────────

    #[test]
    fn is_write_plan_true_for_graph_edge_put_batch() {
        let edge = BatchEdge {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "follows"),
            src_id: "a".to_owned(),
            label: "knows".to_owned(),
            dst_id: "b".to_owned(),
            src_surrogate: Surrogate::new(1),
            dst_surrogate: Surrogate::new(2),
        };
        let plan = PhysicalPlan::Graph(GraphOp::EdgePutBatch { edges: vec![edge] });
        assert!(
            is_write_plan(&plan),
            "GraphOp::EdgePutBatch must be a write"
        );
    }

    #[test]
    fn is_write_plan_true_for_graph_edge_delete_batch() {
        let edge = BatchEdge {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "follows"),
            src_id: "a".to_owned(),
            label: "knows".to_owned(),
            dst_id: "b".to_owned(),
            src_surrogate: Surrogate::new(1),
            dst_surrogate: Surrogate::new(2),
        };
        let plan = PhysicalPlan::Graph(GraphOp::EdgeDeleteBatch { edges: vec![edge] });
        assert!(
            is_write_plan(&plan),
            "GraphOp::EdgeDeleteBatch must be a write"
        );
    }

    #[test]
    fn is_write_plan_true_for_graph_set_node_labels() {
        let plan = PhysicalPlan::Graph(GraphOp::SetNodeLabels {
            node_id: "n1".to_owned(),
            labels: vec!["Person".to_owned()],
        });
        assert!(
            is_write_plan(&plan),
            "GraphOp::SetNodeLabels must be a write"
        );
    }

    #[test]
    fn is_write_plan_true_for_graph_remove_node_labels() {
        let plan = PhysicalPlan::Graph(GraphOp::RemoveNodeLabels {
            node_id: "n1".to_owned(),
            labels: vec!["Person".to_owned()],
        });
        assert!(
            is_write_plan(&plan),
            "GraphOp::RemoveNodeLabels must be a write"
        );
    }

    // ── is_derived_side_effect ──────────────────────────────────────────────────

    fn balance_delta_plan() -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::ApplyBalanceDelta {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "accounts"),
            document_id: "0000010f".to_owned(),
            surrogate: Surrogate::new(271),
            column: "balance".to_owned(),
            delta: "25".to_owned(),
            join_column: "account_id".to_owned(),
            join_value: "acc-1".to_owned(),
            declared_primary_key: None,
        })
    }

    fn point_insert_plan() -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::PointInsert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "entries"),
            document_id: "e1".to_owned(),
            value: Vec::new(),
            if_absent: false,
            surrogate: Surrogate::new(11),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
            deferred_sum_targets: Vec::new(),
        })
    }

    /// A balance write is a DERIVED side effect, exactly as an implicit graph edge
    /// is.
    ///
    /// Both are appended by the Control Plane alongside a statement they do not
    /// appear in, and neither may own that statement's applied response: the
    /// `CommandComplete` tag is shaped from ONE deposited response, so a derived
    /// participant winning the deposit hands the user's `INSERT` a count that
    /// belongs to a row the statement never named.
    #[test]
    fn a_balance_write_is_a_derived_side_effect() {
        assert!(is_derived_side_effect(&balance_delta_plan()));
    }

    /// The user's own write is not, so it remains the participant that deposits.
    /// Without this the fix would leave every cross-shard statement with no applied
    /// response at all.
    #[test]
    fn the_users_own_write_is_not_a_derived_side_effect() {
        assert!(!is_derived_side_effect(&point_insert_plan()));
    }

    /// Derived does NOT mean "not a write": both still enter Calvin's write-key
    /// set, which is what makes the pair multi-shard and commit atomically. The two
    /// classifications answer different questions and must not be collapsed.
    #[test]
    fn a_derived_side_effect_is_still_a_calvin_write() {
        assert!(is_write_plan(&balance_delta_plan()));
        assert!(is_write_plan(&point_insert_plan()));
    }
}
