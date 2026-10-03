// SPDX-License-Identifier: BUSL-1.1

//! Plan builders shared by the cross-engine transaction rollback matrices.

use nodedb_physical::physical_plan::{DocumentOp, GraphOp, PhysicalPlan};

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// The bound identity of "doc1", shared by every plan that names it.
const DOC1: nodedb_types::Surrogate = nodedb_types::Surrogate::new(1);

/// A document PointPut for "doc1" in collection `coll`.
pub fn doc_put(coll: &str, val: &[u8]) -> PhysicalPlan {
    PhysicalPlan::Document(DocumentOp::PointPut {
        collection: nodedb_types::QualifiedCollection::new(nodedb_types::DatabaseId::DEFAULT, coll),
        document_id: "doc1".into(),
        value: val.to_vec(),
        surrogate: DOC1,
        pk_bytes: Vec::new(),
        returning: None,
        rls_filters: Vec::new(),
        resolved_sum_targets: Vec::new(),
    })
}

/// A PointGet for "doc1" in collection `coll`.
pub fn doc_get(coll: &str) -> PhysicalPlan {
    PhysicalPlan::Document(DocumentOp::PointGet {
        collection: nodedb_types::QualifiedCollection::new(nodedb_types::DatabaseId::DEFAULT, coll),
        document_id: "doc1".into(),
        rls_filters: Vec::new(),
        system_time: nodedb_types::SystemTimeScope::Current,
        valid_at_ms: None,
        surrogate: Some(DOC1),
        pk_bytes: Vec::new(),
    })
}

/// A PointInsert (IF NOT EXISTS = false) for "doc2" in collection `coll`.
pub fn doc_insert_conflict(coll: &str) -> PhysicalPlan {
    PhysicalPlan::Document(DocumentOp::PointInsert {
        collection: nodedb_types::QualifiedCollection::new(nodedb_types::DatabaseId::DEFAULT, coll),
        document_id: "doc1".into(),
        value: b"{\"conflict\":true}".to_vec(),
        surrogate: DOC1,
        if_absent: false,
        returning: None,
        rls_filters: Vec::new(),
        resolved_sum_targets: Vec::new(),
        deferred_sum_targets: Vec::new(),
    })
}

/// An EdgePut plan.
pub fn edge_put(coll: &str, src: &str, dst: &str) -> PhysicalPlan {
    PhysicalPlan::Graph(GraphOp::EdgePut {
        collection: nodedb_types::QualifiedCollection::new(nodedb_types::DatabaseId::DEFAULT, coll),
        src_id: src.into(),
        label: "REL".into(),
        dst_id: dst.into(),
        properties: Vec::new(),
        src_surrogate: nodedb_test_support::kv_rows::kv_row_surrogate(src.as_bytes()),
        dst_surrogate: nodedb_test_support::kv_rows::kv_row_surrogate(dst.as_bytes()),
    })
}

/// A Neighbors query to check whether an edge exists.
pub fn neighbors(src: &str) -> PhysicalPlan {
    PhysicalPlan::Graph(GraphOp::Neighbors {
        node_id: src.into(),
        edge_labels: vec!["REL".into()],
        direction: nodedb::engine::graph::edge_store::Direction::Out,
        rls_filters: Vec::new(),
        collection: None,
    })
}
