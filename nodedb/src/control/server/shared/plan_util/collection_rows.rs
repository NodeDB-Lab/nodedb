// SPDX-License-Identifier: BUSL-1.1

//! The collection an engine operation reads or writes rows of.
//!
//! Each function matches its operation family variant by variant. A variant
//! that reads or writes rows of one named collection reports it.

use nodedb_physical::physical_plan::{
    ColumnarOp, DocumentOp, GraphOp, SpatialOp, TextOp, TimeseriesOp, VectorOp,
};

pub(super) fn document_collection(op: &DocumentOp) -> Option<&str> {
    match op {
        DocumentOp::PointGet { collection, .. }
        | DocumentOp::PointPut { collection, .. }
        | DocumentOp::PointInsert { collection, .. }
        | DocumentOp::PointDelete { collection, .. }
        | DocumentOp::PointUpdate { collection, .. }
        | DocumentOp::Scan { collection, .. }
        | DocumentOp::BatchInsert { collection, .. }
        | DocumentOp::RangeScan { collection, .. }
        | DocumentOp::Register { collection, .. }
        | DocumentOp::IndexLookup { collection, .. }
        | DocumentOp::IndexedFetch { collection, .. }
        | DocumentOp::DropIndex { collection, .. }
        | DocumentOp::BackfillIndex { collection, .. }
        | DocumentOp::Truncate { collection, .. }
        | DocumentOp::EstimateCount { collection, .. }
        | DocumentOp::Upsert { collection, .. }
        | DocumentOp::BatchUpsert { collection, .. }
        | DocumentOp::BulkUpdate { collection, .. }
        | DocumentOp::BulkDelete { collection, .. }
        | DocumentOp::MaterializeScan { collection, .. }
        | DocumentOp::ApplyBalanceDelta { collection, .. } => Some(collection.as_str()),
        // The source is read, but every written or RETURNING row belongs to
        // the target: that is the collection whose policies and write
        // version apply.
        DocumentOp::InsertSelect {
            target_collection, ..
        }
        | DocumentOp::Merge {
            target_collection, ..
        }
        | DocumentOp::UpdateFromJoin {
            target_collection, ..
        } => Some(target_collection.as_str()),
        // The wrapped op is the intercepted write verbatim.
        DocumentOp::ResolveWrite(inner) => document_collection(inner),
        // The mutations can span collections. `metered_collections` names
        // each one.
        DocumentOp::ResolvedWrite { .. } => None,
    }
}

pub(super) fn vector_collection(op: &VectorOp) -> Option<&str> {
    match op {
        VectorOp::Search { collection, .. }
        | VectorOp::Insert { collection, .. }
        | VectorOp::BatchInsert { collection, .. }
        | VectorOp::MultiSearch { collection, .. }
        | VectorOp::Delete { collection, .. }
        | VectorOp::DeleteBySurrogate { collection, .. }
        | VectorOp::SetParams { collection, .. }
        | VectorOp::QueryStats { collection, .. }
        | VectorOp::SparseInsert { collection, .. }
        | VectorOp::SparseSearch { collection, .. }
        | VectorOp::SparseDelete { collection, .. }
        | VectorOp::MultiVectorInsert { collection, .. }
        | VectorOp::MultiVectorDelete { collection, .. }
        | VectorOp::MultiVectorScoreSearch { collection, .. }
        // A vector-primary collection stores its row here and nowhere else,
        // so these ops are the only source a policy can key on.
        | VectorOp::DirectUpsert { collection, .. }
        | VectorOp::DirectInsert { collection, .. }
        | VectorOp::DirectInsertIfAbsent { collection, .. }
        | VectorOp::DirectBatchWrite { collection, .. }
        | VectorOp::DirectDelete { collection, .. }
        | VectorOp::DirectTruncate { collection, .. }
        | VectorOp::DirectUpdate { collection, .. }
        | VectorOp::ResolvedDirectWrite { collection, .. } => Some(collection.as_str()),
        // The wrapped op is the intercepted write verbatim.
        VectorOp::ResolveDirectWrite(inner) => vector_collection(inner),
        // Index maintenance: it reorganizes the index and reads or writes
        // no row.
        VectorOp::DropIndex { .. }
        | VectorOp::Seal { .. }
        | VectorOp::CompactIndex { .. }
        | VectorOp::Rebuild { .. } => None,
    }
}

pub(super) fn graph_collection(op: &GraphOp) -> Option<&str> {
    match op {
        GraphOp::EdgePut { collection, .. }
        | GraphOp::EdgeDelete { collection, .. }
        | GraphOp::RagFusion { collection, .. }
        | GraphOp::TemporalNeighbors { collection, .. }
        | GraphOp::NodeEdgeGuard { collection, .. }
        | GraphOp::NodePresenceGuard { collection, .. }
        | GraphOp::TruncateEdges { collection, .. }
        | GraphOp::NodePresenceRead { collection, .. } => Some(collection.as_str()),
        // A traversal scoped to a collection reads its edges. An unscoped
        // traversal names none.
        GraphOp::Hop { collection, .. }
        | GraphOp::Neighbors { collection, .. }
        | GraphOp::NeighborsMulti { collection, .. }
        | GraphOp::Path { collection, .. }
        | GraphOp::Subgraph { collection, .. }
        | GraphOp::Stats { collection, .. } => collection.as_ref().map(|c| c.as_str()),
        // The wrapped op is the intercepted delete verbatim.
        GraphOp::ResolveEdgeDelete(inner) => graph_collection(inner),
        // Each edge names its own collection, so a batch can span several.
        // `metered_collections` names each one.
        GraphOp::EdgePutBatch { .. } | GraphOp::EdgeDeleteBatch { .. } => None,
        // `params.collection` is a bare catalog name, not a qualified
        // collection. The algorithm dispatch gates its read before it runs.
        GraphOp::Algo { .. } | GraphOp::BspSuperstep(_) | GraphOp::WccSuperstep(_) => None,
        // A pattern names its collections inside the encoded query.
        GraphOp::Match { .. }
        | GraphOp::MatchContinuation { .. }
        | GraphOp::MatchVarLenResume { .. } => None,
        // A label change names a node and no collection.
        GraphOp::SetNodeLabels { .. } | GraphOp::RemoveNodeLabels { .. } => None,
    }
}

pub(super) fn text_collection(op: &TextOp) -> Option<&str> {
    match op {
        TextOp::Search { collection, .. }
        | TextOp::BM25ScoreScan { collection, .. }
        | TextOp::PhraseSearch { collection, .. }
        | TextOp::HybridSearch { collection, .. }
        | TextOp::FtsIndexDoc { collection, .. }
        | TextOp::FtsDeleteDoc { collection, .. }
        | TextOp::HybridSearchTriple { collection, .. }
        | TextOp::SetTextConfig { collection, .. } => Some(collection.as_str()),
    }
}

pub(super) fn columnar_collection(op: &ColumnarOp) -> Option<&str> {
    match op {
        ColumnarOp::Scan { collection, .. }
        | ColumnarOp::Insert { collection, .. }
        | ColumnarOp::Update { collection, .. }
        | ColumnarOp::Delete { collection, .. }
        | ColumnarOp::ResolvedUpdate { collection, .. }
        | ColumnarOp::ResolvedDelete { collection, .. }
        | ColumnarOp::ResolveDml { collection, .. }
        | ColumnarOp::MaterializeScan { collection, .. }
        | ColumnarOp::Truncate { collection, .. } => Some(collection.as_str()),
    }
}

pub(super) fn timeseries_collection(op: &TimeseriesOp) -> Option<&str> {
    match op {
        TimeseriesOp::Scan { collection, .. }
        | TimeseriesOp::Ingest { collection, .. }
        | TimeseriesOp::Truncate { collection, .. }
        | TimeseriesOp::RefreshAggregate { collection, .. }
        | TimeseriesOp::Partitions { collection, .. }
        | TimeseriesOp::ReadAggregate { collection, .. } => Some(collection.as_str()),
        // Read-only resolve wrapper: it reports the wrapped ingest's
        // collection.
        TimeseriesOp::ResolveIngest(inner) => timeseries_collection(&inner.ingest),
    }
}

pub(super) fn spatial_collection(op: &SpatialOp) -> Option<&str> {
    match op {
        SpatialOp::Insert { collection, .. }
        | SpatialOp::Delete { collection, .. }
        | SpatialOp::Scan { collection, .. } => Some(collection.as_str()),
    }
}

#[cfg(test)]
mod tests {
    use nodedb_graph::{Direction, GraphTraversalOptions};
    use nodedb_physical::physical_plan::BatchEdge;
    use nodedb_types::geometry::{Geometry, SridGeometry};
    use nodedb_types::{DatabaseId, QualifiedCollection, RlsWriteCheck, Surrogate};

    use super::super::collection::extract_collection;
    use crate::bridge::envelope::PhysicalPlan;

    use super::*;

    fn docs() -> QualifiedCollection {
        QualifiedCollection::new(DatabaseId::DEFAULT, "docs")
    }

    /// Assert every plan reports `docs`.
    fn assert_names_docs(plans: Vec<PhysicalPlan>) {
        for plan in plans {
            assert_eq!(extract_collection(&plan), Some(docs().as_str()), "{plan:?}");
        }
    }

    fn backfill() -> DocumentOp {
        DocumentOp::BackfillIndex {
            collection: docs(),
            path: "$.f".into(),
            is_array: false,
            unique: false,
            case_insensitive: false,
            predicate: None,
        }
    }

    #[test]
    fn document_row_ops_name_their_collection() {
        assert_names_docs(vec![
            PhysicalPlan::Document(backfill()),
            PhysicalPlan::Document(DocumentOp::MaterializeScan {
                collection: docs(),
                cursor: Vec::new(),
                count: 0,
                system_time: nodedb_types::SystemTimeScope::Current,
                raw_bodies: false,
            }),
            PhysicalPlan::Document(DocumentOp::ApplyBalanceDelta {
                collection: docs(),
                document_id: "d".into(),
                surrogate: Surrogate::new(1),
                column: "balance".into(),
                delta: "1".into(),
                join_column: "id".into(),
                join_value: "d".into(),
                declared_primary_key: None,
            }),
            PhysicalPlan::Document(DocumentOp::ResolveWrite(Box::new(backfill()))),
        ]);
    }

    #[test]
    fn vector_row_ops_name_their_collection() {
        assert_names_docs(vec![
            PhysicalPlan::Vector(VectorOp::DeleteBySurrogate {
                collection: docs(),
                surrogate: None,
                field_name: String::new(),
                provenance: None,
            }),
            PhysicalPlan::Vector(VectorOp::QueryStats {
                collection: docs(),
                field_name: String::new(),
            }),
            PhysicalPlan::Vector(VectorOp::SparseInsert {
                collection: docs(),
                field_name: "terms".into(),
                doc_id: "d".into(),
                entries: vec![(1, 1.0)],
            }),
            PhysicalPlan::Vector(VectorOp::SparseSearch {
                collection: docs(),
                field_name: "terms".into(),
                query_entries: vec![(1, 1.0)],
                top_k: 1,
                filters: Vec::new(),
            }),
            PhysicalPlan::Vector(VectorOp::SparseDelete {
                collection: docs(),
                field_name: "terms".into(),
                doc_id: "d".into(),
            }),
            PhysicalPlan::Vector(VectorOp::MultiVectorInsert {
                collection: docs(),
                field_name: "tokens".into(),
                document_surrogate: Surrogate::new(1),
                pk_bytes: None,
                vectors: Vec::new(),
                count: 0,
                dim: 0,
            }),
            PhysicalPlan::Vector(VectorOp::MultiVectorDelete {
                collection: docs(),
                field_name: "tokens".into(),
                document_surrogate: Surrogate::new(1),
            }),
            PhysicalPlan::Vector(VectorOp::MultiVectorScoreSearch {
                collection: docs(),
                field_name: "tokens".into(),
                query_vector: vec![0.0],
                top_k: 1,
                ef_search: 0,
                mode: "max_sim".into(),
            }),
        ]);
    }

    fn edge_delete() -> GraphOp {
        GraphOp::EdgeDelete {
            collection: docs(),
            src_id: "a".into(),
            label: "L".into(),
            dst_id: "b".into(),
            src_surrogate: Surrogate::new(1),
            dst_surrogate: Surrogate::new(2),
            rls_write_check: RlsWriteCheck::NoPolicyApplies,
        }
    }

    #[test]
    fn graph_row_ops_name_their_collection() {
        let scoped = || Some(docs());
        assert_names_docs(vec![
            PhysicalPlan::Graph(GraphOp::EdgePut {
                collection: docs(),
                src_id: "a".into(),
                label: "L".into(),
                dst_id: "b".into(),
                properties: Vec::new(),
                src_surrogate: Surrogate::new(1),
                dst_surrogate: Surrogate::new(2),
                valid_from_ms: None,
                valid_until_ms: None,
            }),
            PhysicalPlan::Graph(edge_delete()),
            PhysicalPlan::Graph(GraphOp::ResolveEdgeDelete(Box::new(edge_delete()))),
            PhysicalPlan::Graph(GraphOp::Hop {
                collection: scoped(),
                start_nodes: Vec::new(),
                edge_labels: Vec::new(),
                direction: Direction::Out,
                depth: 1,
                options: GraphTraversalOptions::default(),
                rls_filters: Vec::new(),
                frontier_bitmap: None,
                scope: nodedb_physical::physical_plan::GraphReadScope::default(),
            }),
            PhysicalPlan::Graph(GraphOp::Neighbors {
                collection: scoped(),
                node_id: "n".into(),
                edge_labels: Vec::new(),
                direction: Direction::Out,
                rls_filters: Vec::new(),
                scope: nodedb_physical::physical_plan::GraphReadScope::default(),
            }),
            PhysicalPlan::Graph(GraphOp::NeighborsMulti {
                collection: scoped(),
                node_ids: Vec::new(),
                edge_labels: Vec::new(),
                direction: Direction::Out,
                max_results: 0,
                rls_filters: Vec::new(),
                edge_predicate: Vec::new(),
                with_properties: false,
                scope: nodedb_physical::physical_plan::GraphReadScope::default(),
            }),
            PhysicalPlan::Graph(GraphOp::Path {
                collection: scoped(),
                src: "a".into(),
                dst: "b".into(),
                edge_labels: Vec::new(),
                max_depth: 1,
                options: GraphTraversalOptions::default(),
                rls_filters: Vec::new(),
                frontier_bitmap: None,
                scope: nodedb_physical::physical_plan::GraphReadScope::default(),
            }),
            PhysicalPlan::Graph(GraphOp::Subgraph {
                collection: scoped(),
                start_nodes: Vec::new(),
                edge_labels: Vec::new(),
                depth: 1,
                options: GraphTraversalOptions::default(),
                rls_filters: Vec::new(),
                scope: nodedb_physical::physical_plan::GraphReadScope::default(),
            }),
            PhysicalPlan::Graph(GraphOp::Stats {
                collection: scoped(),
                as_of: None,
            }),
            PhysicalPlan::Graph(GraphOp::TemporalNeighbors {
                collection: docs(),
                node_id: "n".into(),
                edge_label: None,
                direction: Direction::Out,
                scope: nodedb_physical::physical_plan::GraphReadScope::default(),
                rls_filters: Vec::new(),
            }),
            PhysicalPlan::Graph(GraphOp::NodeEdgeGuard {
                collection: docs(),
                node_id: "n".into(),
                expected: Vec::<BatchEdge>::new(),
            }),
            PhysicalPlan::Graph(GraphOp::NodePresenceGuard {
                collection: docs(),
                vshard: 0,
                present: Vec::new(),
                absent: Vec::new(),
            }),
            PhysicalPlan::Graph(GraphOp::TruncateEdges {
                collection: docs(),
                vshard: 0,
            }),
            PhysicalPlan::Graph(GraphOp::NodePresenceRead {
                collection: docs(),
                vshard: 0,
                ids: Vec::new(),
            }),
        ]);
    }

    /// An unscoped traversal names no collection.
    #[test]
    fn an_unscoped_traversal_names_no_collection() {
        let plan = PhysicalPlan::Graph(GraphOp::Stats {
            collection: None,
            as_of: None,
        });
        assert_eq!(extract_collection(&plan), None);
    }

    #[test]
    fn columnar_and_spatial_row_ops_name_their_collection() {
        assert_names_docs(vec![
            PhysicalPlan::Columnar(ColumnarOp::ResolveDml {
                collection: docs(),
                filters: Vec::new(),
                updates: Vec::new(),
                is_update: true,
                rls_write_check: RlsWriteCheck::NoPolicyApplies,
            }),
            PhysicalPlan::Columnar(ColumnarOp::MaterializeScan {
                collection: docs(),
                cursor: Vec::new(),
                count: 0,
                system_as_of_ms: None,
            }),
            PhysicalPlan::Spatial(SpatialOp::Insert {
                collection: docs(),
                field: "location".into(),
                surrogate: Surrogate::new(1),
                geometry: SridGeometry::wgs84(Geometry::point(0.0, 0.0)),
                provenance: None,
            }),
            PhysicalPlan::Spatial(SpatialOp::Delete {
                collection: docs(),
                field: "location".into(),
                surrogate: None,
                provenance: None,
            }),
        ]);
    }
}
