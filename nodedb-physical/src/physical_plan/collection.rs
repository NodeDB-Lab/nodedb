// SPDX-License-Identifier: Apache-2.0

//! Which user collection a physical plan targets.
//!
//! Kept beside the plan enum rather than inside it so `plan.rs` stays the
//! single declaration of the wire shape and nothing else.

use super::{
    ColumnarOp, DocumentOp, GraphOp, MetaOp, PhysicalPlan, QueryOp, SpatialOp, TextOp,
    TimeseriesOp, VectorOp,
};

impl PhysicalPlan {
    /// Primary read/target collection this plan touches, if it maps to
    /// exactly one user collection.
    ///
    /// Plane-neutral twin of the Control-Plane
    /// `crate::control::server::shared::plan_util::extract_collection` — the
    /// two MUST stay in sync (logic replicated, not called, since the core
    /// crate depends on `nodedb-physical`, not the reverse).
    pub fn collection(&self) -> Option<&str> {
        match self {
            PhysicalPlan::Document(DocumentOp::PointGet { collection, .. })
            | PhysicalPlan::Vector(VectorOp::Search { collection, .. })
            | PhysicalPlan::Document(DocumentOp::RangeScan { collection, .. })
            | PhysicalPlan::Vector(VectorOp::Insert { collection, .. })
            | PhysicalPlan::Vector(VectorOp::BatchInsert { collection, .. })
            | PhysicalPlan::Vector(VectorOp::MultiSearch { collection, .. })
            // A vector-primary row lives here only; `None` left it with no
            // collection to key a redaction policy on.
            | PhysicalPlan::Vector(VectorOp::DirectUpsert { collection, .. })
            | PhysicalPlan::Vector(VectorOp::DirectInsert { collection, .. })
            | PhysicalPlan::Vector(VectorOp::DirectInsertIfAbsent { collection, .. })
            | PhysicalPlan::Vector(VectorOp::DirectDelete { collection, .. })
            | PhysicalPlan::Vector(VectorOp::DirectTruncate { collection, .. })
            | PhysicalPlan::Vector(VectorOp::DirectUpdate { collection, .. })
            | PhysicalPlan::Vector(VectorOp::ResolvedDirectWrite { collection, .. })
            | PhysicalPlan::Vector(VectorOp::Delete { collection, .. })
            | PhysicalPlan::Document(DocumentOp::BatchInsert { collection, .. })
            | PhysicalPlan::Document(DocumentOp::PointPut { collection, .. })
            | PhysicalPlan::Document(DocumentOp::PointInsert { collection, .. })
            | PhysicalPlan::Document(DocumentOp::PointDelete { collection, .. })
            | PhysicalPlan::Document(DocumentOp::PointUpdate { collection, .. })
            | PhysicalPlan::Document(DocumentOp::Scan { collection, .. })
            | PhysicalPlan::Query(QueryOp::Aggregate { collection, .. })
            | PhysicalPlan::Query(QueryOp::HashJoin {
                left_collection: collection,
                ..
            })
            | PhysicalPlan::Query(QueryOp::NestedLoopJoin {
                left_collection: collection,
                ..
            })
            | PhysicalPlan::Graph(GraphOp::RagFusion { collection, .. })
            | PhysicalPlan::Vector(VectorOp::SetParams { collection, .. })
            | PhysicalPlan::Text(TextOp::Search { collection, .. })
            | PhysicalPlan::Text(TextOp::PhraseSearch { collection, .. })
            | PhysicalPlan::Text(TextOp::HybridSearch { collection, .. })
            | PhysicalPlan::Text(TextOp::HybridSearchTriple { collection, .. })
            | PhysicalPlan::Text(TextOp::BM25ScoreScan { collection, .. })
            | PhysicalPlan::Text(TextOp::FtsIndexDoc { collection, .. })
            | PhysicalPlan::Text(TextOp::FtsDeleteDoc { collection, .. })
            | PhysicalPlan::Text(TextOp::SetTextConfig { collection, .. })
            | PhysicalPlan::Query(QueryOp::PartialAggregate { collection, .. })
            | PhysicalPlan::Query(QueryOp::FacetCounts { collection, .. })
            | PhysicalPlan::Document(DocumentOp::BulkUpdate { collection, .. })
            | PhysicalPlan::Document(DocumentOp::BulkDelete { collection, .. })
            | PhysicalPlan::Document(DocumentOp::Upsert { collection, .. })
            | PhysicalPlan::Document(DocumentOp::InsertSelect {
                target_collection: collection,
                ..
            })
            | PhysicalPlan::Document(DocumentOp::Truncate { collection, .. })
            | PhysicalPlan::Document(DocumentOp::EstimateCount { collection, .. })
            | PhysicalPlan::Columnar(ColumnarOp::Scan { collection, .. })
            | PhysicalPlan::Columnar(ColumnarOp::Insert { collection, .. })
            | PhysicalPlan::Columnar(ColumnarOp::Update { collection, .. })
            | PhysicalPlan::Columnar(ColumnarOp::Delete { collection, .. })
            | PhysicalPlan::Columnar(ColumnarOp::ResolvedUpdate { collection, .. })
            | PhysicalPlan::Columnar(ColumnarOp::ResolvedDelete { collection, .. })
            | PhysicalPlan::Columnar(ColumnarOp::Truncate { collection, .. })
            | PhysicalPlan::Timeseries(TimeseriesOp::Scan { collection, .. })
            | PhysicalPlan::Timeseries(TimeseriesOp::Ingest { collection, .. })
            | PhysicalPlan::Timeseries(TimeseriesOp::Truncate { collection, .. })
            | PhysicalPlan::Spatial(SpatialOp::Scan { collection, .. })
            | PhysicalPlan::Document(DocumentOp::Register { collection, .. })
            | PhysicalPlan::Document(DocumentOp::IndexLookup { collection, .. })
            | PhysicalPlan::Document(DocumentOp::IndexedFetch { collection, .. })
            | PhysicalPlan::Document(DocumentOp::DropIndex { collection, .. }) => {
                Some(collection.as_str())
            }
            // Read-only resolve wrapper: it reports the wrapped ingest's
            // collection, which is what the propose step routes on.
            PhysicalPlan::Timeseries(TimeseriesOp::ResolveIngest(inner)) => match inner.as_ref() {
                TimeseriesOp::Scan { collection, .. }
                | TimeseriesOp::Ingest { collection, .. }
                | TimeseriesOp::Truncate { collection, .. } => Some(collection.as_str()),
                TimeseriesOp::ResolveIngest(_) => None,
            },
            // Same shape on the graph side, and `EdgeDelete` itself reports
            // `None` here: an edge plan is key-homed on its endpoints.
            PhysicalPlan::Graph(GraphOp::ResolveEdgeDelete(_)) => None,
            PhysicalPlan::Graph(GraphOp::EdgePut { .. })
            | PhysicalPlan::Graph(GraphOp::EdgeDelete { .. })
            | PhysicalPlan::Graph(GraphOp::Hop { .. })
            | PhysicalPlan::Graph(GraphOp::Neighbors { .. })
            | PhysicalPlan::Graph(GraphOp::Path { .. })
            | PhysicalPlan::Graph(GraphOp::Subgraph { .. })
            | PhysicalPlan::Meta(MetaOp::WalAppend { .. })
            | PhysicalPlan::Meta(MetaOp::Cancel { .. })
            | PhysicalPlan::Meta(MetaOp::TransactionBatch { .. })
            | PhysicalPlan::Meta(MetaOp::CreateSnapshot)
            | PhysicalPlan::Meta(MetaOp::Compact)
            | PhysicalPlan::Meta(MetaOp::Checkpoint)
            | PhysicalPlan::Graph(GraphOp::Algo { .. })
            | PhysicalPlan::Graph(GraphOp::Match { .. })
            | PhysicalPlan::Graph(GraphOp::MatchContinuation { .. })
            | PhysicalPlan::Graph(GraphOp::MatchVarLenResume { .. })
            | PhysicalPlan::Graph(GraphOp::BspSuperstep(_))
            | PhysicalPlan::Graph(GraphOp::WccSuperstep(_)) => None,
            // Read-only resolve wrapper: it reports the wrapped write's collection.
            PhysicalPlan::Vector(VectorOp::ResolveDirectWrite(inner)) => {
                inner.direct_write_collection()
            }
            // Exchange: recurse into the child plan to extract the collection.
            PhysicalPlan::Query(QueryOp::Exchange(op)) => op.child.collection(),
            // PostProcess: recurse into the materialized input plan.
            PhysicalPlan::Query(QueryOp::PostProcess { input, .. }) => input.collection(),
            // SetOp merges N branches; no single collection names the node.
            PhysicalPlan::Query(QueryOp::SetOp { .. }) => None,
            // ProviderScan is a catalog/constant source — no user collection.
            PhysicalPlan::Query(QueryOp::ProviderScan { .. }) => None,
            // KV ops carry their own collection (sorted-index-only ops → None).
            PhysicalPlan::Kv(op) => op.collection(),
            // Every CRDT op is scoped to one collection's Loro document, so the
            // accessor is total over all 20 variants. Listing a subset here let
            // a history read or a constraint install report `None` and lose the
            // collection its policy, clone and metering scoping keys on.
            PhysicalPlan::Crdt(op) => Some(op.collection().as_str()),
            // Remaining ops carry no extractable collection. Exhaustive so a
            // new variant forces a decision rather than silently returning None.
            PhysicalPlan::Document(_)
            | PhysicalPlan::Vector(_)
            | PhysicalPlan::Graph(_)
            | PhysicalPlan::Columnar(_)
            | PhysicalPlan::Spatial(_)
            | PhysicalPlan::Query(_)
            | PhysicalPlan::Meta(_)
            | PhysicalPlan::ClusterEvent(_) => None,
            // An array is a collection for read-set tracking and the commit
            // validator's own-write exclusion: a same-transaction slice after a
            // staged put must key on the name the put's write floor records.
            PhysicalPlan::Array(op) => Some(op.primary_array().name.as_str()),
            PhysicalPlan::ClusterArray(op) => Some(op.array_id().name.as_str()),
        }
    }

    /// Every user collection this plan names: each collection a committed
    /// redo install writes, or else the one [`Self::collection`] reports.
    ///
    /// A committed-redo apply and a Calvin flush install one record that can
    /// write several collections, so [`Self::collection`] reports none for
    /// them. A caller that keys on a collection name uses this instead.
    pub fn named_collections(&self) -> Vec<&str> {
        if let PhysicalPlan::Meta(
            MetaOp::ApplyTransactionRedo { collections, .. }
            | MetaOp::CalvinFlush { collections, .. },
        ) = self
        {
            collections.iter().map(String::as_str).collect()
        } else {
            self.collection().into_iter().collect()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::{DatabaseId, QualifiedCollection};

    use crate::physical_plan::KvOp;

    #[test]
    fn a_redo_install_names_every_collection_it_writes() {
        let collections = vec!["a".to_string(), "b".to_string()];
        let redo = PhysicalPlan::Meta(MetaOp::ApplyTransactionRedo {
            redo: Vec::new(),
            collections: collections.clone(),
            sum_targets: Vec::new(),
            origin: crate::physical_plan::RedoOrigin::Commit,
        });
        let flush = PhysicalPlan::Meta(MetaOp::CalvinFlush {
            epoch: 1,
            position: 0,
            redo: Vec::new(),
            collections,
            sum_targets: Vec::new(),
        });
        for plan in [redo, flush] {
            assert_eq!(plan.collection(), None);
            assert_eq!(plan.named_collections(), vec!["a", "b"]);
        }
    }

    #[test]
    fn a_single_collection_plan_names_its_collection() {
        let get = PhysicalPlan::Kv(KvOp::Get {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "users"),
            key: Vec::new(),
            rls_filters: Vec::new(),
            surrogate_ceiling: None,
        });
        assert_eq!(get.named_collections(), vec!["users"]);
        assert!(
            PhysicalPlan::Meta(MetaOp::Checkpoint)
                .named_collections()
                .is_empty()
        );
    }
}
