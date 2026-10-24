// SPDX-License-Identifier: BUSL-1.1

//! Which user collection a physical plan reads or writes.
//!
//! Redaction, clone interception, read-set tracking, own-write floors, and
//! metering all key on this answer. Every operation family is matched
//! variant by variant, so a new variant fails to compile until it is decided.

use crate::bridge::envelope::PhysicalPlan;
use nodedb_physical::physical_plan::{DocumentOp, GraphOp, KvOp};

use super::collection_meta::{cluster_event_collection, meta_collection, query_collection};
use super::collection_rows::{
    columnar_collection, document_collection, graph_collection, spatial_collection,
    text_collection, timeseries_collection, vector_collection,
};

/// The one user collection `plan` reads or writes, or `None` when it names
/// none or several.
pub(crate) fn extract_collection(plan: &PhysicalPlan) -> Option<&str> {
    match plan {
        PhysicalPlan::Document(op) => document_collection(op),
        PhysicalPlan::Vector(op) => vector_collection(op),
        PhysicalPlan::Graph(op) => graph_collection(op),
        // KV ops carry their own collection. Sorted-index-only ops and a
        // `ResolvedWrite` that can span two collections report `None`.
        PhysicalPlan::Kv(op) => op.collection(),
        PhysicalPlan::Text(op) => text_collection(op),
        PhysicalPlan::Columnar(op) => columnar_collection(op),
        PhysicalPlan::Timeseries(op) => timeseries_collection(op),
        PhysicalPlan::Spatial(op) => spatial_collection(op),
        // Every CRDT op is scoped to one collection's Loro document, so the
        // accessor is total over its variants.
        PhysicalPlan::Crdt(op) => Some(op.collection().as_str()),
        PhysicalPlan::Query(op) => query_collection(op),
        PhysicalPlan::Meta(op) => meta_collection(op),
        // An array is a collection for read-set tracking and the commit
        // validator's own-write exclusion: a same-transaction slice after a
        // staged put must key on the name the put's write floor records.
        PhysicalPlan::Array(op) => Some(op.primary_array().name.as_str()),
        PhysicalPlan::ClusterArray(op) => Some(op.array_id().name.as_str()),
        PhysicalPlan::ClusterEvent(op) => cluster_event_collection(op),
    }
}

/// Every collection a plan's work must be metered against, first-seen order,
/// no duplicates. [`extract_collection`] answers "which one collection" and
/// reports `None` for a write that can span several: a KV or document
/// `ResolvedWrite`, a `TransferItem`, and an edge batch.
pub(crate) fn metered_collections(plan: &PhysicalPlan) -> Vec<String> {
    let mut out = Vec::new();
    if let PhysicalPlan::Kv(KvOp::TransferItem {
        source_collection,
        dest_collection,
        ..
    }) = plan
    {
        push_distinct(&mut out, source_collection.as_str());
        push_distinct(&mut out, dest_collection.as_str());
        return out;
    }
    if let PhysicalPlan::Kv(KvOp::ResolvedWrite { mutations, .. }) = plan {
        for mutation in mutations {
            push_distinct(&mut out, mutation.collection().as_str());
        }
        return out;
    }
    if let PhysicalPlan::Document(DocumentOp::ResolvedWrite { mutations, .. }) = plan {
        for mutation in mutations {
            push_distinct(&mut out, mutation.collection().as_str());
        }
        return out;
    }
    if let PhysicalPlan::Graph(
        GraphOp::EdgePutBatch { edges } | GraphOp::EdgeDeleteBatch { edges },
    ) = plan
    {
        for edge in edges {
            push_distinct(&mut out, edge.collection.as_str());
        }
        return out;
    }
    // Every other plan identifies one collection at most.
    out.extend(extract_collection(plan).map(str::to_string));
    out
}

/// Append `collection` unless it is already present. The list holds at most a
/// handful of names, so a linear scan beats building a set.
fn push_distinct(out: &mut Vec<String>, collection: &str) {
    if !out.iter().any(|held| held == collection) {
        out.push(collection.to_string());
    }
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::BatchEdge;
    use nodedb_types::{DatabaseId, QualifiedCollection, Surrogate};

    use super::*;

    fn edge(collection: &str) -> BatchEdge {
        BatchEdge {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, collection),
            src_id: "a".into(),
            label: "L".into(),
            dst_id: "b".into(),
            src_surrogate: Surrogate::new(1),
            dst_surrogate: Surrogate::new(2),
            write_gate: nodedb_types::RlsWriteCheck::NoPolicyApplies,
        }
    }

    /// An edge batch can span collections, so it names none on its own and
    /// meters each collection its edges write.
    #[test]
    fn an_edge_batch_meters_every_collection_it_writes() {
        let qualified = |c: &str| {
            QualifiedCollection::new(DatabaseId::DEFAULT, c)
                .as_str()
                .to_string()
        };
        for plan in [
            PhysicalPlan::Graph(GraphOp::EdgePutBatch {
                edges: vec![edge("a"), edge("b"), edge("a")],
            }),
            PhysicalPlan::Graph(GraphOp::EdgeDeleteBatch {
                edges: vec![edge("a"), edge("b")],
            }),
        ] {
            assert_eq!(extract_collection(&plan), None, "{plan:?}");
            assert_eq!(
                metered_collections(&plan),
                vec![qualified("a"), qualified("b")],
                "{plan:?}"
            );
        }
    }
}
