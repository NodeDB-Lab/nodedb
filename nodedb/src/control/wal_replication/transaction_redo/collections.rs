// SPDX-License-Identifier: BUSL-1.1

//! Every collection a transaction's buffered write plans write.
//!
//! The redo apply gives each one a collection-floor write version at the
//! record's LSN, as the transaction batch did for every written plan. A floor
//! on a collection the plans only read is harmless: it makes OCC validation
//! stricter, never looser.

use std::collections::BTreeSet;

use nodedb_physical::physical_plan::{GraphOp, PhysicalPlan, SpatialOp, VectorOp};

/// The written collections of `plans`, sorted and deduplicated.
pub fn written_collections(plans: &[PhysicalPlan]) -> Vec<String> {
    let mut collections: BTreeSet<String> = BTreeSet::new();
    for plan in plans {
        collect_plan(plan, &mut collections);
    }
    collections.into_iter().collect()
}

fn collect_plan(plan: &PhysicalPlan, out: &mut BTreeSet<String>) {
    // Ops whose single target `PhysicalPlan::collection` does not report.
    if let PhysicalPlan::Spatial(
        SpatialOp::Insert { collection, .. } | SpatialOp::Delete { collection, .. },
    )
    | PhysicalPlan::Graph(
        GraphOp::EdgePut { collection, .. } | GraphOp::EdgeDelete { collection, .. },
    )
    | PhysicalPlan::Vector(
        VectorOp::DeleteBySurrogate { collection, .. }
        | VectorOp::SparseInsert { collection, .. }
        | VectorOp::SparseDelete { collection, .. }
        | VectorOp::MultiVectorInsert { collection, .. }
        | VectorOp::MultiVectorDelete { collection, .. },
    ) = plan
    {
        out.insert(collection.to_string());
    } else if let PhysicalPlan::Graph(
        GraphOp::EdgePutBatch { edges } | GraphOp::EdgeDeleteBatch { edges },
    ) = plan
    {
        out.extend(edges.iter().map(|edge| edge.collection.to_string()));
    } else if let Some(collection) = plan.collection() {
        out.insert(collection.to_string());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::{DatabaseId, QualifiedCollection};

    #[test]
    fn spatial_writes_name_their_collection_once() {
        let delete = PhysicalPlan::Spatial(SpatialOp::Delete {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "places"),
            field: "loc".into(),
            surrogate: nodedb_types::Surrogate::new(1),
            provenance: None,
        });
        assert_eq!(
            written_collections(&[delete.clone(), delete]),
            vec!["places".to_string()]
        );
    }
}
