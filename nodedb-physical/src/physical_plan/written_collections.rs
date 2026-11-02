// SPDX-License-Identifier: Apache-2.0

//! Every collection a set of write plans writes.
//!
//! The Control Plane names them on a transaction's redo record and on its
//! admission keys. The Data Plane names them when it opens a transaction's
//! staging for a plan. A collection a plan only reads is harmless here: a
//! write note on it changes no stored version.

use std::collections::BTreeSet;

use super::{GraphOp, MetaOp, PhysicalPlan, SpatialOp, VectorOp};

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
    } else if let PhysicalPlan::Meta(MetaOp::RestoreRedo(batch)) = plan {
        out.extend(batch.collections.iter().cloned());
        // The Data Plane keys an array's writes by its bare name, as it
        // keys an array cell write's.
        out.extend(batch.arrays.iter().map(|cells| cells.array_id.name.clone()));
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
            surrogate: Some(nodedb_types::Surrogate::new(1)),
            provenance: None,
        });
        assert_eq!(
            written_collections(&[delete.clone(), delete]),
            vec!["places".to_string()]
        );
    }
}
