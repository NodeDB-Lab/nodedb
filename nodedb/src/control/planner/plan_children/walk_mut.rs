// SPDX-License-Identifier: BUSL-1.1

//! The plans a plan reads its rows from, one level down, for an in-place
//! rewrite.

use nodedb_physical::physical_plan::PhysicalPlan;

use super::{document, query, vector};

/// The plans whose rows `plan` reads, mutable. Exhaustive over
/// [`PhysicalPlan`], and the same plans [`super::children`] names.
pub fn children_mut(plan: &mut PhysicalPlan) -> Vec<&mut PhysicalPlan> {
    match plan {
        PhysicalPlan::Query(op) => query::children_mut(op),
        PhysicalPlan::Document(op) => document::child_mut(op).into_iter().collect(),
        PhysicalPlan::Vector(op) => vector::vector_child_mut(op).into_iter().collect(),
        PhysicalPlan::Graph(_)
        | PhysicalPlan::Kv(_)
        | PhysicalPlan::Text(_)
        | PhysicalPlan::Columnar(_)
        | PhysicalPlan::Timeseries(_)
        | PhysicalPlan::Spatial(_)
        | PhysicalPlan::Crdt(_)
        | PhysicalPlan::Meta(_)
        | PhysicalPlan::Array(_)
        | PhysicalPlan::ClusterArray(_)
        | PhysicalPlan::ClusterEvent(_) => Vec::new(),
    }
}
