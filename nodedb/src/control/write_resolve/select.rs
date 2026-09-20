// SPDX-License-Identifier: BUSL-1.1

//! Pick the [`EngineWriteResolver`] for an intercepted plan.

use crate::bridge::envelope::PhysicalPlan;

use super::columnar::resolver_for_columnar_op;
use super::document::resolver_for_document_op;
use super::graph::resolver_for_graph_op;
use super::kv::resolver_for_kv_op;
use super::resolver::EngineWriteResolver;
use super::timeseries::resolver_for_timeseries_op;
use super::vector::resolver_for_vector_op;

/// The resolver for `plan`, or `None` when it carries no live predicate to
/// resolve before proposing. Exhaustive over `PhysicalPlan` — a new
/// state-dependent op with no resolver fails to compile here.
pub fn resolver_for_plan(plan: &PhysicalPlan) -> Option<Box<dyn EngineWriteResolver>> {
    match plan {
        PhysicalPlan::Columnar(op) => resolver_for_columnar_op(op),
        PhysicalPlan::Kv(op) => resolver_for_kv_op(op),
        PhysicalPlan::Document(op) => resolver_for_document_op(op),
        PhysicalPlan::Timeseries(op) => resolver_for_timeseries_op(op),
        PhysicalPlan::Graph(op) => resolver_for_graph_op(op),
        PhysicalPlan::Vector(op) => resolver_for_vector_op(op),
        PhysicalPlan::Text(_)
        | PhysicalPlan::Spatial(_)
        | PhysicalPlan::Crdt(_)
        | PhysicalPlan::Query(_)
        | PhysicalPlan::Meta(_)
        | PhysicalPlan::Array(_)
        | PhysicalPlan::ClusterArray(_)
        | PhysicalPlan::ClusterEvent(_) => None,
    }
}
