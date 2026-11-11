// SPDX-License-Identifier: BUSL-1.1

//! The plan check a Calvin stage runs before it stages a plan: refuse every
//! plan that transaction resolve refuses from the plan alone.
//!
//! A Calvin resolve runs after the global COMMIT verdict. A resolve that
//! refuses a plan then cannot abort the txn, so the scheduler halts its
//! vShard. The stage runs first and can refuse: its refusal is an abort vote,
//! and the verdict aborts the txn on every participant.
//!
//! The check calls the resolve's own refusal arms, so the two never
//! disagree. It reads only the plan. Every replica holds the same sequenced
//! plan bytes, so every replica reaches the same verdict.

use std::collections::{BTreeMap, BTreeSet};

use nodedb_physical::physical_plan::PhysicalPlan;

use super::classify::{classify_document_op, classify_kv_op};
use super::{array, columnar_image, crdt, graph, timeseries, vector};

/// Refuse `plan` when transaction resolve refuses it from the plan alone.
///
/// Every refusal arm of the resolve plan walk (`resolve_txn_ops`) is checked
/// here. A refusal that depends on the staged overlay is an invariant break
/// of the stage, never a property of the plan, and is not checked.
pub(in crate::data::executor) fn check_calvin_resolvable(plan: &PhysicalPlan) -> crate::Result<()> {
    match plan {
        PhysicalPlan::Kv(op) => classify_kv_op(op, &mut BTreeSet::new()),
        PhysicalPlan::Document(op) => classify_document_op(op, &mut BTreeSet::new()),
        PhysicalPlan::Graph(op) => {
            graph::classify_graph_op(op, &mut BTreeSet::new(), &mut BTreeMap::new())
        }
        PhysicalPlan::Columnar(op) => {
            columnar_image::classify_columnar_op(op, &mut BTreeMap::new())
        }
        PhysicalPlan::Crdt(op) => crdt::refuse_crdt_op(op),
        PhysicalPlan::Vector(op) => vector::refuse_vector_op(op),
        PhysicalPlan::Array(op) => array::refuse_array_op(op),
        PhysicalPlan::Timeseries(op) => timeseries::refuse_timeseries_op(op),
        // Plan-driven families with no refusal arm, and the read-only ones.
        PhysicalPlan::Text(_)
        | PhysicalPlan::Spatial(_)
        | PhysicalPlan::Query(_)
        | PhysicalPlan::Meta(_) => Ok(()),
        PhysicalPlan::ClusterArray(_) | PhysicalPlan::ClusterEvent(_) => {
            Err(control_plane_only_op())
        }
    }
}

/// The refusal of a coordinator-only op, which is never legal on the Data
/// Plane.
pub(super) fn control_plane_only_op() -> crate::Error {
    crate::Error::Internal {
        detail: "Control-Plane-only op reached Data Plane transaction resolve".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::{
        CrdtOp, DocumentOp, KvOp, RefreshCut, TimeseriesOp, VectorOp,
    };
    use nodedb_types::{DatabaseId, QualifiedCollection, Surrogate};

    use super::*;

    fn qualified(name: &str) -> QualifiedCollection {
        QualifiedCollection::new(DatabaseId::DEFAULT, name)
    }

    /// Every op the resolve refuses from its plan alone is refused by the
    /// stage check with the same error kind.
    #[test]
    fn the_check_refuses_what_resolve_refuses() {
        let refused = [
            PhysicalPlan::Crdt(CrdtOp::Apply {
                collection: qualified("notes"),
                document_id: "n1".into(),
                delta: vec![1],
                peer_id: 1,
                mutation_id: 1,
                surrogate: Surrogate::new(5),
                provenance: None,
                constraint_version_required: 0,
                expected_frontier_digest: None,
            }),
            PhysicalPlan::Timeseries(TimeseriesOp::RefreshAggregate {
                collection: qualified("metrics"),
                cut: RefreshCut {
                    aggregate: "metrics_1m".to_owned(),
                    incarnation: nodedb_types::Hlc::new(7, 1),
                    refresh_id: 1,
                    periodic: false,
                },
            }),
            PhysicalPlan::Kv(KvOp::DropIndex {
                collection: qualified("kv"),
                field: "f".into(),
            }),
            PhysicalPlan::Vector(VectorOp::DropIndex {
                collection: qualified("vec"),
                field_name: "emb".into(),
            }),
        ];
        for plan in &refused {
            assert!(
                check_calvin_resolvable(plan).is_err(),
                "the stage check passes {plan:?}"
            );
        }
    }

    /// A plan the resolve serializes passes the check.
    #[test]
    fn the_check_passes_what_resolve_serializes() {
        let passed = [
            PhysicalPlan::Crdt(CrdtOp::DocDelete {
                collection: qualified("notes"),
                document_id: "n1".into(),
                surrogate: Some(Surrogate::new(5)),
                returning: None,
                rls_filters: Vec::new(),
            }),
            PhysicalPlan::Document(DocumentOp::Truncate {
                collection: qualified("docs"),
                restart_identity: false,
                resolved_sum_targets: Vec::new(),
                declared_primary_key: None,
            }),
            PhysicalPlan::Timeseries(TimeseriesOp::Truncate {
                collection: qualified("metrics"),
                restart_identity: false,
            }),
        ];
        for plan in &passed {
            check_calvin_resolvable(plan).expect("resolve serializes the plan");
        }
    }
}
