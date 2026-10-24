// SPDX-License-Identifier: BUSL-1.1

//! Slice a Calvin transaction's edge batches by the homes of their edges.
//!
//! An edge lives on both endpoint homes: its source's key home, which owns
//! it, and its destination's. A batch edge op routes to the homes of all its
//! edges, so one batch can span hundreds of vShards, past what one sequencer
//! entry targets, and every home will stage every edge of it.
//!
//! Before a transaction is submitted, each batch is split into one batch per
//! home pair `(source home, destination home)`. Each slice routes to at most
//! two vShards, so no task spans more vShards than one part allows. Each
//! home stages exactly the edges it holds: both homes of an edge receive it,
//! each with its own copy.
//!
//! Every edge of a slice has the same owner, so the slice's count is
//! counted once, at that owner (see `stage_calvin_plan`). The coordinator of
//! a transaction with no write of its own sums its homes' answers (see
//! `ReplyFold`).

use std::collections::BTreeMap;

use nodedb_cluster::calvin::types::{EngineKeySet, TxClass};
use nodedb_physical::physical_plan::{BatchEdge, GraphOp, PhysicalPlan};

use crate::Error;
use crate::types::{RecordHomes, VShardId};

/// `tx_class`'s decoded plans after edge slicing.
pub(crate) struct SlicedPlans {
    /// The plans, each edge batch split by home pair.
    pub plans: Vec<PhysicalPlan>,
    /// Whether a batch split, so `plans` no longer match the class's plan
    /// bytes.
    pub sliced: bool,
}

/// Split every edge batch of `plans`, `tx_class`'s decoded plans, that spans
/// more than one home pair. Each edge moves into its slice. A class with no
/// edge write, or one already split into parts, keeps its plans as they
/// are. `body_plans` follows the split: a slice of a body task is a body
/// task. `tx_class.body_plans` is sorted and deduplicated, and stays so.
pub(crate) fn slice_edge_batches(
    tx_class: &mut TxClass,
    plans: Vec<PhysicalPlan>,
) -> crate::Result<SlicedPlans> {
    if tx_class.is_multi_part() || !writes_edges(tx_class) {
        return Ok(SlicedPlans {
            plans,
            sliced: false,
        });
    }
    let mut out: Vec<PhysicalPlan> = Vec::with_capacity(plans.len());
    let mut body_plans: Vec<u32> = Vec::with_capacity(tx_class.body_plans.len());
    let mut sliced = false;
    for (task, plan) in (0u32..).zip(plans) {
        let body = tx_class.body_plans.binary_search(&task).is_ok();
        let slices = batch_slices(plan);
        sliced |= slices.len() > 1;
        for slice in slices {
            if body {
                body_plans.push(index_of(out.len())?);
            }
            out.push(slice);
        }
    }
    if sliced {
        tx_class.body_plans = body_plans;
    }
    Ok(SlicedPlans { plans: out, sliced })
}

/// Whether `tx_class` writes an edge: its write set names edge keys.
pub(crate) fn writes_edges(tx_class: &TxClass) -> bool {
    tx_class
        .write_set
        .0
        .iter()
        .any(|keys| matches!(keys, EngineKeySet::Edge { .. }))
}

/// The per-home-pair slices of `plan`, in home-pair order, when it is an
/// edge batch. Every other plan, and a batch over one home pair, is its
/// own one slice. Each edge moves into its slice.
fn batch_slices(plan: PhysicalPlan) -> Vec<PhysicalPlan> {
    let (edges, delete) = match plan {
        PhysicalPlan::Graph(GraphOp::EdgePutBatch { edges }) => (edges, false),
        PhysicalPlan::Graph(GraphOp::EdgeDeleteBatch { edges }) => (edges, true),
        other => return vec![other],
    };
    let mut groups: BTreeMap<(u32, u32), Vec<BatchEdge>> = BTreeMap::new();
    for edge in edges {
        groups.entry(home_pair(&edge)).or_default().push(edge);
    }
    // One group holds every edge in its batch order, so the batch rebuilds
    // whole. An empty batch stays one empty batch.
    if groups.is_empty() {
        return vec![edge_batch(Vec::new(), delete)];
    }
    groups
        .into_values()
        .map(|edges| edge_batch(edges, delete))
        .collect()
}

/// An edge put batch of `edges`, or a delete batch when `delete`.
fn edge_batch(edges: Vec<BatchEdge>, delete: bool) -> PhysicalPlan {
    PhysicalPlan::Graph(if delete {
        GraphOp::EdgeDeleteBatch { edges }
    } else {
        GraphOp::EdgePutBatch { edges }
    })
}

/// `(source home, destination home)` of `edge`. The source home owns it.
fn home_pair(edge: &BatchEdge) -> (u32, u32) {
    (
        RecordHomes::edge_owner(&edge.src_id).as_u32(),
        VShardId::from_key(edge.dst_id.as_bytes()).as_u32(),
    )
}

fn index_of(len: usize) -> crate::Result<u32> {
    u32::try_from(len).map_err(|_| Error::BadRequest {
        detail: format!("a transaction of {len} tasks is more than one transaction indexes"),
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use nodedb_types::{DatabaseId, QualifiedCollection, Surrogate};

    use super::*;
    use crate::control::cluster::calvin::scheduler::driver::core::routing::{
        PlanRouting, plan_vshard_in_database,
    };

    fn edge(src: String, dst: String) -> BatchEdge {
        BatchEdge {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "g"),
            src_id: src,
            label: "L".into(),
            dst_id: dst,
            src_surrogate: Surrogate::new(1),
            dst_surrogate: Surrogate::new(2),
            valid_from_ms: None,
            valid_until_ms: None,
        }
    }

    /// A batch over 400 home pairs splits into one slice per pair. Each
    /// slice routes to at most two vShards, every edge lands in exactly one
    /// slice, and every edge of a slice has the same owner.
    #[test]
    fn a_wide_batch_splits_into_slices_of_one_home_pair() {
        let edges: Vec<BatchEdge> = (0..400)
            .map(|i| edge(format!("src{i}"), format!("dst{i}")))
            .collect();
        let pairs: BTreeSet<(u32, u32)> = edges.iter().map(home_pair).collect();
        let plan = PhysicalPlan::Graph(GraphOp::EdgePutBatch {
            edges: edges.clone(),
        });
        let slices = batch_slices(plan);
        assert_eq!(slices.len(), pairs.len(), "a wide batch splits");
        let mut seen = 0;
        for slice in &slices {
            let PhysicalPlan::Graph(GraphOp::EdgePutBatch { edges }) = slice else {
                panic!("a slice stays a put batch");
            };
            seen += edges.len();
            let owners: BTreeSet<u32> = edges
                .iter()
                .map(|e| RecordHomes::edge_owner(&e.src_id).as_u32())
                .collect();
            assert_eq!(owners.len(), 1, "one owner per slice");
            match plan_vshard_in_database(slice, DatabaseId::DEFAULT) {
                PlanRouting::Vshards(homes) => assert!(homes.len() <= 2),
                _ => panic!("a slice routes to its homes"),
            }
        }
        assert_eq!(seen, edges.len(), "every edge lands in one slice");

        let one_pair = PhysicalPlan::Graph(GraphOp::EdgePutBatch {
            edges: vec![edge("a".into(), "b".into())],
        });
        assert_eq!(
            batch_slices(one_pair.clone()),
            vec![one_pair],
            "one home pair stays whole"
        );
    }
}
