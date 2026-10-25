// SPDX-License-Identifier: BUSL-1.1

//! Grouped decode arm for `ReplicatedWrite` variants that produce
//! `PhysicalPlan::Graph`.
//!
//! Delegated from `decode/entry.rs`'s single grouped match arm. `write` is
//! guaranteed by the caller to already be one of these variants — see
//! `entry_document::decode_arm` for the trailing-arm contract.

use super::super::types::ReplicatedWrite;
use super::graph;
use crate::bridge::envelope::PhysicalPlan;

/// The edge or label write `write` carries, rebuilt as a plan, or `None`
/// for any other write. Every edge and label write runs as a Calvin
/// transaction (`planner::calvin::graph_sequencing`), so the propose seams
/// hand it there.
pub fn sequenced_graph_write_plan(write: &ReplicatedWrite) -> crate::Result<Option<PhysicalPlan>> {
    if matches!(
        write,
        ReplicatedWrite::EdgePut { .. }
            | ReplicatedWrite::EdgeDelete { .. }
            | ReplicatedWrite::EdgePutBatch { .. }
            | ReplicatedWrite::EdgeDeleteBatch { .. }
            | ReplicatedWrite::SetNodeLabels { .. }
            | ReplicatedWrite::RemoveNodeLabels { .. }
    ) {
        decode_arm(write).map(Some)
    } else {
        Ok(None)
    }
}

pub(super) fn decode_arm(write: &ReplicatedWrite) -> crate::Result<PhysicalPlan> {
    match write {
        ReplicatedWrite::EdgePut {
            collection,
            src_id,
            label,
            dst_id,
            properties,
            src_surrogate,
            dst_surrogate,
            valid_from_ms,
            valid_until_ms,
        } => Ok(graph::edge_put(graph::EdgePutFields {
            collection,
            src_id,
            label,
            dst_id,
            properties,
            src_surrogate: *src_surrogate,
            dst_surrogate: *dst_surrogate,
            valid_from_ms: *valid_from_ms,
            valid_until_ms: *valid_until_ms,
        })),
        ReplicatedWrite::EdgeDelete {
            collection,
            src_id,
            label,
            dst_id,
            src_surrogate,
            dst_surrogate,
        } => Ok(graph::edge_delete(
            collection,
            src_id,
            label,
            dst_id,
            *src_surrogate,
            *dst_surrogate,
        )),
        ReplicatedWrite::SetNodeLabels { node_id, labels } => {
            Ok(graph::set_node_labels(node_id, labels))
        }
        ReplicatedWrite::RemoveNodeLabels { node_id, labels } => {
            Ok(graph::remove_node_labels(node_id, labels))
        }
        ReplicatedWrite::EdgePutBatch { edges } => Ok(graph::edge_put_batch(edges)),
        ReplicatedWrite::EdgeDeleteBatch { edges } => Ok(graph::edge_delete_batch(edges)),
        _ => Err(crate::Error::Internal {
            detail: "entry_graph::decode_arm called with a non-Graph ReplicatedWrite variant \
                (dispatch bug in decode/entry.rs's grouped Graph match arm)"
                .into(),
        }),
    }
}
