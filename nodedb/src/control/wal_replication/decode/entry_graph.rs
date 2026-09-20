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
        } => Ok(graph::edge_put(graph::EdgePutFields {
            collection,
            src_id,
            label,
            dst_id,
            properties,
            src_surrogate: *src_surrogate,
            dst_surrogate: *dst_surrogate,
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
