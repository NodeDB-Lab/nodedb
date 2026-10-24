// SPDX-License-Identifier: BUSL-1.1

//! Write keys of a RESTORE batch.
//!
//! A restored row locks what every point writer of the row locks: its
//! surrogate and its row id. A restored edge version locks what every edge
//! write locks: its surrogate pair and both endpoints' node lock pairs, on
//! both endpoint homes. Restored node labels lock what every label write of
//! the node locks. So a restored write orders against every other writer of
//! the same row, edge or node labels, and against a node delete's guard.

use nodedb_physical::physical_plan::RestoredRedo;

use super::graph::NODE_LABEL_LOCKS;
use super::plan::unsequenced;
use super::set::WriteKeys;

/// Add the write keys of the RESTORE batch `batch` to `keys`.
pub(super) fn add_keys(keys: &mut WriteKeys, batch: &RestoredRedo) -> crate::Result<()> {
    if batch.rows.is_empty() && batch.edges.is_empty() && batch.node_labels.is_empty() {
        return Err(unsequenced(
            "a RESTORE batch carries no row, no edge and no node label",
        ));
    }
    for row in &batch.rows {
        keys.rows(&row.collection, [row.surrogate]);
        keys.row_id(&row.collection, &row.document_id);
    }
    for edge in &batch.edges {
        keys.edge(
            &edge.collection,
            &edge.src_id,
            &edge.dst_id,
            (edge.src_surrogate, edge.dst_surrogate),
        );
    }
    for node in &batch.node_labels {
        keys.node(NODE_LABEL_LOCKS, &node.node_id);
    }
    Ok(())
}
