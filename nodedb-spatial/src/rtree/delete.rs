// SPDX-License-Identifier: Apache-2.0

//! R-tree entry deletion with underflow handling.

use nodedb_types::BboxSpace;

use super::node::{EntryId, NodeKind, RTreeEntry};
use super::tree::{RTree, collect_entries_owned};

impl RTree {
    /// Delete an entry by ID. Returns true if found and removed.
    pub fn delete(&mut self, id: EntryId) -> bool {
        let removed = delete_entry(&mut self.nodes, self.root, id, self.space);
        if let Some(orphans) = removed {
            self.history.take_live(id);
            self.len -= 1;
            for entry in orphans {
                self.reinsert_entry(entry);
            }
            self.condense_root();
            true
        } else {
            false
        }
    }
}

/// Recursively delete entry, returning orphaned entries on underflow.
fn delete_entry(
    nodes: &mut Vec<super::node::Node>,
    node_idx: usize,
    id: EntryId,
    space: BboxSpace,
) -> Option<Vec<RTreeEntry>> {
    let is_leaf = nodes[node_idx].is_leaf();

    if is_leaf {
        if let NodeKind::Leaf { entries } = &nodes[node_idx].kind {
            let pos = entries.iter().position(|e| e.id == id);
            if let Some(pos) = pos {
                if let NodeKind::Leaf { entries } = &mut nodes[node_idx].kind {
                    entries.remove(pos);
                }
                nodes[node_idx].recompute_bbox(space);
                return Some(Vec::new());
            }
        }
        return None;
    }

    // Internal node — recurse into children.
    let child_indices: Vec<usize> = if let NodeKind::Internal { children } = &nodes[node_idx].kind {
        children.iter().map(|c| c.node_idx).collect()
    } else {
        return None;
    };

    for child_idx in child_indices {
        if let Some(mut orphans) = delete_entry(nodes, child_idx, id, space) {
            // Update child bbox in parent.
            let child_bbox = nodes[child_idx].bbox;
            if let NodeKind::Internal { children } = &mut nodes[node_idx].kind {
                for c in children.iter_mut() {
                    if c.node_idx == child_idx {
                        c.bbox = child_bbox;
                        break;
                    }
                }
            }
            nodes[node_idx].recompute_bbox(space);

            // Check underflow.
            if nodes[child_idx].is_underflow() {
                let mut collected = Vec::new();
                collect_entries_owned(nodes, child_idx, &mut collected);
                if let NodeKind::Internal { children } = &mut nodes[node_idx].kind {
                    children.retain(|c| c.node_idx != child_idx);
                }
                nodes[node_idx].recompute_bbox(space);
                orphans.extend(collected);
            }

            return Some(orphans);
        }
    }
    None
}
