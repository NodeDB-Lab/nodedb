// SPDX-License-Identifier: Apache-2.0

//! Sort-Tile-Recursive (STR) bulk loading for R-tree.

use nodedb_mem::ScopedMemory;
use nodedb_types::BboxSpace;

use super::node::{ChildRef, INTERNAL_CAPACITY, LEAF_CAPACITY, Node, NodeKind, RTreeEntry};
use super::tree::RTree;
use super::tree_srid::TreeSrid;

impl RTree {
    /// Bulk load entries in `srid` using Sort-Tile-Recursive packing, tracked
    /// against `memory`.
    ///
    /// More efficient than repeated single inserts for large datasets.
    /// Produces better packing (less overlap between nodes).
    pub fn bulk_load(entries: Vec<RTreeEntry>, memory: ScopedMemory, srid: TreeSrid) -> Self {
        if entries.is_empty() {
            return Self::new(memory, srid);
        }
        let space = srid.space;
        let len = entries.len();

        // Reserve budget for nodes vec (approximately len / LEAF_CAPACITY nodes,
        // each holding LEAF_CAPACITY RTreeEntry slots). Best-effort: budget
        // pressure is a backpressure signal, not a hard gate on bulk load.
        let node_count = len.div_ceil(LEAF_CAPACITY) * 2; // leaves + internals
        let bytes = node_count
            * (std::mem::size_of::<Node>() + LEAF_CAPACITY * std::mem::size_of::<RTreeEntry>());
        let _guard = memory.reserve(bytes).ok();

        let mut tree = Self {
            nodes: Vec::new(),
            root: 0,
            len,
            srid: srid.srid,
            space,
            memory,
        };
        tree.root = str_pack(&mut tree.nodes, entries, space);
        tree
    }
}

/// Recursively pack entries into leaf nodes, then group into internal nodes.
fn str_pack(nodes: &mut Vec<Node>, mut entries: Vec<RTreeEntry>, space: BboxSpace) -> usize {
    if entries.len() <= LEAF_CAPACITY {
        let mut node = Node::new_leaf();
        if let NodeKind::Leaf {
            entries: ref mut leaf,
        } = node.kind
        {
            *leaf = entries;
        }
        node.recompute_bbox(space);
        let idx = nodes.len();
        nodes.push(node);
        return idx;
    }

    let num_leaves = entries.len().div_ceil(LEAF_CAPACITY);
    let num_slices = (num_leaves as f64).sqrt().ceil() as usize;
    let slice_size = entries.len().div_ceil(num_slices);

    // Sort by longitude (X).
    entries.sort_by(|a, b| {
        let ca = (a.bbox.min_lng + a.bbox.max_lng) / 2.0;
        let cb = (b.bbox.min_lng + b.bbox.max_lng) / 2.0;
        ca.partial_cmp(&cb).unwrap_or(std::cmp::Ordering::Equal)
    });

    let mut child_nodes: Vec<usize> = Vec::new();

    for slice in entries.chunks(slice_size) {
        let mut slice_vec: Vec<RTreeEntry> = slice.to_vec();
        // Sort each slice by latitude (Y).
        slice_vec.sort_by(|a, b| {
            let ca = (a.bbox.min_lat + a.bbox.max_lat) / 2.0;
            let cb = (b.bbox.min_lat + b.bbox.max_lat) / 2.0;
            ca.partial_cmp(&cb).unwrap_or(std::cmp::Ordering::Equal)
        });

        for chunk in slice_vec.chunks(LEAF_CAPACITY) {
            let child_idx = str_pack(nodes, chunk.to_vec(), space);
            child_nodes.push(child_idx);
        }
    }

    pack_internal(nodes, child_nodes, space)
}

/// Recursively group child node indices into internal nodes.
fn pack_internal(nodes: &mut Vec<Node>, child_indices: Vec<usize>, space: BboxSpace) -> usize {
    if child_indices.len() <= INTERNAL_CAPACITY {
        let level = nodes[child_indices[0]].level + 1;
        let mut node = Node::new_internal(level);
        if let NodeKind::Internal { children } = &mut node.kind {
            for idx in child_indices {
                children.push(ChildRef {
                    bbox: nodes[idx].bbox,
                    node_idx: idx,
                });
            }
        }
        node.recompute_bbox(space);
        let idx = nodes.len();
        nodes.push(node);
        return idx;
    }

    let mut new_children = Vec::new();
    for chunk in child_indices.chunks(INTERNAL_CAPACITY) {
        let level = nodes[chunk[0]].level + 1;
        let mut node = Node::new_internal(level);
        if let NodeKind::Internal { children } = &mut node.kind {
            for &idx in chunk {
                children.push(ChildRef {
                    bbox: nodes[idx].bbox,
                    node_idx: idx,
                });
            }
        }
        node.recompute_bbox(space);
        let idx = nodes.len();
        nodes.push(node);
        new_children.push(idx);
    }

    pack_internal(nodes, new_children, space)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{planar_srid, test_memory, utm48n};
    use nodedb_types::BoundingBox;

    fn make_entry(id: u64, lng: f64, lat: f64) -> RTreeEntry {
        RTreeEntry {
            id,
            bbox: BoundingBox::from_point(lng, lat),
        }
    }

    #[test]
    fn bulk_load_500() {
        let entries: Vec<RTreeEntry> = (0..500)
            .map(|i| make_entry(i, (i as f64) * 0.1 - 25.0, (i as f64) * 0.06 - 15.0))
            .collect();
        let tree = RTree::bulk_load(entries, test_memory(), TreeSrid::WGS84);
        assert_eq!(tree.len(), 500);

        let all = tree.search(&BoundingBox::new(-180.0, -90.0, 180.0, 90.0));
        assert_eq!(all.len(), 500);

        let subset = tree.search(&BoundingBox::new(-5.0, -5.0, 5.0, 5.0));
        assert!(!subset.is_empty());
    }

    #[test]
    fn bulk_load_small() {
        let entries = vec![make_entry(1, 0.0, 0.0), make_entry(2, 1.0, 1.0)];
        let tree = RTree::bulk_load(entries, test_memory(), TreeSrid::WGS84);
        assert_eq!(tree.len(), 2);
    }

    #[test]
    fn bulk_load_empty() {
        let tree = RTree::bulk_load(Vec::new(), test_memory(), planar_srid(32647));
        assert!(tree.is_empty());
        assert_eq!(tree.tree_srid(), planar_srid(32647));
    }

    #[test]
    fn bulk_load_keeps_srid_and_planar_space() {
        let entries = vec![make_entry(1, 1e5, 0.0), make_entry(2, 8e5, 0.0)];
        let tree = RTree::bulk_load(entries, test_memory(), utm48n());
        assert_eq!(tree.tree_srid(), utm48n());
        assert_eq!(tree.search(&BoundingBox::new(7e5, -1.0, 9e5, 1.0)).len(), 1);
    }
}
