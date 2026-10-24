// SPDX-License-Identifier: Apache-2.0

//! R*-tree public API and core structure.

use nodedb_types::geometry::Srid;
use nodedb_types::{BboxSpace, BoundingBox};

use super::node::{Node, NodeKind, RTreeEntry};
use super::search::NnResult;
use super::tree_srid::TreeSrid;
use nodedb_mem::ScopedMemory;

/// R*-tree spatial index.
///
/// Supports insert, delete, range search (bbox intersection), and
/// incremental nearest-neighbor queries. Array-backed nodes for cache
/// friendliness.
///
/// Every entry of one tree is in one SRID. The tree takes it from its first
/// entry, and it fixes the bbox space every union and enlargement uses.
///
/// References:
/// - Beckmann et al., "The R*-tree" (1990)
/// - Hjaltason & Samet, "Distance Browsing in Spatial Databases" (1999)
pub struct RTree {
    pub(crate) nodes: Vec<Node>,
    pub(crate) root: usize,
    pub(crate) len: usize,
    /// SRID of every entry.
    pub(crate) srid: Srid,
    /// Bbox space derived from `srid`, cached for the hot insert path.
    pub(crate) space: BboxSpace,
    /// Enforces database, tenant, and engine memory budgets on large batch
    /// allocations (bulk load, full-scan serialization, range search results).
    pub(crate) memory: ScopedMemory,
}

/// A geometry in one SRID offered to an R-tree holding another.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("R-tree holds SRID {index}, entry is SRID {entry}")]
pub struct SridMismatch {
    pub index: Srid,
    pub entry: Srid,
}

impl RTree {
    /// An empty tree for entries in `srid`.
    pub fn new(memory: ScopedMemory, srid: TreeSrid) -> Self {
        Self {
            nodes: vec![Node::new_leaf()],
            root: 0,
            len: 0,
            srid: srid.srid,
            space: srid.space,
            memory,
        }
    }

    /// Shared reference to the scoped memory handle.
    pub(crate) fn memory(&self) -> &ScopedMemory {
        &self.memory
    }

    /// The SRID every entry is in.
    pub fn srid(&self) -> Srid {
        self.srid
    }

    /// The bbox space unions and enlargements use.
    pub fn space(&self) -> BboxSpace {
        self.space
    }

    /// Whether an entry in `srid` can join this tree. An empty tree accepts
    /// any SRID, since its first entry decides.
    pub fn accepts_srid(&self, srid: Srid) -> bool {
        self.is_empty() || self.srid == srid
    }

    /// Fix the tree's SRID for an entry about to be inserted.
    ///
    /// An empty tree adopts `srid`. A non-empty tree in another SRID refuses
    /// with [`SridMismatch`] and is left unchanged.
    pub fn bind_srid(&mut self, srid: TreeSrid) -> Result<(), SridMismatch> {
        if self.is_empty() {
            self.srid = srid.srid;
            self.space = srid.space;
            return Ok(());
        }
        if self.srid == srid.srid {
            Ok(())
        } else {
            Err(SridMismatch {
                index: self.srid,
                entry: srid.srid,
            })
        }
    }

    /// The SRID and bbox space of every entry.
    pub fn tree_srid(&self) -> TreeSrid {
        TreeSrid {
            srid: self.srid,
            space: self.space,
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Range search: return all entries whose bbox intersects the query bbox.
    pub fn search(&self, query: &BoundingBox) -> Vec<&RTreeEntry> {
        let mut results = Vec::new();
        super::search::search_node(&self.nodes, self.root, query, &mut results);
        results
    }

    /// Range search returning owned entries.
    pub fn search_owned(&self, query: &BoundingBox) -> Vec<RTreeEntry> {
        self.search(query).into_iter().cloned().collect()
    }

    /// Nearest-neighbor search using incremental distance ordering.
    ///
    /// Distance is in the tree's coordinate units: degrees for a geographic
    /// tree, metres (Euclidean) for a projected one.
    pub fn nearest(&self, query_lng: f64, query_lat: f64, k: usize) -> Vec<NnResult> {
        super::search::nearest(
            &self.nodes,
            self.root,
            super::search::NnQuery {
                lng: query_lng,
                lat: query_lat,
                k,
                space: self.space,
            },
            self.is_empty(),
        )
    }

    /// Get all entries (for persistence serialization).
    pub fn entries(&self) -> Vec<&RTreeEntry> {
        let bytes = self.len * std::mem::size_of::<*const RTreeEntry>();
        let _guard = self.memory().reserve(bytes).ok();
        let mut result = Vec::with_capacity(self.len);
        collect_entries(&self.nodes, self.root, &mut result);
        result
    }

    /// Find parent of a node by traversal from root.
    pub(crate) fn find_parent(&self, current: usize, target: usize) -> Option<usize> {
        if let NodeKind::Internal { children } = &self.nodes[current].kind {
            for child in children {
                if child.node_idx == target {
                    return Some(current);
                }
                if let Some(p) = self.find_parent(child.node_idx, target) {
                    return Some(p);
                }
            }
        }
        None
    }

    /// Condense root: if root is internal with 1 child, collapse.
    pub(crate) fn condense_root(&mut self) {
        if let NodeKind::Internal { children } = &self.nodes[self.root].kind
            && children.len() == 1
        {
            self.root = children[0].node_idx;
        }
    }
}

fn collect_entries<'a>(nodes: &'a [Node], node_idx: usize, result: &mut Vec<&'a RTreeEntry>) {
    match &nodes[node_idx].kind {
        NodeKind::Leaf { entries } => result.extend(entries.iter()),
        NodeKind::Internal { children } => {
            for child in children {
                collect_entries(nodes, child.node_idx, result);
            }
        }
    }
}

pub(crate) fn collect_entries_owned(nodes: &[Node], node_idx: usize, result: &mut Vec<RTreeEntry>) {
    match &nodes[node_idx].kind {
        NodeKind::Leaf { entries } => result.extend(entries.iter().cloned()),
        NodeKind::Internal { children } => {
            for child in children {
                collect_entries_owned(nodes, child.node_idx, result);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::node::LEAF_CAPACITY;
    use super::*;
    use crate::test_support::{test_memory, utm48n};

    fn make_entry(id: u64, lng: f64, lat: f64) -> RTreeEntry {
        RTreeEntry {
            id,
            bbox: BoundingBox::from_point(lng, lat),
        }
    }

    fn make_rect(id: u64, min_lng: f64, min_lat: f64, max_lng: f64, max_lat: f64) -> RTreeEntry {
        RTreeEntry {
            id,
            bbox: BoundingBox::new(min_lng, min_lat, max_lng, max_lat),
        }
    }

    #[test]
    fn empty_tree() {
        let tree = RTree::new(test_memory(), TreeSrid::WGS84);
        assert!(tree.is_empty());
        assert!(
            tree.search(&BoundingBox::new(-180.0, -90.0, 180.0, 90.0))
                .is_empty()
        );
    }

    #[test]
    fn insert_and_search_single() {
        let mut tree = RTree::new(test_memory(), TreeSrid::WGS84);
        tree.insert(make_entry(1, 10.0, 20.0));
        assert_eq!(tree.len(), 1);
        assert_eq!(
            tree.search(&BoundingBox::new(5.0, 15.0, 15.0, 25.0)).len(),
            1
        );
        assert!(
            tree.search(&BoundingBox::new(50.0, 50.0, 60.0, 60.0))
                .is_empty()
        );
    }

    #[test]
    fn insert_many_and_search() {
        let mut tree = RTree::new(test_memory(), TreeSrid::WGS84);
        for i in 0..200 {
            tree.insert(make_entry(
                i,
                (i as f64) * 0.5 - 50.0,
                (i as f64) * 0.3 - 30.0,
            ));
        }
        assert_eq!(tree.len(), 200);
        let all = tree.search(&BoundingBox::new(-180.0, -90.0, 180.0, 90.0));
        assert_eq!(all.len(), 200);
    }

    #[test]
    fn delete_entry() {
        let mut tree = RTree::new(test_memory(), TreeSrid::WGS84);
        for i in 0..50 {
            tree.insert(make_entry(i, i as f64, i as f64));
        }
        assert!(tree.delete(25));
        assert_eq!(tree.len(), 49);
        let all = tree.search(&BoundingBox::new(-1.0, -1.0, 100.0, 100.0));
        assert!(all.iter().all(|e| e.id != 25));
        assert!(!tree.delete(999));
    }

    #[test]
    fn nearest_neighbor() {
        let mut tree = RTree::new(test_memory(), TreeSrid::WGS84);
        tree.insert(make_entry(1, 0.0, 0.0));
        tree.insert(make_entry(2, 10.0, 10.0));
        tree.insert(make_entry(3, 5.0, 5.0));
        let nn = tree.nearest(4.0, 4.0, 2);
        assert_eq!(nn.len(), 2);
        assert_eq!(nn[0].entry_id, 3);
        assert_eq!(nn[1].entry_id, 1);
    }

    #[test]
    fn nearest_empty() {
        assert!(
            RTree::new(test_memory(), TreeSrid::WGS84)
                .nearest(0.0, 0.0, 5)
                .is_empty()
        );
    }

    #[test]
    fn rect_overlap_search() {
        let mut tree = RTree::new(test_memory(), TreeSrid::WGS84);
        tree.insert(make_rect(1, 0.0, 0.0, 10.0, 10.0));
        tree.insert(make_rect(2, 5.0, 5.0, 15.0, 15.0));
        tree.insert(make_rect(3, 20.0, 20.0, 30.0, 30.0));
        let results = tree.search(&BoundingBox::new(3.0, 3.0, 7.0, 7.0));
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn stress_insert_delete() {
        let mut tree = RTree::new(test_memory(), TreeSrid::WGS84);
        for i in 0..100_u64 {
            tree.insert(make_entry(i, (i as f64) * 0.5, (i as f64) * 0.3));
        }
        for i in (0..100_u64).step_by(2) {
            assert!(tree.delete(i));
        }
        assert_eq!(tree.len(), 50);
        let all = tree.search(&BoundingBox::new(-180.0, -90.0, 180.0, 90.0));
        assert!(all.iter().all(|e| e.id % 2 == 1));
    }

    #[test]
    fn triggers_node_split() {
        let mut tree = RTree::new(test_memory(), TreeSrid::WGS84);
        let count = LEAF_CAPACITY * 3;
        for i in 0..count as u64 {
            tree.insert(make_entry(i, (i as f64) * 0.1, (i as f64) * 0.1));
        }
        assert_eq!(tree.len(), count);
        let all = tree.search(&BoundingBox::new(-180.0, -90.0, 180.0, 90.0));
        assert_eq!(all.len(), count);
    }

    /// Disjoint metre boxes in a projected tree must union planar: the root
    /// spans them directly instead of wrapping as if x were longitude.
    #[test]
    fn projected_tree_unions_planar() {
        let mut tree = RTree::new(test_memory(), utm48n());
        tree.insert(make_rect(1, 1e5, 0.0, 2e5, 10.0));
        tree.insert(make_rect(2, 7e5, 0.0, 8e5, 10.0));
        let root = tree.nodes[tree.root].bbox;
        assert_eq!((root.min_lng, root.max_lng), (1e5, 8e5));
        assert!(!root.crosses_antimeridian());
        assert_eq!(
            tree.search(&BoundingBox::new(1.5e5, 0.0, 1.6e5, 10.0))
                .len(),
            1
        );
        assert!(
            tree.search(&BoundingBox::new(4e5, 0.0, 5e5, 10.0))
                .is_empty()
        );
    }

    #[test]
    fn projected_nearest_is_euclidean_metres() {
        let mut tree = RTree::new(test_memory(), utm48n());
        tree.insert(make_entry(1, 500_000.0, 0.0));
        tree.insert(make_entry(2, 500_300.0, 400.0));
        let nn = tree.nearest(500_000.0, 0.0, 2);
        assert_eq!(nn[0].entry_id, 1);
        assert!((nn[1].distance - 500.0).abs() < 1e-9);
    }

    #[test]
    fn empty_tree_adopts_first_srid_then_refuses_another() {
        let mut tree = RTree::new(test_memory(), TreeSrid::WGS84);
        assert!(tree.accepts_srid(utm48n().srid));
        assert_eq!(tree.bind_srid(utm48n()), Ok(()));
        tree.insert(make_entry(1, 500_000.0, 0.0));
        assert_eq!(tree.srid(), utm48n().srid);
        assert_eq!(tree.space(), BboxSpace::Planar);
        assert!(!tree.accepts_srid(Srid::WGS84));
        assert_eq!(
            tree.bind_srid(TreeSrid::WGS84),
            Err(SridMismatch {
                index: utm48n().srid,
                entry: Srid::WGS84
            })
        );
        assert_eq!(tree.len(), 1);
    }

    #[test]
    fn entries_enumeration() {
        let mut tree = RTree::new(test_memory(), TreeSrid::WGS84);
        for i in 0..10 {
            tree.insert(make_entry(i, i as f64, i as f64));
        }
        assert_eq!(tree.entries().len(), 10);
    }
}
