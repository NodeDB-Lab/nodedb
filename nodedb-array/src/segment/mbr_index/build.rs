//! Hilbert-packed R-tree bulk loader + read-only tree.
//!
//! Tiles in a NodeDB array segment are already written in Hilbert order
//! (the segment writer appends them in order of `tile_id.hilbert_prefix`).
//! That means consecutive [`TileEntry`]s are spatially clustered, so a
//! packed R-tree built by simple chunking yields tight, well-formed
//! leaves with zero spatial sorting work. Each leaf groups
//! [`FANOUT`] consecutive tiles; internal levels group [`FANOUT`]
//! children, recursively, until a single root remains.

use super::node::{BBox, RNode, RNodeKind};
use super::predicate::MbrQueryPredicate;
use crate::segment::format::TileEntry;

/// Branching factor. 16 keeps internal nodes cache-friendly while
/// matching typical leaf-tile-per-node ratios for genomic / EO scale.
pub const FANOUT: usize = 16;

/// Read-only Hilbert-packed R-tree over a segment's tile MBRs.
#[derive(Debug, Clone)]
pub struct HilbertPackedRTree {
    nodes: Vec<RNode>,
    /// Index of the root in `nodes`. `None` iff the segment has zero
    /// tiles (no tree to query).
    root: Option<usize>,
}

impl HilbertPackedRTree {
    /// Bulk-load from segment tile entries (assumed Hilbert-ordered by
    /// the writer). Time complexity O(n) — one pass per level.
    pub fn build(entries: &[TileEntry]) -> Self {
        let mut nodes: Vec<RNode> = Vec::new();
        if entries.is_empty() {
            return Self { nodes, root: None };
        }
        // Leaves — chunk consecutive tiles, recording each tile's
        // absolute index and individual bbox so per-tile filtering can
        // happen at leaf descent time.
        let mut current_level: Vec<usize> = Vec::new();
        let mut cursor = 0usize;
        for chunk in entries.chunks(FANOUT) {
            let bbox = chunk_bbox(chunk);
            let tiles: Vec<(usize, BBox)> = chunk
                .iter()
                .enumerate()
                .map(|(i, e)| (cursor + i, BBox::from_mbr(&e.mbr)))
                .collect();
            cursor += chunk.len();
            nodes.push(RNode {
                bbox,
                kind: RNodeKind::Leaf { tiles },
            });
            current_level.push(nodes.len() - 1);
        }
        // Internal levels
        while current_level.len() > 1 {
            let mut next_level: Vec<usize> = Vec::new();
            for chunk in current_level.chunks(FANOUT) {
                let mut bbox = BBox {
                    min: vec![],
                    max: vec![],
                };
                for &child in chunk {
                    bbox.extend(&nodes[child].bbox);
                }
                let children = chunk.to_vec();
                nodes.push(RNode {
                    bbox,
                    kind: RNodeKind::Internal { children },
                });
                next_level.push(nodes.len() - 1);
            }
            current_level = next_level;
        }
        let root = current_level.first().copied();
        Self { nodes, root }
    }

    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.root.is_none()
    }

    /// Return the indices of tiles whose MBR intersects `pred`.
    /// Order matches segment Hilbert order.
    pub fn query(&self, pred: &MbrQueryPredicate) -> Vec<usize> {
        let mut hits = Vec::new();
        if let Some(root) = self.root {
            self.descend(root, pred, &mut hits);
        }
        hits.sort_unstable();
        hits
    }

    fn descend(&self, idx: usize, pred: &MbrQueryPredicate, hits: &mut Vec<usize>) {
        let node = &self.nodes[idx];
        if !pred.intersects(&node.bbox) {
            return;
        }
        match &node.kind {
            RNodeKind::Leaf { tiles } => {
                for (idx, bbox) in tiles {
                    if pred.intersects(bbox) {
                        hits.push(*idx);
                    }
                }
            }
            RNodeKind::Internal { children } => {
                for &c in children {
                    self.descend(c, pred, hits);
                }
            }
        }
    }
}

fn chunk_bbox(chunk: &[TileEntry]) -> BBox {
    let mut bbox = BBox {
        min: vec![],
        max: vec![],
    };
    for e in chunk {
        bbox.extend(&BBox::from_mbr(&e.mbr));
    }
    bbox
}

#[cfg(test)]
mod tests {
    use super::super::predicate::DimPredicate;
    use super::*;
    use crate::segment::format::TileKind;
    use crate::tile::mbr::{AttrStats, TileMBR};
    use crate::types::TileId;
    use crate::types::domain::DomainBound;

    fn entry(tid: u64, lo: i64, hi: i64) -> TileEntry {
        let mbr = TileMBR {
            dim_mins: vec![DomainBound::Int64(lo)],
            dim_maxs: vec![DomainBound::Int64(hi)],
            nnz: 1,
            attr_stats: vec![AttrStats::AllNull { null_count: 0 }],
        };
        TileEntry::new(TileId::snapshot(tid), TileKind::Sparse, 0, 0, mbr)
    }

    fn pred(lo: i64, hi: i64) -> MbrQueryPredicate {
        MbrQueryPredicate::new(vec![DimPredicate {
            lo: Some(DomainBound::Int64(lo)),
            hi: Some(DomainBound::Int64(hi)),
        }])
    }

    #[test]
    fn empty_tree_returns_empty_query() {
        let t = HilbertPackedRTree::build(&[]);
        assert!(t.is_empty());
        assert!(t.query(&pred(0, 100)).is_empty());
    }

    #[test]
    fn single_tile_match_and_miss() {
        let entries = vec![entry(1, 0, 10)];
        let t = HilbertPackedRTree::build(&entries);
        assert_eq!(t.query(&pred(5, 7)), vec![0]);
        assert!(t.query(&pred(20, 30)).is_empty());
    }

    #[test]
    fn many_tiles_packed_into_levels() {
        // 50 tiles, ranges [0..10], [10..20], ...
        let entries: Vec<TileEntry> = (0..50)
            .map(|i| entry(i as u64, i * 10, i * 10 + 9))
            .collect();
        let t = HilbertPackedRTree::build(&entries);
        // Predicate [25, 45] → tiles 2,3,4 (covering 20-29, 30-39, 40-49)
        let hits = t.query(&pred(25, 45));
        assert_eq!(hits, vec![2, 3, 4]);
    }

    #[test]
    fn query_returns_sorted_indices() {
        let entries: Vec<TileEntry> = (0..40).map(|i| entry(i as u64, i * 5, i * 5 + 4)).collect();
        let t = HilbertPackedRTree::build(&entries);
        let hits = t.query(&pred(0, 200));
        let mut sorted = hits.clone();
        sorted.sort_unstable();
        assert_eq!(hits, sorted);
        assert_eq!(hits.len(), 40);
    }

    #[test]
    fn query_ignores_unbounded_dims() {
        let entries: Vec<TileEntry> = (0..5)
            .map(|i| entry(i, i as i64 * 10, i as i64 * 10 + 9))
            .collect();
        let t = HilbertPackedRTree::build(&entries);
        let p = MbrQueryPredicate::new(vec![DimPredicate { lo: None, hi: None }]);
        assert_eq!(t.query(&p).len(), 5);
    }
}
