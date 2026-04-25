//! R-tree node + bounding box.
//!
//! BBox stores per-dim min/max as [`DomainBound`] so it can carry the
//! mixed-type coordinates produced by [`crate::tile::mbr`]. The tree
//! is built once from segment-footer entries and is read-only;
//! split/merge operations would land here if/when we need a writeable
//! variant.

use crate::types::domain::DomainBound;

/// Per-dim bounding box. `min`/`max` are parallel to schema dims.
#[derive(Debug, Clone, PartialEq)]
pub struct BBox {
    pub min: Vec<DomainBound>,
    pub max: Vec<DomainBound>,
}

impl BBox {
    pub fn from_mbr(mbr: &crate::tile::mbr::TileMBR) -> Self {
        Self {
            min: mbr.dim_mins.clone(),
            max: mbr.dim_maxs.clone(),
        }
    }

    pub fn arity(&self) -> usize {
        self.min.len()
    }

    /// Union `self` with `other` in-place. Empty boxes (arity 0) are
    /// ignored — callers seed leaves with concrete MBRs.
    pub fn extend(&mut self, other: &BBox) {
        if self.arity() == 0 {
            *self = other.clone();
            return;
        }
        for i in 0..self.arity().min(other.arity()) {
            if super::predicate::lt_bound(&other.min[i], &self.min[i]) {
                self.min[i] = other.min[i].clone();
            }
            if super::predicate::lt_bound(&self.max[i], &other.max[i]) {
                self.max[i] = other.max[i].clone();
            }
        }
    }
}

/// One node in the Hilbert-packed R-tree.
///
/// Leaves carry an index into the segment's `TileEntry` table; internal
/// nodes carry indices into the tree's own `nodes` arena.
#[derive(Debug, Clone)]
pub struct RNode {
    pub bbox: BBox,
    pub kind: RNodeKind,
}

#[derive(Debug, Clone)]
pub enum RNodeKind {
    /// Each entry is `(tile_index_into_segment_footer, per_tile_bbox)`.
    /// The per-tile bbox is required so the leaf can filter individual
    /// tiles whose group bbox spans the predicate but whose own MBR
    /// doesn't overlap.
    Leaf {
        tiles: Vec<(usize, BBox)>,
    },
    Internal {
        children: Vec<usize>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bbox_extend_grows_bounds() {
        let mut a = BBox {
            min: vec![DomainBound::Int64(0)],
            max: vec![DomainBound::Int64(10)],
        };
        let b = BBox {
            min: vec![DomainBound::Int64(-5)],
            max: vec![DomainBound::Int64(20)],
        };
        a.extend(&b);
        assert_eq!(a.min[0], DomainBound::Int64(-5));
        assert_eq!(a.max[0], DomainBound::Int64(20));
    }

    #[test]
    fn bbox_extend_from_empty_takes_other() {
        let mut a = BBox {
            min: vec![],
            max: vec![],
        };
        let b = BBox {
            min: vec![DomainBound::Int64(0)],
            max: vec![DomainBound::Int64(1)],
        };
        a.extend(&b);
        assert_eq!(a, b);
    }
}
