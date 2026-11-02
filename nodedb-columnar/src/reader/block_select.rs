// SPDX-License-Identifier: Apache-2.0

//! The set of 1024-row blocks a segment read decodes.

use crate::format::BLOCK_SIZE;

/// Which blocks of one segment a read decodes. Block `b` holds rows
/// `b * BLOCK_SIZE ..`. A block past the end is never selected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockSelection {
    selected: Vec<bool>,
}

impl BlockSelection {
    /// Every one of `block_count` blocks.
    pub fn all(block_count: usize) -> Self {
        Self {
            selected: vec![true; block_count],
        }
    }

    /// The blocks of `block_count` for which `keep` holds.
    pub fn from_fn(block_count: usize, keep: impl FnMut(usize) -> bool) -> Self {
        Self {
            selected: (0..block_count).map(keep).collect(),
        }
    }

    /// Only the block that holds row `row`, out of `block_count` blocks.
    pub fn only_row(block_count: usize, row: usize) -> Self {
        let block = row / BLOCK_SIZE;
        Self::from_fn(block_count, |b| b == block)
    }

    /// The number of blocks of a segment of `row_count` rows.
    pub fn block_count_for_rows(row_count: usize) -> usize {
        row_count.div_ceil(BLOCK_SIZE)
    }

    /// Number of blocks the selection covers, selected or not.
    pub fn block_count(&self) -> usize {
        self.selected.len()
    }

    /// Whether the read decodes block `block`.
    pub fn is_selected(&self, block: usize) -> bool {
        self.selected.get(block).copied().unwrap_or(false)
    }

    /// Whether the read decodes the block that holds row `row`.
    pub fn holds_row(&self, row: usize) -> bool {
        self.is_selected(row / BLOCK_SIZE)
    }

    /// Number of selected blocks.
    pub fn selected_count(&self) -> usize {
        self.selected.iter().filter(|s| **s).count()
    }

    /// Whether no block is selected.
    pub fn is_empty(&self) -> bool {
        !self.selected.iter().any(|s| *s)
    }

    /// The blocks both selections select. A block past the end of either
    /// is not selected.
    pub fn intersect(&self, other: &Self) -> Self {
        Self::from_fn(self.block_count(), |b| {
            self.is_selected(b) && other.is_selected(b)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn intersection_keeps_blocks_both_select() {
        let a = BlockSelection::from_fn(4, |b| b != 1);
        let b = BlockSelection::from_fn(4, |b| b != 3);
        let both = a.intersect(&b);
        assert_eq!(both.selected_count(), 2);
        assert!(both.is_selected(0) && both.is_selected(2));
        assert!(!both.is_selected(4), "past the end");
    }

    #[test]
    fn a_row_selection_holds_only_its_block() {
        let only = BlockSelection::only_row(3, BLOCK_SIZE + 5);
        assert!(only.holds_row(BLOCK_SIZE));
        assert!(!only.holds_row(0));
        assert_eq!(only.selected_count(), 1);
        assert_eq!(BlockSelection::block_count_for_rows(BLOCK_SIZE + 1), 2);
        assert!(BlockSelection::from_fn(2, |_| false).is_empty());
    }
}
