// SPDX-License-Identifier: Apache-2.0

//! Which nodes a search admits.
//!
//! A current search admits every live node, restricted to a prefilter when
//! one is given. A search at a past system-time cut admits the node of each
//! row visible at the cut: it hides the live nodes written after the cut and
//! admits the soft-deleted nodes ended after it. Ids are global node ids.

use roaring::RoaringBitmap;

/// The node admission rule of one search.
#[derive(Debug, Clone, Copy, Default)]
pub struct NodeFilter<'a> {
    /// Only these ids. `None` admits every id.
    pub allow: Option<&'a RoaringBitmap>,
    /// Live ids the search does not admit.
    pub hide: Option<&'a RoaringBitmap>,
    /// Soft-deleted ids the search admits.
    pub revive: Option<&'a RoaringBitmap>,
}

impl NodeFilter<'_> {
    /// Whether the search admits node `global_id`, which is soft-deleted
    /// when `deleted` is set.
    #[inline]
    pub fn admits(&self, global_id: u32, deleted: bool) -> bool {
        let visible = if deleted {
            self.revive.is_some_and(|r| r.contains(global_id))
        } else {
            !self.hide.is_some_and(|h| h.contains(global_id))
        };
        visible && self.allow.is_none_or(|a| a.contains(global_id))
    }

    /// Whether the search can admit a soft-deleted node.
    #[inline]
    pub fn admits_deleted(&self) -> bool {
        self.revive.is_some_and(|r| !r.is_empty())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_default_admits_live_nodes_only() {
        let filter = NodeFilter::default();
        assert!(filter.admits(3, false));
        assert!(!filter.admits(3, true));
        assert!(!filter.admits_deleted());
    }

    #[test]
    fn hide_revive_and_allow_combine() {
        let hide: RoaringBitmap = [1].into_iter().collect();
        let revive: RoaringBitmap = [2].into_iter().collect();
        let allow: RoaringBitmap = [2, 3].into_iter().collect();
        let filter = NodeFilter {
            allow: Some(&allow),
            hide: Some(&hide),
            revive: Some(&revive),
        };
        assert!(!filter.admits(1, false), "a hidden live node");
        assert!(filter.admits(2, true), "a revived ended node");
        assert!(filter.admits(3, false), "an allowed live node");
        assert!(!filter.admits(4, false), "a node outside the allow set");
        assert!(!filter.admits(5, true), "a deleted node no one revives");
    }
}
