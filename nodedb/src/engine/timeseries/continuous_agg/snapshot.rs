// SPDX-License-Identifier: BUSL-1.1

//! What a read at a system-time cut needs from the manager.
//!
//! A bucket records the newest ordinal of the rows it folds, so a read at
//! ordinal `S` can tell whether a bucket still equals its state at `S`.
//! A refresh cut and a bucket retention pass change buckets in other ways,
//! so each records its own ordinal as the aggregate's rewrite. A read
//! below a rewrite cannot rebuild the buckets it saw from the current ones.
//!
//! A bucket that is not exact at `S` is rebuilt from raw rows:
//! [`ContinuousAggregateManager::delta_at`] folds a batch of root rows,
//! filtered to the cut, down the aggregate's chain without merging.

use std::collections::HashMap;

use super::cut::folds_at_cuts;
use super::manager::{AggKey, ContinuousAggregateManager};
use super::refresh::{self, Buckets, RefreshSource, RowCut};
use super::rollup;

impl ContinuousAggregateManager {
    /// Record that aggregate `name`, rooted at `root`, was rewritten at
    /// `ordinal`. Keeps the newest ordinal.
    pub fn note_rewritten(
        &mut self,
        database_id: u64,
        tenant_id: u64,
        root: &str,
        name: &str,
        ordinal: i64,
    ) {
        let root_key = (database_id, tenant_id, root.to_string());
        let held = self
            .rewritten_at
            .entry(root_key.clone())
            .or_default()
            .entry(name.to_string())
            .or_insert(ordinal);
        *held = (*held).max(ordinal);
        self.dirty_images.insert(root_key);
    }

    /// The newest rewrite ordinal of aggregate `name` rooted at `root`.
    pub fn rewritten_at(
        &self,
        database_id: u64,
        tenant_id: u64,
        root: &str,
        name: &str,
    ) -> Option<i64> {
        self.rewritten_at
            .get(&(database_id, tenant_id, root.to_string()))
            .and_then(|names| names.get(name))
            .copied()
    }

    /// The rewrite ordinals of `root_key`'s aggregates, sorted by name.
    pub(super) fn rewritten_view(&self, root_key: &AggKey) -> Vec<(&str, i64)> {
        let mut view: Vec<(&str, i64)> = self
            .rewritten_at
            .get(root_key)
            .map(|names| {
                names
                    .iter()
                    .map(|(name, ordinal)| (name.as_str(), *ordinal))
                    .collect()
            })
            .unwrap_or_default();
        view.sort_unstable();
        view
    }

    /// Merge rewrite ordinals read from an image. Keeps the newer of two.
    pub(super) fn merge_rewritten(&mut self, root_key: &AggKey, rewritten: Vec<(String, i64)>) {
        if rewritten.is_empty() {
            return;
        }
        let held: &mut HashMap<String, i64> =
            self.rewritten_at.entry(root_key.clone()).or_default();
        for (name, ordinal) in rewritten {
            let slot = held.entry(name).or_insert(ordinal);
            *slot = (*slot).max(ordinal);
        }
    }

    /// Whether aggregate `name` folds only at refresh cuts. `None` when it
    /// is not registered.
    pub fn folds_at_cuts(&self, database_id: u64, tenant_id: u64, name: &str) -> Option<bool> {
        self.definitions
            .get(&(database_id, tenant_id, name.to_string()))
            .map(|def| folds_at_cuts(&def.refresh_policy))
    }

    /// The buckets of aggregate `target` that the rows of `src` selected
    /// by `cut` make. `src` holds rows of `target`'s root. The rows fold
    /// into the first tier of the chain and roll down to `target`, as a
    /// partition fold does, but no tier takes the result. Empty when
    /// `target` or a tier on its chain is not registered, or the chain
    /// loops.
    pub fn delta_at(
        &self,
        database_id: u64,
        tenant_id: u64,
        target: &str,
        src: &RefreshSource<'_>,
        cut: &RowCut<'_>,
    ) -> Buckets {
        let Some(path) = self.chain_path(database_id, tenant_id, target) else {
            return Buckets::new();
        };
        let mut defs = path.iter().filter_map(|name| {
            self.definitions
                .get(&(database_id, tenant_id, name.clone()))
        });
        let Some(first) = defs.next() else {
            return Buckets::new();
        };
        let mut upstream = first;
        let mut delta = refresh::refresh_rows_at(first, src, cut).delta;
        for def in defs {
            delta = rollup::rollup_delta(upstream, def, &delta);
            upstream = def;
        }
        delta
    }

    /// The aggregates from the one that reads `target`'s root down to
    /// `target`, root side first. `None` when one is not registered or the
    /// chain loops.
    fn chain_path(&self, database_id: u64, tenant_id: u64, target: &str) -> Option<Vec<String>> {
        let mut path = vec![target.to_string()];
        loop {
            let current = path.last()?;
            let def = self
                .definitions
                .get(&(database_id, tenant_id, current.clone()))?;
            if !self
                .definitions
                .contains_key(&(database_id, tenant_id, def.source.clone()))
            {
                path.reverse();
                return Some(path);
            }
            if path.contains(&def.source) {
                return None;
            }
            path.push(def.source.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::timeseries::continuous_agg::test_support::exact_def;

    #[test]
    fn a_rewrite_keeps_the_newest_ordinal_per_aggregate() {
        let mut mgr = ContinuousAggregateManager::new();
        mgr.note_rewritten(0, 1, "metrics", "v_1m", 9);
        mgr.note_rewritten(0, 1, "metrics", "v_1m", 4);
        mgr.note_rewritten(0, 1, "metrics", "v_1h", 2);
        assert_eq!(mgr.rewritten_at(0, 1, "metrics", "v_1m"), Some(9));
        assert_eq!(mgr.rewritten_at(0, 1, "metrics", "v_1h"), Some(2));
        assert_eq!(mgr.rewritten_at(0, 1, "other", "v_1m"), None);
        let root = (0, 1, "metrics".to_string());
        assert_eq!(mgr.rewritten_view(&root), vec![("v_1h", 2), ("v_1m", 9)]);
        mgr.merge_rewritten(&root, vec![("v_1h".into(), 5), ("v_1m".into(), 1)]);
        assert_eq!(mgr.rewritten_view(&root), vec![("v_1h", 5), ("v_1m", 9)]);
    }

    #[test]
    fn a_chain_path_runs_from_the_root_tier_to_the_target() {
        let mut mgr = ContinuousAggregateManager::new();
        mgr.register(1, exact_def("v_1m", "metrics", "1m"));
        mgr.register(1, exact_def("v_1h", "v_1m", "1h"));
        assert_eq!(
            mgr.chain_path(0, 1, "v_1h"),
            Some(vec!["v_1m".to_string(), "v_1h".to_string()])
        );
        assert_eq!(mgr.chain_path(0, 1, "missing"), None);
    }
}
