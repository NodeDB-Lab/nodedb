// SPDX-License-Identifier: BUSL-1.1

//! Partition names across a merge of root-source partitions.
//!
//! A merge moves the rows of its source partitions into one new partition
//! and never changes a row. Every record that names partitions — a tier's
//! folded set, a pending cut, an orphan cut, the entry of a tier not
//! registered yet — names either every source or none of them. The merge
//! then replaces the source names with the merged name, in memory and in the
//! image, so each record names exactly the same rows as before.
//!
//! A group that some record names only in part is not merged: the merged
//! name could not say which of its rows that record holds. For a flush tier
//! the maintenance tick folds the missing sources first. For a `Manual` or
//! `Periodic` tier the group waits for the next refresh cut to complete; a
//! `Manual` tier that never refreshes keeps such a group unmerged.

use std::collections::HashSet;

use super::image::{CaggImage, CaggImageEntry, CaggImageEntryView, CaggImageView};
use super::manager::ContinuousAggregateManager;
use super::partial::PartialAggregate;

/// A record names some sources of a merge and not others.
#[derive(Debug, thiserror::Error)]
#[error("a continuous aggregate record names some partitions of a merge and not others")]
pub struct MixedMergeNames;

impl From<MixedMergeNames> for crate::Error {
    fn from(e: MixedMergeNames) -> Self {
        crate::Error::Internal {
            detail: e.to_string(),
        }
    }
}

/// How many of `sources` `holds` names: none, all, or some.
fn coverage(sources: &[String], holds: impl Fn(&str) -> bool) -> Coverage {
    let held = sources.iter().filter(|s| holds(s.as_str())).count();
    match held {
        0 => Coverage::None,
        n if n == sources.len() => Coverage::All,
        _ => Coverage::Some,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Coverage {
    None,
    All,
    Some,
}

/// Replace `sources` with `merged` in `set`. Returns whether it changed.
fn rename_set(
    set: &mut HashSet<String>,
    sources: &[String],
    merged: &str,
) -> Result<bool, MixedMergeNames> {
    match coverage(sources, |s| set.contains(s)) {
        Coverage::None => Ok(false),
        Coverage::Some => Err(MixedMergeNames),
        Coverage::All => {
            for source in sources {
                set.remove(source);
            }
            set.insert(merged.to_string());
            Ok(true)
        }
    }
}

/// Replace `sources` with `merged` in `names`. Returns whether it changed.
fn rename_vec(
    names: &mut Vec<String>,
    sources: &[String],
    merged: &str,
) -> Result<bool, MixedMergeNames> {
    match coverage(sources, |s| names.iter().any(|n| n == s)) {
        Coverage::None => Ok(false),
        Coverage::Some => Err(MixedMergeNames),
        Coverage::All => {
            names.retain(|n| !sources.contains(n));
            if !names.iter().any(|n| n == merged) {
                names.push(merged.to_string());
            }
            Ok(true)
        }
    }
}

impl CaggImageEntry {
    /// Whether this entry names every source or none, in its folded set and
    /// in its pending cut.
    fn merge_uniform(&self, sources: &[String]) -> bool {
        coverage(sources, |s| self.folded.iter().any(|n| n == s)) != Coverage::Some
            && coverage(sources, |s| self.cut.pending.contains(s)) != Coverage::Some
    }

    fn rename_merged(&mut self, sources: &[String], merged: &str) -> Result<bool, MixedMergeNames> {
        let folded = rename_vec(&mut self.folded, sources, merged)?;
        let pending = rename_set(&mut self.cut.pending, sources, merged)?;
        Ok(folded || pending)
    }
}

impl CaggImage {
    /// Replace `sources` with `merged` in every entry and orphan cut.
    /// Returns whether anything changed. `Err` when a record names some
    /// sources and not others.
    pub fn rename_merged(
        &mut self,
        sources: &[String],
        merged: &str,
    ) -> Result<bool, MixedMergeNames> {
        let mut changed = false;
        for entry in &mut self.entries {
            changed |= entry.rename_merged(sources, merged)?;
        }
        for orphan in &mut self.orphans {
            changed |= rename_vec(&mut orphan.partitions, sources, merged)?;
        }
        Ok(changed)
    }

    /// This image as the borrowed form [`super::image::encode_image`] takes.
    pub fn view(&self) -> CaggImageView<'_> {
        CaggImageView {
            entries: self
                .entries
                .iter()
                .map(|entry| {
                    let mut folded: Vec<&str> = entry.folded.iter().map(String::as_str).collect();
                    folded.sort_unstable();
                    let mut buckets: Vec<&PartialAggregate> = entry.buckets.iter().collect();
                    buckets.sort_by(|a, b| {
                        (a.bucket_ts, &a.group_key).cmp(&(b.bucket_ts, &b.group_key))
                    });
                    CaggImageEntryView {
                        def: &entry.def,
                        watermark_ts: entry.watermark_ts,
                        rows_aggregated: entry.rows_aggregated,
                        last_refresh_ms: entry.last_refresh_ms,
                        time_kind: entry.time_kind,
                        folded,
                        buckets,
                        cut: Some(&entry.cut),
                    }
                })
                .collect(),
            orphans: &self.orphans,
            raw_dropped_through: self.raw_dropped_through,
            rewritten: {
                let mut rewritten: Vec<(&str, i64)> = self
                    .rewritten
                    .iter()
                    .map(|(name, ordinal)| (name.as_str(), *ordinal))
                    .collect();
                rewritten.sort_unstable();
                rewritten
            },
        }
    }
}

impl ContinuousAggregateManager {
    /// Whether the partitions `sources` of `root` can merge: `root` is not
    /// quarantined, its image was read, every tier rooted at it is loaded
    /// and not stale, and every record names every source or none.
    pub fn merge_admissible(
        &self,
        database_id: u64,
        tenant_id: u64,
        root: &str,
        sources: &[String],
    ) -> bool {
        let root_key = (database_id, tenant_id, root.to_string());
        if self.quarantined.contains(&root_key) || !self.images_read.contains(&root_key) {
            return false;
        }
        for name in self.chain(database_id, tenant_id, root) {
            let key = (database_id, tenant_id, name);
            if !self.loaded.contains(&key)
                || self.definitions.get(&key).is_some_and(|def| def.stale)
            {
                return false;
            }
            let folded = self.folded.get(&key);
            if coverage(sources, |s| folded.is_some_and(|set| set.contains(s))) == Coverage::Some {
                return false;
            }
            let pending = self.cuts.get(&key).map(|cut| &cut.pending);
            if coverage(sources, |s| pending.is_some_and(|set| set.contains(s))) == Coverage::Some {
                return false;
            }
        }
        let orphans_uniform = self.orphan_cuts.get(&root_key).is_none_or(|orphans| {
            orphans.iter().all(|orphan| {
                coverage(sources, |s| orphan.partitions.iter().any(|p| p == s)) != Coverage::Some
            })
        });
        let unclaimed_uniform = self
            .unclaimed
            .get(&root_key)
            .is_none_or(|entries| entries.iter().all(|entry| entry.merge_uniform(sources)));
        orphans_uniform && unclaimed_uniform
    }

    /// Replace `sources` with `merged` in every record rooted at `root`, and
    /// mark `root`'s image dirty. Call after the merge committed, with the
    /// sources [`Self::merge_admissible`] accepted.
    pub fn rename_merged(
        &mut self,
        database_id: u64,
        tenant_id: u64,
        root: &str,
        sources: &[String],
        merged: &str,
    ) -> Result<(), MixedMergeNames> {
        let root_key = (database_id, tenant_id, root.to_string());
        for name in self.chain(database_id, tenant_id, root) {
            let key = (database_id, tenant_id, name);
            if let Some(set) = self.folded.get_mut(&key) {
                rename_set(set, sources, merged)?;
            }
            if let Some(cut) = self.cuts.get_mut(&key) {
                rename_set(&mut cut.pending, sources, merged)?;
            }
        }
        if let Some(orphans) = self.orphan_cuts.get_mut(&root_key) {
            for orphan in orphans {
                rename_vec(&mut orphan.partitions, sources, merged)?;
            }
        }
        if let Some(entries) = self.unclaimed.get_mut(&root_key) {
            for entry in entries {
                entry.rename_merged(sources, merged)?;
            }
        }
        self.dirty_images.insert(root_key);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names(list: &[&str]) -> Vec<String> {
        list.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn a_set_naming_every_source_takes_the_merged_name() {
        let mut set: HashSet<String> = names(&["a", "b", "c"]).into_iter().collect();
        assert!(rename_set(&mut set, &names(&["a", "b"]), "m").expect("uniform"));
        let mut held: Vec<&String> = set.iter().collect();
        held.sort();
        assert_eq!(held, vec!["c", "m"]);
    }

    #[test]
    fn a_set_naming_no_source_is_unchanged() {
        let mut set: HashSet<String> = names(&["c"]).into_iter().collect();
        assert!(!rename_set(&mut set, &names(&["a", "b"]), "m").expect("uniform"));
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn a_set_naming_some_sources_is_refused() {
        let mut set: HashSet<String> = names(&["a"]).into_iter().collect();
        assert!(rename_set(&mut set, &names(&["a", "b"]), "m").is_err());
        let mut list = names(&["b"]);
        assert!(rename_vec(&mut list, &names(&["a", "b"]), "m").is_err());
    }

    /// Renaming twice is a no-op the second time: recovery may repeat it.
    #[test]
    fn renaming_is_idempotent() {
        let mut list = names(&["a", "b"]);
        assert!(rename_vec(&mut list, &names(&["a", "b"]), "m").expect("uniform"));
        assert!(!rename_vec(&mut list, &names(&["a", "b"]), "m").expect("uniform"));
        assert_eq!(list, names(&["m"]));
    }
}
