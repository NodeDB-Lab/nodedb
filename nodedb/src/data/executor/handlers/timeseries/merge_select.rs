// SPDX-License-Identifier: BUSL-1.1

//! Which partitions of a timeseries collection merge.
//!
//! A flush writes one partition per partition interval its rows span. Late
//! rows for an interval that already has a partition flush into a second
//! one, so an interval collects partitions that overlap in time. A merge
//! combines the partitions of one interval into one.
//!
//! - Only `Sealed` and `Merged` partitions cut at an interval merge. A
//!   collection that flushes without an interval (`UNBOUNDED`) writes one
//!   partition per flush and has no interval to merge within.
//! - Partitions group by the interval they lie in (`cut_interval`) and by
//!   the TRUNCATE end they carry. A partition a TRUNCATE ended never merges
//!   with one it did not, so every partition's rows keep one end.
//! - An interval merges once it ended `merge_quiet_ms` ago, so late rows
//!   still arriving for it flush first. A forced compaction skips the wait.
//! - A group holds at most `merge_max_rows` rows; a larger interval merges
//!   in several groups.
//! - On a bitemporal collection the milliseconds of a group's
//!   `max_system_ts` ordinals lie within one interval of each other.
//!   Retention there drops a partition by its `max_system_ts`, so a merge
//!   delays a row's removal by at most one interval, the granularity one
//!   partition already has.
//!
//! Groups come oldest interval first, each in `(min_ts, WAL LSN, name)`
//! order, so equal timestamps keep their flush order in the merge.

use std::collections::BTreeMap;

use nodedb_types::timeseries::{PartitionInterval, PartitionState};

use crate::engine::timeseries::partition_registry::{
    PartitionEntry, PartitionRegistry, interval_bounds,
};

/// What bounds the groups a selection returns.
#[derive(Debug, Clone, Copy)]
pub(in crate::data::executor) struct MergeBounds {
    pub now_ms: i64,
    pub quiet_ms: u64,
    pub max_rows: u64,
    pub bitemporal: bool,
    /// A forced compaction merges an interval that has not ended yet.
    pub force: bool,
}

/// The groups of `registry` that merge, oldest interval first.
pub(in crate::data::executor) fn merge_groups(
    registry: &PartitionRegistry,
    bounds: MergeBounds,
) -> Vec<Vec<PartitionEntry>> {
    let mut intervals: BTreeMap<(i64, i64, Option<i64>), Vec<PartitionEntry>> = BTreeMap::new();
    for (_, entry) in registry.iter() {
        let meta = &entry.meta;
        if !matches!(meta.state, PartitionState::Sealed | PartitionState::Merged) {
            continue;
        }
        // An installed ended partition keeps the directory its identity
        // names, so a second install of it stays a no-op.
        if entry
            .dir_name
            .starts_with(super::ended_install::ENDED_INSTALL_PREFIX)
        {
            continue;
        }
        let Some((start, end)) = cut_interval(meta.interval_ms, meta.min_ts, meta.max_ts) else {
            continue;
        };
        intervals
            .entry((start, end, meta.ended_at))
            .or_default()
            .push(entry.clone());
    }

    let mut groups = Vec::new();
    for ((start, interval_end, _), mut entries) in intervals {
        if entries.len() < 2 {
            continue;
        }
        let width = interval_end.saturating_sub(start);
        let quiet = i64::try_from(bounds.quiet_ms).unwrap_or(i64::MAX);
        if !bounds.force && bounds.now_ms.saturating_sub(interval_end) < quiet {
            continue;
        }
        entries.sort_by(|a, b| {
            (a.meta.min_ts, a.meta.last_flushed_wal_lsn, &a.dir_name).cmp(&(
                b.meta.min_ts,
                b.meta.last_flushed_wal_lsn,
                &b.dir_name,
            ))
        });
        split(entries, width, bounds, &mut groups);
    }
    groups
}

/// The `[start, end)` interval a partition of width `interval_ms` spanning
/// `min_ts..=max_ts` lies in. A flush records the width of the interval it
/// cut at: the length of a calendar month or year, or a fixed duration. The
/// calendar month and year are tried first, then the duration aligned to the
/// epoch. Every candidate has the recorded width, so a group never spans
/// more than one interval. `None` for a partition no interval cut (width
/// `0`) or one that lies in no candidate.
fn cut_interval(interval_ms: u64, min_ts: i64, max_ts: i64) -> Option<(i64, i64)> {
    if interval_ms == 0 {
        return None;
    }
    let width = i64::try_from(interval_ms).ok()?;
    [
        PartitionInterval::Month,
        PartitionInterval::Year,
        PartitionInterval::Duration(interval_ms),
    ]
    .iter()
    .filter_map(|interval| interval_bounds(interval, min_ts))
    .find(|&(start, end)| end.saturating_sub(start) == width && start <= min_ts && max_ts < end)
}

/// Split one interval's partitions, in order, into groups within the row
/// and system-time bounds. A group of one partition is no merge.
fn split(
    entries: Vec<PartitionEntry>,
    width: i64,
    bounds: MergeBounds,
    groups: &mut Vec<Vec<PartitionEntry>>,
) {
    let mut group: Vec<PartitionEntry> = Vec::new();
    let mut rows = 0u64;
    for entry in entries {
        let over_rows = rows.saturating_add(entry.meta.row_count) > bounds.max_rows;
        let over_time = bounds.bitemporal && !within_system_time(&group, &entry, width);
        if !group.is_empty() && (over_rows || over_time) {
            if group.len() >= 2 {
                groups.push(std::mem::take(&mut group));
            } else {
                group.clear();
            }
            rows = 0;
        }
        rows = rows.saturating_add(entry.meta.row_count);
        group.push(entry);
    }
    if group.len() >= 2 {
        groups.push(group);
    }
}

/// Whether `entry` joins `group` with the millisecond of every
/// `max_system_ts` ordinal within `width` milliseconds of the others.
fn within_system_time(group: &[PartitionEntry], entry: &PartitionEntry, width: i64) -> bool {
    let times = group
        .iter()
        .chain(std::iter::once(entry))
        .map(|e| nodedb_types::ordinal_to_ms(e.meta.max_system_ts));
    let (low, high) = times.fold((i64::MAX, i64::MIN), |(lo, hi), t| (lo.min(t), hi.max(t)));
    high.saturating_sub(low) <= width
}

#[cfg(test)]
mod tests {
    use nodedb_types::timeseries::{PartitionMeta, TieredPartitionConfig};

    use super::*;

    const HOUR: i64 = 3_600_000;

    fn entry(name: &str, min_ts: i64, rows: u64, lsn: u64, system: i64) -> PartitionEntry {
        PartitionEntry {
            meta: PartitionMeta {
                min_ts,
                max_ts: min_ts + 10,
                row_count: rows,
                size_bytes: 0,
                schema_version: 1,
                state: PartitionState::Sealed,
                interval_ms: HOUR as u64,
                last_flushed_wal_lsn: lsn,
                column_stats: std::collections::HashMap::new(),
                max_system_ts: system,
                ended_at: None,
            },
            dir_name: name.to_string(),
        }
    }

    fn registry(entries: Vec<PartitionEntry>) -> PartitionRegistry {
        let mut reg = PartitionRegistry::new(TieredPartitionConfig::origin_defaults());
        for e in entries {
            reg.insert_partition(e);
        }
        reg
    }

    fn bounds(now_ms: i64) -> MergeBounds {
        MergeBounds {
            now_ms,
            quiet_ms: 1_000,
            max_rows: 1_000,
            bitemporal: false,
            force: false,
        }
    }

    fn names(groups: &[Vec<PartitionEntry>]) -> Vec<Vec<&str>> {
        groups
            .iter()
            .map(|g| g.iter().map(|e| e.dir_name.as_str()).collect())
            .collect()
    }

    /// Two partitions of one ended interval merge, in flush order. A lone
    /// partition of another interval does not.
    #[test]
    fn an_interval_with_late_rows_merges() {
        let reg = registry(vec![
            entry("ts-late", 5, 10, 2, 0),
            entry("ts-first", 0, 10, 1, 0),
            entry("ts-next", HOUR, 10, 3, 0),
        ]);
        let groups = merge_groups(&reg, bounds(3 * HOUR));
        assert_eq!(names(&groups), vec![vec!["ts-first", "ts-late"]]);
    }

    /// A partition a TRUNCATE ended never merges with one it did not end, so
    /// the end keeps covering exactly the rows it ended. The names avoid the
    /// installed-partition prefix, which the merge never takes.
    #[test]
    fn a_truncate_end_splits_an_interval() {
        let mut ended = entry("ts-cut", 0, 10, 1, 0);
        ended.meta.ended_at = Some(50);
        let mut ended_too = entry("ts-cut-too", 2, 10, 2, 0);
        ended_too.meta.ended_at = Some(50);
        let reg = registry(vec![ended, ended_too, entry("ts-current", 1, 10, 3, 0)]);
        let groups = merge_groups(&reg, bounds(3 * HOUR));
        assert_eq!(names(&groups), vec![vec!["ts-cut", "ts-cut-too"]]);
    }

    /// An installed ended partition never merges, even with a partition of
    /// the same interval and end.
    #[test]
    fn an_installed_ended_partition_never_merges() {
        let mut installed = entry("ts-ended-0_10_e50", 0, 10, 1, 0);
        installed.meta.ended_at = Some(50);
        let mut cut = entry("ts-cut", 2, 10, 2, 0);
        cut.meta.ended_at = Some(50);
        let reg = registry(vec![installed, cut]);
        assert!(merge_groups(&reg, bounds(3 * HOUR)).is_empty());
    }

    /// An interval that ended less than the quiet window ago waits, unless
    /// the compaction is forced.
    #[test]
    fn a_recent_interval_waits_for_the_quiet_window() {
        let reg = registry(vec![entry("ts-a", 0, 10, 1, 0), entry("ts-b", 1, 10, 2, 0)]);
        assert!(merge_groups(&reg, bounds(HOUR + 10)).is_empty());
        let forced = MergeBounds {
            force: true,
            ..bounds(HOUR + 10)
        };
        assert_eq!(merge_groups(&reg, forced).len(), 1);
    }

    #[test]
    fn the_row_cap_splits_an_interval() {
        let reg = registry(vec![
            entry("ts-a", 0, 600, 1, 0),
            entry("ts-b", 1, 300, 2, 0),
            entry("ts-c", 2, 600, 3, 0),
            entry("ts-d", 3, 300, 4, 0),
        ]);
        let groups = merge_groups(&reg, bounds(3 * HOUR));
        assert_eq!(
            names(&groups),
            vec![vec!["ts-a", "ts-b"], vec!["ts-c", "ts-d"]]
        );
    }

    /// On a bitemporal collection, partitions whose system times lie more
    /// than one interval apart do not merge.
    #[test]
    fn bitemporal_system_times_bound_a_group() {
        let reg = registry(vec![
            entry("ts-a", 0, 10, 1, 0),
            entry(
                "ts-b",
                1,
                10,
                2,
                nodedb_types::ms_to_ordinal_upper(10 * HOUR),
            ),
        ]);
        let mut b = bounds(3 * HOUR);
        assert_eq!(merge_groups(&reg, b).len(), 1);
        b.bitemporal = true;
        assert!(merge_groups(&reg, b).is_empty());
    }
}
