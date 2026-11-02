// SPDX-License-Identifier: BUSL-1.1

//! Per-table store of ended KV row versions.
//!
//! The hash table holds each key's newest version. Every version a write or
//! a delete ends moves here with its closed-open interval `[sys_from,
//! sys_to)`. A delete leaves a gap after the ended version, so a key the
//! table no longer holds still answers a read at an ordinal before its
//! delete.

use std::collections::HashMap;

use nodedb_types::Surrogate;

/// Fixed bytes one history version costs beyond its value: two ordinals,
/// the expiry, the surrogate, and the value's vector header.
const HISTORY_VERSION_FIXED_BYTES: usize = 48;

/// Fixed bytes one history key costs beyond its key bytes: the map slot,
/// the key's vector header, and the version vector header.
const HISTORY_KEY_FIXED_BYTES: usize = 64;

/// One ended version of a KV row: visible at every ordinal `S` with
/// `sys_from <= S < sys_to`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryVersion {
    /// Ordinal of the write that set this version.
    pub sys_from: i64,
    /// Ordinal of the write, delete, or expiry reap that ended it.
    pub sys_to: i64,
    /// Value bytes the version held.
    pub value: Vec<u8>,
    /// Absolute expiry instant in ms the version held, `0` for none.
    pub expire_at_ms: u64,
    /// Bound row identity the version held.
    pub surrogate: Surrogate,
}

impl HistoryVersion {
    /// Whether a read at ordinal `at` sees this version.
    pub fn visible_at(&self, at: i64) -> bool {
        self.sys_from <= at && at < self.sys_to
    }

    /// Approximate memory cost in bytes.
    pub fn mem_size(&self) -> usize {
        HISTORY_VERSION_FIXED_BYTES + self.value.len()
    }
}

/// Ended versions of one table, per key, sorted ascending by `sys_from`.
#[derive(Debug, Default)]
pub struct KvHistory {
    versions: HashMap<Vec<u8>, Vec<HistoryVersion>>,
    bytes: usize,
    count: usize,
}

impl KvHistory {
    /// Record an ended version of `key`. A zero-width version
    /// (`sys_to <= sys_from`) is visible at no ordinal and is not stored.
    pub fn record(&mut self, key: &[u8], version: HistoryVersion) {
        if version.sys_to <= version.sys_from {
            return;
        }
        let size = version.mem_size();
        let list = match self.versions.get_mut(key) {
            Some(list) => list,
            None => {
                self.bytes += HISTORY_KEY_FIXED_BYTES + key.len();
                self.versions.entry(key.to_vec()).or_default()
            }
        };
        let at = list.partition_point(|v| v.sys_from <= version.sys_from);
        list.insert(at, version);
        self.bytes += size;
        self.count += 1;
    }

    /// The ended version of `key` a read at ordinal `at` sees, if any.
    pub fn visible_at(&self, key: &[u8], at: i64) -> Option<&HistoryVersion> {
        let list = self.versions.get(key)?;
        let after = list.partition_point(|v| v.sys_from <= at);
        let candidate = list.get(after.checked_sub(1)?)?;
        candidate.visible_at(at).then_some(candidate)
    }

    /// The ordinal the newest ended version of `key` ends at: the write,
    /// delete, or reap that ended it. `None` when `key` has no ended version.
    pub fn newest_end(&self, key: &[u8]) -> Option<i64> {
        // Versions of one key never overlap, so the newest ends last.
        self.versions.get(key)?.last().map(|v| v.sys_to)
    }

    /// Every ended version of `key`, oldest first.
    pub fn versions_of(&self, key: &[u8]) -> &[HistoryVersion] {
        self.versions.get(key).map_or(&[], Vec::as_slice)
    }

    /// Every key with at least one ended version, with its versions.
    pub fn iter(&self) -> impl Iterator<Item = (&[u8], &[HistoryVersion])> {
        self.versions
            .iter()
            .map(|(key, list)| (key.as_slice(), list.as_slice()))
    }

    /// Remove every version whose successor or delete starts at or below
    /// `horizon` (`sys_to <= horizon`). The version visible at `horizon`
    /// stays. A key whose every version ends at or below `horizon` leaves
    /// the store. Returns the number of versions removed.
    pub fn purge_below(&mut self, horizon: i64) -> usize {
        let mut removed = 0usize;
        let mut freed = 0usize;
        self.versions.retain(|key, list| {
            let before = list.len();
            list.retain(|v| {
                let keep = v.sys_to > horizon;
                if !keep {
                    freed += v.mem_size();
                }
                keep
            });
            removed += before - list.len();
            if list.is_empty() {
                freed += HISTORY_KEY_FIXED_BYTES + key.len();
                false
            } else {
                true
            }
        });
        self.bytes = self.bytes.saturating_sub(freed);
        self.count -= removed;
        removed
    }

    /// Install a restored version list for `key`, sorted ascending.
    pub fn restore(&mut self, key: &[u8], versions: Vec<HistoryVersion>) {
        for version in versions {
            self.record(key, version);
        }
    }

    /// Install `version` of `key` between the versions it already holds. A
    /// stored version at the same `sys_from` is the same version and is
    /// replaced. The version before it ends at its `sys_from`. The caller
    /// sets `sys_to` to the start of the version after it.
    pub fn splice(&mut self, key: &[u8], version: HistoryVersion) {
        if let Some(list) = self.versions.get_mut(key) {
            if let Some(at) = list.iter().position(|v| v.sys_from == version.sys_from) {
                let replaced = list.remove(at);
                self.bytes = self.bytes.saturating_sub(replaced.mem_size());
                self.count -= 1;
            }
            let before = list.partition_point(|v| v.sys_from < version.sys_from);
            if let Some(prev) = before.checked_sub(1).and_then(|i| list.get_mut(i))
                && prev.sys_to > version.sys_from
            {
                prev.sys_to = version.sys_from;
            }
        }
        self.record(key, version);
    }

    /// Remove the ended version of `key` that starts at `sys_from`, as a
    /// rollback removes the version a reversed write ended. A key left with
    /// no version leaves the store. Returns whether a version left.
    pub fn remove_version(&mut self, key: &[u8], sys_from: i64) -> bool {
        let Some(list) = self.versions.get_mut(key) else {
            return false;
        };
        let Some(at) = list.iter().position(|v| v.sys_from == sys_from) else {
            return false;
        };
        let removed = list.remove(at);
        let mut freed = removed.mem_size();
        if list.is_empty() {
            self.versions.remove(key);
            freed += HISTORY_KEY_FIXED_BYTES + key.len();
        }
        self.bytes = self.bytes.saturating_sub(freed);
        self.count -= 1;
        true
    }

    /// Make `versions` the whole ended history of `key`: every stored
    /// version of the key leaves, then each of `versions` is recorded. An
    /// empty `versions` removes the key from the store.
    pub fn replace(&mut self, key: &[u8], versions: Vec<HistoryVersion>) {
        if let Some(list) = self.versions.remove(key) {
            let freed: usize = list.iter().map(HistoryVersion::mem_size).sum::<usize>()
                + HISTORY_KEY_FIXED_BYTES
                + key.len();
            self.bytes = self.bytes.saturating_sub(freed);
            self.count -= list.len();
        }
        for version in versions {
            self.record(key, version);
        }
    }

    /// The highest ordinal any stored version names.
    pub fn max_ordinal(&self) -> Option<i64> {
        self.versions
            .values()
            .filter_map(|list| list.last().map(|v| v.sys_to))
            .max()
    }

    /// Approximate memory cost in bytes.
    pub fn bytes(&self) -> usize {
        self.bytes
    }

    /// Number of stored versions across all keys.
    pub fn version_count(&self) -> usize {
        self.count
    }

    /// Whether the store holds no version.
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn version(sys_from: i64, sys_to: i64, value: &[u8]) -> HistoryVersion {
        HistoryVersion {
            sys_from,
            sys_to,
            value: value.to_vec(),
            expire_at_ms: 0,
            surrogate: Surrogate::new(1),
        }
    }

    #[test]
    fn a_read_sees_the_version_whose_interval_holds_it() {
        let mut h = KvHistory::default();
        h.record(b"k", version(10, 20, b"a"));
        h.record(b"k", version(20, 30, b"b"));
        assert!(h.visible_at(b"k", 9).is_none());
        assert_eq!(
            h.visible_at(b"k", 10).map(|v| v.value.as_slice()),
            Some(&b"a"[..])
        );
        assert_eq!(
            h.visible_at(b"k", 19).map(|v| v.value.as_slice()),
            Some(&b"a"[..])
        );
        assert_eq!(
            h.visible_at(b"k", 20).map(|v| v.value.as_slice()),
            Some(&b"b"[..])
        );
        assert!(h.visible_at(b"k", 30).is_none());
    }

    #[test]
    fn a_gap_after_a_delete_reads_as_absent() {
        let mut h = KvHistory::default();
        h.record(b"k", version(10, 20, b"a"));
        h.record(b"k", version(40, 50, b"b"));
        assert!(h.visible_at(b"k", 25).is_none());
        assert_eq!(
            h.visible_at(b"k", 45).map(|v| v.value.as_slice()),
            Some(&b"b"[..])
        );
    }

    #[test]
    fn a_zero_width_version_is_not_stored() {
        let mut h = KvHistory::default();
        h.record(b"k", version(10, 10, b"a"));
        assert!(h.is_empty());
        assert_eq!(h.bytes(), 0);
    }

    #[test]
    fn purge_keeps_the_version_visible_at_the_horizon() {
        let mut h = KvHistory::default();
        h.record(b"k", version(10, 20, b"a"));
        h.record(b"k", version(20, 30, b"b"));
        h.record(b"gone", version(5, 15, b"x"));
        assert_eq!(h.purge_below(25), 2);
        assert_eq!(
            h.visible_at(b"k", 25).map(|v| v.value.as_slice()),
            Some(&b"b"[..])
        );
        assert!(
            h.versions_of(b"gone").is_empty(),
            "a key deleted below the horizon leaves"
        );
        assert_eq!(h.version_count(), 1);
    }

    #[test]
    fn purge_at_the_end_ordinal_removes_the_version() {
        let mut h = KvHistory::default();
        h.record(b"k", version(10, 20, b"a"));
        assert_eq!(h.purge_below(19), 0);
        assert_eq!(h.purge_below(20), 1);
        assert!(h.is_empty());
        assert_eq!(h.bytes(), 0);
    }

    #[test]
    fn replace_makes_the_list_the_whole_history() {
        let mut h = KvHistory::default();
        let v = |from: i64, to: i64| HistoryVersion {
            sys_from: from,
            sys_to: to,
            value: vec![1],
            expire_at_ms: 0,
            surrogate: Surrogate::new(1),
        };
        h.record(b"k", v(1, 2));
        h.record(b"k", v(2, 3));
        h.replace(b"k", vec![v(5, 6)]);
        assert_eq!(h.versions_of(b"k"), &[v(5, 6)]);
        assert_eq!(h.version_count(), 1);
        h.replace(b"k", Vec::new());
        assert!(h.is_empty());
        assert_eq!(h.bytes(), 0);
    }

    #[test]
    fn splice_ends_the_version_before_and_replaces_its_own() {
        let mut h = KvHistory::default();
        h.record(b"k", version(10, 50, b"a"));
        h.splice(b"k", version(20, 50, b"b"));
        assert_eq!(
            h.versions_of(b"k")
                .iter()
                .map(|v| (v.sys_from, v.sys_to))
                .collect::<Vec<_>>(),
            vec![(10, 20), (20, 50)]
        );
        h.splice(b"k", version(20, 50, b"b"));
        assert_eq!(
            h.version_count(),
            2,
            "a splice at the same ordinal replaces"
        );
        assert_eq!(
            h.visible_at(b"k", 15).map(|v| v.value.as_slice()),
            Some(&b"a"[..])
        );
        assert_eq!(
            h.visible_at(b"k", 25).map(|v| v.value.as_slice()),
            Some(&b"b"[..])
        );
    }

    #[test]
    fn remove_version_takes_out_one_version_and_its_bytes() {
        let mut h = KvHistory::default();
        h.record(b"k", version(10, 20, b"a"));
        let one = h.bytes();
        h.record(b"k", version(20, 30, b"b"));
        assert!(h.remove_version(b"k", 20));
        assert_eq!(h.bytes(), one);
        assert_eq!(h.version_count(), 1);
        assert!(!h.remove_version(b"k", 15), "no version starts there");
        assert!(h.remove_version(b"k", 10));
        assert!(h.is_empty());
        assert_eq!(h.bytes(), 0, "the key leaves with its last version");
    }
}
