// SPDX-License-Identifier: BUSL-1.1

//! Reads of a [`KvHashTable`] at a past system-time ordinal.
//!
//! A read at ordinal `S` sees, per key, the live entry when its `sys_from <=
//! S`, else the ended version whose interval holds `S`. Expiry hides a
//! version whose `expire_at_ms` passed at the read's wall time, as a current
//! read hides one that passed at `now`. A point read costs one probe plus a
//! binary search of the key's ended versions.

use nodedb_types::Surrogate;

use super::super::entry::{KvEntry, NO_EXPIRY};
use super::super::hash_helpers::read_value_from;
use super::super::history::HistoryVersion;
use super::super::scan::matches_pattern_pub;
use super::types::KvHashTable;

/// One version of one KV row, borrowed from the table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VersionedRow<'a> {
    pub key: &'a [u8],
    pub value: &'a [u8],
    pub surrogate: Surrogate,
    /// Absolute expiry instant in ms, `0` for none.
    pub expire_at_ms: u64,
    /// Ordinal of the write that set this version.
    pub sys_from: i64,
    /// Ordinal of the write or delete that ended it. `None` for the live
    /// version.
    pub sys_to: Option<i64>,
}

impl VersionedRow<'_> {
    /// Whether the version's expiry passed at wall time `wall_ms`.
    fn expired_at(&self, wall_ms: u64) -> bool {
        self.expire_at_ms != NO_EXPIRY && wall_ms >= self.expire_at_ms
    }
}

impl KvHashTable {
    fn live_row<'a>(&'a self, entry: &'a KvEntry) -> VersionedRow<'a> {
        VersionedRow {
            key: &entry.key,
            value: read_value_from(entry, &self.overflow),
            surrogate: entry.surrogate,
            expire_at_ms: entry.expire_at_ms,
            sys_from: entry.sys_from,
            sys_to: None,
        }
    }

    fn ended_row<'a>(key: &'a [u8], version: &'a HistoryVersion) -> VersionedRow<'a> {
        VersionedRow {
            key,
            value: &version.value,
            surrogate: version.surrogate,
            expire_at_ms: version.expire_at_ms,
            sys_from: version.sys_from,
            sys_to: Some(version.sys_to),
        }
    }

    /// The version of `key` a read at ordinal `at` sees, without expiry.
    fn version_at<'a>(&'a self, key: &'a [u8], at: i64) -> Option<VersionedRow<'a>> {
        if let Some(entry) = self.find_entry(key)
            && entry.sys_from <= at
        {
            return Some(self.live_row(entry));
        }
        self.history
            .visible_at(key, at)
            .map(|version| Self::ended_row(key, version))
    }

    /// The version of `key` visible at ordinal `at`, hidden when its expiry
    /// passed at wall time `wall_ms`.
    pub fn get_at<'a>(&'a self, key: &'a [u8], at: i64, wall_ms: u64) -> Option<VersionedRow<'a>> {
        self.version_at(key, at)
            .filter(|row| !row.expired_at(wall_ms))
    }

    /// Every row visible at ordinal `at` whose key matches `pattern`, hidden
    /// when its expiry passed at wall time `wall_ms`. Keys the table holds
    /// come first, in slot order. Keys the table no longer holds follow, in
    /// key order.
    pub fn rows_at(&self, at: i64, wall_ms: u64, pattern: Option<&str>) -> Vec<VersionedRow<'_>> {
        let mut rows: Vec<VersionedRow<'_>> = self
            .live_entries()
            .filter(|entry| matches_pattern_pub(&entry.key, pattern))
            .filter_map(|entry| {
                if entry.sys_from <= at {
                    Some(self.live_row(entry))
                } else {
                    self.history
                        .visible_at(&entry.key, at)
                        .map(|version| Self::ended_row(&entry.key, version))
                }
            })
            .filter(|row| !row.expired_at(wall_ms))
            .collect();
        let mut removed: Vec<VersionedRow<'_>> = self
            .history
            .iter()
            .filter(|(key, _)| matches_pattern_pub(key, pattern) && self.find_entry(key).is_none())
            .filter_map(|(key, _)| {
                self.history
                    .visible_at(key, at)
                    .map(|v| Self::ended_row(key, v))
            })
            .filter(|row| !row.expired_at(wall_ms))
            .collect();
        removed.sort_by(|a, b| a.key.cmp(b.key));
        rows.append(&mut removed);
        rows
    }

    /// Every version of every row whose key matches `pattern`, each key's
    /// versions oldest first. Keys the table holds come first, in slot
    /// order. Keys the table no longer holds follow, in key order.
    pub fn all_versions(&self, pattern: Option<&str>) -> Vec<VersionedRow<'_>> {
        let mut rows = Vec::new();
        for entry in self.live_entries() {
            if !matches_pattern_pub(&entry.key, pattern) {
                continue;
            }
            rows.extend(
                self.history
                    .versions_of(&entry.key)
                    .iter()
                    .map(|v| Self::ended_row(&entry.key, v)),
            );
            rows.push(self.live_row(entry));
        }
        let mut removed: Vec<(&[u8], &[HistoryVersion])> = self
            .history
            .iter()
            .filter(|(key, _)| matches_pattern_pub(key, pattern) && self.find_entry(key).is_none())
            .collect();
        removed.sort_by(|a, b| a.0.cmp(b.0));
        for (key, versions) in removed {
            rows.extend(versions.iter().map(|v| Self::ended_row(key, v)));
        }
        rows
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::Surrogate;

    use super::super::super::entry::NO_EXPIRY;
    use super::super::types::KvHashTable;

    fn table() -> KvHashTable {
        KvHashTable::new(16, 0.75, 4, 64)
    }

    fn value_at(t: &KvHashTable, key: &[u8], at: i64) -> Option<Vec<u8>> {
        t.get_at(key, at, 0).map(|row| row.value.to_vec())
    }

    #[test]
    fn put_overwrite_delete_reinsert_read_at_each_ordinal() {
        let mut t = table();
        t.put(b"k", b"v1", NO_EXPIRY, Surrogate::new(1), 10);
        t.put(b"k", b"v2", NO_EXPIRY, Surrogate::new(1), 20);
        assert!(t.delete(b"k", 30));
        t.put(b"k", b"v3", NO_EXPIRY, Surrogate::new(1), 40);

        assert_eq!(value_at(&t, b"k", 9), None);
        assert_eq!(value_at(&t, b"k", 10), Some(b"v1".to_vec()));
        assert_eq!(value_at(&t, b"k", 25), Some(b"v2".to_vec()));
        assert_eq!(value_at(&t, b"k", 35), None, "deleted between 30 and 40");
        assert_eq!(value_at(&t, b"k", 40), Some(b"v3".to_vec()));
        assert_eq!(
            t.get(b"k", 0),
            Some(b"v3".as_slice()),
            "current read unchanged"
        );
    }

    #[test]
    fn rows_at_includes_keys_deleted_after_the_cut() {
        let mut t = table();
        t.put(b"a", b"1", NO_EXPIRY, Surrogate::new(1), 10);
        t.put(b"b", b"2", NO_EXPIRY, Surrogate::new(2), 10);
        assert!(t.delete(b"b", 20));
        t.put(b"c", b"3", NO_EXPIRY, Surrogate::new(3), 30);

        let keys = |at| {
            let mut keys: Vec<Vec<u8>> = t
                .rows_at(at, 0, None)
                .iter()
                .map(|r| r.key.to_vec())
                .collect();
            keys.sort();
            keys
        };
        assert_eq!(keys(15), vec![b"a".to_vec(), b"b".to_vec()]);
        assert_eq!(keys(25), vec![b"a".to_vec()]);
        assert_eq!(keys(35), vec![b"a".to_vec(), b"c".to_vec()]);
    }

    #[test]
    fn expiry_hides_a_version_at_the_read_wall_time() {
        let mut t = table();
        t.put(b"k", b"v", 5_000, Surrogate::new(1), 10);
        assert!(t.get_at(b"k", 10, 4_999).is_some());
        assert!(t.get_at(b"k", 10, 5_000).is_none());
    }

    #[test]
    fn all_versions_lists_each_key_oldest_first() {
        let mut t = table();
        t.put(b"k", b"v1", NO_EXPIRY, Surrogate::new(1), 10);
        t.put(b"k", b"v2", NO_EXPIRY, Surrogate::new(1), 20);
        t.put(b"gone", b"x", NO_EXPIRY, Surrogate::new(2), 5);
        assert!(t.delete(b"gone", 6));
        let rows = t.all_versions(None);
        let summary: Vec<(&[u8], i64, Option<i64>)> =
            rows.iter().map(|r| (r.key, r.sys_from, r.sys_to)).collect();
        assert_eq!(
            summary,
            vec![
                (&b"k"[..], 10, Some(20)),
                (&b"k"[..], 20, None),
                (&b"gone"[..], 5, Some(6)),
            ]
        );
    }
}
