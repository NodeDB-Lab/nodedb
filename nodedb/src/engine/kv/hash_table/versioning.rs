// SPDX-License-Identifier: BUSL-1.1

//! Version retirement for [`KvHashTable`]: moving an ended live version
//! into the table's history, retiring a whole table at a truncate, and the
//! purge of versions below a horizon.

use nodedb_types::Surrogate;

use super::super::entry::KvEntry;
use super::super::hash_helpers::{extract_value_from, free_value_from};
use super::super::history::{HistoryVersion, VersionStamp};
use super::super::slab::SlabAllocator;
use super::types::KvHashTable;

/// Which slot array of the table an index points into.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SlotArray {
    Primary,
    RehashSource,
}

/// The state of a live version a write is about to replace or remove.
pub(super) struct Superseded {
    pub sys_from: i64,
    pub value: Vec<u8>,
    pub expire_at_ms: u64,
    pub surrogate: Surrogate,
    /// [`KvEntry::mem_size`] of the live entry.
    pub mem_size: usize,
}

impl Superseded {
    /// Capture `entry` before its value leaves the overflow pool.
    pub(super) fn of(entry: &KvEntry, overflow: &SlabAllocator) -> Self {
        Self {
            sys_from: entry.sys_from,
            value: extract_value_from(&entry.value, overflow),
            expire_at_ms: entry.expire_at_ms,
            surrogate: entry.surrogate,
            mem_size: entry.mem_size(),
        }
    }
}

impl KvHashTable {
    /// The entry at `idx` of `array`.
    pub(super) fn slot_ref(&self, array: SlotArray, idx: usize) -> Option<&KvEntry> {
        match array {
            SlotArray::Primary => self.slots.get(idx)?.as_ref(),
            SlotArray::RehashSource => self.rehash_source.as_ref()?.get(idx)?.as_ref(),
        }
    }

    /// The entry at `idx` of `array`, mutable.
    pub(super) fn slot_mut(&mut self, array: SlotArray, idx: usize) -> Option<&mut KvEntry> {
        match array {
            SlotArray::Primary => self.slots.get_mut(idx)?.as_mut(),
            SlotArray::RehashSource => self.rehash_source.as_mut()?.get_mut(idx)?.as_mut(),
        }
    }

    /// Record `superseded` as an ended version of `key` when `stamp` starts
    /// a new version. Returns the superseded value.
    pub(super) fn end_version(
        &mut self,
        key: &[u8],
        superseded: Superseded,
        stamp: VersionStamp,
    ) -> Vec<u8> {
        if let VersionStamp::NewVersion(sys_to) = stamp {
            self.history.record(
                key,
                HistoryVersion {
                    sys_from: superseded.sys_from,
                    sys_to,
                    value: superseded.value.clone(),
                    expire_at_ms: superseded.expire_at_ms,
                    surrogate: superseded.surrogate,
                },
            );
        }
        superseded.value
    }

    /// Remove the live entry at `idx` of `array` at system-time `ordinal`.
    /// The removed version moves into history, ended at the removal.
    pub(super) fn retire_slot(&mut self, array: SlotArray, idx: usize, ordinal: i64) -> bool {
        let slots = match array {
            SlotArray::Primary => &mut self.slots,
            SlotArray::RehashSource => match self.rehash_source.as_mut() {
                Some(slots) => slots,
                None => return false,
            },
        };
        let Some(entry) = slots.get_mut(idx).and_then(Option::take) else {
            return false;
        };
        Self::repair_after_delete_static(slots, idx);
        let superseded = Superseded::of(&entry, &self.overflow);
        free_value_from(&entry.value, &mut self.overflow);
        self.surrogate_to_key.remove(&entry.surrogate.0);
        self.len -= 1;
        self.entry_bytes = self.entry_bytes.saturating_sub(superseded.mem_size);
        let stamp = VersionStamp::of(ordinal, superseded.sys_from);
        self.end_version(&entry.key, superseded, stamp);
        true
    }

    /// Remove every live entry at system-time `ordinal`, as a truncate does.
    /// Every removed version moves into history, ended at the truncate, so a
    /// read at an earlier ordinal still sees the rows. Returns the number of
    /// entries removed.
    pub fn retire_all(&mut self, ordinal: i64) -> usize {
        let mut taken: Vec<KvEntry> = self.slots.iter_mut().filter_map(Option::take).collect();
        if let Some(old) = self.rehash_source.take() {
            taken.extend(old.into_iter().flatten());
        }
        self.rehash_cursor = 0;
        let removed = taken.len();
        for entry in taken {
            let superseded = Superseded::of(&entry, &self.overflow);
            free_value_from(&entry.value, &mut self.overflow);
            let stamp = VersionStamp::of(ordinal, superseded.sys_from);
            self.end_version(&entry.key, superseded, stamp);
        }
        self.surrogate_to_key.clear();
        self.len = 0;
        self.entry_bytes = 0;
        removed
    }

    /// Remove every ended version whose successor or delete starts at or
    /// below `horizon`. The version visible at `horizon` stays, live or
    /// ended. Returns the number of versions removed.
    pub fn purge_versions_below(&mut self, horizon: i64) -> usize {
        self.history.purge_below(horizon)
    }

    /// Install the ended versions of `key` a checkpoint carried.
    pub fn restore_history(&mut self, key: &[u8], versions: Vec<HistoryVersion>) {
        self.history.restore(key, versions);
    }

    /// Install an ended version of `key` between the versions it holds.
    pub fn splice_history(&mut self, key: &[u8], version: HistoryVersion) {
        self.history.splice(key, version);
    }

    /// The ordinal the version of `key` that starts at `sys_from` ends at,
    /// when a stored version or delete of the key sits above `sys_from`:
    /// the start of the next version, or the delete that ends the version
    /// before it. `None` when `sys_from` is at or above every stored version
    /// and delete.
    pub fn next_version_above(&self, key: &[u8], sys_from: i64) -> Option<i64> {
        let live = self
            .find_entry(key)
            .map(|entry| entry.sys_from)
            .filter(|start| *start > sys_from);
        let ended = self.history.versions_of(key).iter().filter_map(|v| {
            if v.sys_from > sys_from {
                Some(v.sys_from)
            } else if v.sys_to > sys_from {
                Some(v.sys_to)
            } else {
                None
            }
        });
        live.into_iter().chain(ended).min()
    }

    /// The newest ordinal a version or a delete of `key` names: the live
    /// row's start, or the end of the key's newest ended version. `None`
    /// when the key holds neither.
    pub fn newest_ordinal_of(&self, key: &[u8]) -> Option<i64> {
        let live = self.find_entry(key).map(|entry| entry.sys_from);
        let ended = self.history.versions_of(key).iter().map(|v| v.sys_to).max();
        live.max(ended)
    }

    /// Make `versions` the whole ended history of `key`.
    pub fn replace_history(&mut self, key: &[u8], versions: Vec<HistoryVersion>) {
        self.history.replace(key, versions);
    }

    /// The highest system-time ordinal any version of this table names.
    pub fn max_ordinal(&self) -> Option<i64> {
        let live = self.live_entries().map(|e| e.sys_from).max();
        match (live, self.history.max_ordinal()) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (a, b) => a.or(b),
        }
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

    #[test]
    fn overwrite_moves_the_old_version_into_history() {
        let mut t = table();
        t.put(b"k", b"v1", NO_EXPIRY, Surrogate::new(1), 10);
        t.put(b"k", b"v2", NO_EXPIRY, Surrogate::new(1), 20);
        let ended = t.history().versions_of(b"k");
        assert_eq!(ended.len(), 1);
        assert_eq!((ended[0].sys_from, ended[0].sys_to), (10, 20));
        assert_eq!(ended[0].value, b"v1");
        assert_eq!(t.get(b"k", 0), Some(b"v2".as_slice()));
    }

    #[test]
    fn a_rewrite_at_the_same_ordinal_keeps_no_history() {
        let mut t = table();
        t.put(b"k", b"v1", NO_EXPIRY, Surrogate::new(1), 10);
        t.put(b"k", b"v2", NO_EXPIRY, Surrogate::new(1), 10);
        assert!(t.history().is_empty());
    }

    #[test]
    fn delete_ends_the_version_at_the_delete() {
        let mut t = table();
        t.put(b"k", b"v1", NO_EXPIRY, Surrogate::new(1), 10);
        assert!(t.delete(b"k", 15));
        assert_eq!(t.len(), 0);
        let ended = t.history().versions_of(b"k");
        assert_eq!((ended[0].sys_from, ended[0].sys_to), (10, 15));
    }

    #[test]
    fn retire_all_moves_every_row_into_history() {
        let mut t = table();
        for i in 0..20u8 {
            t.put(&[i], &[i], NO_EXPIRY, Surrogate::new(u32::from(i) + 1), 5);
        }
        assert_eq!(t.retire_all(9), 20);
        assert_eq!(t.len(), 0);
        assert_eq!(t.history().version_count(), 20);
        assert_eq!(t.max_ordinal(), Some(9));
    }

    #[test]
    fn entry_bytes_track_inserts_overwrites_and_deletes() {
        let mut t = table();
        let empty = t.mem_usage();
        t.put(b"k", b"v1", NO_EXPIRY, Surrogate::new(1), 10);
        t.put(b"k", b"v2", NO_EXPIRY, Surrogate::new(1), 10);
        assert!(t.delete(b"k", 10));
        assert_eq!(
            t.mem_usage(),
            empty,
            "a same-ordinal put and delete keep no bytes"
        );
    }
}
