// SPDX-License-Identifier: BUSL-1.1

//! Public KvHashTable API: get, put, delete, reap, set_expire, persist.
//!
//! Every mutation carries the system-time ordinal of the write. A write that
//! ends a stored version moves it into the table's history (see
//! `versioning.rs`), so a read at an earlier ordinal still sees it.

use nodedb_types::Surrogate;

use super::super::entry::{KvEntry, NO_EXPIRY};
use super::super::hash_helpers::{free_value_from, hash_key, read_value_from, store_value_in};
use super::super::history::VersionStamp;
use super::types::{EntryMeta, KvHashTable};
use super::versioning::{SlotArray, Superseded};

impl KvHashTable {
    /// Get a value by key. Returns None if not found or expired.
    ///
    /// Checks the primary table first, then the rehash source (if active).
    /// Expired keys return None (lazy expiry fallback).
    pub fn get(&self, key: &[u8], now_ms: u64) -> Option<&[u8]> {
        self.get_with_surrogate(key, now_ms).map(|(v, _)| v)
    }

    /// Get the value AND the row's stable surrogate for a key.  Used by
    /// the clone-delegated read path to filter rows whose binding was
    /// allocated AFTER the clone's AS-OF point (snapshot isolation).
    /// Returns `None` if the key is missing or expired.
    pub fn get_with_surrogate(&self, key: &[u8], now_ms: u64) -> Option<(&[u8], Surrogate)> {
        let entry = self.find_entry(key)?;
        if entry.is_expired(now_ms) {
            return None;
        }
        Some((read_value_from(entry, &self.overflow), entry.surrogate))
    }

    /// The live entry stored under `key`, primary table first, then the
    /// rehash source. Expiry-blind.
    pub(crate) fn find_entry(&self, key: &[u8]) -> Option<&KvEntry> {
        let h = hash_key(key);
        if let Some(entry) = self.probe_find(&self.slots, h, key) {
            return Some(entry);
        }
        let old = self.rehash_source.as_ref()?;
        self.probe_find(old, h, key)
    }

    /// Get entry metadata without returning the value.
    ///
    /// Returns the TTL state and expiry timestamp for a key, used by
    /// the KV engine to cancel old expiry entries before updates.
    /// Does NOT check expiry — returns metadata even for expired keys,
    /// since the caller needs the original `expire_at_ms` for cancellation.
    pub fn get_entry_meta(&self, key: &[u8]) -> Option<EntryMeta> {
        self.find_entry(key).map(|entry| EntryMeta {
            has_ttl: entry.has_ttl(),
            expire_at_ms: entry.expire_at_ms,
        })
    }

    /// Replace an existing entry's value and expiry at `ordinal`, keeping its
    /// surrogate. Returns the old value. An absent key is not written and
    /// returns `None`.
    pub fn rewrite(
        &mut self,
        key: &[u8],
        value: &[u8],
        expire_at_ms: u64,
        ordinal: i64,
    ) -> Option<Vec<u8>> {
        let surrogate = self.find_entry(key)?.surrogate;
        self.put(key, value, expire_at_ms, surrogate, ordinal)
    }

    /// Read a key's value ignoring expiry.
    ///
    /// Unlike [`get`], returns the bytes of an entry whose TTL has elapsed but
    /// which the expiry wheel has not reaped yet. The reaper needs them to
    /// remove the row's secondary/sorted index entries, which are keyed on the
    /// field values held in the value itself — by the time the reaper runs the
    /// TTL has elapsed by definition, so a `get` would report the row as
    /// already gone and the index entries would be stranded.
    ///
    /// A live entry that a rehash has not migrated yet exists ONLY in the
    /// rehash source, so the probe covers both tables.
    ///
    /// [`get`]: KvHashTable::get
    pub fn get_ignoring_expiry(&self, key: &[u8]) -> Option<&[u8]> {
        self.find_entry(key)
            .map(|entry| read_value_from(entry, &self.overflow))
    }

    /// Insert or update a key-value pair at system-time `ordinal`. Returns
    /// the old value bytes if overwritten.
    ///
    /// `surrogate` is the row's bound global identity, never
    /// `Surrogate::ZERO`: `KvEngine` refuses an unbound write before it
    /// reaches the table.
    /// - On insert of a new row, it is recorded in the reverse map.
    /// - On update of an existing row, the entry keeps the surrogate it was
    ///   first bound under, and the replaced version moves into history
    ///   ([`VersionStamp`]).
    ///
    /// Triggers incremental rehash migration if a rehash is in progress.
    /// Triggers a new rehash if the load factor exceeds the threshold.
    pub fn put(
        &mut self,
        key: &[u8],
        value: &[u8],
        expire_at_ms: u64,
        surrogate: Surrogate,
        ordinal: i64,
    ) -> Option<Vec<u8>> {
        // Progress incremental rehash.
        self.rehash_step();

        let h = hash_key(key);

        // Check if key exists in primary — update in place (no key copy needed).
        if let Some(idx) = Self::probe_find_index_static(&self.slots, h, key) {
            let superseded = {
                let slot = self.slots[idx].as_ref()?;
                let superseded = Superseded::of(slot, &self.overflow);
                free_value_from(&slot.value, &mut self.overflow);
                superseded
            };
            let stamp = VersionStamp::of(ordinal, superseded.sys_from);
            let new_kv_value = store_value_in(&mut self.overflow, value, self.inline_threshold);
            let entry = self.slots[idx].as_mut()?;
            entry.value = new_kv_value;
            entry.expire_at_ms = expire_at_ms;
            if let VersionStamp::NewVersion(at) = stamp {
                entry.sys_from = at;
            }
            let new_size = entry.mem_size();
            self.entry_bytes = self.entry_bytes.saturating_sub(superseded.mem_size) + new_size;
            return Some(self.end_version(key, superseded, stamp));
        }

        // Check rehash source — if found, remove from old and insert into primary.
        if let Some(old_slots) = self.rehash_source.as_mut()
            && let Some(idx) = Self::probe_find_index_static(old_slots, h, key)
        {
            let old_entry = old_slots[idx].take()?;
            let superseded = Superseded::of(&old_entry, &self.overflow);
            free_value_from(&old_entry.value, &mut self.overflow);
            let stamp = VersionStamp::of(ordinal, superseded.sys_from);
            let new_kv_value = store_value_in(&mut self.overflow, value, self.inline_threshold);
            // The migrated entry keeps its bound surrogate, and its reverse-map
            // row already names this key.
            let new_entry = KvEntry {
                hash: h,
                key: key.to_vec(), // Only copy key when migrating from rehash source.
                value: new_kv_value,
                expire_at_ms,
                surrogate: old_entry.surrogate,
                sys_from: match stamp {
                    VersionStamp::SameVersion => superseded.sys_from,
                    VersionStamp::NewVersion(at) => at,
                },
            };
            self.entry_bytes =
                self.entry_bytes.saturating_sub(superseded.mem_size) + new_entry.mem_size();
            Self::robin_hood_insert(&mut self.slots, new_entry);
            return Some(self.end_version(key, superseded, stamp));
        }

        // New key — insert into primary. Single key copy here (unavoidable — entry owns key).
        // A key a delete or a reap ended starts its new version no earlier
        // than that end, so its versions never overlap. The ended version is
        // visible below its end only, so the new one may start at it.
        let sys_from = match self.history.newest_end(key) {
            Some(end) if end > ordinal => end,
            _ => ordinal,
        };
        let kv_value = store_value_in(&mut self.overflow, value, self.inline_threshold);
        self.surrogate_to_key.insert(surrogate.0, key.to_vec());
        let entry = KvEntry {
            hash: h,
            key: key.to_vec(),
            value: kv_value,
            expire_at_ms,
            surrogate,
            sys_from,
        };
        self.entry_bytes += entry.mem_size();
        Self::robin_hood_insert(&mut self.slots, entry);
        self.len += 1;

        // Check if we need to start a rehash.
        self.maybe_start_rehash();

        None
    }

    /// Delete a key at system-time `ordinal`. Returns true if the key existed
    /// and was removed. The removed version moves into history, ended at the
    /// delete.
    pub fn delete(&mut self, key: &[u8], ordinal: i64) -> bool {
        let h = hash_key(key);
        if let Some(idx) = Self::probe_find_index_static(&self.slots, h, key) {
            return self.retire_slot(SlotArray::Primary, idx, ordinal);
        }
        if let Some(old_slots) = self.rehash_source.as_ref()
            && let Some(idx) = Self::probe_find_index_static(old_slots, h, key)
        {
            return self.retire_slot(SlotArray::RehashSource, idx, ordinal);
        }
        false
    }

    /// Remove an expired entry by key at system-time `ordinal` (called by
    /// the expiry wheel reaper). Only removes if the entry still exists and
    /// its expire_at_ms matches. The reaped version moves into history.
    pub fn reap_expired(&mut self, key: &[u8], expected_expire_ms: u64, ordinal: i64) -> bool {
        let h = hash_key(key);
        if let Some(idx) = Self::probe_find_index_static(&self.slots, h, key)
            && self.slots[idx]
                .as_ref()
                .is_some_and(|e| e.expire_at_ms == expected_expire_ms)
        {
            return self.retire_slot(SlotArray::Primary, idx, ordinal);
        }
        if let Some(old_slots) = self.rehash_source.as_ref()
            && let Some(idx) = Self::probe_find_index_static(old_slots, h, key)
            && old_slots[idx]
                .as_ref()
                .is_some_and(|e| e.expire_at_ms == expected_expire_ms)
        {
            return self.retire_slot(SlotArray::RehashSource, idx, ordinal);
        }
        false
    }

    /// Update the TTL of an existing key at system-time `ordinal`. Returns
    /// true if the key was found. The version with the old expiry moves into
    /// history.
    pub fn set_expire(&mut self, key: &[u8], expire_at_ms: u64, ordinal: i64) -> bool {
        let h = hash_key(key);
        let (array, idx) = if let Some(idx) = Self::probe_find_index_static(&self.slots, h, key) {
            (SlotArray::Primary, idx)
        } else if let Some(old_slots) = self.rehash_source.as_ref()
            && let Some(idx) = Self::probe_find_index_static(old_slots, h, key)
        {
            (SlotArray::RehashSource, idx)
        } else {
            return false;
        };
        let Some(entry) = self.slot_ref(array, idx) else {
            return false;
        };
        let superseded = Superseded::of(entry, &self.overflow);
        let stamp = VersionStamp::of(ordinal, superseded.sys_from);
        if let Some(entry) = self.slot_mut(array, idx) {
            entry.expire_at_ms = expire_at_ms;
            if let VersionStamp::NewVersion(at) = stamp {
                entry.sys_from = at;
            }
        }
        self.end_version(key, superseded, stamp);
        true
    }

    /// Remove TTL from a key (make it persistent) at system-time `ordinal`.
    /// Returns true if found.
    pub fn persist(&mut self, key: &[u8], ordinal: i64) -> bool {
        self.set_expire(key, NO_EXPIRY, ordinal)
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::Surrogate;

    use super::super::super::entry::NO_EXPIRY;
    use super::super::types::KvHashTable;

    fn make_table() -> KvHashTable {
        KvHashTable::new(16, 0.75, 4, 64)
    }

    #[test]
    fn basic_put_get_delete() {
        let mut t = make_table();
        assert!(t.is_empty());

        t.put(b"key1", b"value1", NO_EXPIRY, Surrogate::new(1), 1);
        assert_eq!(t.len(), 1);
        assert_eq!(t.get(b"key1", 0), Some(b"value1".as_slice()));

        t.put(b"key2", b"value2", NO_EXPIRY, Surrogate::new(2), 1);
        assert_eq!(t.len(), 2);

        assert!(t.delete(b"key1", 0));
        assert_eq!(t.len(), 1);
        assert!(t.get(b"key1", 0).is_none());
        assert_eq!(t.get(b"key2", 0), Some(b"value2".as_slice()));
    }

    #[test]
    fn overwrite_returns_old_value() {
        let mut t = make_table();
        assert!(
            t.put(b"k", b"v1", NO_EXPIRY, Surrogate::new(1), 1)
                .is_none()
        );
        let old = t.put(b"k", b"v2", NO_EXPIRY, Surrogate::new(1), 1);
        assert_eq!(old, Some(b"v1".to_vec()));
        assert_eq!(t.get(b"k", 0), Some(b"v2".as_slice()));
        assert_eq!(t.len(), 1);
    }

    #[test]
    fn delete_nonexistent_returns_false() {
        let mut t = make_table();
        assert!(!t.delete(b"nope", 0));
    }

    #[test]
    fn lazy_expiry_on_get() {
        let mut t = make_table();
        t.put(b"k", b"v", 1000, Surrogate::new(1), 1);

        assert_eq!(t.get(b"k", 999), Some(b"v".as_slice()));
        assert!(t.get(b"k", 1000).is_none()); // Expired.
        assert!(t.get(b"k", 2000).is_none());
    }

    #[test]
    fn set_expire_and_persist() {
        let mut t = make_table();
        t.put(b"k", b"v", NO_EXPIRY, Surrogate::new(1), 1);

        assert!(t.set_expire(b"k", 5000, 1));
        assert!(t.get(b"k", 4999).is_some());
        assert!(t.get(b"k", 5000).is_none());

        // Reset expiry to force it to be visible again — need to re-put.
        t.put(b"k", b"v", 10000, Surrogate::new(1), 1);
        assert!(t.persist(b"k", 1));
        assert!(t.get(b"k", u64::MAX).is_some()); // Never expires.
    }

    #[test]
    fn reap_expired_removes_matching() {
        let mut t = make_table();
        t.put(b"k", b"v", 5000, Surrogate::new(1), 1);

        // Wrong expire_at_ms — should not reap.
        assert!(!t.reap_expired(b"k", 9999, 1));
        assert_eq!(t.len(), 1);

        // Correct expire_at_ms — should reap.
        assert!(t.reap_expired(b"k", 5000, 1));
        assert_eq!(t.len(), 0);
    }

    #[test]
    fn incremental_rehash() {
        let mut t = KvHashTable::new(16, 0.5, 2, 64);

        // Fill to trigger rehash (>50% of 16 = >8 entries).
        for i in 0..10u32 {
            let key = format!("key{i:03}");
            let val = format!("val{i:03}");
            t.put(
                key.as_bytes(),
                val.as_bytes(),
                NO_EXPIRY,
                Surrogate::new(i + 1),
                1,
            );
        }

        // Rehash should have been triggered.
        // Continue inserting to drive incremental migration.
        for i in 10..20u32 {
            let key = format!("key{i:03}");
            let val = format!("val{i:03}");
            t.put(
                key.as_bytes(),
                val.as_bytes(),
                NO_EXPIRY,
                Surrogate::new(i + 1),
                1,
            );
        }

        // All entries should be findable.
        for i in 0..20 {
            let key = format!("key{i:03}");
            let val = format!("val{i:03}");
            assert_eq!(
                t.get(key.as_bytes(), 0),
                Some(val.as_bytes()),
                "missing key{i:03}"
            );
        }
        assert_eq!(t.len(), 20);
    }

    #[test]
    fn overflow_values() {
        let mut t = KvHashTable::new(16, 0.75, 4, 8); // 8-byte inline threshold.
        let small = b"tiny".to_vec(); // 4 bytes — inline.
        let large = vec![0xAB; 100]; // 100 bytes — overflow.

        t.put(b"s", &small, NO_EXPIRY, Surrogate::new(1), 1);
        t.put(b"l", &large, NO_EXPIRY, Surrogate::new(2), 1);

        assert_eq!(t.get(b"s", 0), Some(small.as_slice()));
        assert_eq!(t.get(b"l", 0), Some(large.as_slice()));
    }

    #[test]
    fn many_inserts_and_deletes_no_corruption() {
        let mut t = KvHashTable::new(32, 0.75, 8, 64);

        // Insert 500 keys.
        for i in 0u32..500 {
            t.put(
                &i.to_be_bytes(),
                &(i * 7).to_be_bytes(),
                NO_EXPIRY,
                Surrogate::new(i + 1),
                1,
            );
        }
        assert_eq!(t.len(), 500);

        // Delete even keys.
        for i in (0u32..500).step_by(2) {
            let key = i.to_be_bytes().to_vec();
            assert!(t.delete(&key, 0), "failed to delete key {i}");
        }
        assert_eq!(t.len(), 250);

        // Verify odd keys are still present.
        for i in (1u32..500).step_by(2) {
            let key = i.to_be_bytes();
            let expected = (i * 7).to_be_bytes();
            assert_eq!(
                t.get(&key, 0),
                Some(expected.as_slice()),
                "missing odd key {i}"
            );
        }

        // Verify even keys are gone.
        for i in (0u32..500).step_by(2) {
            let key = i.to_be_bytes();
            assert!(t.get(&key, 0).is_none(), "even key {i} should be deleted");
        }
    }

    #[test]
    fn get_entry_meta_returns_ttl_info() {
        let mut t = make_table();
        // Key without TTL.
        t.put(b"persistent", b"v", NO_EXPIRY, Surrogate::new(1), 1);
        let meta = t.get_entry_meta(b"persistent").unwrap();
        assert!(!meta.has_ttl);
        assert_eq!(meta.expire_at_ms, NO_EXPIRY);

        // Key with TTL.
        t.put(b"ephemeral", b"v", 5000, Surrogate::new(2), 1);
        let meta = t.get_entry_meta(b"ephemeral").unwrap();
        assert!(meta.has_ttl);
        assert_eq!(meta.expire_at_ms, 5000);

        // Non-existent key.
        assert!(t.get_entry_meta(b"nope").is_none());
    }

    #[test]
    fn surrogate_round_trip_via_reverse_map() {
        let mut t = make_table();
        let s1 = Surrogate::new(101);
        let s2 = Surrogate::new(202);

        t.put(b"alpha", b"v1", NO_EXPIRY, s1, 1);
        t.put(b"beta", b"v2", NO_EXPIRY, s2, 1);

        assert_eq!(t.surrogate_count(), 2);
        assert_eq!(t.key_for_surrogate(s1), Some(b"alpha".as_slice()));
        assert_eq!(t.key_for_surrogate(s2), Some(b"beta".as_slice()));
        assert!(t.key_for_surrogate(Surrogate::new(999)).is_none());
        assert!(t.key_for_surrogate(Surrogate::ZERO).is_none());

        // Updating an existing key keeps the surrogate it was first bound
        // under (the assigner is idempotent).
        t.put(b"alpha", b"v1b", NO_EXPIRY, Surrogate::new(303), 1);
        assert_eq!(t.key_for_surrogate(s1), Some(b"alpha".as_slice()));
        assert!(t.key_for_surrogate(Surrogate::new(303)).is_none());

        // Delete drops the reverse mapping.
        assert!(t.delete(b"alpha", 0));
        assert!(t.key_for_surrogate(s1).is_none());
        assert_eq!(t.surrogate_count(), 1);
    }

    /// A rewrite replaces the value and keeps the entry's bound surrogate, and
    /// a rewrite of an absent key writes nothing.
    #[test]
    fn rewrite_keeps_the_bound_surrogate() {
        let mut t = make_table();
        let s = Surrogate::new(7);
        t.put(b"k", b"v", NO_EXPIRY, s, 1);

        assert_eq!(t.rewrite(b"k", b"v2", NO_EXPIRY, 1), Some(b"v".to_vec()));
        assert_eq!(t.get(b"k", 0), Some(b"v2".as_slice()));
        assert_eq!(t.key_for_surrogate(s), Some(b"k".as_slice()));

        assert!(t.rewrite(b"absent", b"v", NO_EXPIRY, 1).is_none());
        assert!(t.get(b"absent", 0).is_none());
        assert_eq!(t.surrogate_count(), 1);
    }

    #[test]
    fn mem_usage_grows_with_entries() {
        let mut t = make_table();
        let base = t.mem_usage();

        for i in 0..100u32 {
            t.put(
                &i.to_be_bytes(),
                &[0u8; 32],
                NO_EXPIRY,
                Surrogate::new(i + 1),
                1,
            );
        }

        assert!(t.mem_usage() > base);
    }
}
