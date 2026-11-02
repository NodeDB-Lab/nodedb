// SPDX-License-Identifier: BUSL-1.1

//! Reversing one write of a key exactly, as a transaction rollback does.
//!
//! A write over a live version at `sys_from` either replaces it in place
//! (same ordinal) or ends it into history as `[sys_from, ordinal)`. A write
//! of an absent key inserts a live entry and ends nothing. So the state
//! before the write is the prior live entry at its own start, with the
//! ended version that starts at that start removed from history. The
//! reversal ends no version and draws no ordinal.

use nodedb_types::Surrogate;

use super::super::entry::KvEntry;
use super::super::hash_helpers::{extract_value_from, free_value_from, hash_key, store_value_in};
use super::types::KvHashTable;

/// A live version a key held, at the ordinal that set it.
#[derive(Debug, Clone, Copy)]
pub struct LiveVersion<'a> {
    pub value: &'a [u8],
    /// Absolute expiry instant in ms, [`NO_EXPIRY`](super::super::entry::NO_EXPIRY)
    /// for none.
    pub expire_at_ms: u64,
    pub surrogate: Surrogate,
    pub sys_from: i64,
}

impl KvHashTable {
    /// Put `key` back to the state it held before one write. The live entry
    /// leaves without ending a version. With `prior`, the ended version
    /// starting at `prior.sys_from` leaves history, and `prior` is the live
    /// entry again. Returns the value the removed live entry held.
    pub fn restore_version(
        &mut self,
        key: &[u8],
        prior: Option<LiveVersion<'_>>,
    ) -> Option<Vec<u8>> {
        let removed = self.discard_live(key);
        if let Some(prior) = prior {
            self.history.remove_version(key, prior.sys_from);
            let value = store_value_in(&mut self.overflow, prior.value, self.inline_threshold);
            self.surrogate_to_key
                .insert(prior.surrogate.0, key.to_vec());
            let entry = KvEntry {
                hash: hash_key(key),
                key: key.to_vec(),
                value,
                expire_at_ms: prior.expire_at_ms,
                surrogate: prior.surrogate,
                sys_from: prior.sys_from,
            };
            self.entry_bytes += entry.mem_size();
            Self::robin_hood_insert(&mut self.slots, entry);
            self.len += 1;
            self.maybe_start_rehash();
        }
        removed
    }

    /// Remove the live entry of `key` without ending a version. Returns the
    /// value it held.
    fn discard_live(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        let h = hash_key(key);
        let slots = if Self::probe_find_index_static(&self.slots, h, key).is_some() {
            &mut self.slots
        } else {
            self.rehash_source.as_mut()?
        };
        let idx = Self::probe_find_index_static(slots, h, key)?;
        let entry = slots.get_mut(idx).and_then(Option::take)?;
        Self::repair_after_delete_static(slots, idx);
        let value = extract_value_from(&entry.value, &self.overflow);
        free_value_from(&entry.value, &mut self.overflow);
        self.surrogate_to_key.remove(&entry.surrogate.0);
        self.len -= 1;
        self.entry_bytes = self.entry_bytes.saturating_sub(entry.mem_size());
        Some(value)
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::Surrogate;

    use super::super::super::entry::NO_EXPIRY;
    use super::*;

    fn table() -> KvHashTable {
        KvHashTable::new(16, 0.75, 4, 64)
    }

    fn prior(value: &[u8], sys_from: i64) -> LiveVersion<'_> {
        LiveVersion {
            value,
            expire_at_ms: NO_EXPIRY,
            surrogate: Surrogate::new(1),
            sys_from,
        }
    }

    #[test]
    fn a_reversed_overwrite_leaves_no_phantom_version() {
        let mut t = table();
        t.put(b"k", b"v1", NO_EXPIRY, Surrogate::new(1), 10);
        let bytes = t.mem_usage();
        t.put(b"k", b"v2", NO_EXPIRY, Surrogate::new(1), 20);
        let removed = t.restore_version(b"k", Some(prior(b"v1", 10)));
        assert_eq!(removed.as_deref(), Some(&b"v2"[..]));
        assert_eq!(t.get(b"k", 0), Some(&b"v1"[..]));
        assert_eq!(t.newest_ordinal_of(b"k"), Some(10));
        assert!(t.history().is_empty(), "the ended version leaves");
        assert_eq!(t.mem_usage(), bytes);
    }

    #[test]
    fn a_reversed_delete_puts_the_row_back_at_its_own_start() {
        let mut t = table();
        t.put(b"k", b"v0", NO_EXPIRY, Surrogate::new(1), 5);
        t.put(b"k", b"v1", NO_EXPIRY, Surrogate::new(1), 10);
        assert!(t.delete(b"k", 20));
        t.restore_version(b"k", Some(prior(b"v1", 10)));
        assert_eq!(t.newest_ordinal_of(b"k"), Some(10));
        let ended = t.history().versions_of(b"k");
        assert_eq!(ended.len(), 1, "the older history stays");
        assert_eq!((ended[0].sys_from, ended[0].sys_to), (5, 10));
        assert_eq!(t.key_for_surrogate(Surrogate::new(1)), Some(&b"k"[..]));
    }

    #[test]
    fn a_reversed_insert_removes_the_key_and_keeps_its_history() {
        let mut t = table();
        t.put(b"k", b"v0", NO_EXPIRY, Surrogate::new(1), 5);
        assert!(t.delete(b"k", 8));
        t.put(b"k", b"v1", NO_EXPIRY, Surrogate::new(1), 20);
        t.restore_version(b"k", None);
        assert_eq!(t.len(), 0);
        assert_eq!(t.newest_ordinal_of(b"k"), Some(8));
        assert_eq!(t.history().version_count(), 1);
    }

    #[test]
    fn two_reversed_writes_of_one_ordinal_restore_the_first_state() {
        let mut t = table();
        t.put(b"k", b"v0", NO_EXPIRY, Surrogate::new(1), 5);
        t.put(b"k", b"v1", NO_EXPIRY, Surrogate::new(1), 20);
        t.put(b"k", b"v2", NO_EXPIRY, Surrogate::new(1), 20);
        t.restore_version(b"k", Some(prior(b"v1", 20)));
        t.restore_version(b"k", Some(prior(b"v0", 5)));
        assert_eq!(t.get(b"k", 0), Some(&b"v0"[..]));
        assert_eq!(t.newest_ordinal_of(b"k"), Some(5));
        assert!(t.history().is_empty());
    }
}
