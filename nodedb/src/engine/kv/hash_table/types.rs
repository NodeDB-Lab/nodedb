// SPDX-License-Identifier: BUSL-1.1

//! `KvHashTable` struct definition, constructor, and introspection methods.

use std::collections::HashMap;

use nodedb_types::Surrogate;

use super::super::entry::KvEntry;
use super::super::hash_helpers::{extract_value_from, read_value_from};
use super::super::history::KvHistory;
use super::super::slab::SlabAllocator;

/// Metadata about a KV entry, returned by [`KvHashTable::get_entry_meta`].
///
/// Used by [`super::super::engine::KvEngine`] to retrieve the original `expire_at_ms`
/// when cancelling old expiry entries in the timing wheel.
#[derive(Debug, Clone, Copy)]
pub struct EntryMeta {
    /// Whether this key has a TTL set (`expire_at_ms != NO_EXPIRY`).
    pub has_ttl: bool,
    /// Absolute expiry timestamp in milliseconds, or [`NO_EXPIRY`] if persistent.
    pub expire_at_ms: u64,
}

/// One exported KV row, returned by [`KvHashTable::export_entries_with_surrogates`].
///
/// Carries everything needed to reconstruct the row through `KvEngine::put` —
/// including the `surrogate`, without which the restored row would lose its
/// stable cross-engine identity and no longer be reachable from a vector / FTS /
/// spatial bitmap intersection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvExportEntry {
    /// Primary key bytes.
    pub key: Vec<u8>,
    /// Value bytes, already resolved out of the overflow pool.
    pub value: Vec<u8>,
    /// Absolute expiry instant in ms since epoch, or `NO_EXPIRY` (`0`).
    pub expire_at_ms: u64,
    /// Stable global row identity. Never `Surrogate::ZERO`: every entry is
    /// bound.
    pub surrogate: Surrogate,
    /// System-time ordinal of the write that set the row's value.
    pub sys_from: i64,
}

/// Robin Hood hash table with incremental rehash.
///
/// Uses two internal tables during rehash: `primary` (new, larger) and
/// `rehash_source` (old, being migrated). All lookups check both tables.
/// PUTs go to the primary. Each PUT migrates `rehash_batch_size` entries
/// from old to new.
pub struct KvHashTable {
    /// Primary slot array.
    pub(super) slots: Vec<Option<KvEntry>>,
    /// Number of occupied slots in the primary table.
    pub(super) len: usize,
    /// Capacity (number of slots) — always a power of two.
    pub(super) capacity: usize,
    /// Load factor threshold that triggers rehash (0.0–1.0).
    pub(super) load_factor_threshold: f32,
    /// Entries migrated per PUT during incremental rehash.
    pub(super) rehash_batch_size: usize,

    /// Old table being migrated (during incremental rehash).
    pub(super) rehash_source: Option<Vec<Option<KvEntry>>>,
    /// Next index to scan in the old table during incremental rehash.
    pub(super) rehash_cursor: usize,

    /// Slab allocator for overflow values (fixed-size tiers, O(1) alloc/free).
    pub(super) overflow: SlabAllocator,

    /// Inline value threshold in bytes.
    pub(super) inline_threshold: usize,

    /// Reverse mapping `surrogate.0 → primary key bytes`. Populated on
    /// inserts that carry a non-zero surrogate. Read paths consult this
    /// to translate a surrogate back to its user-facing primary key
    /// (e.g. when emitting bitmap-join results).
    pub(super) surrogate_to_key: HashMap<u32, Vec<u8>>,

    /// Ended versions of every key, for reads at a past system-time ordinal.
    pub(super) history: KvHistory,

    /// Sum of [`KvEntry::mem_size`] over every live entry, primary and
    /// rehash source. Maintained by every insert, overwrite, and removal.
    pub(super) entry_bytes: usize,
}

impl KvHashTable {
    /// Create a new hash table with the given initial capacity.
    ///
    /// `capacity` is rounded up to the next power of two.
    /// `inline_threshold` determines whether values are stored inline or in the overflow pool.
    pub fn new(
        capacity: usize,
        load_factor_threshold: f32,
        rehash_batch_size: usize,
        inline_threshold: usize,
    ) -> Self {
        let capacity = capacity.next_power_of_two().max(4);
        Self {
            slots: vec![None; capacity],
            len: 0,
            capacity,
            load_factor_threshold,
            rehash_batch_size,
            rehash_source: None,
            rehash_cursor: 0,
            overflow: SlabAllocator::new(),
            inline_threshold,
            surrogate_to_key: HashMap::new(),
            history: KvHistory::default(),
            entry_bytes: 0,
        }
    }

    /// Ended versions of every key in this table.
    pub fn history(&self) -> &KvHistory {
        &self.history
    }

    /// Look up the primary key bytes for a stable surrogate identity.
    pub fn key_for_surrogate(&self, surrogate: Surrogate) -> Option<&[u8]> {
        self.surrogate_to_key
            .get(&surrogate.0)
            .map(|v| v.as_slice())
    }

    /// Number of bound surrogates in the reverse map. Test/diagnostic only.
    #[cfg(test)]
    pub fn surrogate_count(&self) -> usize {
        self.surrogate_to_key.len()
    }

    /// Number of entries in the table (including entries still in rehash source).
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether the table is empty.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Export all entries INCLUDING each row's stable cross-engine surrogate.
    ///
    /// The surrogate is read straight off the entry rather than reversed out of
    /// `surrogate_to_key`: the entry is the authority, and the reverse map is
    /// a derived index of it.
    pub fn export_entries_with_surrogates(&self) -> Vec<KvExportEntry> {
        self.live_entries()
            .map(|entry| KvExportEntry {
                key: entry.key.clone(),
                value: extract_value_from(&entry.value, &self.overflow),
                expire_at_ms: entry.expire_at_ms,
                surrogate: entry.surrogate,
                sys_from: entry.sys_from,
            })
            .collect()
    }

    /// Every live entry, across BOTH the primary slots and the rehash source.
    ///
    /// While an incremental rehash is in progress, entries that have not yet
    /// been migrated live only in `rehash_source`; an export that walked
    /// `slots` alone would silently omit them. Migration `take()`s the entry
    /// out of the source before inserting it into the primary, so an entry is
    /// never present in both and this yields each live entry exactly once.
    pub(crate) fn live_entries(&self) -> impl Iterator<Item = &KvEntry> {
        self.slots
            .iter()
            .flatten()
            .chain(self.rehash_source.iter().flatten().flatten())
    }

    /// Current load factor of the primary table.
    pub fn load_factor(&self) -> f32 {
        self.len as f32 / self.capacity as f32
    }

    /// Whether an incremental rehash is in progress.
    pub fn is_rehashing(&self) -> bool {
        self.rehash_source.is_some()
    }

    /// Capacity (number of slots) of the primary table.
    pub(crate) fn capacity(&self) -> usize {
        self.capacity
    }

    /// Number of slots in the rehash source (0 if not rehashing).
    pub(crate) fn rehash_source_len(&self) -> usize {
        self.rehash_source.as_ref().map(|s| s.len()).unwrap_or(0)
    }

    /// Access a slot in the primary table by index.
    pub(crate) fn primary_slot(&self, idx: usize) -> Option<&KvEntry> {
        self.slots[idx].as_ref()
    }

    /// Access a slot in the rehash source by index.
    pub(crate) fn rehash_slot(&self, idx: usize) -> Option<&KvEntry> {
        self.rehash_source.as_ref().and_then(|s| s[idx].as_ref())
    }

    /// Read value bytes from an entry, resolving overflow from the pool.
    pub(crate) fn read_value<'a>(&'a self, entry: &'a KvEntry) -> &'a [u8] {
        read_value_from(entry, &self.overflow)
    }

    /// Approximate memory usage in bytes: slot arrays, overflow pool, live
    /// entries, and ended versions. O(1).
    pub fn mem_usage(&self) -> usize {
        let slot_size = std::mem::size_of::<Option<KvEntry>>();
        let primary = self.capacity * slot_size;
        let rehash = self
            .rehash_source
            .as_ref()
            .map(|s| s.len() * slot_size)
            .unwrap_or(0);
        let overflow = self.overflow.capacity();
        primary + rehash + overflow + self.entry_bytes + self.history.bytes()
    }
}
