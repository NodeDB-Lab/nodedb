//! Hierarchical timing wheel for deterministic KV key expiry.
//!
//! O(1) insert and cancel. One wheel per TPC core — no cross-core coordination.
//!
//! Design:
//! - Two-level wheel: fine slots (1 second each, 256 slots = ~4 min range)
//!   and coarse slots (256 seconds each, 64 slots = ~4.5 hour range).
//! - Keys expiring beyond the coarse range are stored in a spillover list
//!   and promoted into the wheel as time advances.
//! - Per-tick reap budget: at most `reap_budget` expirations per `tick()` call,
//!   then yield back to the reactor. Prevents mass-expiry events from stalling
//!   the TPC core (the "reaper lockup" problem).
//! - Expired-but-not-yet-reaped keys are invisible to GET via lazy fallback
//!   in the hash table, so correctness is preserved even when reaping is
//!   spread across multiple ticks.

/// Number of fine-resolution slots (1 tick = 1 second).
const FINE_SLOTS: usize = 256;

/// Number of coarse-resolution slots (1 coarse slot = `FINE_SLOTS` ticks).
const COARSE_SLOTS: usize = 64;

/// Total fine ticks covered by the coarse wheel.
const COARSE_RANGE: u64 = (FINE_SLOTS * COARSE_SLOTS) as u64;

/// An expiry entry: the key + its absolute expiry timestamp.
#[derive(Debug, Clone)]
struct ExpiryEntry {
    key: Vec<u8>,
    expire_at_ms: u64,
}

/// A batch of keys to reap, returned by [`ExpiryWheel::tick`].
pub struct ReapBatch {
    /// Keys that should be deleted from the hash table.
    /// Each entry is `(key_bytes, expire_at_ms)`.
    pub expired: Vec<(Vec<u8>, u64)>,
    /// Number of entries still pending in this tick's slot (backlog).
    pub remaining: usize,
}

/// Hierarchical timing wheel for KV key expiry.
///
/// `!Send` — owned by a single TPC core.
pub struct ExpiryWheel {
    /// Fine-resolution slots. `fine[i]` holds keys expiring at
    /// `base_ms + i * tick_ms` (within ±1 tick).
    fine: Vec<Vec<ExpiryEntry>>,

    /// Coarse-resolution slots. `coarse[i]` holds keys expiring at
    /// `base_ms + (FINE_SLOTS + i * FINE_SLOTS) * tick_ms`.
    coarse: Vec<Vec<ExpiryEntry>>,

    /// Keys expiring beyond the coarse wheel range.
    spillover: Vec<ExpiryEntry>,

    /// Base timestamp in milliseconds (the "current time" of the wheel).
    base_ms: u64,

    /// Tick interval in milliseconds (default: 1000 = 1 second).
    tick_ms: u64,

    /// Current fine slot index (0..FINE_SLOTS-1).
    fine_cursor: usize,

    /// Current coarse slot index (0..COARSE_SLOTS-1).
    coarse_cursor: usize,

    /// Maximum expirations per tick call.
    reap_budget: usize,

    /// Total number of entries across all slots and spillover.
    total_entries: usize,

    /// Entries that were deferred from a previous tick (budget exceeded).
    deferred: Vec<ExpiryEntry>,
}

impl ExpiryWheel {
    /// Create a new expiry wheel.
    ///
    /// - `now_ms`: current wall-clock time in milliseconds.
    /// - `tick_ms`: tick interval (default 1000ms).
    /// - `reap_budget`: max expirations per tick() call (default 1024).
    pub fn new(now_ms: u64, tick_ms: u64, reap_budget: usize) -> Self {
        Self {
            fine: (0..FINE_SLOTS).map(|_| Vec::new()).collect(),
            coarse: (0..COARSE_SLOTS).map(|_| Vec::new()).collect(),
            spillover: Vec::new(),
            base_ms: now_ms,
            tick_ms,
            fine_cursor: 0,
            coarse_cursor: 0,
            reap_budget,
            total_entries: 0,
            deferred: Vec::new(),
        }
    }

    /// Total number of tracked expiry entries.
    pub fn len(&self) -> usize {
        self.total_entries
    }

    pub fn is_empty(&self) -> bool {
        self.total_entries == 0
    }

    /// Number of entries deferred from previous ticks (backlog gauge).
    pub fn backlog(&self) -> usize {
        self.deferred.len()
    }

    /// Schedule a key for expiry at `expire_at_ms`.
    ///
    /// O(1) — computes the target slot and appends.
    pub fn insert(&mut self, key: Vec<u8>, expire_at_ms: u64) {
        let entry = ExpiryEntry { key, expire_at_ms };
        self.place_entry(entry);
        self.total_entries += 1;
    }

    /// Cancel a pending expiry for a key. O(n) scan of the target slot.
    ///
    /// Called when a key's TTL is removed (PERSIST) or updated (EXPIRE with new value).
    /// The slot is determined by the old `expire_at_ms`.
    pub fn cancel(&mut self, key: &[u8], expire_at_ms: u64) -> bool {
        // Check deferred first.
        if let Some(pos) = self
            .deferred
            .iter()
            .position(|e| e.key == key && e.expire_at_ms == expire_at_ms)
        {
            self.deferred.swap_remove(pos);
            self.total_entries -= 1;
            return true;
        }

        let ticks_from_base = expire_at_ms.saturating_sub(self.base_ms) / self.tick_ms;

        if ticks_from_base < FINE_SLOTS as u64 {
            let slot = (self.fine_cursor as u64 + ticks_from_base) as usize % FINE_SLOTS;
            return remove_from_slot(
                &mut self.fine[slot],
                key,
                expire_at_ms,
                &mut self.total_entries,
            );
        }

        if ticks_from_base < COARSE_RANGE {
            let coarse_offset = (ticks_from_base - FINE_SLOTS as u64) / FINE_SLOTS as u64;
            let slot = (self.coarse_cursor as u64 + coarse_offset) as usize % COARSE_SLOTS;
            return remove_from_slot(
                &mut self.coarse[slot],
                key,
                expire_at_ms,
                &mut self.total_entries,
            );
        }

        // Must be in spillover.
        remove_from_slot(
            &mut self.spillover,
            key,
            expire_at_ms,
            &mut self.total_entries,
        )
    }

    /// Drop every wheel entry whose composite key starts with
    /// `prefix`. Returns the number of entries removed.
    ///
    /// Called during collection hard-delete so the wheel does not
    /// carry stale entries for purged collections — without this,
    /// a large purged collection's TTL backlog would burn reap
    /// budget for no-op ticks until every entry's scheduled time
    /// had passed. Safe during a live wheel: entries are removed
    /// in-place; cursors are not advanced.
    pub fn purge_prefix(&mut self, prefix: &[u8]) -> usize {
        if prefix.is_empty() {
            return 0;
        }
        let matches = |e: &ExpiryEntry| e.key.starts_with(prefix);
        let mut removed = 0;
        let mut drop_from = |slot: &mut Vec<ExpiryEntry>| {
            let before = slot.len();
            slot.retain(|e| !matches(e));
            removed += before - slot.len();
        };
        for slot in &mut self.fine {
            drop_from(slot);
        }
        for slot in &mut self.coarse {
            drop_from(slot);
        }
        drop_from(&mut self.spillover);
        drop_from(&mut self.deferred);
        self.total_entries = self.total_entries.saturating_sub(removed);
        removed
    }

    /// Advance the wheel and return up to `reap_budget` expired keys.
    ///
    /// Call this from the TPC core's event loop at `tick_ms` intervals.
    /// For large time jumps, all wheel levels are swept for expired entries
    /// in a single pass, bounded by the reap budget.
    pub fn tick(&mut self, now_ms: u64) -> ReapBatch {
        let mut expired = Vec::new();

        // Drain deferred entries from previous ticks first.
        while !self.deferred.is_empty() && expired.len() < self.reap_budget {
            let Some(entry) = self.deferred.pop() else {
                break;
            };
            expired.push((entry.key, entry.expire_at_ms));
            self.total_entries -= 1;
        }

        if expired.len() >= self.reap_budget {
            return ReapBatch {
                remaining: self.deferred.len(),
                expired,
            };
        }

        // Sweep all wheel levels for entries expired by now_ms.
        // Handles arbitrary time jumps correctly.
        for slot in &mut self.fine {
            let entries = std::mem::take(slot);
            for entry in entries {
                if entry.expire_at_ms <= now_ms {
                    if expired.len() < self.reap_budget {
                        expired.push((entry.key, entry.expire_at_ms));
                        self.total_entries -= 1;
                    } else {
                        self.deferred.push(entry);
                    }
                } else {
                    slot.push(entry);
                }
            }
        }

        // Sweep coarse slots.
        for slot in &mut self.coarse {
            let entries = std::mem::take(slot);
            for entry in entries {
                if entry.expire_at_ms <= now_ms {
                    if expired.len() < self.reap_budget {
                        expired.push((entry.key, entry.expire_at_ms));
                        self.total_entries -= 1;
                    } else {
                        self.deferred.push(entry);
                    }
                } else {
                    slot.push(entry);
                }
            }
        }

        // Sweep spillover.
        let spilled = std::mem::take(&mut self.spillover);
        for entry in spilled {
            if entry.expire_at_ms <= now_ms {
                if expired.len() < self.reap_budget {
                    expired.push((entry.key, entry.expire_at_ms));
                    self.total_entries -= 1;
                } else {
                    self.deferred.push(entry);
                }
            } else {
                self.spillover.push(entry);
            }
        }

        self.base_ms = now_ms;

        let remaining = self.deferred.len();
        ReapBatch { expired, remaining }
    }

    // -----------------------------------------------------------------------
    // Internal
    // -----------------------------------------------------------------------

    /// Place an entry into the appropriate slot based on its expiry time.
    fn place_entry(&mut self, entry: ExpiryEntry) {
        let ticks_from_base = entry.expire_at_ms.saturating_sub(self.base_ms) / self.tick_ms;

        if ticks_from_base < FINE_SLOTS as u64 {
            let slot = (self.fine_cursor as u64 + ticks_from_base) as usize % FINE_SLOTS;
            self.fine[slot].push(entry);
            return;
        }

        if ticks_from_base < COARSE_RANGE {
            let coarse_offset = (ticks_from_base - FINE_SLOTS as u64) / FINE_SLOTS as u64;
            let slot = (self.coarse_cursor as u64 + coarse_offset) as usize % COARSE_SLOTS;
            self.coarse[slot].push(entry);
            return;
        }

        // Beyond coarse range — spillover.
        self.spillover.push(entry);
    }
}

/// Remove a specific entry from a slot. Returns true if found and removed.
fn remove_from_slot(
    slot: &mut Vec<ExpiryEntry>,
    key: &[u8],
    expire_at_ms: u64,
    total_entries: &mut usize,
) -> bool {
    if let Some(pos) = slot
        .iter()
        .position(|e| e.key == key && e.expire_at_ms == expire_at_ms)
    {
        slot.swap_remove(pos);
        *total_entries -= 1;
        true
    } else {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_and_tick_expires_key() {
        let mut w = ExpiryWheel::new(0, 1000, 1024);
        w.insert(b"k1".to_vec(), 2000);
        assert_eq!(w.len(), 1);

        // Tick at t=1000: key not yet expired.
        let batch = w.tick(1000);
        assert!(batch.expired.is_empty());

        // Tick at t=2000: key should expire.
        let batch = w.tick(2000);
        assert_eq!(batch.expired.len(), 1);
        assert_eq!(batch.expired[0].0, b"k1");
        assert_eq!(w.len(), 0);
    }

    #[test]
    fn purge_prefix_drops_scoped_entries_across_slots() {
        let mut w = ExpiryWheel::new(0, 1000, 1024);
        let prefix = b"1:users\0";
        // Mix fine-slot, coarse-slot, spillover, and a sibling
        // collection/tenant that must NOT be dropped.
        w.insert([prefix, b"a".as_slice()].concat(), 2_000); // fine
        w.insert([prefix, b"b".as_slice()].concat(), 300_000); // coarse
        w.insert([prefix, b"c".as_slice()].concat(), 10_000_000); // spillover
        w.insert(b"1:orders\0x".to_vec(), 2_000); // different collection
        w.insert(b"2:users\0y".to_vec(), 2_000); // different tenant
        assert_eq!(w.len(), 5);

        let removed = w.purge_prefix(prefix);
        assert_eq!(removed, 3);
        assert_eq!(w.len(), 2);

        // Sibling entries still fire.
        let batch = w.tick(2_000);
        let remaining_keys: Vec<_> = batch.expired.iter().map(|(k, _)| k.clone()).collect();
        assert!(remaining_keys.iter().any(|k| k.starts_with(b"1:orders\0")));
        assert!(remaining_keys.iter().any(|k| k.starts_with(b"2:users\0")));
    }

    #[test]
    fn purge_prefix_empty_prefix_is_noop() {
        let mut w = ExpiryWheel::new(0, 1000, 1024);
        w.insert(b"1:x\0k".to_vec(), 2_000);
        assert_eq!(w.purge_prefix(&[]), 0);
        assert_eq!(w.len(), 1);
    }

    #[test]
    fn cancel_removes_entry() {
        let mut w = ExpiryWheel::new(0, 1000, 1024);
        w.insert(b"k1".to_vec(), 5000);
        assert_eq!(w.len(), 1);

        assert!(w.cancel(b"k1", 5000));
        assert_eq!(w.len(), 0);

        // Tick past expiry — nothing should come out.
        let batch = w.tick(6000);
        assert!(batch.expired.is_empty());
    }

    #[test]
    fn cancel_wrong_expire_fails() {
        let mut w = ExpiryWheel::new(0, 1000, 1024);
        w.insert(b"k1".to_vec(), 5000);

        assert!(!w.cancel(b"k1", 9999)); // Wrong expire_at_ms.
        assert_eq!(w.len(), 1);
    }

    #[test]
    fn reap_budget_limits_per_tick() {
        let mut w = ExpiryWheel::new(0, 1000, 3); // Budget = 3 per tick.

        // Insert 10 keys all expiring at t=1000.
        for i in 0..10u32 {
            w.insert(i.to_be_bytes().to_vec(), 1000);
        }
        assert_eq!(w.len(), 10);

        // First tick: only 3 should be reaped.
        let batch = w.tick(1000);
        assert_eq!(batch.expired.len(), 3);
        assert!(batch.remaining > 0);

        // Second tick: 3 more.
        let batch = w.tick(1000);
        assert_eq!(batch.expired.len(), 3);

        // Third tick: 3 more.
        let batch = w.tick(1000);
        assert_eq!(batch.expired.len(), 3);

        // Fourth tick: last 1.
        let batch = w.tick(1000);
        assert_eq!(batch.expired.len(), 1);
        assert_eq!(batch.remaining, 0);
        assert_eq!(w.len(), 0);
    }

    #[test]
    fn multiple_keys_same_slot() {
        let mut w = ExpiryWheel::new(0, 1000, 1024);
        w.insert(b"a".to_vec(), 3000);
        w.insert(b"b".to_vec(), 3000);
        w.insert(b"c".to_vec(), 3000);
        assert_eq!(w.len(), 3);

        let batch = w.tick(3000);
        assert_eq!(batch.expired.len(), 3);
        assert_eq!(w.len(), 0);
    }

    #[test]
    fn coarse_slot_cascade() {
        let mut w = ExpiryWheel::new(0, 1000, 1024);
        // Key expiring at 300 seconds — in coarse range.
        w.insert(b"far".to_vec(), 300_000);
        assert_eq!(w.len(), 1);

        // Advance time to just before expiry.
        let batch = w.tick(299_000);
        assert!(batch.expired.is_empty());

        // Advance past expiry.
        let batch = w.tick(300_000);
        assert_eq!(batch.expired.len(), 1);
        assert_eq!(batch.expired[0].0, b"far");
    }

    #[test]
    fn spillover_for_very_far_future() {
        let mut w = ExpiryWheel::new(0, 1000, 1024);
        // Key expiring at 100,000 seconds — beyond coarse range.
        w.insert(b"distant".to_vec(), 100_000_000);
        assert_eq!(w.len(), 1);

        // Tick close but not at expiry — should not expire.
        let batch = w.tick(99_999_000);
        assert!(batch.expired.is_empty());

        // Tick past expiry.
        let batch = w.tick(100_000_000);
        assert_eq!(batch.expired.len(), 1);
    }

    #[test]
    fn backlog_gauge_tracks_deferred() {
        let mut w = ExpiryWheel::new(0, 1000, 2);
        for i in 0..5u32 {
            w.insert(i.to_be_bytes().to_vec(), 1000);
        }

        let batch = w.tick(1000);
        assert_eq!(batch.expired.len(), 2);
        assert!(w.backlog() > 0);

        // Drain backlog.
        let batch = w.tick(1000);
        assert_eq!(batch.expired.len(), 2);
        let batch = w.tick(1000);
        assert_eq!(batch.expired.len(), 1);
        assert_eq!(w.backlog(), 0);
    }
}
