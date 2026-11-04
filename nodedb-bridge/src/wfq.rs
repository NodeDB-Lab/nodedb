// SPDX-License-Identifier: BUSL-1.1

//! Weighted-fair queue for per-database SPSC bridge dispatch.
//!
//! Implements deficit round-robin (DRR) across per-database virtual sub-queues.
//! Each database receives a quantum proportional to its `PriorityClass`:
//!
//! - `Critical` → weight 4
//! - `Standard` → weight 2  (default)
//! - `Bulk`     → weight 1
//!
//! The `cache_weight` field on `QuotaRecord` governs the *doc cache* shard
//! size (Section E); dispatch quantum uses `PriorityClass` only, not
//! `cache_weight`.
//!
//! # Backpressure
//!
//! Per-virtual-queue thresholds are computed against each database's fair
//! share of total capacity:
//!
//! - ≥ 85% of fair share → throttled
//! - ≥ 95% of fair share → suspended
//!
//! A database at its threshold does not block other databases that have
//! remaining headroom.

use std::collections::{HashMap, HashSet, VecDeque};

use nodedb_types::PriorityClass;

/// Dispatch weight for a given priority class.
///
/// Critical gets 4×, Standard 2×, Bulk 1× the basic quantum.
pub fn priority_weight(cls: PriorityClass) -> u32 {
    match cls {
        PriorityClass::Critical => 4,
        PriorityClass::Standard => 2,
        PriorityClass::Bulk => 1,
    }
}

/// State for one per-database virtual sub-queue.
struct VirtualQueue<T> {
    items: VecDeque<T>,
    /// Accumulated deficit: carries forward unused quantum from the previous
    /// round so databases with lower arrival rates still get fair throughput.
    deficit: u32,
}

impl<T> VirtualQueue<T> {
    fn new() -> Self {
        Self {
            items: VecDeque::new(),
            deficit: 0,
        }
    }
}

/// Weighted-fair queue, parameterized over item type `T`.
///
/// Maintains one virtual sub-queue per active `database_id`. The dispatcher
/// calls `pop_next()` to retrieve the next item following deficit round-robin
/// ordering. Total capacity across all virtual queues is bounded; `try_enqueue`
/// returns `Err(item)` when the total is full.
pub struct WeightedFairQueue<T> {
    /// Per-database virtual queues. Created lazily on first enqueue.
    queues: HashMap<u64, VirtualQueue<T>>,

    /// Round-robin traversal order (stable insertion order for existing DBs).
    db_order: VecDeque<u64>,

    /// Total number of items across all virtual queues.
    total: usize,

    /// Hard cap on total items across all virtual queues.
    capacity: usize,

    /// Per-database priority class, consulted during `pop_next`.
    priorities: HashMap<u64, PriorityClass>,
    /// Databases whose priority was refreshed since the last reap.
    ///
    /// `priorities` deliberately counts a registered-but-idle database in
    /// `fair_share_slots`, so it cannot be reaped the moment its queue drains.
    /// Without an expiry it never shrinks: `set_priority` runs on every
    /// dispatch, so every database that ever dispatched inflates the
    /// fair-share denominator for the process lifetime, suspending live
    /// databases at ever-shallower depth.
    touched_since_reap: HashSet<u64>,

    /// Number of `pop_next` calls since the last queue was reaped; used to
    /// garbage-collect drained virtual queues lazily.
    pops_since_reap: usize,

    /// Reap empty virtual queues after this many pop attempts without activity.
    reap_after_pops: usize,
}

impl<T> WeightedFairQueue<T> {
    /// Create a new weighted-fair queue with the given total capacity and reap
    /// threshold. `reap_after_pops` controls how many empty queues persist
    /// after draining before being garbage-collected.
    pub fn new(capacity: usize, reap_after_pops: usize) -> Self {
        Self {
            queues: HashMap::new(),
            db_order: VecDeque::new(),
            total: 0,
            capacity,
            priorities: HashMap::new(),
            touched_since_reap: HashSet::new(),
            pops_since_reap: 0,
            reap_after_pops,
        }
    }

    /// Attempt to enqueue `item` for `database_id`. Returns `Err(item)` if the
    /// total queue has reached capacity.
    pub fn try_enqueue(&mut self, database_id: u64, item: T) -> Result<(), T> {
        if self.total >= self.capacity {
            return Err(item);
        }
        if let std::collections::hash_map::Entry::Vacant(e) = self.queues.entry(database_id) {
            e.insert(VirtualQueue::new());
            self.db_order.push_back(database_id);
        }
        // Safe: we just ensured the key exists.
        let vq = self.queues.get_mut(&database_id).expect("just inserted");
        vq.items.push_back(item);
        self.total += 1;
        Ok(())
    }

    /// Set (or update) the priority class for a database. Applied on the next
    /// `pop_next` call after this update.
    pub fn set_priority(&mut self, database_id: u64, cls: PriorityClass) {
        self.priorities.insert(database_id, cls);
        self.touched_since_reap.insert(database_id);
    }

    /// Pop the next item using deficit round-robin across all virtual queues.
    ///
    /// Returns `None` if all virtual queues are empty.
    ///
    /// Each database is served for up to `priority_weight(class)` consecutive
    /// items before the scheduler rotates to the next database. Deficit credits
    /// are added once per turn (when a DB's deficit reaches zero and it re-enters
    /// the front of the rotation) and carried across calls so databases with
    /// lower arrival rates still accumulate credits fairly.
    pub fn pop_next(&mut self) -> Option<T> {
        if self.total == 0 {
            return None;
        }

        // Walk the round-robin ring. We may need to skip empty queues, so we
        // bound the scan to at most `n` DB rotations to avoid an infinite loop
        // when all but one queue is empty.
        let n = self.db_order.len();
        for _ in 0..n {
            let db_id = match self.db_order.front().copied() {
                Some(id) => id,
                None => break,
            };

            let vq = match self.queues.get_mut(&db_id) {
                Some(vq) => vq,
                None => {
                    self.db_order.pop_front();
                    continue;
                }
            };

            // If this DB has no deficit remaining from its previous turn, grant
            // a new quantum now (beginning of a new turn for this DB).
            if vq.deficit == 0 {
                let cls = self.priorities.get(&db_id).copied().unwrap_or_default();
                vq.deficit = priority_weight(cls);
            }

            if let Some(item) = vq.items.pop_front() {
                vq.deficit -= 1;
                self.total -= 1;

                // If this DB's deficit is now exhausted, rotate it to the back
                // so the next DB gets its turn. Otherwise leave it at the front
                // so we keep draining it next call.
                if vq.deficit == 0 {
                    self.db_order.pop_front();
                    self.db_order.push_back(db_id);
                }

                self.pops_since_reap += 1;
                if self.pops_since_reap >= self.reap_after_pops {
                    self.reap_empty_queues();
                    self.pops_since_reap = 0;
                }
                return Some(item);
            } else {
                // Queue drained; reset deficit so credits don't accumulate
                // unboundedly for an inactive DB, then rotate to next.
                vq.deficit = 0;
                self.db_order.pop_front();
                self.db_order.push_back(db_id);
            }
        }
        None
    }

    /// Remove and return every queued item, leaving the queue empty.
    ///
    /// Ordering is DRR order, the same order `pop_next` would have produced,
    /// so a caller failing the drained items reports them in the order they
    /// would have been dispatched. Used when the destination core is gone and
    /// the staged items can never be flushed.
    pub fn drain(&mut self) -> Vec<T> {
        let mut out = Vec::with_capacity(self.total);
        while let Some(item) = self.pop_next() {
            out.push(item);
        }
        out
    }

    /// Number of items queued for a specific database.
    pub fn depth_for(&self, database_id: u64) -> usize {
        self.queues
            .get(&database_id)
            .map(|vq| vq.items.len())
            .unwrap_or(0)
    }

    /// Yield `(database_id, depth)` for every virtual queue holding items.
    ///
    /// Empty, not-yet-reaped queues are skipped. Order is unspecified.
    /// Walks the queue map once and allocates nothing.
    pub fn nonempty_depths(&self) -> impl Iterator<Item = (u64, usize)> + '_ {
        self.queues
            .iter()
            .filter(|(_, vq)| !vq.items.is_empty())
            .map(|(&database_id, vq)| (database_id, vq.items.len()))
    }

    /// Total items across all virtual queues.
    pub fn total_depth(&self) -> usize {
        self.total
    }

    /// Returns `true` if the given database's virtual queue has reached ≥ 85%
    /// of its fair share of total capacity.
    ///
    /// Fair share = `capacity / active_databases` (floor division, min 1).
    /// Databases with higher priority class receive proportionally more fair
    /// share in the weight sense but the *slot* fair share is still equal
    /// (per-DB slot pressure uses equal division to avoid one class starving
    /// another's absolute headroom).
    pub fn is_throttled_for(&self, database_id: u64) -> bool {
        let depth = self.depth_for(database_id);
        let fair_share = self.fair_share_slots();
        depth * 100 >= fair_share * 85
    }

    /// Returns `true` if the given database's virtual queue has reached ≥ 95%
    /// of its fair share of total capacity.
    pub fn is_suspended_for(&self, database_id: u64) -> bool {
        let depth = self.depth_for(database_id);
        let fair_share = self.fair_share_slots();
        depth * 100 >= fair_share * 95
    }

    /// Number of active virtual queues (including empty, not-yet-reaped ones).
    pub fn active_database_count(&self) -> usize {
        self.queues.len()
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    /// Slots allocated per database for fair-share computations.
    ///
    /// Active count is the number of databases that have been explicitly
    /// registered (via `set_priority`) or have an active virtual queue.
    /// This prevents the fair-share from inflating when only one DB has
    /// enqueued items but multiple DBs are known to the scheduler.
    #[cfg(test)]
    pub(crate) fn fair_share_slots_for_test(&self) -> usize {
        self.fair_share_slots()
    }

    fn fair_share_slots(&self) -> usize {
        let active = self.priorities.len().max(self.queues.len()).max(1);
        (self.capacity / active).max(1)
    }

    /// Remove virtual queues that have been empty for a full reap cycle, and
    /// priority registrations for databases idle across one.
    ///
    /// A database that dispatched during the cycle keeps its registration, so
    /// the fair-share denominator still counts a database between requests.
    /// One that dispatched nothing and holds no queued item is forgotten.
    fn reap_empty_queues(&mut self) {
        let empty_ids: Vec<u64> = self
            .queues
            .iter()
            .filter(|(_, vq)| vq.items.is_empty())
            .map(|(&id, _)| id)
            .collect();
        for id in empty_ids {
            self.queues.remove(&id);
            self.db_order.retain(|&x| x != id);
        }
        let touched = std::mem::take(&mut self.touched_since_reap);
        self.priorities
            .retain(|id, _| touched.contains(id) || self.queues.contains_key(id));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nonempty_depths_lists_only_queues_with_items() {
        let mut wfq: WeightedFairQueue<u32> = WeightedFairQueue::new(64, 100);
        wfq.try_enqueue(1, 10).unwrap();
        wfq.try_enqueue(1, 11).unwrap();
        wfq.try_enqueue(2, 20).unwrap();

        let mut depths: Vec<_> = wfq.nonempty_depths().collect();
        depths.sort_unstable();
        assert_eq!(depths, vec![(1, 2), (2, 1)]);

        while wfq.pop_next().is_some() {}
        assert_eq!(wfq.nonempty_depths().count(), 0);
    }

    #[test]
    fn single_db_behaves_like_fifo() {
        let mut wfq: WeightedFairQueue<u32> = WeightedFairQueue::new(64, 100);
        for i in 0..8u32 {
            wfq.try_enqueue(1, i).unwrap();
        }
        for i in 0..8u32 {
            assert_eq!(wfq.pop_next(), Some(i));
        }
        assert_eq!(wfq.pop_next(), None);
    }

    #[test]
    fn two_dbs_equal_priority_round_robin() {
        let mut wfq: WeightedFairQueue<(u64, u32)> = WeightedFairQueue::new(64, 100);
        wfq.set_priority(1, PriorityClass::Standard);
        wfq.set_priority(2, PriorityClass::Standard);

        // Enqueue 4 items each.
        for i in 0..4u32 {
            wfq.try_enqueue(1, (1, i)).unwrap();
            wfq.try_enqueue(2, (2, i)).unwrap();
        }

        let mut db1_count = 0u32;
        let mut db2_count = 0u32;
        while let Some((db, _)) = wfq.pop_next() {
            match db {
                1 => db1_count += 1,
                2 => db2_count += 1,
                _ => panic!("unexpected db"),
            }
        }
        // Equal priority → equal share.
        assert_eq!(db1_count, 4);
        assert_eq!(db2_count, 4);
    }

    #[test]
    fn critical_drains_roughly_4x_faster_than_bulk() {
        let mut wfq: WeightedFairQueue<(u64, u32)> = WeightedFairQueue::new(256, 1000);
        wfq.set_priority(1, PriorityClass::Critical);
        wfq.set_priority(2, PriorityClass::Bulk);

        // Enqueue 80 items each.
        for i in 0..80u32 {
            wfq.try_enqueue(1, (1, i)).unwrap();
            wfq.try_enqueue(2, (2, i)).unwrap();
        }

        // Pop the first 20 items and count by DB.
        let mut critical_count = 0u32;
        let mut bulk_count = 0u32;
        for _ in 0..20 {
            match wfq.pop_next() {
                Some((1, _)) => critical_count += 1,
                Some((2, _)) => bulk_count += 1,
                _ => {}
            }
        }
        // Critical weight=4, Bulk weight=1 → ratio ≥ 3:1 in first 20 pops.
        assert!(
            critical_count >= 3 * bulk_count,
            "critical={critical_count} bulk={bulk_count}: expected ≥ 3:1 ratio"
        );
    }

    #[test]
    fn saturated_db_a_does_not_block_db_b() {
        let capacity = 8;
        let mut wfq: WeightedFairQueue<u32> = WeightedFairQueue::new(capacity, 100);
        wfq.set_priority(1, PriorityClass::Standard);
        wfq.set_priority(2, PriorityClass::Standard);

        // Fill up fair share for DB 1 (4 out of 8 slots).
        for i in 0..4u32 {
            wfq.try_enqueue(1, i).unwrap();
        }

        // DB 2 should still be enqueueable.
        for i in 0..4u32 {
            assert!(
                wfq.try_enqueue(2, i).is_ok(),
                "DB 2 enqueue {i} should succeed while DB 1 occupies its fair share"
            );
        }
        assert_eq!(wfq.total_depth(), 8);
    }

    #[test]
    fn bound_total_never_exceeded() {
        let capacity = 4;
        let mut wfq: WeightedFairQueue<u32> = WeightedFairQueue::new(capacity, 100);

        // Fill to capacity across two databases.
        for i in 0..2u32 {
            wfq.try_enqueue(1, i).unwrap();
            wfq.try_enqueue(2, i).unwrap();
        }
        assert_eq!(wfq.total_depth(), capacity);

        // Next enqueue must fail regardless of which DB.
        assert!(wfq.try_enqueue(1, 99).is_err());
        assert!(wfq.try_enqueue(2, 99).is_err());
        assert!(wfq.try_enqueue(3, 99).is_err());
    }

    #[test]
    fn backpressure_thresholds_per_virtual_queue() {
        let mut wfq: WeightedFairQueue<u32> = WeightedFairQueue::new(8, 100);
        wfq.set_priority(1, PriorityClass::Standard);
        wfq.set_priority(2, PriorityClass::Standard);

        // Fair share = 8 / 2 = 4 per DB.
        // Push 3 items into DB 1 → 3/4 = 75% → not throttled.
        for _ in 0..3 {
            wfq.try_enqueue(1, 0).unwrap();
        }
        assert!(!wfq.is_throttled_for(1));
        assert!(!wfq.is_suspended_for(1));

        // Push 1 more → 4/4 = 100% → throttled AND suspended.
        wfq.try_enqueue(1, 0).unwrap();
        assert!(wfq.is_throttled_for(1));
        assert!(wfq.is_suspended_for(1));

        // DB 2 is untouched → not throttled.
        assert!(!wfq.is_throttled_for(2));
    }

    /// A database that stops dispatching must stop inflating the fair-share
    /// denominator. `set_priority` runs on every dispatch, so without an
    /// expiry the denominator only grows and live databases are suspended at
    /// ever-shallower depth.
    #[test]
    fn an_idle_database_stops_counting_toward_fair_share() {
        let mut wfq: WeightedFairQueue<u32> = WeightedFairQueue::new(100, 1);
        wfq.set_priority(1, PriorityClass::Standard);
        wfq.set_priority(2, PriorityClass::Standard);

        // Database 2 goes quiet. Database 1 keeps working, and each pop
        // crosses a reap cycle.
        for round in 0..4 {
            wfq.set_priority(1, PriorityClass::Standard);
            wfq.try_enqueue(1, round).expect("enqueue");
            assert_eq!(wfq.pop_next(), Some(round));
        }

        let busy = wfq.fair_share_slots_for_test();
        wfq.set_priority(2, PriorityClass::Standard);
        wfq.try_enqueue(1, 99).expect("enqueue");
        assert_eq!(wfq.pop_next(), Some(99));
        assert!(
            wfq.fair_share_slots_for_test() <= busy,
            "a database that resumed dispatching may share capacity again, \
             never widen it beyond the one-active-database share"
        );
        assert_eq!(
            busy, 100,
            "with only database 1 dispatching, it must get the whole capacity"
        );
    }
}
