//! GAP_FREE sequence mode: serialized reservation with rollback support.
//!
//! In GAP_FREE mode, only one transaction holds the next number at a time.
//! Others block until the holder commits or rolls back. This is an intentional
//! throughput trade-off (~10K-50K numbers/sec, Raft-commit-latency bound).
//!
//! # Lifecycle
//!
//! 1. `reserve()` — wait until no active reservation, mark locked, increment counter
//! 2. Transaction executes (other `reserve()` callers block on the condvar)
//! 3. `commit(handle)` or `rollback(handle)` — unlock, wake blocked waiters

use std::collections::HashMap;
use std::sync::{Arc, Condvar, Mutex};

use super::types::SequenceError;

/// Unique identifier for a gap-free reservation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ReservationId(u64);

/// Handle returned by `reserve()`. Must be committed or rolled back.
///
/// While this handle exists, the per-sequence lock is held — other
/// transactions calling `reserve()` on the same sequence will block.
#[derive(Debug, Clone)]
pub struct ReservationHandle {
    pub id: ReservationId,
    /// Registry key: `"{tenant_id}:{sequence_name}"`.
    pub sequence_key: String,
    /// The reserved counter value.
    pub value: i64,
}

/// Per-sequence serialization lock for GAP_FREE mode.
///
/// Uses a `Mutex<bool>` + `Condvar` pair so the lock is held across the
/// transaction boundary (reserve → commit/rollback) without holding a
/// `MutexGuard`. The bool is `true` while a reservation is active.
struct SequenceLock {
    /// `true` when a reservation is active; `false` when idle.
    locked: Mutex<bool>,
    /// Signaled when `locked` transitions from `true` to `false`.
    unlocked: Condvar,
}

/// Manager for all GAP_FREE sequence locks.
pub struct GapFreeManager {
    /// Per-sequence locks keyed by registry key.
    locks: Mutex<HashMap<String, Arc<SequenceLock>>>,
    /// Monotonic reservation ID counter.
    next_id: Mutex<u64>,
}

impl GapFreeManager {
    pub fn new() -> Self {
        Self {
            locks: Mutex::new(HashMap::new()),
            next_id: Mutex::new(1),
        }
    }

    /// Reserve the next gap-free number for a sequence.
    ///
    /// Blocks if another transaction holds a reservation on this sequence.
    /// Returns a handle that must be committed or rolled back — until then,
    /// other callers on the same sequence will block.
    ///
    /// `advance_fn` is called after acquiring the lock to atomically advance
    /// the underlying counter.
    pub fn reserve(
        &self,
        sequence_key: &str,
        advance_fn: impl FnOnce() -> Result<i64, SequenceError>,
    ) -> Result<ReservationHandle, SequenceError> {
        // Get or create the per-sequence lock.
        let lock = {
            let mut locks = self.locks.lock().unwrap_or_else(|p| p.into_inner());
            locks
                .entry(sequence_key.to_string())
                .or_insert_with(|| {
                    Arc::new(SequenceLock {
                        locked: Mutex::new(false),
                        unlocked: Condvar::new(),
                    })
                })
                .clone()
        };

        // Wait until no active reservation, then mark as locked.
        {
            let mut is_locked = lock.locked.lock().unwrap_or_else(|p| p.into_inner());
            while *is_locked {
                is_locked = lock
                    .unlocked
                    .wait(is_locked)
                    .unwrap_or_else(|p| p.into_inner());
            }
            *is_locked = true;
        }

        // Advance the counter while holding the logical lock.
        let value = match advance_fn() {
            Ok(v) => v,
            Err(e) => {
                // Unlock on advance failure.
                self.unlock_sequence(&lock);
                return Err(e);
            }
        };

        // Generate reservation ID.
        let id = {
            let mut next = self.next_id.lock().unwrap_or_else(|p| p.into_inner());
            let id = ReservationId(*next);
            *next += 1;
            id
        };

        Ok(ReservationHandle {
            id,
            sequence_key: sequence_key.to_string(),
            value,
        })
    }

    /// Commit a reservation: the number is now permanent.
    /// Releases the per-sequence lock so the next caller can proceed.
    pub fn commit(&self, handle: &ReservationHandle) {
        let locks = self.locks.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(lock) = locks.get(&handle.sequence_key) {
            self.unlock_sequence(lock);
        }
    }

    /// Rollback a reservation: decrement the counter, recycle the number.
    /// Releases the per-sequence lock so the next caller can proceed.
    ///
    /// `rollback_fn` is called to decrement the sequence counter by one increment.
    pub fn rollback(&self, handle: &ReservationHandle, rollback_fn: impl FnOnce()) {
        rollback_fn();

        let locks = self.locks.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(lock) = locks.get(&handle.sequence_key) {
            self.unlock_sequence(lock);
        }
    }

    /// Release the per-sequence lock and wake one blocked waiter.
    fn unlock_sequence(&self, lock: &SequenceLock) {
        let mut is_locked = lock.locked.lock().unwrap_or_else(|p| p.into_inner());
        *is_locked = false;
        lock.unlocked.notify_one();
    }
}

impl Default for GapFreeManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicI64, Ordering};

    #[test]
    fn reserve_commit() {
        let mgr = GapFreeManager::new();
        let counter = AtomicI64::new(0);

        let handle = mgr
            .reserve("1:test", || Ok(counter.fetch_add(1, Ordering::Relaxed) + 1))
            .unwrap();

        assert_eq!(handle.value, 1);
        mgr.commit(&handle);
        assert_eq!(counter.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn reserve_rollback() {
        let mgr = GapFreeManager::new();
        let counter = AtomicI64::new(0);

        let handle = mgr
            .reserve("1:test", || Ok(counter.fetch_add(1, Ordering::Relaxed) + 1))
            .unwrap();

        assert_eq!(handle.value, 1);
        assert_eq!(counter.load(Ordering::Relaxed), 1);

        mgr.rollback(&handle, || {
            counter.fetch_sub(1, Ordering::Relaxed);
        });
        assert_eq!(counter.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn sequential_reservations() {
        let mgr = GapFreeManager::new();
        let counter = AtomicI64::new(0);

        let h1 = mgr
            .reserve("1:test", || Ok(counter.fetch_add(1, Ordering::Relaxed) + 1))
            .unwrap();
        mgr.commit(&h1);

        let h2 = mgr
            .reserve("1:test", || Ok(counter.fetch_add(1, Ordering::Relaxed) + 1))
            .unwrap();
        assert_eq!(h2.value, 2);
        mgr.commit(&h2);
    }

    #[test]
    fn different_sequences_independent() {
        let mgr = GapFreeManager::new();
        let c1 = AtomicI64::new(0);
        let c2 = AtomicI64::new(0);

        let h1 = mgr
            .reserve("1:seq_a", || Ok(c1.fetch_add(1, Ordering::Relaxed) + 1))
            .unwrap();

        let h2 = mgr
            .reserve("1:seq_b", || Ok(c2.fetch_add(1, Ordering::Relaxed) + 1))
            .unwrap();

        assert_eq!(h1.value, 1);
        assert_eq!(h2.value, 1);
        mgr.commit(&h1);
        mgr.commit(&h2);
    }

    #[test]
    fn concurrent_serialization() {
        // Verify that reserve() on the same sequence blocks until commit/rollback.
        let mgr = Arc::new(GapFreeManager::new());
        let counter = Arc::new(AtomicI64::new(0));
        let order = Arc::new(AtomicI64::new(0));

        let mgr2 = Arc::clone(&mgr);
        let counter2 = Arc::clone(&counter);
        let order2 = Arc::clone(&order);

        // First reservation — held for a bit.
        let h1 = mgr
            .reserve("1:test", || Ok(counter.fetch_add(1, Ordering::SeqCst) + 1))
            .unwrap();
        assert_eq!(h1.value, 1);
        order.store(1, Ordering::SeqCst);

        // Second reservation in another thread — should block.
        let t = std::thread::spawn(move || {
            let h2 = mgr2
                .reserve("1:test", || Ok(counter2.fetch_add(1, Ordering::SeqCst) + 1))
                .unwrap();
            // When we get here, h1 must have been committed (order == 2).
            assert!(order2.load(Ordering::SeqCst) >= 2);
            assert_eq!(h2.value, 2);
            mgr2.commit(&h2);
        });

        // Small sleep to let thread start and block on reserve().
        std::thread::sleep(std::time::Duration::from_millis(50));
        order.store(2, Ordering::SeqCst);
        mgr.commit(&h1);

        t.join().unwrap();
        assert_eq!(counter.load(Ordering::SeqCst), 2);
    }
}
