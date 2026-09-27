// SPDX-License-Identifier: Apache-2.0

//! Per-engine memory budget tracking.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Decrement `counter` by `size`, clamping at zero.
///
/// Plain `fetch_sub` wraps on underflow — a counter released past zero by
/// two `ReservationToken`s crediting the same engine budget, one dropping
/// while the other is still alive, would otherwise jump to ~`usize::MAX`,
/// which every utilization reader then interprets as 100 % → permanent
/// Emergency pressure. All release sites must go through this so an
/// over-release is at worst a saturated zero, never a wrapped maximum.
///
/// Returns the shortfall: `size.saturating_sub(current)` at the CAS that
/// succeeds. Zero means the release was clean. A nonzero shortfall is the
/// number of bytes the caller released that this counter never held — the
/// caller records it against the matching `Layer` on `OverRelease`.
pub(crate) fn atomic_saturating_sub(counter: &AtomicUsize, size: usize) -> usize {
    if size == 0 {
        return 0;
    }
    let mut current = counter.load(Ordering::Acquire);
    loop {
        let new_val = current.saturating_sub(size);
        match counter.compare_exchange_weak(current, new_val, Ordering::Release, Ordering::Relaxed)
        {
            Ok(_) => return size.saturating_sub(current),
            Err(actual) => current = actual,
        }
    }
}

/// A memory budget for a single engine.
///
/// Tracks current allocation against a configurable limit using atomic
/// counters (safe to read from any thread — metrics exporter, governor, etc.).
///
/// The `allocated` counter is stored behind an `Arc` so that
/// [`ReservationToken`](crate::reservation_token::ReservationToken) can hold a
/// reference to it without requiring a back-reference to the governor.
#[derive(Debug)]
pub struct Budget {
    /// Hard limit in bytes. Allocations beyond this are rejected.
    limit: AtomicUsize,

    /// Current allocated bytes. Arc-wrapped so tokens can release on drop.
    allocated: Arc<AtomicUsize>,

    /// Peak allocated bytes (high-water mark).
    peak: AtomicUsize,

    /// Number of times an allocation was rejected due to budget exhaustion.
    rejection_count: AtomicUsize,
}

impl Budget {
    /// Create a new budget with the given limit.
    pub fn new(limit: usize) -> Self {
        Self {
            limit: AtomicUsize::new(limit),
            allocated: Arc::new(AtomicUsize::new(0)),
            peak: AtomicUsize::new(0),
            rejection_count: AtomicUsize::new(0),
        }
    }

    /// Raise `peak` to `new_allocated` if it is a new high-water mark.
    fn raise_peak(&self, new_allocated: usize) {
        let mut peak = self.peak.load(Ordering::Relaxed);
        while new_allocated > peak {
            match self.peak.compare_exchange_weak(
                peak,
                new_allocated,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => peak = actual,
            }
        }
    }

    /// Try to reserve `size` bytes from this budget.
    ///
    /// Returns `true` if the reservation succeeded, `false` if it would
    /// exceed the limit.
    pub fn try_reserve(&self, size: usize) -> bool {
        let limit = self.limit.load(Ordering::Relaxed);

        // CAS loop to atomically check and increment.
        loop {
            let current = self.allocated.load(Ordering::Relaxed);
            if current + size > limit {
                self.rejection_count.fetch_add(1, Ordering::Relaxed);
                return false;
            }

            match self.allocated.compare_exchange_weak(
                current,
                current + size,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    self.raise_peak(current + size);
                    return true;
                }
                Err(_) => continue, // Retry CAS.
            }
        }
    }

    /// Try to reserve `size` bytes and return a shared `Arc` to the allocated
    /// counter so the caller can decrement it on drop.
    ///
    /// Returns `Some(arc)` on success, `None` on budget exhaustion.
    pub fn try_reserve_arc(&self, size: usize) -> Option<Arc<AtomicUsize>> {
        if self.try_reserve(size) {
            Some(Arc::clone(&self.allocated))
        } else {
            None
        }
    }

    /// Credit `size` bytes unconditionally, ignoring the limit.
    ///
    /// The caller already holds this memory — a denial here would only
    /// hide it from the budget, not free it. Returns the shared `Arc` so
    /// the caller can release it on drop, same as `try_reserve_arc`.
    pub(crate) fn credit_arc(&self, size: usize) -> Arc<AtomicUsize> {
        let new_allocated = self.allocated.fetch_add(size, Ordering::AcqRel) + size;
        self.raise_peak(new_allocated);
        Arc::clone(&self.allocated)
    }

    /// Current allocated bytes.
    pub fn allocated(&self) -> usize {
        self.allocated.load(Ordering::Relaxed)
    }

    /// Hard limit in bytes.
    pub fn limit(&self) -> usize {
        self.limit.load(Ordering::Relaxed)
    }

    /// Remaining bytes available.
    pub fn available(&self) -> usize {
        let limit = self.limit();
        let allocated = self.allocated();
        limit.saturating_sub(allocated)
    }

    /// Utilization as a percentage (0-100).
    ///
    /// Computed in `u128` so a corrupted (e.g. underflow-wrapped near
    /// `usize::MAX`) `allocated` clamps to 100 % rather than panicking on
    /// `allocated * 100` overflow — a panic here is taken inside the Data
    /// Plane core loop (`apply_spsc_pressure` → `snapshot`) and escalates
    /// to a DEGRADED core.
    pub fn utilization_percent(&self) -> u8 {
        let limit = self.limit();
        let allocated = self.allocated();
        if limit == 0 {
            // A zero-limit engine holds nothing and can accept nothing, so it
            // exerts no pressure. Reporting it full would drive
            // `worst_engine_pressure` to Emergency and throttle every core.
            // A non-zero count against a zero limit is an over-release, which
            // does deserve the loudest level.
            return if allocated == 0 { 0 } else { 100 };
        }
        ((allocated as u128 * 100) / limit as u128).min(100) as u8
    }

    /// Peak allocation (high-water mark).
    pub fn peak(&self) -> usize {
        self.peak.load(Ordering::Relaxed)
    }

    /// Number of rejected allocation attempts.
    pub fn rejections(&self) -> usize {
        self.rejection_count.load(Ordering::Relaxed)
    }

    /// Update the limit dynamically (for rebalancing).
    ///
    /// The new limit must be >= current allocation. If it's less, the limit
    /// is set to the current allocation (no immediate eviction).
    pub fn set_limit(&self, new_limit: usize) {
        let allocated = self.allocated();
        let effective = new_limit.max(allocated);
        self.limit.store(effective, Ordering::Release);
    }

    /// Reset all counters (for testing).
    #[cfg(test)]
    pub fn reset(&self) {
        self.allocated.store(0, Ordering::Relaxed);
        self.peak.store(0, Ordering::Relaxed);
        self.rejection_count.store(0, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn utilization_and_available_never_panic_on_corrupted_allocated() {
        // `allocated` is the sum of `try_reserve`/`release` deltas. An
        // unbalanced call site — or a `ReservationToken` drop that
        // `fetch_sub`s past zero — leaves it wrapped near `usize::MAX`.
        // `utilization_percent` must not panic computing `allocated * 100`
        // (it does today in debug builds: `attempt to multiply with
        // overflow`, taken inside the Data Plane core loop via
        // `apply_spsc_pressure → snapshot`, which the panic watchdog then
        // escalates to a DEGRADED core). In release the same multiply
        // wraps and reports a garbage level. A budget reader must be
        // robust to a corrupted counter: report a clamped 100 % (so the
        // pressure detector at least errs toward Emergency, not toward
        // "idle") and never crash.
        let budget = Budget::new(1024);
        budget.allocated.store(usize::MAX, Ordering::Relaxed);

        assert_eq!(
            budget.utilization_percent(),
            100,
            "a wrapped/over-large allocated counter must clamp to 100%, not \
             panic on `allocated * 100` and not wrap to a small percentage"
        );
        assert_eq!(
            budget.available(),
            0,
            "no capacity is available when allocated has run past the limit"
        );
    }

    #[test]
    fn reserve_within_limit() {
        let budget = Budget::new(1024);
        assert!(budget.try_reserve(512));
        assert_eq!(budget.allocated(), 512);
        assert_eq!(budget.available(), 512);
        assert_eq!(budget.utilization_percent(), 50);
    }

    #[test]
    fn reserve_at_limit() {
        let budget = Budget::new(1024);
        assert!(budget.try_reserve(1024));
        assert!(!budget.try_reserve(1));
        assert_eq!(budget.rejections(), 1);
    }

    #[test]
    fn reserve_exceeds_limit() {
        let budget = Budget::new(100);
        assert!(!budget.try_reserve(101));
        assert_eq!(budget.allocated(), 0);
        assert_eq!(budget.rejections(), 1);
    }

    #[test]
    fn release_frees_capacity() {
        // `Budget::release` is gone — the live release path is the shared
        // `allocated` counter handed out by `try_reserve_arc`, decremented
        // through `atomic_saturating_sub` the way `ReservationToken::drop`
        // does it.
        let budget = Budget::new(1024);
        assert!(budget.try_reserve(512));
        let arc = budget.try_reserve_arc(512).expect("within budget");
        assert!(!budget.try_reserve(1));

        let shortfall = atomic_saturating_sub(&arc, 256);
        assert_eq!(
            shortfall, 0,
            "releasing less than held is never a shortfall"
        );
        assert!(budget.try_reserve(256));
    }

    #[test]
    fn peak_tracks_high_water_mark() {
        let budget = Budget::new(1024);
        budget.try_reserve(800);
        let arc = budget
            .try_reserve_arc(0)
            .expect("zero-size reserve always succeeds");
        atomic_saturating_sub(&arc, 500);
        budget.try_reserve(100);

        assert_eq!(budget.peak(), 800);
        assert_eq!(budget.allocated(), 400);
    }

    #[test]
    fn dynamic_limit_adjustment() {
        let budget = Budget::new(1024);
        budget.try_reserve(600);

        // Increase limit.
        budget.set_limit(2048);
        assert_eq!(budget.limit(), 2048);
        assert!(budget.try_reserve(1000));

        // Decrease limit — but not below current allocation.
        budget.set_limit(100);
        assert_eq!(budget.limit(), 1600); // max(100, 1600 allocated)
    }

    #[test]
    fn try_reserve_arc_returns_shared_counter() {
        let budget = Budget::new(1024);
        let arc = budget.try_reserve_arc(512).expect("within budget");
        assert_eq!(arc.load(Ordering::Relaxed), 512);
        // Release via the arc.
        arc.fetch_sub(512, Ordering::Relaxed);
        assert_eq!(budget.allocated(), 0);
    }

    // wasm32 has no thread spawn: `std::thread::spawn` fails with
    // `Unsupported`, and wasm targets abort on panic, so this test cannot run
    // there. It still runs in full on native.
    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn concurrent_reserves() {
        use std::sync::Arc;
        use std::thread;

        let budget = Arc::new(Budget::new(10_000));
        let mut handles = Vec::new();

        for _ in 0..10 {
            let b = Arc::clone(&budget);
            handles.push(thread::spawn(move || {
                let mut reserved = 0;
                for _ in 0..100 {
                    if b.try_reserve(10) {
                        reserved += 10;
                    }
                }
                reserved
            }));
        }

        let total_reserved: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();

        // All 1000 reservations of 10 bytes should succeed (10 * 100 * 10 = 10000).
        assert_eq!(total_reserved, 10_000);
        assert_eq!(budget.allocated(), 10_000);
    }
}
