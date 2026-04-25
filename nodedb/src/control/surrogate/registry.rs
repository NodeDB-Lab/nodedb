//! `SurrogateRegistry` — thread-safe monotonic surrogate allocator.
//!
//! ## Counter semantics
//!
//! The internal `AtomicU64` counter stores the **next** surrogate to be
//! handed out. `alloc_one()` does `fetch_add(1, AcqRel)`; the returned
//! value is the previous counter (i.e. the surrogate the caller now owns).
//! After every successful allocation, `current_hwm()` returns the highest
//! surrogate ever issued — equivalently, `counter - 1`.
//!
//! ## Restart semantics
//!
//! `from_persisted_hwm(hwm)` initializes `counter = hwm + 1`. On a fresh
//! database, persisted hwm is `0` and the first allocation returns `1`
//! (`Surrogate::ZERO` is reserved). After a restart with persisted hwm
//! `5000`, the first allocation returns `5001`.
//!
//! ## Width / overflow
//!
//! Surrogates are `u32`. The internal counter is `u64` so we can detect
//! the boundary cleanly: any allocation that would push `counter` past
//! `u32::MAX + 1` returns `SurrogateAllocError::Exhausted`. Concurrent
//! callers who race past the boundary all observe the typed error rather
//! than silently wrapping into `0`.

use std::ops::RangeInclusive;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use nodedb_types::Surrogate;

use super::persist::SurrogateHwmPersist;

/// Periodic flush trigger: every N allocations, regardless of elapsed time.
pub const FLUSH_OPS_THRESHOLD: u64 = 1024;

/// Periodic flush trigger: every T elapsed since the last flush.
pub const FLUSH_ELAPSED_THRESHOLD: Duration = Duration::from_millis(200);

/// Allocation errors. Surfaced to the caller; `From` impl wires this into
/// the crate's central `Error` enum (see bottom of this file).
#[derive(Debug, thiserror::Error)]
pub enum SurrogateAllocError {
    #[error("surrogate space exhausted (u32::MAX reached)")]
    Exhausted,

    #[error("surrogate batch size 0 is not allowed")]
    EmptyBatch,

    #[error("surrogate flush failed: {detail}")]
    FlushFailed { detail: String },
}

/// Thread-safe surrogate allocator.
///
/// The `Mutex<Instant>` for `last_flush_at` is uncontended on the hot path
/// (`alloc_one`/`alloc` only touch atomics); only `should_flush` and
/// `flush` take the lock, which run at most once per ~200 ms or per 1024
/// allocations.
pub struct SurrogateRegistry {
    /// Next surrogate to hand out. Starts at `1` on a fresh registry, or
    /// `persisted_hwm + 1` on restart.
    counter: AtomicU64,
    /// Allocations since the last flush. Reset by `flush()`.
    allocs_since_flush: AtomicU64,
    /// Wall-clock anchor for the elapsed-time flush trigger.
    last_flush_at: Mutex<Instant>,
}

impl SurrogateRegistry {
    /// Create an empty registry — first allocation returns `Surrogate(1)`.
    pub fn new() -> Self {
        Self::from_persisted_hwm(0)
    }

    /// Restore from a persisted high-watermark. Next allocation returns
    /// `hwm + 1`. `hwm == 0` is equivalent to `new()` (no allocations yet).
    pub fn from_persisted_hwm(hwm: u32) -> Self {
        Self {
            counter: AtomicU64::new(u64::from(hwm) + 1),
            allocs_since_flush: AtomicU64::new(0),
            last_flush_at: Mutex::new(Instant::now()),
        }
    }

    /// Allocate a single surrogate. Returns `Exhausted` if the u32 space
    /// is full.
    pub fn alloc_one(&self) -> Result<Surrogate, SurrogateAllocError> {
        let prev = self.counter.fetch_add(1, Ordering::AcqRel);
        if prev > u64::from(u32::MAX) {
            // Restore the counter so future callers also see Exhausted
            // rather than racing past us into a wrapped value. We don't
            // need atomicity with the racing increments — once any caller
            // observes `prev > u32::MAX`, the space is effectively dead.
            self.counter
                .store(u64::from(u32::MAX) + 1, Ordering::Release);
            return Err(SurrogateAllocError::Exhausted);
        }
        self.allocs_since_flush.fetch_add(1, Ordering::AcqRel);
        // Safe: prev <= u32::MAX is guaranteed by the check above.
        Ok(Surrogate::new(prev as u32))
    }

    /// Allocate `n` contiguous surrogates as an inclusive range.
    /// Returns `EmptyBatch` for `n == 0`, `Exhausted` if the batch
    /// would cross the `u32::MAX` boundary.
    pub fn alloc(&self, n: u32) -> Result<RangeInclusive<Surrogate>, SurrogateAllocError> {
        if n == 0 {
            return Err(SurrogateAllocError::EmptyBatch);
        }
        let prev = self.counter.fetch_add(u64::from(n), Ordering::AcqRel);
        let last = prev + u64::from(n) - 1;
        if last > u64::from(u32::MAX) {
            self.counter
                .store(u64::from(u32::MAX) + 1, Ordering::Release);
            return Err(SurrogateAllocError::Exhausted);
        }
        self.allocs_since_flush
            .fetch_add(u64::from(n), Ordering::AcqRel);
        Ok(Surrogate::new(prev as u32)..=Surrogate::new(last as u32))
    }

    /// Highest surrogate ever issued — `0` if no allocations yet.
    pub fn current_hwm(&self) -> u32 {
        let next = self.counter.load(Ordering::Acquire);
        // `next` is `hwm + 1` on a healthy registry; saturate at u32::MAX
        // for the exhausted case where `next == u32::MAX as u64 + 1`.
        next.saturating_sub(1).min(u64::from(u32::MAX)) as u32
    }

    /// True if the periodic-flush thresholds (ops or elapsed) are tripped.
    pub fn should_flush(&self) -> bool {
        if self.allocs_since_flush.load(Ordering::Acquire) >= FLUSH_OPS_THRESHOLD {
            return true;
        }
        if let Ok(last) = self.last_flush_at.lock() {
            return last.elapsed() >= FLUSH_ELAPSED_THRESHOLD;
        }
        false
    }

    /// Persist the current high-watermark and reset flush counters.
    ///
    /// Idempotent: calling on an unmodified registry just rewrites the
    /// same hwm.
    pub fn flush(&self, persist: &dyn SurrogateHwmPersist) -> Result<(), SurrogateAllocError> {
        let hwm = self.current_hwm();
        persist
            .checkpoint(hwm)
            .map_err(|e| SurrogateAllocError::FlushFailed {
                detail: e.to_string(),
            })?;
        self.allocs_since_flush.store(0, Ordering::Release);
        if let Ok(mut guard) = self.last_flush_at.lock() {
            *guard = Instant::now();
        }
        Ok(())
    }

    /// Test-only: force the elapsed-flush trigger to fire on the next
    /// `should_flush` call by rewinding the wall-clock anchor.
    #[cfg(test)]
    fn rewind_flush_clock(&self, by: Duration) {
        if let Ok(mut guard) = self.last_flush_at.lock()
            && let Some(earlier) = guard.checked_sub(by)
        {
            *guard = earlier;
        }
    }
}

impl Default for SurrogateRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl From<SurrogateAllocError> for crate::Error {
    fn from(e: SurrogateAllocError) -> Self {
        match e {
            SurrogateAllocError::Exhausted => crate::Error::Internal {
                detail: "surrogate space exhausted (u32::MAX reached)".into(),
            },
            SurrogateAllocError::EmptyBatch => crate::Error::BadRequest {
                detail: "surrogate batch size 0 is not allowed".into(),
            },
            SurrogateAllocError::FlushFailed { detail } => crate::Error::Storage {
                engine: "surrogate".into(),
                detail,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicU32;

    use super::*;

    /// In-memory persist for tests — captures the latest checkpoint.
    struct MemPersist {
        last: std::sync::Mutex<Option<u32>>,
        calls: AtomicU32,
    }

    impl MemPersist {
        fn new() -> Self {
            Self {
                last: std::sync::Mutex::new(None),
                calls: AtomicU32::new(0),
            }
        }

        fn last(&self) -> Option<u32> {
            *self.last.lock().unwrap()
        }

        fn calls(&self) -> u32 {
            self.calls.load(Ordering::Acquire)
        }
    }

    impl SurrogateHwmPersist for MemPersist {
        fn checkpoint(&self, hwm: u32) -> crate::Result<()> {
            *self.last.lock().unwrap() = Some(hwm);
            self.calls.fetch_add(1, Ordering::AcqRel);
            Ok(())
        }

        fn load(&self) -> crate::Result<u32> {
            Ok(self.last().unwrap_or(0))
        }
    }

    #[test]
    fn monotonic_10k() {
        let reg = SurrogateRegistry::new();
        let mut prev = 0u32;
        for _ in 0..10_000 {
            let s = reg.alloc_one().unwrap().as_u32();
            assert!(s > prev, "expected monotonic, got {prev} then {s}");
            prev = s;
        }
        assert_eq!(reg.current_hwm(), 10_000);
    }

    #[test]
    fn batch_alloc_returns_range_then_advances() {
        let reg = SurrogateRegistry::new();
        let range = reg.alloc(100).unwrap();
        assert_eq!(*range.start(), Surrogate::new(1));
        assert_eq!(*range.end(), Surrogate::new(100));
        // Length of an inclusive range over u32-equivalents:
        let count = (range.end().as_u32() - range.start().as_u32() + 1) as usize;
        assert_eq!(count, 100);
        let next = reg.alloc_one().unwrap();
        assert_eq!(next, Surrogate::new(101));
    }

    #[test]
    fn batch_alloc_zero_rejected() {
        let reg = SurrogateRegistry::new();
        assert!(matches!(reg.alloc(0), Err(SurrogateAllocError::EmptyBatch)));
    }

    #[test]
    fn restart_survives_hwm() {
        let reg = SurrogateRegistry::from_persisted_hwm(5000);
        let s = reg.alloc_one().unwrap();
        assert_eq!(s, Surrogate::new(5001));
        assert_eq!(reg.current_hwm(), 5001);
    }

    #[test]
    fn concurrent_16x1000_unique() {
        let reg = Arc::new(SurrogateRegistry::new());
        let mut handles = Vec::with_capacity(16);
        for _ in 0..16 {
            let r = reg.clone();
            handles.push(std::thread::spawn(move || {
                let mut local = Vec::with_capacity(1000);
                for _ in 0..1000 {
                    local.push(r.alloc_one().unwrap());
                }
                local
            }));
        }
        let mut all = Vec::with_capacity(16_000);
        for h in handles {
            all.extend(h.join().unwrap());
        }
        all.sort();
        all.dedup();
        assert_eq!(all.len(), 16_000, "expected 16000 unique surrogates");
        assert!(reg.current_hwm() >= 16_000);
    }

    #[test]
    fn overflow_surfaces_typed_error() {
        // Bootstrap right at the edge: counter = u32::MAX, so the next
        // alloc returns Surrogate(u32::MAX), and the one after fails.
        let reg = SurrogateRegistry::from_persisted_hwm(u32::MAX - 1);
        let last = reg.alloc_one().unwrap();
        assert_eq!(last, Surrogate::new(u32::MAX));
        let err = reg.alloc_one().unwrap_err();
        assert!(matches!(err, SurrogateAllocError::Exhausted));
        // Subsequent calls also exhausted — counter does not wrap.
        assert!(matches!(
            reg.alloc_one().unwrap_err(),
            SurrogateAllocError::Exhausted
        ));
    }

    #[test]
    fn batch_overflow_surfaces_typed_error() {
        let reg = SurrogateRegistry::from_persisted_hwm(u32::MAX - 5);
        let err = reg.alloc(100).unwrap_err();
        assert!(matches!(err, SurrogateAllocError::Exhausted));
    }

    #[test]
    fn flush_threshold_ops() {
        let reg = SurrogateRegistry::new();
        assert!(!reg.should_flush(), "fresh registry should not flush yet");
        for _ in 0..(FLUSH_OPS_THRESHOLD - 1) {
            let _ = reg.alloc_one().unwrap();
        }
        assert!(!reg.should_flush(), "below ops threshold should not flush");
        let _ = reg.alloc_one().unwrap();
        assert!(reg.should_flush(), "at ops threshold should flush");

        let persist = MemPersist::new();
        reg.flush(&persist).unwrap();
        assert_eq!(persist.calls(), 1);
        assert_eq!(persist.last(), Some(FLUSH_OPS_THRESHOLD as u32));
        assert!(!reg.should_flush(), "post-flush should clear ops");
    }

    #[test]
    fn flush_threshold_elapsed() {
        let reg = SurrogateRegistry::new();
        let _ = reg.alloc_one().unwrap();
        assert!(!reg.should_flush());
        reg.rewind_flush_clock(FLUSH_ELAPSED_THRESHOLD * 2);
        assert!(reg.should_flush(), "rewound clock should fire elapsed");
        let persist = MemPersist::new();
        reg.flush(&persist).unwrap();
        assert!(!reg.should_flush(), "post-flush should reset clock");
    }

    #[test]
    fn flush_idempotent_on_empty_registry() {
        let reg = SurrogateRegistry::new();
        let persist = MemPersist::new();
        reg.flush(&persist).unwrap();
        reg.flush(&persist).unwrap();
        assert_eq!(persist.calls(), 2);
        assert_eq!(persist.last(), Some(0));
    }

    #[test]
    fn current_hwm_tracks_allocs() {
        let reg = SurrogateRegistry::new();
        assert_eq!(reg.current_hwm(), 0);
        let _ = reg.alloc_one().unwrap();
        assert_eq!(reg.current_hwm(), 1);
        let _ = reg.alloc(10).unwrap();
        assert_eq!(reg.current_hwm(), 11);
    }
}
