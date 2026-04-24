//! Strictly-monotonic `i64` ordinal clock for bitemporal `system_from` keys.
//!
//! Requirements the storage layer places on `system_from`:
//!
//! 1. Strictly monotonic — two writes to the same edge within the same
//!    wall-clock nanosecond must produce distinct key suffixes.
//! 2. Aligned with wall-clock time to nanosecond precision so that user
//!    "AS OF SYSTEM TIME `<ms>`" queries translate cleanly via
//!    [`ms_to_ordinal_upper`].
//! 3. Representable as `i64` so the 20-digit zero-padded decimal suffix
//!    used in `versioned_edge_key` has room for year ≤ 2262.
//!
//! `HlcClock` (in `nodedb-types::hlc`) satisfies (1) via a `logical`
//! sidecar, but its external value space is `(u64 wall_ns, u32 logical)`
//! which does not pack into a single `i64` monotonically. `OrdinalClock`
//! therefore owns its own advancing counter, seeded each tick by the
//! wall clock and forcibly incremented if wall time didn't advance.
//!
//! Cross-node convergence (folding a remote peer's ordinal forward) is
//! handled by the closed-timestamp wiring; `update_from_remote` is
//! exposed here so that wiring has a single canonical API to call.

use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

/// Nanoseconds in one millisecond.
pub const NANOS_PER_MS: i64 = 1_000_000;

/// Strictly monotonic `i64` ordinal clock.
///
/// Cheap to share across Data Plane cores via `Arc` — the internal mutex
/// is only taken for the length of a counter read/compare/write and is
/// not a hot-path bottleneck compared to redb commits.
#[derive(Debug, Default)]
pub struct OrdinalClock {
    last: Mutex<i64>,
}

impl OrdinalClock {
    pub fn new() -> Self {
        Self {
            last: Mutex::new(0),
        }
    }

    /// Return a strictly-greater ordinal than any previously returned value,
    /// seeded by the current wall-clock nanosecond.
    pub fn next_ordinal(&self) -> i64 {
        let wall = wall_now_ns();
        let mut st = self.last.lock().unwrap_or_else(|p| p.into_inner());
        let next = wall.max(st.saturating_add(1));
        *st = next;
        next
    }

    /// Fold a remote peer's observed ordinal into the local clock so the
    /// next local [`next_ordinal`](Self::next_ordinal) call is strictly
    /// greater. No-op if the remote value is already behind us.
    pub fn update_from_remote(&self, remote: i64) {
        let mut st = self.last.lock().unwrap_or_else(|p| p.into_inner());
        if remote > *st {
            *st = remote;
        }
    }

    /// Peek the last emitted ordinal without advancing.
    pub fn peek(&self) -> i64 {
        *self.last.lock().unwrap_or_else(|p| p.into_inner())
    }
}

/// Convert a user-supplied wall-clock millisecond into the **inclusive upper
/// bound** ordinal that Ceiling scans should target.
///
/// Because ordinals are ns-precision, "AS OF SYSTEM TIME `<ms>`" must
/// include every ordinal written during `[ms*1e6, ms*1e6 + 999_999]`.
pub fn ms_to_ordinal_upper(ms: i64) -> i64 {
    ms.saturating_mul(NANOS_PER_MS)
        .saturating_add(NANOS_PER_MS - 1)
}

/// Convert an ordinal back to wall-clock milliseconds (truncating).
pub fn ordinal_to_ms(ordinal: i64) -> i64 {
    ordinal / NANOS_PER_MS
}

fn wall_now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| {
            let ns = d.as_nanos();
            if ns > i64::MAX as u128 {
                i64::MAX
            } else {
                ns as i64
            }
        })
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn next_ordinal_is_strictly_monotonic() {
        let clock = OrdinalClock::new();
        let mut prev = clock.next_ordinal();
        for _ in 0..10_000 {
            let next = clock.next_ordinal();
            assert!(next > prev, "expected {next} > {prev}");
            prev = next;
        }
    }

    #[test]
    fn next_ordinal_tracks_wall_clock_roughly() {
        let clock = OrdinalClock::new();
        let ord = clock.next_ordinal();
        let wall = wall_now_ns();
        // The ordinal should be within one second of wall clock on any
        // reasonable machine — proves we're not emitting a pure counter.
        assert!((ord - wall).abs() < 1_000_000_000, "ord={ord} wall={wall}");
    }

    #[test]
    fn update_from_remote_advances_on_greater() {
        let clock = OrdinalClock::new();
        let _ = clock.next_ordinal();
        let remote = wall_now_ns() + 10_000_000_000; // 10s in future
        clock.update_from_remote(remote);
        let next = clock.next_ordinal();
        assert!(next > remote);
    }

    #[test]
    fn update_from_remote_noop_when_behind() {
        let clock = OrdinalClock::new();
        let local = clock.next_ordinal();
        clock.update_from_remote(local - 1_000);
        assert!(clock.peek() >= local);
    }

    #[test]
    fn ms_to_ordinal_upper_round_trip() {
        let ms = 1_700_000_000_000;
        let upper = ms_to_ordinal_upper(ms);
        assert_eq!(ordinal_to_ms(upper), ms);
        // The *next* ms's lower bound is strictly greater than this upper.
        let next_lower = (ms + 1).saturating_mul(NANOS_PER_MS);
        assert!(next_lower > upper);
    }

    #[test]
    fn ms_conversions_saturate_on_overflow() {
        // i64::MAX ms would overflow when multiplied by 1e6; saturating op
        // must not panic.
        let upper = ms_to_ordinal_upper(i64::MAX);
        assert_eq!(upper, i64::MAX);
    }
}
