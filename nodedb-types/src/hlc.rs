// SPDX-License-Identifier: Apache-2.0

//! Hybrid Logical Clock.
//!
//! Canonical timestamp for replicated metadata entries, descriptor
//! modification times, and descriptor lease expiry. Monotonic across
//! wall-clock skew: `update` folds a remote HLC into the local clock so
//! causally later events always receive a strictly greater timestamp.
//!
//! Metadata DDL is not a hot path (hundreds per second at most), so the
//! clock is backed by a short-lived mutex rather than atomics.

use std::cmp::Ordering;
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

/// How far ahead of local wall time a remote HLC may be before it is refused.
///
/// A node whose clock is further ahead than this is misconfigured, and folding
/// its timestamp in would move this node's clock permanently. Five seconds
/// absorbs ordinary NTP disagreement while staying far below the descriptor
/// drain timeout, which must outlast any skew this admits.
pub const MAX_CLOCK_SKEW_NS: u64 = 5_000_000_000;

/// A remote HLC refused for running too far ahead of local wall time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClockSkew {
    /// The remote observation's wall clock, in nanoseconds since the epoch.
    pub remote_wall_ns: u64,
    /// This node's wall clock when the observation arrived.
    pub local_wall_ns: u64,
    /// How far ahead the remote clock ran.
    pub skew_ns: u64,
}

impl std::fmt::Display for ClockSkew {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "remote HLC wall clock is {}ms ahead of local (remote {}ns, local {}ns); \
             the maximum accepted skew is {}ms",
            self.skew_ns / 1_000_000,
            self.remote_wall_ns,
            self.local_wall_ns,
            MAX_CLOCK_SKEW_NS / 1_000_000
        )
    }
}

impl std::error::Error for ClockSkew {}

/// Hybrid Logical Clock timestamp.
///
/// `#[non_exhaustive]` — a `epoch_id` discriminant for multi-cluster
/// logical epochs may be added in a future release.
///
/// # Wire format note
///
/// `logical` is a 64-bit counter. MessagePack integers decode by value,
/// so small values serialised by future readers will still deserialise
/// correctly into this type.
#[non_exhaustive]
#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct Hlc {
    /// Wall-clock component: nanoseconds since the Unix epoch.
    pub wall_ns: u64,
    /// Logical counter incremented when two events share a wall-clock tick.
    pub logical: u64,
}

impl Hlc {
    pub const ZERO: Hlc = Hlc {
        wall_ns: 0,
        logical: 0,
    };

    pub const fn new(wall_ns: u64, logical: u64) -> Self {
        Self { wall_ns, logical }
    }
}

impl PartialOrd for Hlc {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Hlc {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.wall_ns.cmp(&other.wall_ns) {
            Ordering::Equal => self.logical.cmp(&other.logical),
            other => other,
        }
    }
}

/// Thread-safe HLC source. One instance per node.
#[derive(Debug, Default)]
pub struct HlcClock {
    state: Mutex<Hlc>,
}

impl HlcClock {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(Hlc::ZERO),
        }
    }

    /// Return a new HLC strictly greater than any previously returned value
    /// and ≥ the current wall clock.
    pub fn now(&self) -> Hlc {
        let wall = wall_now_ns();
        let mut st = self.state.lock().unwrap_or_else(|p| p.into_inner());
        // Clamp wall to at least st.wall_ns so the HLC never regresses on
        // clock skew or NTP adjustments.
        let wall = wall.max(st.wall_ns);
        let next = if wall > st.wall_ns {
            Hlc::new(wall, 0)
        } else {
            Hlc::new(st.wall_ns, st.logical + 1)
        };
        *st = next;
        next
    }

    /// Fold a remote HLC into the local clock and return a strictly greater HLC
    /// than both the prior local state and the remote observation.
    pub fn update(&self, remote: Hlc) -> Hlc {
        let wall = wall_now_ns();
        let mut st = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let prev = *st;
        let max_wall = wall.max(prev.wall_ns).max(remote.wall_ns);
        let next_logical = if max_wall == prev.wall_ns && max_wall == remote.wall_ns {
            prev.logical.max(remote.logical) + 1
        } else if max_wall == prev.wall_ns {
            prev.logical + 1
        } else if max_wall == remote.wall_ns {
            remote.logical + 1
        } else {
            0
        };
        let next = Hlc::new(max_wall, next_logical);
        *st = next;
        next
    }

    /// Fold a remote HLC into the local clock, refusing one whose wall clock
    /// runs further ahead than [`MAX_CLOCK_SKEW_NS`].
    ///
    /// [`Self::update`] takes an unconditional `max`, so a single far-future
    /// observation drags this node's clock forward permanently and every
    /// later local timestamp inherits the error. Every remote HLC crossing a
    /// node boundary goes through this, never `update`.
    ///
    /// Returns the skew in nanoseconds on refusal, so the caller can name the
    /// offending peer.
    pub fn update_checked(&self, remote: Hlc) -> Result<Hlc, ClockSkew> {
        let wall = wall_now_ns();
        let skew = remote.wall_ns.saturating_sub(wall);
        if skew > MAX_CLOCK_SKEW_NS {
            return Err(ClockSkew {
                remote_wall_ns: remote.wall_ns,
                local_wall_ns: wall,
                skew_ns: skew,
            });
        }
        Ok(self.update(remote))
    }

    /// Read the last observed HLC without advancing.
    pub fn peek(&self) -> Hlc {
        *self.state.lock().unwrap_or_else(|p| p.into_inner())
    }
}

fn wall_now_ns() -> u64 {
    crate::clock::since_epoch()
        .map(|d| d.as_nanos() as u64)
        .unwrap_or_else(|| {
            use std::sync::atomic::{AtomicBool, Ordering};
            static LOGGED: AtomicBool = AtomicBool::new(false);
            if !LOGGED.swap(true, Ordering::Relaxed) {
                tracing::error!(
                    module = module_path!(),
                    "system clock is before UNIX_EPOCH; using 0 (epoch) \
                     — check NTP/RTC configuration"
                );
            }
            0
        })
}

#[cfg(test)]
mod tests {

    /// A remote HLC within the bound is folded exactly as `update` would.
    #[test]
    fn a_remote_hlc_inside_the_bound_is_accepted() {
        let clock = HlcClock::new();
        let local = clock.now();
        let remote = Hlc::new(local.wall_ns + 1_000_000, 0);
        let folded = clock
            .update_checked(remote)
            .expect("a remote clock 1ms ahead is ordinary skew");
        assert!(folded > remote, "the fold must exceed the observation");
        assert!(clock.peek() >= folded);
    }

    /// A remote HLC past the bound is refused, and the clock does not move.
    #[test]
    fn a_remote_hlc_past_the_bound_is_refused_without_moving_the_clock() {
        let clock = HlcClock::new();
        let before = clock.peek();
        let far_future = Hlc::new(wall_now_ns() + MAX_CLOCK_SKEW_NS * 4, 0);
        let err = clock
            .update_checked(far_future)
            .expect_err("a clock four times the bound ahead must be refused");
        assert!(err.skew_ns > MAX_CLOCK_SKEW_NS);
        assert!(
            clock.peek() < far_future,
            "a refused observation must never advance the clock"
        );
        assert!(clock.peek() >= before);
    }

    /// A remote HLC behind local wall time is always in bounds — skew only
    /// bounds the future direction.
    #[test]
    fn a_remote_hlc_in_the_past_is_never_refused() {
        let clock = HlcClock::new();
        let past = Hlc::new(wall_now_ns().saturating_sub(MAX_CLOCK_SKEW_NS * 10), 0);
        assert!(clock.update_checked(past).is_ok());
    }
    use super::*;

    #[test]
    fn monotonic_within_tick() {
        let clock = HlcClock::new();
        let mut prev = clock.now();
        for _ in 0..1_000 {
            let next = clock.now();
            assert!(next > prev);
            prev = next;
        }
    }

    #[test]
    fn update_produces_strictly_greater() {
        let clock = HlcClock::new();
        let local = clock.now();
        let remote = Hlc::new(local.wall_ns + 1_000_000, 7);
        let merged = clock.update(remote);
        assert!(merged > remote);
        assert!(merged > local);
    }

    #[test]
    fn ordering_is_total() {
        let a = Hlc::new(10, 0);
        let b = Hlc::new(10, 1);
        let c = Hlc::new(11, 0);
        assert!(a < b);
        assert!(b < c);
        assert!(a < c);
    }

    /// 100K rapid-fire `now()` calls must each be strictly greater than
    /// the previous. Verifies the u64 logical counter advances without
    /// saturation.
    #[test]
    fn burst_strictly_monotonic() {
        let clock = HlcClock::new();
        let mut prev = clock.now();
        for _ in 0..100_000 {
            let next = clock.now();
            assert!(
                next > prev,
                "monotonicity violated: prev={prev:?} next={next:?}"
            );
            prev = next;
        }
    }

    /// Directly set the clock's logical counter to `u32::MAX` (the old
    /// saturation ceiling) and assert that the next `now()` returns a
    /// strictly greater HLC. Under the old `u32::saturating_add(1)` this
    /// would have pinned and returned an equal value.
    #[test]
    fn saturating_add_regression() {
        let clock = HlcClock::new();
        // Prime the clock so wall_ns is fixed.
        let seed = clock.now();
        // Inject logical = u32::MAX at the same wall_ns to reproduce the
        // old saturation point.
        {
            let mut st = clock.state.lock().unwrap_or_else(|p| p.into_inner());
            *st = Hlc::new(seed.wall_ns, u32::MAX as u64);
        }
        let next = clock.now();
        assert!(
            next > Hlc::new(seed.wall_ns, u32::MAX as u64),
            "logical counter pinned at u32::MAX: next={next:?}"
        );
    }

    /// 100K `update(remote)` calls with a fixed remote must each produce
    /// a strictly greater HLC than the prior call.
    #[test]
    fn update_burst_strictly_monotonic() {
        let clock = HlcClock::new();
        let remote = clock.now();
        let mut prev = clock.update(remote);
        for _ in 0..100_000 {
            let next = clock.update(remote);
            assert!(
                next > prev,
                "update monotonicity violated: prev={prev:?} next={next:?}"
            );
            prev = next;
        }
    }

    /// An `Hlc` with `logical` above `u32::MAX` must survive a zerompk
    /// encode/decode roundtrip with the value preserved exactly.
    #[test]
    fn roundtrip_logical_gt_u32_max() {
        let logical_val = (u32::MAX as u64) + 1;
        let original = Hlc::new(1_700_000_000_000_000_000_u64, logical_val);
        let bytes = zerompk::to_msgpack_vec(&original).expect("encode");
        let decoded: Hlc = zerompk::from_msgpack(&bytes).expect("decode");
        assert_eq!(decoded, original);
        assert_eq!(decoded.logical, logical_val);
    }
}
