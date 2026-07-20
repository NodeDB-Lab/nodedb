// SPDX-License-Identifier: BUSL-1.1

//! Bitemporal stamping helpers for `CoreLoop`.
//!
//! `_ts_system` on every bitemporal write must be strictly monotonic
//! per-core so that purge / `AS OF` queries see a total version order
//! even when the wall clock moves backwards (NTP step, VM pause, leap
//! second smear). We implement an HLC-lite: each stamp returns
//! `max(wall_ms, last_stamp_ms + 1)` and advances an atomic watermark.
//! Because each `CoreLoop` is pinned to one Data Plane thread, `Relaxed`
//! ordering is sufficient.

use std::sync::atomic::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::types::TenantId;

use super::CoreLoop;

impl CoreLoop {
    /// Is this collection configured for bitemporal (versioned) storage?
    /// Collections flagged `bitemporal: true` on registration route every
    /// write to the versioned redb table and every read via the Ceiling
    /// resolver, enabling `FOR SYSTEM_TIME AS OF` / `FOR VALID_TIME`
    /// queries.
    #[inline]
    pub(in crate::data::executor) fn is_bitemporal(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
    ) -> bool {
        let key = (
            crate::types::DatabaseId::new(database_id),
            TenantId::new(tid),
            collection.to_string(),
        );
        self.doc_configs.get(&key).is_some_and(|c| c.bitemporal)
    }

    /// Monotonic millisecond stamp for versioned writes. Used as
    /// `system_from_ms` on every write to a bitemporal collection.
    ///
    /// When executing inside a Calvin epoch (`epoch_system_ms` is `Some`),
    /// uses the epoch's deterministic timestamp anchor so all replicas produce
    /// byte-identical bitemporal versions. Outside the Calvin path (single-shard
    /// writes, WAL replay, etc.) falls back to the wall clock.
    ///
    /// Returns `max(anchor_ms, last_stamp_ms + 1)` and bumps the per-core
    /// watermark. Guarantees: strictly increasing on this core.
    #[inline]
    pub(in crate::data::executor) fn bitemporal_now_ms(&self) -> i64 {
        let prev = self.last_stamp_ms.load(Ordering::Relaxed);
        let anchor = if let Some(epoch_ms) = self.epoch_system_ms {
            epoch_ms
        } else {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
                .unwrap_or(prev)
        };
        let next = anchor.max(prev.saturating_add(1));
        self.last_stamp_ms.store(next, Ordering::Relaxed);
        next
    }

    /// Advance the per-core HLC watermark to at least `sys_from_ms` without
    /// producing a new stamp. Used by WAL replay of a carried bitemporal stamp:
    /// the stamp was assigned on the original core at commit, so post-restart
    /// writes must not hand out a value at or below it. `fetch_max` keeps the
    /// update monotonic and lock-free (single-threaded per core, `Relaxed`).
    #[inline]
    pub(in crate::data::executor) fn observe_bitemporal_stamp(&self, sys_from_ms: i64) {
        self.last_stamp_ms.fetch_max(sys_from_ms, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicI64, Ordering};

    /// The HLC invariant, verified against the same atomic the real
    /// method uses. Keeps the test hermetic — doesn't depend on
    /// constructing a full `CoreLoop`.
    fn stamp(clock: &AtomicI64, wall: i64) -> i64 {
        let prev = clock.load(Ordering::Relaxed);
        let next = wall.max(prev.saturating_add(1));
        clock.store(next, Ordering::Relaxed);
        next
    }

    #[test]
    fn strictly_monotonic_under_backward_wall_clock() {
        let clock = AtomicI64::new(0);
        let a = stamp(&clock, 1_000);
        let b = stamp(&clock, 999); // wall moved backward
        let c = stamp(&clock, 500); // bigger jump backward
        assert_eq!(a, 1_000);
        assert_eq!(b, 1_001);
        assert_eq!(c, 1_002);
        assert!(a < b && b < c);
    }

    #[test]
    fn follows_wall_clock_when_it_advances() {
        let clock = AtomicI64::new(0);
        assert_eq!(stamp(&clock, 1_000), 1_000);
        assert_eq!(stamp(&clock, 2_000), 2_000);
        assert_eq!(stamp(&clock, 2_000), 2_001); // same ms → +1
        assert_eq!(stamp(&clock, 5_000), 5_000);
    }

    #[test]
    fn saturates_at_i64_max() {
        let clock = AtomicI64::new(i64::MAX);
        assert_eq!(stamp(&clock, 0), i64::MAX);
    }
}
