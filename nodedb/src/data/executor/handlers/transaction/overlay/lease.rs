// SPDX-License-Identifier: BUSL-1.1

//! Shared lease-stamp and frozen system-time-ordinal primitives backing the
//! per-transaction staging overlays (GRAPH, ARRAY). Pure `Cell<i64>`
//! wrappers with no coupling to any overlay's staged-value types.

use std::cell::Cell;

/// Ordinal-clock stamp of the last time a transaction touched one overlay.
/// `Cell` (interior mutability) so a read-your-own-write path holding only
/// `&self` can still refresh the stamp; sound because a `CoreLoop` is
/// `!Send` and single-threaded per core. Read by the lease reaper, which
/// reclaims overlays whose stamp has aged past `OVERLAY_LEASE_NS`.
#[derive(Debug, Default)]
pub(in crate::data::executor) struct LeaseStamp(Cell<i64>);

impl LeaseStamp {
    /// Refresh the stamp to `ord`.
    pub fn touch(&self, ord: i64) {
        self.0.set(ord);
    }

    /// The last lease stamp (0 if never touched).
    pub fn last_touch(&self) -> i64 {
        self.0.get()
    }
}

/// Frozen system-time ordinal, set once on first resolve and returned
/// byte-for-byte on every later call. Separate from [`LeaseStamp`] so lease
/// refreshes can never change history.
#[derive(Debug, Default)]
pub(in crate::data::executor) struct SystemFromLatch(Cell<i64>);

impl SystemFromLatch {
    /// Freeze `candidate` on first call; every later call returns the
    /// originally-frozen value regardless of `candidate`.
    pub fn freeze(&self, candidate: i64) -> i64 {
        let frozen = self.0.get();
        if frozen != 0 {
            frozen
        } else {
            self.0.set(candidate);
            candidate
        }
    }

    /// The frozen value, or `None` if never resolved.
    pub fn resolved(&self) -> Option<i64> {
        let value = self.0.get();
        (value != 0).then_some(value)
    }
}
