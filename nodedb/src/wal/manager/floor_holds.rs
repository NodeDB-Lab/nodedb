// SPDX-License-Identifier: BUSL-1.1

//! Floors WAL truncation must keep.
//!
//! A holder that reads records back after a restart takes a [`WalFloorHold`]
//! at the lowest LSN it needs. [`super::WalManager::truncate_before`] never
//! deletes a segment holding a record at or above the lowest held LSN. A
//! hold lasts until its guard drops.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::types::Lsn;

/// The held floors of one WAL.
#[derive(Default)]
pub struct WalFloorHolds {
    inner: Arc<HoldsInner>,
}

#[derive(Default)]
struct HoldsInner {
    next_id: AtomicU64,
    /// Hold id to the LSN it holds.
    held: Mutex<BTreeMap<u64, u64>>,
}

/// One held floor. Dropping it releases the floor.
pub struct WalFloorHold {
    inner: Arc<HoldsInner>,
    id: u64,
    lsn: Lsn,
}

impl std::fmt::Debug for WalFloorHold {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WalFloorHold")
            .field("id", &self.id)
            .field("lsn", &self.lsn)
            .finish()
    }
}

impl WalFloorHolds {
    /// Hold truncation below `lsn` until the returned guard drops.
    pub fn hold(&self, lsn: Lsn) -> WalFloorHold {
        let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        self.inner
            .held
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(id, lsn.as_u64());
        WalFloorHold {
            inner: Arc::clone(&self.inner),
            id,
            lsn,
        }
    }

    /// The lowest held LSN, or `None` when nothing is held.
    pub fn lowest(&self) -> Option<Lsn> {
        self.inner
            .held
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .values()
            .min()
            .map(|lsn| Lsn::new(*lsn))
    }

    /// `lsn`, lowered to the lowest held LSN.
    pub fn clamp(&self, lsn: Lsn) -> Lsn {
        match self.lowest() {
            Some(held) if held < lsn => held,
            _ => lsn,
        }
    }
}

impl WalFloorHold {
    /// The LSN this guard holds.
    pub fn lsn(&self) -> Lsn {
        self.lsn
    }
}

impl Drop for WalFloorHold {
    fn drop(&mut self) {
        self.inner
            .held
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&self.id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_lowest_live_hold_clamps_truncation() {
        let holds = WalFloorHolds::default();
        assert_eq!(holds.clamp(Lsn::new(50)), Lsn::new(50));
        let high = holds.hold(Lsn::new(30));
        let low = holds.hold(Lsn::new(10));
        assert_eq!(holds.clamp(Lsn::new(50)), Lsn::new(10));
        assert_eq!(holds.clamp(Lsn::new(5)), Lsn::new(5));
        drop(low);
        assert_eq!(holds.clamp(Lsn::new(50)), Lsn::new(30));
        drop(high);
        assert_eq!(holds.lowest(), None);
    }
}
