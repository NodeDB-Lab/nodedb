// SPDX-License-Identifier: BUSL-1.1

//! Commit stamps of the user writes a server with no Raft groups is minting.
//!
//! Such a write takes its commit stamp before it mints its WAL record, so its
//! mark can be durable first. Between the stamp and the mint, a backup cut
//! would not see the write: the cut waits only for records already minted.
//! So each stamp stays registered here until its record is minted, and the
//! cut waits for every stamp at or below its watermark to go.
//!
//! A stamp is taken under the registry lock, and the cut reads the registry
//! under the same lock after it moves the clock past its watermark. So either
//! the cut sees the stamp, or the stamp reads above the watermark.

use std::collections::BTreeMap;
use std::sync::Mutex;

#[derive(Debug, Default)]
struct Registry {
    next_id: u64,
    /// Open stamp id → its commit HLC wall time, in nanoseconds.
    open: BTreeMap<u64, u64>,
}

/// The open commit stamps of local writes.
#[derive(Debug, Default)]
pub struct LocalWriteStamps {
    registry: Mutex<Registry>,
    /// Woken each time a stamp closes.
    closed: tokio::sync::Notify,
}

/// One open stamp. Dropping it states the write minted its record, or will
/// mint none.
#[derive(Debug)]
pub struct LocalWriteStamp<'a> {
    stamps: &'a LocalWriteStamps,
    id: u64,
    hlc: u64,
}

impl LocalWriteStamp<'_> {
    /// The write's commit HLC wall time, in nanoseconds.
    pub fn hlc(&self) -> u64 {
        self.hlc
    }
}

impl Drop for LocalWriteStamp<'_> {
    fn drop(&mut self) {
        self.stamps
            .registry
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .open
            .remove(&self.id);
        self.stamps.closed.notify_waiters();
    }
}

impl LocalWriteStamps {
    /// Open a stamp at the clock's next instant.
    pub fn stamp(&self, clock: &nodedb_types::HlcClock) -> LocalWriteStamp<'_> {
        let mut registry = self.registry.lock().unwrap_or_else(|p| p.into_inner());
        let hlc = clock.now().wall_ns;
        let id = registry.next_id;
        registry.next_id += 1;
        registry.open.insert(id, hlc);
        LocalWriteStamp {
            stamps: self,
            id,
            hlc,
        }
    }

    fn open_through(&self, watermark: u64) -> bool {
        self.registry
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .open
            .values()
            .any(|hlc| *hlc <= watermark)
    }

    /// Wait until no stamp at or below `watermark` is open. `false` when
    /// `deadline` passes first.
    pub async fn await_minted_through(
        &self,
        watermark: u64,
        deadline: tokio::time::Instant,
    ) -> bool {
        loop {
            // Created before the check: `notify_waiters` wakes it from here on.
            let closed = self.closed.notified();
            if !self.open_through(watermark) {
                return true;
            }
            if tokio::time::timeout_at(deadline, closed).await.is_err() {
                return !self.open_through(watermark);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn the_wait_ends_once_every_stamp_below_the_watermark_closes() {
        let clock = nodedb_types::HlcClock::new();
        let stamps = LocalWriteStamps::default();
        let early = stamps.stamp(&clock);
        let watermark = early.hlc();
        let late = stamps.stamp(&clock);
        assert!(late.hlc() > watermark);

        let short = tokio::time::Instant::now() + Duration::from_millis(20);
        assert!(
            !stamps.await_minted_through(watermark, short).await,
            "an open stamp at the watermark holds the wait"
        );

        drop(early);
        let long = tokio::time::Instant::now() + Duration::from_secs(5);
        assert!(
            stamps.await_minted_through(watermark, long).await,
            "a stamp above the watermark does not hold the wait"
        );
        drop(late);
    }
}
