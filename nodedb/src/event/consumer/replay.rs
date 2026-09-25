// SPDX-License-Identifier: BUSL-1.1

//! Rebuild the events of a range of WAL records for one core.

use crate::event::types::WriteEvent;
use crate::event::wal_replay::{replay_wal_mmap, replay_wal_to_events};
use crate::types::Lsn;
use crate::wal::WalManager;

use super::fail_stop::is_retained_floor_violation;

/// The events of every record from `from` through `upto` routed to
/// `core_id`, in LSN order.
pub fn replay_range(
    wal: &WalManager,
    from: Lsn,
    upto: Lsn,
    core_id: usize,
    num_cores: usize,
) -> crate::Result<Vec<WriteEvent>> {
    // Rebuilt events carry no ring number; the guard names them by record.
    let events = replay_wal_mmap(wal, from, core_id, num_cores, 0).or_else(|e| {
        // The sequential reader is a fallback for readers that cannot see the
        // bytes (mmap misses O_DIRECT writes to the active segment), not for a
        // log that no longer holds the records: both read the same directory.
        if is_retained_floor_violation(&e) {
            return Err(e);
        }
        replay_wal_to_events(wal, from, core_id, num_cores, 0)
    })?;
    Ok(events
        .into_iter()
        .filter(|event| event.lsn <= upto)
        .collect())
}
