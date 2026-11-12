// SPDX-License-Identifier: BUSL-1.1

//! Wall-clock millisecond → LSN resolution for the clone CoW resolver.
//!
//! Resolves a user-supplied `AS OF SYSTEM TIME <ms>` value to the WAL state
//! committed by then, from the WAL's time anchors. LSNs here are exclusive
//! bounds, as `wal.next_lsn()` is.

use nodedb_types::{Lsn, LsnTimeError};

use crate::control::state::SharedState;

/// The exclusive LSN bound of the state committed by millisecond `wall_ms`.
///
/// Fails when `wall_ms` is before the oldest retained time anchor. See
/// [`SharedState::ms_to_lsn`].
pub fn wall_ms_to_lsn(state: &SharedState, wall_ms: i64) -> Result<Lsn, LsnTimeError> {
    state.ms_to_lsn(wall_ms)
}

/// System time, in wall ms, at which a read through a clone cuts its source:
/// the commit time of the source state below `as_of_lsn`.
///
/// `None` for a non-bitemporal source. It keeps only current versions, so a
/// system-time cut hides every row. A clone collection carries its
/// source's `bitemporal` flag.
pub fn source_as_of_ms(
    state: &SharedState,
    source_bitemporal: bool,
    as_of_lsn: Lsn,
) -> Option<i64> {
    if !source_bitemporal {
        return None;
    }
    state.ms_to_lsn_inverse(as_of_lsn)
}

/// The creation LSN of a `CLONE DATABASE`: the WAL frontier, made durable
/// and anchored before the clone exists.
///
/// An append only buffers its record. The record's time anchor enters the
/// map only after the group-commit fsync that covers it. A read cut maps to
/// an LSN through those anchors ([`SharedState::ordinal_to_lsn`]), and a
/// time past the newest anchor resolves to that anchor. So a raw
/// `wal.next_lsn()` counts records that no cut can reach yet. A read whose
/// cut is taken after the clone then resolves below the creation LSN, and
/// the resolver answers it as a read that predates the clone. The window
/// lasts until some writer next waits for an fsync, which grows under load.
///
/// The fsync here stamps an anchor at or above the frontier's last record
/// before the clone is proposed. Every cut taken after the clone is created
/// then resolves at or above the creation LSN.
pub async fn anchored_clone_point(state: &SharedState) -> crate::Result<Lsn> {
    let frontier = state.wal.next_lsn();
    if let Some(last) = frontier.as_u64().checked_sub(1) {
        state.wal.wait_durable(Lsn::new(last)).await?;
    }
    Ok(frontier)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::types::{DatabaseId, TenantId, VShardId};
    use crate::wal::manager::NO_APPLY_KEY;

    fn shared_state(dir: &tempfile::TempDir) -> Arc<SharedState> {
        let wal = Arc::new(
            crate::wal::WalManager::open_for_testing(&dir.path().join("test.wal")).expect("wal"),
        );
        let (dispatcher, _data_sides) = crate::bridge::dispatch::Dispatcher::new(1, 64);
        SharedState::new(dispatcher, wal).expect("state")
    }

    /// A record appended with no fsync behind it sits below the clone point.
    /// A cut taken after the clone point still resolves at or above it, so
    /// a read inside a later transaction never predates the clone.
    #[tokio::test]
    async fn a_cut_after_the_clone_point_reaches_it_past_an_unsynced_append() {
        let dir = tempfile::tempdir().expect("tempdir");
        let state = shared_state(&dir);
        state
            .wal
            .appender(NO_APPLY_KEY)
            .with_event_source(crate::event::EventSource::User)
            .append_put(
                TenantId::new(1),
                VShardId::new(0),
                DatabaseId::DEFAULT,
                b"unsynced",
            )
            .expect("append");
        let raw_frontier = state.wal.next_lsn();
        let cut = state.wal.ordinal_clock().next_ordinal();
        assert!(
            state.ordinal_to_lsn(cut).expect("cut lsn") < raw_frontier,
            "an unsynced record sits past every anchor"
        );

        let clone_point = anchored_clone_point(&state).await.expect("clone point");
        assert_eq!(clone_point, raw_frontier);
        let later_cut = state.wal.ordinal_clock().next_ordinal();
        assert!(
            state.ordinal_to_lsn(later_cut).expect("cut lsn") >= clone_point,
            "a cut after the clone point resolves at or above it"
        );
    }
}
