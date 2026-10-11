// SPDX-License-Identifier: BUSL-1.1

//! The stored dependent-read barrier log of each txn on each vShard.
//!
//! The data-group apply loop concludes a barrier entry as durably applied
//! only once its row in `_system.calvin_barrier_logs` holds the event. The
//! durable applied floor of a group asserts that every entry at or below it
//! is durable, and boot resumes Raft delivery above it. So an entry below
//! the floor is never delivered again, and its row is what a barrier of the
//! txn reads after a restart:
//!
//! - [`restore_at_boot`] marks the txn of every stored row stored in its
//!   vShard's buffer, and removes the rows of finished txns;
//! - [`capture_group`] and [`install_group`] carry the rows of a data group's
//!   unfinished txns in its snapshot, for the entries the snapshot covers;
//! - the scheduler removes a txn's row once the txn finished, and
//!   [`sweep_finished`] removes on its stall tick each finished txn's row
//!   that removal missed.

use std::collections::{BTreeMap, BTreeSet};

use super::applied_ledger::CalvinAppliedLedger;
use super::barrier_log::{BarrierEvent, BarrierLog};
use super::lock::TxnId;
use crate::control::security::catalog::SystemCatalog;
use crate::control::security::catalog::calvin_barrier_logs::StoredBarrierLog;
use crate::control::state::SharedState;
use crate::types::CutBarrierLog;

/// Fold `event` into the stored row of `txn` on `vshard_id`. An event the
/// row's log does not take, a copy it holds or one after its timeout,
/// writes nothing.
pub fn save_event(
    catalog: &SystemCatalog,
    vshard_id: u32,
    txn: TxnId,
    event: &BarrierEvent,
) -> crate::Result<()> {
    catalog.fold_calvin_barrier_log(vshard_id, txn.epoch, txn.position, |held| {
        let mut log = match held {
            Some(bytes) => BarrierLog::from_bytes(bytes)?,
            None => BarrierLog::default(),
        };
        if held.is_some() && !log.accepts(event) {
            return Ok(None);
        }
        log.note(event.clone());
        log.to_bytes().map(Some)
    })
}

/// The stored log of `txn` on `vshard_id`. A txn with no row has an empty
/// log.
pub fn load_log(catalog: &SystemCatalog, vshard_id: u32, txn: TxnId) -> crate::Result<BarrierLog> {
    match catalog.load_calvin_barrier_log(vshard_id, txn.epoch, txn.position)? {
        Some(bytes) => BarrierLog::from_bytes(&bytes),
        None => Ok(BarrierLog::default()),
    }
}

/// Remove the stored row of `txn` on `vshard_id`, which finished.
pub fn remove_log(catalog: &SystemCatalog, vshard_id: u32, txn: TxnId) -> crate::Result<()> {
    catalog.remove_calvin_barrier_logs(&[(vshard_id, txn.epoch, txn.position)])
}

/// Remove every stored row of `vshard_id` whose txn `ledger` marks applied.
/// Returns how many rows went.
///
/// A finished txn's row stays when its removal at the finish failed, or
/// when its event was saved while the txn finished. The scheduler runs this
/// on its stall tick, so such a row goes while the node runs, and a failed
/// pass retries on the next tick.
pub fn sweep_finished(
    catalog: &SystemCatalog,
    vshard_id: u32,
    ledger: &CalvinAppliedLedger,
) -> crate::Result<usize> {
    let finished: Vec<(u32, u64, u32)> = catalog
        .load_calvin_barrier_logs(Some(&BTreeSet::from([vshard_id])))?
        .into_iter()
        .filter(|row| ledger.is_applied(row.epoch, row.position))
        .map(|row| (row.vshard_id, row.epoch, row.position))
        .collect();
    catalog.remove_calvin_barrier_logs(&finished)?;
    Ok(finished.len())
}

/// Whether `shared`'s ledger of `vshard_id` holds `(epoch, position)`.
fn is_applied(shared: &SharedState, vshard_id: u32, epoch: u64, position: u32) -> bool {
    shared
        .calvin
        .applied
        .get(vshard_id)
        .is_some_and(|ledger| ledger.is_applied(epoch, position))
}

/// Mark the txn of every stored row stored in its vShard's buffer, and
/// remove the rows of finished txns. Boot runs this once the applied
/// ledgers are filled, before the data-group apply loop and the schedulers
/// start.
pub fn restore_at_boot(shared: &SharedState) -> crate::Result<()> {
    let catalog = shared.credentials.catalog();
    let mut finished = Vec::new();
    let mut waiting: BTreeMap<u32, Vec<TxnId>> = BTreeMap::new();
    for row in catalog.load_calvin_barrier_logs(None)? {
        if is_applied(shared, row.vshard_id, row.epoch, row.position) {
            finished.push((row.vshard_id, row.epoch, row.position));
        } else {
            waiting
                .entry(row.vshard_id)
                .or_default()
                .push(TxnId::new(row.epoch, row.position));
        }
    }
    catalog.remove_calvin_barrier_logs(&finished)?;
    for (vshard_id, txns) in waiting {
        shared
            .calvin
            .read_results
            .vshard(vshard_id)
            .mark_stored(txns);
    }
    Ok(())
}

/// The stored rows of the unfinished txns of `vshards`, for a snapshot of
/// their data group. The caller fences the group's apply, so the rows hold
/// exactly the barrier entries at or below the snapshot's index.
///
/// Fails when a vShard holds an event its row lacks: the rows do not hold
/// every entry the snapshot covers. The build retries on the next
/// heartbeat.
pub fn capture_group(
    shared: &SharedState,
    vshards: &BTreeSet<u32>,
) -> crate::Result<Vec<CutBarrierLog>> {
    if let Some(vshard_id) = vshards
        .iter()
        .copied()
        .find(|vshard_id| shared.calvin.read_results.vshard(*vshard_id).has_unsaved())
    {
        return Err(crate::Error::Internal {
            detail: format!(
                "snapshot build: vShard {vshard_id} holds a dependent-read barrier event its \
                 stored row lacks; the build retries on the next heartbeat"
            ),
        });
    }
    let rows = shared
        .credentials
        .catalog()
        .load_calvin_barrier_logs(Some(vshards))?;
    Ok(rows
        .into_iter()
        .filter(|row| !is_applied(shared, row.vshard_id, row.epoch, row.position))
        .map(|row| CutBarrierLog {
            vshard_id: row.vshard_id,
            epoch: row.epoch,
            position: row.position,
            log: row.log,
        })
        .collect())
}

/// Install `logs`, the rows a snapshot of the data group of `vshards`
/// carries: they replace this node's rows of `vshards`, and each vShard's
/// buffer holds their txns as stored.
pub fn install_group(
    shared: &SharedState,
    vshards: &BTreeSet<u32>,
    logs: Vec<CutBarrierLog>,
) -> crate::Result<()> {
    let rows: Vec<StoredBarrierLog> = logs
        .into_iter()
        .filter(|log| vshards.contains(&log.vshard_id))
        .map(|log| StoredBarrierLog {
            vshard_id: log.vshard_id,
            epoch: log.epoch,
            position: log.position,
            log: log.log,
        })
        .collect();
    shared
        .credentials
        .catalog()
        .replace_calvin_barrier_logs(vshards, &rows)?;
    for vshard_id in vshards {
        let stored: BTreeSet<TxnId> = rows
            .iter()
            .filter(|row| row.vshard_id == *vshard_id)
            .map(|row| TxnId::new(row.epoch, row.position))
            .collect();
        shared
            .calvin
            .read_results
            .vshard(*vshard_id)
            .reset_to_stored(stored);
    }
    Ok(())
}

/// Remove every row of `vshard_id`, which left this node, and its buffer.
pub fn forget_vshard(shared: &SharedState, vshard_id: u32) -> crate::Result<()> {
    shared.calvin.read_results.forget(vshard_id);
    shared
        .credentials
        .catalog()
        .replace_calvin_barrier_logs(&BTreeSet::from([vshard_id]), &[])
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nodedb_physical::physical_plan::meta::PassiveReadKeyId;
    use nodedb_types::{QualifiedCollection, Value};

    use super::*;
    use crate::bridge::dispatch::Dispatcher;
    use crate::control::cluster::calvin::scheduler::NOT_YET_APPLIED_EPOCH;
    use crate::wal::WalManager;

    const VSHARD: u32 = 5;

    fn state() -> (Arc<SharedState>, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = Arc::new(WalManager::open_for_testing(&dir.path().join("wal")).expect("wal"));
        let (dispatcher, _sides) = Dispatcher::new(1, 64);
        (
            SharedState::new(dispatcher, wal).expect("shared state"),
            dir,
        )
    }

    fn read(passive_vshard: u32, value: i64) -> BarrierEvent {
        BarrierEvent::Read {
            passive_vshard,
            values: vec![(
                PassiveReadKeyId::kv(
                    QualifiedCollection::from_stored("items".to_owned()),
                    b"k".to_vec(),
                ),
                Value::Integer(value),
            )],
        }
    }

    /// Saved events fold in order into the row, and the row reloads.
    #[test]
    fn saved_events_fold_into_the_row() {
        let (state, _dir) = state();
        let catalog = state.credentials.catalog();
        let txn = TxnId::new(4, 1);
        save_event(catalog, VSHARD, txn, &read(1, 10)).expect("save");
        save_event(catalog, VSHARD, txn, &BarrierEvent::Timeout).expect("save");
        save_event(catalog, VSHARD, txn, &read(2, 20)).expect("save");
        let mut expected = BarrierLog::default();
        expected.note(read(1, 10));
        expected.note(BarrierEvent::Timeout);
        assert_eq!(load_log(catalog, VSHARD, txn).expect("load"), expected);
        remove_log(catalog, VSHARD, txn).expect("remove");
        assert_eq!(
            load_log(catalog, VSHARD, txn).expect("load"),
            BarrierLog::default()
        );
    }

    /// Boot rebuilds the buffer from the stored rows: an unfinished txn's
    /// events wait as stored, and a finished txn's row is removed.
    #[test]
    fn boot_rebuilds_the_buffer_from_the_rows() {
        let (state, _dir) = state();
        let catalog = state.credentials.catalog();
        let open = TxnId::new(7, 0);
        let done = TxnId::new(6, 0);
        save_event(catalog, VSHARD, open, &read(1, 10)).expect("save");
        save_event(catalog, VSHARD, done, &read(1, 11)).expect("save");
        state
            .calvin
            .applied
            .install(VSHARD, NOT_YET_APPLIED_EPOCH, BTreeSet::from([(6, 0)]));

        restore_at_boot(&state).expect("restore");

        let buffer = state.calvin.read_results.vshard(VSHARD);
        assert_eq!(buffer.waiting_txns(), 1);
        let events = buffer.take(open).expect("the open txn waits");
        assert!(events.stored);
        let mut expected = BarrierLog::default();
        expected.note(read(1, 10));
        assert_eq!(load_log(catalog, VSHARD, open).expect("load"), expected);
        assert!(
            catalog
                .load_calvin_barrier_log(VSHARD, done.epoch, done.position)
                .expect("load")
                .is_none(),
            "the finished txn's row is removed"
        );
    }

    /// A snapshot carries the rows of unfinished txns, and its install
    /// replaces the target's rows and buffer with them.
    #[test]
    fn a_snapshot_carries_the_rows_of_unfinished_txns() {
        let (builder, _builder_dir) = state();
        let open = TxnId::new(3, 2);
        save_event(builder.credentials.catalog(), VSHARD, open, &read(1, 10)).expect("save");
        save_event(
            builder.credentials.catalog(),
            VSHARD,
            TxnId::new(2, 0),
            &read(1, 9),
        )
        .expect("save");
        builder
            .calvin
            .applied
            .install(VSHARD, NOT_YET_APPLIED_EPOCH, BTreeSet::from([(2, 0)]));
        let vshards = BTreeSet::from([VSHARD]);
        let logs = capture_group(&builder, &vshards).expect("capture");
        assert_eq!(logs.len(), 1);
        assert_eq!((logs[0].epoch, logs[0].position), (3, 2));

        let (target, _target_dir) = state();
        let stale = TxnId::new(1, 0);
        save_event(target.credentials.catalog(), VSHARD, stale, &read(1, 1)).expect("save");
        target
            .calvin
            .read_results
            .vshard(VSHARD)
            .push(stale, read(1, 1), 8, false);
        install_group(&target, &vshards, logs).expect("install");

        let buffer = target.calvin.read_results.vshard(VSHARD);
        assert!(!buffer.waits(stale));
        assert!(!buffer.has_unsaved());
        assert!(buffer.take(open).expect("installed txn").stored);
        let mut expected = BarrierLog::default();
        expected.note(read(1, 10));
        assert_eq!(
            load_log(target.credentials.catalog(), VSHARD, open).expect("load"),
            expected
        );
        assert_eq!(
            load_log(target.credentials.catalog(), VSHARD, stale).expect("load"),
            BarrierLog::default()
        );
    }

    /// A leftover row of a finished txn goes on the sweep, with no restart.
    /// The row of an unfinished txn stays.
    #[test]
    fn the_sweep_removes_a_finished_txns_leftover_row() {
        let (state, _dir) = state();
        let catalog = state.credentials.catalog();
        let done = TxnId::new(8, 0);
        let open = TxnId::new(9, 0);
        save_event(catalog, VSHARD, done, &read(1, 1)).expect("save");
        save_event(catalog, VSHARD, open, &read(1, 2)).expect("save");
        save_event(catalog, VSHARD + 1, done, &read(1, 3)).expect("save");
        let ledger = state.calvin.applied.get_or_create(VSHARD);
        ledger.mark_terminal(done.epoch, done.position);

        assert_eq!(sweep_finished(catalog, VSHARD, &ledger).expect("sweep"), 1);

        let left: Vec<(u32, u64, u32)> = catalog
            .load_calvin_barrier_logs(None)
            .expect("load")
            .into_iter()
            .map(|row| (row.vshard_id, row.epoch, row.position))
            .collect();
        assert_eq!(left, vec![(VSHARD, 9, 0), (VSHARD + 1, 8, 0)]);
        assert_eq!(sweep_finished(catalog, VSHARD, &ledger).expect("sweep"), 0);
    }

    /// A vShard holding an unsaved event refuses the capture.
    #[test]
    fn an_unsaved_event_refuses_the_capture() {
        let (state, _dir) = state();
        state
            .calvin
            .read_results
            .vshard(VSHARD)
            .push(TxnId::new(1, 0), read(1, 1), 8, false);
        assert!(capture_group(&state, &BTreeSet::from([VSHARD])).is_err());
    }
}
