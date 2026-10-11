// SPDX-License-Identifier: BUSL-1.1

//! Fold a committed `CalvinReadResult` or `CalvinReadTimeout` entry into the
//! barrier log of its txn on its target vShard.
//!
//! The event lands in the txn's stored row first (see
//! [`crate::control::cluster::calvin::scheduler::barrier_store`]), then in
//! the vShard's read-result buffer, which wakes the scheduler. An event that
//! applies before this node granted the txn, or before a scheduler for the
//! vShard runs here, waits in the buffer or its row: none is dropped. An
//! event for a position the vShard's applied ledger holds is dropped: the
//! txn finished, and no barrier of it opens again.
//!
//! The entry counts as durably applied once its row holds the event. Boot
//! resumes Raft delivery above the group's durable applied floor, so the
//! row is what a barrier reads after a restart. A row write that fails
//! leaves the event in memory alone, and the entry breaks the applied
//! prefix: a restart delivers it again.

use crate::control::array_sync::raft_apply::AppliedPosition;
use crate::control::cluster::calvin::BarrierEvent;
use crate::control::cluster::calvin::scheduler::barrier_store;
use crate::control::cluster::calvin::scheduler::lock_manager::TxnId;
use crate::control::distributed_applier::propose_tracker::AppliedWrite;

use super::context::ApplyContext;
use super::proposal_gate::EntryOutcome;

/// Fields extracted from a `ReplicatedWrite::CalvinReadResult` entry.
pub(super) struct CalvinReadResultFields<'a> {
    pub target_vshard: u32,
    pub epoch: u64,
    pub position: u32,
    pub passive_vshard: u32,
    pub values: &'a [u8],
}

/// Decode `fields.values` and fold the read result into the barrier log of
/// its txn on `fields.target_vshard`, then complete the propose waiter.
pub(super) fn forward_calvin_read_result(
    ctx: ApplyContext<'_>,
    pos: AppliedPosition,
    fields: CalvinReadResultFields<'_>,
) -> EntryOutcome {
    let values: Vec<(
        nodedb_physical::physical_plan::meta::PassiveReadKeyId,
        nodedb_types::Value,
    )> = match zerompk::from_msgpack(fields.values) {
        Ok(decoded) => decoded,
        Err(e) => {
            // Every replica decodes the same bytes and refuses them alike,
            // so each barrier of the txn lacks this result and waits for
            // the log's timeout entry. A restart refuses them again, so the
            // refusal holds nothing to keep.
            tracing::warn!(
                group_id = pos.group_id,
                index = pos.log_index,
                error = %e,
                "failed to decode CalvinReadResult payload"
            );
            ctx.tracker.complete(
                pos.group_id,
                pos.log_index,
                pos.applied_key,
                Err(crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("CalvinReadResult payload: {e}"),
                }),
            );
            return EntryOutcome::Applied {
                durable: true,
                result: None,
            };
        }
    };
    fold_barrier_event(
        ctx,
        pos,
        fields.target_vshard,
        TxnId::new(fields.epoch, fields.position),
        BarrierEvent::Read {
            passive_vshard: fields.passive_vshard,
            values,
        },
    )
}

/// Fold a `CalvinReadTimeout` entry for txn `(epoch, position)` into its
/// barrier log on `target_vshard`, then complete the propose waiter.
pub(super) fn forward_calvin_read_timeout(
    ctx: ApplyContext<'_>,
    pos: AppliedPosition,
    target_vshard: u32,
    epoch: u64,
    position: u32,
) -> EntryOutcome {
    fold_barrier_event(
        ctx,
        pos,
        target_vshard,
        TxnId::new(epoch, position),
        BarrierEvent::Timeout,
    )
}

/// Fold `event` for `txn` into its stored row and `target_vshard`'s buffer,
/// unless the txn finished on this replica, and complete the entry's
/// propose waiter.
fn fold_barrier_event(
    ctx: ApplyContext<'_>,
    pos: AppliedPosition,
    target_vshard: u32,
    txn: TxnId,
    event: BarrierEvent,
) -> EntryOutcome {
    if matches!(event, BarrierEvent::Read { .. }) {
        ctx.state
            .calvin
            .read_results
            .vshard(target_vshard)
            .note_read_applied(pos.group_id, pos.log_index);
    }
    let saved = fold_unless_finished(ctx, target_vshard, txn, event);
    tracing::debug!(
        node_id = ctx.state.node_id,
        group_id = pos.group_id,
        index = pos.log_index,
        vshard_id = target_vshard,
        epoch = txn.epoch,
        position = txn.position,
        saved,
        "calvin: barrier entry applied"
    );
    // The event reached this replica's barrier either way: the waiter
    // learns the entry committed.
    ctx.tracker.complete(
        pos.group_id,
        pos.log_index,
        pos.applied_key,
        Ok(AppliedWrite::unversioned(Vec::new())),
    );
    EntryOutcome::Applied {
        durable: saved,
        result: None,
    }
}

/// Fold `event` of `txn` into its stored row and `vshard_id`'s buffer,
/// unless the txn finished on this replica. Returns whether the row holds
/// the event, or needs none.
fn fold_unless_finished(
    ctx: ApplyContext<'_>,
    vshard_id: u32,
    txn: TxnId,
    event: BarrierEvent,
) -> bool {
    if is_finished(ctx, vshard_id, txn) {
        return true;
    }
    let saved = save_and_buffer(ctx, vshard_id, txn, event);
    // The scheduler marks the ledger before it drops a finished txn's
    // entries. A finish between the check above and the save saw no entry
    // to drop, so this drops the one just saved.
    if is_finished(ctx, vshard_id, txn) {
        drop_finished(ctx, vshard_id, txn);
    }
    saved
}

/// Fold the barrier event of `entry`, at `log_index` of `group_id`, ahead
/// of its turn in the group's lane. Entries that are not barrier entries
/// and payloads that do not decode change nothing.
///
/// An earlier entry of the group can wait for this node's metadata apply,
/// and every later entry waits behind it. A barrier event names no
/// collection and needs no catalog, so it folds while the lane waits. Its
/// entry still concludes in log order, and that conclusion folds it again:
/// a second fold of an event the log already holds changes nothing. The
/// waiter, the applied counter and the durable prefix move only at the
/// conclusion.
///
/// A barrier decision never runs ahead of the lane: a leader stages only
/// once its group applied its term's first entry.
pub(super) fn prefold_barrier_entry(
    ctx: ApplyContext<'_>,
    group_id: u64,
    log_index: u64,
    entry: &crate::control::wal_replication::ReplicatedEntry,
) {
    use crate::control::wal_replication::ReplicatedWrite;
    let (txn, event) = match &entry.write {
        ReplicatedWrite::CalvinReadResult {
            epoch,
            position,
            passive_vshard,
            values,
            ..
        } => {
            let Ok(values) = zerompk::from_msgpack(values) else {
                // The conclusion reports the payload.
                return;
            };
            (
                TxnId::new(*epoch, *position),
                BarrierEvent::Read {
                    passive_vshard: *passive_vshard,
                    values,
                },
            )
        }
        ReplicatedWrite::CalvinReadTimeout {
            epoch, position, ..
        } => (TxnId::new(*epoch, *position), BarrierEvent::Timeout),
        _ => return,
    };
    let saved = fold_unless_finished(ctx, entry.vshard_id, txn, event);
    tracing::debug!(
        node_id = ctx.state.node_id,
        group_id,
        index = log_index,
        vshard_id = entry.vshard_id,
        epoch = txn.epoch,
        position = txn.position,
        saved,
        "calvin: barrier entry folded ahead of its lane"
    );
}

/// Whether `txn` finished on `vshard_id` here: its position is applied.
fn is_finished(ctx: ApplyContext<'_>, vshard_id: u32, txn: TxnId) -> bool {
    ctx.state
        .calvin
        .applied
        .get(vshard_id)
        .is_some_and(|ledger| ledger.is_applied(txn.epoch, txn.position))
}

/// Drop the buffered entries and the row of `txn` on `vshard_id`, which
/// finished. A failed remove leaves a row no barrier reads, and the
/// scheduler's stall-tick sweep removes it.
fn drop_finished(ctx: ApplyContext<'_>, vshard_id: u32, txn: TxnId) {
    ctx.state
        .calvin
        .read_results
        .vshard(vshard_id)
        .forget_txn(txn);
    if let Err(e) = barrier_store::remove_log(ctx.state.credentials.catalog(), vshard_id, txn) {
        tracing::warn!(
            vshard_id,
            epoch = txn.epoch,
            position = txn.position,
            error = %e,
            "calvin: the barrier row of a finished txn was not removed"
        );
        crate::diag::calvin_barrier_log_store_failed(
            vshard_id,
            Some((txn.epoch, txn.position)),
            "remove",
            &e,
        );
    }
}

/// Save `event` in the row of `txn` on `vshard_id`, then hand it to the
/// vShard's buffer. Returns whether the row holds it.
///
/// A txn with an event its row lacks skips the row: the row stays a prefix
/// of the txn's events, so a restart that delivers the missing entry again
/// folds them in log order.
fn save_and_buffer(ctx: ApplyContext<'_>, vshard_id: u32, txn: TxnId, event: BarrierEvent) -> bool {
    let buffer = ctx.state.calvin.read_results.vshard(vshard_id);
    let saved = !buffer.is_unsaved(txn)
        && match barrier_store::save_event(ctx.state.credentials.catalog(), vshard_id, txn, &event)
        {
            Ok(()) => true,
            Err(e) => {
                tracing::error!(
                    vshard_id,
                    epoch = txn.epoch,
                    position = txn.position,
                    error = %e,
                    "calvin: the barrier event's row write failed; the event stays in memory"
                );
                crate::diag::calvin_barrier_log_store_failed(
                    vshard_id,
                    Some((txn.epoch, txn.position)),
                    "save",
                    &e,
                );
                false
            }
        };
    buffer.push(
        txn,
        event,
        ctx.state.tuning.calvin.max_inflight_backlog,
        saved,
    );
    saved
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nodedb_physical::physical_plan::meta::PassiveReadKeyId;
    use nodedb_types::{QualifiedCollection, Value};

    use super::*;
    use crate::bridge::dispatch::Dispatcher;
    use crate::control::distributed_applier::propose_tracker::ProposeTracker;
    use crate::control::state::SharedState;
    use crate::wal::WalManager;

    const VSHARD: u32 = 6;

    fn pos(log_index: u64) -> AppliedPosition {
        AppliedPosition {
            group_id: 2,
            log_index,
            applied_key: 0,
            commit_hlc: 0,
        }
    }

    fn values() -> Vec<u8> {
        let values = vec![(
            PassiveReadKeyId::kv(
                QualifiedCollection::from_stored("items".to_owned()),
                b"k".to_vec(),
            ),
            Value::Bytes(b"v".to_vec()),
        )];
        zerompk::to_msgpack_vec(&values).expect("encode values")
    }

    fn state() -> (Arc<SharedState>, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = Arc::new(WalManager::open_for_testing(&dir.path().join("wal")).expect("wal"));
        let (dispatcher, _sides) = Dispatcher::new(1, 64);
        (
            SharedState::new(dispatcher, wal).expect("shared state"),
            dir,
        )
    }

    /// A read result for a txn no scheduler here holds waits in the buffer.
    #[test]
    fn a_result_before_the_grant_waits_in_the_buffer() {
        let (state, _dir) = state();
        let tracker = Arc::new(ProposeTracker::new());
        let ctx = ApplyContext {
            state: &state,
            tracker: &tracker,
        };
        let bytes = values();
        let outcome = forward_calvin_read_result(
            ctx,
            pos(10),
            CalvinReadResultFields {
                target_vshard: VSHARD,
                epoch: 4,
                position: 0,
                passive_vshard: 9,
                values: &bytes,
            },
        );
        assert!(matches!(
            outcome,
            EntryOutcome::Applied {
                durable: true,
                result: None
            }
        ));
        forward_calvin_read_timeout(ctx, pos(11), VSHARD, 4, 0);
        assert_eq!(
            state.calvin.read_results.vshard(VSHARD).read_entries(),
            vec![(2, 10)],
            "the buffer names the read result's entry, not the timeout's"
        );
        assert_eq!(state.calvin.read_results.waiting_txns(VSHARD), 1);
        let events = state
            .calvin
            .read_results
            .vshard(VSHARD)
            .take(TxnId::new(4, 0))
            .expect("the buffered log");
        assert!(!events.stored);
        assert!(events.memory.missing(&[9].into_iter().collect()).is_empty());
        // The row holds both events: a restart reads them from it.
        let stored = barrier_store::load_log(state.credentials.catalog(), VSHARD, TxnId::new(4, 0))
            .expect("load the row");
        assert_eq!(stored, events.memory);
    }

    /// A barrier entry folds ahead of its turn while its lane waits, and
    /// its conclusion folds it again with no change. Only the conclusion
    /// counts the entry and completes its waiter.
    #[test]
    fn a_prefolded_entry_concludes_without_a_second_change() {
        let (state, _dir) = state();
        let tracker = Arc::new(ProposeTracker::new());
        let ctx = ApplyContext {
            state: &state,
            tracker: &tracker,
        };
        let bytes = values();
        let entry = crate::control::wal_replication::ReplicatedEntry::new(
            1,
            0,
            VSHARD,
            crate::control::wal_replication::ReplicatedWrite::CalvinReadResult {
                epoch: 5,
                position: 0,
                passive_vshard: 9,
                tenant_id: 1,
                values: bytes.clone(),
            },
        );
        prefold_barrier_entry(ctx, 2, 12, &entry);
        let buffer = state.calvin.read_results.vshard(VSHARD);
        assert_eq!(buffer.waiting_txns(), 1, "the event waits before its turn");
        assert!(buffer.read_entries().is_empty(), "a prefold counts nothing");
        let stored = barrier_store::load_log(state.credentials.catalog(), VSHARD, TxnId::new(5, 0))
            .expect("load the row");
        assert!(stored.missing(&[9].into_iter().collect()).is_empty());

        let outcome = forward_calvin_read_result(
            ctx,
            pos(12),
            CalvinReadResultFields {
                target_vshard: VSHARD,
                epoch: 5,
                position: 0,
                passive_vshard: 9,
                values: &bytes,
            },
        );
        assert!(matches!(
            outcome,
            EntryOutcome::Applied {
                durable: true,
                result: None
            }
        ));
        assert_eq!(buffer.read_entries(), vec![(2, 12)]);
        assert_eq!(
            barrier_store::load_log(state.credentials.catalog(), VSHARD, TxnId::new(5, 0))
                .expect("load the row"),
            stored
        );
        let events = buffer.take(TxnId::new(5, 0)).expect("buffered events");
        assert_eq!(events.memory, stored);
    }

    /// An event for a position the vShard's ledger holds is dropped: the
    /// txn finished.
    #[test]
    fn an_event_of_a_finished_txn_is_dropped() {
        let (state, _dir) = state();
        state
            .calvin
            .applied
            .get_or_create(VSHARD)
            .mark_terminal(4, 0);
        let tracker = Arc::new(ProposeTracker::new());
        let ctx = ApplyContext {
            state: &state,
            tracker: &tracker,
        };
        forward_calvin_read_timeout(ctx, pos(11), VSHARD, 4, 0);
        assert_eq!(state.calvin.read_results.waiting_txns(VSHARD), 0);
        assert!(
            state
                .credentials
                .catalog()
                .load_calvin_barrier_log(VSHARD, 4, 0)
                .expect("load")
                .is_none(),
            "a finished txn's event writes no row"
        );
    }
}
