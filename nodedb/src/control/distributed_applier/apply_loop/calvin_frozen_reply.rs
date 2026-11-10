// SPDX-License-Identifier: BUSL-1.1

//! Store a committed `CalvinFrozenReply` entry as the frozen `RETURNING`
//! reply of its txn on its vShard.
//!
//! The row is the entry's durable effect: the entry extends the applied
//! prefix once the row commits. The first stored copy of a txn's reply
//! stays, so a later copy changes nothing. A reply of a txn this replica
//! finished is spent, and is not stored. A row write that fails breaks the
//! applied prefix: a restart delivers the entry again.
//!
//! The store never asks whether this node still replicates the vShard. The
//! routing table can lag the group's own log, so that check could skip a
//! row a member needs. A row stored after its vShard left goes on the next
//! scheduler reconcile pass (`left_vshard_forget`).

use crate::control::array_sync::raft_apply::AppliedPosition;
use crate::control::cluster::calvin::scheduler::frozen_replies;
use crate::control::cluster::calvin::scheduler::lock_manager::TxnId;
use crate::control::distributed_applier::propose_tracker::AppliedWrite;
use crate::control::wal_replication::types::CalvinFrozenReply;

use super::context::ApplyContext;
use super::proposal_gate::EntryOutcome;

/// Store `reply`, the entry at `pos` of `target_vshard`'s data group, then
/// wake the vShard's scheduler and complete the propose waiter.
pub(super) fn apply_calvin_frozen_reply(
    ctx: ApplyContext<'_>,
    pos: AppliedPosition,
    target_vshard: u32,
    reply: &CalvinFrozenReply,
) -> EntryOutcome {
    let txn = TxnId::new(reply.epoch, reply.position);
    if reply.vshard != target_vshard {
        // Every replica reads the same entry and refuses it alike. The
        // leader votes only once a reply of its own vShard is stored.
        tracing::warn!(
            group_id = pos.group_id,
            index = pos.log_index,
            target_vshard,
            reply_vshard = reply.vshard,
            "calvin: a frozen reply names another vShard than its entry; nothing is stored"
        );
        ctx.tracker.complete(
            pos.group_id,
            pos.log_index,
            pos.applied_key,
            Err(crate::Error::Internal {
                detail: format!(
                    "calvin frozen reply of vShard {} proposed to vShard {target_vshard}",
                    reply.vshard
                ),
            }),
        );
        return EntryOutcome::Applied {
            durable: true,
            result: None,
        };
    }
    let finished = ctx
        .state
        .calvin
        .applied
        .get(target_vshard)
        .is_some_and(|ledger| ledger.is_applied(txn.epoch, txn.position));
    let durable = finished || store(ctx, pos, target_vshard, txn, reply);
    ctx.tracker.complete(
        pos.group_id,
        pos.log_index,
        pos.applied_key,
        Ok(AppliedWrite::new(Vec::new())),
    );
    EntryOutcome::Applied {
        durable,
        result: None,
    }
}

/// Store `reply` and wake its scheduler. Returns whether the row holds a
/// copy of the txn's reply.
fn store(
    ctx: ApplyContext<'_>,
    pos: AppliedPosition,
    vshard_id: u32,
    txn: TxnId,
    reply: &CalvinFrozenReply,
) -> bool {
    match frozen_replies::store(ctx.state.credentials.catalog(), reply) {
        Ok(stored) => {
            tracing::debug!(
                node_id = ctx.state.node_id,
                group_id = pos.group_id,
                index = pos.log_index,
                vshard_id,
                epoch = txn.epoch,
                position = txn.position,
                stored,
                "calvin: frozen RETURNING reply applied"
            );
            ctx.state.calvin.frozen_replies.vshard(vshard_id).wake();
            true
        }
        Err(e) => {
            tracing::error!(
                vshard_id,
                epoch = txn.epoch,
                position = txn.position,
                error = %e,
                "calvin: the frozen reply's row write failed; a restart applies the entry again"
            );
            crate::diag::calvin_frozen_reply_store_failed(
                vshard_id,
                Some((txn.epoch, txn.position)),
                "store",
                &e,
            );
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

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
            system_ordinal: None,
        }
    }

    fn reply(epoch: u64, payload: u8) -> CalvinFrozenReply {
        CalvinFrozenReply {
            epoch,
            position: 0,
            vshard: VSHARD,
            plans: vec![crate::control::wal_replication::types::FrozenPlanReply {
                plan_index: 0,
                payload: vec![payload],
                column_types: Vec::new(),
                sequence_effects: Vec::new(),
                rejected: None,
            }],
        }
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

    /// The first copy of a txn's reply is stored. A later copy changes
    /// nothing, both conclude durable, and the vShard's scheduler is woken.
    #[tokio::test]
    async fn the_first_copy_is_stored_and_a_later_one_changes_nothing() {
        let (state, _dir) = state();
        let tracker = Arc::new(ProposeTracker::new());
        let ctx = ApplyContext {
            state: &state,
            tracker: &tracker,
        };
        for (index, payload) in [(10, 1), (11, 2)] {
            let outcome = apply_calvin_frozen_reply(ctx, pos(index), VSHARD, &reply(4, payload));
            assert!(matches!(
                outcome,
                EntryOutcome::Applied {
                    durable: true,
                    result: None
                }
            ));
        }
        let stored = frozen_replies::load(state.credentials.catalog(), VSHARD, TxnId::new(4, 0))
            .expect("load")
            .expect("a stored reply");
        assert_eq!(stored, reply(4, 1));
        // The wake leaves a permit, so a wait that starts now returns.
        let woken = state.calvin.frozen_replies.vshard(VSHARD);
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(5), woken.stored())
                .await
                .is_ok(),
            "the store wakes the scheduler"
        );
    }

    /// A reply of a txn this replica finished is spent.
    #[test]
    fn a_reply_of_a_finished_txn_is_not_stored() {
        let (state, _dir) = state();
        state
            .calvin
            .applied
            .get_or_create(VSHARD)
            .mark_terminal(5, 0);
        let tracker = Arc::new(ProposeTracker::new());
        let ctx = ApplyContext {
            state: &state,
            tracker: &tracker,
        };
        apply_calvin_frozen_reply(ctx, pos(12), VSHARD, &reply(5, 1));
        assert!(
            !frozen_replies::holds(state.credentials.catalog(), VSHARD, TxnId::new(5, 0))
                .expect("holds")
        );
    }
}
