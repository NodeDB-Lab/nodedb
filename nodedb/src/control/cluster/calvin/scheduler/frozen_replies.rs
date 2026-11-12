// SPDX-License-Identifier: BUSL-1.1

//! The stored frozen `RETURNING` replies of each txn on each vShard.
//!
//! The data-group apply loop stores each `CalvinFrozenReply` entry in
//! `_system.calvin_frozen_replies` before the entry counts as applied, then
//! wakes the vShard's scheduler. The first stored copy of a txn's reply
//! stays, unless a later copy extends it with more plans.
//!
//! - The slice's leader votes commit only once its txn's row holds every
//!   local plan.
//! - The slice's install answers the row on every replica. No replica
//!   reads a leader's in-memory evaluation.
//! - The scheduler removes a txn's row once the txn finished, and
//!   [`sweep_finished`] removes on its stall tick each finished txn's row
//!   that removal missed.
//! - [`restore_at_boot`] removes the rows of every txn the applied ledgers
//!   mark finished, before the apply loop and the schedulers start.
//! - [`capture_group`] and [`install_group`] carry the rows of a data
//!   group's unfinished txns in its snapshot, for the entries the snapshot
//!   covers. An install keeps no row of a txn already finished here.
//! - [`stored_vshards`] names every vShard that holds rows. Each scheduler
//!   reconcile pass forgets those this node does not replicate, so a row
//!   the apply loop stores after its vShard left goes on the next pass.

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex};

use tokio::sync::Notify;

use super::applied_ledger::CalvinAppliedLedger;
use super::barrier_store::is_applied;
use super::lock::TxnId;
use crate::control::security::catalog::SystemCatalog;
use crate::control::security::catalog::calvin_frozen_replies::StoredFrozenReply;
use crate::control::state::SharedState;
use crate::control::wal_replication::types::CalvinFrozenReply;
use crate::types::CutFrozenReply;

/// Store `reply` when no copy of its txn's reply is stored, or when it
/// extends the stored copy (see [`CalvinFrozenReply::extended_by`]). Any
/// other copy writes nothing. Returns whether this call stored it. The row
/// is durable when this returns.
pub fn store(catalog: &SystemCatalog, reply: &CalvinFrozenReply) -> crate::Result<bool> {
    catalog.fold_calvin_frozen_reply(reply.vshard, reply.epoch, reply.position, |held| {
        let Some(held) = held else {
            return reply.to_bytes().map(Some);
        };
        CalvinFrozenReply::from_bytes(held)?
            .extended_by(reply)
            .map(|extended| extended.to_bytes())
            .transpose()
    })
}

/// The stored reply of `txn` on `vshard_id`, when one is stored.
pub fn load(
    catalog: &SystemCatalog,
    vshard_id: u32,
    txn: TxnId,
) -> crate::Result<Option<CalvinFrozenReply>> {
    catalog
        .load_calvin_frozen_reply(vshard_id, txn.epoch, txn.position)?
        .map(|bytes| CalvinFrozenReply::from_bytes(&bytes))
        .transpose()
}

/// Whether a reply of `txn` on `vshard_id` is stored.
pub fn holds(catalog: &SystemCatalog, vshard_id: u32, txn: TxnId) -> crate::Result<bool> {
    catalog.has_calvin_frozen_reply(vshard_id, txn.epoch, txn.position)
}

/// Remove the stored reply of `txn` on `vshard_id`, which finished.
pub fn remove(catalog: &SystemCatalog, vshard_id: u32, txn: TxnId) -> crate::Result<()> {
    catalog.remove_calvin_frozen_replies(&[(vshard_id, txn.epoch, txn.position)])
}

/// Remove every stored reply of `vshard_id` whose txn `ledger` marks
/// applied. Returns how many rows went.
pub fn sweep_finished(
    catalog: &SystemCatalog,
    vshard_id: u32,
    ledger: &CalvinAppliedLedger,
) -> crate::Result<usize> {
    let finished: Vec<(u32, u64, u32)> = catalog
        .calvin_frozen_reply_keys(vshard_id)?
        .into_iter()
        .filter(|(_, epoch, position)| ledger.is_applied(*epoch, *position))
        .collect();
    catalog.remove_calvin_frozen_replies(&finished)?;
    Ok(finished.len())
}

/// Remove the stored reply of every txn the applied ledgers mark finished.
/// Boot runs this once the applied ledgers are filled, before the
/// data-group apply loop and the schedulers start.
///
/// Returns the vShards that still hold rows. The first scheduler reconcile
/// forgets those this node does not replicate.
pub fn restore_at_boot(shared: &SharedState) -> crate::Result<BTreeSet<u32>> {
    let catalog = shared.credentials.catalog();
    let (finished, waiting): (Vec<_>, Vec<_>) = catalog
        .all_calvin_frozen_reply_keys()?
        .into_iter()
        .partition(|(vshard_id, epoch, position)| {
            is_applied(shared, *vshard_id, *epoch, *position)
        });
    catalog.remove_calvin_frozen_replies(&finished)?;
    Ok(waiting
        .into_iter()
        .map(|(vshard_id, _, _)| vshard_id)
        .collect())
}

/// Every vShard that holds a stored reply.
pub fn stored_vshards(catalog: &SystemCatalog) -> crate::Result<BTreeSet<u32>> {
    Ok(catalog
        .all_calvin_frozen_reply_keys()?
        .into_iter()
        .map(|(vshard_id, _, _)| vshard_id)
        .collect())
}

/// The stored replies of the unfinished txns of `vshards`, for a snapshot
/// of their data group. The caller fences the group's apply, so the rows
/// hold exactly the frozen entries at or below the snapshot's index.
pub fn capture_group(
    shared: &SharedState,
    vshards: &BTreeSet<u32>,
) -> crate::Result<Vec<CutFrozenReply>> {
    let rows = shared
        .credentials
        .catalog()
        .load_calvin_frozen_replies(vshards)?;
    Ok(rows
        .into_iter()
        .filter(|row| !is_applied(shared, row.vshard_id, row.epoch, row.position))
        .map(|row| CutFrozenReply {
            vshard_id: row.vshard_id,
            epoch: row.epoch,
            position: row.position,
            reply: row.reply,
        })
        .collect())
}

/// Install `replies`, the rows a snapshot of the data group of `vshards`
/// carries: they replace this node's rows of `vshards`. A row of a txn this
/// node's applied ledger marks finished is spent, and is not installed. The
/// caller installs the snapshot's ledgers first.
pub fn install_group(
    shared: &SharedState,
    vshards: &BTreeSet<u32>,
    replies: Vec<CutFrozenReply>,
) -> crate::Result<()> {
    let rows: Vec<StoredFrozenReply> = replies
        .into_iter()
        .filter(|reply| vshards.contains(&reply.vshard_id))
        .filter(|reply| !is_applied(shared, reply.vshard_id, reply.epoch, reply.position))
        .map(|reply| StoredFrozenReply {
            vshard_id: reply.vshard_id,
            epoch: reply.epoch,
            position: reply.position,
            reply: reply.reply,
        })
        .collect();
    shared
        .credentials
        .catalog()
        .replace_calvin_frozen_replies(vshards, &rows)?;
    for vshard_id in vshards {
        shared.calvin.frozen_replies.vshard(*vshard_id).wake();
    }
    Ok(())
}

/// Remove every stored reply of `vshard_id`, which left this node.
pub fn forget_vshard(shared: &SharedState, vshard_id: u32) -> crate::Result<()> {
    shared.calvin.frozen_replies.forget(vshard_id);
    shared
        .credentials
        .catalog()
        .replace_calvin_frozen_replies(&BTreeSet::from([vshard_id]), &[])
}

/// What one vShard's scheduler hears of its stored frozen replies. It
/// holds no record of which txns stored one: the catalog row is the only
/// record.
#[derive(Debug, Default)]
pub struct VShardFrozenReplies {
    /// Wakes the scheduler: a reply was stored.
    stored_wake: Notify,
}

impl VShardFrozenReplies {
    /// Wake the scheduler: replies changed.
    pub fn wake(&self) {
        self.stored_wake.notify_one();
    }

    /// Resolves once a reply was stored since the last wake.
    pub async fn stored(&self) {
        self.stored_wake.notified().await;
    }
}

/// The frozen-reply wakes of every vShard of this node.
#[derive(Debug, Default)]
pub struct CalvinFrozenReplies {
    by_vshard: Mutex<HashMap<u32, Arc<VShardFrozenReplies>>>,
}

impl CalvinFrozenReplies {
    /// The wakes of `vshard_id`, created on first use.
    pub fn vshard(&self, vshard_id: u32) -> Arc<VShardFrozenReplies> {
        let mut by_vshard = self.by_vshard.lock().unwrap_or_else(|p| p.into_inner());
        Arc::clone(by_vshard.entry(vshard_id).or_default())
    }

    /// Drop the wakes of `vshard_id`, which left this node.
    pub fn forget(&self, vshard_id: u32) {
        self.by_vshard
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&vshard_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::cluster::calvin::scheduler::NOT_YET_APPLIED_EPOCH;

    fn reply(epoch: u64) -> CalvinFrozenReply {
        CalvinFrozenReply {
            epoch,
            position: 0,
            vshard: 5,
            plans: Vec::new(),
        }
    }

    #[test]
    fn a_stored_reply_reloads_and_a_finished_one_is_swept() {
        let catalog = SystemCatalog::open_in_memory().expect("catalog");
        assert!(store(&catalog, &reply(3)).expect("store"));
        assert!(store(&catalog, &reply(4)).expect("store"));
        assert_eq!(
            load(&catalog, 5, TxnId::new(3, 0)).expect("load"),
            Some(reply(3))
        );
        assert!(holds(&catalog, 5, TxnId::new(4, 0)).expect("holds"));
        let ledger = CalvinAppliedLedger::new(NOT_YET_APPLIED_EPOCH, BTreeSet::new());
        ledger.mark_terminal(3, 0);
        assert_eq!(sweep_finished(&catalog, 5, &ledger).expect("sweep"), 1);
        assert!(!holds(&catalog, 5, TxnId::new(3, 0)).expect("holds"));
        assert!(holds(&catalog, 5, TxnId::new(4, 0)).expect("holds"));
        remove(&catalog, 5, TxnId::new(4, 0)).expect("remove");
        assert!(!holds(&catalog, 5, TxnId::new(4, 0)).expect("holds"));
    }

    fn state() -> (std::sync::Arc<SharedState>, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = std::sync::Arc::new(
            crate::wal::WalManager::open_for_testing(&dir.path().join("wal")).expect("wal"),
        );
        let (dispatcher, _sides) = crate::bridge::dispatch::Dispatcher::new(1, 64);
        (
            SharedState::new(dispatcher, wal).expect("shared state"),
            dir,
        )
    }

    fn at(vshard: u32, epoch: u64) -> CalvinFrozenReply {
        CalvinFrozenReply {
            epoch,
            position: 0,
            vshard,
            plans: Vec::new(),
        }
    }

    /// Boot removes the rows of finished txns before anything reads them,
    /// and names every vShard whose rows wait.
    #[test]
    fn boot_removes_finished_rows_and_names_the_waiting_vshards() {
        let (state, _dir) = state();
        let catalog = state.credentials.catalog();
        for reply in [at(5, 3), at(5, 4), at(6, 2)] {
            store(catalog, &reply).expect("store");
        }
        state.calvin.applied.install(5, 3, BTreeSet::new());
        state.calvin.applied.install(6, 2, BTreeSet::new());

        let waiting = restore_at_boot(&state).expect("restore");

        assert_eq!(waiting, BTreeSet::from([5]));
        assert!(!holds(catalog, 5, TxnId::new(3, 0)).expect("holds"));
        assert!(holds(catalog, 5, TxnId::new(4, 0)).expect("holds"));
        assert!(!holds(catalog, 6, TxnId::new(2, 0)).expect("holds"));
        assert_eq!(
            stored_vshards(catalog).expect("vshards"),
            BTreeSet::from([5])
        );
    }

    /// A snapshot install keeps no row of a txn this node finished, and
    /// replaces the rest of the group's rows.
    #[test]
    fn a_snapshot_install_drops_rows_of_finished_txns() {
        let (state, _dir) = state();
        let catalog = state.credentials.catalog();
        store(catalog, &at(5, 1)).expect("store");
        state.calvin.applied.install(5, 3, BTreeSet::new());
        let cut = |epoch: u64| CutFrozenReply {
            vshard_id: 5,
            epoch,
            position: 0,
            reply: at(5, epoch).to_bytes().expect("encode"),
        };

        install_group(&state, &BTreeSet::from([5]), vec![cut(2), cut(4)]).expect("install");

        assert!(!holds(catalog, 5, TxnId::new(1, 0)).expect("holds"));
        assert!(!holds(catalog, 5, TxnId::new(2, 0)).expect("holds"));
        assert!(holds(catalog, 5, TxnId::new(4, 0)).expect("holds"));
    }
}
