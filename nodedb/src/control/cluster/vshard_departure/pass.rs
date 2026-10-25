// SPDX-License-Identifier: BUSL-1.1

//! One departure pass: record vShards that left, purge their data here, and
//! drop the rows of purges no replay can undo.
//!
//! 1. Ledger, under the `MultiRaft` lock. A vShard that left gets a
//!    departed row, and its data group owes a snapshot install here, both
//!    durable before any purge. A remount of the group resumes its old log,
//!    whose applied index the purged data no longer matches: the debt makes
//!    the remount catch up by snapshot instead. A group that mounts
//!    meanwhile keeps its vShards resident, so none of them departs.
//! 2. After a boot, every purged row whose barrier the WAL still holds
//!    records below is owed again: replay re-applied edge, label and array
//!    records of the purged vShards.
//! 3. Purge, under the departure fence ([`super::fence`]). The fence, and
//!    the snapshot debt of the purged vShards' groups, are taken under the
//!    `MultiRaft` lock at the residency read. A group that mounts later
//!    owes a snapshot, and its install waits for the purge to end, so the
//!    purge removes no row an install wrote. A pass that finds such an
//!    install running purges nothing. Then the barrier is fsynced and every
//!    core purges. A vShard that became resident meanwhile is hosted again.
//! 4. Settle. A purged row goes once the WAL holds no record below its
//!    barrier.
//!
//! A step that fails records a faultbox capture and leaves its rows as they
//! were. The next pass, or the first pass after a restart, retries it. No
//! pass runs before boot recovery sets `SharedState::data_groups_replayed`:
//! until every mounted group applied its committed log, the routing table
//! can omit a group this node replicates.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::Mutex;

use nodedb_cluster::multi_raft::MultiRaft;

use crate::control::security::catalog::vshard_residency::{ResidencyChange, VShardResidency};
use crate::control::state::SharedState;

use super::ledger;
use super::purge::{
    append_purge_barrier, departed_collections, purge_on_every_core, wal_oldest_lsn,
};
use super::resident::ResidencyView;

/// The departure pass state of this process.
#[derive(Debug, Default)]
pub struct DeparturePass {
    /// Whether this process ran step 2 since it booted.
    boot_checked: bool,
}

impl DeparturePass {
    /// Run one pass on node `node_id`, whose groups `multi_raft` mounts.
    pub async fn run(&mut self, shared: &SharedState, node_id: u64, multi_raft: &Mutex<MultiRaft>) {
        if !shared
            .data_groups_replayed
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return;
        }
        if let Err((step, vshards, error)) = record_departures(shared, node_id, multi_raft) {
            report(step, &vshards, &error);
            return;
        }
        if !self.boot_checked {
            match reopen_replayed(shared) {
                Ok(()) => self.boot_checked = true,
                Err(error) => {
                    report("settle", &[], &error);
                    return;
                }
            }
        }
        if let Err((step, vshards, error)) = purge_pending(shared, node_id, multi_raft).await {
            report(step, &vshards, &error);
            return;
        }
        if let Err(error) = settle(shared) {
            report("settle", &[], &error);
        }
    }
}

/// A failed step: its name, the vShards it ran for, and its error.
type StepError = (&'static str, Vec<u32>, Box<crate::Error>);

fn report(step: &'static str, vshards: &[u32], error: &crate::Error) {
    tracing::warn!(
        step,
        ?vshards,
        %error,
        "vshard departure: a purge step failed; the next pass retries it"
    );
    crate::diag::vshard_departure_purge_failed(step, vshards, error);
}

/// The residency of this node now.
fn residency_now(node_id: u64, multi_raft: &MultiRaft) -> ResidencyView {
    let mounted: HashSet<u64> = multi_raft.group_ids().into_iter().collect();
    let routing = multi_raft.routing();
    let routing = routing.read().unwrap_or_else(|p| p.into_inner());
    ResidencyView::of(node_id, &mounted, &routing)
}

fn load_rows(shared: &SharedState) -> crate::Result<BTreeMap<u32, VShardResidency>> {
    Ok(shared
        .credentials
        .catalog()
        .load_vshard_residency()?
        .into_iter()
        .collect())
}

/// Step 1. The `MultiRaft` lock is held throughout, so no group mounts
/// between the residency read and the debt write.
fn record_departures(
    shared: &SharedState,
    node_id: u64,
    multi_raft: &Mutex<MultiRaft>,
) -> Result<(), StepError> {
    let catalog = shared.credentials.catalog();
    let multi_raft = multi_raft.lock().unwrap_or_else(|p| p.into_inner());
    let residency = residency_now(node_id, &multi_raft);
    let rows = load_rows(shared).map_err(|e| ("ledger", Vec::new(), Box::new(e)))?;
    let plan = ledger::reconcile(&rows, &residency);
    if !plan.departed.is_empty() {
        shared
            .redo_chunks
            .record_owed(catalog, &residency.groups_of(&plan.departed))
            .map_err(|e| ("snapshot_debt", plan.departed.clone(), Box::new(e)))?;
    }
    catalog
        .apply_vshard_residency(&plan.changes)
        .map_err(|e| ("ledger", plan.departed.clone(), Box::new(e)))?;
    drop(multi_raft);
    if !plan.departed.is_empty() {
        tracing::info!(
            node_id,
            departed = ?plan.departed,
            "vshard departure: vShards left this node; their data here is purged"
        );
    }
    if !plan.rehosted.is_empty() {
        tracing::info!(
            node_id,
            rehosted = ?plan.rehosted,
            "vshard departure: vShards are hosted here again; their pending purge is cancelled"
        );
    }
    Ok(())
}

/// Step 2: owe again every purge whose rows a boot replay re-applied.
fn reopen_replayed(shared: &SharedState) -> crate::Result<()> {
    let oldest = wal_oldest_lsn(&shared.wal)?;
    let changes: Vec<ResidencyChange> = load_rows(shared)?
        .into_iter()
        .filter(|(_, row)| row.departed && row.purged && row.barrier_lsn > oldest)
        .map(|(vshard_id, row)| {
            (
                vshard_id,
                Some(VShardResidency {
                    purged: false,
                    ..row
                }),
            )
        })
        .collect();
    shared
        .credentials
        .catalog()
        .apply_vshard_residency(&changes)
}

/// Step 3.
async fn purge_pending(
    shared: &SharedState,
    node_id: u64,
    multi_raft: &Mutex<MultiRaft>,
) -> Result<(), StepError> {
    let catalog = shared.credentials.catalog();
    let rows = load_rows(shared).map_err(|e| ("ledger", Vec::new(), Box::new(e)))?;
    let pending: BTreeMap<u32, VShardResidency> = rows
        .into_iter()
        .filter(|(_, row)| row.departed && !row.purged)
        .collect();
    if pending.is_empty() {
        return Ok(());
    }
    let departed: BTreeSet<u32> = pending.keys().copied().collect();
    let vshards: Vec<u32> = departed.iter().copied().collect();
    let (resident, fence) = {
        let multi_raft = multi_raft.lock().unwrap_or_else(|p| p.into_inner());
        let residency = residency_now(node_id, &multi_raft);
        // Step 1 hosted every resident vShard again, so none is pending
        // here. One that became resident since stays out of the purge.
        if departed.iter().any(|vshard_id| residency.holds(*vshard_id)) {
            return Ok(());
        }
        // An install of a vShard this purge removes runs: the next pass
        // retries.
        let Some(fence) = shared.departure_fence.try_purge(&residency.resident) else {
            return Ok(());
        };
        // The purge leaves the groups' data here behind their logs. An
        // install can settle the debt step 1 recorded, and the group can
        // leave again, before the purge runs, so the debt is recorded again.
        shared
            .redo_chunks
            .record_owed(catalog, &residency.groups_of(&vshards))
            .map_err(|e| ("snapshot_debt", vshards.clone(), Box::new(e)))?;
        (residency.resident, fence)
    };

    let collections = departed_collections(catalog, &departed)
        .map_err(|e| ("barrier", vshards.clone(), Box::new(e)))?;
    let barriered: BTreeMap<u32, VShardResidency> =
        if pending.values().any(|row| row.barrier_lsn == 0) {
            let barrier = append_purge_barrier(&shared.wal, &collections)
                .map_err(|e| ("barrier", vshards.clone(), Box::new(e)))?;
            let rows: BTreeMap<u32, VShardResidency> = pending
                .iter()
                .map(|(&vshard_id, row)| {
                    let barrier_lsn = if row.barrier_lsn == 0 {
                        barrier
                    } else {
                        row.barrier_lsn
                    };
                    (
                        vshard_id,
                        VShardResidency {
                            barrier_lsn,
                            ..*row
                        },
                    )
                })
                .collect();
            let changes: Vec<ResidencyChange> =
                rows.iter().map(|(&v, row)| (v, Some(*row))).collect();
            catalog
                .apply_vshard_residency(&changes)
                .map_err(|e| ("barrier", vshards.clone(), Box::new(e)))?;
            rows
        } else {
            pending
        };

    let purged = purge_on_every_core(shared, &departed, &resident, &collections)
        .await
        .map_err(|e| ("purge", vshards.clone(), Box::new(e)))?;
    // Every core answered: an install waiting on the fence can start.
    drop(fence);

    // A vShard resident again is hosted. Its group owes the snapshot debt
    // recorded above, which only an install after the purge settles.
    let multi_raft = multi_raft.lock().unwrap_or_else(|p| p.into_inner());
    let after = residency_now(node_id, &multi_raft);
    let changes: Vec<ResidencyChange> = barriered
        .iter()
        .map(|(&vshard_id, row)| {
            let row = if after.holds(vshard_id) {
                VShardResidency::HOSTED
            } else {
                VShardResidency {
                    purged: true,
                    ..*row
                }
            };
            (vshard_id, Some(row))
        })
        .collect();
    catalog
        .apply_vshard_residency(&changes)
        .map_err(|e| ("settle", vshards.clone(), Box::new(e)))?;
    drop(multi_raft);
    tracing::info!(
        node_id,
        departed = ?vshards,
        purged,
        "vshard departure: every core purged the data of vShards this node left"
    );
    Ok(())
}

/// Step 4.
fn settle(shared: &SharedState) -> crate::Result<()> {
    let oldest = wal_oldest_lsn(&shared.wal)?;
    let changes: Vec<ResidencyChange> = load_rows(shared)?
        .into_iter()
        .filter(|(_, row)| row.departed && row.purged && row.barrier_lsn <= oldest)
        .map(|(vshard_id, _)| (vshard_id, None))
        .collect();
    shared
        .credentials
        .catalog()
        .apply_vshard_residency(&changes)
}
