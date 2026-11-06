// SPDX-License-Identifier: BUSL-1.1

//! Forget the stored Calvin state of the vShards this node left.
//!
//! A vShard that left keeps no applied positions, base or dependent-read
//! barrier rows here. A forget that fails leaves the vShard in
//! `CalvinLocalState::forget_pending`, and every scheduler reconcile pass
//! retries it. Boot adds every vShard that holds any stored Calvin state.
//!
//! A pending vShard whose group lists this node as a voter or learner
//! leaves the set with no forget: its state belongs to that membership.
//!
//! No pass forgets before boot recovery sets
//! `SharedState::data_groups_replayed`. Until every mounted data group
//! applied its committed log, a group can still re-apply a conf change, so
//! the routing table can omit a group this node replicates.

use std::collections::BTreeSet;
use std::sync::RwLock;

use nodedb_cluster::RoutingTable;

use crate::control::cluster::calvin::scheduler::{NOT_YET_APPLIED_EPOCH, barrier_store};
use crate::control::cluster::calvin_snapshot;
use crate::control::state::SharedState;

/// Add to the pending set every vShard with stored Calvin state at boot:
/// `holding_rows`, the vShards with barrier rows, and every vShard with an
/// applied ledger or a base. Boot runs this once both are loaded.
pub fn seed_at_boot(shared: &SharedState, holding_rows: BTreeSet<u32>) {
    let mut seeded = holding_rows;
    seeded.extend(
        shared
            .calvin
            .applied
            .snapshot_all()
            .into_iter()
            // A reset ledger holds no applied position.
            .filter(|state| {
                state.fully_applied_epoch != NOT_YET_APPLIED_EPOCH || !state.tail.is_empty()
            })
            .map(|state| state.vshard_id),
    );
    seeded.extend(shared.calvin.bases.vshards());
    shared
        .calvin
        .forget_pending
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .extend(seeded);
}

/// The vShards of every Raft group that lists `node_id` as a voter or a
/// learner, sorted. A learner applies the group's entries, so it holds the
/// group's Calvin state too.
pub fn replicated_vshards(routing: &RwLock<RoutingTable>, node_id: u64) -> Vec<u32> {
    let routing = routing.read().unwrap_or_else(|p| p.into_inner());
    let mut vshards = Vec::new();
    for (group_id, info) in routing.group_members() {
        if info.members.contains(&node_id) || info.learners.contains(&node_id) {
            vshards.extend(routing.vshards_for_group(*group_id));
        }
    }
    vshards.sort_unstable();
    vshards.dedup();
    vshards
}

/// Whether the routing table names every group this node replicates: boot
/// recovery saw every mounted data group apply its committed log.
fn membership_settled(shared: &SharedState) -> bool {
    shared
        .data_groups_replayed
        .load(std::sync::atomic::Ordering::Acquire)
}

/// Forget the stored Calvin state of `left`, the vShards this node no
/// longer hosts. Each vShard whose forget fails waits in the pending set.
pub fn forget_left_vshards(shared: &SharedState, left: &[u32]) {
    if left.is_empty() {
        return;
    }
    let state_forgotten = calvin_snapshot::forget_left(shared, left).is_ok();
    let mut failed = Vec::new();
    for &vshard_id in left {
        let rows_forgotten = forget_rows(shared, vshard_id);
        if !state_forgotten || !rows_forgotten {
            failed.push(vshard_id);
        }
    }
    if failed.is_empty() {
        return;
    }
    shared
        .calvin
        .forget_pending
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .extend(failed);
}

/// Retry the forget of every pending vShard. `replicated` is sorted: the
/// vShards of every group that lists this node as a voter or learner. A
/// replicated vShard leaves the set with no forget, and a forgotten one
/// leaves it. Before membership settles, the pass changes nothing.
pub fn retry_pending_forgets(shared: &SharedState, replicated: &[u32]) {
    if !membership_settled(shared) {
        return;
    }
    let pending: Vec<u32> = shared
        .calvin
        .forget_pending
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .iter()
        .copied()
        .collect();
    if pending.is_empty() {
        return;
    }
    let mut settled = Vec::new();
    for vshard_id in pending {
        if replicated.binary_search(&vshard_id).is_ok() {
            settled.push(vshard_id);
            continue;
        }
        // Both forgets are idempotent, so a retry runs both.
        let state_forgotten = calvin_snapshot::forget_left(shared, &[vshard_id]).is_ok();
        let rows_forgotten = forget_rows(shared, vshard_id);
        if state_forgotten && rows_forgotten {
            tracing::info!(
                vshard_id,
                "calvin: the stored state of a vShard that left is forgotten"
            );
            settled.push(vshard_id);
        }
    }
    let mut set = shared
        .calvin
        .forget_pending
        .lock()
        .unwrap_or_else(|p| p.into_inner());
    for vshard_id in settled {
        set.remove(&vshard_id);
    }
}

/// Remove every barrier row and the buffer of `vshard_id`. Returns whether
/// the rows went.
fn forget_rows(shared: &SharedState, vshard_id: u32) -> bool {
    match barrier_store::forget_vshard(shared, vshard_id) {
        Ok(()) => true,
        Err(e) => {
            tracing::warn!(
                vshard_id,
                error = %e,
                "calvin: the barrier rows of a vShard that left were not removed; the next \
                 reconcile pass retries"
            );
            crate::diag::calvin_barrier_log_store_failed(Some(vshard_id), None, "forget", &e);
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::Arc;

    use super::*;
    use crate::bridge::dispatch::Dispatcher;
    use crate::control::cluster::calvin::BarrierEvent;
    use crate::control::cluster::calvin::scheduler::lock_manager::TxnId;
    use crate::wal::WalManager;

    const VSHARD: u32 = 5;
    const NODE: u64 = 1;
    const OTHER: u64 = 2;

    /// Routing whose one data group holds every vShard, with `voters`.
    fn routing(voters: &[u64]) -> RwLock<RoutingTable> {
        RwLock::new(RoutingTable::uniform(1, voters, 1))
    }

    /// A state whose boot recovery has not run.
    fn booting_state() -> (Arc<SharedState>, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = Arc::new(WalManager::open_for_testing(&dir.path().join("wal")).expect("wal"));
        let (dispatcher, _sides) = Dispatcher::new(1, 64);
        (
            SharedState::new(dispatcher, wal).expect("shared state"),
            dir,
        )
    }

    /// A state whose boot recovery finished.
    fn state() -> (Arc<SharedState>, tempfile::TempDir) {
        let (state, dir) = booting_state();
        state
            .data_groups_replayed
            .store(true, std::sync::atomic::Ordering::Release);
        (state, dir)
    }

    fn save_row(state: &SharedState, vshard_id: u32) {
        barrier_store::save_event(
            state.credentials.catalog(),
            vshard_id,
            TxnId::new(3, 0),
            &BarrierEvent::Timeout,
        )
        .expect("save the row");
    }

    fn row_vshards(state: &SharedState) -> BTreeSet<u32> {
        state
            .credentials
            .catalog()
            .load_calvin_barrier_logs(None)
            .expect("load rows")
            .into_iter()
            .map(|row| row.vshard_id)
            .collect()
    }

    fn pending(state: &SharedState) -> BTreeSet<u32> {
        state
            .calvin
            .forget_pending
            .lock()
            .expect("pending set")
            .clone()
    }

    /// A failed row removal keeps the vShard pending. The next pass that
    /// finds it unhosted removes its rows and drops it.
    #[test]
    fn a_failed_forget_retries_on_the_next_pass() {
        let (state, _dir) = state();
        save_row(&state, VSHARD);
        save_row(&state, VSHARD + 1);

        state
            .credentials
            .catalog()
            .fail_next_calvin_barrier_replace_for_test();
        forget_left_vshards(&state, &[VSHARD]);
        assert_eq!(pending(&state), BTreeSet::from([VSHARD]));
        assert_eq!(row_vshards(&state), BTreeSet::from([VSHARD, VSHARD + 1]));

        retry_pending_forgets(&state, &[VSHARD + 1]);
        assert!(pending(&state).is_empty());
        assert_eq!(
            row_vshards(&state),
            BTreeSet::from([VSHARD + 1]),
            "the hosted vShard keeps its rows"
        );
    }

    /// A pending vShard hosted again leaves the set, and its rows stay.
    #[test]
    fn a_rehosted_vshard_leaves_the_set_without_a_purge() {
        let (state, _dir) = state();
        save_row(&state, VSHARD);
        state
            .credentials
            .catalog()
            .fail_next_calvin_barrier_replace_for_test();
        forget_left_vshards(&state, &[VSHARD]);
        assert_eq!(pending(&state), BTreeSet::from([VSHARD]));

        retry_pending_forgets(&state, &[VSHARD]);
        assert!(pending(&state).is_empty());
        assert_eq!(row_vshards(&state), BTreeSet::from([VSHARD]));
    }

    /// Boot names every vShard that holds rows. The first pass removes the
    /// rows of the vShards this node does not host and keeps the rest.
    #[test]
    fn boot_rows_of_an_unhosted_vshard_go_on_the_first_pass() {
        let (state, _dir) = state();
        save_row(&state, VSHARD);
        save_row(&state, VSHARD + 1);

        let holding = barrier_store::restore_at_boot(&state).expect("restore");
        assert_eq!(holding, BTreeSet::from([VSHARD, VSHARD + 1]));
        state
            .calvin
            .forget_pending
            .lock()
            .expect("pending set")
            .extend(holding);

        retry_pending_forgets(&state, &[VSHARD + 1]);
        assert!(pending(&state).is_empty());
        assert_eq!(row_vshards(&state), BTreeSet::from([VSHARD + 1]));
        assert_eq!(
            state.calvin.read_results.waiting_txns(VSHARD),
            0,
            "the unhosted vShard's buffer goes with its rows"
        );
        assert_eq!(state.calvin.read_results.waiting_txns(VSHARD + 1), 1);
    }

    /// A pass that runs before boot recovery forgets nothing, even when its
    /// routing omits a group this node replicates. Recovery waits for the
    /// group's leader. The first pass after it keeps the state of every
    /// replicated vShard.
    #[tokio::test]
    async fn an_early_pass_never_purges_a_replicated_vshard() {
        let (state, _dir) = booting_state();
        let leader = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let status_leader = Arc::clone(&leader);
        let status_fn: Arc<dyn Fn() -> Vec<nodedb_cluster::GroupStatus> + Send + Sync> =
            Arc::new(move || {
                vec![nodedb_cluster::GroupStatus {
                    group_id: 1,
                    role: "Leader".into(),
                    leader_id: status_leader.load(std::sync::atomic::Ordering::Acquire),
                    term: 1,
                    commit_index: 0,
                    last_applied: 0,
                    last_log_index: 0,
                    snapshot_index: 0,
                    retained_bytes: 0,
                    replica_state: nodedb_raft::ReplicaState::Whole,
                    member_count: 1,
                    learner_count: 0,
                    vshard_count: 1,
                }]
            });
        assert!(state.raft_status_fn.set(status_fn).is_ok());

        save_row(&state, VSHARD);
        state.calvin.applied.install(VSHARD, 2, BTreeSet::new());
        let holding = barrier_store::restore_at_boot(&state).expect("restore");
        seed_at_boot(&state, holding);
        assert_eq!(pending(&state), BTreeSet::from([VSHARD]));

        // The routing omits this node: membership has not settled.
        retry_pending_forgets(&state, &replicated_vshards(&routing(&[OTHER]), NODE));
        assert_eq!(pending(&state), BTreeSet::from([VSHARD]));
        assert_eq!(row_vshards(&state), BTreeSet::from([VSHARD]));
        assert!(state.calvin.applied.get(VSHARD).is_some());

        // The group elects, and boot recovery finishes.
        leader.store(NODE, std::sync::atomic::Ordering::Release);
        crate::bootstrap::data_group_recovery::await_data_group_recovery(
            &state,
            nodedb_types::config::tuning::DataGroupRecoveryTimeout(std::time::Duration::from_secs(
                60,
            )),
        )
        .await
        .expect("recovery");
        assert!(
            state
                .data_groups_replayed
                .load(std::sync::atomic::Ordering::Acquire)
        );
        retry_pending_forgets(&state, &replicated_vshards(&routing(&[NODE]), NODE));
        assert!(pending(&state).is_empty());
        assert_eq!(row_vshards(&state), BTreeSet::from([VSHARD]));
        assert!(state.calvin.applied.get(VSHARD).is_some());
    }

    /// A learner replicates its group, so its vShards count as replicated.
    #[test]
    fn a_learner_replicates_its_groups_vshards() {
        let mut table = RoutingTable::uniform(1, &[OTHER], 1);
        table.set_group_learners(1, vec![NODE]);
        assert!(
            replicated_vshards(&RwLock::new(table), NODE)
                .binary_search(&VSHARD)
                .is_ok()
        );
        assert!(replicated_vshards(&routing(&[OTHER]), NODE).is_empty());
    }

    /// An unreplicated vShard with only an applied ledger and a base is
    /// forgotten on the first pass after the data groups replay.
    #[test]
    fn an_unreplicated_ledger_and_base_are_forgotten() {
        let (state, _dir) = state();
        let catalog = state.credentials.catalog();
        state.calvin.applied.install(VSHARD, 4, BTreeSet::new());
        state
            .calvin
            .bases
            .record_kept(catalog, &[(VSHARD, 1)])
            .expect("record the base");
        seed_at_boot(&state, BTreeSet::new());
        assert_eq!(pending(&state), BTreeSet::from([VSHARD]));

        retry_pending_forgets(&state, &replicated_vshards(&routing(&[OTHER]), NODE));
        assert!(pending(&state).is_empty());
        assert!(state.calvin.applied.get(VSHARD).is_none());
        assert!(state.calvin.bases.base(VSHARD).is_none());
        assert!(
            catalog
                .load_calvin_bases()
                .expect("load bases")
                .iter()
                .all(|(vshard_id, _)| *vshard_id != VSHARD)
        );
        assert!(
            catalog
                .load_all_calvin_applied()
                .expect("load ledgers")
                .iter()
                .filter(|stored| stored.vshard_id == VSHARD)
                .all(|stored| {
                    stored.fully_applied_epoch == NOT_YET_APPLIED_EPOCH && stored.tail.is_empty()
                }),
            "a stored ledger of the vShard is a reset one"
        );
    }
}
