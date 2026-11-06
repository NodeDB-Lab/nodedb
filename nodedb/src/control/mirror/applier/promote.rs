// SPDX-License-Identifier: BUSL-1.1

//! `ALTER DATABASE ... PROMOTE`: end a mirror at its last ready cut.
//!
//! Three steps, each in a log, so a crash between two leaves a state any later
//! metadata leader finishes:
//!
//! 1. `Seal` in the metadata log. Apply fixes `Promoting { final_cut }` at the
//!    row's `ready_cut`. Log order puts every `MirrorCut` before or after it, so
//!    the final cut is always the ready cut, and each later cut changes nothing.
//!    The applier reads `Promoting` and stops.
//! 2. A `last` seal in every mirror group. It proceeds past the cut verdict, so
//!    it needs no live epoch. Each entry ahead of it concludes on every replica:
//!    a cut at or below the final cut applies, and any other skips.
//! 3. `Promote` in the metadata log, once every group applied its seal. Apply
//!    flips the row to `Promoted`, and writes open.
//!
//! The steps repeat freely: `Seal` and `Promote` change nothing on a row past
//! them, and a second seal records the cut the first one did.

use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::future::join_all;
use nodedb_cluster::METADATA_GROUP_ID;
use nodedb_cluster::calvin::SEQUENCER_GROUP_ID;
use nodedb_types::{DatabaseId, MirrorEntryStamp, MirrorStatus, MirrorStatusStep};

use crate::control::catalog_entry::CatalogEntry;
use crate::control::metadata_proposer::propose_catalog_entry_async;
use crate::control::security::auth_fence::cluster::routed_groups;
use crate::control::shutdown::{ShutdownPhase, spawn_loop};
use crate::control::state::SharedState;
use crate::control::wal_replication::{ReplicatedEntry, ReplicatedWrite};

use super::cut::seal_vshard;
use super::progress::{PendingCut, every_group_applied};
use super::propose::{GroupProposal, MirrorRaft, SealAt, propose_group};
use super::rewrite::rewrite_entry;

/// The gate each node parks at before it finishes a sealed PROMOTE.
pub const PROMOTE_FINISH_FAIL_POINT: &str = "mirror_promote::before_finish";

/// How long one group gets to commit its last seal.
const SEAL_COMMIT_TIMEOUT: Duration = Duration::from_secs(30);
/// How long every group gets to apply its last seal.
const SEAL_APPLY_TIMEOUT: Duration = Duration::from_secs(120);
/// How long a wait sleeps between two looks at the groups.
const SEAL_POLL: Duration = Duration::from_millis(50);
/// How often the metadata leader looks for a PROMOTE to finish.
const FINISH_INTERVAL: Duration = Duration::from_secs(1);

async fn propose_step(
    state: &SharedState,
    database: DatabaseId,
    step: MirrorStatusStep,
) -> crate::Result<MirrorStatus> {
    propose_catalog_entry_async(
        state,
        &CatalogEntry::StepMirrorStatus {
            database_id: database.as_u64(),
            step,
        },
    )
    .await?;
    status_of(state, database)
}

fn status_of(state: &SharedState, database: DatabaseId) -> crate::Result<MirrorStatus> {
    state
        .credentials
        .catalog()
        .get_database(database)?
        .and_then(|descriptor| descriptor.mirror_origin)
        .map(|origin| origin.status)
        .ok_or_else(|| crate::Error::Internal {
            detail: format!(
                "PROMOTE: database {} is no mirror; it was dropped during the statement",
                database.as_u64()
            ),
        })
}

/// Fix the mirror's last cut, and answer it. A mirror PROMOTE already sealed
/// answers the cut it holds.
pub async fn seal_mirror(state: &SharedState, database: DatabaseId) -> crate::Result<u64> {
    let status = propose_step(state, database, MirrorStatusStep::Seal).await?;
    match status {
        MirrorStatus::Promoting { final_cut } => Ok(final_cut),
        MirrorStatus::Promoted => {
            let cut = state.credentials.catalog().get_mirror_cut_state(database)?;
            Ok(cut.map_or(0, |cut| cut.ready_cut))
        }
        // A failed mirror with no ready cut reaches no first cut on its own.
        MirrorStatus::Failed { failure } => Err(no_ready_cut(
            database,
            format!(
                "stopped on: {failure}; run ALTER DATABASE ... MIRROR REBOOTSTRAP, or DROP DATABASE"
            ),
        )),
        MirrorStatus::Bootstrapping { .. }
        | MirrorStatus::Following
        | MirrorStatus::Degraded
        | MirrorStatus::Disconnected => Err(no_ready_cut(
            database,
            format!("holds status {status:?}; wait for its first cut, or DROP DATABASE"),
        )),
    }
}

/// The refusal of a PROMOTE the mirror's state cannot serve, as `55000`.
fn no_ready_cut(database: DatabaseId, next_action: String) -> crate::Error {
    crate::Error::ObjectNotInPrerequisiteState {
        object: format!("mirror database {}", database.as_u64()),
        detail: format!(
            "mirror database {} has no ready cut to end at: it {next_action}",
            database.as_u64()
        ),
    }
}

/// Finish a PROMOTE the row shows sealed: seal every group, wait for each to
/// apply, and open writes. A mirror already promoted answers at once.
pub async fn finish_promote(state: &SharedState, database: DatabaseId) -> crate::Result<()> {
    // Holds a sealed mirror sealed, so a test can restart the cluster with the
    // row at `Promoting`.
    #[cfg(feature = "failpoints")]
    if !crate::control::fail_gate::before_promote_finish(
        state.node_id,
        state.shutdown.raw_receiver(),
    )
    .await
    {
        return Err(crate::Error::Internal {
            detail: "PROMOTE: this node shut down before it finished; a later leader finishes"
                .to_string(),
        });
    }
    let final_cut = match status_of(state, database)? {
        MirrorStatus::Promoting { final_cut } => final_cut,
        MirrorStatus::Promoted => return Ok(()),
        status @ (MirrorStatus::Bootstrapping { .. }
        | MirrorStatus::Following
        | MirrorStatus::Degraded
        | MirrorStatus::Disconnected
        | MirrorStatus::Failed { .. }) => {
            return Err(crate::Error::Internal {
                detail: format!(
                    "PROMOTE: database {} is not sealed, and holds status {status:?}; run \
                     ALTER DATABASE PROMOTE again",
                    database.as_u64()
                ),
            });
        }
    };
    let raft = state
        .mirror
        .raft
        .get()
        .map(Arc::clone)
        .ok_or_else(|| crate::Error::Internal {
            detail: "PROMOTE: this node's Raft has not started; retry once it serves".to_string(),
        })?;
    let seals = seal_groups(state, raft.as_ref(), database, final_cut).await?;
    await_seals_applied(state, raft.as_ref(), database, final_cut, seals).await?;
    propose_step(state, database, MirrorStatusStep::Promote).await?;
    Ok(())
}

/// Propose and commit one `last` seal in every mirror group.
async fn seal_groups(
    state: &SharedState,
    raft: &dyn MirrorRaft,
    database: DatabaseId,
    final_cut: u64,
) -> crate::Result<Vec<SealAt>> {
    // A replica waits for this floor before it applies the seal, so it reads
    // the entries ahead of the seal with the final cut in hand.
    let metadata_floor = state.applied_index_watcher(METADATA_GROUP_ID).floor();
    let fence_epoch = state
        .credentials
        .catalog()
        .get_mirror_cut_state(database)?
        .map_or(0, |cut| cut.fence_epoch);
    let mut proposals = Vec::new();
    for group in routed_groups(state)
        .into_iter()
        .filter(|group| *group != SEQUENCER_GROUP_ID && *group != METADATA_GROUP_ID)
    {
        let vshard = seal_vshard(state, group)?;
        let mut seal = ReplicatedEntry::new(
            0,
            database.as_u64(),
            vshard,
            ReplicatedWrite::MirrorSeal {
                last: true,
                calvin_floors: Vec::new(),
            },
        );
        rewrite_entry(
            &mut seal,
            MirrorEntryStamp {
                source_group: 0,
                source_index: 0,
                cut: final_cut,
                epoch: fence_epoch,
            },
            metadata_floor,
        );
        proposals.push(GroupProposal {
            group,
            entries: vec![(vshard, seal.encode()?)],
        });
    }
    let deadline = tokio::time::Instant::now() + SEAL_COMMIT_TIMEOUT;
    join_all(
        proposals
            .into_iter()
            .map(|proposal| propose_group(raft, proposal, deadline)),
    )
    .await
    .into_iter()
    .collect()
}

async fn await_seals_applied(
    state: &SharedState,
    raft: &dyn MirrorRaft,
    database: DatabaseId,
    final_cut: u64,
    seals: Vec<SealAt>,
) -> crate::Result<()> {
    let pending = PendingCut {
        cut: final_cut,
        hlc_ns: 0,
        cap_ordinal: 0,
        seals,
    };
    let deadline = Instant::now() + SEAL_APPLY_TIMEOUT;
    while !every_group_applied(state, raft, database, &pending).await {
        if Instant::now() >= deadline {
            return Err(crate::Error::Dispatch {
                detail: format!(
                    "PROMOTE: database {} sealed at cut {final_cut}, and a group did not apply \
                     the seal within {SEAL_APPLY_TIMEOUT:?}; the metadata leader keeps finishing \
                     it",
                    database.as_u64()
                ),
            });
        }
        tokio::time::sleep(SEAL_POLL).await;
    }
    Ok(())
}

/// One pass: finish every PROMOTE a former leader left. Only the metadata
/// leader acts.
async fn finish_pass(state: &SharedState) {
    if !state.is_singleton_worker() || state.mirror.raft.get().is_none() {
        return;
    }
    let databases = match state.credentials.catalog().list_databases() {
        Ok(databases) => databases,
        Err(error) => {
            tracing::warn!(%error, "mirror PROMOTE: the catalog did not read");
            return;
        }
    };
    let sealed = databases.into_iter().filter_map(|descriptor| {
        let origin = descriptor.mirror_origin?;
        matches!(origin.status, MirrorStatus::Promoting { .. }).then_some(descriptor.id)
    });
    let finished = join_all(
        sealed.map(|database| async move { (database, finish_promote(state, database).await) }),
    )
    .await;
    for (database, outcome) in finished {
        if let Err(error) = outcome {
            tracing::warn!(
                database = database.as_u64(),
                %error,
                "mirror PROMOTE: a pass did not finish; the next pass retries"
            );
        }
    }
}

/// Run the pass that lets any later leader finish a PROMOTE.
pub(super) fn spawn_promote_finisher(shared: &Arc<SharedState>) {
    let state = Arc::clone(shared);
    spawn_loop(
        &shared.loop_registry,
        &shared.shutdown,
        "mirror_promote_finisher",
        ShutdownPhase::DrainingControlPlane,
        move |mut shutdown| async move {
            let mut tick = tokio::time::interval(FINISH_INTERVAL);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = shutdown.wait_cancelled() => break,
                    _ = tick.tick() => {}
                }
                if shutdown.is_cancelled() {
                    break;
                }
                finish_pass(&state).await;
            }
        },
    );
}
