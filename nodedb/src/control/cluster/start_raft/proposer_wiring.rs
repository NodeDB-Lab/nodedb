// SPDX-License-Identifier: BUSL-1.1

//! Phase 4 of `start_raft`: install the sync/async Raft proposer, compactor,
//! and durable applied-index closures onto `SharedState`, and spawn the
//! background apply loop that drains `DistributedApplier::apply_committed`
//! into the Data Plane and notifies propose waiters.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use tokio::sync::mpsc::{self, Sender};

use crate::control::cluster::calvin::ReadResultEvent;
use crate::control::distributed_applier::{ApplyBatch, ProposeTracker, run_apply_loop};
use crate::control::state::SharedState;

use super::loop_build::RaftLoopType;

/// Install the sync `raft_proposer` / `raft_compactor` /
/// `raft_applied_index_sink`, the async `async_raft_proposer`, and spawn the
/// apply loop.
pub(super) fn wire_proposers(
    shared: &Arc<SharedState>,
    raft_loop: &Arc<RaftLoopType>,
    tracker: Arc<ProposeTracker>,
    apply_rx: mpsc::Receiver<ApplyBatch>,
    calvin_read_result_senders: Arc<Mutex<BTreeMap<u32, Sender<ReadResultEvent>>>>,
    sequencer_state_machine: Arc<Mutex<nodedb_cluster::calvin::SequencerStateMachine>>,
) -> crate::Result<()> {
    // Wire the Raft proposer into SharedState so CP dispatch paths
    // (pgwire, HTTP, array inbound) can route writes through Raft.
    // Hold `raft_loop` weakly: `SharedState` owns this closure, and the
    // closure must NOT keep `raft_loop` alive or the two form a strong
    // reference cycle that pins `SharedState` forever. During normal
    // operation the loop's spawned tasks keep it alive so `upgrade`
    // always succeeds; `None` only occurs once those tasks have stopped
    // on shutdown, where a clean "cluster not running" error is correct.
    let raft_loop_for_propose = Arc::downgrade(raft_loop);
    let proposer: Arc<crate::control::wal_replication::RaftProposer> =
        Arc::new(move |vshard_id, data| {
            let rl = raft_loop_for_propose
                .upgrade()
                .ok_or_else(|| crate::Error::Internal {
                    detail: "raft propose: cluster not running".into(),
                })?;
            rl.propose(vshard_id, data)
                .map_err(|e| crate::Error::Internal {
                    detail: format!("raft propose: {e}"),
                })
        });
    if shared.raft_proposer.set(proposer).is_err() {
        tracing::warn!("raft_proposer already set — start_raft appears to have run twice");
    }

    // Wire the Raft log-compaction trigger. `run_apply_loop` invokes this
    // after a committed entry has been durably applied to the Data Plane,
    // so compaction is gated on the data-plane applied watermark — never
    // raft's commit index. A no-op for groups whose
    // `log_compaction_threshold` is `None`.
    // Weak for the same cycle-breaking reason as `raft_proposer` above.
    let raft_loop_for_compact = Arc::downgrade(raft_loop);
    let sm_for_compact = Arc::clone(&sequencer_state_machine);
    let compactor: Arc<crate::control::wal_replication::RaftCompactor> =
        Arc::new(move |group_id, applied_index| {
            let rl = raft_loop_for_compact
                .upgrade()
                .ok_or_else(|| crate::Error::Internal {
                    detail: "raft log compaction: cluster not running".into(),
                })?;

            // Sequencer-group hold-down. Unlike a data group — whose entries are
            // replayable from each replica's own durable state — a cross-shard
            // Calvin txn is re-derived on every replica ONLY from the sequencer
            // log. A scheduler that missed a fan-out (channel full/closed, or it
            // had not subscribed yet) recovers by replaying that log from its
            // armed catch-up index. Compacting past an armed index destroys the
            // only copy, permanently losing the txn on that replica — which for a
            // cross-shard graph edge means the edge silently vanishes from that
            // node's index. Floor the compaction boundary strictly below the
            // lowest armed catch-up so the replay range always survives.
            let effective_index = if group_id == nodedb_cluster::calvin::SEQUENCER_GROUP_ID {
                match sm_for_compact
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .min_catch_up_from()
                {
                    // Keep index `m` itself: compaction discards entries at and
                    // below its boundary, and the replay range starts AT `m`.
                    Some(m) => applied_index.min(m.saturating_sub(1)),
                    None => applied_index,
                }
            } else {
                applied_index
            };
            if effective_index == 0 {
                // Nothing compactable once held down.
                return Ok(false);
            }

            rl.maybe_compact_group(group_id, effective_index)
                .map_err(|e| crate::Error::Internal {
                    detail: format!("raft log compaction: {e}"),
                })
        });
    if shared.raft_compactor.set(compactor).is_err() {
        tracing::warn!("raft_compactor already set — start_raft appears to have run twice");
    }

    // Wire the durable applied-index sink. `run_apply_loop` invokes this for
    // each committed entry once the write funnel's durable-at-ack barrier has
    // fsynced that entry's redo record, so the next boot resumes Raft delivery
    // above it — without this floor the whole retained log is re-delivered on
    // every boot and WAL replay applies the same entries a second time.
    // Weak for the same cycle-breaking reason as `raft_proposer` above.
    let raft_loop_for_applied = Arc::downgrade(raft_loop);
    let applied_index_sink: Arc<crate::control::wal_replication::RaftAppliedIndexSink> =
        Arc::new(move |group_id, applied_index| {
            let rl = raft_loop_for_applied
                .upgrade()
                .ok_or_else(|| crate::Error::Internal {
                    detail: "raft applied index: cluster not running".into(),
                })?;
            rl.save_applied_index(group_id, applied_index)
                .map_err(|e| crate::Error::Internal {
                    detail: format!("raft applied index: {e}"),
                })
        });
    if shared
        .raft_applied_index_sink
        .set(applied_index_sink)
        .is_err()
    {
        tracing::warn!(
            "raft_applied_index_sink already set — start_raft appears to have run twice"
        );
    }

    // Install the async proposer with transparent leader forwarding.
    //
    // Proposes via the data group leader (forwarding to a remote leader if
    // needed), then registers a ProposeTracker waiter and awaits apply.
    //
    // The ProposeTracker is race-safe: if `run_apply_loop` calls complete()
    // before register() is called (possible on fast clusters where the entry
    // commits and applies on this node before the proposer returns), the
    // result is stored and register() picks it up immediately with no timeout.
    // Weak for the same cycle-breaking reason as `raft_proposer` above.
    let raft_loop_async = Arc::downgrade(raft_loop);
    let tracker_for_proposer = tracker.clone();
    // Held weakly for the same cycle-breaking reason as `raft_proposer` above:
    // the proposer lives on `SharedState`.
    let state_for_proposer = Arc::downgrade(shared);
    let async_proposer: Arc<crate::control::wal_replication::AsyncRaftProposer> =
        Arc::new(move |vshard_id, idempotency_key, data, deadline| {
            let rl_weak = raft_loop_async.clone();
            let tk = tracker_for_proposer.clone();
            let state_weak = state_for_proposer.clone();
            Box::pin(async move {
                let rl = rl_weak.upgrade().ok_or_else(|| crate::Error::Internal {
                    detail: "raft propose (async): cluster not running".into(),
                })?;
                // The attempt gets only what remains of the caller's deadline.
                if tokio::time::Instant::now() >= deadline {
                    return Err(propose_deadline_exceeded());
                }
                let (group_id, log_index) =
                    tokio::time::timeout_at(deadline, rl.propose_via_data_leader(vshard_id, data))
                        .await
                        .map_err(|_| propose_deadline_exceeded())?
                        .map_err(|e| async_propose_error(vshard_id, e))?;

                // Register the waiter with the proposer's idempotency
                // key. The apply path compares against the committed
                // entry's key so a leader-change overwrite at the same
                // (group_id, log_index) — by either an empty no-op or a
                // different proposer's real entry — surfaces as
                // `RetryableLeaderChange` instead of leaking a
                // not-our-payload back to the caller.
                let rx = tk.register(group_id, log_index, idempotency_key);
                let applied = await_local_apply(LocalApplyWait {
                    state: &state_weak,
                    tracker: &tk,
                    group_id,
                    log_index,
                    vshard_id,
                    deadline,
                    rx,
                })
                .await
                // Preserve `RetryableLeaderChange` so the gateway
                // retry loop can re-propose against the new leader
                // — wrapping it in `Dispatch` would hide the
                // retryable signal and surface as silent INSERT
                // success. Only machinery failures stay wrapped for
                // diagnostics; a classified apply verdict keeps its
                // client-visible classification.
                .map_err(|e| {
                    if crate::error_classify::is_unclassified_failure(&e) {
                        crate::Error::Dispatch {
                            detail: format!("apply error: {e}"),
                        }
                    } else {
                        e
                    }
                })
                // Carry out the write-version the APPLY side stamped, not
                // `log_index`. The tracker resolves on the node that applied
                // the entry locally, so `write_version` is this replica's own
                // post-write `coll_write_lsn` — a WAL LSN, the same domain
                // every other feed of that map records in, and the only
                // domain the shard-local OCC read validator compares in. The
                // raft log index is a per-group counter on a different scale
                // entirely; publishing it here made reads validate a WAL LSN
                // against a log index.
                .map(|applied| (applied.payload, applied.write_version));
                let applied = applied?;
                // A write to a vShard homing a permission-tree source is
                // acknowledged only once every lease holder covers it, or its
                // lease expired.
                if let Some(state) = state_weak.upgrade()
                    && state
                        .authorization_fence
                        .sources()
                        .is_source_vshard(vshard_id)
                {
                    crate::control::security::auth_lease::authorization_barrier(
                        &state,
                        vec![nodedb_cluster::GroupCoverage {
                            group_id,
                            through: log_index,
                        }],
                    )
                    .await?;
                }
                Ok(applied)
            })
        });
    crate::control::vshard_admission::install_async_raft_proposer(shared, async_proposer)?;

    // Spawn the background apply loop. It reads from the mpsc channel
    // pushed by `DistributedApplier::apply_committed`, dispatches to the
    // Data Plane, and notifies propose waiters. Registered via
    // `spawn_loop_no_abort` so the Control Plane drain waits for it to exit
    // (dropping its captured `Arc<SharedState>` deterministically) but NEVER
    // force-aborts it — an abort mid-apply would strand
    // committed-but-unapplied entries. It drains at `DrainingControlPlane`
    // because its applies dispatch to the Data Plane.
    let apply_state = shared.clone();
    let apply_tracker = tracker.clone();
    let apply_calvin_read_result_senders = Arc::clone(&calvin_read_result_senders);
    crate::control::shutdown::spawn_loop_no_abort(
        &shared.loop_registry,
        &shared.shutdown,
        "raft_apply_loop",
        crate::control::shutdown::ShutdownPhase::DrainingControlPlane,
        move |mut shutdown| async move {
            // `biased` polls `run_apply_loop` FIRST on every iteration: any
            // committed batch already queued in `apply_rx` is drained to the
            // Data Plane before the shutdown arm can win, so shutdown never
            // cuts the loop off mid-apply.
            tokio::select! {
                biased;
                _ = run_apply_loop(
                    apply_rx,
                    apply_state,
                    apply_tracker,
                    apply_calvin_read_result_senders,
                ) => {}
                _ = shutdown.wait_cancelled() => {}
            }
        },
    );
    Ok(())
}

/// Where a group's pipeline stands, for a propose waiter that timed out: the
/// Raft commit index, the index handed to the apply loop, and the index the
/// apply loop applied. The first of the three that stops short of the waited
/// index names the stage that stalled.
fn apply_progress(state: Option<&SharedState>, group_id: u64) -> String {
    let Some(state) = state else {
        return "node is shutting down".to_owned();
    };
    let status = state
        .raft_status_fn
        .get()
        .and_then(|status| status().into_iter().find(|g| g.group_id == group_id));
    let applied = state.applied_index_watcher(group_id).current();
    match status {
        Some(group) => format!(
            "commit_index={} handed_to_apply_loop={} applied={applied} role={} leader={}",
            group.commit_index, group.last_applied, group.role, group.leader_id
        ),
        None => format!("group not hosted here, applied={applied}"),
    }
}

/// The error an async propose that reached no leader returns.
///
/// A group with no leader to take the proposal right now accepts the same
/// proposal once it has one, so the proposal is retried:
/// [`crate::Error::NoLeader`]. That covers an election, a leadership transfer
/// in flight, and a leader that stepped down after this node or a forwarding
/// node chose it; a forwarded refusal arrives here with its typed Raft error
/// (`DataProposeResponse::refusal_error`). Every other failure is final here.
fn async_propose_error(vshard_id: u32, error: nodedb_cluster::ClusterError) -> crate::Error {
    match error {
        nodedb_cluster::ClusterError::Raft(
            nodedb_raft::RaftError::LeadershipTransferInProgress
            | nodedb_raft::RaftError::NotLeader { .. },
        ) => crate::Error::NoLeader {
            vshard_id: crate::types::VShardId::new(vshard_id),
        },
        other => crate::Error::Internal {
            detail: format!("raft propose (async): {other}"),
        },
    }
}

/// The error a proposal returns once the caller's statement deadline passed.
///
/// The proposer carries no request id, so the error names request 0.
fn propose_deadline_exceeded() -> crate::Error {
    crate::Error::DeadlineExceeded {
        request_id: crate::types::RequestId::new(0),
    }
}

/// How often a waiting proposer checks that this node still replicates the
/// entry's group.
const MEMBERSHIP_CHECK: std::time::Duration = std::time::Duration::from_millis(100);

/// One proposer's wait for this node's apply of its entry.
struct LocalApplyWait<'a> {
    state: &'a std::sync::Weak<SharedState>,
    tracker: &'a ProposeTracker,
    group_id: u64,
    log_index: u64,
    vshard_id: u32,
    deadline: tokio::time::Instant,
    rx: tokio::sync::oneshot::Receiver<crate::control::distributed_applier::ProposeResult>,
}

/// Wait until this node applied the proposer's entry, and return what the
/// apply produced.
///
/// The wait ends early when this node leaves the entry's group: a removed
/// replica receives no further entries, so it never applies the index. That
/// ends as [`crate::Error::NotLeader`] naming no leader, which sends the
/// caller to the group's current members.
///
/// `deadline` is the caller's statement deadline, shared by every attempt.
/// Once it passes, the wait ends as [`crate::Error::DeadlineExceeded`]. The
/// pipeline stage that stalled goes to the log first.
async fn await_local_apply(
    wait: LocalApplyWait<'_>,
) -> crate::Result<crate::control::distributed_applier::AppliedWrite> {
    let LocalApplyWait {
        state,
        tracker,
        group_id,
        log_index,
        vshard_id,
        deadline,
        mut rx,
    } = wait;
    loop {
        let check = tokio::time::sleep(
            MEMBERSHIP_CHECK.min(deadline.saturating_duration_since(tokio::time::Instant::now())),
        );
        tokio::select! {
            received = &mut rx => {
                return received.map_err(|_| crate::Error::Dispatch {
                    detail: "propose waiter channel closed".into(),
                })?;
            }
            _ = check => {}
        }
        let state = state.upgrade();
        if let Some(state) = state.as_deref()
            && !crate::control::security::auth_fence::cluster::hosts_group(state, group_id)
        {
            tracker.abandon(group_id, log_index);
            return Err(crate::Error::NotLeader {
                vshard_id: crate::types::VShardId::new(vshard_id),
                leader_node: 0,
                leader_addr: format!(
                    "this node left raft group {group_id} before it applied index {log_index}"
                ),
            });
        }
        if tokio::time::Instant::now() >= deadline {
            tracker.abandon(group_id, log_index);
            tracing::warn!(
                group_id,
                log_index,
                progress = %apply_progress(state.as_deref(), group_id),
                oldest_unfinished = %tracker
                    .applying(group_id)
                    .map_or_else(|| "nothing".to_owned(), |entry| entry.to_string()),
                "raft proposal reached the statement deadline before this node applied it"
            );
            return Err(propose_deadline_exceeded());
        }
    }
}
