//! Single tick of the Raft event loop.
//!
//! Ordering (each phase uses a short-lived `MultiRaft` lock acquisition —
//! the lock is never held across an `.await`):
//!
//! 1. **Tick Raft groups**: drive election timeouts / heartbeats and pull
//!    `MultiRaftReady` output (messages, vote requests, committed
//!    entries, snapshot-needed peers).
//! 2. **Dispatch AppendEntries**: one background task per peer, batching
//!    all messages targeting the same peer.
//! 3. **Dispatch RequestVote**: same batching strategy.
//! 4. **Apply committed entries**: feed them to the user-supplied
//!    `CommitApplier`. Conf-change entries are detected and applied to
//!    `MultiRaft` before the user applier sees them.
//! 5. **Install snapshots**: send `InstallSnapshot` RPCs to peers that
//!    have fallen behind the leader's snapshot boundary.
//! 6. **Promote caught-up learners**: for every group where this node is
//!    leader, query learners whose `match_index >= commit_index` and
//!    propose `PromoteLearner` for each. Idempotent by design — after
//!    the first promotion the peer has moved from `learners` to
//!    `members` and won't be returned again.

use std::collections::HashMap as BatchMap;

use tracing::{debug, warn};

use nodedb_raft::transport::RaftTransport;

use crate::conf_change::{ConfChange, ConfChangeType};
use crate::forward::PlanExecutor;

use super::loop_core::{CommitApplier, RaftLoop};

impl<A: CommitApplier, P: PlanExecutor> RaftLoop<A, P> {
    /// Execute a single tick: drive Raft, dispatch outbound messages,
    /// apply commits, promote caught-up learners.
    pub(super) fn do_tick(&self) {
        // Phase 1: tick under lock, extract Ready.
        let ready = {
            let mut mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
            mr.tick()
        };

        // Phase 2+3: dispatch messages first (even if ready looks "empty"
        // we still want to run the learner-promotion phase each tick so a
        // just-caught-up learner is promoted promptly).
        if !ready.is_empty() {
            let mut ae_batches: BatchMap<u64, Vec<(u64, nodedb_raft::AppendEntriesRequest)>> =
                BatchMap::new();
            let mut vote_batches: BatchMap<u64, Vec<(u64, nodedb_raft::RequestVoteRequest)>> =
                BatchMap::new();

            for (group_id, group_ready) in &ready.groups {
                for (peer, req) in &group_ready.messages {
                    ae_batches
                        .entry(*peer)
                        .or_default()
                        .push((*group_id, req.clone()));
                }
                for (peer, req) in &group_ready.vote_requests {
                    vote_batches
                        .entry(*peer)
                        .or_default()
                        .push((*group_id, req.clone()));
                }
            }

            // Dispatch batched AppendEntries — one task per peer.
            //
            // Each detached task subscribes to the shutdown watch
            // and wraps its RPC awaits in `tokio::select!` so a
            // `RaftLoop::begin_shutdown` signal (or the `run` loop
            // propagating an external shutdown) cancels the
            // in-flight QUIC call at the next await point. This
            // is what lets graceful shutdown drop the
            // `Arc<Mutex<MultiRaft>>` clone promptly and release
            // per-group redb locks for an in-process restart.
            for (peer, messages) in ae_batches {
                let transport = self.transport.clone();
                let mr = self.multi_raft.clone();
                let mut shutdown_rx = self.shutdown_watch.subscribe();
                tokio::spawn(async move {
                    if *shutdown_rx.borrow() {
                        return;
                    }
                    for (group_id, req) in messages {
                        tokio::select! {
                            biased;
                            _ = shutdown_rx.changed() => return,
                            rpc = transport.append_entries(peer, req) => {
                                match rpc {
                                    Ok(resp) => {
                                        let mut mr =
                                            mr.lock().unwrap_or_else(|p| p.into_inner());
                                        if let Err(e) = mr
                                            .handle_append_entries_response(group_id, peer, &resp)
                                        {
                                            debug!(group_id, peer, error = %e, "handle ae response");
                                        }
                                    }
                                    Err(e) => {
                                        warn!(group_id, peer, error = %e, "append_entries RPC failed");
                                        break; // Peer is down — skip remaining groups.
                                    }
                                }
                            }
                        }
                    }
                });
            }

            // Dispatch batched RequestVote — one task per peer.
            for (peer, votes) in vote_batches {
                let transport = self.transport.clone();
                let mr = self.multi_raft.clone();
                let mut shutdown_rx = self.shutdown_watch.subscribe();
                tokio::spawn(async move {
                    if *shutdown_rx.borrow() {
                        return;
                    }
                    for (group_id, req) in votes {
                        tokio::select! {
                            biased;
                            _ = shutdown_rx.changed() => return,
                            rpc = transport.request_vote(peer, req) => {
                                match rpc {
                                    Ok(resp) => {
                                        let mut mr =
                                            mr.lock().unwrap_or_else(|p| p.into_inner());
                                        if let Err(e) = mr
                                            .handle_request_vote_response(group_id, peer, &resp)
                                        {
                                            debug!(group_id, peer, error = %e, "handle vote response");
                                        }
                                    }
                                    Err(e) => {
                                        warn!(group_id, peer, error = %e, "request_vote RPC failed");
                                        break;
                                    }
                                }
                            }
                        }
                    }
                });
            }

            // Phase 4: apply committed entries and conf-changes.
            for (group_id, group_ready) in ready.groups {
                if !group_ready.committed_entries.is_empty() {
                    for entry in &group_ready.committed_entries {
                        if let Some(cc) = ConfChange::from_entry_data(&entry.data) {
                            let mut mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
                            if let Err(e) = mr.apply_conf_change(group_id, &cc) {
                                warn!(group_id, error = %e, "failed to apply conf change");
                            }
                        }
                    }

                    let last_applied = if group_id == crate::metadata_group::METADATA_GROUP_ID {
                        // Metadata group (0): dispatch to the metadata applier.
                        // Raft no-op entries and conf-changes are already
                        // handled above; data entries carry a serialized
                        // `MetadataEntry` and are decoded by the applier.
                        let pairs: Vec<(u64, Vec<u8>)> = group_ready
                            .committed_entries
                            .iter()
                            .filter(|e| ConfChange::from_entry_data(&e.data).is_none())
                            .map(|e| (e.index, e.data.clone()))
                            .collect();
                        self.metadata_applier.apply(&pairs)
                    } else {
                        self.applier
                            .apply_committed(group_id, &group_ready.committed_entries)
                    };
                    if last_applied > 0 {
                        let mut mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
                        if let Err(e) = mr.advance_applied(group_id, last_applied) {
                            warn!(group_id, error = %e, "failed to advance applied index");
                        }
                    }

                    // Boot-time readiness: the first time the metadata
                    // group (0) applies any entry on this node — which
                    // is the leader-election no-op or a replayed entry
                    // — flip the ready watch. The host crate's
                    // `start_raft` returns the receiver; `main.rs`
                    // awaits it before binding client-facing
                    // listeners. Idempotent: subsequent ticks are a
                    // no-op once the latch is set.
                    if group_id == crate::metadata_group::METADATA_GROUP_ID
                        && !*self.ready_watch.borrow()
                    {
                        let _ = self.ready_watch.send(true);
                    }
                }

                // Phase 5: install-snapshot dispatch for lagging peers.
                if !group_ready.snapshots_needed.is_empty() {
                    let snapshot_meta = {
                        let mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
                        mr.snapshot_metadata(group_id).ok()
                    };

                    if let Some((term, snap_index, snap_term)) = snapshot_meta {
                        for peer in group_ready.snapshots_needed {
                            let transport = self.transport.clone();
                            let mr = self.multi_raft.clone();
                            let mut shutdown_rx = self.shutdown_watch.subscribe();
                            let req = nodedb_raft::InstallSnapshotRequest {
                                term,
                                leader_id: self.node_id,
                                last_included_index: snap_index,
                                last_included_term: snap_term,
                                offset: 0,
                                data: vec![],
                                done: true,
                                group_id,
                            };
                            tokio::spawn(async move {
                                if *shutdown_rx.borrow() {
                                    return;
                                }
                                tokio::select! {
                                    biased;
                                    _ = shutdown_rx.changed() => {}
                                    rpc = transport.install_snapshot(peer, req) => {
                                        match rpc {
                                            Ok(resp) => {
                                                if resp.term > term {
                                                    let mut mr =
                                                        mr.lock().unwrap_or_else(|p| p.into_inner());
                                                    // Higher term — let the tick loop handle step-down.
                                                    let _ = mr.handle_append_entries_response(
                                                        group_id,
                                                        peer,
                                                        &nodedb_raft::AppendEntriesResponse {
                                                            term: resp.term,
                                                            success: false,
                                                            last_log_index: 0,
                                                        },
                                                    );
                                                }
                                                debug!(group_id, peer, "install_snapshot sent");
                                            }
                                            Err(e) => {
                                                warn!(
                                                    group_id, peer, error = %e,
                                                    "install_snapshot RPC failed"
                                                );
                                            }
                                        }
                                    }
                                }
                            });
                        }
                    }
                }
            }
        }

        // Phase 6: promote caught-up learners. Runs every tick so a
        // just-caught-up learner is promoted within one tick interval of
        // catching up. Idempotent: once promoted, the peer is in
        // `members` and `ready_learners` no longer returns it.
        self.promote_ready_learners();
    }

    /// For every group where this node is leader, propose
    /// `PromoteLearner` for every learner whose `match_index` has
    /// reached the current `commit_index`.
    ///
    /// This is the automated second phase of the two-step Raft single-
    /// server add. The first phase (`AddLearner`) is proposed by the
    /// join flow; the second phase is proposed here, asynchronously,
    /// once the learner has caught up.
    fn promote_ready_learners(&self) {
        // Snapshot the work list under one lock acquisition.
        let promotions: Vec<(u64, u64)> = {
            let mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
            let group_ids: Vec<u64> = mr.routing().group_ids();
            group_ids
                .into_iter()
                .flat_map(|gid| {
                    mr.ready_learners(gid)
                        .into_iter()
                        .map(move |learner| (gid, learner))
                })
                .collect()
        };

        // Propose each promotion in its own lock acquisition. If any fails
        // (e.g., `NotLeader` after a step-down between phases), log and
        // move on — the next tick will retry any still-pending learner.
        for (group_id, learner_id) in promotions {
            let mut mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
            let change = ConfChange {
                change_type: ConfChangeType::PromoteLearner,
                node_id: learner_id,
            };
            match mr.propose_conf_change(group_id, &change) {
                Ok((_gid, idx)) => {
                    debug!(
                        group_id,
                        learner_id,
                        log_index = idx,
                        "proposed learner promotion"
                    );
                }
                Err(e) => {
                    debug!(
                        group_id,
                        learner_id,
                        error = %e,
                        "learner promotion proposal deferred"
                    );
                }
            }
        }
    }
}
