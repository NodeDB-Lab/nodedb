// SPDX-License-Identifier: BUSL-1.1

//! Raft consensus RPC bodies: AppendEntries, RequestVote, InstallSnapshot,
//! and the TimeoutNow election trigger.

use crate::error::Result;
use crate::forward::PlanExecutor;
use crate::rpc_codec::RaftRpc;
use nodedb_raft::message::{
    AppendEntriesRequest, InstallSnapshotRequest, PreVoteRequest, RequestVoteRequest,
    TimeoutNowRequest,
};

use super::super::loop_core::{CommitApplier, RaftLoop};

impl<A: CommitApplier, P: PlanExecutor> RaftLoop<A, P> {
    /// The leader counts a successful answer as this node holding the
    /// claimed entries durably. The answer waits only for what it claims:
    /// - the latest term and vote, whoever staged them
    /// - the entries and truncations this request staged, with every write
    ///   staged before them
    ///
    /// A request that staged no entries claims only the durable prefix. It
    /// never waits on writes the apply loop staged. The disk wait runs
    /// without the `MultiRaft` lock.
    pub(super) async fn handle_append_entries_rpc(
        &self,
        req: AppendEntriesRequest,
    ) -> Result<RaftRpc> {
        let (resp, ticket) = {
            let mut mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
            let mark = mr.staged_through(req.group_id);
            let resp = mr.handle_append_entries(&req)?;
            mr.persist_group_hard_state(req.group_id)?;
            (resp, mr.reply_ticket(req.group_id, mark))
        };
        if let Some(ticket) = ticket {
            ticket.durable().await?;
        }
        Ok(RaftRpc::AppendEntriesResponse(resp))
    }

    /// `voted_for` and `current_term` are durable before the answer leaves
    /// this node, so a restart cannot double-vote. A repeated request that
    /// staged nothing still waits for a vote an earlier one staged. The disk
    /// wait runs without the `MultiRaft` lock.
    pub(super) async fn handle_request_vote_rpc(&self, req: RequestVoteRequest) -> Result<RaftRpc> {
        let (resp, ticket) = {
            let mut mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
            let mark = mr.staged_through(req.group_id);
            let resp = mr.handle_request_vote(&req)?;
            mr.persist_group_hard_state(req.group_id)?;
            (resp, mr.reply_ticket(req.group_id, mark))
        };
        if let Some(ticket) = ticket {
            ticket.durable().await?;
        }
        Ok(RaftRpc::RequestVoteResponse(resp))
    }

    /// Handle a PreVote probe. Deliberately mutates neither `current_term`
    /// nor `voted_for` — a pre-vote never adopts the hypothetical term it
    /// probes — so there is nothing to persist here, unlike RequestVote.
    pub(super) fn handle_pre_vote_rpc(&self, req: PreVoteRequest) -> Result<RaftRpc> {
        let mut mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
        let resp = mr.handle_pre_vote(&req)?;
        Ok(RaftRpc::PreVoteResponse(resp))
    }

    /// Apply snapshot bytes only after the cluster transport has authenticated
    /// the sender's mTLS identity, HMAC envelope, and replay sequence. CRC and
    /// chunk framing below detect corruption; they are not authenticity checks.
    pub(super) async fn handle_install_snapshot_rpc(
        &self,
        mut req: InstallSnapshotRequest,
    ) -> Result<RaftRpc> {
        // Validate snapshot framing for any non-empty chunk, then STRIP
        // the frame header so everything below this RPC boundary
        // (`receiver::handle_chunk`, `finalize::commit`, the
        // `SnapshotApplier`) sees the raw payload it expects — the
        // partial-file bytes, the whole-snapshot CRC, and the applier's
        // `zerompk::from_msgpack` all operate on the unframed payload.
        // Empty data is the bootstrap stub (no engine data yet); skip
        // framing in that case. The sender frames every non-empty chunk
        // with `encode_snapshot_chunk`.
        if !req.data.is_empty() {
            // Short-circuit immediately if this chunk has already been
            // quarantined after two consecutive CRC failures. Without
            // this check a quarantined chunk would re-attempt the
            // (always-failing) decode on every incoming RPC and never
            // surface a stable, operator-visible error.
            if let Some(ref hook) = self.snapshot_quarantine_hook
                && hook.is_quarantined(req.group_id, req.last_included_index)
            {
                return Err(crate::error::ClusterError::Codec {
                    detail: format!(
                        "InstallSnapshot chunk quarantined: group={} index={}",
                        req.group_id, req.last_included_index
                    ),
                });
            }

            match nodedb_raft::decode_snapshot_chunk(&req.data) {
                Ok((_engine_id, payload)) => {
                    // Successful decode — reset any prior strike so a
                    // single transient CRC error does not permanently
                    // count against a healthy peer.
                    let stripped = payload.to_vec();
                    if let Some(ref hook) = self.snapshot_quarantine_hook {
                        hook.record_success(req.group_id, req.last_included_index);
                    }
                    // Replace the framed chunk with its raw payload so the
                    // accumulator writes unframed bytes (offsets/CRC below
                    // are payload-space).
                    req.data = stripped;
                }
                Err(e) => {
                    let is_crc_class = matches!(
                        e,
                        nodedb_raft::snapshot_framing::SnapshotFramingError::CrcMismatch { .. }
                            | nodedb_raft::snapshot_framing::SnapshotFramingError::Truncated(_)
                    );
                    if is_crc_class && let Some(ref hook) = self.snapshot_quarantine_hook {
                        hook.record_failure(req.group_id, req.last_included_index, &e.to_string());
                    }
                    return Err(crate::error::ClusterError::Codec {
                        detail: format!("InstallSnapshot framing: {e}"),
                    });
                }
            }
        }

        let last_included_index = req.last_included_index;
        let group_id = req.group_id;

        // The snapshot covers conf changes this node never applies. The
        // final chunk carries the membership they produced.
        if req.done {
            self.adopt_snapshot_membership(&req).await?;
        }

        // Route through the chunk accumulator when a data directory is
        // configured. The accumulator writes chunks to a `.partial` file,
        // validates the full CRC on the final chunk, and then calls
        // `mr.handle_install_snapshot` after atomic rename.
        //
        // When `data_dir` is `None` (unit tests that don't set a data
        // directory) fall through to the original direct call so test
        // coverage for Raft state-machine transitions is unaffected.
        //
        // Quarantine accounting for offset regression and CRC errors is
        // preserved: the `SnapshotOffsetRegression` and
        // `SnapshotCrcMismatch` error paths in the receiver both surface
        // as `ClusterError` variants that are propagated here.
        if let Some(ref data_dir) = self.data_dir {
            match crate::install_snapshot::receiver::handle_chunk(
                &req,
                &self.partial_snapshots,
                data_dir,
                &self.multi_raft,
                self.snapshot_applier.as_ref(),
            )
            .await
            {
                Ok(crate::install_snapshot::ChunkOutcome::Committed(committed)) => {
                    // The watcher means "state visible through N". It moves
                    // only when the host state machine holds the snapshot,
                    // for every group kind. Later entries move it on apply.
                    if committed.state_installed {
                        self.group_watchers.bump(group_id, last_included_index);
                    }
                    // The leader resumes replication after the snapshot index
                    // once this answer arrives, so the new boundary and any
                    // term bump are durable first.
                    self.await_group_durable(group_id).await?;
                    return Ok(RaftRpc::InstallSnapshotResponse(committed.response));
                }
                Ok(crate::install_snapshot::ChunkOutcome::Pending) => {
                    // Non-final chunk — pass a done=false stub to MultiRaft so
                    // it resets its election timeout and returns the current term.
                    let pending_req = nodedb_raft::InstallSnapshotRequest {
                        term: req.term,
                        leader_id: req.leader_id,
                        last_included_index: req.last_included_index,
                        last_included_term: req.last_included_term,
                        offset: req.offset,
                        data: vec![],
                        done: false,
                        group_id,
                        total_size: 0,
                        voters: Vec::new(),
                        learners: Vec::new(),
                    };
                    let resp = {
                        let mut mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
                        let resp = mr.handle_install_snapshot(&pending_req)?;
                        mr.persist_group_hard_state(group_id)?;
                        resp
                    };
                    // Any term bump is durable before the reply.
                    self.await_group_durable(group_id).await?;
                    return Ok(RaftRpc::InstallSnapshotResponse(resp));
                }
                Err(e @ crate::error::ClusterError::SnapshotOffsetRegression { .. }) => {
                    // Record the regression as a quarantine strike so the
                    // sender knows to retransmit from offset 0.
                    if let Some(ref hook) = self.snapshot_quarantine_hook {
                        hook.record_failure(group_id, last_included_index, &e.to_string());
                    }
                    // Reset partial state so the next offset-0 chunk starts fresh.
                    self.partial_snapshots
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .remove(&group_id);
                    return Err(e);
                }
                Err(e @ crate::error::ClusterError::SnapshotCrcMismatch { .. }) => {
                    if let Some(ref hook) = self.snapshot_quarantine_hook {
                        hook.record_failure(group_id, last_included_index, &e.to_string());
                    }
                    return Err(e);
                }
                Err(e) => return Err(e),
            }
        }

        // Fallback: no data_dir — direct call (unit test path).
        let resp = {
            let mut mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
            let resp = mr.handle_install_snapshot(&req)?;
            mr.persist_group_hard_state(group_id)?;
            resp
        };
        // Any term bump is durable before the reply.
        self.await_group_durable(group_id).await?;
        // No host state machine restores anything on this path, so no
        // watcher moves: the watcher means "state visible through N".
        Ok(RaftRpc::InstallSnapshotResponse(resp))
    }

    /// Wait until every write `group_id` staged so far is durable. Takes the
    /// ticket under the `MultiRaft` lock and waits without it.
    async fn await_group_durable(&self, group_id: u64) -> Result<()> {
        let ticket = self
            .multi_raft
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .durability_ticket(group_id);
        match ticket {
            Some(ticket) => ticket.durable().await,
            None => Ok(()),
        }
    }

    /// A TimeoutNow triggers an immediate election: a term bump and a
    /// self-vote. The hard state is staged here. The tick loop sends the
    /// resulting vote requests only once the group's staged writes are
    /// durable, so a restart cannot forget the term.
    pub(super) async fn on_timeout_now_impl(&self, req: TimeoutNowRequest) {
        let mut mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
        mr.handle_timeout_now(&req);
        if let Err(e) = mr.persist_group_hard_state(req.group_id) {
            tracing::error!(
                group_id = req.group_id,
                error = %e,
                "failed to stage hard state after timeout-now election trigger"
            );
        }
    }
}
