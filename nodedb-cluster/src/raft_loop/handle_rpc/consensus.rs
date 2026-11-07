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
            // Replace the framed chunk with its raw payload so the
            // accumulator writes unframed bytes (offsets/CRC below are
            // payload-space).
            req.data = admit_snapshot_chunk(
                self.snapshot_quarantine_hook.as_deref(),
                req.group_id,
                req.last_included_index,
                &req.data,
            )?;
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

/// Check one framed snapshot chunk and return its payload.
///
/// Every chunk decodes, a quarantined one included. A leader whose group takes
/// no writes resends the snapshot at the same index, and a replica awaiting a
/// snapshot neither replicates nor campaigns. So refusing good bytes at that
/// index holds the replica behind until a write or a restart. Bytes that check
/// out clear the record. A bad chunk never reaches the partial file, so the
/// good retransmit replaces it. Bad bytes stay refused, with a stable error
/// that names the quarantine.
fn admit_snapshot_chunk(
    hook: Option<&dyn crate::raft_loop::SnapshotQuarantineHook>,
    group_id: u64,
    last_included_index: u64,
    framed: &[u8],
) -> Result<Vec<u8>> {
    use nodedb_raft::snapshot_framing::SnapshotFramingError;

    match nodedb_raft::decode_snapshot_chunk(framed) {
        Ok((_engine_id, payload)) => {
            if let Some(hook) = hook {
                hook.record_success(group_id, last_included_index);
            }
            Ok(payload.to_vec())
        }
        Err(e) => {
            let is_crc_class = matches!(
                e,
                SnapshotFramingError::CrcMismatch { .. } | SnapshotFramingError::Truncated(_)
            );
            let quarantined = is_crc_class
                && hook.is_some_and(|hook| {
                    hook.record_failure(group_id, last_included_index, &e.to_string())
                });
            let detail = if quarantined {
                format!(
                    "InstallSnapshot chunk quarantined: group={group_id} \
                     index={last_included_index}: {e}"
                )
            } else {
                format!("InstallSnapshot framing: {e}")
            };
            Err(crate::error::ClusterError::Codec { detail })
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use nodedb_raft::snapshot_framing::{SnapshotEngineId, encode_snapshot_chunk};

    use super::*;
    use crate::raft_loop::SnapshotQuarantineHook;

    /// Two consecutive failures quarantine, and a success clears everything.
    #[derive(Default)]
    struct Strikes(Mutex<u32>);

    impl SnapshotQuarantineHook for Strikes {
        fn is_quarantined(&self, _group_id: u64, _index: u64) -> bool {
            *self.0.lock().expect("strikes") >= 2
        }

        fn record_success(&self, _group_id: u64, _index: u64) {
            *self.0.lock().expect("strikes") = 0;
        }

        fn record_failure(&self, _group_id: u64, _index: u64, _error: &str) -> bool {
            let mut strikes = self.0.lock().expect("strikes");
            *strikes += 1;
            *strikes >= 2
        }
    }

    fn good_chunk() -> Vec<u8> {
        encode_snapshot_chunk(SnapshotEngineId::Composite, b"payload")
    }

    fn bad_chunk() -> Vec<u8> {
        let mut chunk = good_chunk();
        if let Some(last) = chunk.last_mut() {
            *last ^= 0xFF;
        }
        chunk
    }

    fn detail(result: Result<Vec<u8>>) -> String {
        match result {
            Err(crate::error::ClusterError::Codec { detail }) => detail,
            other => panic!("expected a codec error, got {other:?}"),
        }
    }

    /// A retransmit at a quarantined index with good bytes is admitted and
    /// clears the record. An idle group resends at this index until it lands.
    #[test]
    fn a_good_retransmit_clears_a_quarantined_chunk() {
        let hook = Strikes::default();
        let bad = bad_chunk();
        assert!(admit_snapshot_chunk(Some(&hook), 1, 9, &bad).is_err());
        assert!(admit_snapshot_chunk(Some(&hook), 1, 9, &bad).is_err());
        assert!(hook.is_quarantined(1, 9));

        let payload = admit_snapshot_chunk(Some(&hook), 1, 9, &good_chunk()).expect("good bytes");

        assert_eq!(payload, b"payload");
        assert!(!hook.is_quarantined(1, 9));
    }

    /// Bad bytes at a quarantined index stay refused, and the error names the
    /// quarantine once it applies.
    #[test]
    fn bad_bytes_stay_refused_with_a_stable_error() {
        let hook = Strikes::default();
        let bad = bad_chunk();
        let first = detail(admit_snapshot_chunk(Some(&hook), 1, 9, &bad));
        assert!(first.contains("framing"), "{first}");
        for _ in 0..2 {
            let later = detail(admit_snapshot_chunk(Some(&hook), 1, 9, &bad));
            assert!(later.contains("quarantined: group=1 index=9"), "{later}");
        }
        assert!(hook.is_quarantined(1, 9));
    }

    #[test]
    fn no_hook_still_checks_the_frame() {
        assert!(admit_snapshot_chunk(None, 1, 9, &bad_chunk()).is_err());
        assert_eq!(
            admit_snapshot_chunk(None, 1, 9, &good_chunk()).expect("good"),
            b"payload"
        );
    }
}
