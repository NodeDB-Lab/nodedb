// SPDX-License-Identifier: BUSL-1.1

//! Physical-plan execution (C-β), metadata/data propose forwarding, and
//! VShardEnvelope routing RPC bodies.

use crate::calvin::SEQUENCER_GROUP_ID;
use crate::error::{CalvinError, ClusterError, Result};
use crate::forward::{ChunkSink, PlanExecutor};
use crate::multi_raft::MultiRaft;
use crate::rpc_codec::{
    DataProposeRequest, DataProposeResponse, ExecuteRequest, MetadataProposeRequest, ProposeTarget,
    RaftRpc, TypedClusterError, VShardRefusal, forwarded_deadline,
};

use super::super::loop_core::{CommitApplier, RaftLoop};

impl<A: CommitApplier, P: PlanExecutor> RaftLoop<A, P> {
    // Physical-plan execution (C-β) — execute locally via the PlanExecutor,
    // skipping SQL re-planning entirely.
    pub(super) async fn handle_execute_rpc(&self, req: ExecuteRequest) -> Result<RaftRpc> {
        let resp = self.plan_executor.execute_plan(req).await;
        Ok(RaftRpc::ExecuteResponse(resp))
    }

    // Metadata-group proposal forwarding — apply locally if
    // we're the metadata leader, otherwise return a
    // NotLeader response with a leader hint so the
    // forwarder can chase the redirect.
    pub(super) fn handle_metadata_propose_rpc(
        &self,
        req: MetadataProposeRequest,
    ) -> Result<RaftRpc> {
        let proposed = if req.stamp {
            self.propose_stamped_to_metadata_group(&req.bytes)
        } else {
            self.propose_to_metadata_group(req.bytes)
        };
        let resp = match proposed {
            Ok(log_index) => crate::rpc_codec::MetadataProposeResponse::ok(log_index),
            Err(crate::error::ClusterError::Raft(nodedb_raft::RaftError::NotLeader {
                leader_hint,
                term,
            })) => crate::rpc_codec::MetadataProposeResponse::err("not leader", leader_hint, term),
            Err(e) => crate::rpc_codec::MetadataProposeResponse::err(e.to_string(), None, 0),
        };
        Ok(RaftRpc::MetadataProposeResponse(resp))
    }

    // Data-group and sequencer-group proposal forwarding — apply locally if
    // we lead the target group, otherwise return NotLeader with a hint so the
    // forwarder can chase the redirect. A data-group entry passes this
    // leader's write gate first, exactly as a local proposal does.
    pub(super) async fn handle_data_propose_rpc(&self, req: DataProposeRequest) -> Result<RaftRpc> {
        let resp = match req.target {
            ProposeTarget::VShard {
                vshard_id,
                deadline_remaining_ms,
            } => match forwarded_deadline(deadline_remaining_ms) {
                // The proposer stopped waiting: the entry is refused unproposed.
                None => DataProposeResponse::refused(&ClusterError::Calvin(
                    CalvinError::AdmissionTimedOut,
                )),
                Some(deadline) => {
                    match self.propose_admitted(vshard_id, &req.bytes, deadline).await {
                        Ok((group_id, log_index)) => DataProposeResponse::ok(group_id, log_index),
                        Err(error) => DataProposeResponse::refused(&error),
                    }
                }
            },
            ProposeTarget::Sequencer => {
                let mut mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
                propose_to_sequencer(&mut mr, req.bytes)
            }
        };
        Ok(RaftRpc::DataProposeResponse(resp))
    }

    // VShardEnvelope — dispatch to registered handler (Event Plane, etc.).
    // Every handler error answers as a typed `VShardRefusal` frame, so the
    // caller rebuilds the same `ClusterError` instead of seeing a closed
    // stream.
    pub(super) async fn handle_vshard_envelope_rpc(&self, bytes: Vec<u8>) -> Result<RaftRpc> {
        let result = match self.vshard_handler {
            Some(ref handler) => handler(bytes).await,
            None => Err(ClusterError::Transport {
                detail: "VShardEnvelope handler not configured".into(),
            }),
        };
        Ok(vshard_answer(result))
    }

    // Streaming physical-plan execution (L4) — delegate to the PlanExecutor's
    // streaming path. The transport drives the multi-frame chunk/end envelope
    // writes; this just runs the plan and feeds `sink`.
    pub(super) async fn handle_rpc_streaming_impl(
        &self,
        req: ExecuteRequest,
        sink: impl ChunkSink,
    ) -> Option<TypedClusterError> {
        self.plan_executor.execute_plan_streaming(req, sink).await
    }
}

/// The frame that answers a VShardEnvelope request: the handler's response
/// envelope, or its typed error as a refusal.
fn vshard_answer(result: Result<Vec<u8>>) -> RaftRpc {
    match result {
        Ok(response_bytes) => RaftRpc::VShardEnvelope(response_bytes),
        Err(error) => RaftRpc::VShardRefusal(VShardRefusal::from(error)),
    }
}

/// Propose a forwarded sequencer entry to the sequencer group on this node.
///
/// Answers `not leader` with the known leader as a hint when this node does
/// not lead the sequencer group.
fn propose_to_sequencer(mr: &mut MultiRaft, bytes: Vec<u8>) -> DataProposeResponse {
    match mr.propose_to_group(SEQUENCER_GROUP_ID, bytes) {
        Ok(log_index) => DataProposeResponse::ok(SEQUENCER_GROUP_ID, log_index),
        Err(error) => DataProposeResponse::refused(&error),
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::*;
    use crate::routing::RoutingTable;

    fn multi_raft_with_sequencer(dir: &std::path::Path) -> MultiRaft {
        let mut mr = MultiRaft::new(1, RoutingTable::uniform(1, &[1], 1), dir.to_path_buf());
        mr.add_group(SEQUENCER_GROUP_ID, vec![])
            .expect("add sequencer group");
        mr
    }

    fn elect_sequencer_leader(mr: &mut MultiRaft) {
        if let Some(node) = mr.groups_mut().get_mut(&SEQUENCER_GROUP_ID) {
            node.election_deadline_override(Instant::now() - Duration::from_millis(1));
        }
        for _ in 0..20 {
            mr.tick().expect("tick");
            if mr.is_group_leader(SEQUENCER_GROUP_ID) {
                return;
            }
        }
        panic!("sequencer group did not elect this single node");
    }

    fn sequencer_request() -> DataProposeRequest {
        DataProposeRequest {
            target: ProposeTarget::Sequencer,
            bytes: vec![7, 7, 7],
        }
    }

    /// A handler error such as `WrongOwner` answers as a typed refusal the
    /// caller rebuilds, not as a closed stream.
    #[test]
    fn a_handler_error_answers_as_a_typed_refusal() {
        let answer = vshard_answer(Err(ClusterError::WrongOwner {
            vshard_id: 7,
            expected_owner_node: None,
        }));
        match answer {
            RaftRpc::VShardRefusal(refusal) => assert!(matches!(
                ClusterError::from(refusal.error),
                ClusterError::WrongOwner {
                    vshard_id: 7,
                    expected_owner_node: None
                }
            )),
            other => panic!("expected a refusal frame, got {other:?}"),
        }
    }

    #[test]
    fn a_handler_response_answers_as_an_envelope() {
        match vshard_answer(Ok(vec![1, 2, 3])) {
            RaftRpc::VShardEnvelope(bytes) => assert_eq!(bytes, vec![1, 2, 3]),
            other => panic!("expected a response envelope, got {other:?}"),
        }
    }

    #[test]
    fn sequencer_target_is_proposed_to_the_sequencer_group_on_its_leader() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut mr = multi_raft_with_sequencer(dir.path());
        elect_sequencer_leader(&mut mr);
        let before = mr.last_log_index(SEQUENCER_GROUP_ID).unwrap_or(0);

        let resp = propose_to_sequencer(&mut mr, sequencer_request().bytes);

        assert!(resp.success, "{}", resp.error_message);
        assert_eq!(resp.group_id, SEQUENCER_GROUP_ID);
        assert!(resp.log_index > before);
        assert_eq!(mr.last_log_index(SEQUENCER_GROUP_ID), Some(resp.log_index));
    }

    #[test]
    fn sequencer_target_on_a_non_leader_answers_not_leader() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut mr = multi_raft_with_sequencer(dir.path());

        let resp = propose_to_sequencer(&mut mr, sequencer_request().bytes);

        assert!(!resp.success);
        assert_eq!(
            resp.refusal,
            Some(crate::rpc_codec::ForwardedProposeRefusal::NotLeader)
        );
    }
}
