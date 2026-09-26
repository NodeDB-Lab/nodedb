// SPDX-License-Identifier: BUSL-1.1

//! Physical-plan execution (C-β), metadata/data propose forwarding, and
//! VShardEnvelope routing RPC bodies.

use crate::calvin::SEQUENCER_GROUP_ID;
use crate::error::{ClusterError, Result};
use crate::forward::{ChunkSink, PlanExecutor};
use crate::multi_raft::MultiRaft;
use crate::rpc_codec::{
    DataProposeRequest, DataProposeResponse, ExecuteRequest, MetadataProposeRequest, ProposeTarget,
    RaftRpc, TypedClusterError,
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
        let resp = match self.propose_to_metadata_group(req.bytes) {
            Ok(log_index) => crate::rpc_codec::MetadataProposeResponse::ok(log_index),
            Err(crate::error::ClusterError::Raft(nodedb_raft::RaftError::NotLeader {
                leader_hint,
            })) => crate::rpc_codec::MetadataProposeResponse::err("not leader", leader_hint),
            Err(e) => crate::rpc_codec::MetadataProposeResponse::err(e.to_string(), None),
        };
        Ok(RaftRpc::MetadataProposeResponse(resp))
    }

    // Data-group and sequencer-group proposal forwarding — apply locally if
    // we lead the target group, otherwise return NotLeader with a hint so the
    // forwarder can chase the redirect.
    pub(super) fn handle_data_propose_rpc(&self, req: DataProposeRequest) -> Result<RaftRpc> {
        let resp = {
            let mut mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
            propose_forwarded(&mut mr, req)
        };
        Ok(RaftRpc::DataProposeResponse(resp))
    }

    // VShardEnvelope — dispatch to registered handler (Event Plane, etc.).
    pub(super) async fn handle_vshard_envelope_rpc(&self, bytes: Vec<u8>) -> Result<RaftRpc> {
        if let Some(ref handler) = self.vshard_handler {
            let response_bytes = handler(bytes).await?;
            Ok(RaftRpc::VShardEnvelope(response_bytes))
        } else {
            Err(ClusterError::Transport {
                detail: "VShardEnvelope handler not configured".into(),
            })
        }
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

/// Propose a forwarded entry to its target group on this node.
///
/// Answers `not leader` with the known leader as a hint when this node does
/// not lead the target group.
fn propose_forwarded(mr: &mut MultiRaft, req: DataProposeRequest) -> DataProposeResponse {
    let proposed = match req.target {
        ProposeTarget::VShard(vshard_id) => mr.propose(vshard_id, req.bytes),
        ProposeTarget::Sequencer => mr
            .propose_to_group(SEQUENCER_GROUP_ID, req.bytes)
            .map(|log_index| (SEQUENCER_GROUP_ID, log_index)),
    };
    match proposed {
        Ok((group_id, log_index)) => DataProposeResponse::ok(group_id, log_index),
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

    #[test]
    fn sequencer_target_is_proposed_to_the_sequencer_group_on_its_leader() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut mr = multi_raft_with_sequencer(dir.path());
        elect_sequencer_leader(&mut mr);
        let before = mr.last_log_index(SEQUENCER_GROUP_ID).unwrap_or(0);

        let resp = propose_forwarded(&mut mr, sequencer_request());

        assert!(resp.success, "{}", resp.error_message);
        assert_eq!(resp.group_id, SEQUENCER_GROUP_ID);
        assert!(resp.log_index > before);
        assert_eq!(mr.last_log_index(SEQUENCER_GROUP_ID), Some(resp.log_index));
    }

    #[test]
    fn sequencer_target_on_a_non_leader_answers_not_leader() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut mr = multi_raft_with_sequencer(dir.path());

        let resp = propose_forwarded(&mut mr, sequencer_request());

        assert!(!resp.success);
        assert_eq!(resp.error_message, "not leader");
    }
}
