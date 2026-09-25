// SPDX-License-Identifier: BUSL-1.1

//! The single `impl RaftRpcHandler for RaftLoop` block. Thin dispatch only —
//! each method delegates to a helper defined in a sibling module
//! ([`super::consensus`], [`super::membership`], [`super::plan_dispatch`],
//! [`super::shuffle_calvin`]).

use crate::error::{ClusterError, Result};
use crate::forward::{ChunkSink, PlanExecutor};
use crate::rpc_codec::{
    AssignSurrogateRequest, AssignSurrogateResponse, ExecuteRequest, RaftRpc,
    ReleaseReservationRequest, ReleaseReservationResponse, ReserveReadRequest, ReserveReadResponse,
    ShuffleAggregateConsumeRequest, ShuffleAggregateConsumeResponse, ShuffleConsumeRequest,
    ShuffleConsumeResponse, ShuffleProduceRequest, ShuffleProduceResponse, ShufflePushRequest,
    SubmitCalvinInboxRequest, SubmitCalvinInboxResponse, SubmitCalvinTxnRequest,
    SubmitCalvinTxnResponse, TypedClusterError,
};
use crate::transport::RaftRpcHandler;
use nodedb_raft::message::TimeoutNowRequest;

use super::super::loop_core::{CommitApplier, RaftLoop};

impl<A: CommitApplier, P: PlanExecutor> RaftRpcHandler for RaftLoop<A, P> {
    async fn handle_rpc(&self, rpc: RaftRpc) -> Result<RaftRpc> {
        match rpc {
            // Raft consensus RPCs — lock MultiRaft (sync, never across await).
            RaftRpc::AppendEntriesRequest(req) => self.handle_append_entries_rpc(req),
            RaftRpc::RequestVoteRequest(req) => self.handle_request_vote_rpc(req),
            RaftRpc::PreVoteRequest(req) => self.handle_pre_vote_rpc(req),
            RaftRpc::InstallSnapshotRequest(req) => self.handle_install_snapshot_rpc(req).await,
            // Cluster join — full orchestration in `super::join`.
            RaftRpc::JoinRequest(req) => Ok(RaftRpc::JoinResponse(self.join_flow(req).await)),
            // Health check.
            RaftRpc::Ping(req) => self.handle_ping_rpc(req),
            // Topology broadcast.
            RaftRpc::TopologyUpdate(update) => self.handle_topology_update_rpc(update),
            // Physical-plan execution (C-β) — execute locally via the PlanExecutor,
            // skipping SQL re-planning entirely.
            RaftRpc::ExecuteRequest(req) => self.handle_execute_rpc(req).await,
            // Metadata-group proposal forwarding.
            RaftRpc::MetadataProposeRequest(req) => self.handle_metadata_propose_rpc(req),
            // Data-group proposal forwarding.
            RaftRpc::DataProposeRequest(req) => self.handle_data_propose_rpc(req),
            // Read index for a node that does not lead the group.
            RaftRpc::ReadIndexRequest(req) => self.handle_read_index_rpc(req).await,
            // Authorization lease renewal and barrier, answered by the host hook.
            RaftRpc::AuthLeaseRenewRequest(req) => self.handle_auth_lease_renew_rpc(req).await,
            RaftRpc::AuthBarrierRequest(req) => self.handle_auth_barrier_rpc(req).await,
            // VShardEnvelope — dispatch to registered handler (Event Plane, etc.).
            RaftRpc::VShardEnvelope(bytes) => self.handle_vshard_envelope_rpc(bytes).await,
            other => Err(ClusterError::Transport {
                detail: format!("unexpected request type in RPC handler: {other:?}"),
            }),
        }
    }

    // Streaming physical-plan execution (L4) — delegate to the PlanExecutor's
    // streaming path. The transport drives the multi-frame chunk/end envelope
    // writes; this just runs the plan and feeds `sink`.
    async fn handle_rpc_streaming(
        &self,
        req: ExecuteRequest,
        sink: impl ChunkSink,
    ) -> Option<TypedClusterError> {
        self.handle_rpc_streaming_impl(req, sink).await
    }

    async fn on_shuffle_request(&self, req: ShufflePushRequest) {
        self.on_shuffle_request_impl(req).await
    }

    async fn on_shuffle_chunk(
        &self,
        shuffle_id: u64,
        part: u32,
        side: u8,
        payload: Vec<u8>,
    ) -> Result<()> {
        self.on_shuffle_chunk_impl(shuffle_id, part, side, payload)
            .await
    }

    async fn on_shuffle_end(
        &self,
        shuffle_id: u64,
        part: u32,
        side: u8,
        error: Option<TypedClusterError>,
    ) {
        self.on_shuffle_end_impl(shuffle_id, part, side, error)
            .await
    }

    async fn on_shuffle_produce(&self, req: ShuffleProduceRequest) -> ShuffleProduceResponse {
        self.on_shuffle_produce_impl(req).await
    }

    async fn on_shuffle_consume(&self, req: ShuffleConsumeRequest) -> ShuffleConsumeResponse {
        self.on_shuffle_consume_impl(req).await
    }

    async fn on_shuffle_aggregate(
        &self,
        req: ShuffleAggregateConsumeRequest,
    ) -> ShuffleAggregateConsumeResponse {
        self.on_shuffle_aggregate_impl(req).await
    }

    async fn on_assign_surrogate(&self, req: AssignSurrogateRequest) -> AssignSurrogateResponse {
        self.on_assign_surrogate_impl(req).await
    }

    async fn on_submit_calvin_txn(&self, req: SubmitCalvinTxnRequest) -> SubmitCalvinTxnResponse {
        self.on_submit_calvin_txn_impl(req).await
    }

    async fn on_submit_calvin_inbox(
        &self,
        req: SubmitCalvinInboxRequest,
    ) -> SubmitCalvinInboxResponse {
        self.on_submit_calvin_inbox_impl(req).await
    }

    async fn on_reserve_read(&self, req: ReserveReadRequest) -> ReserveReadResponse {
        self.on_reserve_read_impl(req).await
    }

    async fn on_release_reservation(
        &self,
        req: ReleaseReservationRequest,
    ) -> ReleaseReservationResponse {
        self.on_release_reservation_impl(req).await
    }

    async fn on_timeout_now(&self, req: TimeoutNowRequest) {
        self.on_timeout_now_impl(req).await
    }
}

#[cfg(test)]
mod tests {
    use crate::multi_raft::MultiRaft;
    use crate::routing::RoutingTable;
    use crate::rpc_codec::RaftRpc;
    use crate::topology::{ClusterTopology, NodeInfo, NodeState};
    use crate::transport::{NexarTransport, RaftRpcHandler};
    use nodedb_raft::message::LogEntry;
    use std::sync::{Arc, RwLock};
    use std::time::{Duration, Instant};

    use super::super::super::loop_core::{CommitApplier, RaftLoop};

    /// No-op applier for tests that don't care about state machine output.
    struct NoopApplier;
    impl CommitApplier for NoopApplier {
        fn apply_committed(&self, _group_id: u64, entries: &[LogEntry]) -> u64 {
            entries.last().map(|e| e.index).unwrap_or(0)
        }
    }

    fn make_transport(node_id: u64) -> Arc<NexarTransport> {
        Arc::new(
            NexarTransport::new(
                node_id,
                "127.0.0.1:0".parse().unwrap(),
                crate::transport::credentials::TransportCredentials::Insecure,
            )
            .unwrap(),
        )
    }

    #[tokio::test]
    async fn rpc_handler_routes_append_entries() {
        let dir = tempfile::tempdir().unwrap();
        let transport = make_transport(1);
        let rt = RoutingTable::uniform(1, &[1], 1);
        let mut mr = MultiRaft::new(1, rt, dir.path().to_path_buf());
        mr.add_group(0, vec![]).unwrap();

        for node in mr.groups_mut().values_mut() {
            node.election_deadline_override(Instant::now() - Duration::from_millis(1));
        }

        let topo = Arc::new(RwLock::new(ClusterTopology::new()));
        let raft_loop = RaftLoop::new(mr, transport, topo, NoopApplier);

        raft_loop.do_tick();
        tokio::time::sleep(Duration::from_millis(20)).await;

        let req = RaftRpc::AppendEntriesRequest(nodedb_raft::AppendEntriesRequest {
            term: 99,
            leader_id: 2,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
            group_id: 0,
        });

        let resp = raft_loop.handle_rpc(req).await.unwrap();
        match resp {
            RaftRpc::AppendEntriesResponse(r) => {
                assert!(r.success);
                assert_eq!(r.term, 99);
            }
            other => panic!("expected AppendEntriesResponse, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn rpc_handler_routes_request_vote() {
        let dir = tempfile::tempdir().unwrap();
        let transport = make_transport(1);
        let rt = RoutingTable::uniform(1, &[1, 2, 3], 3);
        let mut mr = MultiRaft::new(1, rt, dir.path().to_path_buf());
        mr.add_group(0, vec![2, 3]).unwrap();

        let topo = Arc::new(RwLock::new(ClusterTopology::new()));
        let raft_loop = RaftLoop::new(mr, transport, topo, NoopApplier);

        let req = RaftRpc::RequestVoteRequest(nodedb_raft::RequestVoteRequest {
            term: 1,
            candidate_id: 2,
            last_log_index: 0,
            last_log_term: 0,
            group_id: 0,
        });

        let resp = raft_loop.handle_rpc(req).await.unwrap();
        match resp {
            RaftRpc::RequestVoteResponse(r) => {
                assert!(r.vote_granted);
                assert_eq!(r.term, 1);
            }
            other => panic!("expected RequestVoteResponse, got {other:?}"),
        }
    }

    /// JoinRequest on a freshly-bootstrapped single-seed RaftLoop is
    /// admitted locally: this node is leader of every group, so
    /// `AddLearner` conf-changes are proposed and (because the groups
    /// are single-voter) commit instantly.
    #[tokio::test]
    async fn rpc_handler_accepts_join_on_bootstrap_seed() {
        let dir = tempfile::tempdir().unwrap();
        let transport = make_transport(1);
        // uniform(2, ...) creates metadata group 0 + data groups 1 and 2.
        let rt = RoutingTable::uniform(2, &[1], 1);
        let mut mr = MultiRaft::new(1, rt, dir.path().to_path_buf());
        mr.add_group(0, vec![]).unwrap();
        mr.add_group(1, vec![]).unwrap();
        mr.add_group(2, vec![]).unwrap();
        // Force immediate election so both groups reach Leader before
        // the join flow proposes AddLearner.
        for node in mr.groups_mut().values_mut() {
            node.election_deadline_override(Instant::now() - Duration::from_millis(1));
        }

        let mut topology = ClusterTopology::new();
        topology.add_node(NodeInfo::new(
            1,
            "127.0.0.1:9400".parse().unwrap(),
            NodeState::Active,
        ));
        let topo = Arc::new(RwLock::new(topology));

        let raft_loop = RaftLoop::new(mr, transport, topo.clone(), NoopApplier);
        raft_loop.do_tick();
        tokio::time::sleep(Duration::from_millis(20)).await;

        let req = RaftRpc::JoinRequest(crate::rpc_codec::JoinRequest {
            node_id: 2,
            listen_addr: "127.0.0.1:9401".into(),
            wire_version: crate::topology::CLUSTER_WIRE_FORMAT_VERSION,
            spiffe_id: None,
            spki_pin: None,
        });

        let resp = raft_loop.handle_rpc(req).await.unwrap();
        match resp {
            RaftRpc::JoinResponse(r) => {
                assert!(
                    r.success,
                    "join should succeed on bootstrap seed: {}",
                    r.error
                );
                assert_eq!(r.nodes.len(), 2);
                // uniform(2, ...) creates 3 groups (metadata + 2 data).
                assert_eq!(r.groups.len(), 3);
                assert_eq!(r.vshard_to_group.len(), 1024);
                // The new node should appear as a learner on every group,
                // not as a voter — voter promotion happens asynchronously
                // via the tick loop's promotion phase.
                for g in &r.groups {
                    assert!(
                        g.learners.contains(&2),
                        "expected node 2 as learner in group {}, got learners={:?} members={:?}",
                        g.group_id,
                        g.learners,
                        g.members
                    );
                }
            }
            other => panic!("expected JoinResponse, got {other:?}"),
        }

        let topo_guard = topo.read().unwrap();
        assert_eq!(topo_guard.node_count(), 2);
        assert!(topo_guard.contains(2));
    }
}
