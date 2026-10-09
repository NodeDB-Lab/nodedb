// SPDX-License-Identifier: BUSL-1.1

//! Which requests the transport can send a second time after a written
//! attempt got no answer.

use super::raft_rpc::RaftRpc;

impl RaftRpc {
    /// Whether a second run of this request on the peer leaves the same
    /// state and answer as one run.
    ///
    /// The transport resends a written request with no answer only when
    /// this holds. The peer can have run the first copy. Every request that
    /// proposes, submits, executes, assigns or moves state answers `false`,
    /// and a lost answer ends as an unknown outcome. A reply variant never
    /// goes out as a request, so it answers `false`.
    pub fn resend_safe(&self) -> bool {
        match self {
            // Raft log matching makes a repeated append a no-op.
            RaftRpc::AppendEntriesRequest(_)
            // A voter grants at most one candidate per term, and a repeated
            // request from that candidate gets the same grant.
            | RaftRpc::RequestVoteRequest(_)
            // A pre-vote changes no Raft state.
            | RaftRpc::PreVoteRequest(_)
            // Reads of the peer's state.
            | RaftRpc::Ping(_)
            | RaftRpc::ReadIndexRequest(_)
            | RaftRpc::LeaderStatusRequest(_) => true,
            // A second copy starts a second election.
            RaftRpc::TimeoutNowRequest(_)
            // The receiver takes a chunk only at its next expected offset,
            // so it refuses a repeat of a chunk that landed.
            | RaftRpc::InstallSnapshotRequest(_)
            // Membership, topology and lease changes.
            | RaftRpc::JoinRequest(_)
            | RaftRpc::TopologyUpdate(_)
            | RaftRpc::AuthLeaseRenewRequest(_)
            | RaftRpc::AuthBarrierRequest(_)
            // Proposals, submits and executions.
            | RaftRpc::VShardEnvelope(_)
            | RaftRpc::MetadataProposeRequest(_)
            | RaftRpc::ExecuteRequest(_)
            | RaftRpc::ExecuteStreamRequest(_)
            | RaftRpc::ShufflePushRequest(_)
            | RaftRpc::ShufflePushChunk(_)
            | RaftRpc::ShufflePushEnd(_)
            | RaftRpc::ShuffleProduceRequest(_)
            | RaftRpc::ShuffleConsumeRequest(_)
            | RaftRpc::ShuffleAggregateConsumeRequest(_)
            | RaftRpc::AssignSurrogateRequest(_)
            | RaftRpc::SubmitCalvinTxnRequest(_)
            | RaftRpc::SubmitCalvinInboxRequest(_)
            | RaftRpc::CalvinPartsRequest(_)
            | RaftRpc::ReserveReadRequest(_)
            | RaftRpc::ReleaseReservationRequest(_)
            | RaftRpc::DataProposeRequest(_)
            // Replies.
            | RaftRpc::AppendEntriesResponse(_)
            | RaftRpc::RequestVoteResponse(_)
            | RaftRpc::PreVoteResponse(_)
            | RaftRpc::InstallSnapshotResponse(_)
            | RaftRpc::JoinResponse(_)
            | RaftRpc::Pong(_)
            | RaftRpc::TopologyAck(_)
            | RaftRpc::MetadataProposeResponse(_)
            | RaftRpc::ExecuteResponse(_)
            | RaftRpc::ExecuteStreamChunk(_)
            | RaftRpc::ExecuteStreamEnd(_)
            | RaftRpc::ShuffleProduceResponse(_)
            | RaftRpc::ShuffleConsumeResponse(_)
            | RaftRpc::ShuffleAggregateConsumeResponse(_)
            | RaftRpc::AssignSurrogateResponse(_)
            | RaftRpc::SubmitCalvinTxnResponse(_)
            | RaftRpc::SubmitCalvinInboxResponse(_)
            | RaftRpc::CalvinPartsResponse(_)
            | RaftRpc::ReserveReadResponse(_)
            | RaftRpc::ReleaseReservationResponse(_)
            | RaftRpc::DataProposeResponse(_)
            | RaftRpc::ReadIndexResponse(_)
            | RaftRpc::LeaderStatusResponse(_)
            | RaftRpc::AuthLeaseRenewResponse(_)
            | RaftRpc::AuthBarrierResponse(_)
            | RaftRpc::VShardRefusal(_)
            | RaftRpc::FrameRefused(_)
            | RaftRpc::RequestRefused(_) => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpc_codec::{DataProposeRequest, PingRequest, ProposeTarget};

    #[test]
    fn a_data_propose_is_never_resent() {
        let propose = RaftRpc::DataProposeRequest(DataProposeRequest {
            target: ProposeTarget::VShard {
                vshard_id: 3,
                deadline_remaining_ms: 100,
            },
            bytes: vec![1, 2, 3],
        });
        assert!(!propose.resend_safe());
    }

    #[test]
    fn a_ping_is_resent() {
        let ping = RaftRpc::Ping(PingRequest {
            sender_id: 1,
            topology_version: 0,
        });
        assert!(ping.resend_safe());
    }
}
