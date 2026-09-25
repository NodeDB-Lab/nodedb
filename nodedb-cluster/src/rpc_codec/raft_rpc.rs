// SPDX-License-Identifier: BUSL-1.1

//! Top-level `RaftRpc` enum and `encode` / `decode` dispatcher.

use nodedb_raft::message::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    PreVoteRequest, PreVoteResponse, RequestVoteRequest, RequestVoteResponse, TimeoutNowRequest,
};

use super::auth_lease::{
    AuthBarrierRequest, AuthBarrierResponse, AuthLeaseRenewRequest, AuthLeaseRenewResponse,
};
use super::calvin_submit::{
    SubmitCalvinInboxRequest, SubmitCalvinInboxResponse, SubmitCalvinTxnRequest,
    SubmitCalvinTxnResponse,
};
use super::cluster_mgmt::{
    JoinRequest, JoinResponse, PingRequest, PongResponse, TopologyAck, TopologyUpdate,
};
use super::data_propose::{DataProposeRequest, DataProposeResponse};
use super::discriminants::*;
use super::execute::{ExecuteRequest, ExecuteResponse, ExecuteStreamChunk, ExecuteStreamEnd};
use super::header::HEADER_SIZE;
use super::metadata::{MetadataProposeRequest, MetadataProposeResponse};
use super::read_index::{ReadIndexRequest, ReadIndexResponse};
use super::reservation::{
    ReleaseReservationRequest, ReleaseReservationResponse, ReserveReadRequest, ReserveReadResponse,
};
use super::shuffle::{
    ShuffleAggregateConsumeRequest, ShuffleAggregateConsumeResponse, ShuffleConsumeRequest,
    ShuffleConsumeResponse, ShuffleProduceRequest, ShuffleProduceResponse, ShufflePushChunk,
    ShufflePushEnd, ShufflePushRequest,
};
use super::surrogate::{AssignSurrogateRequest, AssignSurrogateResponse};
use super::{
    auth_lease, calvin_submit, cluster_mgmt, data_propose, execute, metadata, raft_msgs,
    read_index, reservation, shuffle, surrogate, vshard,
};
use crate::error::{ClusterError, Result};
use crate::wire_version::{unwrap_bytes_versioned, wrap_bytes_versioned};

/// An RPC message — Raft consensus or cluster management.
#[derive(Debug, Clone)]
pub enum RaftRpc {
    // Raft consensus
    AppendEntriesRequest(AppendEntriesRequest),
    AppendEntriesResponse(AppendEntriesResponse),
    RequestVoteRequest(RequestVoteRequest),
    RequestVoteResponse(RequestVoteResponse),
    // Pre-vote (Raft pre-vote extension). A candidate probes whether peers
    // WOULD grant a vote at a hypothetical term before bumping its own term
    // and running a real election — avoids disruptive term inflation from a
    // partitioned node.
    PreVoteRequest(PreVoteRequest),
    PreVoteResponse(PreVoteResponse),
    TimeoutNowRequest(TimeoutNowRequest),
    InstallSnapshotRequest(InstallSnapshotRequest),
    InstallSnapshotResponse(InstallSnapshotResponse),
    // Cluster management
    JoinRequest(JoinRequest),
    JoinResponse(JoinResponse),
    // Health check
    Ping(PingRequest),
    Pong(PongResponse),
    // Topology broadcast
    TopologyUpdate(TopologyUpdate),
    TopologyAck(TopologyAck),
    // Discriminants 13/14 (ForwardRequest/ForwardResponse) retired in C-δ.6.
    // VShardEnvelope
    VShardEnvelope(Vec<u8>),
    // Metadata-group proposal forwarding (group 0)
    MetadataProposeRequest(MetadataProposeRequest),
    MetadataProposeResponse(MetadataProposeResponse),
    // Physical-plan execution (Batch C-β onwards)
    ExecuteRequest(ExecuteRequest),
    ExecuteResponse(ExecuteResponse),
    // Streaming physical-plan execution (L4). The request reuses the
    // `ExecuteRequest` body; the chunk/end frames form the multi-frame
    // response sequence on the same QUIC stream.
    ExecuteStreamRequest(ExecuteRequest),
    ExecuteStreamChunk(ExecuteStreamChunk),
    ExecuteStreamEnd(ExecuteStreamEnd),
    // Cross-node streaming shuffle (E1). The request opens the stream; the
    // chunk/end frames are pushed producer → receiver on the same QUIC stream
    // (opposite direction from ExecuteStream, which streams the response back).
    ShufflePushRequest(ShufflePushRequest),
    ShufflePushChunk(ShufflePushChunk),
    ShufflePushEnd(ShufflePushEnd),
    // Cross-node shuffle PRODUCER trigger (E4a). The coordinator sends a
    // `ShuffleProduceRequest`; the producer node runs a local scan, fans the
    // hash-partitioned rows out as `ShufflePush` streams, and replies with one
    // terminal `ShuffleProduceResponse`.
    ShuffleProduceRequest(ShuffleProduceRequest),
    ShuffleProduceResponse(ShuffleProduceResponse),
    // Cross-node shuffle CONSUMER trigger (E4b). The coordinator sends a
    // `ShuffleConsumeRequest` to a part-owner; that node waits for both staged
    // sides of its part to finalize, runs the node-local grace-hash join, and
    // replies with one `ShuffleConsumeResponse` carrying the join rows.
    ShuffleConsumeRequest(ShuffleConsumeRequest),
    ShuffleConsumeResponse(ShuffleConsumeResponse),
    // Cross-node distributed GROUP BY shuffle CONSUMER trigger (E5b). The
    // single-sided aggregate sibling of `ShuffleConsume*`: the coordinator sends
    // a `ShuffleAggregateConsumeRequest` to a part-owner; that node waits for its
    // part's single staged producer side to finalize, merges + finalizes the
    // partial GroupStates, and replies with one `ShuffleAggregateConsumeResponse`
    // carrying the aggregate rows.
    ShuffleAggregateConsumeRequest(ShuffleAggregateConsumeRequest),
    ShuffleAggregateConsumeResponse(ShuffleAggregateConsumeResponse),
    // Routed-surrogate-exchange (F1b). A coordinator planning a cross-shard
    // graph edge sends an `AssignSurrogateRequest` to the home vShard's leader;
    // that leader assign-or-returns the authoritative surrogate for the
    // `(collection, pk)` endpoint key and replies with one
    // `AssignSurrogateResponse`.
    AssignSurrogateRequest(AssignSurrogateRequest),
    AssignSurrogateResponse(AssignSurrogateResponse),
    // Routed Calvin-submit (Cv1). A coordinator that is NOT the sequencer-group
    // leader sends a `SubmitCalvinTxnRequest` carrying a msgpack-encoded
    // `TxClass` to the leader; the leader submits it to its local Calvin
    // sequencer inbox, awaits assignment + completion, and replies with one
    // `SubmitCalvinTxnResponse` carrying success or a typed error.
    SubmitCalvinTxnRequest(SubmitCalvinTxnRequest),
    SubmitCalvinTxnResponse(SubmitCalvinTxnResponse),
    // Routed Calvin-INBOX submit (Cv1). The OLLP dependent sibling of
    // `SubmitCalvinTxn*`: a coordinator that is NOT the sequencer-group leader
    // sends a `SubmitCalvinInboxRequest` carrying a msgpack-encoded dependent
    // `TxClass` to the leader; the leader submits it to its local Calvin
    // sequencer inbox, awaits only the ASSIGNMENT (NOT completion), and replies
    // with one `SubmitCalvinInboxResponse` carrying the assignment or a typed
    // error.
    SubmitCalvinInboxRequest(SubmitCalvinInboxRequest),
    SubmitCalvinInboxResponse(SubmitCalvinInboxResponse),
    // Routed reserve-read (Calvin OLLP). A coordinator sends a
    // `ReserveReadRequest` carrying a msgpack-encoded `LockKeyWire` to the
    // sequencer-group leader; the leader assign-only reserves the read lock
    // and replies with one `ReserveReadResponse` carrying the minted owner
    // (msgpack-encoded `TxnIdWire`) or a typed error.
    ReserveReadRequest(ReserveReadRequest),
    ReserveReadResponse(ReserveReadResponse),
    // Routed release-reservation (Calvin OLLP). The ack-only sibling of
    // `ReserveRead*`: a coordinator sends a `ReleaseReservationRequest`
    // carrying the msgpack-encoded owner and release reason to the
    // sequencer-group leader; the leader releases the reservation and
    // replies with one `ReleaseReservationResponse` carrying success or a
    // typed error.
    ReleaseReservationRequest(ReleaseReservationRequest),
    ReleaseReservationResponse(ReleaseReservationResponse),
    // Data-group proposal forwarding (groups 1+)
    DataProposeRequest(DataProposeRequest),
    DataProposeResponse(DataProposeResponse),
    // Routed read index. A node that does not lead a group asks the leader
    // for a read index confirmed against a quorum.
    ReadIndexRequest(ReadIndexRequest),
    ReadIndexResponse(ReadIndexResponse),
    // Authorization lease renewal and the writer-side barrier, both answered
    // by the metadata group leader.
    AuthLeaseRenewRequest(AuthLeaseRenewRequest),
    AuthLeaseRenewResponse(AuthLeaseRenewResponse),
    AuthBarrierRequest(AuthBarrierRequest),
    AuthBarrierResponse(AuthBarrierResponse),
}

/// Encode a [`RaftRpc`] into a framed binary message stamped with `epoch`.
///
/// `epoch` is the sending node's own state; the frame carries the generation
/// that node has applied.
pub fn encode(rpc: &RaftRpc, epoch: &crate::cluster_epoch::ClusterEpochState) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(HEADER_SIZE + 64);
    match rpc {
        RaftRpc::AppendEntriesRequest(m) => raft_msgs::encode_append_entries_req(m, &mut out),
        RaftRpc::AppendEntriesResponse(m) => raft_msgs::encode_append_entries_resp(m, &mut out),
        RaftRpc::RequestVoteRequest(m) => raft_msgs::encode_request_vote_req(m, &mut out),
        RaftRpc::RequestVoteResponse(m) => raft_msgs::encode_request_vote_resp(m, &mut out),
        RaftRpc::PreVoteRequest(m) => raft_msgs::encode_pre_vote_req(m, &mut out),
        RaftRpc::PreVoteResponse(m) => raft_msgs::encode_pre_vote_resp(m, &mut out),
        RaftRpc::TimeoutNowRequest(m) => raft_msgs::encode_timeout_now_req(m, &mut out),
        RaftRpc::InstallSnapshotRequest(m) => raft_msgs::encode_install_snapshot_req(m, &mut out),
        RaftRpc::InstallSnapshotResponse(m) => raft_msgs::encode_install_snapshot_resp(m, &mut out),
        RaftRpc::JoinRequest(m) => cluster_mgmt::encode_join_req(m, &mut out),
        RaftRpc::JoinResponse(m) => cluster_mgmt::encode_join_resp(m, &mut out),
        RaftRpc::Ping(m) => cluster_mgmt::encode_ping(m, &mut out),
        RaftRpc::Pong(m) => cluster_mgmt::encode_pong(m, &mut out),
        RaftRpc::TopologyUpdate(m) => cluster_mgmt::encode_topology_update(m, &mut out),
        RaftRpc::TopologyAck(m) => cluster_mgmt::encode_topology_ack(m, &mut out),
        RaftRpc::VShardEnvelope(bytes) => vshard::encode_vshard_envelope(bytes, &mut out),
        RaftRpc::MetadataProposeRequest(m) => metadata::encode_metadata_propose_req(m, &mut out),
        RaftRpc::MetadataProposeResponse(m) => metadata::encode_metadata_propose_resp(m, &mut out),
        RaftRpc::ExecuteRequest(m) => execute::encode_execute_req(m, &mut out),
        RaftRpc::ExecuteResponse(m) => execute::encode_execute_resp(m, &mut out),
        RaftRpc::ExecuteStreamRequest(m) => execute::encode_execute_stream_req(m, &mut out),
        RaftRpc::ExecuteStreamChunk(m) => execute::encode_execute_stream_chunk(m, &mut out),
        RaftRpc::ExecuteStreamEnd(m) => execute::encode_execute_stream_end(m, &mut out),
        RaftRpc::ShufflePushRequest(m) => shuffle::encode_shuffle_push_req(m, &mut out),
        RaftRpc::ShufflePushChunk(m) => shuffle::encode_shuffle_push_chunk(m, &mut out),
        RaftRpc::ShufflePushEnd(m) => shuffle::encode_shuffle_push_end(m, &mut out),
        RaftRpc::ShuffleProduceRequest(m) => shuffle::encode_shuffle_produce_req(m, &mut out),
        RaftRpc::ShuffleProduceResponse(m) => shuffle::encode_shuffle_produce_resp(m, &mut out),
        RaftRpc::ShuffleConsumeRequest(m) => shuffle::encode_shuffle_consume_req(m, &mut out),
        RaftRpc::ShuffleConsumeResponse(m) => shuffle::encode_shuffle_consume_resp(m, &mut out),
        RaftRpc::ShuffleAggregateConsumeRequest(m) => {
            shuffle::encode_shuffle_agg_consume_req(m, &mut out)
        }
        RaftRpc::ShuffleAggregateConsumeResponse(m) => {
            shuffle::encode_shuffle_agg_consume_resp(m, &mut out)
        }
        RaftRpc::AssignSurrogateRequest(m) => surrogate::encode_assign_surrogate_req(m, &mut out),
        RaftRpc::AssignSurrogateResponse(m) => surrogate::encode_assign_surrogate_resp(m, &mut out),
        RaftRpc::SubmitCalvinTxnRequest(m) => {
            calvin_submit::encode_submit_calvin_txn_req(m, &mut out)
        }
        RaftRpc::SubmitCalvinTxnResponse(m) => {
            calvin_submit::encode_submit_calvin_txn_resp(m, &mut out)
        }
        RaftRpc::SubmitCalvinInboxRequest(m) => {
            calvin_submit::encode_submit_calvin_inbox_req(m, &mut out)
        }
        RaftRpc::SubmitCalvinInboxResponse(m) => {
            calvin_submit::encode_submit_calvin_inbox_resp(m, &mut out)
        }
        RaftRpc::ReserveReadRequest(m) => reservation::encode_reserve_read_req(m, &mut out),
        RaftRpc::ReserveReadResponse(m) => reservation::encode_reserve_read_resp(m, &mut out),
        RaftRpc::ReleaseReservationRequest(m) => {
            reservation::encode_release_reservation_req(m, &mut out)
        }
        RaftRpc::ReleaseReservationResponse(m) => {
            reservation::encode_release_reservation_resp(m, &mut out)
        }
        RaftRpc::DataProposeRequest(m) => data_propose::encode_data_propose_req(m, &mut out),
        RaftRpc::DataProposeResponse(m) => data_propose::encode_data_propose_resp(m, &mut out),
        RaftRpc::ReadIndexRequest(m) => read_index::encode_read_index_req(m, &mut out),
        RaftRpc::ReadIndexResponse(m) => read_index::encode_read_index_resp(m, &mut out),
        RaftRpc::AuthLeaseRenewRequest(m) => auth_lease::encode_renew_req(m, &mut out),
        RaftRpc::AuthLeaseRenewResponse(m) => auth_lease::encode_renew_resp(m, &mut out),
        RaftRpc::AuthBarrierRequest(m) => auth_lease::encode_barrier_req(m, &mut out),
        RaftRpc::AuthBarrierResponse(m) => auth_lease::encode_barrier_resp(m, &mut out),
    }?;
    super::header::stamp_epoch(&mut out, epoch)?;
    Ok(out)
}

/// Encode a [`RaftRpc`] and wrap the framed bytes in a v2 versioned envelope.
///
/// The outer envelope allows peers to detect and reject incompatible future
/// wire versions rather than silently misinterpreting them.
pub fn versioned_encode(
    rpc: &RaftRpc,
    epoch: &crate::cluster_epoch::ClusterEpochState,
) -> Result<Vec<u8>> {
    let framed = encode(rpc, epoch)?;
    wrap_bytes_versioned(&framed).map_err(|e| ClusterError::Codec {
        detail: format!("RaftRpc versioned encode: {e}"),
    })
}

/// Decode a versioned-envelope-wrapped [`RaftRpc`] frame.
///
/// Accepts both v2 versioned envelopes and raw v1 (pre-versioning) frames;
/// rejects envelopes with unsupported future version numbers.
///
/// The v1 fallback preserves wire compat with peers running pre-versioning
/// cluster code: if the bytes do not match the `fixarray(2)` envelope shape,
/// they are passed directly to [`decode`] as-is.
pub fn versioned_decode(
    data: &[u8],
    epoch: &crate::cluster_epoch::ClusterEpochState,
) -> Result<RaftRpc> {
    let inner = unwrap_bytes_versioned(data).map_err(|e| ClusterError::Codec {
        detail: format!("RaftRpc versioned decode: {e}"),
    })?;
    decode(inner, epoch)
}

/// Decode a framed binary message into a [`RaftRpc`], recording the sender's
/// stamped epoch as an observation in `epoch`.
pub fn decode(data: &[u8], epoch: &crate::cluster_epoch::ClusterEpochState) -> Result<RaftRpc> {
    let (rpc_type, payload) = super::header::parse_frame(data, epoch)?;
    match rpc_type {
        RPC_APPEND_ENTRIES_REQ => raft_msgs::decode_append_entries_req(payload),
        RPC_APPEND_ENTRIES_RESP => raft_msgs::decode_append_entries_resp(payload),
        RPC_REQUEST_VOTE_REQ => raft_msgs::decode_request_vote_req(payload),
        RPC_REQUEST_VOTE_RESP => raft_msgs::decode_request_vote_resp(payload),
        RPC_PRE_VOTE_REQ => raft_msgs::decode_pre_vote_req(payload),
        RPC_PRE_VOTE_RESP => raft_msgs::decode_pre_vote_resp(payload),
        RPC_TIMEOUT_NOW_REQ => raft_msgs::decode_timeout_now_req(payload),
        RPC_INSTALL_SNAPSHOT_REQ => raft_msgs::decode_install_snapshot_req(payload),
        RPC_INSTALL_SNAPSHOT_RESP => raft_msgs::decode_install_snapshot_resp(payload),
        RPC_JOIN_REQ => cluster_mgmt::decode_join_req(payload),
        RPC_JOIN_RESP => cluster_mgmt::decode_join_resp(payload),
        RPC_PING => cluster_mgmt::decode_ping(payload),
        RPC_PONG => cluster_mgmt::decode_pong(payload),
        RPC_TOPOLOGY_UPDATE => cluster_mgmt::decode_topology_update(payload),
        RPC_TOPOLOGY_ACK => cluster_mgmt::decode_topology_ack(payload),
        // Discriminants 13/14 (ForwardRequest/ForwardResponse) are retired.
        // A node receiving these has a peer still running an older version.
        // Return a typed error so the operator sees a clear message.
        RPC_FORWARD_REQ | RPC_FORWARD_RESP => Err(ClusterError::Codec {
            detail: format!(
                "rpc_type {rpc_type} is a retired wire variant (ForwardRequest/ForwardResponse, \
                 retired in C-δ.6); upgrade all cluster nodes to remove this peer"
            ),
        }),
        RPC_VSHARD_ENVELOPE => vshard::decode_vshard_envelope(payload),
        RPC_METADATA_PROPOSE_REQ => metadata::decode_metadata_propose_req(payload),
        RPC_METADATA_PROPOSE_RESP => metadata::decode_metadata_propose_resp(payload),
        RPC_EXECUTE_REQ => execute::decode_execute_req(payload),
        RPC_EXECUTE_RESP => execute::decode_execute_resp(payload),
        RPC_EXECUTE_STREAM_REQ => execute::decode_execute_stream_req(payload),
        RPC_EXECUTE_STREAM_CHUNK => execute::decode_execute_stream_chunk(payload),
        RPC_EXECUTE_STREAM_END => execute::decode_execute_stream_end(payload),
        RPC_SHUFFLE_PUSH_REQ => shuffle::decode_shuffle_push_req(payload),
        RPC_SHUFFLE_PUSH_CHUNK => shuffle::decode_shuffle_push_chunk(payload),
        RPC_SHUFFLE_PUSH_END => shuffle::decode_shuffle_push_end(payload),
        RPC_SHUFFLE_PRODUCE_REQ => shuffle::decode_shuffle_produce_req(payload),
        RPC_SHUFFLE_PRODUCE_RESP => shuffle::decode_shuffle_produce_resp(payload),
        RPC_SHUFFLE_CONSUME_REQ => shuffle::decode_shuffle_consume_req(payload),
        RPC_SHUFFLE_CONSUME_RESP => shuffle::decode_shuffle_consume_resp(payload),
        RPC_SHUFFLE_AGG_CONSUME_REQ => shuffle::decode_shuffle_agg_consume_req(payload),
        RPC_SHUFFLE_AGG_CONSUME_RESP => shuffle::decode_shuffle_agg_consume_resp(payload),
        RPC_ASSIGN_SURROGATE_REQ => surrogate::decode_assign_surrogate_req(payload),
        RPC_ASSIGN_SURROGATE_RESP => surrogate::decode_assign_surrogate_resp(payload),
        RPC_SUBMIT_CALVIN_TXN_REQ => calvin_submit::decode_submit_calvin_txn_req(payload),
        RPC_SUBMIT_CALVIN_TXN_RESP => calvin_submit::decode_submit_calvin_txn_resp(payload),
        RPC_SUBMIT_CALVIN_INBOX_REQ => calvin_submit::decode_submit_calvin_inbox_req(payload),
        RPC_SUBMIT_CALVIN_INBOX_RESP => calvin_submit::decode_submit_calvin_inbox_resp(payload),
        RPC_RESERVE_READ_REQ => reservation::decode_reserve_read_req(payload),
        RPC_RESERVE_READ_RESP => reservation::decode_reserve_read_resp(payload),
        RPC_RELEASE_RESERVATION_REQ => reservation::decode_release_reservation_req(payload),
        RPC_RELEASE_RESERVATION_RESP => reservation::decode_release_reservation_resp(payload),
        RPC_DATA_PROPOSE_REQ => data_propose::decode_data_propose_req(payload),
        RPC_DATA_PROPOSE_RESP => data_propose::decode_data_propose_resp(payload),
        RPC_READ_INDEX_REQ => read_index::decode_read_index_req(payload),
        RPC_READ_INDEX_RESP => read_index::decode_read_index_resp(payload),
        RPC_AUTH_LEASE_RENEW_REQ => auth_lease::decode_renew_req(payload),
        RPC_AUTH_LEASE_RENEW_RESP => auth_lease::decode_renew_resp(payload),
        RPC_AUTH_BARRIER_REQ => auth_lease::decode_barrier_req(payload),
        RPC_AUTH_BARRIER_RESP => auth_lease::decode_barrier_resp(payload),
        _ => Err(ClusterError::Codec {
            detail: format!("unknown rpc_type: {rpc_type}"),
        }),
    }
}

/// Return the total frame size for a buffer that starts with a valid header.
pub fn frame_size(header: &[u8; HEADER_SIZE]) -> Result<usize> {
    super::header::frame_size(header)
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_raft::message::{AppendEntriesResponse, RequestVoteResponse};

    #[test]
    fn crc_corruption_detected() {
        let rpc = RaftRpc::RequestVoteResponse(RequestVoteResponse {
            term: 1,
            vote_granted: false,
        });
        let mut encoded =
            encode(&rpc, &crate::cluster_epoch::ClusterEpochState::default()).unwrap();
        if let Some(last) = encoded.last_mut() {
            *last ^= 0x01;
        }
        let err = decode(
            &encoded,
            &crate::cluster_epoch::ClusterEpochState::default(),
        )
        .unwrap_err();
        assert!(err.to_string().contains("CRC32C mismatch"), "{err}");
    }

    #[test]
    fn version_mismatch_rejected() {
        let rpc = RaftRpc::RequestVoteResponse(RequestVoteResponse {
            term: 1,
            vote_granted: false,
        });
        let mut encoded =
            encode(&rpc, &crate::cluster_epoch::ClusterEpochState::default()).unwrap();
        encoded[0] = 99;
        let err = decode(
            &encoded,
            &crate::cluster_epoch::ClusterEpochState::default(),
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("unsupported wire version"),
            "{err}"
        );
    }

    #[test]
    fn truncated_frame_rejected() {
        // Version byte 3 (RPC_FRAME_VERSION) passes version check; then header
        // size check fires because we only have 3 bytes total.
        let err = decode(
            &[3, 2, 3],
            &crate::cluster_epoch::ClusterEpochState::default(),
        )
        .unwrap_err();
        assert!(err.to_string().contains("frame too short"), "{err}");
    }

    #[test]
    fn unknown_rpc_type_rejected() {
        let rpc = RaftRpc::RequestVoteResponse(RequestVoteResponse {
            term: 1,
            vote_granted: false,
        });
        let mut encoded =
            encode(&rpc, &crate::cluster_epoch::ClusterEpochState::default()).unwrap();
        encoded[1] = 255;
        let err = decode(
            &encoded,
            &crate::cluster_epoch::ClusterEpochState::default(),
        )
        .unwrap_err();
        assert!(err.to_string().contains("unknown rpc_type"), "{err}");
    }

    #[test]
    fn payload_too_large_rejected() {
        use super::super::header::MAX_RPC_PAYLOAD_SIZE;
        let mut frame = vec![0u8; HEADER_SIZE];
        frame[0] = 3u8; // RPC_FRAME_VERSION
        frame[1] = RPC_APPEND_ENTRIES_REQ;
        let huge: u32 = MAX_RPC_PAYLOAD_SIZE + 1;
        frame[2..6].copy_from_slice(&huge.to_le_bytes());
        let err = decode(&frame, &crate::cluster_epoch::ClusterEpochState::default()).unwrap_err();
        assert!(err.to_string().contains("exceeds maximum"), "{err}");
    }

    #[test]
    fn frame_size_helper() {
        let rpc = RaftRpc::AppendEntriesResponse(AppendEntriesResponse {
            term: 1,
            success: true,
            last_log_index: 5,
        });
        let encoded = encode(&rpc, &crate::cluster_epoch::ClusterEpochState::default()).unwrap();
        let header: [u8; HEADER_SIZE] = encoded[..HEADER_SIZE].try_into().unwrap();
        let size = frame_size(&header).unwrap();
        assert_eq!(size, encoded.len());
    }
}
