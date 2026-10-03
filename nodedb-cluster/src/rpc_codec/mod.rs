// SPDX-License-Identifier: BUSL-1.1

//! Raft RPC binary codec — split into logical sub-modules.
//!
//! Public interface mirrors the old flat `rpc_codec.rs`:
//!   - `encode(rpc) -> Result<Vec<u8>>`
//!   - `decode(data) -> Result<RaftRpc>`
//!   - `frame_size(header) -> Result<usize>`
//!   - All wire types re-exported from their sub-modules.

pub mod auth_envelope;
pub mod auth_lease;
pub mod calvin_parts;
pub mod calvin_submit;
pub mod cluster_mgmt;
pub mod data_plane_error;
pub mod data_propose;
pub mod discriminants;
pub mod execute;
pub mod frame_refusal;
pub mod header;
pub mod leader_status;
pub mod mac;
pub mod metadata;
pub mod peer_seq;
pub mod raft_msgs;
pub mod raft_rpc;
pub mod read_index;
pub mod request_refusal;
pub mod reservation;
pub mod shard_error;
pub mod shuffle;
pub mod surrogate;
pub mod vshard;

pub use auth_envelope::{
    ENVELOPE_OVERHEAD, ENVELOPE_VERSION, EnvelopeFields, parse_envelope, write_envelope,
};
pub use auth_lease::{
    AuthBarrierOutcome, AuthBarrierRequest, AuthBarrierResponse, AuthLeaseRenewOutcome,
    AuthLeaseRenewRequest, AuthLeaseRenewResponse, GroupCoverage,
};
pub use calvin_parts::{CalvinPartsRequest, CalvinPartsResponse, MAX_PARTS_BATCH_BYTES};
pub use calvin_submit::{
    SubmitCalvinInboxRequest, SubmitCalvinInboxResponse, SubmitCalvinTxnRequest,
    SubmitCalvinTxnResponse,
};
pub use cluster_mgmt::{
    JoinGroupInfo, JoinNodeInfo, JoinRequest, JoinResponse, LEADER_REDIRECT_PREFIX, PingRequest,
    PongResponse, TopologyAck, TopologyUpdate,
};
pub use data_plane_error::{
    DataPlaneCounterFault, DataPlaneErrorCode, DataPlaneSyncHold, DataPlaneTextColumnFault,
};
pub use data_propose::{
    DataProposeRequest, DataProposeResponse, ForwardedProposeRefusal, ProposeTarget,
};
pub use execute::{
    DescriptorVersionEntry, ExecuteRequest, ExecuteResponse, ExecuteStreamChunk, ExecuteStreamEnd,
    PLAN_DECODE_FAILED, TypedClusterError,
};
pub use frame_refusal::FrameRefusal;
pub use header::{HEADER_SIZE, MAX_RPC_PAYLOAD_SIZE};
pub use leader_status::{LeaderMembership, LeaderStatusRequest, LeaderStatusResponse};
pub use mac::{MAC_LEN, MacKey};
pub use metadata::{MetadataProposeRequest, MetadataProposeResponse};
pub use peer_seq::{BOOT_EPOCH_SHIFT, PeerSeqSender, PeerSeqWindow, REPLAY_WINDOW};
pub use raft_rpc::{RaftRpc, decode, encode, frame_size};
pub use read_index::{ReadIndexOutcome, ReadIndexRequest, ReadIndexResponse};
pub use request_refusal::{RefusalReason, RequestRefusal};
pub use reservation::{
    ReleaseReservationRequest, ReleaseReservationResponse, ReserveReadRequest, ReserveReadResponse,
};
pub use shard_error::{RaftErrorWire, ShardErrorWire};
pub use shuffle::{
    JoinKeyPair, PartNodeEntry, ShuffleAggregateConsumeRequest, ShuffleAggregateConsumeResponse,
    ShuffleConsumeRequest, ShuffleConsumeResponse, ShuffleProduceRequest, ShuffleProduceResponse,
    ShufflePushChunk, ShufflePushEnd, ShufflePushRequest, SortKey,
};
pub use surrogate::{AssignSurrogateRequest, AssignSurrogateResponse};
pub use vshard::VShardRefusal;
