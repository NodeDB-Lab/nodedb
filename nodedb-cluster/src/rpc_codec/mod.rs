//! Raft RPC binary codec — split into logical sub-modules.
//!
//! Public interface mirrors the old flat `rpc_codec.rs`:
//!   - `encode(rpc) -> Result<Vec<u8>>`
//!   - `decode(data) -> Result<RaftRpc>`
//!   - `frame_size(header) -> Result<usize>`
//!   - All wire types re-exported from their sub-modules.

pub mod auth_envelope;
pub mod cluster_mgmt;
pub mod data_propose;
pub mod discriminants;
pub mod execute;
pub mod header;
pub mod mac;
pub mod metadata;
pub mod peer_seq;
pub mod raft_msgs;
pub mod raft_rpc;
pub mod vshard;

pub use auth_envelope::{
    ENVELOPE_OVERHEAD, ENVELOPE_VERSION, EnvelopeFields, parse_envelope, write_envelope,
};
pub use cluster_mgmt::{
    JoinGroupInfo, JoinNodeInfo, JoinRequest, JoinResponse, LEADER_REDIRECT_PREFIX, PingRequest,
    PongResponse, TopologyAck, TopologyUpdate,
};
pub use data_propose::{DataProposeRequest, DataProposeResponse};
pub use execute::{
    DescriptorVersionEntry, ExecuteRequest, ExecuteResponse, PLAN_DECODE_FAILED, TypedClusterError,
};
pub use header::{HEADER_SIZE, MAX_RPC_PAYLOAD_SIZE};
pub use mac::{MAC_LEN, MacKey};
pub use metadata::{MetadataProposeRequest, MetadataProposeResponse};
pub use peer_seq::{PeerSeqSender, PeerSeqWindow, REPLAY_WINDOW};
pub use raft_rpc::{RaftRpc, decode, encode, frame_size};
