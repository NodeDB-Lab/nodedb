//! Raft RPC binary codec — split into logical sub-modules.
//!
//! Public interface mirrors the old flat `rpc_codec.rs`:
//!   - `encode(rpc) -> Result<Vec<u8>>`
//!   - `decode(data) -> Result<RaftRpc>`
//!   - `frame_size(header) -> Result<usize>`
//!   - All wire types re-exported from their sub-modules.

pub mod cluster_mgmt;
pub mod discriminants;
pub mod execute;
pub mod header;
pub mod metadata;
pub mod raft_msgs;
pub mod raft_rpc;
pub mod vshard;

pub use cluster_mgmt::{
    JoinGroupInfo, JoinNodeInfo, JoinRequest, JoinResponse, LEADER_REDIRECT_PREFIX, PingRequest,
    PongResponse, TopologyAck, TopologyUpdate,
};
pub use execute::{
    DescriptorVersionEntry, ExecuteRequest, ExecuteResponse, PLAN_DECODE_FAILED, TypedClusterError,
};
pub use header::{HEADER_SIZE, MAX_RPC_PAYLOAD_SIZE};
pub use metadata::{MetadataProposeRequest, MetadataProposeResponse};
pub use raft_rpc::{RaftRpc, decode, encode, frame_size};
