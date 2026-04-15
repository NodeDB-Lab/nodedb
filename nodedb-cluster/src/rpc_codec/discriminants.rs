//! RPC type discriminant constants.
//!
//! All constants MUST remain stable across versions — they appear on the
//! wire. Adding new constants is fine; changing existing ones breaks
//! binary compatibility.

pub const RPC_APPEND_ENTRIES_REQ: u8 = 1;
pub const RPC_APPEND_ENTRIES_RESP: u8 = 2;
pub const RPC_REQUEST_VOTE_REQ: u8 = 3;
pub const RPC_REQUEST_VOTE_RESP: u8 = 4;
pub const RPC_INSTALL_SNAPSHOT_REQ: u8 = 5;
pub const RPC_INSTALL_SNAPSHOT_RESP: u8 = 6;
pub const RPC_JOIN_REQ: u8 = 7;
pub const RPC_JOIN_RESP: u8 = 8;
pub const RPC_PING: u8 = 9;
pub const RPC_PONG: u8 = 10;
pub const RPC_TOPOLOGY_UPDATE: u8 = 11;
pub const RPC_TOPOLOGY_ACK: u8 = 12;
/// Retired in Phase C-δ.6: reserved, do not reuse — was ForwardRequest/Response
/// (SQL-string forwarding path replaced by gateway.execute / ExecuteRequest).
#[allow(dead_code)]
pub const RPC_FORWARD_REQ: u8 = 13;
/// Retired in Phase C-δ.6: reserved, do not reuse — was ForwardRequest/Response
/// (SQL-string forwarding path replaced by gateway.execute / ExecuteRequest).
#[allow(dead_code)]
pub const RPC_FORWARD_RESP: u8 = 14;
pub const RPC_VSHARD_ENVELOPE: u8 = 15;
pub const RPC_METADATA_PROPOSE_REQ: u8 = 16;
pub const RPC_METADATA_PROPOSE_RESP: u8 = 17;
pub const RPC_EXECUTE_REQ: u8 = 18;
pub const RPC_EXECUTE_RESP: u8 = 19;
