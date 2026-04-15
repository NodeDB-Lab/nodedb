//! VShardEnvelope RPC glue.
//!
//! The VShardEnvelope carries graph BSP, timeseries scatter-gather, migration,
//! retention, and archival messages. The inner VShardMessageType determines
//! the handler. The envelope bytes are passed through raw (already serialized
//! in their own binary format).

use super::discriminants::RPC_VSHARD_ENVELOPE;
use super::header::write_frame;
use super::raft_rpc::RaftRpc;
use crate::error::Result;

pub(super) fn encode_vshard_envelope(bytes: &[u8], out: &mut Vec<u8>) -> Result<()> {
    write_frame(RPC_VSHARD_ENVELOPE, bytes, out)
}

pub(super) fn decode_vshard_envelope(payload: &[u8]) -> Result<RaftRpc> {
    // VShardEnvelope is already in its own binary format — pass through raw.
    Ok(RaftRpc::VShardEnvelope(payload.to_vec()))
}
