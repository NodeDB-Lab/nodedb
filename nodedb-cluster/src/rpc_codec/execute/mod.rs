// SPDX-License-Identifier: BUSL-1.1

//! ExecuteRequest / ExecuteResponse — cross-node physical-plan execution RPC.
//!
//! Discriminants 18 and 19 are permanently assigned to these variants.

pub mod codec;
pub mod types;

pub use types::{
    DescriptorVersionEntry, ExecuteRequest, ExecuteResponse, ExecuteStreamChunk, ExecuteStreamEnd,
    ForwardedStream, PLAN_DECODE_FAILED, TypedClusterError,
};

pub(in crate::rpc_codec) use codec::{
    decode_execute_req, decode_execute_resp, decode_execute_stream_chunk,
    decode_execute_stream_end, decode_execute_stream_req, encode_execute_req, encode_execute_resp,
    encode_execute_stream_chunk, encode_execute_stream_end, encode_execute_stream_req,
};
