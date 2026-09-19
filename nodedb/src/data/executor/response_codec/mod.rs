// SPDX-License-Identifier: BUSL-1.1

//! Response payload serialization for SPSC bridge transport.
//!
//! Replaces `serde_json::to_vec` + `serde_json::json!` on all Data Plane
//! response hot paths. Uses MessagePack (`zerompk`) for serialization which
//! is 2-3x faster and 30-50% smaller than JSON.
//!
//! Every encoder here emits MessagePack, including the ones named for the
//! `serde_json::Value` they take. `decode_payload` / `decode_payload_to_json`
//! are their counterparts — see `encode`'s module docs.
//!
//! Split by concern:
//!
//! - `encode` — generic encoders + the `decode_payload` /
//!   `decode_payload_to_json` / `decode_payload_value` counterparts used at
//!   the Control Plane boundary.
//! - `decode` — payload→docs decoders for inline sub-plans (e.g. multi-way
//!   joins consuming an inner-join Response).
//! - `raw` — raw-msgpack passthrough encoders (`encode_raw_document_rows`,
//!   `encode_binary_rows`) plus `decode_raw_scan_to_docs`.
//! - `arrow` — Arrow IPC encoder for columnar transport.
//! - `hits` — hit-row structs (Vector / Text / Hybrid search, GraphRag, etc.)
//!   plus their `ToMessagePack` impls.

mod arrow;
mod decode;
mod encode;
mod hits;
mod raw;

pub use arrow::encode_as_arrow_ipc;
pub(in crate::data::executor) use decode::decode_response_to_docs;
pub use encode::{decode_payload, decode_payload_to_json, decode_payload_value};
pub(in crate::data::executor) use encode::{
    encode, encode_json_as_msgpack, encode_json_vec_as_msgpack, encode_serde, encode_value_vec,
};
pub(crate) use encode::{encode_affected, encode_affected_with_op, encode_count};
#[allow(unused_imports)]
pub(crate) use hits::ArrayAggregateResponse;
pub(crate) use hits::{ArraySliceResponse, RowsPayload};
pub(in crate::data::executor) use hits::{
    DocumentRow, GraphRagMetadata, GraphRagResponse, GraphRagResult, HybridSearchHit,
    NeighborEntry, NeighborMultiEntry, SubgraphEdge, VectorSearchHit,
};
pub(crate) use raw::{decode_raw_scan_to_docs, encode_raw_document_rows};
pub use raw::{
    encode_binary_rows, flatten_hybrid_hits_to_relational_rows, flatten_to_relational_rows,
    flatten_vector_hits_to_relational_rows,
};
