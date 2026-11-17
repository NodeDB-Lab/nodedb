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
//!   `encode_binary_rows`) plus the context-stated row splitters
//!   `scan_rows_in_context` / `flatten_rows_in_context`.
//! - `search_hits` — flatteners from gathered vector / hybrid hit arrays to
//!   bare relational rows.
//! - `rows` — row-context decoders: under a caller-stated `RowContext`,
//!   native object rows decode to typed values or render as JSON.
//! - `arrow` — Arrow IPC encoder for columnar transport.
//! - `stage_images` — the `RETURNING` images a Calvin stage answers with.
//! - `hits` — hit-row structs (Vector / Text / Hybrid search, GraphRag, etc.)
//!   plus their `ToMessagePack` impls.

mod arrow;
mod decode;
mod encode;
mod graph_rag;
mod hits;
mod raw;
mod rows;
mod search_hits;
mod stage_images;

pub use arrow::encode_as_arrow_ipc;
pub(in crate::data::executor) use decode::decode_response_to_docs;
pub use encode::{decode_payload, decode_payload_to_json, decode_payload_value};
pub(in crate::data::executor) use encode::{
    encode, encode_json_as_msgpack, encode_json_vec_as_msgpack, encode_serde,
};
pub(crate) use encode::{
    encode_affected, encode_affected_with_op, encode_count, encode_native_rows,
};
pub(crate) use graph_rag::{GraphRagMetadata, GraphRagResponse, GraphRagResult};
#[allow(unused_imports)]
pub(crate) use hits::ArrayAggregateResponse;
pub(crate) use hits::{
    ArraySliceResponse, IngestRejection, RejectingRowsPayload, ReturningRowsReply, RowsPayload,
    StagedReturningReply,
};
pub(in crate::data::executor) use hits::{
    DocumentRow, HybridSearchHit, NeighborEntry, NeighborMultiEntry, SubgraphEdge, VectorSearchHit,
};
pub(crate) use raw::encode_raw_document_rows;
pub use raw::{encode_binary_rows, flatten_rows_in_context, scan_rows_in_context};
pub use rows::{
    RowContext, decode_body_plain_json, decode_row_body, decode_row_payload_to_json,
    decode_row_payload_value, is_native_row, render_native_body_json,
};
pub use search_hits::{
    flatten_hybrid_hits_to_relational_rows, flatten_vector_hits_to_relational_rows,
    hybrid_hit_surrogates, vector_hit_surrogates,
};
pub(crate) use stage_images::{StagedImageRow, StagedPlanImages, StagedReturningImages};
