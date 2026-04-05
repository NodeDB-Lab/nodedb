//! Zero-deserialization binary scanner for MessagePack documents.
//!
//! Operates directly on `&[u8]` MessagePack bytes without decoding into
//! `serde_json::Value` or `nodedb_types::Value`. Field extraction, numeric
//! reads, comparisons, and hashing all work on raw byte offsets.

pub mod aggregate;
pub mod compare;
pub mod field;
pub mod filter;
pub mod group_key;
pub mod index;
pub mod reader;
pub mod sidecar;

pub use aggregate::compute_aggregate_binary;
pub use compare::{compare_field_bytes, hash_field_bytes};
pub use field::{extract_field, extract_path};
pub use group_key::build_group_key;
pub use index::FieldIndex;
pub use reader::{
    array_header, map_header, read_bool, read_f64, read_i64, read_null, read_str, read_value,
    skip_value,
};
pub use sidecar::{
    SidecarEntry, SidecarFieldIndex, build_sidecar, field_index_from_sidecar, has_sidecar,
    msgpack_bytes, sidecar_lookup,
};
