// SPDX-License-Identifier: Apache-2.0

//! Zero-deserialization binary scanner for MessagePack documents.
//!
//! Operates directly on `&[u8]` MessagePack bytes without decoding into
//! `serde_json::Value` or `nodedb_types::Value`. Field extraction, numeric
//! reads, comparisons, and hashing all work on raw byte offsets.

pub mod aggregate;
pub mod aggregate_helpers;
pub mod compare;
pub mod field;
pub mod filter;
pub mod group_key;
pub mod index;
pub mod kv_row;
pub mod reader;
pub mod sidecar;
pub mod writer;

pub use aggregate::compute_aggregate_binary;
pub use compare::{compare_field_bytes, hash_field_bytes};
pub use field::{extract_field, extract_path};
pub use group_key::build_group_key;
pub use index::FieldIndex;
pub use kv_row::kv_row_msgpack;
pub use reader::{
    array_header, map_header, read_bin_advance, read_bool, read_f64, read_i64, read_null, read_str,
    read_str_advance, read_u32_advance, read_value, skip_value,
};
pub use sidecar::{
    SidecarEntry, SidecarFieldIndex, build_sidecar, field_index_from_sidecar, has_sidecar,
    msgpack_bytes, sidecar_lookup,
};
pub use writer::{
    build_str_map, inject_str_field, merge_fields, write_array_header, write_bin, write_bool,
    write_f64, write_i64, write_instant, write_kv_bool, write_kv_f64, write_kv_i64,
    write_kv_instant, write_kv_null, write_kv_raw, write_kv_str, write_map_header, write_null,
    write_str,
};
