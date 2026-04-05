//! Zero-deserialization binary scanner for MessagePack documents.
//!
//! Operates directly on `&[u8]` MessagePack bytes without decoding into
//! `serde_json::Value` or `nodedb_types::Value`. Field extraction, numeric
//! reads, comparisons, and hashing all work on raw byte offsets.

pub mod compare;
pub mod field;
pub mod reader;

pub use compare::{compare_field_bytes, hash_field_bytes};
pub use field::{extract_field, extract_path};
pub use reader::{array_header, read_bool, read_f64, read_i64, read_null, read_str, skip_value};
