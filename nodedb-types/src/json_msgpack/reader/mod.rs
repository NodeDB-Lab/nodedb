// SPDX-License-Identifier: Apache-2.0

//! Cursor-based msgpack → `serde_json::Value` and `nodedb_types::Value` readers.
//!
//! Deterministic raw byte parser — the first byte of each msgpack value
//! unambiguously identifies its type per the msgpack specification.

pub mod cursor;
pub mod json;
pub mod native;

pub(crate) use cursor::Cursor;
pub(crate) use json::base64_encode;
pub use json::json_from_msgpack;
pub use native::value_from_msgpack;
