// SPDX-License-Identifier: Apache-2.0

//! Msgpack → `serde_json::Value` and `nodedb_types::Value` readers.
//!
//! The JSON reader walks the crate's own `Cursor`. The native reader is
//! generic over `zerompk::Read` and is shared with `NativeCell`. Both are
//! deterministic raw byte parsers — the first byte of each msgpack value
//! unambiguously identifies its type per the msgpack specification.

pub mod cursor;
pub mod json;
pub mod native;

pub(crate) use cursor::Cursor;
pub(crate) use json::base64_encode;
pub use json::json_from_msgpack;
pub use native::value_from_msgpack;
