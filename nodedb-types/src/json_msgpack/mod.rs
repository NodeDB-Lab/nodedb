// SPDX-License-Identifier: Apache-2.0

pub mod error;
pub mod instant_ext;
pub mod json_value;
pub mod native_cell;
pub mod reader;
pub mod transcoder;
pub mod writer;

pub use error::{MsgpackError, MsgpackResult};
pub use instant_ext::{
    EXT_INSTANT_NAIVE, EXT_INSTANT_UTC, INSTANT_EXT_LEN, InstantKind, instant_from_ext,
    read_instant, write_instant,
};
pub use json_value::JsonValue;
pub use native_cell::NativeCell;
pub use reader::{json_from_msgpack, value_from_msgpack};
pub use transcoder::msgpack_to_json_string;
pub use writer::{json_to_msgpack, json_to_msgpack_or_empty, value_to_msgpack};
