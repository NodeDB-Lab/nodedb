// SPDX-License-Identifier: Apache-2.0

pub mod error;
pub mod geometry_ext;
pub mod instant_ext;
pub mod json_value;
pub mod key_check;
pub mod native_cell;
pub mod reader;
pub mod scalar_ext;
pub mod transcoder;
pub mod writer;

pub use error::{MsgpackError, MsgpackResult};
pub use geometry_ext::{EXT_GEOMETRY, geometry_from_ext, write_geometry_ext};
pub use instant_ext::{
    EXT_INSTANT_NAIVE, EXT_INSTANT_UTC, INSTANT_EXT_LEN, InstantKind, instant_from_ext,
    read_instant, write_instant,
};
pub use json_value::JsonValue;
pub use key_check::{NonStringKey, find_body_non_string_key, find_non_string_key};
pub use native_cell::{
    NATIVE_CELL_CONSTANT, NATIVE_CELL_GROWTH_DEN, NATIVE_CELL_GROWTH_NUM, NativeCell,
    native_cell_bound,
};
pub use reader::{json_from_msgpack, value_from_msgpack};
pub use scalar_ext::{EXT_DECIMAL, EXT_UUID, scalar_ext, scalar_from_ext};
pub use transcoder::msgpack_to_json_string;
pub use writer::{
    json_to_msgpack, json_to_msgpack_or_empty, ordered_map_to_msgpack, plain_msgpack_keeps_type,
    value_to_document_body, value_to_msgpack, value_to_typed_msgpack,
};
