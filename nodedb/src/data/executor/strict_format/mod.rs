// SPDX-License-Identifier: BUSL-1.1

//! Strict document encoding/decoding: Value ↔ Binary Tuple.
//!
//! All internal encoding uses `nodedb_types::Value` directly — no JSON intermediary.
//! JSON is only produced at the read boundary (`binary_tuple_to_json`) for pgwire clients.

mod coerce;
mod decode;
mod encode;

pub(crate) use decode::{
    binary_tuple_to_json, binary_tuple_to_msgpack, binary_tuple_to_row_value,
    binary_tuple_to_value, undecodable_strict_row,
};
pub(super) use encode::{
    bytes_to_binary_tuple, bytes_to_binary_tuple_bitemporal, value_to_binary_tuple,
    value_to_binary_tuple_bitemporal,
};
