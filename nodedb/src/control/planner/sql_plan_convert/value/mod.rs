// SPDX-License-Identifier: BUSL-1.1

//! Value conversion utilities: SqlValue ↔ nodedb_types::Value and msgpack
//! encoding.

pub(super) mod assignments;
pub(super) mod convert;
pub(super) mod msgpack_write;
pub(super) mod rows;

pub(super) use assignments::{
    assignments_to_update_values, assignments_to_update_values_qualified,
};
pub(super) use convert::{
    sql_value_to_bytes, sql_value_to_msgpack, sql_value_to_nodedb_value, sql_value_to_string,
};
pub(super) use msgpack_write::{
    InstantForm, row_to_msgpack, row_to_msgpack_with, write_msgpack_array_header,
    write_msgpack_map_header, write_msgpack_str, write_msgpack_value,
};
pub(super) use rows::rows_to_msgpack_array;
