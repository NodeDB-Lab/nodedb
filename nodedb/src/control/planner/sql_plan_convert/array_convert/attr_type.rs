// SPDX-License-Identifier: BUSL-1.1

//! The one mapping between the SQL attr type tag and the engine's.

use nodedb_array::schema::AttrType;
use nodedb_sql::types_array::ArrayAttrType;

/// The engine attr type of a SQL attr type.
pub(crate) fn engine_attr_type(t: ArrayAttrType) -> AttrType {
    match t {
        ArrayAttrType::Int64 => AttrType::Int64,
        ArrayAttrType::Float64 => AttrType::Float64,
        ArrayAttrType::String => AttrType::String,
        ArrayAttrType::Bytes => AttrType::Bytes,
        ArrayAttrType::UInt8 => AttrType::UInt8,
        ArrayAttrType::Int16 => AttrType::Int16,
        ArrayAttrType::UInt16 => AttrType::UInt16,
        ArrayAttrType::Int32 => AttrType::Int32,
        ArrayAttrType::UInt32 => AttrType::UInt32,
        ArrayAttrType::Float32 => AttrType::Float32,
        ArrayAttrType::Int8 => AttrType::Int8,
    }
}

/// The SQL attr type of an engine attr type.
pub(crate) fn sql_attr_type(t: AttrType) -> ArrayAttrType {
    match t {
        AttrType::Int64 => ArrayAttrType::Int64,
        AttrType::Float64 => ArrayAttrType::Float64,
        AttrType::String => ArrayAttrType::String,
        AttrType::Bytes => ArrayAttrType::Bytes,
        AttrType::UInt8 => ArrayAttrType::UInt8,
        AttrType::Int16 => ArrayAttrType::Int16,
        AttrType::UInt16 => ArrayAttrType::UInt16,
        AttrType::Int32 => ArrayAttrType::Int32,
        AttrType::UInt32 => ArrayAttrType::UInt32,
        AttrType::Float32 => ArrayAttrType::Float32,
        AttrType::Int8 => ArrayAttrType::Int8,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_mapping_round_trips() {
        let all = [
            ArrayAttrType::Int64,
            ArrayAttrType::Float64,
            ArrayAttrType::String,
            ArrayAttrType::Bytes,
            ArrayAttrType::UInt8,
            ArrayAttrType::Int16,
            ArrayAttrType::UInt16,
            ArrayAttrType::Int32,
            ArrayAttrType::UInt32,
            ArrayAttrType::Float32,
            ArrayAttrType::Int8,
        ];
        for t in all {
            assert_eq!(sql_attr_type(engine_attr_type(t)), t);
            assert_eq!(engine_attr_type(t).sql_name(), t.sql_name());
        }
    }
}
