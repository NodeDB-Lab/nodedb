// SPDX-License-Identifier: Apache-2.0

//! Dim, attr type, order and bound shapes of `CREATE ARRAY`.

/// Dimension type tag (mirrors `nodedb_array::schema::DimType`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArrayDimType {
    Int64,
    Float64,
    TimestampMs,
    String,
}

/// Attribute type tag (mirrors `nodedb_array::schema::AttrType`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArrayAttrType {
    Int64,
    Float64,
    String,
    Bytes,
    UInt8,
    Int16,
    UInt16,
    Int32,
    UInt32,
    Float32,
    /// 1-byte signed integer. The array DDL names integer widths in bits,
    /// so `INT8` here is one byte, not PostgreSQL's 8-byte `int8`.
    Int8,
}

impl ArrayAttrType {
    /// Whether the type holds integers.
    pub fn is_integer(self) -> bool {
        match self {
            ArrayAttrType::Int64
            | ArrayAttrType::Int8
            | ArrayAttrType::UInt8
            | ArrayAttrType::Int16
            | ArrayAttrType::UInt16
            | ArrayAttrType::Int32
            | ArrayAttrType::UInt32 => true,
            ArrayAttrType::Float64
            | ArrayAttrType::Float32
            | ArrayAttrType::String
            | ArrayAttrType::Bytes => false,
        }
    }

    /// Whether the type holds floats.
    pub fn is_float(self) -> bool {
        match self {
            ArrayAttrType::Float64 | ArrayAttrType::Float32 => true,
            ArrayAttrType::Int64
            | ArrayAttrType::Int8
            | ArrayAttrType::UInt8
            | ArrayAttrType::Int16
            | ArrayAttrType::UInt16
            | ArrayAttrType::Int32
            | ArrayAttrType::UInt32
            | ArrayAttrType::String
            | ArrayAttrType::Bytes => false,
        }
    }

    /// The canonical DDL name of the type.
    pub fn sql_name(self) -> &'static str {
        match self {
            ArrayAttrType::Int64 => "INT64",
            ArrayAttrType::Float64 => "FLOAT64",
            ArrayAttrType::String => "STRING",
            ArrayAttrType::Bytes => "BYTES",
            ArrayAttrType::UInt8 => "UINT8",
            ArrayAttrType::Int16 => "INT16",
            ArrayAttrType::UInt16 => "UINT16",
            ArrayAttrType::Int32 => "INT32",
            ArrayAttrType::UInt32 => "UINT32",
            ArrayAttrType::Float32 => "FLOAT32",
            ArrayAttrType::Int8 => "INT8",
        }
    }
}

/// Cell-order strategy AST mirror.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ArrayCellOrderAst {
    RowMajor,
    ColMajor,
    #[default]
    Hilbert,
    ZOrder,
}

/// Tile-order strategy AST mirror.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ArrayTileOrderAst {
    RowMajor,
    ColMajor,
    #[default]
    Hilbert,
    ZOrder,
}

/// Domain bound carrying its dim's type tag.
#[derive(Debug, Clone, PartialEq)]
pub enum ArrayDomainBound {
    Int64(i64),
    Float64(f64),
    TimestampMs(i64),
    String(String),
}

/// One dim spec in `CREATE ARRAY ... DIMS (...)`.
#[derive(Debug, Clone, PartialEq)]
pub struct ArrayDimAst {
    pub name: String,
    pub dtype: ArrayDimType,
    pub lo: ArrayDomainBound,
    pub hi: ArrayDomainBound,
}
