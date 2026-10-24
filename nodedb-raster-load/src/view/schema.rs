// SPDX-License-Identifier: Apache-2.0

//! The dims, attrs and georef of the array a load writes into.

use nodedb_raster::GeoTransform;

/// The array a load plans against.
#[derive(Debug, Clone, PartialEq)]
pub struct ArrayView {
    pub name: String,
    /// Dims in schema order.
    pub dims: Vec<ViewDim>,
    /// Attrs in schema order.
    pub attrs: Vec<ViewAttr>,
    /// The georef, axes named by dim. `None` when the array has none.
    pub georef: Option<ViewGeoref>,
}

/// One dim and its domain.
#[derive(Debug, Clone, PartialEq)]
pub struct ViewDim {
    pub name: String,
    pub dtype: DimType,
    pub lo: DomainBound,
    pub hi: DomainBound,
}

/// The type of a dim.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DimType {
    Int64,
    Float64,
    TimestampMs,
    String,
}

/// A domain bound, tagged with its dim's type.
#[derive(Debug, Clone, PartialEq)]
pub enum DomainBound {
    Int64(i64),
    Float64(f64),
    TimestampMs(i64),
    String(String),
}

/// One attr.
#[derive(Debug, Clone, PartialEq)]
pub struct ViewAttr {
    pub name: String,
    pub dtype: AttrType,
    pub nullable: bool,
    pub meta: AttrMeta,
}

/// The type of an attr.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttrType {
    Int8,
    UInt8,
    Int16,
    UInt16,
    Int32,
    UInt32,
    Int64,
    Float32,
    Float64,
    String,
    Bytes,
}

impl AttrType {
    /// Whether the type holds floats.
    pub fn is_float(self) -> bool {
        match self {
            AttrType::Float32 | AttrType::Float64 => true,
            AttrType::Int8
            | AttrType::UInt8
            | AttrType::Int16
            | AttrType::UInt16
            | AttrType::Int32
            | AttrType::UInt32
            | AttrType::Int64
            | AttrType::String
            | AttrType::Bytes => false,
        }
    }

    /// The DDL name of the type.
    pub fn sql_name(self) -> &'static str {
        match self {
            AttrType::Int8 => "INT8",
            AttrType::UInt8 => "UINT8",
            AttrType::Int16 => "INT16",
            AttrType::UInt16 => "UINT16",
            AttrType::Int32 => "INT32",
            AttrType::UInt32 => "UINT32",
            AttrType::Int64 => "INT64",
            AttrType::Float32 => "FLOAT32",
            AttrType::Float64 => "FLOAT64",
            AttrType::String => "STRING",
            AttrType::Bytes => "BYTES",
        }
    }
}

/// The metadata of an attr.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct AttrMeta {
    pub nodata: Option<f64>,
    pub scale: Option<f64>,
    pub offset: Option<f64>,
    pub units: Option<String>,
}

/// The georef of an array: the SRID, the dims of the x and y axes, and the
/// transform of a pixel-mode array. `None` is a world-mode array.
#[derive(Debug, Clone, PartialEq)]
pub struct ViewGeoref {
    pub srid: u32,
    pub x_dim: String,
    pub y_dim: String,
    pub transform: Option<GeoTransform>,
}
