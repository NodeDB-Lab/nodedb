// SPDX-License-Identifier: Apache-2.0

//! What a load produces: one row per kept pixel, and the attr metadata
//! `ADOPT_METADATA` takes from the files.

use super::schema::AttrMeta;

/// One coordinate of a row. A raster yields integer and float coordinates
/// only.
#[derive(Debug, Clone, PartialEq)]
pub enum CoordLiteral {
    Int64(i64),
    Float64(f64),
}

/// One attr value of a row.
#[derive(Debug, Clone, PartialEq)]
pub enum AttrLiteral {
    Int64(i64),
    Float64(f64),
    Null,
}

/// One row: a coordinate per dim and a value per attr, in schema order.
#[derive(Debug, Clone, PartialEq)]
pub struct LoadRow {
    pub coords: Vec<CoordLiteral>,
    pub attrs: Vec<AttrLiteral>,
}

/// The metadata adoption sets on one attr.
#[derive(Debug, Clone, PartialEq)]
pub struct AdoptedMeta {
    pub attr: String,
    pub meta: AttrMeta,
}
