// SPDX-License-Identifier: Apache-2.0

//! Literals, slice boxes, zones and georefs carried on array plans.

/// One coord scalar in `INSERT INTO ARRAY ... COORDS (...)` — a literal
/// value typed loosely; the converter coerces against the schema's
/// declared dim type.
#[derive(Debug, Clone, PartialEq)]
pub enum ArrayCoordLiteral {
    Int64(i64),
    Float64(f64),
    String(String),
}

/// One attribute literal in `... VALUES (...)`. `Null` permitted only
/// for nullable attrs (validated in the converter).
#[derive(Debug, Clone, PartialEq)]
pub enum ArrayAttrLiteral {
    Int64(i64),
    Float64(f64),
    String(String),
    Bytes(Vec<u8>),
    Null,
}

/// One row of an `INSERT INTO ARRAY ...`: `COORDS (...) VALUES (...)`.
#[derive(Debug, Clone, PartialEq)]
pub struct ArrayInsertRow {
    pub coords: Vec<ArrayCoordLiteral>,
    pub attrs: Vec<ArrayAttrLiteral>,
}

/// One named `dim: [lo, hi]` entry in a slice predicate.
///
/// The literal type is loose at planning time; the converter coerces
/// against the array's declared dim dtype before encoding to wire.
#[derive(Debug, Clone, PartialEq)]
pub struct NamedDimRange {
    /// Schema dim name. Resolved to a positional index by the converter.
    pub dim: String,
    pub lo: ArrayCoordLiteral,
    pub hi: ArrayCoordLiteral,
}

/// `ARRAY_SLICE(...)` slice predicate: an unordered set of named
/// dim ranges, expanded to per-position `Option<DimRange>` by the
/// converter (unconstrained dims become `None`).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ArraySliceAst {
    pub dim_ranges: Vec<NamedDimRange>,
}

/// `ARRAY_AGG(...)` / `ARRAY_SLICE(...)` polygon zone over two numeric dims.
///
/// `polygons` holds one entry per member polygon: its exterior ring, then
/// its holes. Each ring is closed and lists `[x, y]` positions, `x` on
/// `x_dim` and `y` on `y_dim`.
///
/// `srid: None` means the positions are dim coordinates. `Some` means they
/// are in that SRID, and `x_dim` / `y_dim` are the array's GEOREF axes.
#[derive(Debug, Clone, PartialEq)]
pub struct ArrayZoneAst {
    pub polygons: Vec<Vec<Vec<[f64; 2]>>>,
    pub x_dim: String,
    pub y_dim: String,
    pub srid: Option<u32>,
}

/// `GEOREF (SRID n, X dim, Y dim [, TRANSFORM (gt0, .., gt5)])` clause.
///
/// `transform: None` means the dims hold pixel-centre world coordinates.
/// `Some` means the dims hold `INT64` pixel indices and the six
/// coefficients map them to world coordinates in GDAL order.
#[derive(Debug, Clone, PartialEq)]
pub struct ArrayGeorefAst {
    pub srid: u32,
    pub x_dim: String,
    pub y_dim: String,
    pub transform: Option<[f64; 6]>,
}

/// Georef change carried by `ALTER ARRAY`.
#[derive(Debug, Clone, PartialEq)]
pub enum ArrayGeorefAlter {
    /// `SET GEOREF (...)`: install or replace the georef.
    Set(ArrayGeorefAst),
    /// `DROP GEOREF`: remove the georef.
    Drop,
}
