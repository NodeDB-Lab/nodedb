//! Engine-agnostic AST shapes for array DDL/DML carried on `SqlPlan`.
//!
//! Kept free of any dependency on `nodedb-array` so the SQL crate stays
//! lean. The Origin-side SqlPlan→PhysicalPlan converter is responsible
//! for translating these into typed `nodedb_array::ArraySchema` and
//! engine-typed coord/cell values before crossing the bridge.

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

/// One attribute spec in `CREATE ARRAY ... ATTRS (...)`.
#[derive(Debug, Clone, PartialEq)]
pub struct ArrayAttrAst {
    pub name: String,
    pub dtype: ArrayAttrType,
    pub nullable: bool,
}

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

/// `NDARRAY_SLICE(...)` slice predicate: an unordered set of named
/// dim ranges, expanded to per-position `Option<DimRange>` by the
/// converter (unconstrained dims become `None`).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ArraySliceAst {
    pub dim_ranges: Vec<NamedDimRange>,
}

/// Reducer carried on `SqlPlan::NdArrayAgg`. Mirrors
/// `crate::bridge::physical_plan::ArrayReducer` without the bridge dep.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArrayReducerAst {
    Sum,
    Count,
    Min,
    Max,
    Mean,
}

impl ArrayReducerAst {
    /// Parse the SQL-surface string ('sum', 'count', ...). Returns `None`
    /// for any other value so the planner can surface an `Unsupported`
    /// error with the offending token.
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "sum" => Some(ArrayReducerAst::Sum),
            "count" => Some(ArrayReducerAst::Count),
            "min" => Some(ArrayReducerAst::Min),
            "max" => Some(ArrayReducerAst::Max),
            "mean" | "avg" => Some(ArrayReducerAst::Mean),
            _ => None,
        }
    }
}

/// Pairwise op carried on `SqlPlan::NdArrayElementwise`. Mirrors
/// `crate::bridge::physical_plan::ArrayBinaryOp`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArrayBinaryOpAst {
    Add,
    Sub,
    Mul,
    Div,
}

impl ArrayBinaryOpAst {
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "add" | "+" => Some(ArrayBinaryOpAst::Add),
            "sub" | "-" => Some(ArrayBinaryOpAst::Sub),
            "mul" | "*" => Some(ArrayBinaryOpAst::Mul),
            "div" | "/" => Some(ArrayBinaryOpAst::Div),
            _ => None,
        }
    }
}
