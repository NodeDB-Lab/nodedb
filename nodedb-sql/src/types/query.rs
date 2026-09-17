// SPDX-License-Identifier: Apache-2.0

//! Query structure types: projections, sort keys, aggregates, windows, engine/join/spatial enums.

use crate::types_expr::SqlExpr;

/// Database engine type for a collection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EngineType {
    DocumentSchemaless,
    DocumentStrict,
    KeyValue,
    Columnar,
    Timeseries,
    Spatial,
    /// ND sparse array engine — `CREATE ARRAY ...`. Routes through
    /// `ArrayRules` for SQL-level validation, but most table-shaped
    /// operations are unsupported on this engine (DML happens via
    /// dedicated `INSERT INTO ARRAY` / `DELETE FROM ARRAY` syntax).
    Array,
}

/// SQL join type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Semi,
    Anti,
    Cross,
}

impl JoinType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Inner => "inner",
            Self::Left => "left",
            Self::Right => "right",
            Self::Full => "full",
            Self::Semi => "semi",
            Self::Anti => "anti",
            Self::Cross => "cross",
        }
    }
}

/// Spatial predicate types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpatialPredicate {
    DWithin,
    Contains,
    Intersects,
    Within,
}

/// Projection item in SELECT.
#[derive(Debug, Clone)]
pub enum Projection {
    /// Simple column reference: `SELECT name`
    Column(String),
    /// All columns: `SELECT *`
    Star,
    /// Qualified star: `SELECT t.*`
    QualifiedStar(String),
    /// Computed expression: `SELECT price * qty AS total`
    Computed { expr: SqlExpr, alias: String },
    /// Expression the Control Plane evaluates per output row after the Data
    /// Plane returns the rows (a sequence accessor, possibly inside a larger
    /// expression). The Data Plane projects the base columns the expression
    /// references; the response stage evaluates `expr`, writes `alias`, and
    /// drops those base columns.
    CpComputed { expr: SqlExpr, alias: String },
}

/// Sort key for ORDER BY.
#[derive(Debug, Clone)]
pub struct SortKey {
    pub expr: SqlExpr,
    pub ascending: bool,
    pub nulls_first: bool,
}

/// Aggregate expression: `COUNT(*)`, `SUM(amount)`, etc.
#[derive(Debug, Clone)]
pub struct AggregateExpr {
    pub function: String,
    pub args: Vec<SqlExpr>,
    pub alias: String,
    pub distinct: bool,
    /// For the synthetic `GROUPING(col)` pseudo-aggregate: the index of `col`
    /// in the canonical group-by key list. The executor uses this index to read
    /// the grouping-set bitmask and return 0 (present) or 1 (NULL-filled).
    /// `None` for all ordinary aggregate functions.
    pub grouping_col_index: Option<usize>,
}

/// Records SELECT-list interleaving of GROUP BY keys and aggregate
/// expressions so the output-schema builder emits columns in the user's
/// SELECT order instead of a hardcoded group-keys-first order.
/// `GroupKey(i)` indexes `Aggregate::group_by[i]`; `Aggregate(j)` indexes
/// `Aggregate::aggregates[j]`. Empty = no projection was in scope when the
/// plan was built (engine-rules base plan); output_schema falls back to
/// group-keys-first, matching pre-fix behavior.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AggOutputSlot {
    GroupKey(usize),
    Aggregate(usize),
}

/// Window function specification.
#[derive(Debug, Clone)]
pub struct WindowSpec {
    pub function: String,
    pub args: Vec<SqlExpr>,
    pub partition_by: Vec<SqlExpr>,
    pub order_by: Vec<SortKey>,
    pub alias: String,
    /// Frame specification (`ROWS`/`RANGE` BETWEEN ... AND ...).
    /// Defaults to `RANGE UNBOUNDED PRECEDING TO CURRENT ROW`
    /// when the SQL omits a frame clause.
    pub frame: nodedb_query::WindowFrame,
}
