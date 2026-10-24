// SPDX-License-Identifier: Apache-2.0

//! SQL value / expression / operator types.
//!
//! Re-exported from `types` so downstream `use crate::types::*` continues
//! to resolve these symbols without change.

use nodedb_types::columnar::DecimalTypmod;
use nodedb_types::datetime::NdbDateTime;

use crate::types::SqlPlan;

/// SQL value literal.
#[derive(Debug, Clone, PartialEq)]
pub enum SqlValue {
    Int(i64),
    Float(f64),
    /// Arbitrary-precision decimal (exact arithmetic, no float rounding).
    Decimal(rust_decimal::Decimal),
    String(String),
    Bool(bool),
    Null,
    Bytes(Vec<u8>),
    Array(Vec<SqlValue>),
    /// Typed naive timestamp literal: `TIMESTAMP '...'` or `'...'::TIMESTAMP`.
    Timestamp(NdbDateTime),
    /// Typed timezone-aware timestamp literal: `TIMESTAMPTZ '...'` or `'...'::TIMESTAMPTZ`.
    Timestamptz(NdbDateTime),
    /// A geometry: what a geometry-returning call folds to. Carries its
    /// SRID, Z and M.
    Geometry(nodedb_types::geometry::SridGeometry),
}

/// SQL-side payload-bitmap predicate atom. Mirrors `nodedb_types::PayloadAtom`
/// but holds `SqlValue` (not `nodedb_types::Value`) so the planner can build
/// it without a sql→types translation step. The convert layer lowers
/// `SqlPayloadAtom` to `nodedb_types::PayloadAtom` before crossing the bridge.
#[derive(Debug, Clone, PartialEq)]
pub enum SqlPayloadAtom {
    Eq(String, SqlValue),
    In(String, Vec<SqlValue>),
    Range {
        field: String,
        low: Option<SqlValue>,
        low_inclusive: bool,
        high: Option<SqlValue>,
        high_inclusive: bool,
    },
}

/// SQL expression tree.
#[derive(Debug, Clone)]
pub enum SqlExpr {
    /// Column reference, optionally qualified: `name` or `users.name`
    Column { table: Option<String>, name: String },
    /// Literal value.
    Literal(SqlValue),
    /// Binary operation: `a + b`, `x > 5`
    BinaryOp {
        left: Box<SqlExpr>,
        op: BinaryOp,
        right: Box<SqlExpr>,
    },
    /// Unary operation: `-x`, `NOT flag`
    UnaryOp { op: UnaryOp, expr: Box<SqlExpr> },
    /// Function call: `COUNT(*)`, `vector_distance(field, ARRAY[...])`
    Function {
        name: String,
        args: Vec<SqlExpr>,
        distinct: bool,
    },
    /// CASE WHEN ... THEN ... ELSE ... END
    Case {
        operand: Option<Box<SqlExpr>>,
        when_then: Vec<(SqlExpr, SqlExpr)>,
        else_expr: Option<Box<SqlExpr>>,
    },
    /// CAST(expr AS type)
    Cast { expr: Box<SqlExpr>, to_type: String },
    /// Subquery expression (IN, EXISTS, scalar)
    Subquery(Box<SqlPlan>),
    /// Wildcard `*`
    Wildcard,
    /// `IS NULL` / `IS NOT NULL`
    IsNull { expr: Box<SqlExpr>, negated: bool },
    /// `expr IN (values)`
    InList {
        expr: Box<SqlExpr>,
        list: Vec<SqlExpr>,
        negated: bool,
    },
    /// `expr BETWEEN low AND high`
    Between {
        expr: Box<SqlExpr>,
        low: Box<SqlExpr>,
        high: Box<SqlExpr>,
        negated: bool,
    },
    /// `expr LIKE pattern` / `expr ILIKE pattern`
    Like {
        expr: Box<SqlExpr>,
        pattern: Box<SqlExpr>,
        negated: bool,
        /// `true` for ILIKE (case-insensitive match), `false` for LIKE.
        case_insensitive: bool,
    },
    /// Array literal: `ARRAY[1.0, 2.0, 3.0]`
    ArrayLiteral(Vec<SqlExpr>),
}

/// Binary operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOp {
    // Arithmetic
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    // Comparison
    Eq,
    Ne,
    Gt,
    Ge,
    Lt,
    Le,
    // Logical
    And,
    Or,
    // String
    Concat,
}

/// Unary operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    Neg,
    Not,
}

/// SQL data type for schema resolution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlDataType {
    Int64,
    Float64,
    String,
    Bool,
    Bytes,
    /// Naive (no-timezone) timestamp.
    Timestamp,
    /// Timezone-aware timestamp.
    Timestamptz,
    /// Exact decimal. `Some` carries the declared `DECIMAL(p,s)` typmod that
    /// every written value is fitted to. `None` is a plain `DECIMAL`.
    Decimal(Option<DecimalTypmod>),
    Uuid,
    Vector(usize),
    Geometry,
    /// A structured value: a JSON document, or a typed `ARRAY`, `SET`,
    /// `RANGE` or `RECORD` cell. It reads back as its JSON text.
    Json,
    Unknown,
}
