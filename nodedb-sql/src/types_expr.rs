//! SQL value / expression / operator types.
//!
//! Extracted from `types.rs` to keep both files under the 500-line limit.
//! Re-exported from `types` so downstream `use crate::types::*` continues
//! to resolve these symbols without change.

use crate::types::SqlPlan;

/// SQL value literal.
#[derive(Debug, Clone, PartialEq)]
pub enum SqlValue {
    Int(i64),
    Float(f64),
    String(String),
    Bool(bool),
    Null,
    Bytes(Vec<u8>),
    Array(Vec<SqlValue>),
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
    /// `expr LIKE pattern`
    Like {
        expr: Box<SqlExpr>,
        pattern: Box<SqlExpr>,
        negated: bool,
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
    Timestamp,
    Decimal,
    Uuid,
    Vector(usize),
    Geometry,
}
