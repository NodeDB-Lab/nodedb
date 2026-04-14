//! SqlExpr AST node definitions.

use nodedb_types::Value;

/// A serializable SQL expression that can be evaluated against a document.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum SqlExpr {
    /// Column reference: extract field value from the document.
    Column(String),
    /// Literal value.
    Literal(Value),
    /// Binary operation: left op right.
    BinaryOp {
        left: Box<SqlExpr>,
        op: BinaryOp,
        right: Box<SqlExpr>,
    },
    /// Unary negation: -expr or NOT expr.
    Negate(Box<SqlExpr>),
    /// Scalar function call.
    Function { name: String, args: Vec<SqlExpr> },
    /// CAST(expr AS type).
    Cast {
        expr: Box<SqlExpr>,
        to_type: CastType,
    },
    /// CASE WHEN cond1 THEN val1 ... ELSE default END.
    Case {
        operand: Option<Box<SqlExpr>>,
        when_thens: Vec<(SqlExpr, SqlExpr)>,
        else_expr: Option<Box<SqlExpr>>,
    },
    /// COALESCE(expr1, expr2, ...): first non-null value.
    Coalesce(Vec<SqlExpr>),
    /// NULLIF(expr1, expr2): returns NULL if expr1 = expr2, else expr1.
    NullIf(Box<SqlExpr>, Box<SqlExpr>),
    /// IS NULL / IS NOT NULL.
    IsNull { expr: Box<SqlExpr>, negated: bool },
    /// OLD column reference: extract field value from the pre-update document.
    /// Used in TRANSITION CHECK predicates. Resolves against the OLD row
    /// when evaluated via `eval_with_old()`. Returns NULL in normal `eval()`.
    OldColumn(String),
}

/// Binary operators.
#[derive(
    Debug,
    Clone,
    Copy,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(c_enum)]
pub enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Eq,
    NotEq,
    Gt,
    GtEq,
    Lt,
    LtEq,
    And,
    Or,
    Concat,
}

/// Target types for CAST.
#[derive(
    Debug,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(c_enum)]
pub enum CastType {
    Int,
    Float,
    String,
    Bool,
}

/// A computed projection column: alias + expression.
#[derive(
    Debug,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct ComputedColumn {
    pub alias: String,
    pub expr: SqlExpr,
}
