// SPDX-License-Identifier: Apache-2.0

//! SqlExpr AST node definitions.

use nodedb_types::Value;

/// The column name that references the whole row.
///
/// SQL `*` as a function argument (`to_jsonb(*)`) lowers to
/// `Column(WHOLE_ROW_COLUMN)`, which evaluates to the row document itself.
pub const WHOLE_ROW_COLUMN: &str = "*";

/// A serializable SQL expression that can be evaluated against a document.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum SqlExpr {
    /// Column reference: extract field value from the document.
    /// [`WHOLE_ROW_COLUMN`] is the whole document.
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
    /// `EXCLUDED.col` reference from `INSERT ... ON CONFLICT DO UPDATE`:
    /// the column value from the row proposed for insertion that
    /// triggered the conflict. Resolves against the incoming row when
    /// evaluated via `eval_with_excluded()`. Returns NULL in plain
    /// `eval()` / `eval_with_old()`.
    ExcludedColumn(String),
}

/// Binary operators.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
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

pub use super::cast_type::CastType;

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

/// A GROUP BY key specification for aggregate physical execution.
///
/// A bare-column key extracts its value from the document field named by
/// `field` and emits it in the aggregate output row under `output_name`. For a
/// plain column these are the same string. `expr` carries a computed-key
/// expression to evaluate per row; it is always `None` for bare-column keys.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct GroupKeySpec {
    /// Output column name for this group key (alias, else the raw column name).
    pub output_name: String,
    /// Bare-column extraction key. `Some` for a plain `Column` group key.
    pub field: Option<String>,
    /// Computed-key expression, evaluated per row. `None` for bare-column keys.
    pub expr: Option<SqlExpr>,
}

impl GroupKeySpec {
    /// Build a bare-column group key: extract from `name` and emit under the
    /// same column name, with no computed expression.
    pub fn column(name: impl Into<String>) -> Self {
        let name = name.into();
        Self {
            output_name: name.clone(),
            field: Some(name),
            expr: None,
        }
    }
}
