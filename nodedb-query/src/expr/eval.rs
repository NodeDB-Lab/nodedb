// SPDX-License-Identifier: Apache-2.0

//! Row-scope evaluator for [`SqlExpr`].
//!
//! `eval()` resolves column references against a single document. `eval_with_old()`
//! resolves `Column(..)` against the post-update ("new") document and `OldColumn(..)`
//! against the pre-update ("old") document — this is the path used by TRANSITION
//! CHECK and similar old/new diff predicates.

use nodedb_types::Value;

use crate::value_ops::{coerced_eq, is_truthy, to_value_number, value_to_f64};

use super::binary::eval_binary_op;
use super::types::SqlExpr;

/// Error type for row-scope `SqlExpr` evaluation.
///
/// Mirrors the [`WindowError`](crate::window::WindowError) idiom: a small,
/// `thiserror`-derived enum living next to the evaluator it describes.
/// Bad casts, wrong argument counts and unknown-value coercions fold to
/// `Value::Null`. These faults fail the statement instead:
/// - division or modulo by a zero divisor, SQLSTATE `22012`;
/// - a call to a function no evaluator implements. It never evaluates to a
///   silent `NULL`;
/// - a function argument it cannot compute on: vectors of different
///   dimensions, an argument of the wrong type, a malformed JSONPath.
///   SQLSTATE `22000`. A `NULL` argument stays `NULL` instead;
/// - an exact integer SUM past the `Decimal` range, SQLSTATE `22000`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EvalError {
    #[error("division by zero")]
    DivisionByZero,
    #[error("function {name}() has no evaluator")]
    UnknownFunction {
        /// The function name as called.
        name: String,
    },
    /// Two vector operands have different dimensions. Same message shape
    /// as the vector engine's dimension error.
    #[error("{function}(): vector dimension mismatch: expected {expected}, got {got}")]
    VectorDimensionMismatch {
        function: &'static str,
        /// Dimension of the first operand.
        expected: usize,
        /// Dimension of the second operand.
        got: usize,
    },
    /// An argument holds a value of a type the function cannot compute on.
    #[error("{function}(): argument {position} must be {expected}, got {got}")]
    ArgumentType {
        function: &'static str,
        /// 1-based argument position.
        position: usize,
        expected: &'static str,
        /// `Value::type_name` of the argument received.
        got: &'static str,
    },
    /// A path argument is not a supported JSONPath.
    #[error("{function}(): invalid JSONPath {path:?}: {reason}")]
    InvalidJsonPath {
        function: &'static str,
        path: String,
        reason: String,
    },
    /// An exact integer aggregate total lies outside the `Decimal` range.
    #[error("{function}(): integer total out of range")]
    NumericOverflow { function: &'static str },
}

/// Row scope for `SqlExpr::eval_scope`: how `Column(..)` and `OldColumn(..)`
/// resolve to `Value`s. The shared evaluator walks the AST once and calls
/// into this scope for every leaf column reference — both `eval()` and
/// `eval_with_old()` delegate here instead of duplicating the walk.
struct RowScope<'a> {
    new_doc: &'a Value,
    /// Pre-update row, if this is an old/new evaluation (TRANSITION CHECK).
    /// `None` means `OldColumn(..)` resolves to `Null`, matching plain `eval`.
    old_doc: Option<&'a Value>,
    /// Incoming `EXCLUDED.*` row for
    /// `INSERT ... ON CONFLICT DO UPDATE SET col = EXCLUDED.col`. `None`
    /// means `ExcludedColumn(..)` resolves to `Null`, matching plain `eval`.
    excluded_doc: Option<&'a Value>,
}

impl<'a> RowScope<'a> {
    fn column(&self, name: &str) -> Value {
        self.new_doc.get(name).cloned().unwrap_or(Value::Null)
    }

    fn old_column(&self, name: &str) -> Value {
        match self.old_doc {
            Some(old) => old.get(name).cloned().unwrap_or(Value::Null),
            None => Value::Null,
        }
    }

    fn excluded_column(&self, name: &str) -> Value {
        match self.excluded_doc {
            Some(excluded) => excluded.get(name).cloned().unwrap_or(Value::Null),
            None => Value::Null,
        }
    }
}

impl SqlExpr {
    /// Evaluate this expression against a document.
    ///
    /// Column references look up fields in the document. Missing fields
    /// return `Null`. Arithmetic on non-numeric values returns `Null`.
    /// `OldColumn(..)` resolves to `Null` (use `eval_with_old` for the
    /// TRANSITION CHECK path).
    ///
    /// Returns `Err(EvalError::DivisionByZero)` if evaluation reaches a
    /// `/` or `%` with a zero divisor — every other
    /// historically-`NULL`-folding case (bad casts, wrong arg counts,
    /// unknown-value coercions) is unaffected and still evaluates to
    /// `Ok(Value::Null)`.
    pub fn eval(&self, doc: &Value) -> Result<Value, EvalError> {
        self.eval_scope(&RowScope {
            new_doc: doc,
            old_doc: None,
            excluded_doc: None,
        })
    }

    /// Evaluate with access to both NEW and OLD documents, used by
    /// TRANSITION CHECK predicates. `Column(name)` resolves against
    /// `new_doc`; `OldColumn(name)` resolves against `old_doc`.
    pub fn eval_with_old(&self, new_doc: &Value, old_doc: &Value) -> Result<Value, EvalError> {
        self.eval_scope(&RowScope {
            new_doc,
            old_doc: Some(old_doc),
            excluded_doc: None,
        })
    }

    /// Evaluate with access to the incoming `EXCLUDED.*` row, used by
    /// `INSERT ... ON CONFLICT DO UPDATE`. `Column(name)` resolves
    /// against the existing (current) row `doc`; `ExcludedColumn(name)`
    /// resolves against `excluded`.
    pub fn eval_with_excluded(&self, doc: &Value, excluded: &Value) -> Result<Value, EvalError> {
        self.eval_scope(&RowScope {
            new_doc: doc,
            old_doc: None,
            excluded_doc: Some(excluded),
        })
    }

    /// Shared walker: one match, one recursion scheme, parameterised by the
    /// row-scope so `eval` and `eval_with_old` can't drift out of sync.
    ///
    /// CASE and COALESCE stay short-circuiting: an untaken CASE branch or an
    /// already-satisfied COALESCE argument is never evaluated, so `CASE WHEN
    /// false THEN 1/0 ELSE 0 END` and `COALESCE(1, 1/0)` never reach the
    /// division and cannot raise `DivisionByZero`.
    fn eval_scope(&self, scope: &RowScope<'_>) -> Result<Value, EvalError> {
        match self {
            SqlExpr::Column(name) => Ok(scope.column(name)),
            SqlExpr::OldColumn(name) => Ok(scope.old_column(name)),
            SqlExpr::ExcludedColumn(name) => Ok(scope.excluded_column(name)),

            SqlExpr::Literal(v) => Ok(v.clone()),

            SqlExpr::BinaryOp { left, op, right } => {
                let l = left.eval_scope(scope)?;
                let r = right.eval_scope(scope)?;
                eval_binary_op(&l, *op, &r)
            }

            SqlExpr::Negate(inner) => {
                let v = inner.eval_scope(scope)?;
                if let Some(b) = v.as_bool() {
                    Ok(Value::Bool(!b))
                } else {
                    match value_to_f64(&v, false) {
                        Some(n) => Ok(to_value_number(-n)),
                        None => Ok(Value::Null),
                    }
                }
            }

            SqlExpr::Function { name, args } => {
                let evaluated: Vec<Value> = args
                    .iter()
                    .map(|a| a.eval_scope(scope))
                    .collect::<Result<_, EvalError>>()?;
                crate::functions::eval_function(name, &evaluated)
            }

            SqlExpr::Cast { expr, to_type } => {
                let v = expr.eval_scope(scope)?;
                Ok(crate::cast::eval_cast(&v, to_type))
            }

            SqlExpr::Case {
                operand,
                when_thens,
                else_expr,
            } => {
                let op_val = match operand {
                    Some(e) => Some(e.eval_scope(scope)?),
                    None => None,
                };
                for (when_expr, then_expr) in when_thens {
                    let when_val = when_expr.eval_scope(scope)?;
                    let matches = match &op_val {
                        Some(ov) => coerced_eq(ov, &when_val),
                        None => is_truthy(&when_val),
                    };
                    if matches {
                        return then_expr.eval_scope(scope);
                    }
                }
                match else_expr {
                    Some(e) => e.eval_scope(scope),
                    None => Ok(Value::Null),
                }
            }

            SqlExpr::Coalesce(exprs) => {
                for expr in exprs {
                    let v = expr.eval_scope(scope)?;
                    if !v.is_null() {
                        return Ok(v);
                    }
                }
                Ok(Value::Null)
            }

            SqlExpr::NullIf(a, b) => {
                let va = a.eval_scope(scope)?;
                let vb = b.eval_scope(scope)?;
                if coerced_eq(&va, &vb) {
                    Ok(Value::Null)
                } else {
                    Ok(va)
                }
            }

            SqlExpr::IsNull { expr, negated } => {
                let v = expr.eval_scope(scope)?;
                let is_null = v.is_null();
                Ok(Value::Bool(if *negated { !is_null } else { is_null }))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::types::BinaryOp;
    use super::*;

    fn doc() -> Value {
        Value::Object(
            [
                ("name".to_string(), Value::String("Alice".into())),
                ("age".to_string(), Value::Integer(30)),
                ("price".to_string(), Value::Float(10.5)),
                ("qty".to_string(), Value::Integer(4)),
                ("active".to_string(), Value::Bool(true)),
                ("email".to_string(), Value::Null),
            ]
            .into_iter()
            .collect(),
        )
    }

    #[test]
    fn column_ref() {
        let expr = SqlExpr::Column("name".into());
        assert_eq!(expr.eval(&doc()).unwrap(), Value::String("Alice".into()));
    }

    #[test]
    fn missing_column() {
        let expr = SqlExpr::Column("missing".into());
        assert_eq!(expr.eval(&doc()).unwrap(), Value::Null);
    }

    #[test]
    fn literal() {
        let expr = SqlExpr::Literal(Value::Integer(42));
        assert_eq!(expr.eval(&doc()).unwrap(), Value::Integer(42));
    }

    #[test]
    fn add() {
        let expr = SqlExpr::BinaryOp {
            left: Box::new(SqlExpr::Column("price".into())),
            op: BinaryOp::Add,
            right: Box::new(SqlExpr::Literal(Value::Float(1.5))),
        };
        assert_eq!(expr.eval(&doc()).unwrap(), Value::Integer(12));
    }

    #[test]
    fn multiply() {
        let expr = SqlExpr::BinaryOp {
            left: Box::new(SqlExpr::Column("price".into())),
            op: BinaryOp::Mul,
            right: Box::new(SqlExpr::Column("qty".into())),
        };
        assert_eq!(expr.eval(&doc()).unwrap(), Value::Integer(42));
    }

    #[test]
    fn case_when() {
        let expr = SqlExpr::Case {
            operand: None,
            when_thens: vec![(
                SqlExpr::BinaryOp {
                    left: Box::new(SqlExpr::Column("age".into())),
                    op: BinaryOp::GtEq,
                    right: Box::new(SqlExpr::Literal(Value::Integer(18))),
                },
                SqlExpr::Literal(Value::String("adult".into())),
            )],
            else_expr: Some(Box::new(SqlExpr::Literal(Value::String("minor".into())))),
        };
        assert_eq!(expr.eval(&doc()).unwrap(), Value::String("adult".into()));
    }

    #[test]
    fn coalesce() {
        let expr = SqlExpr::Coalesce(vec![
            SqlExpr::Column("email".into()),
            SqlExpr::Literal(Value::String("default@example.com".into())),
        ]);
        assert_eq!(
            expr.eval(&doc()).unwrap(),
            Value::String("default@example.com".into())
        );
    }

    #[test]
    fn is_null() {
        let expr = SqlExpr::IsNull {
            expr: Box::new(SqlExpr::Column("email".into())),
            negated: false,
        };
        assert_eq!(expr.eval(&doc()).unwrap(), Value::Bool(true));
    }

    // ── Division/modulo by zero ──────────────────────────────────────────────

    #[test]
    fn integer_division_by_zero_errors() {
        let expr = SqlExpr::BinaryOp {
            left: Box::new(SqlExpr::Literal(Value::Integer(1))),
            op: BinaryOp::Div,
            right: Box::new(SqlExpr::Literal(Value::Integer(0))),
        };
        assert_eq!(expr.eval(&doc()), Err(EvalError::DivisionByZero));
    }

    #[test]
    fn float_division_by_zero_errors() {
        let expr = SqlExpr::BinaryOp {
            left: Box::new(SqlExpr::Literal(Value::Float(1.5))),
            op: BinaryOp::Div,
            right: Box::new(SqlExpr::Literal(Value::Float(0.0))),
        };
        assert_eq!(expr.eval(&doc()), Err(EvalError::DivisionByZero));
    }

    #[test]
    fn decimal_division_by_decimal_zero_errors() {
        let expr = SqlExpr::BinaryOp {
            left: Box::new(SqlExpr::Literal(Value::Decimal(
                rust_decimal::Decimal::new(15, 1),
            ))),
            op: BinaryOp::Div,
            right: Box::new(SqlExpr::Literal(Value::Decimal(
                rust_decimal::Decimal::ZERO,
            ))),
        };
        assert_eq!(expr.eval(&doc()), Err(EvalError::DivisionByZero));
    }

    #[test]
    fn modulo_by_zero_errors() {
        let expr = SqlExpr::BinaryOp {
            left: Box::new(SqlExpr::Literal(Value::Integer(5))),
            op: BinaryOp::Mod,
            right: Box::new(SqlExpr::Literal(Value::Integer(0))),
        };
        assert_eq!(expr.eval(&doc()), Err(EvalError::DivisionByZero));
    }

    #[test]
    fn scalar_mod_function_by_zero_errors() {
        let expr = SqlExpr::Function {
            name: "mod".into(),
            args: vec![
                SqlExpr::Literal(Value::Integer(5)),
                SqlExpr::Literal(Value::Integer(0)),
            ],
        };
        assert_eq!(expr.eval(&doc()), Err(EvalError::DivisionByZero));
    }

    /// An untaken CASE branch must never be evaluated — `1/0` in the THEN
    /// arm of a WHEN that doesn't match must not raise `DivisionByZero`.
    #[test]
    fn case_laziness_untaken_branch_not_evaluated() {
        let expr = SqlExpr::Case {
            operand: None,
            when_thens: vec![(
                SqlExpr::Literal(Value::Bool(false)),
                SqlExpr::BinaryOp {
                    left: Box::new(SqlExpr::Literal(Value::Integer(1))),
                    op: BinaryOp::Div,
                    right: Box::new(SqlExpr::Literal(Value::Integer(0))),
                },
            )],
            else_expr: Some(Box::new(SqlExpr::Literal(Value::Integer(0)))),
        };
        assert_eq!(expr.eval(&doc()).unwrap(), Value::Integer(0));
    }

    /// COALESCE stops at the first non-null argument — a later `1/0`
    /// argument must never be evaluated once an earlier one is non-null.
    #[test]
    fn coalesce_laziness_short_circuits_before_division() {
        let expr = SqlExpr::Coalesce(vec![
            SqlExpr::Literal(Value::Integer(1)),
            SqlExpr::BinaryOp {
                left: Box::new(SqlExpr::Literal(Value::Integer(1))),
                op: BinaryOp::Div,
                right: Box::new(SqlExpr::Literal(Value::Integer(0))),
            },
        ]);
        assert_eq!(expr.eval(&doc()).unwrap(), Value::Integer(1));
    }

    /// Control: `NULL + 1` still folds to `Ok(Null)` — non-division NULL
    /// propagation is untouched by this fix.
    #[test]
    fn null_arithmetic_still_folds_to_null() {
        let expr = SqlExpr::BinaryOp {
            left: Box::new(SqlExpr::Literal(Value::Null)),
            op: BinaryOp::Add,
            right: Box::new(SqlExpr::Literal(Value::Integer(1))),
        };
        assert_eq!(expr.eval(&doc()).unwrap(), Value::Null);
    }

    /// Control: a valid (non-zero-divisor) division still succeeds.
    #[test]
    fn valid_division_still_succeeds() {
        let expr = SqlExpr::BinaryOp {
            left: Box::new(SqlExpr::Literal(Value::Integer(10))),
            op: BinaryOp::Div,
            right: Box::new(SqlExpr::Literal(Value::Integer(2))),
        };
        assert_eq!(expr.eval(&doc()).unwrap(), Value::Integer(5));
    }
}
