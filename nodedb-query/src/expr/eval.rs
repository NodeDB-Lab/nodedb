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

/// Row scope for `SqlExpr::eval_scope`: how `Column(..)` and `OldColumn(..)`
/// resolve to `Value`s. The shared evaluator walks the AST once and calls
/// into this scope for every leaf column reference — both `eval()` and
/// `eval_with_old()` delegate here instead of duplicating the walk.
struct RowScope<'a> {
    new_doc: &'a Value,
    /// Pre-update row, if this is an old/new evaluation (TRANSITION CHECK).
    /// `None` means `OldColumn(..)` resolves to `Null`, matching plain `eval`.
    old_doc: Option<&'a Value>,
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
}

impl SqlExpr {
    /// Evaluate this expression against a document.
    ///
    /// Column references look up fields in the document. Missing fields
    /// return `Null`. Arithmetic on non-numeric values returns `Null`.
    /// `OldColumn(..)` resolves to `Null` (use `eval_with_old` for the
    /// TRANSITION CHECK path).
    pub fn eval(&self, doc: &Value) -> Value {
        self.eval_scope(&RowScope {
            new_doc: doc,
            old_doc: None,
        })
    }

    /// Evaluate with access to both NEW and OLD documents, used by
    /// TRANSITION CHECK predicates. `Column(name)` resolves against
    /// `new_doc`; `OldColumn(name)` resolves against `old_doc`.
    pub fn eval_with_old(&self, new_doc: &Value, old_doc: &Value) -> Value {
        self.eval_scope(&RowScope {
            new_doc,
            old_doc: Some(old_doc),
        })
    }

    /// Shared walker: one match, one recursion scheme, parameterised by the
    /// row-scope so `eval` and `eval_with_old` can't drift out of sync.
    fn eval_scope(&self, scope: &RowScope<'_>) -> Value {
        match self {
            SqlExpr::Column(name) => scope.column(name),
            SqlExpr::OldColumn(name) => scope.old_column(name),

            SqlExpr::Literal(v) => v.clone(),

            SqlExpr::BinaryOp { left, op, right } => {
                let l = left.eval_scope(scope);
                let r = right.eval_scope(scope);
                eval_binary_op(&l, *op, &r)
            }

            SqlExpr::Negate(inner) => {
                let v = inner.eval_scope(scope);
                if let Some(b) = v.as_bool() {
                    Value::Bool(!b)
                } else {
                    match value_to_f64(&v, false) {
                        Some(n) => to_value_number(-n),
                        None => Value::Null,
                    }
                }
            }

            SqlExpr::Function { name, args } => {
                let evaluated: Vec<Value> = args.iter().map(|a| a.eval_scope(scope)).collect();
                crate::functions::eval_function(name, &evaluated)
            }

            SqlExpr::Cast { expr, to_type } => {
                let v = expr.eval_scope(scope);
                crate::cast::eval_cast(&v, to_type)
            }

            SqlExpr::Case {
                operand,
                when_thens,
                else_expr,
            } => {
                let op_val = operand.as_ref().map(|e| e.eval_scope(scope));
                for (when_expr, then_expr) in when_thens {
                    let when_val = when_expr.eval_scope(scope);
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
                    None => Value::Null,
                }
            }

            SqlExpr::Coalesce(exprs) => {
                for expr in exprs {
                    let v = expr.eval_scope(scope);
                    if !v.is_null() {
                        return v;
                    }
                }
                Value::Null
            }

            SqlExpr::NullIf(a, b) => {
                let va = a.eval_scope(scope);
                let vb = b.eval_scope(scope);
                if coerced_eq(&va, &vb) {
                    Value::Null
                } else {
                    va
                }
            }

            SqlExpr::IsNull { expr, negated } => {
                let v = expr.eval_scope(scope);
                let is_null = v.is_null();
                Value::Bool(if *negated { !is_null } else { is_null })
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
        assert_eq!(expr.eval(&doc()), Value::String("Alice".into()));
    }

    #[test]
    fn missing_column() {
        let expr = SqlExpr::Column("missing".into());
        assert_eq!(expr.eval(&doc()), Value::Null);
    }

    #[test]
    fn literal() {
        let expr = SqlExpr::Literal(Value::Integer(42));
        assert_eq!(expr.eval(&doc()), Value::Integer(42));
    }

    #[test]
    fn add() {
        let expr = SqlExpr::BinaryOp {
            left: Box::new(SqlExpr::Column("price".into())),
            op: BinaryOp::Add,
            right: Box::new(SqlExpr::Literal(Value::Float(1.5))),
        };
        assert_eq!(expr.eval(&doc()), Value::Integer(12));
    }

    #[test]
    fn multiply() {
        let expr = SqlExpr::BinaryOp {
            left: Box::new(SqlExpr::Column("price".into())),
            op: BinaryOp::Mul,
            right: Box::new(SqlExpr::Column("qty".into())),
        };
        assert_eq!(expr.eval(&doc()), Value::Integer(42));
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
        assert_eq!(expr.eval(&doc()), Value::String("adult".into()));
    }

    #[test]
    fn coalesce() {
        let expr = SqlExpr::Coalesce(vec![
            SqlExpr::Column("email".into()),
            SqlExpr::Literal(Value::String("default@example.com".into())),
        ]);
        assert_eq!(
            expr.eval(&doc()),
            Value::String("default@example.com".into())
        );
    }

    #[test]
    fn is_null() {
        let expr = SqlExpr::IsNull {
            expr: Box::new(SqlExpr::Column("email".into())),
            negated: false,
        };
        assert_eq!(expr.eval(&doc()), Value::Bool(true));
    }
}
