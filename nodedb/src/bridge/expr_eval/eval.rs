//! SqlExpr AST definition and core evaluation.

use crate::bridge::json_ops::{
    coerced_eq, compare_json, is_truthy, json_to_display_string, json_to_f64, to_json_number,
};

/// A serializable SQL expression that can be evaluated against a JSON document.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum SqlExpr {
    /// Column reference: extract field value from the document.
    Column(String),
    /// Literal value.
    Literal(serde_json::Value),
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
}

/// Binary operators.
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
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
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum CastType {
    Int,
    Float,
    String,
    Bool,
}

/// A computed projection column: alias + expression.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ComputedColumn {
    pub alias: String,
    pub expr: SqlExpr,
}

impl SqlExpr {
    /// Evaluate this expression against a JSON document.
    ///
    /// Returns a JSON value. Column references look up fields in the document.
    /// Missing fields return `null`. Arithmetic on non-numeric values returns `null`.
    pub fn eval(&self, doc: &serde_json::Value) -> serde_json::Value {
        match self {
            SqlExpr::Column(name) => doc.get(name).cloned().unwrap_or(serde_json::Value::Null),

            SqlExpr::Literal(v) => v.clone(),

            SqlExpr::BinaryOp { left, op, right } => {
                let l = left.eval(doc);
                let r = right.eval(doc);
                eval_binary_op(&l, *op, &r)
            }

            SqlExpr::Negate(inner) => {
                let v = inner.eval(doc);
                // Booleans: NOT (logical negation). Numbers: arithmetic negation.
                if let Some(b) = v.as_bool() {
                    serde_json::Value::Bool(!b)
                } else {
                    match json_to_f64(&v, false) {
                        Some(n) => to_json_number(-n),
                        None => serde_json::Value::Null,
                    }
                }
            }

            SqlExpr::Function { name, args } => {
                let evaluated: Vec<serde_json::Value> = args.iter().map(|a| a.eval(doc)).collect();
                super::functions::eval_function(name, &evaluated)
            }

            SqlExpr::Cast { expr, to_type } => {
                let v = expr.eval(doc);
                super::cast::eval_cast(&v, to_type)
            }

            SqlExpr::Case {
                operand,
                when_thens,
                else_expr,
            } => {
                let op_val = operand.as_ref().map(|e| e.eval(doc));
                for (when_expr, then_expr) in when_thens {
                    let when_val = when_expr.eval(doc);
                    let matches = match &op_val {
                        Some(ov) => coerced_eq(ov, &when_val),
                        None => is_truthy(&when_val),
                    };
                    if matches {
                        return then_expr.eval(doc);
                    }
                }
                match else_expr {
                    Some(e) => e.eval(doc),
                    None => serde_json::Value::Null,
                }
            }

            SqlExpr::Coalesce(exprs) => {
                for expr in exprs {
                    let v = expr.eval(doc);
                    if !v.is_null() {
                        return v;
                    }
                }
                serde_json::Value::Null
            }

            SqlExpr::NullIf(a, b) => {
                let va = a.eval(doc);
                let vb = b.eval(doc);
                if coerced_eq(&va, &vb) {
                    serde_json::Value::Null
                } else {
                    va
                }
            }

            SqlExpr::IsNull { expr, negated } => {
                let v = expr.eval(doc);
                let is_null = v.is_null();
                serde_json::Value::Bool(if *negated { !is_null } else { is_null })
            }

        }
    }
}

fn eval_binary_op(
    left: &serde_json::Value,
    op: BinaryOp,
    right: &serde_json::Value,
) -> serde_json::Value {
    match op {
        // Arithmetic (bool coercion: true=1, false=0).
        BinaryOp::Add => match (json_to_f64(left, true), json_to_f64(right, true)) {
            (Some(a), Some(b)) => to_json_number(a + b),
            _ => serde_json::Value::Null,
        },
        BinaryOp::Sub => match (json_to_f64(left, true), json_to_f64(right, true)) {
            (Some(a), Some(b)) => to_json_number(a - b),
            _ => serde_json::Value::Null,
        },
        BinaryOp::Mul => match (json_to_f64(left, true), json_to_f64(right, true)) {
            (Some(a), Some(b)) => to_json_number(a * b),
            _ => serde_json::Value::Null,
        },
        // Division by zero returns NULL (matches PostgreSQL behavior).
        BinaryOp::Div => match (json_to_f64(left, true), json_to_f64(right, true)) {
            (Some(a), Some(b)) => {
                if b == 0.0 {
                    serde_json::Value::Null
                } else {
                    to_json_number(a / b)
                }
            }
            _ => serde_json::Value::Null,
        },
        BinaryOp::Mod => match (json_to_f64(left, true), json_to_f64(right, true)) {
            (Some(a), Some(b)) => {
                if b == 0.0 {
                    serde_json::Value::Null
                } else {
                    to_json_number(a % b)
                }
            }
            _ => serde_json::Value::Null,
        },
        // String concatenation.
        BinaryOp::Concat => {
            let ls = json_to_display_string(left);
            let rs = json_to_display_string(right);
            serde_json::Value::String(format!("{ls}{rs}"))
        }
        // Comparison with type coercion (e.g., 5 == "5" → true).
        BinaryOp::Eq => serde_json::Value::Bool(coerced_eq(left, right)),
        BinaryOp::NotEq => serde_json::Value::Bool(!coerced_eq(left, right)),
        BinaryOp::Gt => {
            serde_json::Value::Bool(compare_json(left, right) == std::cmp::Ordering::Greater)
        }
        BinaryOp::GtEq => {
            let c = compare_json(left, right);
            serde_json::Value::Bool(
                c == std::cmp::Ordering::Greater || c == std::cmp::Ordering::Equal,
            )
        }
        BinaryOp::Lt => {
            serde_json::Value::Bool(compare_json(left, right) == std::cmp::Ordering::Less)
        }
        BinaryOp::LtEq => {
            let c = compare_json(left, right);
            serde_json::Value::Bool(c == std::cmp::Ordering::Less || c == std::cmp::Ordering::Equal)
        }
        // Logical.
        BinaryOp::And => serde_json::Value::Bool(is_truthy(left) && is_truthy(right)),
        BinaryOp::Or => serde_json::Value::Bool(is_truthy(left) || is_truthy(right)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn doc() -> serde_json::Value {
        json!({
            "name": "Alice",
            "age": 30,
            "price": 10.5,
            "qty": 4,
            "active": true,
            "email": null
        })
    }

    #[test]
    fn column_ref() {
        let expr = SqlExpr::Column("name".into());
        assert_eq!(expr.eval(&doc()), json!("Alice"));
    }

    #[test]
    fn missing_column() {
        let expr = SqlExpr::Column("missing".into());
        assert_eq!(expr.eval(&doc()), json!(null));
    }

    #[test]
    fn literal() {
        let expr = SqlExpr::Literal(json!(42));
        assert_eq!(expr.eval(&doc()), json!(42));
    }

    #[test]
    fn add() {
        let expr = SqlExpr::BinaryOp {
            left: Box::new(SqlExpr::Column("price".into())),
            op: BinaryOp::Add,
            right: Box::new(SqlExpr::Literal(json!(1.5))),
        };
        assert_eq!(expr.eval(&doc()), json!(12));
    }

    #[test]
    fn multiply() {
        let expr = SqlExpr::BinaryOp {
            left: Box::new(SqlExpr::Column("price".into())),
            op: BinaryOp::Mul,
            right: Box::new(SqlExpr::Column("qty".into())),
        };
        assert_eq!(expr.eval(&doc()), json!(42));
    }

    #[test]
    fn div_by_zero() {
        let expr = SqlExpr::BinaryOp {
            left: Box::new(SqlExpr::Literal(json!(10))),
            op: BinaryOp::Div,
            right: Box::new(SqlExpr::Literal(json!(0))),
        };
        assert_eq!(expr.eval(&doc()), json!(null));
    }

    #[test]
    fn eq_with_coercion() {
        let expr = SqlExpr::BinaryOp {
            left: Box::new(SqlExpr::Literal(json!(5))),
            op: BinaryOp::Eq,
            right: Box::new(SqlExpr::Literal(json!("5"))),
        };
        assert_eq!(expr.eval(&doc()), json!(true));
    }

    #[test]
    fn gt_comparison() {
        let expr = SqlExpr::BinaryOp {
            left: Box::new(SqlExpr::Column("age".into())),
            op: BinaryOp::Gt,
            right: Box::new(SqlExpr::Literal(json!(25))),
        };
        assert_eq!(expr.eval(&doc()), json!(true));
    }

    #[test]
    fn negate() {
        let expr = SqlExpr::Negate(Box::new(SqlExpr::Literal(json!(5))));
        assert_eq!(expr.eval(&doc()), json!(-5));
    }

    #[test]
    fn negate_bool() {
        let expr = SqlExpr::Negate(Box::new(SqlExpr::Literal(json!(true))));
        assert_eq!(expr.eval(&doc()), json!(false));
    }

    #[test]
    fn coalesce() {
        let expr = SqlExpr::Coalesce(vec![
            SqlExpr::Column("email".into()),
            SqlExpr::Literal(json!("default@example.com")),
        ]);
        assert_eq!(expr.eval(&doc()), json!("default@example.com"));
    }

    #[test]
    fn is_null() {
        let expr = SqlExpr::IsNull {
            expr: Box::new(SqlExpr::Column("email".into())),
            negated: false,
        };
        assert_eq!(expr.eval(&doc()), json!(true));
    }

    #[test]
    fn case_when() {
        let expr = SqlExpr::Case {
            operand: None,
            when_thens: vec![(
                SqlExpr::BinaryOp {
                    left: Box::new(SqlExpr::Column("age".into())),
                    op: BinaryOp::GtEq,
                    right: Box::new(SqlExpr::Literal(json!(18))),
                },
                SqlExpr::Literal(json!("adult")),
            )],
            else_expr: Some(Box::new(SqlExpr::Literal(json!("minor")))),
        };
        assert_eq!(expr.eval(&doc()), json!("adult"));
    }

    #[test]
    fn nullif() {
        let expr = SqlExpr::NullIf(
            Box::new(SqlExpr::Literal(json!(5))),
            Box::new(SqlExpr::Literal(json!(5))),
        );
        assert_eq!(expr.eval(&doc()), json!(null));
    }

    #[test]
    fn concat_op() {
        let expr = SqlExpr::BinaryOp {
            left: Box::new(SqlExpr::Literal(json!("hello "))),
            op: BinaryOp::Concat,
            right: Box::new(SqlExpr::Literal(json!("world"))),
        };
        assert_eq!(expr.eval(&doc()), json!("hello world"));
    }

    #[test]
    fn bool_arithmetic() {
        let expr = SqlExpr::BinaryOp {
            left: Box::new(SqlExpr::Literal(json!(true))),
            op: BinaryOp::Add,
            right: Box::new(SqlExpr::Literal(json!(1))),
        };
        assert_eq!(expr.eval(&doc()), json!(2));
    }
}
