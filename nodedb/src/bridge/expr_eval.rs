//! Serializable expression tree for Data Plane evaluation.
//!
//! `SqlExpr` is the bridge between DataFusion's `Expr` (Control Plane) and
//! the Data Plane's JSON document evaluation. It supports computed columns
//! (`SELECT price * qty AS total`), scalar functions (`UPPER(name)`),
//! conditional logic (`CASE WHEN`, `COALESCE`), and type casting.
//!
//! Serialized via serde (MessagePack) for SPSC bridge transport.

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
    Cast { expr: Box<SqlExpr>, to_type: CastType },

    /// CASE WHEN cond1 THEN val1 WHEN cond2 THEN val2 ... ELSE default END.
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
            SqlExpr::Column(name) => doc
                .get(name)
                .cloned()
                .unwrap_or(serde_json::Value::Null),

            SqlExpr::Literal(v) => v.clone(),

            SqlExpr::BinaryOp { left, op, right } => {
                let l = left.eval(doc);
                let r = right.eval(doc);
                eval_binary_op(&l, *op, &r)
            }

            SqlExpr::Negate(inner) => {
                let v = inner.eval(doc);
                match as_f64(&v) {
                    Some(n) => to_json_number(-n),
                    None => match v.as_bool() {
                        Some(b) => serde_json::Value::Bool(!b),
                        None => serde_json::Value::Null,
                    },
                }
            }

            SqlExpr::Function { name, args } => {
                let evaluated: Vec<serde_json::Value> = args.iter().map(|a| a.eval(doc)).collect();
                eval_function(name, &evaluated)
            }

            SqlExpr::Cast { expr, to_type } => {
                let v = expr.eval(doc);
                eval_cast(&v, to_type)
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
                        Some(ov) => ov == &when_val,
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
                if va == vb {
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
        // Arithmetic.
        BinaryOp::Add => match (as_f64(left), as_f64(right)) {
            (Some(a), Some(b)) => to_json_number(a + b),
            _ => serde_json::Value::Null,
        },
        BinaryOp::Sub => match (as_f64(left), as_f64(right)) {
            (Some(a), Some(b)) => to_json_number(a - b),
            _ => serde_json::Value::Null,
        },
        BinaryOp::Mul => match (as_f64(left), as_f64(right)) {
            (Some(a), Some(b)) => to_json_number(a * b),
            _ => serde_json::Value::Null,
        },
        BinaryOp::Div => match (as_f64(left), as_f64(right)) {
            (Some(a), Some(b)) => {
                if b == 0.0 { serde_json::Value::Null } else { to_json_number(a / b) }
            }
            _ => serde_json::Value::Null,
        },
        BinaryOp::Mod => match (as_f64(left), as_f64(right)) {
            (Some(a), Some(b)) => {
                if b == 0.0 { serde_json::Value::Null } else { to_json_number(a % b) }
            }
            _ => serde_json::Value::Null,
        },
        // String concatenation.
        BinaryOp::Concat => {
            let ls = json_to_string(left);
            let rs = json_to_string(right);
            serde_json::Value::String(format!("{ls}{rs}"))
        }
        // Comparison.
        BinaryOp::Eq => serde_json::Value::Bool(left == right),
        BinaryOp::NotEq => serde_json::Value::Bool(left != right),
        BinaryOp::Gt => serde_json::Value::Bool(cmp_json(left, right) == std::cmp::Ordering::Greater),
        BinaryOp::GtEq => {
            let c = cmp_json(left, right);
            serde_json::Value::Bool(c == std::cmp::Ordering::Greater || c == std::cmp::Ordering::Equal)
        }
        BinaryOp::Lt => serde_json::Value::Bool(cmp_json(left, right) == std::cmp::Ordering::Less),
        BinaryOp::LtEq => {
            let c = cmp_json(left, right);
            serde_json::Value::Bool(c == std::cmp::Ordering::Less || c == std::cmp::Ordering::Equal)
        }
        // Logical.
        BinaryOp::And => serde_json::Value::Bool(is_truthy(left) && is_truthy(right)),
        BinaryOp::Or => serde_json::Value::Bool(is_truthy(left) || is_truthy(right)),
    }
}

/// Evaluate a scalar function call.
fn eval_function(name: &str, args: &[serde_json::Value]) -> serde_json::Value {
    match name {
        // String functions.
        "upper" => args.first().and_then(|v| v.as_str()).map_or(
            serde_json::Value::Null,
            |s| serde_json::Value::String(s.to_uppercase()),
        ),
        "lower" => args.first().and_then(|v| v.as_str()).map_or(
            serde_json::Value::Null,
            |s| serde_json::Value::String(s.to_lowercase()),
        ),
        "trim" => args.first().and_then(|v| v.as_str()).map_or(
            serde_json::Value::Null,
            |s| serde_json::Value::String(s.trim().to_string()),
        ),
        "ltrim" => args.first().and_then(|v| v.as_str()).map_or(
            serde_json::Value::Null,
            |s| serde_json::Value::String(s.trim_start().to_string()),
        ),
        "rtrim" => args.first().and_then(|v| v.as_str()).map_or(
            serde_json::Value::Null,
            |s| serde_json::Value::String(s.trim_end().to_string()),
        ),
        "length" | "char_length" | "character_length" => args
            .first()
            .and_then(|v| v.as_str())
            .map_or(serde_json::Value::Null, |s| {
                serde_json::Value::Number(serde_json::Number::from(s.len() as i64))
            }),
        "substr" | "substring" => {
            let s = args.first().and_then(|v| v.as_str()).unwrap_or("");
            let start = args.get(1).and_then(as_f64).unwrap_or(1.0) as usize;
            let len = args.get(2).and_then(as_f64).map(|n| n as usize);
            let start_idx = start.saturating_sub(1); // SQL is 1-based.
            let result: String = match len {
                Some(l) => s.chars().skip(start_idx).take(l).collect(),
                None => s.chars().skip(start_idx).collect(),
            };
            serde_json::Value::String(result)
        }
        "concat" => {
            let parts: Vec<String> = args.iter().map(json_to_string).collect();
            serde_json::Value::String(parts.join(""))
        }
        "replace" => {
            let s = args.first().and_then(|v| v.as_str()).unwrap_or("");
            let from = args.get(1).and_then(|v| v.as_str()).unwrap_or("");
            let to = args.get(2).and_then(|v| v.as_str()).unwrap_or("");
            serde_json::Value::String(s.replace(from, to))
        }
        "reverse" => args.first().and_then(|v| v.as_str()).map_or(
            serde_json::Value::Null,
            |s| serde_json::Value::String(s.chars().rev().collect()),
        ),

        // Math functions.
        "abs" => args.first().and_then(as_f64).map_or(
            serde_json::Value::Null,
            |n| to_json_number(n.abs()),
        ),
        "round" => {
            let n = args.first().and_then(as_f64).unwrap_or(0.0);
            let decimals = args.get(1).and_then(as_f64).unwrap_or(0.0) as i32;
            let factor = 10.0_f64.powi(decimals);
            to_json_number((n * factor).round() / factor)
        }
        "ceil" | "ceiling" => args
            .first()
            .and_then(as_f64)
            .map_or(serde_json::Value::Null, |n| to_json_number(n.ceil())),
        "floor" => args
            .first()
            .and_then(as_f64)
            .map_or(serde_json::Value::Null, |n| to_json_number(n.floor())),
        "power" | "pow" => {
            let base = args.first().and_then(as_f64).unwrap_or(0.0);
            let exp = args.get(1).and_then(as_f64).unwrap_or(1.0);
            to_json_number(base.powf(exp))
        }
        "sqrt" => args
            .first()
            .and_then(as_f64)
            .map_or(serde_json::Value::Null, |n| to_json_number(n.sqrt())),
        "mod" => {
            let a = args.first().and_then(as_f64).unwrap_or(0.0);
            let b = args.get(1).and_then(as_f64).unwrap_or(1.0);
            if b == 0.0 {
                serde_json::Value::Null
            } else {
                to_json_number(a % b)
            }
        }
        "sign" => args.first().and_then(as_f64).map_or(
            serde_json::Value::Null,
            |n| to_json_number(n.signum()),
        ),
        "log" | "ln" => args
            .first()
            .and_then(as_f64)
            .map_or(serde_json::Value::Null, |n| to_json_number(n.ln())),
        "log10" => args
            .first()
            .and_then(as_f64)
            .map_or(serde_json::Value::Null, |n| to_json_number(n.log10())),
        "log2" => args
            .first()
            .and_then(as_f64)
            .map_or(serde_json::Value::Null, |n| to_json_number(n.log2())),
        "exp" => args
            .first()
            .and_then(as_f64)
            .map_or(serde_json::Value::Null, |n| to_json_number(n.exp())),

        // Conditional.
        "coalesce" => {
            for arg in args {
                if !arg.is_null() {
                    return arg.clone();
                }
            }
            serde_json::Value::Null
        }
        "nullif" => {
            if args.len() >= 2 && args[0] == args[1] {
                serde_json::Value::Null
            } else {
                args.first().cloned().unwrap_or(serde_json::Value::Null)
            }
        }
        "greatest" => {
            args.iter()
                .filter(|v| !v.is_null())
                .max_by(|a, b| cmp_json(a, b))
                .cloned()
                .unwrap_or(serde_json::Value::Null)
        }
        "least" => {
            args.iter()
                .filter(|v| !v.is_null())
                .min_by(|a, b| cmp_json(a, b))
                .cloned()
                .unwrap_or(serde_json::Value::Null)
        }

        _ => serde_json::Value::Null,
    }
}

fn eval_cast(val: &serde_json::Value, to_type: &CastType) -> serde_json::Value {
    match to_type {
        CastType::Int => match val {
            serde_json::Value::Number(n) => {
                let i = n.as_i64().unwrap_or(n.as_f64().unwrap_or(0.0) as i64);
                serde_json::Value::Number(i.into())
            }
            serde_json::Value::String(s) => s
                .parse::<i64>()
                .map(|n| serde_json::Value::Number(n.into()))
                .unwrap_or(serde_json::Value::Null),
            serde_json::Value::Bool(b) => serde_json::Value::Number((*b as i64).into()),
            _ => serde_json::Value::Null,
        },
        CastType::Float => match val {
            serde_json::Value::Number(n) => n
                .as_f64()
                .and_then(serde_json::Number::from_f64)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
            serde_json::Value::String(s) => s
                .parse::<f64>()
                .ok()
                .and_then(serde_json::Number::from_f64)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
            _ => serde_json::Value::Null,
        },
        CastType::String => serde_json::Value::String(json_to_string(val)),
        CastType::Bool => serde_json::Value::Bool(is_truthy(val)),
    }
}

// ── Helpers ──

fn as_f64(v: &serde_json::Value) -> Option<f64> {
    match v {
        serde_json::Value::Number(n) => n.as_f64(),
        serde_json::Value::String(s) => s.parse::<f64>().ok(),
        serde_json::Value::Bool(b) => Some(if *b { 1.0 } else { 0.0 }),
        _ => None,
    }
}

fn to_json_number(n: f64) -> serde_json::Value {
    // Prefer integer representation when the value is exact.
    if n.fract() == 0.0 && n.abs() < i64::MAX as f64 {
        serde_json::Value::Number(serde_json::Number::from(n as i64))
    } else {
        serde_json::Number::from_f64(n)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null)
    }
}

fn json_to_string(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Null => String::new(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        other => other.to_string(),
    }
}

fn is_truthy(v: &serde_json::Value) -> bool {
    match v {
        serde_json::Value::Bool(b) => *b,
        serde_json::Value::Null => false,
        serde_json::Value::Number(n) => n.as_f64().unwrap_or(0.0) != 0.0,
        serde_json::Value::String(s) => !s.is_empty(),
        _ => true,
    }
}

fn cmp_json(a: &serde_json::Value, b: &serde_json::Value) -> std::cmp::Ordering {
    match (as_f64(a), as_f64(b)) {
        (Some(na), Some(nb)) => na.partial_cmp(&nb).unwrap_or(std::cmp::Ordering::Equal),
        _ => {
            let sa = json_to_string(a);
            let sb = json_to_string(b);
            sa.cmp(&sb)
        }
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
    fn literal() {
        let expr = SqlExpr::Literal(json!(42));
        assert_eq!(expr.eval(&doc()), json!(42));
    }

    #[test]
    fn arithmetic_mul() {
        // price * qty = 10.5 * 4 = 42.0
        let expr = SqlExpr::BinaryOp {
            left: Box::new(SqlExpr::Column("price".into())),
            op: BinaryOp::Mul,
            right: Box::new(SqlExpr::Column("qty".into())),
        };
        assert_eq!(expr.eval(&doc()), json!(42));
    }

    #[test]
    fn arithmetic_add() {
        let expr = SqlExpr::BinaryOp {
            left: Box::new(SqlExpr::Column("age".into())),
            op: BinaryOp::Add,
            right: Box::new(SqlExpr::Literal(json!(5))),
        };
        assert_eq!(expr.eval(&doc()), json!(35));
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
    fn upper_function() {
        let expr = SqlExpr::Function {
            name: "upper".into(),
            args: vec![SqlExpr::Column("name".into())],
        };
        assert_eq!(expr.eval(&doc()), json!("ALICE"));
    }

    #[test]
    fn lower_function() {
        let expr = SqlExpr::Function {
            name: "lower".into(),
            args: vec![SqlExpr::Column("name".into())],
        };
        assert_eq!(expr.eval(&doc()), json!("alice"));
    }

    #[test]
    fn substr_function() {
        let expr = SqlExpr::Function {
            name: "substr".into(),
            args: vec![
                SqlExpr::Column("name".into()),
                SqlExpr::Literal(json!(1)),
                SqlExpr::Literal(json!(3)),
            ],
        };
        assert_eq!(expr.eval(&doc()), json!("Ali"));
    }

    #[test]
    fn concat_function() {
        let expr = SqlExpr::Function {
            name: "concat".into(),
            args: vec![
                SqlExpr::Column("name".into()),
                SqlExpr::Literal(json!(" is ")),
                SqlExpr::Column("age".into()),
            ],
        };
        assert_eq!(expr.eval(&doc()), json!("Alice is 30"));
    }

    #[test]
    fn length_function() {
        let expr = SqlExpr::Function {
            name: "length".into(),
            args: vec![SqlExpr::Column("name".into())],
        };
        assert_eq!(expr.eval(&doc()), json!(5));
    }

    #[test]
    fn replace_function() {
        let expr = SqlExpr::Function {
            name: "replace".into(),
            args: vec![
                SqlExpr::Column("name".into()),
                SqlExpr::Literal(json!("Alice")),
                SqlExpr::Literal(json!("Bob")),
            ],
        };
        assert_eq!(expr.eval(&doc()), json!("Bob"));
    }

    #[test]
    fn abs_function() {
        let expr = SqlExpr::Function {
            name: "abs".into(),
            args: vec![SqlExpr::Literal(json!(-42))],
        };
        assert_eq!(expr.eval(&doc()), json!(42));
    }

    #[test]
    fn round_function() {
        let expr = SqlExpr::Function {
            name: "round".into(),
            args: vec![
                SqlExpr::Column("price".into()),
                SqlExpr::Literal(json!(0)),
            ],
        };
        assert_eq!(expr.eval(&doc()), json!(11));
    }

    #[test]
    fn ceil_floor() {
        let ceil = SqlExpr::Function {
            name: "ceil".into(),
            args: vec![SqlExpr::Column("price".into())],
        };
        let floor = SqlExpr::Function {
            name: "floor".into(),
            args: vec![SqlExpr::Column("price".into())],
        };
        assert_eq!(ceil.eval(&doc()), json!(11));
        assert_eq!(floor.eval(&doc()), json!(10));
    }

    #[test]
    fn power_function() {
        let expr = SqlExpr::Function {
            name: "power".into(),
            args: vec![SqlExpr::Literal(json!(2)), SqlExpr::Literal(json!(10))],
        };
        assert_eq!(expr.eval(&doc()), json!(1024));
    }

    #[test]
    fn cast_string_to_int() {
        let expr = SqlExpr::Cast {
            expr: Box::new(SqlExpr::Literal(json!("42"))),
            to_type: CastType::Int,
        };
        assert_eq!(expr.eval(&doc()), json!(42));
    }

    #[test]
    fn cast_float_to_int() {
        let expr = SqlExpr::Cast {
            expr: Box::new(SqlExpr::Column("price".into())),
            to_type: CastType::Int,
        };
        assert_eq!(expr.eval(&doc()), json!(10));
    }

    #[test]
    fn cast_to_string() {
        let expr = SqlExpr::Cast {
            expr: Box::new(SqlExpr::Column("age".into())),
            to_type: CastType::String,
        };
        assert_eq!(expr.eval(&doc()), json!("30"));
    }

    #[test]
    fn case_when() {
        let expr = SqlExpr::Case {
            operand: None,
            when_thens: vec![
                (
                    SqlExpr::BinaryOp {
                        left: Box::new(SqlExpr::Column("age".into())),
                        op: BinaryOp::Gt,
                        right: Box::new(SqlExpr::Literal(json!(65))),
                    },
                    SqlExpr::Literal(json!("senior")),
                ),
                (
                    SqlExpr::BinaryOp {
                        left: Box::new(SqlExpr::Column("age".into())),
                        op: BinaryOp::Gt,
                        right: Box::new(SqlExpr::Literal(json!(18))),
                    },
                    SqlExpr::Literal(json!("adult")),
                ),
            ],
            else_expr: Some(Box::new(SqlExpr::Literal(json!("minor")))),
        };
        assert_eq!(expr.eval(&doc()), json!("adult"));
    }

    #[test]
    fn coalesce_skips_null() {
        let expr = SqlExpr::Coalesce(vec![
            SqlExpr::Column("email".into()),    // null
            SqlExpr::Column("name".into()),      // "Alice"
            SqlExpr::Literal(json!("default")),
        ]);
        assert_eq!(expr.eval(&doc()), json!("Alice"));
    }

    #[test]
    fn nullif_equal() {
        let expr = SqlExpr::NullIf(
            Box::new(SqlExpr::Column("age".into())),
            Box::new(SqlExpr::Literal(json!(30))),
        );
        assert_eq!(expr.eval(&doc()), json!(null));
    }

    #[test]
    fn nullif_not_equal() {
        let expr = SqlExpr::NullIf(
            Box::new(SqlExpr::Column("age".into())),
            Box::new(SqlExpr::Literal(json!(99))),
        );
        assert_eq!(expr.eval(&doc()), json!(30));
    }

    #[test]
    fn is_null_check() {
        let is_null = SqlExpr::IsNull {
            expr: Box::new(SqlExpr::Column("email".into())),
            negated: false,
        };
        let is_not_null = SqlExpr::IsNull {
            expr: Box::new(SqlExpr::Column("name".into())),
            negated: true,
        };
        assert_eq!(is_null.eval(&doc()), json!(true));
        assert_eq!(is_not_null.eval(&doc()), json!(true));
    }

    #[test]
    fn greatest_least() {
        let g = SqlExpr::Function {
            name: "greatest".into(),
            args: vec![
                SqlExpr::Literal(json!(3)),
                SqlExpr::Literal(json!(7)),
                SqlExpr::Literal(json!(1)),
            ],
        };
        let l = SqlExpr::Function {
            name: "least".into(),
            args: vec![
                SqlExpr::Literal(json!(3)),
                SqlExpr::Literal(json!(7)),
                SqlExpr::Literal(json!(1)),
            ],
        };
        assert_eq!(g.eval(&doc()), json!(7));
        assert_eq!(l.eval(&doc()), json!(1));
    }

    #[test]
    fn nested_expression() {
        // ROUND(price * qty + 0.5, 0)
        let expr = SqlExpr::Function {
            name: "round".into(),
            args: vec![
                SqlExpr::BinaryOp {
                    left: Box::new(SqlExpr::BinaryOp {
                        left: Box::new(SqlExpr::Column("price".into())),
                        op: BinaryOp::Mul,
                        right: Box::new(SqlExpr::Column("qty".into())),
                    }),
                    op: BinaryOp::Add,
                    right: Box::new(SqlExpr::Literal(json!(0.5))),
                },
                SqlExpr::Literal(json!(0)),
            ],
        };
        assert_eq!(expr.eval(&doc()), json!(43)); // 10.5*4 + 0.5 = 42.5 → round(42.5, 0) = 43 (round-half-up gives 43 on f64)
    }
}
