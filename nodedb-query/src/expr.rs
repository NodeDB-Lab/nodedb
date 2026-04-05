//! SqlExpr AST definition and core evaluation.

use crate::json_ops::{
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

// ─── Manual zerompk impls for SqlExpr ────────────────────────────────────────
//
// SqlExpr contains `serde_json::Value` (in the Literal variant) which does not
// implement `zerompk::ToMessagePack` directly. We use `nodedb_types::json_msgpack::JsonValue`
// as a bridge for that variant.
//
// Encoding format: each variant is an array `[tag_u8, field1, field2, ...]`.
// Tags: Column=0, Literal=1, BinaryOp=2, Negate=3, Function=4, Cast=5,
//       Case=6, Coalesce=7, NullIf=8, IsNull=9, OldColumn=10.

impl zerompk::ToMessagePack for SqlExpr {
    fn write<W: zerompk::Write>(&self, writer: &mut W) -> zerompk::Result<()> {
        use nodedb_types::json_msgpack::JsonValue;
        match self {
            SqlExpr::Column(s) => {
                writer.write_array_len(2)?;
                writer.write_u8(0)?;
                writer.write_string(s)
            }
            SqlExpr::Literal(v) => {
                writer.write_array_len(2)?;
                writer.write_u8(1)?;
                JsonValue(v.clone()).write(writer)
            }
            SqlExpr::BinaryOp { left, op, right } => {
                writer.write_array_len(4)?;
                writer.write_u8(2)?;
                left.write(writer)?;
                op.write(writer)?;
                right.write(writer)
            }
            SqlExpr::Negate(inner) => {
                writer.write_array_len(2)?;
                writer.write_u8(3)?;
                inner.write(writer)
            }
            SqlExpr::Function { name, args } => {
                writer.write_array_len(3)?;
                writer.write_u8(4)?;
                writer.write_string(name)?;
                args.write(writer)
            }
            SqlExpr::Cast { expr, to_type } => {
                writer.write_array_len(3)?;
                writer.write_u8(5)?;
                expr.write(writer)?;
                to_type.write(writer)
            }
            SqlExpr::Case {
                operand,
                when_thens,
                else_expr,
            } => {
                writer.write_array_len(4)?;
                writer.write_u8(6)?;
                operand.write(writer)?;
                // Encode when_thens as array of 2-element arrays.
                writer.write_array_len(when_thens.len())?;
                for (cond, val) in when_thens {
                    writer.write_array_len(2)?;
                    cond.write(writer)?;
                    val.write(writer)?;
                }
                else_expr.write(writer)
            }
            SqlExpr::Coalesce(exprs) => {
                writer.write_array_len(2)?;
                writer.write_u8(7)?;
                exprs.write(writer)
            }
            SqlExpr::NullIf(e1, e2) => {
                writer.write_array_len(3)?;
                writer.write_u8(8)?;
                e1.write(writer)?;
                e2.write(writer)
            }
            SqlExpr::IsNull { expr, negated } => {
                writer.write_array_len(3)?;
                writer.write_u8(9)?;
                expr.write(writer)?;
                writer.write_boolean(*negated)
            }
            SqlExpr::OldColumn(s) => {
                writer.write_array_len(2)?;
                writer.write_u8(10)?;
                writer.write_string(s)
            }
        }
    }
}

impl<'a> zerompk::FromMessagePack<'a> for SqlExpr {
    fn read<R: zerompk::Read<'a>>(reader: &mut R) -> zerompk::Result<Self> {
        use nodedb_types::json_msgpack::JsonValue;
        let len = reader.read_array_len()?;
        if len == 0 {
            return Err(zerompk::Error::ArrayLengthMismatch {
                expected: 1,
                actual: 0,
            });
        }
        let tag = reader.read_u8()?;
        match tag {
            0 => {
                // Column(String)
                Ok(SqlExpr::Column(reader.read_string()?.into_owned()))
            }
            1 => {
                // Literal(serde_json::Value)
                let jv = JsonValue::read(reader)?;
                Ok(SqlExpr::Literal(jv.0))
            }
            2 => {
                // BinaryOp { left, op, right }
                let left = SqlExpr::read(reader)?;
                let op = BinaryOp::read(reader)?;
                let right = SqlExpr::read(reader)?;
                Ok(SqlExpr::BinaryOp {
                    left: Box::new(left),
                    op,
                    right: Box::new(right),
                })
            }
            3 => {
                // Negate(Box<SqlExpr>)
                let inner = SqlExpr::read(reader)?;
                Ok(SqlExpr::Negate(Box::new(inner)))
            }
            4 => {
                // Function { name, args }
                let name = reader.read_string()?.into_owned();
                let args = Vec::<SqlExpr>::read(reader)?;
                Ok(SqlExpr::Function { name, args })
            }
            5 => {
                // Cast { expr, to_type }
                let expr = SqlExpr::read(reader)?;
                let to_type = CastType::read(reader)?;
                Ok(SqlExpr::Cast {
                    expr: Box::new(expr),
                    to_type,
                })
            }
            6 => {
                // Case { operand, when_thens, else_expr }
                let operand = Option::<Box<SqlExpr>>::read(reader)?;
                let wt_len = reader.read_array_len()?;
                let mut when_thens = Vec::with_capacity(wt_len);
                for _ in 0..wt_len {
                    let pair_len = reader.read_array_len()?;
                    if pair_len != 2 {
                        return Err(zerompk::Error::ArrayLengthMismatch {
                            expected: 2,
                            actual: pair_len,
                        });
                    }
                    let cond = SqlExpr::read(reader)?;
                    let val = SqlExpr::read(reader)?;
                    when_thens.push((cond, val));
                }
                let else_expr = Option::<Box<SqlExpr>>::read(reader)?;
                Ok(SqlExpr::Case {
                    operand,
                    when_thens,
                    else_expr,
                })
            }
            7 => {
                // Coalesce(Vec<SqlExpr>)
                let exprs = Vec::<SqlExpr>::read(reader)?;
                Ok(SqlExpr::Coalesce(exprs))
            }
            8 => {
                // NullIf(Box<SqlExpr>, Box<SqlExpr>)
                let e1 = SqlExpr::read(reader)?;
                let e2 = SqlExpr::read(reader)?;
                Ok(SqlExpr::NullIf(Box::new(e1), Box::new(e2)))
            }
            9 => {
                // IsNull { expr, negated }
                let expr = SqlExpr::read(reader)?;
                let negated = reader.read_boolean()?;
                Ok(SqlExpr::IsNull {
                    expr: Box::new(expr),
                    negated,
                })
            }
            10 => {
                // OldColumn(String)
                Ok(SqlExpr::OldColumn(reader.read_string()?.into_owned()))
            }
            _ => Err(zerompk::Error::InvalidMarker(tag)),
        }
    }
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
                crate::functions::eval_function(name, &evaluated)
            }

            SqlExpr::Cast { expr, to_type } => {
                let v = expr.eval(doc);
                crate::cast::eval_cast(&v, to_type)
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

            SqlExpr::OldColumn(_) => serde_json::Value::Null,
        }
    }

    /// Evaluate with access to both NEW and OLD documents (for TRANSITION CHECK).
    ///
    /// `Column(name)` resolves against `new_doc`.
    /// `OldColumn(name)` resolves against `old_doc`.
    pub fn eval_with_old(
        &self,
        new_doc: &serde_json::Value,
        old_doc: &serde_json::Value,
    ) -> serde_json::Value {
        match self {
            SqlExpr::Column(name) => new_doc
                .get(name)
                .cloned()
                .unwrap_or(serde_json::Value::Null),
            SqlExpr::OldColumn(name) => old_doc
                .get(name)
                .cloned()
                .unwrap_or(serde_json::Value::Null),
            SqlExpr::Literal(v) => v.clone(),
            SqlExpr::BinaryOp { left, op, right } => {
                let l = left.eval_with_old(new_doc, old_doc);
                let r = right.eval_with_old(new_doc, old_doc);
                eval_binary_op(&l, *op, &r)
            }
            SqlExpr::Negate(inner) => {
                let v = inner.eval_with_old(new_doc, old_doc);
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
                let evaluated: Vec<serde_json::Value> = args
                    .iter()
                    .map(|a| a.eval_with_old(new_doc, old_doc))
                    .collect();
                crate::functions::eval_function(name, &evaluated)
            }
            SqlExpr::Cast { expr, to_type } => {
                let v = expr.eval_with_old(new_doc, old_doc);
                crate::cast::eval_cast(&v, to_type)
            }
            SqlExpr::Case {
                operand,
                when_thens,
                else_expr,
            } => {
                let op_val = operand.as_ref().map(|e| e.eval_with_old(new_doc, old_doc));
                for (when_expr, then_expr) in when_thens {
                    let when_val = when_expr.eval_with_old(new_doc, old_doc);
                    let matches = match &op_val {
                        Some(ov) => coerced_eq(ov, &when_val),
                        None => is_truthy(&when_val),
                    };
                    if matches {
                        return then_expr.eval_with_old(new_doc, old_doc);
                    }
                }
                match else_expr {
                    Some(e) => e.eval_with_old(new_doc, old_doc),
                    None => serde_json::Value::Null,
                }
            }
            SqlExpr::Coalesce(exprs) => {
                for expr in exprs {
                    let v = expr.eval_with_old(new_doc, old_doc);
                    if !v.is_null() {
                        return v;
                    }
                }
                serde_json::Value::Null
            }
            SqlExpr::NullIf(a, b) => {
                let va = a.eval_with_old(new_doc, old_doc);
                let vb = b.eval_with_old(new_doc, old_doc);
                if coerced_eq(&va, &vb) {
                    serde_json::Value::Null
                } else {
                    va
                }
            }
            SqlExpr::IsNull { expr, negated } => {
                let v = expr.eval_with_old(new_doc, old_doc);
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
        BinaryOp::Concat => {
            let ls = json_to_display_string(left);
            let rs = json_to_display_string(right);
            serde_json::Value::String(format!("{ls}{rs}"))
        }
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
}
