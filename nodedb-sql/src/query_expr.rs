// SPDX-License-Identifier: Apache-2.0

//! Convert `nodedb_sql::types_expr::SqlExpr` to `nodedb_query::expr::types::SqlExpr`
//! for WHERE predicates, sort keys, projections and assignments.
//!
//! The evaluator has no `IN`, `BETWEEN`, `LIKE` or array-construct variant.
//! Each is lowered the way Origin's planner lowers it, so an expression
//! evaluates the same on both.

use crate::types_expr::{BinaryOp as SBinaryOp, SqlExpr as SExpr, UnaryOp};
use nodedb_query::expr::types::{BinaryOp as QBinaryOp, CastType, SqlExpr as QExpr};

use crate::error::SqlError;
use crate::types_expr::SqlValue;
use nodedb_types::Value;

/// Convert a SQL-side expression to a query-side expression.
///
/// Lowerings, as on Origin:
/// - `e IN (a, b)` is `e = a OR e = b`, and `NOT IN` is `e <> a AND e <> b`.
///   An empty list is `false`, or `true` for `NOT IN`.
/// - `e BETWEEN lo AND hi` is `e >= lo AND e <= hi`, and `NOT BETWEEN` is
///   `e < lo OR e > hi`.
/// - `e LIKE p` calls `like(e, p)`, or `ilike` when case-insensitive.
///   `NOT LIKE` negates the call.
/// - `NOT e` is [`QExpr::logical_not`]: NULL stays NULL.
/// - An array of literals is one array literal. An array with any other
///   element calls `make_array`, which builds the array per row.
///
/// A subquery or wildcard returns `SqlError::Unsupported`.
pub fn convert_sql_expr(expr: &SExpr) -> Result<QExpr, SqlError> {
    convert(expr, &[], false, None)
}

/// [`convert_sql_expr`] for an expression that reads a joined source row.
///
/// A column qualified by any name in `source_refs` converts to
/// `Column("<source_refs[0]>.<name>")`, the key a joined row gives that
/// source field, as Origin's qualified conversion does. Any other qualifier
/// names the target and is dropped.
pub fn convert_sql_expr_with_source(
    expr: &SExpr,
    source_refs: &[String],
) -> Result<QExpr, SqlError> {
    convert(expr, source_refs, false, None)
}

type ColumnResolver<'a, E> = dyn Fn(&str) -> Result<String, E> + 'a;

pub fn convert_sql_expr_qualified<E: From<SqlError>>(
    expr: &SExpr,
    resolve: &ColumnResolver<'_, E>,
) -> Result<QExpr, E> {
    convert(expr, &[], true, Some(resolve))
}

fn convert<E: From<SqlError>>(
    expr: &SExpr,
    source_refs: &[String],
    qualified: bool,
    resolve: Option<&ColumnResolver<'_, E>>,
) -> Result<QExpr, E> {
    match expr {
        // `EXCLUDED.col` (`INSERT ... ON CONFLICT DO UPDATE`) resolves
        // against the incoming row via `eval_with_excluded`, mirroring
        // Origin's `sql_expr_to_bridge_expr`.
        SExpr::Column { table, name }
            if !qualified
                && table
                    .as_deref()
                    .is_some_and(|t| t.eq_ignore_ascii_case("excluded")) =>
        {
            Ok(QExpr::ExcludedColumn(name.clone()))
        }
        SExpr::Column {
            table: Some(table),
            name,
        } if source_refs.iter().any(|r| r.eq_ignore_ascii_case(table)) => {
            let alias = source_refs.first().map_or(table.as_str(), String::as_str);
            Ok(QExpr::Column(format!("{alias}.{name}")))
        }
        SExpr::Column {
            table: Some(table),
            name,
        } if qualified => {
            let key = format!("{table}.{name}");
            Ok(QExpr::Column(match resolve {
                Some(resolve) => resolve(&key)?,
                None => key,
            }))
        }
        SExpr::Column { name, .. } => Ok(QExpr::Column(match resolve {
            Some(resolve) => resolve(name)?,
            None => name.clone(),
        })),

        SExpr::Literal(v) => {
            let val = sql_value_to_value(v)?;
            Ok(QExpr::Literal(val))
        }

        SExpr::BinaryOp { left, op, right } => {
            let ql = convert(left, source_refs, qualified, resolve)?;
            let qr = convert(right, source_refs, qualified, resolve)?;
            let qop = convert_binary_op(*op)?;
            Ok(QExpr::BinaryOp {
                left: Box::new(ql),
                op: qop,
                right: Box::new(qr),
            })
        }

        SExpr::UnaryOp { op, expr } => {
            let inner = convert(expr, source_refs, qualified, resolve)?;
            match op {
                UnaryOp::Neg => Ok(QExpr::Negate(Box::new(inner))),
                UnaryOp::Not => Ok(QExpr::logical_not(inner)),
            }
        }

        SExpr::Function { name, args, .. } => {
            let qargs: Result<Vec<QExpr>, E> = args
                .iter()
                .map(|a| convert(a, source_refs, qualified, resolve))
                .collect();
            Ok(QExpr::Function {
                name: name.clone(),
                args: qargs?,
            })
        }

        SExpr::Case {
            operand,
            when_then,
            else_expr,
        } => {
            let qoperand = operand
                .as_ref()
                .map(|e| convert(e, source_refs, qualified, resolve).map(Box::new))
                .transpose()?;
            let qwhen: Result<Vec<(QExpr, QExpr)>, E> = when_then
                .iter()
                .map(|(cond, val)| {
                    Ok((
                        convert(cond, source_refs, qualified, resolve)?,
                        convert(val, source_refs, qualified, resolve)?,
                    ))
                })
                .collect();
            let qelse = else_expr
                .as_ref()
                .map(|e| convert(e, source_refs, qualified, resolve).map(Box::new))
                .transpose()?;
            Ok(QExpr::Case {
                operand: qoperand,
                when_thens: qwhen?,
                else_expr: qelse,
            })
        }

        SExpr::Cast { expr, to_type } => {
            let inner = convert(expr, source_refs, qualified, resolve)?;
            let ct = convert_cast_type(to_type)?;
            Ok(QExpr::Cast {
                expr: Box::new(inner),
                to_type: ct,
            })
        }

        SExpr::IsNull { expr, negated } => {
            let inner = convert(expr, source_refs, qualified, resolve)?;
            Ok(QExpr::IsNull {
                expr: Box::new(inner),
                negated: *negated,
            })
        }

        SExpr::InList {
            expr,
            list,
            negated,
        } => {
            let target = convert(expr, source_refs, qualified, resolve)?;
            let (eq_op, combine_op) = if *negated {
                (QBinaryOp::NotEq, QBinaryOp::And)
            } else {
                (QBinaryOp::Eq, QBinaryOp::Or)
            };
            let mut lowered: Option<QExpr> = None;
            for item in list {
                let test = QExpr::BinaryOp {
                    left: Box::new(target.clone()),
                    op: eq_op,
                    right: Box::new(convert(item, source_refs, qualified, resolve)?),
                };
                lowered = Some(match lowered {
                    None => test,
                    Some(acc) => QExpr::BinaryOp {
                        left: Box::new(acc),
                        op: combine_op,
                        right: Box::new(test),
                    },
                });
            }
            Ok(lowered.unwrap_or(QExpr::Literal(nodedb_types::Value::Bool(*negated))))
        }

        SExpr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let e = convert(expr, source_refs, qualified, resolve)?;
            let lo = convert(low, source_refs, qualified, resolve)?;
            let hi = convert(high, source_refs, qualified, resolve)?;
            let (low_op, high_op, combine_op) = if *negated {
                (QBinaryOp::Lt, QBinaryOp::Gt, QBinaryOp::Or)
            } else {
                (QBinaryOp::GtEq, QBinaryOp::LtEq, QBinaryOp::And)
            };
            Ok(QExpr::BinaryOp {
                left: Box::new(QExpr::BinaryOp {
                    left: Box::new(e.clone()),
                    op: low_op,
                    right: Box::new(lo),
                }),
                op: combine_op,
                right: Box::new(QExpr::BinaryOp {
                    left: Box::new(e),
                    op: high_op,
                    right: Box::new(hi),
                }),
            })
        }

        SExpr::Like {
            expr,
            pattern,
            negated,
            case_insensitive,
        } => {
            let name = if *case_insensitive { "ilike" } else { "like" };
            let call = QExpr::Function {
                name: name.into(),
                args: vec![
                    convert(expr, source_refs, qualified, resolve)?,
                    convert(pattern, source_refs, qualified, resolve)?,
                ],
            };
            Ok(if *negated {
                QExpr::Negate(Box::new(call))
            } else {
                call
            })
        }

        SExpr::ArrayLiteral(elems) => {
            let lowered = elems
                .iter()
                .map(|e| convert(e, source_refs, qualified, resolve))
                .collect::<Result<Vec<QExpr>, E>>()?;
            let literals: Option<Vec<nodedb_types::Value>> = lowered
                .iter()
                .map(|elem| {
                    if let QExpr::Literal(v) = elem {
                        Some(v.clone())
                    } else {
                        None
                    }
                })
                .collect();
            Ok(match literals {
                Some(values) => QExpr::Literal(nodedb_types::Value::Array(values)),
                None => QExpr::Function {
                    name: "make_array".into(),
                    args: lowered,
                },
            })
        }

        SExpr::Subquery(_) => Err(SqlError::Unsupported {
            detail: "a subquery has no expression form here".to_string(),
        }
        .into()),

        SExpr::Wildcard => Err(SqlError::Unsupported {
            detail: "a wildcard has no expression form here".to_string(),
        }
        .into()),
    }
}

fn convert_binary_op(op: SBinaryOp) -> Result<QBinaryOp, SqlError> {
    Ok(match op {
        SBinaryOp::Add => QBinaryOp::Add,
        SBinaryOp::Sub => QBinaryOp::Sub,
        SBinaryOp::Mul => QBinaryOp::Mul,
        SBinaryOp::Div => QBinaryOp::Div,
        SBinaryOp::Mod => QBinaryOp::Mod,
        SBinaryOp::Eq => QBinaryOp::Eq,
        SBinaryOp::Ne => QBinaryOp::NotEq,
        SBinaryOp::Gt => QBinaryOp::Gt,
        SBinaryOp::Ge => QBinaryOp::GtEq,
        SBinaryOp::Lt => QBinaryOp::Lt,
        SBinaryOp::Le => QBinaryOp::LtEq,
        SBinaryOp::And => QBinaryOp::And,
        SBinaryOp::Or => QBinaryOp::Or,
        SBinaryOp::Concat => QBinaryOp::Concat,
    })
}

fn convert_cast_type(to_type: &str) -> Result<CastType, SqlError> {
    CastType::from_type_name(to_type).ok_or_else(|| SqlError::Unsupported {
        detail: format!("CAST to type '{}' is not supported", to_type.to_uppercase()),
    })
}

pub fn sql_value_to_value(v: &SqlValue) -> Result<Value, SqlError> {
    match v {
        SqlValue::Int(i) => Ok(Value::Integer(*i)),
        SqlValue::Float(f) => Ok(Value::Float(*f)),
        // An exact decimal stays exact, its scale included.
        SqlValue::Decimal(d) => Ok(Value::Decimal(*d)),
        SqlValue::String(s) => Ok(Value::String(s.clone())),
        SqlValue::Bool(b) => Ok(Value::Bool(*b)),
        SqlValue::Null => Ok(Value::Null),
        SqlValue::Bytes(b) => Ok(Value::Bytes(b.clone())),
        SqlValue::Uuid(u) => Ok(Value::Uuid(u.clone())),
        SqlValue::Object(fields) => Ok(Value::Object(
            fields
                .iter()
                .map(|(key, value)| Ok((key.clone(), sql_value_to_value(value)?)))
                .collect::<Result<_, SqlError>>()?,
        )),
        SqlValue::Array(elems) => {
            let vs: Result<Vec<Value>, SqlError> = elems.iter().map(sql_value_to_value).collect();
            Ok(Value::Array(vs?))
        }
        SqlValue::Timestamp(ts) => Ok(Value::NaiveDateTime(*ts)),
        SqlValue::Timestamptz(ts) => Ok(Value::DateTime(*ts)),
        SqlValue::Geometry(g) => Ok(Value::Geometry(g.clone())),
        SqlValue::Duration(d) => Ok(Value::Duration(*d)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn not_of(operand: SqlValue) -> Value {
        let expr = SExpr::UnaryOp {
            op: UnaryOp::Not,
            expr: Box::new(SExpr::Literal(operand)),
        };
        let lowered = match convert_sql_expr(&expr) {
            Ok(lowered) => lowered,
            Err(e) => panic!("NOT lowers: {e}"),
        };
        let doc = Value::Object(Default::default());
        match lowered.eval(&doc, &nodedb_crs::CrsContext::builtin_only()) {
            Ok(v) => v,
            Err(e) => panic!("NOT evaluates: {e:?}"),
        }
    }

    #[test]
    fn not_keeps_null_and_never_negates_a_number() {
        assert_eq!(not_of(SqlValue::Null), Value::Null);
        assert_eq!(not_of(SqlValue::Bool(true)), Value::Bool(false));
        assert_eq!(not_of(SqlValue::Bool(false)), Value::Bool(true));
        assert_eq!(not_of(SqlValue::Int(5)), Value::Bool(false));
    }
}
