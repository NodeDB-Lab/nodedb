//! Convert sqlparser AST expressions to our SqlExpr IR.

use sqlparser::ast::{self, Expr, UnaryOperator, Value};

use crate::error::{Result, SqlError};
use crate::parser::normalize::normalize_ident;
use crate::types::*;

/// Convert a sqlparser `Expr` to our `SqlExpr`.
pub fn convert_expr(expr: &Expr) -> Result<SqlExpr> {
    match expr {
        Expr::Identifier(ident) => Ok(SqlExpr::Column {
            table: None,
            name: normalize_ident(ident),
        }),
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => Ok(SqlExpr::Column {
            table: Some(normalize_ident(&parts[0])),
            name: normalize_ident(&parts[1]),
        }),
        Expr::Value(val) => Ok(SqlExpr::Literal(convert_value(&val.value)?)),
        Expr::BinaryOp { left, op, right } => Ok(SqlExpr::BinaryOp {
            left: Box::new(convert_expr(left)?),
            op: convert_binary_op(op)?,
            right: Box::new(convert_expr(right)?),
        }),
        Expr::UnaryOp { op, expr } => Ok(SqlExpr::UnaryOp {
            op: convert_unary_op(op)?,
            expr: Box::new(convert_expr(expr)?),
        }),
        Expr::Function(func) => convert_function(func),
        Expr::Nested(inner) => convert_expr(inner),
        Expr::IsNull(inner) => Ok(SqlExpr::IsNull {
            expr: Box::new(convert_expr(inner)?),
            negated: false,
        }),
        Expr::IsNotNull(inner) => Ok(SqlExpr::IsNull {
            expr: Box::new(convert_expr(inner)?),
            negated: true,
        }),
        Expr::InList {
            expr,
            list,
            negated,
        } => Ok(SqlExpr::InList {
            expr: Box::new(convert_expr(expr)?),
            list: list.iter().map(convert_expr).collect::<Result<_>>()?,
            negated: *negated,
        }),
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => Ok(SqlExpr::Between {
            expr: Box::new(convert_expr(expr)?),
            low: Box::new(convert_expr(low)?),
            high: Box::new(convert_expr(high)?),
            negated: *negated,
        }),
        Expr::Like {
            expr,
            pattern,
            negated,
            ..
        } => Ok(SqlExpr::Like {
            expr: Box::new(convert_expr(expr)?),
            pattern: Box::new(convert_expr(pattern)?),
            negated: *negated,
        }),
        Expr::ILike {
            expr,
            pattern,
            negated,
            ..
        } => Ok(SqlExpr::Like {
            expr: Box::new(convert_expr(expr)?),
            pattern: Box::new(convert_expr(pattern)?),
            negated: *negated,
        }),
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            let when_then = conditions
                .iter()
                .map(|cw| Ok((convert_expr(&cw.condition)?, convert_expr(&cw.result)?)))
                .collect::<Result<Vec<_>>>()?;
            Ok(SqlExpr::Case {
                operand: operand
                    .as_ref()
                    .map(|e| convert_expr(e).map(Box::new))
                    .transpose()?,
                when_then,
                else_expr: else_result
                    .as_ref()
                    .map(|e| convert_expr(e).map(Box::new))
                    .transpose()?,
            })
        }
        Expr::Cast {
            expr, data_type, ..
        } => Ok(SqlExpr::Cast {
            expr: Box::new(convert_expr(expr)?),
            to_type: format!("{data_type}"),
        }),
        Expr::Array(ast::Array { elem, .. }) => {
            let elems = elem.iter().map(convert_expr).collect::<Result<_>>()?;
            Ok(SqlExpr::ArrayLiteral(elems))
        }
        Expr::Wildcard(_) => Ok(SqlExpr::Wildcard),
        // TRIM([BOTH|LEADING|TRAILING] [what FROM] expr)
        Expr::Trim { expr, .. } => Ok(SqlExpr::Function {
            name: "trim".into(),
            args: vec![convert_expr(expr)?],
            distinct: false,
        }),
        // CEIL(expr) / FLOOR(expr)
        Expr::Ceil { expr, .. } => Ok(SqlExpr::Function {
            name: "ceil".into(),
            args: vec![convert_expr(expr)?],
            distinct: false,
        }),
        Expr::Floor { expr, .. } => Ok(SqlExpr::Function {
            name: "floor".into(),
            args: vec![convert_expr(expr)?],
            distinct: false,
        }),
        // SUBSTRING(expr FROM start FOR len)
        Expr::Substring {
            expr,
            substring_from,
            substring_for,
            ..
        } => {
            let mut args = vec![convert_expr(expr)?];
            if let Some(from) = substring_from {
                args.push(convert_expr(from)?);
            }
            if let Some(len) = substring_for {
                args.push(convert_expr(len)?);
            }
            Ok(SqlExpr::Function {
                name: "substring".into(),
                args,
                distinct: false,
            })
        }
        Expr::Interval(interval) => {
            // INTERVAL '1 hour' → microseconds as i64 literal.
            // The interval value is typically a string literal.
            let interval_str = match interval.value.as_ref() {
                Expr::Value(v) => match &v.value {
                    Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => s.clone(),
                    Value::Number(n, _) => {
                        // INTERVAL 5 HOUR → combine number with leading_field.
                        if let Some(ref field) = interval.leading_field {
                            format!("{n} {field}")
                        } else {
                            n.clone()
                        }
                    }
                    _ => {
                        return Err(SqlError::Unsupported {
                            detail: format!("INTERVAL value: {}", interval.value),
                        });
                    }
                },
                _ => {
                    return Err(SqlError::Unsupported {
                        detail: format!("INTERVAL expression: {}", interval.value),
                    });
                }
            };

            // If leading_field is specified, append it: INTERVAL '5' HOUR → "5 HOUR"
            let full_str = if interval_str.chars().all(|c| c.is_ascii_digit())
                && let Some(ref field) = interval.leading_field
            {
                format!("{interval_str} {field}")
            } else {
                interval_str
            };

            let micros = parse_interval_to_micros(&full_str).ok_or_else(|| SqlError::Parse {
                detail: format!("cannot parse INTERVAL '{full_str}'"),
            })?;

            Ok(SqlExpr::Literal(SqlValue::Int(micros)))
        }
        _ => Err(SqlError::Unsupported {
            detail: format!("expression: {expr}"),
        }),
    }
}

/// Parse an interval string to microseconds.
///
/// Delegates to `nodedb_types::kv_parsing::parse_interval_to_ms` (ms → μs)
/// and `NdbDuration::parse` for compound shorthand forms.
fn parse_interval_to_micros(s: &str) -> Option<i64> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }

    // Try NdbDuration::parse first (handles compound "1h30m", "500ms", "2d").
    if let Some(dur) = nodedb_types::NdbDuration::parse(s) {
        return Some(dur.micros);
    }

    // Delegate to shared interval parser (handles all forms including compound).
    if let Ok(ms) = nodedb_types::kv_parsing::parse_interval_to_ms(s) {
        return Some(ms as i64 * 1000); // ms → μs
    }

    None
}

/// Convert a sqlparser `Value` to our `SqlValue`.
pub fn convert_value(val: &Value) -> Result<SqlValue> {
    match val {
        Value::Number(n, _) => {
            if let Ok(i) = n.parse::<i64>() {
                Ok(SqlValue::Int(i))
            } else if let Ok(f) = n.parse::<f64>() {
                Ok(SqlValue::Float(f))
            } else {
                Ok(SqlValue::String(n.clone()))
            }
        }
        Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => {
            Ok(SqlValue::String(s.clone()))
        }
        Value::Boolean(b) => Ok(SqlValue::Bool(*b)),
        Value::Null => Ok(SqlValue::Null),
        _ => Err(SqlError::Unsupported {
            detail: format!("value literal: {val}"),
        }),
    }
}

fn convert_function(func: &ast::Function) -> Result<SqlExpr> {
    let name = func
        .name
        .0
        .iter()
        .map(|p| match p {
            ast::ObjectNamePart::Identifier(ident) => normalize_ident(ident),
            _ => String::new(),
        })
        .collect::<Vec<_>>()
        .join(".");

    let args = match &func.args {
        ast::FunctionArguments::None => Vec::new(),
        ast::FunctionArguments::Subquery(_) => {
            return Err(SqlError::Unsupported {
                detail: "subquery in function args".into(),
            });
        }
        ast::FunctionArguments::List(arg_list) => arg_list
            .args
            .iter()
            .filter_map(|a| match a {
                ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => Some(convert_expr(e)),
                ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Wildcard) => {
                    Some(Ok(SqlExpr::Wildcard))
                }
                ast::FunctionArg::Named {
                    arg: ast::FunctionArgExpr::Expr(e),
                    ..
                } => Some(convert_expr(e)),
                _ => None,
            })
            .collect::<Result<Vec<_>>>()?,
    };

    let distinct = match &func.args {
        ast::FunctionArguments::List(arg_list) => {
            matches!(
                arg_list.duplicate_treatment,
                Some(ast::DuplicateTreatment::Distinct)
            )
        }
        _ => false,
    };

    Ok(SqlExpr::Function {
        name,
        args,
        distinct,
    })
}

fn convert_binary_op(op: &ast::BinaryOperator) -> Result<BinaryOp> {
    match op {
        ast::BinaryOperator::Plus => Ok(BinaryOp::Add),
        ast::BinaryOperator::Minus => Ok(BinaryOp::Sub),
        ast::BinaryOperator::Multiply => Ok(BinaryOp::Mul),
        ast::BinaryOperator::Divide => Ok(BinaryOp::Div),
        ast::BinaryOperator::Modulo => Ok(BinaryOp::Mod),
        ast::BinaryOperator::Eq => Ok(BinaryOp::Eq),
        ast::BinaryOperator::NotEq => Ok(BinaryOp::Ne),
        ast::BinaryOperator::Gt => Ok(BinaryOp::Gt),
        ast::BinaryOperator::GtEq => Ok(BinaryOp::Ge),
        ast::BinaryOperator::Lt => Ok(BinaryOp::Lt),
        ast::BinaryOperator::LtEq => Ok(BinaryOp::Le),
        ast::BinaryOperator::And => Ok(BinaryOp::And),
        ast::BinaryOperator::Or => Ok(BinaryOp::Or),
        ast::BinaryOperator::StringConcat => Ok(BinaryOp::Concat),
        _ => Err(SqlError::Unsupported {
            detail: format!("binary operator: {op}"),
        }),
    }
}

fn convert_unary_op(op: &UnaryOperator) -> Result<UnaryOp> {
    match op {
        UnaryOperator::Minus => Ok(UnaryOp::Neg),
        UnaryOperator::Not => Ok(UnaryOp::Not),
        _ => Err(SqlError::Unsupported {
            detail: format!("unary operator: {op}"),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_interval_sql_word_forms() {
        assert_eq!(parse_interval_to_micros("1 hour"), Some(3_600_000_000));
        assert_eq!(parse_interval_to_micros("5 days"), Some(5 * 86_400_000_000));
        assert_eq!(
            parse_interval_to_micros("30 minutes"),
            Some(30 * 60_000_000)
        );
        assert_eq!(
            parse_interval_to_micros("2 hours 30 minutes"),
            Some(9_000_000_000)
        );
        assert_eq!(parse_interval_to_micros("1 week"), Some(604_800_000_000));
        assert_eq!(parse_interval_to_micros("100 milliseconds"), Some(100_000));
    }

    #[test]
    fn parse_interval_shorthand() {
        assert_eq!(parse_interval_to_micros("1h"), Some(3_600_000_000));
        assert_eq!(parse_interval_to_micros("30m"), Some(30 * 60_000_000));
        assert_eq!(parse_interval_to_micros("1h30m"), Some(5_400_000_000));
        assert_eq!(parse_interval_to_micros("500ms"), Some(500_000));
    }

    #[test]
    fn parse_interval_invalid() {
        assert_eq!(parse_interval_to_micros(""), None);
        assert_eq!(parse_interval_to_micros("abc"), None);
    }
}
