//! DML planning helpers extracted from `dml.rs` to keep both files under
//! the 500-line limit. Visibility is `pub(super)` so only `planner::dml`
//! can reach these.

use sqlparser::ast;

use crate::error::{Result, SqlError};
use crate::parser::normalize::{normalize_ident, normalize_object_name};
use crate::resolver::expr::convert_value;
use crate::types::*;

pub(super) fn convert_value_rows(
    columns: &[String],
    rows: &[Vec<ast::Expr>],
) -> Result<Vec<Vec<(String, SqlValue)>>> {
    rows.iter()
        .map(|row| {
            row.iter()
                .enumerate()
                .map(|(i, expr)| {
                    let col = columns.get(i).cloned().unwrap_or_else(|| format!("col{i}"));
                    let val = expr_to_sql_value(expr)?;
                    Ok((col, val))
                })
                .collect::<Result<Vec<_>>>()
        })
        .collect()
}

pub(super) fn expr_to_sql_value(expr: &ast::Expr) -> Result<SqlValue> {
    match expr {
        ast::Expr::Value(v) => convert_value(&v.value),
        ast::Expr::UnaryOp {
            op: ast::UnaryOperator::Minus,
            expr: inner,
        } => {
            let val = expr_to_sql_value(inner)?;
            match val {
                SqlValue::Int(n) => Ok(SqlValue::Int(-n)),
                SqlValue::Float(f) => Ok(SqlValue::Float(-f)),
                _ => Err(SqlError::TypeMismatch {
                    detail: "cannot negate non-numeric value".into(),
                }),
            }
        }
        ast::Expr::Array(ast::Array { elem, .. }) => {
            let vals = elem.iter().map(expr_to_sql_value).collect::<Result<_>>()?;
            Ok(SqlValue::Array(vals))
        }
        ast::Expr::Function(func) => {
            let func_name = func
                .name
                .0
                .iter()
                .map(|p| match p {
                    ast::ObjectNamePart::Identifier(ident) => normalize_ident(ident),
                    _ => String::new(),
                })
                .collect::<Vec<_>>()
                .join(".")
                .to_lowercase();
            match func_name.as_str() {
                "st_point" => {
                    let args = super::select::extract_func_args(func)?;
                    if args.len() >= 2 {
                        let lon = super::select::extract_float(&args[0])?;
                        let lat = super::select::extract_float(&args[1])?;
                        Ok(SqlValue::String(format!(
                            r#"{{"type":"Point","coordinates":[{lon},{lat}]}}"#
                        )))
                    } else {
                        Ok(SqlValue::String(format!("{expr}")))
                    }
                }
                "st_geomfromgeojson" => {
                    let args = super::select::extract_func_args(func)?;
                    if !args.is_empty() {
                        let s = super::select::extract_string_literal(&args[0])?;
                        Ok(SqlValue::String(s))
                    } else {
                        Ok(SqlValue::String(format!("{expr}")))
                    }
                }
                _ => {
                    if let Ok(sql_expr) = crate::resolver::expr::convert_expr(expr)
                        && let Some(v) = super::const_fold::fold_constant_default(&sql_expr)
                    {
                        Ok(v)
                    } else {
                        Ok(SqlValue::String(format!("{expr}")))
                    }
                }
            }
        }
        _ => Err(SqlError::Unsupported {
            detail: format!("value expression: {expr}"),
        }),
    }
}

pub(super) fn extract_table_name_from_table_with_joins(
    table: &ast::TableWithJoins,
) -> Result<String> {
    match &table.relation {
        ast::TableFactor::Table { name, .. } => Ok(normalize_object_name(name)),
        _ => Err(SqlError::Unsupported {
            detail: "non-table target in DML".into(),
        }),
    }
}

/// Extract point-operation keys from WHERE clause (WHERE pk = literal OR pk IN (...)).
pub(super) fn extract_point_keys(
    selection: Option<&ast::Expr>,
    info: &CollectionInfo,
) -> Vec<SqlValue> {
    let pk = match &info.primary_key {
        Some(pk) => pk.clone(),
        None => return Vec::new(),
    };

    let expr = match selection {
        Some(e) => e,
        None => return Vec::new(),
    };

    let mut keys = Vec::new();
    collect_pk_equalities(expr, &pk, &mut keys);
    keys
}

fn collect_pk_equalities(expr: &ast::Expr, pk: &str, keys: &mut Vec<SqlValue>) {
    match expr {
        ast::Expr::BinaryOp {
            left,
            op: ast::BinaryOperator::Eq,
            right,
        } => {
            if is_column(left, pk)
                && let Ok(v) = expr_to_sql_value(right)
            {
                keys.push(v);
            } else if is_column(right, pk)
                && let Ok(v) = expr_to_sql_value(left)
            {
                keys.push(v);
            }
        }
        ast::Expr::BinaryOp {
            left,
            op: ast::BinaryOperator::Or,
            right,
        } => {
            collect_pk_equalities(left, pk, keys);
            collect_pk_equalities(right, pk, keys);
        }
        ast::Expr::InList {
            expr: inner,
            list,
            negated: false,
        } if is_column(inner, pk) => {
            for item in list {
                if let Ok(v) = expr_to_sql_value(item) {
                    keys.push(v);
                }
            }
        }
        _ => {}
    }
}

fn is_column(expr: &ast::Expr, name: &str) -> bool {
    match expr {
        ast::Expr::Identifier(ident) => normalize_ident(ident) == name,
        ast::Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            normalize_ident(&parts[1]) == name
        }
        _ => false,
    }
}
