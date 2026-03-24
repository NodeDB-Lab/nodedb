//! Convert DataFusion `Expr` → NodeDB `SqlExpr` for Data Plane evaluation.
//!
//! This bridges the Control Plane's logical plan expressions to the
//! serializable expression tree that the Data Plane can evaluate against
//! JSON documents. Used for computed columns in SELECT projections.

use datafusion::prelude::*;

use crate::bridge::expr_eval::{BinaryOp, CastType, ComputedColumn, SqlExpr};
use crate::control::planner::expr_convert::expr_to_json_value;

/// Convert a DataFusion `Expr` to a NodeDB `SqlExpr`.
///
/// Returns `None` if the expression type is not supported (e.g., window
/// functions, correlated subqueries). Callers should fall back to
/// column-only projection in that case.
pub(super) fn datafusion_expr_to_sql_expr(expr: &Expr) -> Option<SqlExpr> {
    match expr {
        Expr::Column(col) => Some(SqlExpr::Column(col.name.clone())),

        Expr::Literal(..) => {
            let json_val = expr_to_json_value(expr);
            Some(SqlExpr::Literal(json_val))
        }

        Expr::Alias(alias) => {
            // Recurse through alias — the alias name is captured separately
            // by the ComputedColumn wrapper.
            datafusion_expr_to_sql_expr(&alias.expr)
        }

        Expr::BinaryExpr(binary) => {
            let left = datafusion_expr_to_sql_expr(&binary.left)?;
            let right = datafusion_expr_to_sql_expr(&binary.right)?;
            let op = match binary.op {
                datafusion::logical_expr::Operator::Plus => BinaryOp::Add,
                datafusion::logical_expr::Operator::Minus => BinaryOp::Sub,
                datafusion::logical_expr::Operator::Multiply => BinaryOp::Mul,
                datafusion::logical_expr::Operator::Divide => BinaryOp::Div,
                datafusion::logical_expr::Operator::Modulo => BinaryOp::Mod,
                datafusion::logical_expr::Operator::Eq => BinaryOp::Eq,
                datafusion::logical_expr::Operator::NotEq => BinaryOp::NotEq,
                datafusion::logical_expr::Operator::Gt => BinaryOp::Gt,
                datafusion::logical_expr::Operator::GtEq => BinaryOp::GtEq,
                datafusion::logical_expr::Operator::Lt => BinaryOp::Lt,
                datafusion::logical_expr::Operator::LtEq => BinaryOp::LtEq,
                datafusion::logical_expr::Operator::And => BinaryOp::And,
                datafusion::logical_expr::Operator::Or => BinaryOp::Or,
                datafusion::logical_expr::Operator::StringConcat => BinaryOp::Concat,
                _ => return None,
            };
            Some(SqlExpr::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            })
        }

        Expr::Not(inner) => {
            let inner_expr = datafusion_expr_to_sql_expr(inner)?;
            Some(SqlExpr::Negate(Box::new(inner_expr)))
        }

        Expr::Negative(inner) => {
            let inner_expr = datafusion_expr_to_sql_expr(inner)?;
            Some(SqlExpr::Negate(Box::new(inner_expr)))
        }

        Expr::IsNull(inner) => {
            let inner_expr = datafusion_expr_to_sql_expr(inner)?;
            Some(SqlExpr::IsNull {
                expr: Box::new(inner_expr),
                negated: false,
            })
        }

        Expr::IsNotNull(inner) => {
            let inner_expr = datafusion_expr_to_sql_expr(inner)?;
            Some(SqlExpr::IsNull {
                expr: Box::new(inner_expr),
                negated: true,
            })
        }

        Expr::Cast(cast) => {
            let inner = datafusion_expr_to_sql_expr(&cast.expr)?;
            let to_type = arrow_type_to_cast_type(&cast.data_type)?;
            Some(SqlExpr::Cast {
                expr: Box::new(inner),
                to_type,
            })
        }

        Expr::TryCast(cast) => {
            let inner = datafusion_expr_to_sql_expr(&cast.expr)?;
            let to_type = arrow_type_to_cast_type(&cast.data_type)?;
            Some(SqlExpr::Cast {
                expr: Box::new(inner),
                to_type,
            })
        }

        Expr::Case(case) => {
            let operand = case
                .expr
                .as_ref()
                .and_then(|e| datafusion_expr_to_sql_expr(e))
                .map(Box::new);

            let mut when_thens = Vec::new();
            for (when, then) in &case.when_then_expr {
                let w = datafusion_expr_to_sql_expr(when)?;
                let t = datafusion_expr_to_sql_expr(then)?;
                when_thens.push((w, t));
            }

            let else_expr = case
                .else_expr
                .as_ref()
                .and_then(|e| datafusion_expr_to_sql_expr(e))
                .map(Box::new);

            Some(SqlExpr::Case {
                operand,
                when_thens,
                else_expr,
            })
        }

        Expr::ScalarFunction(func) => {
            let name = func.name().to_lowercase();
            let mut args = Vec::new();
            for arg in func.args.iter() {
                args.push(datafusion_expr_to_sql_expr(arg)?);
            }

            // Map DataFusion function names to our function names.
            let mapped_name = match name.as_str() {
                "character_length" | "char_length" => "length".to_string(),
                "substr" => "substr".to_string(),
                other => other.to_string(),
            };

            Some(SqlExpr::Function {
                name: mapped_name,
                args,
            })
        }

        // Unsupported: window functions, subqueries, wildcards, etc.
        _ => None,
    }
}

/// Extract alias name from a DataFusion Expr.
pub(super) fn extract_alias(expr: &Expr) -> String {
    match expr {
        Expr::Alias(alias) => alias.name.clone(),
        Expr::Column(col) => col.name.clone(),
        _ => format!("{expr}"),
    }
}

/// Convert a projection expression list to ComputedColumns.
///
/// Returns `None` if all expressions are simple column references
/// (no computed columns needed — use column-only projection instead).
pub(super) fn try_convert_projection(exprs: &[Expr]) -> Option<Vec<ComputedColumn>> {
    let has_computed = exprs.iter().any(|e| {
        !matches!(e, Expr::Column(_))
            && !matches!(e, Expr::Alias(a) if matches!(*a.expr, Expr::Column(_)))
    });

    if !has_computed {
        return None;
    }

    let mut columns = Vec::with_capacity(exprs.len());
    for expr in exprs {
        let alias = extract_alias(expr);
        let sql_expr = datafusion_expr_to_sql_expr(expr)?;
        columns.push(ComputedColumn {
            alias,
            expr: sql_expr,
        });
    }
    Some(columns)
}

fn arrow_type_to_cast_type(dt: &datafusion::arrow::datatypes::DataType) -> Option<CastType> {
    use datafusion::arrow::datatypes::DataType;
    match dt {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => Some(CastType::Int),
        DataType::Float16 | DataType::Float32 | DataType::Float64 => Some(CastType::Float),
        DataType::Utf8 | DataType::LargeUtf8 => Some(CastType::String),
        DataType::Boolean => Some(CastType::Bool),
        _ => None,
    }
}
