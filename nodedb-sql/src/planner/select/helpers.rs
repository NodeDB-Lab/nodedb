// SPDX-License-Identifier: Apache-2.0

//! Shared helpers for SELECT planning: projection conversion, WHERE
//! filter conversion, and AST literal extraction utilities.

use sqlparser::ast;

use crate::error::{Result, SqlError};
use crate::parser::normalize::{SCHEMA_QUALIFIED_MSG, normalize_ident};
use crate::resolver::ColumnScope;
use crate::resolver::columns::TableScope;
use crate::resolver::expr::convert_expr;
use crate::types::*;

/// Output projection carried by a base read plan, used to seed the
/// `projection` of a search variant rewritten from it. Returns the resolved
/// SELECT target list for the plans that carry one (`Scan`, `Join`,
/// `TextSearch`); an empty list for shapes that do not.
pub(super) fn source_projection(plan: &SqlPlan) -> Vec<Projection> {
    match plan {
        SqlPlan::Scan { projection, .. }
        | SqlPlan::Join { projection, .. }
        | SqlPlan::TextSearch { projection, .. } => projection.clone(),
        _ => Vec::new(),
    }
}

/// Convert SELECT projection items.
pub fn convert_projection(
    items: &[ast::SelectItem],
    scope: &TableScope,
) -> Result<Vec<Projection>> {
    let scope = ColumnScope::Relations(scope);
    let mut result = Vec::new();
    for item in items {
        match item {
            ast::SelectItem::UnnamedExpr(expr) => {
                let sql_expr = convert_expr(expr, &scope)?;
                match &sql_expr {
                    SqlExpr::Column { table, name } => {
                        result.push(Projection::Column(qualified_name(table.as_deref(), name)));
                    }
                    SqlExpr::Wildcard => {
                        result.push(Projection::Star);
                    }
                    _ => {
                        result.push(Projection::Computed {
                            expr: sql_expr,
                            alias: format!("{expr}").to_lowercase(),
                        });
                    }
                }
            }
            ast::SelectItem::ExprWithAlias { expr, alias } => {
                let sql_expr = convert_expr(expr, &scope)?;
                result.push(Projection::Computed {
                    expr: sql_expr,
                    alias: normalize_ident(alias),
                });
            }
            // `expr AS (a, b)` binds one expression to several output names,
            // which only has meaning for a dialect that expands a projection
            // into multiple columns. `Projection` carries a single name, and
            // NodeDB's PostgreSQL surface has no such expansion, so the
            // statement is refused rather than silently taking one alias.
            ast::SelectItem::ExprWithAliases { aliases, .. } => {
                return Err(SqlError::Unsupported {
                    detail: format!(
                        "multi-alias projection ('AS' with {} names) is not supported; \
                         give the expression a single alias",
                        aliases.len()
                    ),
                });
            }
            ast::SelectItem::Wildcard(_) => {
                result.push(Projection::Star);
            }
            ast::SelectItem::QualifiedWildcard(kind, _) => {
                let table_name = match kind {
                    ast::SelectItemQualifiedWildcardKind::ObjectName(name) => {
                        crate::parser::normalize::normalize_object_name_checked(name)?
                    }
                    _ => String::new(),
                };
                result.push(Projection::QualifiedStar(table_name));
            }
        }
    }
    Ok(result)
}

/// Build a qualified column reference (`table.name` or just `name`).
pub fn qualified_name(table: Option<&str>, name: &str) -> String {
    table.map_or_else(|| name.to_string(), |table| format!("{table}.{name}"))
}

/// Convert a WHERE expression into a list of Filter.
pub fn convert_where_to_filters(expr: &ast::Expr, scope: &TableScope) -> Result<Vec<Filter>> {
    let sql_expr = canonicalize_predicate(convert_expr(expr, &ColumnScope::Relations(scope))?);
    Ok(vec![Filter {
        expr: FilterExpr::Expr(sql_expr),
    }])
}

/// Canonicalize comparison operand order so a bare column always sits on the
/// left of a comparison (`column <op> literal`), flipping the operator when a
/// swap happens (`5 < id` → `id > 5`).
///
/// Written literal-first (`'x' = id`), a predicate is logically identical to
/// its column-first spelling but reaches the planner in a shape only some
/// extractors recognize: the primary-key point-get rewrite accepts
/// `column = literal` but not `literal = column`, so the two spellings would
/// route to different physical operators (index point-lookup vs sequential
/// scan) that disagree whenever the index and the stored tuples diverge.
/// Normalizing at this single WHERE→filter choke point guarantees every
/// downstream extractor — point-get, index lookup, scan-filter fast path —
/// sees one canonical shape.
fn canonicalize_predicate(expr: SqlExpr) -> SqlExpr {
    match expr {
        SqlExpr::BinaryOp { left, op, right } => {
            let left = canonicalize_predicate(*left);
            let right = canonicalize_predicate(*right);
            // Only swap when the column sits on the right and the left side is
            // not itself a bare column — the exact shape (`literal = column`)
            // the point-get / fast-path extractors fail to recognize.
            if let Some(flipped) = flip_comparison(op)
                && !matches!(left, SqlExpr::Column { .. })
                && matches!(right, SqlExpr::Column { .. })
            {
                return SqlExpr::BinaryOp {
                    left: Box::new(right),
                    op: flipped,
                    right: Box::new(left),
                };
            }
            SqlExpr::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            }
        }
        SqlExpr::UnaryOp { op, expr } => SqlExpr::UnaryOp {
            op,
            expr: Box::new(canonicalize_predicate(*expr)),
        },
        other => other,
    }
}

/// Map a comparison operator to the operator that preserves meaning when its
/// two operands are swapped. Returns `None` for non-comparison operators
/// (arithmetic, logical, string), which are never operand-swapped here.
fn flip_comparison(op: BinaryOp) -> Option<BinaryOp> {
    match op {
        BinaryOp::Eq => Some(BinaryOp::Eq),
        BinaryOp::Ne => Some(BinaryOp::Ne),
        BinaryOp::Gt => Some(BinaryOp::Lt),
        BinaryOp::Ge => Some(BinaryOp::Le),
        BinaryOp::Lt => Some(BinaryOp::Gt),
        BinaryOp::Le => Some(BinaryOp::Ge),
        BinaryOp::Add
        | BinaryOp::Sub
        | BinaryOp::Mul
        | BinaryOp::Div
        | BinaryOp::Mod
        | BinaryOp::And
        | BinaryOp::Or
        | BinaryOp::Concat => None,
    }
}

pub fn extract_func_args(func: &ast::Function) -> Result<Vec<ast::Expr>> {
    match &func.args {
        ast::FunctionArguments::List(args) => Ok(args
            .args
            .iter()
            .filter_map(|a| match a {
                ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => Some(e.clone()),
                _ => None,
            })
            .collect()),
        _ => Ok(Vec::new()),
    }
}

pub(super) fn extract_column_name(expr: &ast::Expr) -> Result<String> {
    match expr {
        ast::Expr::Identifier(ident) => Ok(normalize_ident(ident)),
        ast::Expr::CompoundIdentifier(parts) if parts.len() >= 3 => {
            let qualified: String = parts
                .iter()
                .map(normalize_ident)
                .collect::<Vec<_>>()
                .join(".");
            Err(SqlError::Unsupported {
                detail: format!(
                    "schema-qualified column reference '{qualified}': {SCHEMA_QUALIFIED_MSG}"
                ),
            })
        }
        ast::Expr::CompoundIdentifier(parts) => Ok(parts
            .iter()
            .map(normalize_ident)
            .collect::<Vec<_>>()
            .join(".")),
        _ => Err(SqlError::Unsupported {
            detail: format!("expected column name, got: {expr}"),
        }),
    }
}

pub fn extract_string_literal(expr: &ast::Expr) -> Result<String> {
    match expr {
        ast::Expr::Value(v) => match &v.value {
            ast::Value::SingleQuotedString(s) => Ok(s.clone()),
            _ => Err(SqlError::Unsupported {
                detail: format!("expected string literal, got: {expr}"),
            }),
        },
        _ => Err(SqlError::Unsupported {
            detail: format!("expected string literal, got: {expr}"),
        }),
    }
}

pub fn extract_float(expr: &ast::Expr) -> Result<f64> {
    match expr {
        ast::Expr::Value(v) => match &v.value {
            ast::Value::Number(n, _) => n.parse::<f64>().map_err(|_| SqlError::TypeMismatch {
                detail: format!("expected number: {n}"),
            }),
            _ => Err(SqlError::TypeMismatch {
                detail: format!("expected number, got: {expr}"),
            }),
        },
        // Handle negative numbers: -73.9855 is parsed as UnaryOp { Minus, 73.9855 }
        ast::Expr::UnaryOp {
            op: ast::UnaryOperator::Minus,
            expr: inner,
        } => extract_float(inner).map(|f| -f),
        _ => Err(SqlError::TypeMismatch {
            detail: format!("expected number, got: {expr}"),
        }),
    }
}

/// Map a vector distance function name to its `DistanceMetric`.
///
/// `vector_distance` (and the rewritten `<->` operator) → L2;
/// `vector_cosine_distance` (and `<=>`) → Cosine;
/// `vector_neg_inner_product` (and `<#>`) → InnerProduct.
/// Unknown names default to L2 — callers must gate on a `VectorSearch`
/// search-trigger before invoking this so unknown names cannot leak in.
pub(super) fn metric_from_func_name(name: &str) -> DistanceMetric {
    if name.eq_ignore_ascii_case("vector_cosine_distance") {
        DistanceMetric::Cosine
    } else if name.eq_ignore_ascii_case("vector_neg_inner_product") {
        DistanceMetric::InnerProduct
    } else {
        DistanceMetric::L2
    }
}

/// Extract a float array from ARRAY[...], make_array(...), or a JSON-array
/// string literal like `'[1.0, 0.5, 0.0]'`.
pub(super) fn extract_float_array(expr: &ast::Expr) -> Result<Vec<f32>> {
    match expr {
        ast::Expr::Array(ast::Array { elem, .. }) => elem
            .iter()
            .map(|e| extract_float(e).map(|f| f as f32))
            .collect(),
        ast::Expr::Function(func) => {
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
            if name == "make_array" || name == "array" {
                let args = extract_func_args(func)?;
                args.iter()
                    .map(|e| extract_float(e).map(|f| f as f32))
                    .collect()
            } else {
                Err(SqlError::Unsupported {
                    detail: format!("expected array, got function: {name}"),
                })
            }
        }
        // Accept JSON-array string literals: `'[1.0, 0.5, 0.0]'`.
        // This is the canonical pgvector-compatible form for embedding vectors
        // passed as SQL string literals.
        ast::Expr::Value(v) => {
            let s = match &v.value {
                sqlparser::ast::Value::SingleQuotedString(s) => s.as_str(),
                sqlparser::ast::Value::DoubleQuotedString(s) => s.as_str(),
                _ => {
                    return Err(SqlError::Unsupported {
                        detail: format!("expected array literal, got: {expr}"),
                    });
                }
            };
            let trimmed = s.trim();
            if !trimmed.starts_with('[') || !trimmed.ends_with(']') {
                return Err(SqlError::Unsupported {
                    detail: format!("expected JSON array string, got: {s:?}"),
                });
            }
            let inner = &trimmed[1..trimmed.len() - 1];
            if inner.trim().is_empty() {
                return Ok(Vec::new());
            }
            inner
                .split(',')
                .map(|part| {
                    part.trim()
                        .parse::<f32>()
                        .map_err(|_| SqlError::Unsupported {
                            detail: format!("cannot parse float from array element: {part:?}"),
                        })
                })
                .collect()
        }
        _ => Err(SqlError::Unsupported {
            detail: format!("expected array literal, got: {expr}"),
        }),
    }
}
