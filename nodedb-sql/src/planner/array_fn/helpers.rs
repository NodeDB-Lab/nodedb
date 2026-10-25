// SPDX-License-Identifier: Apache-2.0

//! Shared helpers for ARRAY_* function planners: argument extraction
//! and validation against the array catalog.

use sqlparser::ast;

use nodedb_types::Value;

use crate::catalog::ArrayCatalogView;
use crate::error::{Result, SqlError};
use crate::parser::normalize::{SCHEMA_QUALIFIED_MSG, normalize_ident};
use crate::parser::object_literal::parse_object_literal_complete;
use crate::types::SqlCatalog;
use crate::types_array::{ArrayCoordLiteral, ArraySliceAst, NamedDimRange};

/// The positional arguments of a call to `fn_name`, which takes no named
/// argument. A named argument, or a wildcard, is refused.
pub(super) fn collect_args(args: &[ast::FunctionArg], fn_name: &str) -> Result<Vec<ast::Expr>> {
    let (positional, named) = split_args(args, fn_name)?;
    if let Some((name, _)) = named.first() {
        return Err(SqlError::Unsupported {
            detail: format!("{fn_name}: takes no named argument, got '{name}'"),
        });
    }
    Ok(positional)
}

/// `name => value` arguments in call order, names lower-cased.
pub(super) type NamedArgs = Vec<(String, ast::Expr)>;

/// The positional and the `name => value` arguments of a call to
/// `fn_name`, each in call order. Names are lower-cased. A wildcard
/// argument, a name that is not an identifier, or `=` in place of `=>` is
/// refused.
pub(super) fn split_args(
    args: &[ast::FunctionArg],
    fn_name: &str,
) -> Result<(Vec<ast::Expr>, NamedArgs)> {
    let mut positional = Vec::new();
    let mut named: NamedArgs = Vec::new();
    for arg in args {
        let (name, value, operator) = match arg {
            ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => {
                positional.push(e.clone());
                continue;
            }
            ast::FunctionArg::Unnamed(other) => {
                return Err(SqlError::Unsupported {
                    detail: format!("{fn_name}: argument {other} is not a value"),
                });
            }
            ast::FunctionArg::Named {
                name,
                arg,
                operator,
            } => (name.value.to_ascii_lowercase(), arg, operator),
            ast::FunctionArg::ExprNamed {
                name: ast::Expr::Identifier(ident),
                arg,
                operator,
            } => (ident.value.to_ascii_lowercase(), arg, operator),
            ast::FunctionArg::ExprNamed { name, .. } => {
                return Err(SqlError::Unsupported {
                    detail: format!("{fn_name}: argument name {name} is not an identifier"),
                });
            }
        };
        if *operator != ast::FunctionArgOperator::RightArrow {
            return Err(SqlError::Unsupported {
                detail: format!("{fn_name}: use '=>' not '{operator}' for named argument '{name}'"),
            });
        }
        let ast::FunctionArgExpr::Expr(value) = value else {
            return Err(SqlError::Unsupported {
                detail: format!("{fn_name}: named argument '{name}' expects a value"),
            });
        };
        if named.iter().any(|(n, _)| *n == name) {
            return Err(SqlError::Unsupported {
                detail: format!("{fn_name}: named argument '{name}' is given twice"),
            });
        }
        named.push((name, value.clone()));
    }
    Ok((positional, named))
}

/// The named arguments of a call to `fn_name`, each name checked against
/// the closed set `allowed`. An unknown name is refused, naming the set.
pub(super) fn collect_named_args(
    named: NamedArgs,
    fn_name: &str,
    allowed: &[&str],
) -> Result<NamedArgs> {
    if let Some((name, _)) = named.iter().find(|(n, _)| !allowed.contains(&n.as_str())) {
        let detail = if allowed.is_empty() {
            format!("{fn_name}: takes no named argument, got '{name}'")
        } else {
            format!(
                "{fn_name}: unknown named argument '{name}'; valid: {}",
                allowed.join(", ")
            )
        };
        return Err(SqlError::Unsupported { detail });
    }
    Ok(named)
}

/// A boolean literal.
pub(super) fn expect_bool(expr: &ast::Expr, ctx: &str) -> Result<bool> {
    match expr {
        ast::Expr::Value(v) => match &v.value {
            ast::Value::Boolean(b) => Ok(*b),
            other => Err(SqlError::TypeMismatch {
                detail: format!("{ctx}: expected true or false, got {other}"),
            }),
        },
        _ => Err(SqlError::TypeMismatch {
            detail: format!("{ctx}: expected true or false, got {expr}"),
        }),
    }
}

/// A numeric literal, with an optional leading minus.
pub(super) fn expect_f64(expr: &ast::Expr, ctx: &str) -> Result<f64> {
    let mismatch = || SqlError::TypeMismatch {
        detail: format!("{ctx}: expected a number, got {expr}"),
    };
    match expr {
        ast::Expr::Value(v) => match &v.value {
            ast::Value::Number(n, _) => n.parse::<f64>().map_err(|_| mismatch()),
            _ => Err(mismatch()),
        },
        ast::Expr::UnaryOp {
            op: ast::UnaryOperator::Minus,
            expr: inner,
        } => expect_f64(inner, ctx).map(|v| -v),
        ast::Expr::Nested(inner) => expect_f64(inner, ctx),
        _ => Err(mismatch()),
    }
}

pub(super) fn require_array_name(
    args: &[ast::Expr],
    idx: usize,
    fn_name: &str,
    catalog: &dyn SqlCatalog,
) -> Result<String> {
    let expr = args.get(idx).ok_or_else(|| SqlError::Unsupported {
        detail: format!("{fn_name}: missing array name at arg {idx}"),
    })?;
    let name = match expr {
        ast::Expr::Identifier(ident) => normalize_ident(ident),
        ast::Expr::CompoundIdentifier(parts) if parts.len() >= 2 => {
            let qualified: String = parts
                .iter()
                .map(normalize_ident)
                .collect::<Vec<_>>()
                .join(".");
            return Err(SqlError::Unsupported {
                detail: format!(
                    "schema-qualified array name '{qualified}': {SCHEMA_QUALIFIED_MSG}"
                ),
            });
        }
        _ => expect_string_literal(expr, &format!("{fn_name} array name"))?,
    };
    if !catalog.array_exists(&name) {
        return Err(SqlError::Unsupported {
            detail: format!("{fn_name}: array '{name}' not found"),
        });
    }
    Ok(name)
}

pub(super) fn expect_string_literal(expr: &ast::Expr, ctx: &str) -> Result<String> {
    match expr {
        ast::Expr::Value(v) => match &v.value {
            ast::Value::SingleQuotedString(s) => Ok(s.clone()),
            other => Err(SqlError::Unsupported {
                detail: format!("{ctx}: expected string literal, got {other}"),
            }),
        },
        ast::Expr::Identifier(ident) => Ok(normalize_ident(ident)),
        _ => Err(SqlError::Unsupported {
            detail: format!("{ctx}: expected string literal, got {expr}"),
        }),
    }
}

pub(super) fn expect_u32(expr: &ast::Expr, ctx: &str) -> Result<u32> {
    match expr {
        ast::Expr::Value(v) => match &v.value {
            ast::Value::Number(n, _) => n.parse::<u32>().map_err(|_| SqlError::TypeMismatch {
                detail: format!("{ctx}: expected u32, got {n}"),
            }),
            other => Err(SqlError::TypeMismatch {
                detail: format!("{ctx}: expected number, got {other}"),
            }),
        },
        _ => Err(SqlError::TypeMismatch {
            detail: format!("{ctx}: expected number literal, got {expr}"),
        }),
    }
}

pub(super) fn expect_string_array(expr: &ast::Expr, ctx: &str) -> Result<Vec<String>> {
    let elems = match expr {
        ast::Expr::Array(arr) => arr.elem.clone(),
        ast::Expr::Function(f)
            if matches!(
                crate::parser::normalize::normalize_object_name_checked(&f.name)
                    .as_deref()
                    .unwrap_or(""),
                "make_array" | "array"
            ) =>
        {
            match &f.args {
                ast::FunctionArguments::List(list) => collect_args(&list.args, ctx)?,
                _ => Vec::new(),
            }
        }
        _ => {
            return Err(SqlError::Unsupported {
                detail: format!("{ctx}: expected array literal, got {expr}"),
            });
        }
    };
    elems
        .iter()
        .map(|e| expect_string_literal(e, ctx))
        .collect()
}

pub(super) fn is_null_literal(expr: &ast::Expr) -> bool {
    matches!(
        expr,
        ast::Expr::Value(v) if matches!(v.value, ast::Value::Null)
    )
}

/// Parse a slice-predicate argument of `fn_name` over array `name`.
///
/// The argument is a quoted string holding the brace-form object literal
/// `'{dim: [lo, hi], ...}'`: the PostgreSQL dialect does not accept a bare
/// `{...}` in expression position. Each named dim must exist on the array.
pub(super) fn parse_slice_arg(
    expr: &ast::Expr,
    fn_name: &str,
    name: &str,
    view: &ArrayCatalogView,
) -> Result<ArraySliceAst> {
    let slice_str = expect_string_literal(expr, &format!("{fn_name} slice predicate"))?;
    let parsed =
        parse_object_literal_complete(&slice_str).ok_or_else(|| SqlError::Unsupported {
            detail: format!("{fn_name}: slice predicate must be an object literal: {slice_str}"),
        })?;
    let map = parsed.map_err(|detail| SqlError::Unsupported {
        detail: format!("{fn_name}: slice parse: {detail}"),
    })?;
    let mut dim_ranges: Vec<NamedDimRange> = Vec::with_capacity(map.len());
    for (dim, val) in map {
        if !view.dims.iter().any(|d| d.name == dim) {
            return Err(SqlError::Unsupported {
                detail: format!("{fn_name}: array '{name}' has no dim '{dim}'"),
            });
        }
        let arr = match val {
            Value::Array(a) if a.len() == 2 => a,
            _ => {
                return Err(SqlError::Unsupported {
                    detail: format!(
                        "{fn_name}: dim '{dim}' range must be a 2-element array [lo, hi]"
                    ),
                });
            }
        };
        let lo = value_to_coord_literal(&arr[0], &dim)?;
        let hi = value_to_coord_literal(&arr[1], &dim)?;
        dim_ranges.push(NamedDimRange { dim, lo, hi });
    }
    Ok(ArraySliceAst { dim_ranges })
}

pub(super) fn value_to_coord_literal(v: &Value, dim: &str) -> Result<ArrayCoordLiteral> {
    match v {
        Value::Integer(i) => Ok(ArrayCoordLiteral::Int64(*i)),
        Value::Float(f) => Ok(ArrayCoordLiteral::Float64(*f)),
        Value::String(s) => Ok(ArrayCoordLiteral::String(s.clone())),
        other => Err(SqlError::TypeMismatch {
            detail: format!("ARRAY_SLICE dim '{dim}' bound: unsupported value kind {other:?}"),
        }),
    }
}
