// SPDX-License-Identifier: Apache-2.0

//! Recursive catalog-dependent expression validation and folding.

use nodedb_types::DatabaseId;

use crate::catalog::SqlCatalog;
use crate::functions::registry::FunctionRegistry;
use crate::planner::const_fold::FoldScope;
use crate::types::{SqlExpr, SqlValue};

pub(super) fn eval_catalog_constant(
    expr: &SqlExpr,
    catalog: &dyn SqlCatalog,
    functions: &FunctionRegistry,
) -> crate::Result<SqlValue> {
    if let SqlExpr::Cast {
        expr: inner,
        to_type,
    } = expr
        && to_type.eq_ignore_ascii_case("regclass")
        && let SqlExpr::Literal(SqlValue::String(name)) = inner.as_ref()
    {
        let normalized = crate::catalog::normalize_regclass_name(name)
            .ok_or_else(|| crate::SqlError::UnknownTable { name: name.clone() })?;
        catalog
            .resolve_regclass(DatabaseId::DEFAULT, 0, name)
            .ok_or_else(|| crate::SqlError::UnknownTable {
                name: normalized.clone(),
            })?;
        return Ok(SqlValue::String(normalized));
    }
    if let Some(value) = eval_sequence_accessor(expr, catalog)? {
        return Ok(value);
    }
    // A from-less SELECT's plan is marked volatile when it holds a volatile
    // call, so it is never cached. Evaluating the call here therefore serves
    // this execution only, and the next execution re-plans and re-evaluates.
    Ok(
        crate::planner::const_fold::fold_constant_scoped(expr, functions, FoldScope::Once)?
            .unwrap_or(SqlValue::Null),
    )
}

/// Route `nextval` / `currval` / `setval` to the catalog's sequence state.
///
/// Returns `Ok(None)` for every other expression. These calls are `Volatile`,
/// so the constant folder never reaches them and the plan holding the result
/// is never cached — each execution re-plans and allocates again.
pub(super) fn eval_sequence_accessor(
    expr: &SqlExpr,
    catalog: &dyn SqlCatalog,
) -> crate::Result<Option<SqlValue>> {
    let SqlExpr::Function { name, args, .. } = expr else {
        return Ok(None);
    };
    let lowered = name.to_ascii_lowercase();
    if !matches!(lowered.as_str(), "nextval" | "currval" | "setval") {
        return Ok(None);
    }
    let sequence = match args.first() {
        Some(SqlExpr::Literal(SqlValue::String(name))) => name.as_str(),
        _ => {
            return Err(crate::SqlError::Arity {
                detail: format!("{lowered} requires a literal sequence name"),
            });
        }
    };
    let value = match lowered.as_str() {
        "nextval" => catalog.sequence_nextval(DatabaseId::DEFAULT, 0, sequence)?,
        "currval" => catalog.sequence_currval(DatabaseId::DEFAULT, 0, sequence)?,
        _ => {
            let Some(SqlExpr::Literal(SqlValue::Int(target))) = args.get(1) else {
                return Err(crate::SqlError::Arity {
                    detail: "setval requires a literal bigint second argument".into(),
                });
            };
            catalog.sequence_setval(DatabaseId::DEFAULT, 0, sequence, *target)?
        }
    };
    Ok(Some(SqlValue::Int(value)))
}

pub(super) fn validate_expr(
    expr: &SqlExpr,
    catalog: &dyn SqlCatalog,
    database_id: DatabaseId,
    tenant_id: u64,
) -> crate::Result<()> {
    match expr {
        SqlExpr::Cast { expr, to_type }
            if to_type.eq_ignore_ascii_case("regclass")
                && matches!(expr.as_ref(), SqlExpr::Literal(SqlValue::String(_))) =>
        {
            let SqlExpr::Literal(SqlValue::String(name)) = expr.as_ref() else {
                unreachable!();
            };
            if catalog
                .resolve_regclass(database_id, tenant_id, name)
                .is_none()
            {
                return Err(crate::SqlError::UnknownTable { name: name.clone() });
            }
            Ok(())
        }
        SqlExpr::Cast { expr, .. }
        | SqlExpr::IsNull { expr, .. }
        | SqlExpr::UnaryOp { expr, .. } => validate_expr(expr, catalog, database_id, tenant_id),
        SqlExpr::BinaryOp { left, right, .. } => {
            validate_expr(left, catalog, database_id, tenant_id)?;
            validate_expr(right, catalog, database_id, tenant_id)
        }
        SqlExpr::InList { expr, list, .. } => {
            validate_expr(expr, catalog, database_id, tenant_id)?;
            for item in list {
                validate_expr(item, catalog, database_id, tenant_id)?;
            }
            Ok(())
        }
        SqlExpr::ArrayLiteral(items) | SqlExpr::Function { args: items, .. } => {
            for item in items {
                validate_expr(item, catalog, database_id, tenant_id)?;
            }
            Ok(())
        }
        SqlExpr::Between {
            expr, low, high, ..
        } => {
            validate_expr(expr, catalog, database_id, tenant_id)?;
            validate_expr(low, catalog, database_id, tenant_id)?;
            validate_expr(high, catalog, database_id, tenant_id)
        }
        SqlExpr::Like { expr, pattern, .. } => {
            validate_expr(expr, catalog, database_id, tenant_id)?;
            validate_expr(pattern, catalog, database_id, tenant_id)
        }
        SqlExpr::Case {
            operand,
            when_then,
            else_expr,
        } => {
            if let Some(operand) = operand {
                validate_expr(operand, catalog, database_id, tenant_id)?;
            }
            for (when, then) in when_then {
                validate_expr(when, catalog, database_id, tenant_id)?;
                validate_expr(then, catalog, database_id, tenant_id)?;
            }
            if let Some(expr) = else_expr {
                validate_expr(expr, catalog, database_id, tenant_id)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

/// Recursively fold catalog-dependent expressions to constants.
pub(super) fn fold_expr(
    expr: SqlExpr,
    catalog: &dyn SqlCatalog,
    database_id: DatabaseId,
    tenant_id: u64,
) -> SqlExpr {
    match expr {
        SqlExpr::Cast {
            expr: inner_expr,
            to_type,
        } => {
            let upper = to_type.to_ascii_uppercase();
            if upper == "REGCLASS" {
                if let SqlExpr::Literal(SqlValue::String(ref name)) = *inner_expr
                    && let Some(oid) = catalog.resolve_regclass(database_id, tenant_id, name)
                {
                    return SqlExpr::Literal(SqlValue::Int(oid));
                }
            } else if upper == "REGTYPE"
                && let SqlExpr::Literal(SqlValue::String(ref name)) = *inner_expr
                && let Some(oid) = catalog.resolve_regtype(name)
            {
                return SqlExpr::Literal(SqlValue::Int(oid));
            }
            SqlExpr::Cast {
                expr: Box::new(fold_expr(*inner_expr, catalog, database_id, tenant_id)),
                to_type,
            }
        }
        SqlExpr::BinaryOp { left, op, right } => SqlExpr::BinaryOp {
            left: Box::new(fold_expr(*left, catalog, database_id, tenant_id)),
            op,
            right: Box::new(fold_expr(*right, catalog, database_id, tenant_id)),
        },
        SqlExpr::InList {
            expr,
            list,
            negated,
        } => SqlExpr::InList {
            expr: Box::new(fold_expr(*expr, catalog, database_id, tenant_id)),
            list: list
                .into_iter()
                .map(|e| fold_expr(e, catalog, database_id, tenant_id))
                .collect(),
            negated,
        },
        SqlExpr::IsNull { expr, negated } => SqlExpr::IsNull {
            expr: Box::new(fold_expr(*expr, catalog, database_id, tenant_id)),
            negated,
        },
        SqlExpr::UnaryOp { op, expr } => SqlExpr::UnaryOp {
            op,
            expr: Box::new(fold_expr(*expr, catalog, database_id, tenant_id)),
        },
        SqlExpr::ArrayLiteral(elems) => SqlExpr::ArrayLiteral(
            elems
                .into_iter()
                .map(|e| fold_expr(e, catalog, database_id, tenant_id))
                .collect(),
        ),
        SqlExpr::Function {
            name,
            args,
            distinct,
        } => SqlExpr::Function {
            name,
            args: args
                .into_iter()
                .map(|arg| fold_expr(arg, catalog, database_id, tenant_id))
                .collect(),
            distinct,
        },
        SqlExpr::Between {
            expr,
            low,
            high,
            negated,
        } => SqlExpr::Between {
            expr: Box::new(fold_expr(*expr, catalog, database_id, tenant_id)),
            low: Box::new(fold_expr(*low, catalog, database_id, tenant_id)),
            high: Box::new(fold_expr(*high, catalog, database_id, tenant_id)),
            negated,
        },
        SqlExpr::Like {
            expr,
            pattern,
            negated,
            case_insensitive,
        } => SqlExpr::Like {
            expr: Box::new(fold_expr(*expr, catalog, database_id, tenant_id)),
            pattern: Box::new(fold_expr(*pattern, catalog, database_id, tenant_id)),
            negated,
            case_insensitive,
        },
        SqlExpr::Case {
            operand,
            when_then,
            else_expr,
        } => SqlExpr::Case {
            operand: operand.map(|e| Box::new(fold_expr(*e, catalog, database_id, tenant_id))),
            when_then: when_then
                .into_iter()
                .map(|(when, then)| {
                    (
                        fold_expr(when, catalog, database_id, tenant_id),
                        fold_expr(then, catalog, database_id, tenant_id),
                    )
                })
                .collect(),
            else_expr: else_expr.map(|e| Box::new(fold_expr(*e, catalog, database_id, tenant_id))),
        },
        leaf => leaf,
    }
}
