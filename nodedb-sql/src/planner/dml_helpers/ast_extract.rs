// SPDX-License-Identifier: Apache-2.0

//! AST extraction helpers shared by the DML planners: pulling a bare table
//! name out of a join tree, and pulling primary-key point-lookup keys out of
//! a `WHERE` clause.

use sqlparser::ast;

use super::value_convert::expr_to_sql_value;
use crate::error::{Result, SqlError};
use crate::parser::normalize::{normalize_ident, normalize_object_name_checked};
use crate::types::*;

pub(crate) fn extract_table_name_from_table_with_joins(
    table: &ast::TableWithJoins,
) -> Result<String> {
    match &table.relation {
        ast::TableFactor::Table { name, .. } => Ok(normalize_object_name_checked(name)?),
        _ => Err(SqlError::Unsupported {
            detail: "non-table target in DML".into(),
        }),
    }
}

/// Extract point-operation keys from WHERE clause (WHERE pk = literal OR pk IN (...)).
/// Geo functions in a key value resolve SRIDs through `crs`.
pub fn extract_point_keys(
    selection: Option<&ast::Expr>,
    info: &CollectionInfo,
    crs: &nodedb_crs::CrsContext<'_>,
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
    // The keys stand for the WHERE only when they cover every row it matches.
    // A disjunct that is no key equality (`id = 'd' OR v < 2`) matches rows no
    // key names, so the statement takes the predicate path.
    if collect_pk_equalities(expr, &pk, &mut keys, crs) {
        keys
    } else {
        Vec::new()
    }
}

/// Push the keys `expr` names into `keys`. Returns `false` when `expr` matches
/// a row no pushed key names: a disjunct that is no key equality, or a key
/// value that is no literal.
fn collect_pk_equalities(
    expr: &ast::Expr,
    pk: &str,
    keys: &mut Vec<SqlValue>,
    crs: &nodedb_crs::CrsContext<'_>,
) -> bool {
    match expr {
        ast::Expr::BinaryOp {
            left,
            op: ast::BinaryOperator::Eq,
            right,
        } => {
            let value = if is_column(left, pk) {
                right
            } else if is_column(right, pk) {
                left
            } else {
                return false;
            };
            match expr_to_sql_value(value, crs) {
                Ok(v) => {
                    keys.push(v);
                    true
                }
                Err(_) => false,
            }
        }
        ast::Expr::BinaryOp {
            left,
            op: ast::BinaryOperator::Or,
            right,
        } => {
            collect_pk_equalities(left, pk, keys, crs)
                && collect_pk_equalities(right, pk, keys, crs)
        }
        ast::Expr::InList {
            expr: inner,
            list,
            negated: false,
        } if is_column(inner, pk) => list.iter().all(|item| match expr_to_sql_value(item, crs) {
            Ok(v) => {
                keys.push(v);
                true
            }
            Err(_) => false,
        }),
        _ => false,
    }
}

fn is_column(expr: &ast::Expr, name: &str) -> bool {
    match expr {
        ast::Expr::Identifier(ident) => normalize_ident(ident) == name,
        // Three or more parts: schema.table.col — never matches a plain pk name.
        ast::Expr::CompoundIdentifier(parts) if parts.len() >= 3 => false,
        ast::Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            normalize_ident(&parts[1]) == name
        }
        _ => false,
    }
}
