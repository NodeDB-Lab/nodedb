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
            push_key(value, keys, crs)
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
        } if is_column(inner, pk) => list.iter().all(|item| push_key(item, keys, crs)),
        _ => false,
    }
}

/// Push the literal `expr` as a key. Skips NULL (returns `true` without
/// pushing): `pk = NULL` is unknown and matches no row, so `pk IN ('a', NULL)`
/// matches only `'a'` and `pk = NULL` / `pk IN (NULL)` produce an empty key
/// list rather than looking up the key `''`. Returns `false` for any
/// non-literal so the statement falls back to the predicate path.
fn push_key(expr: &ast::Expr, keys: &mut Vec<SqlValue>, crs: &nodedb_crs::CrsContext<'_>) -> bool {
    match expr_to_sql_value(expr, crs) {
        Ok(SqlValue::Null) => true,
        Err(_) => false,
        Ok(v) => {
            keys.push(v);
            true
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    fn keys_for(where_sql: &str) -> Vec<SqlValue> {
        let expr = Parser::new(&GenericDialect {})
            .try_with_sql(where_sql)
            .expect("tokenize")
            .parse_expr()
            .expect("parse");
        let crs = nodedb_crs::CrsContext::builtin_only();
        let mut keys = Vec::new();
        if collect_pk_equalities(&expr, "id", &mut keys, &crs) {
            keys
        } else {
            Vec::new()
        }
    }

    #[test]
    fn pk_equal_null_is_not_a_point_key() {
        assert!(keys_for("id = NULL").is_empty());
        assert!(keys_for("NULL = id").is_empty());
    }

    #[test]
    fn pk_in_list_with_null_drops_only_null() {
        assert!(keys_for("id IN (NULL)").is_empty());
        assert_eq!(
            keys_for("id IN ('a', NULL)"),
            vec![SqlValue::String("a".into())]
        );
    }

    #[test]
    fn pk_equal_literal_stays_a_point_key() {
        assert_eq!(keys_for("id = 'a'"), vec![SqlValue::String("a".into())]);
        assert_eq!(
            keys_for("id IN ('a', 'b')"),
            vec![SqlValue::String("a".into()), SqlValue::String("b".into())]
        );
    }

    fn bound_selection_yields_key(sql: &str, params: &[crate::params::ParamValue]) -> bool {
        let mut stmt = crate::parser::statement::parse_sql(sql)
            .expect("parse")
            .remove(0);
        crate::params::bind_params(&mut stmt, params);
        let selection = match &stmt {
            ast::Statement::Delete(d) => d.selection.clone(),
            ast::Statement::Update(u) => u.selection.clone(),
            other => panic!("not a DML statement: {other}"),
        };
        let crs = nodedb_crs::CrsContext::builtin_only();
        let mut keys = Vec::new();
        collect_pk_equalities(&selection.expect("WHERE"), "id", &mut keys, &crs) && !keys.is_empty()
    }

    #[test]
    fn bound_null_param_is_not_a_point_key() {
        use crate::params::ParamValue;
        assert!(!bound_selection_yields_key(
            "DELETE FROM t WHERE id = $1",
            &[ParamValue::Null]
        ));
        assert!(!bound_selection_yields_key(
            "UPDATE t SET v = 1 WHERE id = $1",
            &[ParamValue::Null]
        ));
        assert!(bound_selection_yields_key(
            "DELETE FROM t WHERE id = $1",
            &[ParamValue::Text("a".into())]
        ));
    }
}
