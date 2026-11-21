// SPDX-License-Identifier: Apache-2.0

//! AST extraction helpers shared by the DML planners: pulling a bare table
//! name out of a join tree, and pulling primary-key point-lookup keys out of
//! a `WHERE` clause.

use sqlparser::ast;

use super::value_convert::expr_to_sql_value;
use crate::error::{Result, SqlError};
use crate::parser::normalize::{normalize_ident, normalize_object_name_checked};
use crate::planner::predicate_coerce::coerce_read_literal;
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
///
/// Each key takes the read-side literal rule of the key column
/// ([`coerce_read_literal`]), the rule the write side applies to the same
/// column. An instant-typed key is stored under its typed instant, so a raw
/// integer or text key names no stored row. A key the rule refuses makes the
/// statement take the predicate path, whose own coercion reports the literal.
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

    let key_column = info
        .columns
        .iter()
        .find(|c| c.name.eq_ignore_ascii_case(&pk));
    let key = PointKey {
        name: &pk,
        column: key_column,
        crs,
    };
    let mut keys = Vec::new();
    // The keys stand for the WHERE only when they cover every row it matches.
    // A disjunct that is no key equality (`id = 'd' OR v < 2`) matches rows no
    // key names, so the statement takes the predicate path.
    if collect_pk_equalities(expr, &key, &mut keys) {
        keys
    } else {
        Vec::new()
    }
}

/// The key column a point key is extracted for.
struct PointKey<'a, 'c> {
    name: &'a str,
    /// The declared key column. `None` when the catalog declares no column
    /// under the key name (the KV `key` convention): its literal stays as
    /// written.
    column: Option<&'a ColumnInfo>,
    crs: &'a nodedb_crs::CrsContext<'c>,
}

impl PointKey<'_, '_> {
    /// The key a literal expression names, coerced by the key column's
    /// read-side rule. `None` when the expression is no literal or the rule
    /// refuses it.
    fn literal(&self, expr: &ast::Expr) -> Option<SqlValue> {
        let value = expr_to_sql_value(expr, self.crs).ok()?;
        match self.column {
            Some(column) => coerce_read_literal(column, value).ok(),
            None => Some(value),
        }
    }
}

/// Push the keys `expr` names into `keys`. Returns `false` when `expr` matches
/// a row no pushed key names: a disjunct that is no key equality, or a key
/// value that is no literal.
fn collect_pk_equalities(
    expr: &ast::Expr,
    key: &PointKey<'_, '_>,
    keys: &mut Vec<SqlValue>,
) -> bool {
    match expr {
        ast::Expr::BinaryOp {
            left,
            op: ast::BinaryOperator::Eq,
            right,
        } => {
            let value = if is_column(left, key.name) {
                right
            } else if is_column(right, key.name) {
                left
            } else {
                return false;
            };
            match key.literal(value) {
                Some(v) => {
                    keys.push(v);
                    true
                }
                None => false,
            }
        }
        ast::Expr::BinaryOp {
            left,
            op: ast::BinaryOperator::Or,
            right,
        } => collect_pk_equalities(left, key, keys) && collect_pk_equalities(right, key, keys),
        ast::Expr::InList {
            expr: inner,
            list,
            negated: false,
        } if is_column(inner, key.name) => list.iter().all(|item| match key.literal(item) {
            Some(v) => {
                keys.push(v);
                true
            }
            None => false,
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

#[cfg(test)]
mod tests {
    use nodedb_types::datetime::NdbDateTime;

    use super::*;

    /// `2020-03-05 10:00:00 UTC`, in epoch milliseconds.
    const EARLY_MS: i64 = 1_583_402_400_000;

    fn column(name: &str, data_type: SqlDataType) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            data_type,
            nullable: false,
            is_primary_key: true,
            default: None,
            raw_type: None,
            int_width: None,
            float_width: None,
        }
    }

    fn collection(key: ColumnInfo) -> CollectionInfo {
        CollectionInfo {
            name: "t".to_string(),
            engine: EngineType::DocumentStrict,
            primary_key: Some(key.name.clone()),
            columns: vec![key],
            has_auto_tier: false,
            indexes: Vec::new(),
            bitemporal: false,
            primary: nodedb_types::PrimaryEngine::Document,
            vector_primary: None,
            partition_strategy: nodedb_types::PartitionStrategy::CollectionHomed,
            open_schema: CollectionInfo::open_schema_for(EngineType::DocumentStrict),
            text_indexes: None,
        }
    }

    fn keys(info: &CollectionInfo, selection: &str) -> Vec<SqlValue> {
        let dialect = sqlparser::dialect::PostgreSqlDialect {};
        let expr = sqlparser::parser::Parser::new(&dialect)
            .try_with_sql(selection)
            .and_then(|mut parser| parser.parse_expr())
            .expect("selection parses");
        extract_point_keys(Some(&expr), info, &nodedb_crs::CrsContext::builtin_only())
    }

    /// An instant-typed key is written under its typed instant, so the key
    /// an `UPDATE` / `DELETE` targets must be that instant too. Left as the
    /// bare literal, the key renders as `1583402400000` while the stored row
    /// is keyed by its instant text, and the statement touches no row.
    #[test]
    fn an_instant_typed_key_is_coerced_to_its_instant() {
        let early = SqlValue::Timestamp(NdbDateTime::from_micros(EARLY_MS * 1_000));
        let info = collection(column("pk_at", SqlDataType::Timestamp));
        assert_eq!(
            keys(&info, &format!("pk_at = {EARLY_MS}")),
            vec![early.clone()]
        );
        assert_eq!(
            keys(
                &info,
                &format!("pk_at IN ({EARLY_MS}, '2020-03-05 10:00:00')")
            ),
            vec![early.clone(), early]
        );
    }

    /// A key literal the instant rule refuses names no point key: the
    /// statement takes the predicate path, whose coercion reports it.
    #[test]
    fn an_unparseable_instant_key_names_no_point_key() {
        let info = collection(column("pk_at", SqlDataType::Timestamp));
        assert!(keys(&info, "pk_at = 'not a date'").is_empty());
    }

    /// A non-instant key keeps its literal as written, the write side's
    /// primary-key exemption.
    #[test]
    fn a_non_instant_key_keeps_its_literal() {
        let info = collection(column("id", SqlDataType::String));
        assert_eq!(keys(&info, "id = 5"), vec![SqlValue::Int(5)]);
    }
}
