// SPDX-License-Identifier: Apache-2.0

//! Plan a predicate against one collection the way a single-collection
//! `SELECT ... WHERE` plans its `WHERE`.
//!
//! An entry point that receives a predicate as a typed tree instead of SQL
//! text (the native protocol's metadata filter) plans it here. The
//! predicate then gets the same column check and the same literal coercion
//! as the `WHERE` a SQL client writes for the same filter, so both entry
//! points match the same rows.

use nodedb_types::DatabaseId;

use crate::error::{Result, SqlError};
use crate::planner::predicate_coerce::coerce_predicate_literals;
use crate::resolver::columns::{ResolvedTable, TableScope};
use crate::types::{Filter, FilterExpr, SqlCatalog, SqlExpr};

/// Plan `predicate` as the `WHERE` of `SELECT * FROM <collection>`.
///
/// A collection the catalog does not hold is `UnknownTable`. A column the
/// collection does not have is `UnknownColumn`. A literal compared against a
/// declared `TIMESTAMP` / `TIMESTAMPTZ` column becomes a typed instant.
pub fn plan_collection_predicate(
    catalog: &dyn SqlCatalog,
    collection: &str,
    mut predicate: SqlExpr,
) -> Result<Vec<Filter>> {
    let info = catalog
        .resolve_relation(DatabaseId::DEFAULT, collection)?
        .ok_or_else(|| SqlError::UnknownTable {
            name: collection.to_string(),
        })?;
    let scope = TableScope::single(ResolvedTable {
        name: collection.to_string(),
        alias: None,
        info,
    })?;
    check_columns(&predicate, &scope)?;
    coerce_predicate_literals(&mut predicate, &scope)?;
    Ok(vec![Filter {
        expr: FilterExpr::Expr(predicate),
    }])
}

/// Reject a column reference that names no column of the scope.
fn check_columns(expr: &SqlExpr, scope: &TableScope) -> Result<()> {
    match expr {
        SqlExpr::Column { table, name } => scope.check_name(table.as_deref(), name),
        SqlExpr::Literal(_) | SqlExpr::Wildcard | SqlExpr::Subquery(_) => Ok(()),
        SqlExpr::BinaryOp { left, right, .. } => {
            check_columns(left, scope)?;
            check_columns(right, scope)
        }
        SqlExpr::UnaryOp { expr, .. }
        | SqlExpr::Cast { expr, .. }
        | SqlExpr::IsNull { expr, .. } => check_columns(expr, scope),
        SqlExpr::Function { args, .. } | SqlExpr::ArrayLiteral(args) => {
            args.iter().try_for_each(|arg| check_columns(arg, scope))
        }
        SqlExpr::Case {
            operand,
            when_then,
            else_expr,
        } => {
            if let Some(operand) = operand {
                check_columns(operand, scope)?;
            }
            for (when, then) in when_then {
                check_columns(when, scope)?;
                check_columns(then, scope)?;
            }
            match else_expr {
                Some(else_expr) => check_columns(else_expr, scope),
                None => Ok(()),
            }
        }
        SqlExpr::InList { expr, list, .. } => {
            check_columns(expr, scope)?;
            list.iter().try_for_each(|item| check_columns(item, scope))
        }
        SqlExpr::Between {
            expr, low, high, ..
        } => {
            check_columns(expr, scope)?;
            check_columns(low, scope)?;
            check_columns(high, scope)
        }
        SqlExpr::Like { expr, pattern, .. } => {
            check_columns(expr, scope)?;
            check_columns(pattern, scope)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::SqlCatalogError;
    use crate::types::{BinaryOp, CollectionInfo, ColumnInfo, EngineType, SqlDataType, SqlValue};

    struct OneCollection;

    impl SqlCatalog for OneCollection {
        fn get_collection(
            &self,
            _database_id: DatabaseId,
            name: &str,
        ) -> std::result::Result<Option<CollectionInfo>, SqlCatalogError> {
            if name != "docs" {
                return Ok(None);
            }
            Ok(Some(CollectionInfo {
                name: "docs".into(),
                engine: EngineType::DocumentStrict,
                columns: vec![ColumnInfo {
                    name: "category".into(),
                    data_type: SqlDataType::String,
                    nullable: true,
                    is_primary_key: false,
                    default: None,
                    raw_type: None,
                    int_width: None,
                    float_width: None,
                }],
                primary_key: None,
                has_auto_tier: false,
                indexes: Vec::new(),
                bitemporal: false,
                primary: nodedb_types::PrimaryEngine::Document,
                vector_primary: None,
                partition_strategy: nodedb_types::PartitionStrategy::CollectionHomed,
                open_schema: CollectionInfo::open_schema_for(EngineType::DocumentStrict),
            }))
        }
    }

    fn eq(field: &str, value: &str) -> SqlExpr {
        SqlExpr::BinaryOp {
            left: Box::new(SqlExpr::Column {
                table: None,
                name: field.into(),
            }),
            op: BinaryOp::Eq,
            right: Box::new(SqlExpr::Literal(SqlValue::String(value.into()))),
        }
    }

    #[test]
    fn declared_column_plans_as_where_expression() {
        let filters = plan_collection_predicate(&OneCollection, "docs", eq("category", "ai"))
            .expect("declared column plans");
        assert_eq!(filters.len(), 1);
        assert!(matches!(filters[0].expr, FilterExpr::Expr(_)));
    }

    #[test]
    fn undeclared_column_is_unknown_column() {
        let err = plan_collection_predicate(&OneCollection, "docs", eq("ghost", "x"))
            .expect_err("a strict collection rejects an undeclared column");
        assert!(matches!(err, SqlError::UnknownColumn { .. }), "{err:?}");
    }

    #[test]
    fn unknown_collection_is_unknown_table() {
        let err = plan_collection_predicate(&OneCollection, "nope", eq("category", "ai"))
            .expect_err("no such collection");
        assert!(matches!(err, SqlError::UnknownTable { .. }), "{err:?}");
    }
}
