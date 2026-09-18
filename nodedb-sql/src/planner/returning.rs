// SPDX-License-Identifier: Apache-2.0

//! Resolution of a DML `RETURNING` item list against the write's target.
//!
//! The clause is a projection over the target collection, so it converts
//! through the same path a top-level SELECT list does. A bare column and a
//! star pass through. Every other item resolves to
//! [`Projection::CpComputed`]: the Data Plane returns the base columns the
//! expression reads and the Control Plane evaluates it once per returned
//! row. A sequence accessor is allowed here exactly as in a top-level SELECT
//! list.

use sqlparser::ast;

use nodedb_types::DatabaseId;

use crate::error::{Result, SqlError};
use crate::parser::statement::parse_sql;
use crate::planner::select::helpers::convert_projection;
use crate::resolver::columns::{ResolvedTable, TableScope};
use crate::types::{Projection, SqlCatalog, SqlExpr};

/// Resolve a RETURNING item list against the DML target collection.
///
/// `items_sql` is the text after the `RETURNING` keyword. Every non-column
/// item resolves to `Projection::CpComputed`: the Data Plane returns the base
/// columns the expression reads and the Control Plane evaluates it per
/// returned row.
pub fn resolve_returning_items(
    items_sql: &str,
    target: &str,
    catalog: &dyn SqlCatalog,
) -> Result<Vec<Projection>> {
    let items = parse_items(items_sql)?;
    let info = catalog
        .get_collection(DatabaseId::DEFAULT, target)?
        .ok_or_else(|| SqlError::UnknownTable {
            name: target.to_string(),
        })?;
    let scope = TableScope::single(ResolvedTable {
        name: target.to_string(),
        alias: None,
        info,
    })?
    .as_statement_output()
    .allowing_cp_functions();
    let projection = convert_projection(&items, &scope)?;
    Ok(projection.into_iter().map(to_cp_computed).collect())
}

/// The SELECT items of `SELECT <items_sql>`.
///
/// The wrapped statement must be exactly one SELECT with nothing but a
/// projection list: a FROM, WHERE, GROUP BY, ORDER BY, LIMIT, or WITH clause
/// after the keyword is refused, never silently ignored.
fn parse_items(items_sql: &str) -> Result<Vec<ast::SelectItem>> {
    let refuse = || SqlError::Parse {
        detail: format!("invalid RETURNING list: '{}'", items_sql.trim()),
    };
    let mut statements = parse_sql(&format!("SELECT {items_sql}"))?;
    if statements.len() != 1 {
        return Err(refuse());
    }
    let Some(ast::Statement::Query(query)) = statements.pop() else {
        return Err(refuse());
    };
    if query.with.is_some()
        || query.order_by.is_some()
        || query.limit_clause.is_some()
        || query.fetch.is_some()
    {
        return Err(refuse());
    }
    let ast::SetExpr::Select(select) = *query.body else {
        return Err(refuse());
    };
    let grouped = match &select.group_by {
        ast::GroupByExpr::All(_) => true,
        ast::GroupByExpr::Expressions(exprs, _) => !exprs.is_empty(),
    };
    if !select.from.is_empty() || select.selection.is_some() || grouped || select.having.is_some() {
        return Err(refuse());
    }
    Ok(select.projection)
}

/// A computed item becomes Control-Plane computed. A computed item that is a
/// bare column under an alias stays `Computed`: the value is the stored
/// column, looked up under the source name and displayed under the alias, so
/// nothing is evaluated.
fn to_cp_computed(projection: Projection) -> Projection {
    match projection {
        Projection::Computed { expr, alias } => match expr {
            SqlExpr::Column { .. } => Projection::Computed { expr, alias },
            SqlExpr::Function { .. }
            | SqlExpr::Literal(_)
            | SqlExpr::BinaryOp { .. }
            | SqlExpr::UnaryOp { .. }
            | SqlExpr::Case { .. }
            | SqlExpr::Cast { .. }
            | SqlExpr::Subquery(_)
            | SqlExpr::Wildcard
            | SqlExpr::IsNull { .. }
            | SqlExpr::InList { .. }
            | SqlExpr::Between { .. }
            | SqlExpr::Like { .. }
            | SqlExpr::ArrayLiteral(_) => Projection::CpComputed { expr, alias },
        },
        Projection::Column(_)
        | Projection::Star
        | Projection::QualifiedStar(_)
        | Projection::CpComputed { .. } => projection,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::SqlCatalogError;
    use crate::types::{CollectionInfo, ColumnInfo, EngineType, SqlDataType};

    /// One strict `items` collection: `id TEXT`, `score BIGINT`.
    struct ItemsCatalog;

    impl SqlCatalog for ItemsCatalog {
        fn get_collection(
            &self,
            _database_id: DatabaseId,
            name: &str,
        ) -> std::result::Result<Option<CollectionInfo>, SqlCatalogError> {
            if name != "items" {
                return Ok(None);
            }
            let col = |n: &str, t: SqlDataType| ColumnInfo {
                name: n.to_string(),
                data_type: t,
                nullable: true,
                is_primary_key: false,
                default: None,
                raw_type: None,
                int_width: None,
                float_width: None,
            };
            Ok(Some(CollectionInfo {
                name: "items".to_string(),
                engine: EngineType::DocumentStrict,
                columns: vec![
                    col("id", SqlDataType::String),
                    col("score", SqlDataType::Int64),
                ],
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

    fn resolve(items: &str) -> Result<Vec<Projection>> {
        resolve_returning_items(items, "items", &ItemsCatalog)
    }

    #[test]
    fn a_bare_column_stays_a_column() {
        let projection = resolve("id, score").unwrap();
        assert_eq!(projection.len(), 2);
        assert!(matches!(&projection[0], Projection::Column(c) if c == "id"));
        assert!(matches!(&projection[1], Projection::Column(c) if c == "score"));
    }

    #[test]
    fn an_aliased_column_stays_computed_over_the_column() {
        let projection = resolve("id AS a").unwrap();
        assert_eq!(projection.len(), 1);
        match &projection[0] {
            Projection::Computed { expr, alias } => {
                assert_eq!(alias, "a");
                assert!(matches!(expr, SqlExpr::Column { name, .. } if name == "id"));
            }
            other => panic!("expected Computed, got {other:?}"),
        }
    }

    #[test]
    fn an_arithmetic_item_is_cp_computed() {
        let projection = resolve("score * 2 AS d").unwrap();
        assert_eq!(projection.len(), 1);
        match &projection[0] {
            Projection::CpComputed { expr, alias } => {
                assert_eq!(alias, "d");
                assert!(matches!(expr, SqlExpr::BinaryOp { .. }));
            }
            other => panic!("expected CpComputed, got {other:?}"),
        }
    }

    #[test]
    fn an_unaliased_expression_is_named_by_its_text() {
        let projection = resolve("score + 1").unwrap();
        match &projection[0] {
            Projection::CpComputed { alias, .. } => assert_eq!(alias, "score + 1"),
            other => panic!("expected CpComputed, got {other:?}"),
        }
    }

    #[test]
    fn a_sequence_accessor_is_cp_computed() {
        let projection = resolve("id, nextval('s') AS n").unwrap();
        assert_eq!(projection.len(), 2);
        match &projection[1] {
            Projection::CpComputed { expr, alias } => {
                assert_eq!(alias, "n");
                assert!(matches!(expr, SqlExpr::Function { name, .. } if name == "nextval"));
            }
            other => panic!("expected CpComputed, got {other:?}"),
        }
    }

    #[test]
    fn a_star_passes_through() {
        let projection = resolve("*").unwrap();
        assert_eq!(projection.len(), 1);
        assert!(matches!(projection[0], Projection::Star));
    }

    #[test]
    fn an_unknown_column_is_a_resolve_error() {
        let err = resolve("ghost").unwrap_err();
        assert!(
            matches!(err, SqlError::UnknownColumn { ref column, .. } if column == "ghost"),
            "expected UnknownColumn, got {err:?}"
        );
    }

    #[test]
    fn an_unknown_target_is_an_unknown_table() {
        let err = resolve_returning_items("id", "ghost", &ItemsCatalog).unwrap_err();
        assert!(matches!(err, SqlError::UnknownTable { ref name } if name == "ghost"));
    }

    #[test]
    fn a_trailing_clause_is_refused() {
        assert!(resolve("id FROM other").is_err());
        assert!(resolve("id WHERE score > 1").is_err());
        assert!(resolve("id ORDER BY id").is_err());
        assert!(resolve("id; SELECT 1").is_err());
        assert!(resolve("").is_err());
    }
}
