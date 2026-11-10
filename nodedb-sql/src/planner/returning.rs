// SPDX-License-Identifier: Apache-2.0

//! Resolve each DML output list and compile its native response projection.

use sqlparser::ast;

use nodedb_types::DatabaseId;

use crate::error::{Result, SqlError};
use crate::parser::statement::parse_sql;
use crate::planner::cp_projection::to_cp_computed;
use crate::planner::select::helpers::convert_projection;
use crate::resolver::columns::{ResolvedTable, TableScope};
use crate::types::{Projection, SqlCatalog};

/// Resolve an output list against its write target.
pub fn resolve_returning_items(
    items_sql: &str,
    target: &str,
    catalog: &dyn SqlCatalog,
) -> Result<Vec<Projection>> {
    let items = parse_items(items_sql)?;
    resolve_returning_ast(&items, target, None, catalog)
}

/// Resolve AST items while preserving the statement's target alias.
pub fn resolve_returning_ast(
    items: &[ast::SelectItem],
    target: &str,
    alias: Option<&str>,
    catalog: &dyn SqlCatalog,
) -> Result<Vec<Projection>> {
    let info = catalog
        .get_collection(DatabaseId::DEFAULT, target)?
        .ok_or_else(|| SqlError::UnknownTable {
            name: target.to_string(),
        })?;
    let scope = TableScope::single(ResolvedTable {
        name: target.to_string(),
        alias: alias.map(str::to_owned),
        info,
    })?
    .as_statement_output()
    .allowing_cp_functions();
    let projection = convert_projection(items, &scope)?;
    let mut projection: Vec<_> = projection.into_iter().map(to_cp_computed).collect();
    super::catalog_plan_shapes::validate_projection(&projection, catalog, DatabaseId::DEFAULT, 0)?;
    super::catalog_plan_shapes::fold_projection(&mut projection, catalog, DatabaseId::DEFAULT, 0);
    Ok(projection)
}

/// Attach one statement's resolved clause to its routed write plans.
pub(crate) fn attach_returning(
    mut plans: Vec<crate::types::SqlPlan>,
    items: Option<&[ast::SelectItem]>,
    alias: Option<&str>,
    catalog: &dyn SqlCatalog,
) -> Result<Vec<crate::types::SqlPlan>> {
    let Some(items) = items else { return Ok(plans) };
    for plan in &mut plans {
        let target = plan
            .returning_target()
            .ok_or_else(|| SqlError::Unsupported {
                detail: format!("RETURNING is not supported on {}", plan.variant_name()),
            })?;
        let projection = resolve_returning_ast(items, target, alias, catalog)?;
        let spec = returning_spec(&projection)?;
        if let Some(slot) = plan.returning_slot_mut() {
            *slot = Some(crate::types::ResolvedReturning { projection, spec });
        }
    }
    Ok(plans)
}

/// Compile ordered fields and expressions for precommit response evaluation.
pub fn returning_spec(projection: &[Projection]) -> Result<nodedb_types::ReturningSpec> {
    use nodedb_types::{ReturningColumns, ReturningItem, ReturningSpec};
    if matches!(
        projection,
        [Projection::Star] | [Projection::QualifiedStar(_)]
    ) {
        return Ok(ReturningSpec {
            columns: ReturningColumns::Star,
            precommit: None,
        });
    }
    let mut items = Vec::with_capacity(projection.len());
    for item in projection {
        match item {
            Projection::Column(name) => items.push(ReturningItem::Column {
                name: name.rsplit('.').next().unwrap_or(name).to_owned(),
                alias: None,
            }),
            Projection::Computed {
                expr: crate::types::SqlExpr::Column { name, .. },
                alias,
            } => {
                items.push(ReturningItem::Column {
                    name: name.clone(),
                    alias: Some(alias.clone()),
                });
            }
            Projection::Computed { expr, alias } | Projection::CpComputed { expr, alias } => {
                let expression = crate::query_expr::convert_sql_expr(expr)?;
                let expression = zerompk::to_msgpack_vec(&expression).map_err(|error| {
                    SqlError::Unsupported {
                        detail: format!("encode RETURNING expression '{alias}': {error}"),
                    }
                })?;
                items.push(ReturningItem::Computed {
                    expression,
                    name: alias.clone(),
                });
            }
            Projection::Star | Projection::QualifiedStar(_) => items.push(ReturningItem::Star),
        }
    }
    Ok(ReturningSpec {
        columns: ReturningColumns::Named(items),
        precommit: None,
    })
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::SqlCatalogError;
    use crate::types::{CollectionInfo, ColumnInfo, EngineType, SqlDataType, SqlExpr};

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
    #[test]
    fn each_statement_retains_its_own_response_clause() {
        let plans = crate::plan_sql(
            "INSERT INTO items (id, score) VALUES ('a', 3) RETURNING id AS inserted; DELETE FROM items WHERE id = 'a' RETURNING score AS removed",
            &ItemsCatalog,
        ).expect("plan statements");
        assert_eq!(plans.len(), 2);
        assert_eq!(
            plans[0]
                .returning()
                .expect("insert clause")
                .spec
                .output_names(),
            Some(vec!["inserted".into()])
        );
        assert_eq!(
            plans[1]
                .returning()
                .expect("delete clause")
                .spec
                .output_names(),
            Some(vec!["removed".into()])
        );
    }

    #[test]
    fn target_alias_and_computed_labels_reach_native_projection() {
        let plans = crate::plan_sql(
            "UPDATE items AS target SET score = 4 RETURNING target.id AS renamed, target.score * 2 AS doubled",
            &ItemsCatalog,
        ).expect("plan update");
        let clause = plans[0].returning().expect("response clause");
        assert_eq!(
            clause.spec.output_names(),
            Some(vec!["renamed".into(), "doubled".into()])
        );
        let nodedb_types::ReturningColumns::Named(items) = &clause.spec.columns else {
            panic!("named output")
        };
        let nodedb_types::ReturningItem::Computed { expression, .. } = &items[1] else {
            panic!("computed output")
        };
        let expr: nodedb_query::expr::SqlExpr =
            zerompk::from_msgpack(expression).expect("decode expression");
        let row = nodedb_types::Value::Object(std::collections::HashMap::from([(
            "score".into(),
            nodedb_types::Value::Integer(4),
        )]));
        assert_eq!(
            expr.eval(&row, &nodedb_crs::CrsContext::builtin_only())
                .expect("evaluate"),
            nodedb_types::Value::Integer(8)
        );
    }

    #[test]
    fn mixed_star_keeps_projection_order() {
        let projection = resolve("score AS first, *, id AS last").expect("resolve");
        let spec = returning_spec(&projection).expect("compile");
        let nodedb_types::ReturningColumns::Named(items) = spec.columns else {
            panic!("ordered output")
        };
        assert_eq!(items[0].output_name(), "first");
        assert!(matches!(items[1], nodedb_types::ReturningItem::Star));
        assert_eq!(items[2].output_name(), "last");
    }
}
