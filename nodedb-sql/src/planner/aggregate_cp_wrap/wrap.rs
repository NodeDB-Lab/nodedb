// SPDX-License-Identifier: Apache-2.0

//! Wrapping a grouped plan so a Control-Plane-computed SELECT-list item keeps
//! its position.
//!
//! An `Aggregate` plan emits group keys and aggregate values only. A SELECT
//! item that calls a sequence accessor is neither, so the aggregate cannot
//! carry it. The planner wraps the finished aggregate in a `Subquery` whose
//! projection restates every output column in SELECT-list order and places a
//! [`crate::types::Projection::CpComputed`] entry at the item's position. The wrap runs
//! after ORDER BY and LIMIT are attached, so those stay on the aggregate.

use crate::types::CtePlan;
use sqlparser::ast;

use super::projection::aggregate_cp_projection;
use crate::error::{Result, SqlError};
use crate::functions::registry::FunctionRegistry;
use crate::planner::cp_projection::ast_sequence_accessor;
use crate::resolver::columns::TableScope;
use crate::types::SqlPlan;

/// Wrap `plan` when `items` hold a sequence accessor the aggregate cannot
/// carry. A plan with no such item, or one that is not grouped, returns
/// unchanged: `convert_projection` marks the item on a row plan itself.
///
/// `grouped` says whether the SELECT aggregates (GROUP BY, or an aggregate
/// call in its list). `scope` is the relation set the SELECT resolved
/// against. A grouped SELECT that does not produce the statement's output
/// rows (a subquery, CTE body, UNION branch, derived table) refuses the
/// item: the aggregate planner never converts a bare non-key item, so
/// nothing else reports it.
pub fn wrap_aggregate_cp_items(
    plan: SqlPlan,
    items: &[ast::SelectItem],
    grouped: bool,
    functions: &FunctionRegistry,
    scope: &TableScope,
) -> Result<SqlPlan> {
    if !grouped || !scope.is_row_scope() {
        return Ok(plan);
    }
    let Some(accessor) = items.iter().find_map(item_accessor) else {
        return Ok(plan);
    };
    if !scope.is_statement_output() {
        return Err(SqlError::SequencePerRowUnsupported { name: accessor });
    }
    match plan {
        SqlPlan::Cte(CtePlan { definitions, outer }) => Ok(SqlPlan::Cte(CtePlan {
            definitions,
            outer: Box::new(wrap_aggregate_cp_items(
                *outer, items, grouped, functions, scope,
            )?),
        })),
        SqlPlan::Aggregate { .. } => {
            let projection = aggregate_cp_projection(&plan, items, functions, scope)?;
            Ok(SqlPlan::Subquery {
                input: Box::new(plan),
                filters: Vec::new(),
                projection,
                window_functions: Vec::new(),
                sort_keys: Vec::new(),
                offset: 0,
                distinct: false,
                limit: None,
            })
        }
        // An OFFSET the aggregate cannot hold already sits in a
        // post-processing tail with no projection of its own. The tail
        // projects after it offsets and sorts, so the restated columns land
        // on it instead of a second wrapper.
        SqlPlan::Subquery {
            input,
            filters,
            projection,
            window_functions,
            sort_keys,
            offset,
            distinct,
            limit,
        } if matches!(*input, SqlPlan::Aggregate { .. }) && projection.is_empty() => {
            let projection = aggregate_cp_projection(&input, items, functions, scope)?;
            Ok(SqlPlan::Subquery {
                input,
                filters,
                projection,
                window_functions,
                sort_keys,
                offset,
                distinct,
                limit,
            })
        }
        // A grouped query the engine rules planned as another shape (a
        // timeseries bucket scan, a grouped query joined to a scalar
        // subquery) names its output columns by rules this wrap does not
        // restate. Refusing beats dropping the item from the result.
        other => Err(SqlError::Unsupported {
            detail: format!(
                "a sequence accessor in the SELECT list of a grouped query planned as {}; \
                 call it in a FROM-less SELECT instead",
                other.variant_name()
            ),
        }),
    }
}

/// The sequence accessor a SELECT item calls, when it calls one.
fn item_accessor(item: &ast::SelectItem) -> Option<String> {
    match item {
        ast::SelectItem::UnnamedExpr(expr) | ast::SelectItem::ExprWithAlias { expr, .. } => {
            ast_sequence_accessor(expr)
        }
        ast::SelectItem::ExprWithAliases { .. }
        | ast::SelectItem::Wildcard(_)
        | ast::SelectItem::QualifiedWildcard(..) => None,
    }
}

#[cfg(test)]
mod tests {
    use crate::types::{
        CollectionInfo, EngineType, PlanCacheEligibility, Projection, SqlCatalog, SqlCatalogError,
        SqlExpr, SqlPlan,
    };
    use crate::{SqlError, plan_sql};

    struct Catalog;

    impl SqlCatalog for Catalog {
        fn get_collection(
            &self,
            _: nodedb_types::DatabaseId,
            name: &str,
        ) -> std::result::Result<Option<CollectionInfo>, SqlCatalogError> {
            if name != "t" {
                return Ok(None);
            }
            Ok(Some(CollectionInfo {
                name: "t".into(),
                engine: EngineType::DocumentSchemaless,
                columns: Vec::new(),
                primary_key: Some("id".into()),
                has_auto_tier: false,
                indexes: Vec::new(),
                bitemporal: false,
                primary: nodedb_types::PrimaryEngine::Document,
                vector_primary: None,
                partition_strategy: nodedb_types::PartitionStrategy::CollectionHomed,
                open_schema: CollectionInfo::open_schema_for(EngineType::DocumentSchemaless),
            }))
        }
    }

    fn plan(sql: &str) -> SqlPlan {
        plan_sql(sql, &Catalog)
            .unwrap_or_else(|e| panic!("{sql}: {e}"))
            .remove(0)
    }

    fn plan_err(sql: &str) -> SqlError {
        match plan_sql(sql, &Catalog) {
            Ok(plans) => panic!("{sql} must not plan, got {plans:?}"),
            Err(e) => e,
        }
    }

    fn is_nextval(expr: &SqlExpr) -> bool {
        matches!(expr, SqlExpr::Function { name, .. } if name == "nextval")
    }

    #[test]
    fn a_scan_marks_the_accessor_item_as_control_plane_computed() {
        let plan = plan("SELECT id, nextval('s') FROM t");
        let SqlPlan::Scan { projection, .. } = &plan else {
            panic!("expected Scan, got {plan:?}");
        };
        assert_eq!(projection.len(), 2);
        assert!(matches!(&projection[0], Projection::Column(name) if name == "id"));
        match &projection[1] {
            Projection::CpComputed { expr, alias } => {
                assert_eq!(alias, "nextval('s')");
                assert!(is_nextval(expr));
            }
            other => panic!("expected CpComputed, got {other:?}"),
        }
        assert_eq!(
            plan.cache_eligibility(),
            PlanCacheEligibility::DataDependent
        );
    }

    #[test]
    fn a_wider_aliased_expression_keeps_its_alias() {
        let plan = plan("SELECT nextval('s') * 2 AS n FROM t");
        let SqlPlan::Scan { projection, .. } = &plan else {
            panic!("expected Scan, got {plan:?}");
        };
        match &projection[0] {
            Projection::CpComputed { expr, alias } => {
                assert_eq!(alias, "n");
                assert!(matches!(expr, SqlExpr::BinaryOp { .. }));
            }
            other => panic!("expected CpComputed, got {other:?}"),
        }
    }

    #[test]
    fn every_other_row_clause_refuses_the_accessor() {
        for sql in [
            "SELECT id FROM t WHERE nextval('s') > 0",
            "SELECT id FROM t ORDER BY nextval('s')",
            "SELECT SUM(nextval('s')) FROM t",
            "SELECT grp, COUNT(*) + nextval('s') FROM t GROUP BY grp",
            "SELECT grp, COUNT(*) FROM t GROUP BY grp HAVING COUNT(*) > nextval('s')",
            "SELECT n FROM (SELECT id, nextval('s') AS n FROM t) d",
            "SELECT n FROM (SELECT grp, COUNT(*), nextval('s') AS n FROM t GROUP BY grp) d",
            "SELECT id FROM t UNION ALL SELECT nextval('s') FROM t",
            "WITH c AS (SELECT nextval('s') AS n FROM t) SELECT n FROM c",
            "INSERT INTO t (id, n) SELECT id, nextval('s') FROM t",
        ] {
            let err = plan_err(sql);
            assert!(
                matches!(err, SqlError::SequencePerRowUnsupported { .. }),
                "{sql}: expected SequencePerRowUnsupported, got {err:?}"
            );
        }
    }

    #[test]
    fn a_grouped_query_wraps_the_aggregate_in_select_list_order() {
        let plan = plan("SELECT grp, COUNT(*), nextval('s') FROM t GROUP BY grp");
        let SqlPlan::Subquery {
            input,
            projection,
            filters,
            sort_keys,
            limit,
            ..
        } = &plan
        else {
            panic!("expected Subquery, got {plan:?}");
        };
        assert!(matches!(**input, SqlPlan::Aggregate { .. }));
        assert!(filters.is_empty());
        assert!(sort_keys.is_empty());
        assert_eq!(*limit, None);
        assert_eq!(projection.len(), 3);
        assert!(matches!(&projection[0], Projection::Column(name) if name == "grp"));
        assert!(matches!(&projection[1], Projection::Column(name) if name == "count(*)"));
        assert!(matches!(&projection[2], Projection::CpComputed { expr, .. } if is_nextval(expr)));
        assert_eq!(
            plan.cache_eligibility(),
            PlanCacheEligibility::DataDependent
        );
    }

    #[test]
    fn order_by_and_limit_stay_on_the_wrapped_aggregate() {
        let plan = plan(
            "SELECT t.grp, nextval('s') + t.grp AS n, COUNT(*) AS c FROM t \
             GROUP BY t.grp ORDER BY c DESC LIMIT 5",
        );
        let SqlPlan::Subquery {
            input, projection, ..
        } = &plan
        else {
            panic!("expected Subquery, got {plan:?}");
        };
        let SqlPlan::Aggregate {
            sort_keys, limit, ..
        } = &**input
        else {
            panic!("expected Aggregate, got {input:?}");
        };
        assert_eq!(sort_keys.len(), 1);
        assert_eq!(*limit, 5);
        assert!(matches!(&projection[0], Projection::Column(name) if name == "grp"));
        match &projection[1] {
            Projection::CpComputed { expr, alias } => {
                assert_eq!(alias, "n");
                let SqlExpr::BinaryOp { right, .. } = expr else {
                    panic!("expected a binary expression, got {expr:?}");
                };
                assert!(
                    matches!(&**right, SqlExpr::Column { table: None, name } if name == "grp"),
                    "the key reference must lose its qualifier, got {right:?}"
                );
            }
            other => panic!("expected CpComputed, got {other:?}"),
        }
        assert!(matches!(&projection[2], Projection::Column(name) if name == "c"));
    }

    #[test]
    fn an_offset_tail_carries_the_restated_projection_itself() {
        let plan = plan("SELECT grp, nextval('s'), COUNT(*) FROM t GROUP BY grp OFFSET 2");
        let SqlPlan::Subquery {
            input,
            projection,
            offset,
            ..
        } = &plan
        else {
            panic!("expected Subquery, got {plan:?}");
        };
        assert!(matches!(**input, SqlPlan::Aggregate { .. }));
        assert_eq!(*offset, 2);
        assert_eq!(projection.len(), 3);
        assert!(matches!(&projection[1], Projection::CpComputed { .. }));
    }

    #[test]
    fn a_grouped_accessor_item_may_reference_group_keys_only() {
        let err = plan_err("SELECT grp, COUNT(*), nextval('s') + other FROM t GROUP BY grp");
        assert!(
            matches!(err, SqlError::Unsupported { ref detail } if detail.contains("'other'")),
            "got {err:?}"
        );
    }

    #[test]
    fn a_from_less_select_still_folds_the_accessor_at_plan_time() {
        let plan = plan("SELECT 1 AS one");
        assert!(matches!(plan, SqlPlan::ConstantResult { .. }));
        let err = plan_err("SELECT nextval('s')");
        assert!(
            !matches!(err, SqlError::SequencePerRowUnsupported { .. }),
            "a FROM-less accessor reaches the catalog, not the per-row gate: {err:?}"
        );
    }
}
