// SPDX-License-Identifier: Apache-2.0

//! Top-level query entry: CTE handling and UNION dispatch. ORDER BY and
//! search-trigger detection live in `order_by.rs`; LIMIT / OFFSET application
//! lives in `limit.rs`.

use sqlparser::ast::{Query, SetExpr};

use crate::error::{Result, SqlError};
use crate::functions::registry::FunctionRegistry;
use crate::planner::select::cte_catalog::CteCatalog;
use crate::reserved::check_ast_identifier;
use crate::resolver::derived::{infer_subquery_relation, rename_output_columns};
use crate::temporal::TemporalScope;
use crate::types::{CtePlan, SqlCatalog, SqlPlan};

/// Plan a SELECT query that produces the statement's result rows.
///
/// Only this SELECT list may hold a Control-Plane-computed item (a sequence
/// accessor over a relation): the Control Plane evaluates the statement's
/// output rows and nothing deeper.
pub fn plan_statement_query(
    query: &Query,
    catalog: &dyn SqlCatalog,
    functions: &FunctionRegistry,
    temporal: TemporalScope,
) -> Result<SqlPlan> {
    plan_query_at(query, catalog, functions, temporal, true)
}

/// Plan a nested SELECT query: a subquery, CTE body, UNION branch, derived
/// table, INSERT source, or MERGE source. Its SELECT list refuses a
/// sequence accessor over a relation.
pub fn plan_query(
    query: &Query,
    catalog: &dyn SqlCatalog,
    functions: &FunctionRegistry,
    temporal: TemporalScope,
) -> Result<SqlPlan> {
    plan_query_at(query, catalog, functions, temporal, false)
}

/// Plan a SELECT query. `statement_output` says whether its rows are the
/// statement's result; a WITH clause passes it to the outer query only.
fn plan_query_at(
    query: &Query,
    catalog: &dyn SqlCatalog,
    functions: &FunctionRegistry,
    temporal: TemporalScope,
    statement_output: bool,
) -> Result<SqlPlan> {
    // Handle CTEs (WITH clause).
    if let Some(with) = &query.with
        && with.recursive
    {
        return crate::planner::cte::plan_recursive_cte(query, catalog, functions, temporal);
    }
    // Non-recursive CTEs: plan each CTE subquery and the outer query.
    if let Some(with) = &query.with
        && !with.cte_tables.is_empty()
    {
        let inner_query = Query {
            with: None,
            body: query.body.clone(),
            order_by: query.order_by.clone(),
            limit_clause: query.limit_clause.clone(),
            fetch: query.fetch.clone(),
            locks: query.locks.clone(),
            for_clause: query.for_clause.clone(),
            settings: query.settings.clone(),
            format_clause: query.format_clause.clone(),
            pipe_operators: query.pipe_operators.clone(),
        };

        // Plan each CTE subquery and infer the relation it exposes.
        let mut definitions = Vec::new();
        let mut relations = Vec::new();
        for cte in &with.cte_tables {
            let name = check_ast_identifier(&cte.alias.name)?;
            let declared: Vec<String> = cte
                .alias
                .columns
                .iter()
                .map(|column| check_ast_identifier(&column.name))
                .collect::<Result<_>>()?;
            let cte_plan = plan_query(&cte.query, catalog, functions, temporal)?;
            let info = infer_subquery_relation(
                catalog,
                &name,
                &cte.query,
                Some(&cte_plan),
                functions,
                temporal,
            )?;
            definitions.push((name.clone(), cte_plan));
            relations.push((name, rename_output_columns(info, &declared)));
        }

        // Build CTE-aware catalog so the outer query can reference CTE names.
        let cte_catalog = CteCatalog {
            inner: catalog,
            relations,
        };
        let outer = plan_query_at(
            &inner_query,
            &cte_catalog,
            functions,
            temporal,
            statement_output,
        )?;

        return Ok(SqlPlan::Cte(CtePlan {
            definitions,
            outer: Box::new(outer),
        }));
    }

    // Handle UNION.
    match &*query.body {
        SetExpr::Select(select) => super::search::plan_select_query(
            query,
            select,
            catalog,
            functions,
            temporal,
            statement_output,
        ),
        SetExpr::SetOperation {
            op,
            left,
            right,
            set_quantifier,
        } => crate::planner::union::plan_set_operation(
            op,
            left,
            right,
            set_quantifier,
            catalog,
            functions,
            temporal,
        ),
        _ => Err(SqlError::Unsupported {
            detail: format!("query body type: {}", query.body),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::super::fixtures::plan_select_sql;
    use crate::types::*;
    #[test]
    fn aggregate_subquery_join_filters_input_before_aggregation() {
        let plan = plan_select_sql(
            "SELECT AVG(price) FROM products WHERE category IN (SELECT DISTINCT category FROM products WHERE qty > 100)",
        );

        let SqlPlan::Aggregate { input, .. } = plan else {
            panic!("expected aggregate plan");
        };

        let SqlPlan::Join {
            left,
            join_type,
            on,
            ..
        } = *input
        else {
            panic!("expected semi-join below aggregate");
        };

        assert_eq!(join_type, JoinType::Semi);
        assert_eq!(on, vec![("category".into(), "category".into())]);
        assert!(matches!(*left, SqlPlan::Scan { .. }));
    }

    #[test]
    fn scalar_subquery_defers_projection_until_after_join_filter() {
        let plan = plan_select_sql(
            "SELECT user_id FROM orders WHERE amount > (SELECT AVG(amount) FROM orders)",
        );

        let SqlPlan::Join {
            left,
            projection,
            filters,
            ..
        } = plan
        else {
            panic!("expected join plan");
        };

        let SqlPlan::Scan {
            projection: scan_projection,
            ..
        } = *left
        else {
            panic!("expected scan on join left");
        };

        assert!(scan_projection.is_empty(), "scan projected too early");
        assert_eq!(projection.len(), 1);
        match &projection[0] {
            Projection::Column(name) => assert_eq!(name, "user_id"),
            other => panic!("expected user_id projection, got {other:?}"),
        }
        assert!(
            !filters.is_empty(),
            "scalar comparison should stay post-join"
        );
    }

    #[test]
    fn chained_join_preserves_qualified_on_keys() {
        let plan = plan_select_sql(
            "SELECT d.name, t.tag, p.theme \
             FROM docs d \
             LEFT JOIN tags t ON d.id = t.doc_id \
             INNER JOIN user_prefs p ON d.id = p.key",
        );

        let SqlPlan::Join { left, on, .. } = plan else {
            panic!("expected outer join plan");
        };
        assert_eq!(on, vec![("d.id".into(), "p.key".into())]);

        let SqlPlan::Join { on: inner_on, .. } = *left else {
            panic!("expected nested left join");
        };
        assert_eq!(inner_on, vec![("d.id".into(), "t.doc_id".into())]);
    }
}
