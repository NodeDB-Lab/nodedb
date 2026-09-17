// SPDX-License-Identifier: Apache-2.0

//! Derived FROM subquery: `FROM (SELECT ...) AS t`.

use sqlparser::ast::{self, Select};

use super::cte_catalog::CteCatalog;
use super::entry::plan_query;
use super::query_tail::QueryTail;
use super::select_stmt::{PlannedSelect, plan_select};
use crate::error::Result;
use crate::functions::registry::FunctionRegistry;
use crate::temporal::TemporalScope;
use crate::types::*;

/// Desugar `FROM (SELECT ...) AS alias` into a synthetic single-CTE plan.
///
/// Recognises the single-source, non-LATERAL derived-table pattern. The
/// inner subquery is planned with the original catalog; the outer
/// SELECT is replanned with a `CteCatalog` that resolves the alias to the
/// relation the subquery projects. The result is wrapped as `SqlPlan::Cte`
/// so the `convert_cte` lowering takes care of execution.
///
/// Returns `Ok(None)` when the FROM clause is not a single derived
/// table, so the caller falls through to the regular planning path.
pub(in crate::planner::select) fn try_plan_derived_from(
    select: &Select,
    catalog: &dyn SqlCatalog,
    functions: &FunctionRegistry,
    temporal: TemporalScope,
    tail: &QueryTail<'_>,
    statement_output: bool,
) -> Result<Option<PlannedSelect>> {
    if select.from.len() != 1 {
        return Ok(None);
    }
    let from = &select.from[0];
    if !from.joins.is_empty() {
        return Ok(None);
    }
    let (subquery, alias_ident) = match &from.relation {
        ast::TableFactor::Derived {
            lateral: false,
            subquery,
            alias: Some(alias),
            ..
        } => (subquery, alias),
        _ => return Ok(None),
    };

    let alias_name = crate::reserved::check_ast_identifier(&alias_ident.name)?;
    let declared: Vec<String> = alias_ident
        .columns
        .iter()
        .map(|column| crate::reserved::check_ast_identifier(&column.name))
        .collect::<Result<_>>()?;
    let inner_plan = plan_query(subquery, catalog, functions, temporal)?;

    // Replan the outer SELECT against a catalog that resolves the alias to
    // the columns the subquery projects. The outer can reference `alias.col`
    // qualified or unqualified.
    let relation =
        crate::resolver::derived::infer_subquery_relation(catalog, &alias_name, subquery)?;
    let derived_catalog = CteCatalog {
        inner: catalog,
        relations: vec![(
            alias_name.clone(),
            crate::resolver::derived::rename_output_columns(relation, &declared),
        )],
    };
    let mut outer_select = select.clone();
    outer_select.from[0].relation = ast::TableFactor::Table {
        name: ast::ObjectName(vec![ast::ObjectNamePart::Identifier(
            alias_ident.name.clone(),
        )]),
        alias: None,
        args: None,
        with_hints: Vec::new(),
        version: None,
        with_ordinality: false,
        partitions: Vec::new(),
        json_path: None,
        sample: None,
        index_hints: Vec::new(),
    };
    // The outer SELECT keeps the caller's output standing; the derived body
    // planned above is nested.
    let outer = plan_select(
        &outer_select,
        &derived_catalog,
        functions,
        temporal,
        tail,
        statement_output,
    )?;

    Ok(Some(PlannedSelect {
        plan: SqlPlan::Cte {
            definitions: vec![(alias_name, inner_plan)],
            outer: Box::new(outer.plan),
        },
        scope: outer.scope,
    }))
}
