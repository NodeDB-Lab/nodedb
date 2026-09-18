// SPDX-License-Identifier: Apache-2.0

//! JOIN planning entry: builds left + right scans (or array-TVF arms),
//! attaches projection/filters/aggregation.

use sqlparser::ast::{self, Select};

use super::array_arm;
use super::constraint::extract_join_spec;
use crate::error::{Result, SqlError};
use crate::functions::registry::FunctionRegistry;
use crate::planner::lateral::plan::{LateralJoinArgs, plan_lateral_join};
use crate::planner::lateral::subquery::{
    is_lateral_derived, lateral_alias_from_factor, subquery_from_factor,
};
use crate::resolver::columns::TableScope;
use crate::types::*;

pub fn plan_join_from_select(
    select: &Select,
    scope: &TableScope,
    catalog: &dyn SqlCatalog,
    functions: &FunctionRegistry,
    temporal: crate::TemporalScope,
) -> Result<Option<SqlPlan>> {
    let from = &select.from[0];

    // Left side: either an ARRAY_* TVF or a named table.
    let left_plan =
        if let Some(plan) = array_arm::try_plan_relation(&from.relation, catalog, temporal)? {
            plan
        } else {
            scan_for_relation(&from.relation, scope)?
        };

    let outer_alias = scan_alias_from_relation(&from.relation)?;

    let mut current_plan = left_plan;

    for join_item in &from.joins {
        // Detect LATERAL derived subquery on the right side.
        if is_lateral_derived(&join_item.relation) {
            let lateral_alias =
                lateral_alias_from_factor(&join_item.relation)?.ok_or_else(|| {
                    SqlError::Unsupported {
                        detail: "LATERAL subquery requires an alias (e.g. LATERAL (...) AS x)"
                            .into(),
                    }
                })?;
            let subquery = subquery_from_factor(&join_item.relation)
                .expect("is_lateral_derived guarantees Derived variant");
            let left_join = is_left_join_operator(&join_item.join_operator);
            let projection = super::super::select::convert_projection(&select.projection, scope)?;
            return Ok(Some(plan_lateral_join(LateralJoinArgs {
                outer_plan: current_plan,
                outer_alias,
                subquery,
                lateral_alias: &lateral_alias,
                left_join,
                outer_projection: projection,
                outer_scope: scope,
                catalog,
                functions,
                temporal,
            })?));
        }

        // Right side: array TVF or named table.
        let right_plan = if let Some(plan) =
            array_arm::try_plan_relation(&join_item.relation, catalog, temporal)?
        {
            plan
        } else {
            scan_for_relation(&join_item.relation, scope)?
        };

        let (join_type, mut on_keys, condition) =
            extract_join_spec(&join_item.join_operator, scope)?;

        // Orient equi-keys to FROM order: `on.0` must reference the left input
        // and `on.1` the right input. The ON clause may write the operands in
        // either order (`ON right.k = left.k`); the physical join builds the
        // hash index on the right key, so a reversed pair would index the wrong
        // side and match zero rows.
        let right_ids = right_side_identifiers(&join_item.relation)?;
        super::constraint::orient_keys_to_sides(&mut on_keys, &right_ids);

        current_plan = SqlPlan::Join {
            left: Box::new(current_plan),
            right: Box::new(right_plan),
            on: on_keys,
            join_type,
            condition,
            limit: None,
            projection: Vec::new(),
            filters: Vec::new(),
        };
    }

    let (subquery_joins, effective_where) = if let Some(expr) = &select.selection {
        let extraction =
            super::super::subquery::extract_subqueries(expr, scope, catalog, functions, temporal)?;
        (extraction.joins, extraction.remaining_where)
    } else {
        (Vec::new(), None)
    };

    let projection = super::super::select::convert_projection(&select.projection, scope)?;
    let filters = match &effective_where {
        Some(expr) => super::super::select::convert_where_to_filters(expr, scope)?,
        None => Vec::new(),
    };

    for sq in subquery_joins {
        current_plan = SqlPlan::Join {
            left: Box::new(current_plan),
            right: Box::new(sq.inner_plan),
            on: sq.on,
            join_type: sq.join_type,
            condition: None,
            limit: None,
            projection: Vec::new(),
            filters: Vec::new(),
        };
    }

    let group_by_non_empty = match &select.group_by {
        ast::GroupByExpr::All(_) => true,
        ast::GroupByExpr::Expressions(exprs, _) => !exprs.is_empty(),
    };
    if group_by_non_empty {
        let aggregates = super::super::aggregate::extract_aggregates_from_projection(
            &select.projection,
            functions,
            scope,
        )?;
        let group_by = super::super::group_by::convert_group_by(&select.group_by, scope)?;
        let group_by_aliases =
            super::super::group_by::group_by_output_aliases(&select.projection, &group_by);
        let output_order = super::super::aggregate_order::compute_output_order(
            &select.projection,
            &group_by,
            functions,
            scope,
        )?;
        let having = match &select.having {
            Some(expr) => {
                // A HAVING term addresses a computed group column, so the
                // aggregate output names join the input columns here.
                let having_scope =
                    scope.with_output_names(aggregates.iter().map(|a| a.alias.clone()).chain(
                        super::super::select::select_output_aliases(&select.projection),
                    ));
                super::super::select::convert_where_to_filters(expr, &having_scope)?
            }
            None => Vec::new(),
        };
        return Ok(Some(SqlPlan::Aggregate {
            input: Box::new(current_plan),
            group_by,
            group_by_aliases,
            output_order,
            aggregates,
            having,
            limit: 10000,
            grouping_sets: None,
            sort_keys: Vec::new(),
        }));
    }

    if let SqlPlan::Join {
        projection: ref mut proj,
        filters: ref mut filt,
        ..
    } = current_plan
    {
        *proj = projection;
        *filt = filters;
    }
    Ok(Some(current_plan))
}

/// Collect the normalized identifiers (alias and/or table name) by which a
/// right-side join relation can be referenced in an ON clause. Used to orient
/// equi-keys so the right-side operand is always `on.1`.
fn right_side_identifiers(factor: &ast::TableFactor) -> Result<Vec<String>> {
    let mut ids = Vec::new();
    if let Some((name, alias)) = crate::parser::normalize::table_name_from_factor(factor)? {
        if let Some(alias) = alias {
            ids.push(alias);
        }
        ids.push(name);
    }
    Ok(ids)
}

/// Extract an alias (or table name) from a named-table `TableFactor`.
fn scan_alias_from_relation(factor: &ast::TableFactor) -> Result<Option<String>> {
    crate::parser::normalize::table_name_from_factor(factor)
        .map(|relation| relation.map(|(name, alias)| alias.unwrap_or(name)))
}

/// True when the join operator represents a LEFT join variant.
fn is_left_join_operator(op: &ast::JoinOperator) -> bool {
    matches!(
        op,
        ast::JoinOperator::Left(_) | ast::JoinOperator::LeftOuter(_)
    )
}

/// Build a `SqlPlan::Scan` for a named-table TableFactor.
fn scan_for_relation(rel: &ast::TableFactor, scope: &TableScope) -> Result<SqlPlan> {
    let (rel_name, rel_alias) =
        crate::parser::normalize::table_name_from_factor(rel)?.ok_or_else(|| {
            SqlError::Unsupported {
                detail: "non-table JOIN target".into(),
            }
        })?;
    let table = scope
        .tables
        .values()
        .find(|t| t.name == rel_name || t.alias.as_deref() == Some(&rel_name))
        .ok_or_else(|| SqlError::UnknownTable {
            name: rel_name.clone(),
        })?;
    Ok(SqlPlan::Scan {
        collection: table.name.clone(),
        alias: rel_alias.or_else(|| table.alias.clone()),
        engine: table.info.engine,
        filters: Vec::new(),
        projection: Vec::new(),
        sort_keys: Vec::new(),
        limit: None,
        offset: 0,
        distinct: false,
        window_functions: Vec::new(),
        temporal: crate::temporal::TemporalScope::default(),
    })
}
