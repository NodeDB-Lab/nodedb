// SPDX-License-Identifier: Apache-2.0

//! JOIN planning entry: builds left + right scans (or array-TVF arms),
//! attaches projection/filters/aggregation.

use sqlparser::ast::{self, Select};

use super::array_arm;
use super::constraint::extract_join_spec;
use super::derived_side::plan_derived_side;
use crate::error::{Result, SqlError};
use crate::functions::registry::FunctionRegistry;
use crate::planner::lateral::plan::{LateralJoinArgs, plan_lateral_join};
use crate::planner::lateral::subquery::{
    is_lateral_derived, lateral_alias_from_factor, lateral_subquery,
};
use crate::resolver::columns::{ColumnNaming, TableScope};
use crate::types::*;

pub fn plan_join_from_select(
    select: &Select,
    scope: &TableScope,
    catalog: &dyn SqlCatalog,
    functions: &FunctionRegistry,
    temporal: crate::TemporalScope,
) -> Result<Option<SqlPlan>> {
    let from = &select.from[0];

    // Every clause below reads the join's merged rows, which key each field
    // `qualifier.column`, so a column reference names its owning relation's
    // qualifier. A LATERAL body reads its outer row through its own
    // correlation, so a join with a LATERAL item keeps references as written.
    let has_lateral = from
        .joins
        .iter()
        .any(|join_item| is_lateral_derived(&join_item.relation));
    let join_row_scope;
    let scope = if has_lateral {
        scope
    } else {
        join_row_scope = scope.clone().with_column_naming(ColumnNaming::JoinRow);
        &join_row_scope
    };

    // Left side: an ARRAY_* TVF, a derived table, or a named table.
    let left_plan =
        if let Some(plan) = array_arm::try_plan_relation(&from.relation, catalog, temporal)? {
            plan
        } else if let Some(plan) =
            plan_derived_side(&from.relation, scope, catalog, functions, temporal)?
        {
            plan
        } else {
            scan_for_relation(&from.relation, scope, temporal)?
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
            let subquery = lateral_subquery(&join_item.relation)?;
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

        // Right side: array TVF, derived table, or named table.
        let right_plan = if let Some(plan) =
            array_arm::try_plan_relation(&join_item.relation, catalog, temporal)?
        {
            plan
        } else if let Some(plan) =
            plan_derived_side(&join_item.relation, scope, catalog, functions, temporal)?
        {
            plan
        } else {
            scan_for_relation(&join_item.relation, scope, temporal)?
        };

        let right_ids = right_side_identifiers(&join_item.relation)?;
        let (join_type, mut on_keys, condition) = extract_join_spec(
            &join_item.join_operator,
            scope,
            right_ids.first().map(String::as_str),
        )?;

        // Orient equi-keys to FROM order: `on.0` must reference the left input
        // and `on.1` the right input. The ON clause may write the operands in
        // either order (`ON right.k = left.k`); the physical join builds the
        // hash index on the right key, so a reversed pair would index the wrong
        // side and match zero rows.
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

    if super::super::select::has_aggregation(select, functions) {
        // The WHERE predicates filter the joined rows before grouping.
        if let SqlPlan::Join {
            filters: ref mut filt,
            ..
        } = current_plan
        {
            *filt = filters;
        }
        // A join aggregate is always the generic aggregate, so ORDER BY
        // need not be inspected for aggregates of its own.
        let clauses = super::super::aggregate_clauses::plan_aggregate_clauses(
            select,
            None,
            &[],
            scope,
            functions,
        )?;
        return Ok(Some(SqlPlan::Aggregate {
            input: Box::new(current_plan),
            group_by: clauses.group_by,
            group_by_aliases: clauses.group_by_aliases,
            output_order: clauses.output_order,
            aggregates: clauses.aggregates,
            having: clauses.having,
            limit: super::super::aggregate::AGGREGATE_NO_LIMIT,
            grouping_sets: clauses.grouping_sets,
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
    } else if let Some(alias) = derived_alias(factor)? {
        ids.push(alias);
    }
    Ok(ids)
}

/// Extract an alias (or table name) from a named-table `TableFactor`, or the
/// alias of a derived table.
fn scan_alias_from_relation(factor: &ast::TableFactor) -> Result<Option<String>> {
    match crate::parser::normalize::table_name_from_factor(factor)? {
        Some((name, alias)) => Ok(Some(alias.unwrap_or(name))),
        None => derived_alias(factor),
    }
}

/// The alias of a derived-table `TableFactor`, `None` for any other factor.
fn derived_alias(factor: &ast::TableFactor) -> Result<Option<String>> {
    match factor {
        ast::TableFactor::Derived {
            alias: Some(alias), ..
        } => crate::reserved::check_ast_identifier(&alias.name).map(Some),
        _ => Ok(None),
    }
}

/// True when the join operator represents a LEFT join variant.
fn is_left_join_operator(op: &ast::JoinOperator) -> bool {
    matches!(
        op,
        ast::JoinOperator::Left(_) | ast::JoinOperator::LeftOuter(_)
    )
}

/// Build a `SqlPlan::Scan` for a named-table TableFactor.
///
/// The statement's `FOR SYSTEM_TIME` / `FOR VALID_TIME` scope applies to
/// every joined relation, so each side must be bitemporal.
fn scan_for_relation(
    rel: &ast::TableFactor,
    scope: &TableScope,
    temporal: crate::TemporalScope,
) -> Result<SqlPlan> {
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
    temporal.require_bitemporal(&table.name, table.info.bitemporal)?;
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
        temporal,
    })
}
