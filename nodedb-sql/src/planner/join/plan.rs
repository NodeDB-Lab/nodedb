//! JOIN planning entry: builds left + right scans (or array-TVF arms),
//! attaches projection/filters/aggregation.

use sqlparser::ast::{self, Select};

use super::array_arm;
use super::constraint::extract_join_spec;
use crate::error::{Result, SqlError};
use crate::functions::registry::FunctionRegistry;
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

    // Left side: either an NDARRAY_* TVF or a named table.
    let left_plan = if let Some(plan) = array_arm::try_plan_relation(&from.relation, catalog)? {
        plan
    } else {
        scan_for_relation(&from.relation, scope)?
    };

    let mut current_plan = left_plan;

    for join_item in &from.joins {
        // Right side: array TVF or named table.
        let right_plan =
            if let Some(plan) = array_arm::try_plan_relation(&join_item.relation, catalog)? {
                plan
            } else {
                scan_for_relation(&join_item.relation, scope)?
            };

        let (join_type, on_keys, condition) = extract_join_spec(&join_item.join_operator)?;

        current_plan = SqlPlan::Join {
            left: Box::new(current_plan),
            right: Box::new(right_plan),
            on: on_keys,
            join_type,
            condition,
            limit: 10000,
            projection: Vec::new(),
            filters: Vec::new(),
        };
    }

    let (subquery_joins, effective_where) = if let Some(expr) = &select.selection {
        let extraction =
            super::super::subquery::extract_subqueries(expr, catalog, functions, temporal)?;
        (extraction.joins, extraction.remaining_where)
    } else {
        (Vec::new(), None)
    };

    let projection = super::super::select::convert_projection(&select.projection)?;
    let filters = match &effective_where {
        Some(expr) => super::super::select::convert_where_to_filters(expr)?,
        None => Vec::new(),
    };

    for sq in subquery_joins {
        current_plan = SqlPlan::Join {
            left: Box::new(current_plan),
            right: Box::new(sq.inner_plan),
            on: vec![(sq.outer_column, sq.inner_column)],
            join_type: sq.join_type,
            condition: None,
            limit: 10000,
            projection: Vec::new(),
            filters: Vec::new(),
        };
    }

    let group_by_non_empty = match &select.group_by {
        ast::GroupByExpr::All(_) => true,
        ast::GroupByExpr::Expressions(exprs, _) => !exprs.is_empty(),
    };
    if super::super::select::convert_projection(&select.projection).is_ok() && group_by_non_empty {
        let aggregates = super::super::aggregate::extract_aggregates_from_projection(
            &select.projection,
            functions,
        )?;
        let group_by = super::super::aggregate::convert_group_by(&select.group_by)?;
        let having = match &select.having {
            Some(expr) => super::super::select::convert_where_to_filters(expr)?,
            None => Vec::new(),
        };
        return Ok(Some(SqlPlan::Aggregate {
            input: Box::new(current_plan),
            group_by,
            aggregates,
            having,
            limit: 10000,
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

/// Build a `SqlPlan::Scan` for a named-table TableFactor.
fn scan_for_relation(rel: &ast::TableFactor, scope: &TableScope) -> Result<SqlPlan> {
    let (rel_name, rel_alias) =
        crate::parser::normalize::table_name_from_factor(rel).ok_or_else(|| {
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
