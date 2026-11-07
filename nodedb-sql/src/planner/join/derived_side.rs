// SPDX-License-Identifier: Apache-2.0

//! A derived table as a JOIN side: `JOIN (SELECT ...) AS b ON ...`, on
//! either side of any join, including the FROM side of a multi-way join.
//!
//! The side is the subquery's plan, named by its alias. It is planned as a
//! one-definition CTE whose outer query is a scan of the alias:
//! `Cte { definitions: [(b, body)], outer: Scan { collection: b, alias: b } }`.
//! The scan keeps the alias, which is the qualifier of the side's columns
//! in the joined row, and the CTE lowering embeds the body's rows as the
//! join input.

use sqlparser::ast;

use crate::error::{Result, SqlError};
use crate::functions::registry::FunctionRegistry;
use crate::planner::select::rekey_join_output;
use crate::resolver::columns::TableScope;
use crate::types::*;

/// The join side a non-LATERAL derived table plans to, or `None` when
/// `factor` is not a derived table.
///
/// A derived table needs an alias, as in PostgreSQL. A reference to another
/// relation of the same FROM clause is refused when the scope is resolved,
/// before this runs.
pub(super) fn plan_derived_side(
    factor: &ast::TableFactor,
    scope: &TableScope,
    catalog: &dyn SqlCatalog,
    functions: &FunctionRegistry,
    temporal: crate::TemporalScope,
) -> Result<Option<SqlPlan>> {
    let ast::TableFactor::Derived {
        lateral: false,
        subquery,
        alias,
        ..
    } = factor
    else {
        return Ok(None);
    };
    let alias = alias.as_ref().ok_or_else(|| SqlError::Unsupported {
        detail: "a subquery in a JOIN must have an alias: write JOIN (SELECT ...) AS name ON ..."
            .into(),
    })?;
    let name = crate::reserved::check_ast_identifier(&alias.name)?;
    crate::resolver::derived::check_materialized_column_aliases(
        catalog,
        &name,
        alias.columns.len(),
    )?;
    let relation = scope
        .table_by_ref(&name)
        .ok_or_else(|| SqlError::UnknownTable { name: name.clone() })?;
    let body = crate::planner::select::plan_query(subquery, catalog, functions, temporal)?;
    Ok(Some(SqlPlan::Cte(CtePlan {
        definitions: vec![(name.clone(), rekey_join_output(body))],
        outer: Box::new(SqlPlan::Scan {
            collection: name.clone(),
            alias: Some(name),
            engine: relation.info.engine,
            filters: Vec::new(),
            projection: Vec::new(),
            sort_keys: Vec::new(),
            limit: None,
            offset: 0,
            distinct: false,
            window_functions: Vec::new(),
            // The body carries the statement's temporal scope. The scan
            // only names the body.
            temporal: crate::TemporalScope::default(),
        }),
    })))
}
