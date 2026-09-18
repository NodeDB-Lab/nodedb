// SPDX-License-Identifier: Apache-2.0

//! Comma-LATERAL FROM planning: `FROM t, LATERAL (SELECT ...) x`.

use nodedb_types::DatabaseId;
use sqlparser::ast::Select;

use super::helpers::convert_projection;
use crate::error::{Result, SqlError};
use crate::planner::lateral::plan::{LateralJoinArgs, plan_lateral_join};
use crate::planner::lateral::subquery::{
    is_lateral_derived, lateral_alias_from_factor, subquery_from_factor,
};
use crate::resolver::columns::TableScope;
use crate::temporal::TemporalScope;
use crate::types::*;

/// Plan `FROM t, LATERAL (SELECT ...) x`.
///
/// sqlparser represents this as two `TableWithJoins` elements in `select.from`,
/// where the second has an empty joins list and a `Derived { lateral: true }`
/// relation. Returns `Ok(None)` when the FROM clause has another shape.
pub(super) fn try_plan_comma_lateral(
    select: &Select,
    scope: &TableScope,
    catalog: &dyn SqlCatalog,
    functions: &crate::functions::registry::FunctionRegistry,
    temporal: TemporalScope,
) -> Result<Option<SqlPlan>> {
    if select.from.len() != 2 || !is_lateral_derived(&select.from[1].relation) {
        return Ok(None);
    }
    let outer_twj = &select.from[0];
    let lateral_twj = &select.from[1];

    let outer_alias = extract_table_alias_from_twj(outer_twj)?;
    let outer_collection = crate::parser::normalize::table_name_from_factor(&outer_twj.relation)?
        .map(|(n, _)| n)
        .ok_or_else(|| SqlError::Unsupported {
            detail: "LATERAL: outer side must be a plain table".into(),
        })?;
    let outer_info = catalog
        .resolve_relation(DatabaseId::DEFAULT, &outer_collection)?
        .ok_or_else(|| SqlError::UnknownTable {
            name: outer_collection.clone(),
        })?;
    let outer_scan = SqlPlan::Scan {
        collection: outer_collection,
        alias: outer_alias.clone(),
        engine: outer_info.engine,
        filters: Vec::new(),
        projection: Vec::new(),
        sort_keys: Vec::new(),
        limit: None,
        offset: 0,
        distinct: false,
        window_functions: Vec::new(),
        temporal,
    };

    let lateral_alias =
        lateral_alias_from_factor(&lateral_twj.relation)?.ok_or_else(|| SqlError::Unsupported {
            detail: "LATERAL subquery requires an alias (e.g. LATERAL (...) AS x)".into(),
        })?;
    let subquery = subquery_from_factor(&lateral_twj.relation)
        .expect("is_lateral_derived guarantees Derived variant");
    let projection = convert_projection(&select.projection, scope)?;
    plan_lateral_join(LateralJoinArgs {
        outer_plan: outer_scan,
        outer_alias,
        subquery,
        lateral_alias: &lateral_alias,
        // Comma-LATERAL carries INNER semantics, never LEFT.
        left_join: false,
        outer_projection: projection,
        outer_scope: scope,
        catalog,
        functions,
        temporal,
    })
    .map(Some)
}

/// The alias of the first table in a `TableWithJoins`, defaulting to its name.
fn extract_table_alias_from_twj(twj: &sqlparser::ast::TableWithJoins) -> Result<Option<String>> {
    crate::parser::normalize::table_name_from_factor(&twj.relation)
        .map(|relation| relation.map(|(name, alias)| alias.unwrap_or(name)))
}
