// SPDX-License-Identifier: BUSL-1.1

//! The one DEFAULT materialization point for the row-shaped DML converters.

use nodedb_sql::types::SqlValue;

use super::super::convert::ConvertContext;
use crate::control::planner::plan_error_map::map_plan_error;
use crate::types::TenantId;

/// Expand each row's omitted DEFAULT columns, before engine dispatch.
///
/// INSERT and UPSERT call this once per statement, so identity derivation,
/// the primary-key NOT NULL gate, and the stored payload all read the same
/// row. A DEFAULT materialized per engine after that gate refuses a key the
/// declaration supplies.
///
/// Each DEFAULT compiles once per statement and evaluates once per row, so a
/// `nextval` DEFAULT allocates exactly one value per row of a multi-row VALUES
/// clause and the expression is parsed once however many rows it fills.
///
/// `column_defaults` empty means nothing to expand: the rows pass through and
/// the catalog is never read.
pub(in super::super) fn expand_row_defaults(
    rows: &[Vec<(String, SqlValue)>],
    column_defaults: &[(String, String)],
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<Vec<Vec<(String, SqlValue)>>> {
    if column_defaults.is_empty() {
        return Ok(rows.to_vec());
    }
    let catalog = ctx.sql_catalog()?;
    let compiled = nodedb_sql::planner::defaults::ColumnDefaults::compile_pairs(column_defaults)
        .map_err(|e| map_plan_error(e, tenant_id))?;
    let mut expanded = Vec::with_capacity(rows.len());
    for row in rows {
        let mut row = row.clone();
        compiled
            .materialize_row(&mut row, catalog)
            .map_err(|e| map_plan_error(e, tenant_id))?;
        expanded.push(row);
    }
    Ok(expanded)
}
