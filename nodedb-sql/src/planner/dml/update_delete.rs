// SPDX-License-Identifier: Apache-2.0

//! UPDATE, DELETE, and TRUNCATE planning.

use nodedb_types::DatabaseId;
use sqlparser::ast;

use super::super::dml_helpers::{
    VectorPrimaryUpdateParams, build_vector_primary_delete_plan,
    build_vector_primary_truncate_plan, build_vector_primary_update_plan,
    check_declared_float_ranges_in_assignments, check_declared_int_ranges_in_assignments,
    extract_point_keys, extract_table_name_from_table_with_joins, is_vector_primary,
};
use crate::engine_rules::{self, DeleteParams, TruncateParams, UpdateParams};
use crate::error::{Result, SqlError};
use crate::parser::normalize::{SCHEMA_QUALIFIED_MSG, normalize_object_name_checked};
use crate::planner::declared_type_coerce::coerce_assignments_to_declared_types;
use crate::resolver::ColumnScope;
use crate::resolver::columns::{ResolvedTable, TableScope};
use crate::resolver::expr::convert_expr;
use crate::types::*;

/// Plan an UPDATE statement.
pub fn plan_update(stmt: &ast::Statement, catalog: &dyn SqlCatalog) -> Result<Vec<SqlPlan>> {
    let ast::Statement::Update(update) = stmt else {
        return Err(SqlError::Parse {
            detail: "expected UPDATE statement".into(),
        });
    };

    if update.limit.is_some() {
        return Err(SqlError::Unsupported {
            detail: "UPDATE ... LIMIT is not supported".into(),
        });
    }

    // Delegate to the UPDATE...FROM path when a FROM clause is present.
    if let Some(from_kind) = update.from.as_ref() {
        return super::update_from::plan_update_from(update, from_kind, catalog);
    }

    let table_name = extract_table_name_from_table_with_joins(&update.table)?;
    // Validate an optional target alias even though the non-FROM update plan
    // does not retain it.
    crate::parser::normalize::table_name_from_factor(&update.table.relation)?;
    let info = catalog
        .get_collection(DatabaseId::DEFAULT, &table_name)?
        .ok_or_else(|| SqlError::UnknownTable {
            name: table_name.clone(),
        })?;

    let target_scope = TableScope::single(ResolvedTable {
        name: table_name.clone(),
        alias: None,
        info: info.clone(),
    })?;

    let mut assigns = convert_assignments(&update.assignments, &target_scope, &target_scope)?;
    // Re-type each literal assignment to its declared column type before the
    // range check reads it — the same order, and for the same reason, as the
    // INSERT path's `coerce_and_check_rows`. Engines with a typed write path
    // coerce it themselves; the schemaless-document and key-value engines,
    // which persist the value they are handed, do not — and an `UPDATE` that
    // leaves a numeric column holding text is the same silent loss on read as
    // an `INSERT` that does (see `declared_type_coerce`).
    coerce_assignments_to_declared_types(&info.columns, &mut assigns, info.primary_key.as_deref())?;
    check_declared_int_ranges_in_assignments(&info.columns, &assigns)?;
    check_declared_float_ranges_in_assignments(&info.columns, &assigns)?;

    let filters = match &update.selection {
        Some(expr) => super::super::select::convert_where_to_filters(expr, &target_scope)?,
        None => Vec::new(),
    };

    let target_keys = extract_point_keys(update.selection.as_ref(), &info, &catalog.crs_context());

    // Vector-primary collection: the row lives in the HNSW index and its
    // payload sidecar, never in the document store the engine rules target.
    if is_vector_primary(&info)
        && let Some(ref vpc) = info.vector_primary
    {
        return build_vector_primary_update_plan(VectorPrimaryUpdateParams {
            collection: &table_name,
            info: &info,
            vpc,
            assignments: assigns,
            filters,
            target_keys,
            returning: update.returning.is_some(),
        });
    }

    let rules = engine_rules::resolve_engine_rules(info.engine);
    rules.plan_update(UpdateParams {
        collection: table_name,
        assignments: assigns,
        filters,
        target_keys,
        returning: update.returning.is_some(),
    })
}

/// Convert `update.assignments` into `Vec<(col, SqlExpr)>`.
pub(super) fn convert_assignments(
    assignments: &[ast::Assignment],
    target_scope: &TableScope,
    value_scope: &TableScope,
) -> Result<Vec<(String, SqlExpr)>> {
    assignments
        .iter()
        .map(|a| {
            let col = match &a.target {
                ast::AssignmentTarget::ColumnName(name) => {
                    if name.0.len() > 1 {
                        return Err(SqlError::Unsupported {
                            detail: format!(
                                "qualified column name in SET target: {SCHEMA_QUALIFIED_MSG}"
                            ),
                        });
                    }
                    normalize_object_name_checked(name)?
                }
                ast::AssignmentTarget::Tuple(names) => names
                    .iter()
                    .map(normalize_object_name_checked)
                    .collect::<Result<Vec<_>>>()?
                    .join(","),
            };
            for name in col.split(',') {
                target_scope.check_name(None, name)?;
            }
            let val = convert_expr(&a.value, &ColumnScope::Relations(value_scope))?;
            Ok((col, val))
        })
        .collect()
}

/// Plan a DELETE statement.
pub fn plan_delete(stmt: &ast::Statement, catalog: &dyn SqlCatalog) -> Result<Vec<SqlPlan>> {
    let ast::Statement::Delete(delete) = stmt else {
        return Err(SqlError::Parse {
            detail: "expected DELETE statement".into(),
        });
    };

    if delete.limit.is_some() {
        return Err(SqlError::Unsupported {
            detail: "DELETE ... LIMIT is not supported".into(),
        });
    }
    if !delete.order_by.is_empty() {
        return Err(SqlError::Unsupported {
            detail: "DELETE ... ORDER BY is not supported".into(),
        });
    }

    let from_tables = match &delete.from {
        ast::FromTable::WithFromKeyword(tables) | ast::FromTable::WithoutKeyword(tables) => tables,
    };
    let from = from_tables.first().ok_or_else(|| SqlError::Parse {
        detail: "DELETE requires a FROM table".into(),
    })?;
    let table_name = extract_table_name_from_table_with_joins(from)?;
    crate::parser::normalize::table_name_from_factor(&from.relation)?;
    let info = catalog
        .get_collection(DatabaseId::DEFAULT, &table_name)?
        .ok_or_else(|| SqlError::UnknownTable {
            name: table_name.clone(),
        })?;

    let target_scope = TableScope::single(ResolvedTable {
        name: table_name.clone(),
        alias: None,
        info: info.clone(),
    })?;

    let filters = match &delete.selection {
        Some(expr) => super::super::select::convert_where_to_filters(expr, &target_scope)?,
        None => Vec::new(),
    };

    let target_keys = extract_point_keys(delete.selection.as_ref(), &info, &catalog.crs_context());

    // Vector-primary collection: see `plan_update`.
    if is_vector_primary(&info)
        && let Some(ref vpc) = info.vector_primary
    {
        return Ok(build_vector_primary_delete_plan(
            &table_name,
            &info,
            vpc,
            filters,
            target_keys,
        ));
    }

    let rules = engine_rules::resolve_engine_rules(info.engine);
    rules.plan_delete(DeleteParams {
        collection: table_name,
        filters,
        target_keys,
    })
}

/// Plan a TRUNCATE statement.
///
/// Each named collection resolves through the catalog: a vector-primary
/// collection lowers to `SqlPlan::VectorPrimaryTruncate`, every other
/// engine routes through its `EngineRules::plan_truncate`.
pub fn plan_truncate_stmt(stmt: &ast::Statement, catalog: &dyn SqlCatalog) -> Result<Vec<SqlPlan>> {
    let ast::Statement::Truncate(truncate) = stmt else {
        return Err(SqlError::Parse {
            detail: "expected TRUNCATE statement".into(),
        });
    };
    let restart_identity = matches!(
        truncate.identity,
        Some(sqlparser::ast::TruncateIdentityOption::Restart)
    );
    let mut plans = Vec::with_capacity(truncate.table_names.len());
    for target in &truncate.table_names {
        let table_name = normalize_object_name_checked(&target.name)?;
        // An array lives in its own catalog namespace, so its refusal comes
        // from `ArrayRules` before the collection lookup can miss it.
        if catalog.array_exists(&table_name) {
            plans.append(
                &mut engine_rules::resolve_engine_rules(EngineType::Array).plan_truncate(
                    TruncateParams {
                        collection: table_name,
                        restart_identity,
                    },
                )?,
            );
            continue;
        }
        let info = catalog
            .get_collection(DatabaseId::DEFAULT, &table_name)?
            .ok_or_else(|| SqlError::UnknownTable {
                name: table_name.clone(),
            })?;
        // Vector-primary collection: see `plan_update`.
        if is_vector_primary(&info)
            && let Some(ref vpc) = info.vector_primary
        {
            plans.push(build_vector_primary_truncate_plan(
                &table_name,
                vpc,
                restart_identity,
            ));
            continue;
        }
        let rules = engine_rules::resolve_engine_rules(info.engine);
        plans.append(&mut rules.plan_truncate(TruncateParams {
            collection: table_name,
            restart_identity,
        })?);
    }
    Ok(plans)
}
