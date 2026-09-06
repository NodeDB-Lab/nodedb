// SPDX-License-Identifier: Apache-2.0

//! UPDATE, DELETE, and TRUNCATE planning — extracted from `dml.rs`.

use nodedb_types::DatabaseId;
use sqlparser::ast;

use super::super::ast_helpers::{
    flatten_and_expr, qualified_ident_pair, strip_and_convert_filters,
};
use super::super::dml_helpers::{
    check_declared_float_ranges_in_assignments, check_declared_int_ranges_in_assignments,
    extract_point_keys, extract_table_name_from_table_with_joins,
};
use crate::engine_rules::{self, DeleteParams, UpdateFromParams, UpdateParams};
use crate::error::{Result, SqlError};
use crate::parser::normalize::{
    SCHEMA_QUALIFIED_MSG, normalize_ident, normalize_object_name_checked,
};
use crate::planner::declared_type_coerce::coerce_assignments_to_declared_types;
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
    if update.from.is_some() {
        return plan_update_from(update, catalog);
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

    // Closed-schema existence gate (42703): a WHERE or SET target naming a
    // column the collection does not declare must not silently no-op.
    let resolved = crate::resolver::columns::ResolvedTable {
        name: table_name.clone(),
        alias: None,
        info: info.clone(),
    };
    if let Some(selection) = &update.selection {
        crate::planner::select::validate_columns::validate_where(selection, &resolved)?;
    }

    let mut assigns = convert_assignments(&update.assignments)?;
    for (column, _) in &assigns {
        crate::planner::select::validate_columns::validate_write_column(column, &resolved)?;
    }
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
        Some(expr) => super::super::select::convert_where_to_filters(expr)?,
        None => Vec::new(),
    };

    let target_keys = extract_point_keys(update.selection.as_ref(), &info);

    let rules = engine_rules::resolve_engine_rules(info.engine);
    rules.plan_update(UpdateParams {
        collection: table_name,
        assignments: assigns,
        filters,
        target_keys,
        returning: update.returning.is_some(),
    })
}

/// Plan `UPDATE target SET ... FROM src WHERE target.col = src.col ...`.
fn plan_update_from(update: &ast::Update, catalog: &dyn SqlCatalog) -> Result<Vec<SqlPlan>> {
    let target_name = extract_table_name_from_table_with_joins(&update.table)?;

    // Extract alias for the target table if present.
    let target_alias: Option<String> = match &update.table.relation {
        ast::TableFactor::Table { alias, .. } => alias
            .as_ref()
            .map(|alias| crate::reserved::check_ast_identifier(&alias.name))
            .transpose()?,
        _ => None,
    };
    let target_ref = target_alias.as_deref().unwrap_or(target_name.as_str());

    let from_kind = update.from.as_ref().expect("caller ensures from.is_some()");
    let from_tables: &Vec<ast::TableWithJoins> = match from_kind {
        ast::UpdateTableFromKind::AfterSet(tables)
        | ast::UpdateTableFromKind::BeforeSet(tables) => tables,
    };

    // Reject multi-table FROM.
    if from_tables.len() > 1 {
        return Err(SqlError::Unsupported {
            detail: format!(
                "UPDATE ... FROM with {} source tables is not supported; \
                 only a single FROM table is accepted",
                from_tables.len()
            ),
        });
    }
    let from_table = from_tables.first().ok_or_else(|| SqlError::Parse {
        detail: "UPDATE ... FROM requires at least one source table".into(),
    })?;

    // Reject subquery in FROM.
    let source_name = match &from_table.relation {
        ast::TableFactor::Table { name, .. } => normalize_object_name_checked(name)?,
        ast::TableFactor::Derived { .. } => {
            return Err(SqlError::Unsupported {
                detail: "UPDATE ... FROM (subquery) is not supported; \
                     use a CTE: WITH cte AS (SELECT ...) UPDATE t SET ... FROM cte WHERE ..."
                    .into(),
            });
        }
        _ => {
            return Err(SqlError::Unsupported {
                detail: "non-table relation in UPDATE ... FROM is not supported".into(),
            });
        }
    };
    // Reject joins in the FROM source.
    if !from_table.joins.is_empty() {
        return Err(SqlError::Unsupported {
            detail: "JOIN in UPDATE ... FROM source is not supported; \
                     use a CTE to pre-join the source"
                .into(),
        });
    }

    let source_alias: Option<String> = match &from_table.relation {
        ast::TableFactor::Table { alias, .. } => alias
            .as_ref()
            .map(|alias| crate::reserved::check_ast_identifier(&alias.name))
            .transpose()?,
        _ => None,
    };
    let source_ref = source_alias.as_deref().unwrap_or(source_name.as_str());

    // Validate that the target and source collections exist.
    let target_info = catalog
        .get_collection(DatabaseId::DEFAULT, &target_name)?
        .ok_or_else(|| SqlError::UnknownTable {
            name: target_name.clone(),
        })?;
    let source_info = catalog
        .get_collection(DatabaseId::DEFAULT, &source_name)?
        .ok_or_else(|| SqlError::UnknownTable {
            name: source_name.clone(),
        })?;

    let assigns = convert_assignments(&update.assignments)?;

    // Split the WHERE clause into:
    //   - one equi-join predicate linking target and source (required)
    //   - remaining predicates that apply to target only
    let (target_join_col, source_join_col, target_filters) = match &update.selection {
        None => {
            return Err(SqlError::Parse {
                detail: "UPDATE ... FROM requires a WHERE clause with an equi-join predicate \
                         linking the target and source tables"
                    .into(),
            });
        }
        Some(expr) => extract_join_predicate(expr, target_ref, source_ref)?,
    };

    // Plan the source as a simple scan (no filters — all filtering is via join key).
    let source_rules = engine_rules::resolve_engine_rules(source_info.engine);
    let source_plan = source_rules.plan_scan(crate::engine_rules::ScanParams {
        collection: source_name,
        alias: source_alias,
        filters: Vec::new(),
        projection: Vec::new(),
        sort_keys: Vec::new(),
        limit: None,
        offset: 0,
        distinct: false,
        window_functions: Vec::new(),
        indexes: Vec::new(),
        temporal: crate::temporal::TemporalScope::default(),
        bitemporal: source_info.bitemporal,
    })?;

    let rules = engine_rules::resolve_engine_rules(target_info.engine);
    rules.plan_update_from(UpdateFromParams {
        collection: target_name,
        source: Box::new(source_plan),
        target_join_col,
        source_join_col,
        assignments: assigns,
        target_filters,
        returning: update.returning.is_some(),
    })
}

/// Extract a single equi-join predicate of the form `target_table.col = source_table.col`
/// (or the reverse) from a WHERE expression, returning `(target_col, source_col, remaining_filters)`.
///
/// Also accepts `col = other_table.col` where `col` without a table qualifier is
/// assumed to belong to the target (PostgreSQL behavior).
fn extract_join_predicate(
    expr: &ast::Expr,
    target_ref: &str,
    source_ref: &str,
) -> Result<(String, String, Vec<Filter>)> {
    // Flatten the top-level AND chain.
    let mut conjuncts: Vec<ast::Expr> = Vec::new();
    flatten_and_expr(expr, &mut conjuncts);

    // Find the first conjunct that is an equi-join between target and source.
    let mut join_idx: Option<usize> = None;
    let mut target_col = String::new();
    let mut source_col = String::new();

    for (i, conjunct) in conjuncts.iter().enumerate() {
        if let Some((tc, sc)) = try_equijoin_pair(conjunct, target_ref, source_ref) {
            target_col = tc;
            source_col = sc;
            join_idx = Some(i);
            break;
        }
    }

    let join_idx = join_idx.ok_or_else(|| SqlError::Parse {
        detail: format!(
            "UPDATE ... FROM requires a WHERE clause equi-join predicate of the form \
             `{target_ref}.col = {source_ref}.col`; none found"
        ),
    })?;

    conjuncts.remove(join_idx);

    // Remaining conjuncts become target_filters. Strip table qualifier so
    // `uf_target.score` becomes `score` — documents store bare field names.
    let target_filters = strip_and_convert_filters(conjuncts, target_ref)?;

    Ok((target_col, source_col, target_filters))
}

/// Try to extract `(target_col, source_col)` from an equality expression
/// where one side is `target_ref.col` and the other is `source_ref.col`.
/// Also handles unqualified names by assuming they belong to the target.
fn try_equijoin_pair(
    expr: &ast::Expr,
    target_ref: &str,
    source_ref: &str,
) -> Option<(String, String)> {
    let ast::Expr::BinaryOp {
        left,
        op: ast::BinaryOperator::Eq,
        right,
    } = expr
    else {
        return None;
    };

    let lhs = qualified_ident_pair(left);
    let rhs = qualified_ident_pair(right);

    match (lhs, rhs) {
        (Some((lt, lc)), Some((rt, rc))) => {
            if lt == target_ref && rt == source_ref {
                Some((lc, rc))
            } else if lt == source_ref && rt == target_ref {
                Some((rc, lc))
            } else {
                None
            }
        }
        // One side is unqualified — treat it as belonging to target.
        (Some((t, c)), None) if t == source_ref => {
            if let ast::Expr::Identifier(ident) = right.as_ref() {
                Some((normalize_ident(ident), c))
            } else {
                None
            }
        }
        (None, Some((t, c))) if t == source_ref => {
            if let ast::Expr::Identifier(ident) = left.as_ref() {
                Some((normalize_ident(ident), c))
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Convert `update.assignments` into `Vec<(col, SqlExpr)>`.
fn convert_assignments(assignments: &[ast::Assignment]) -> Result<Vec<(String, SqlExpr)>> {
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
            let val = convert_expr(&a.value)?;
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

    // Closed-schema existence gate (42703) for the DELETE predicate.
    let resolved = crate::resolver::columns::ResolvedTable {
        name: table_name.clone(),
        alias: None,
        info: info.clone(),
    };
    if let Some(selection) = &delete.selection {
        crate::planner::select::validate_columns::validate_where(selection, &resolved)?;
    }

    let filters = match &delete.selection {
        Some(expr) => super::super::select::convert_where_to_filters(expr)?,
        None => Vec::new(),
    };

    let target_keys = extract_point_keys(delete.selection.as_ref(), &info);

    let rules = engine_rules::resolve_engine_rules(info.engine);
    rules.plan_delete(DeleteParams {
        collection: table_name,
        filters,
        target_keys,
    })
}

/// Plan a TRUNCATE statement.
pub fn plan_truncate_stmt(stmt: &ast::Statement) -> Result<Vec<SqlPlan>> {
    let ast::Statement::Truncate(truncate) = stmt else {
        return Err(SqlError::Parse {
            detail: "expected TRUNCATE statement".into(),
        });
    };
    let restart_identity = matches!(
        truncate.identity,
        Some(sqlparser::ast::TruncateIdentityOption::Restart)
    );
    truncate
        .table_names
        .iter()
        .map(|t| {
            Ok(SqlPlan::Truncate {
                collection: normalize_object_name_checked(&t.name)?,
                restart_identity,
            })
        })
        .collect()
}
