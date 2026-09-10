// SPDX-License-Identifier: Apache-2.0

//! INSERT, UPDATE, DELETE planning.

use nodedb_types::DatabaseId;
use sqlparser::ast::{self};

use super::dml_helpers::{
    KvInsertParams, bind_insert_select_columns, build_kv_insert_plan,
    build_vector_primary_insert_plan, check_declared_float_ranges_in_assignments,
    check_declared_int_ranges_in_assignments, coerce_and_check_rows, convert_value_rows,
    materialize_defaults_in_rows, resolve_insert_columns,
};
use crate::engine_rules::{self, InsertParams};
use crate::error::{Result, SqlError};
use crate::parser::normalize::{normalize_insert_column, normalize_object_name_checked};
use crate::planner::declared_type_coerce::coerce_assignments_to_declared_types;
use crate::resolver::ColumnScope;
use crate::resolver::columns::{ResolvedTable, TableScope};
use crate::resolver::expr::convert_expr;
use crate::types::*;

pub use dml_update_delete::{plan_delete, plan_truncate_stmt, plan_update};

#[path = "dml_update_delete.rs"]
mod dml_update_delete;

/// The column namespace of an INSERT target.
fn target_scope(table_name: &str, info: &CollectionInfo) -> Result<TableScope> {
    let mut scope = TableScope::single(ResolvedTable {
        name: table_name.to_string(),
        alias: None,
        info: info.clone(),
    })?;
    // `ON CONFLICT DO UPDATE` addresses the proposed row as `excluded`. It
    // carries the target's columns and is qualified-only, so a bare name in
    // the SET clause names the stored row.
    scope.add_qualified_only(ResolvedTable {
        name: EXCLUDED_RELATION.to_string(),
        alias: None,
        info: info.clone(),
    })?;
    Ok(scope)
}

/// The pseudo-relation `ON CONFLICT DO UPDATE` uses for the proposed row.
const EXCLUDED_RELATION: &str = "excluded";

/// Normalize an INSERT column list and reject a name the target does not have.
fn insert_columns(columns: &[ast::ObjectName], scope: &TableScope) -> Result<Vec<String>> {
    columns
        .iter()
        .map(|c| {
            let col = normalize_insert_column(c)?;
            scope.check_name(None, &col)?;
            Ok(col)
        })
        .collect()
}

/// Classification of an `ON CONFLICT` clause attached to an INSERT.
enum OnConflict {
    /// No `ON CONFLICT` clause — plain INSERT (error on duplicate PK).
    None,
    /// `ON CONFLICT DO NOTHING` — skip rows that would conflict, no error.
    DoNothing,
    /// `ON CONFLICT (...) DO UPDATE SET ...` — apply the assignments against
    /// the existing row on conflict.
    DoUpdate(Vec<(String, SqlExpr)>),
}

fn classify_on_conflict(ins: &ast::Insert, scope: &TableScope) -> Result<OnConflict> {
    let Some(on) = ins.on.as_ref() else {
        return Ok(OnConflict::None);
    };
    let ast::OnInsert::OnConflict(oc) = on else {
        return Ok(OnConflict::None);
    };
    match &oc.action {
        ast::OnConflictAction::DoNothing => Ok(OnConflict::DoNothing),
        ast::OnConflictAction::DoUpdate(do_update) => {
            let mut pairs = Vec::with_capacity(do_update.assignments.len());
            for a in &do_update.assignments {
                let name = match &a.target {
                    ast::AssignmentTarget::ColumnName(obj) => normalize_object_name_checked(obj)?,
                    _ => {
                        return Err(SqlError::Unsupported {
                            detail: "ON CONFLICT DO UPDATE SET target must be a column name".into(),
                        });
                    }
                };
                scope.check_name(None, &name)?;
                let expr = convert_expr(&a.value, &ColumnScope::Relations(scope))?;
                pairs.push((name, expr));
            }
            Ok(OnConflict::DoUpdate(pairs))
        }
    }
}

/// Plan an INSERT statement.
pub fn plan_insert(ins: &ast::Insert, catalog: &dyn SqlCatalog) -> Result<Vec<SqlPlan>> {
    let table_name = match &ins.table {
        ast::TableObject::TableName(name) => normalize_object_name_checked(name)?,
        ast::TableObject::TableFunction(_) => {
            return Err(SqlError::Unsupported {
                detail: "INSERT INTO table function not supported".into(),
            });
        }
        // Oracle's `INSERT INTO (SELECT ...)`: the target is a subquery, so
        // there is no collection to resolve or route to an engine.
        ast::TableObject::TableQuery(_) => {
            return Err(SqlError::Unsupported {
                detail: "INSERT INTO a subquery target is not supported".into(),
            });
        }
    };
    let info = catalog
        .get_collection(DatabaseId::DEFAULT, &table_name)?
        .ok_or_else(|| SqlError::UnknownTable {
            name: table_name.clone(),
        })?;
    let target_scope = target_scope(&table_name, &info)?;

    // `INSERT ... ON CONFLICT DO UPDATE SET` reroutes to the upsert path
    // with the assignments carried through. `DO NOTHING` stays on the
    // INSERT path with `if_absent=true`.
    let if_absent = match classify_on_conflict(ins, &target_scope)? {
        OnConflict::None => false,
        OnConflict::DoNothing => true,
        OnConflict::DoUpdate(updates) => {
            return plan_upsert_with_on_conflict(ins, catalog, updates);
        }
    };

    let columns = insert_columns(&ins.columns, &target_scope)?;

    // Check for INSERT...SELECT.
    if let Some(source) = &ins.source
        && let ast::SetExpr::Select(select) = &*source.body
    {
        let column_map = bind_insert_select_columns(catalog, &columns, select, &info)?;
        let source_plan = super::select::plan_query(
            source,
            catalog,
            &crate::functions::registry::FunctionRegistry::new(),
            crate::TemporalScope::default(),
        )?;
        return Ok(vec![SqlPlan::InsertSelect {
            target: table_name,
            source: Box::new(source_plan),
            limit: 0,
            column_map,
        }]);
    }

    // VALUES clause.
    let source = ins.source.as_ref().ok_or_else(|| SqlError::Parse {
        detail: "INSERT requires VALUES or SELECT".into(),
    })?;

    let rows_ast = match &*source.body {
        ast::SetExpr::Values(values) => &values.rows,
        _ => {
            return Err(SqlError::Unsupported {
                detail: "INSERT source must be VALUES or SELECT".into(),
            });
        }
    };

    // KV engine: key and value are fundamentally separate — handle directly.
    // Positional column binding (below) does not apply here: the KV path
    // matches columns by name against `pk_col`/`"key"`/`"ttl"`, which is
    // orthogonal to declared column order.
    if info.engine == EngineType::KeyValue {
        let intent = if if_absent {
            KvInsertIntent::InsertIfAbsent
        } else {
            KvInsertIntent::Insert
        };
        return build_kv_insert_plan(KvInsertParams {
            collection: table_name,
            columns: &columns,
            rows_ast,
            intent,
            on_conflict_updates: Vec::new(),
            pk_col: info.primary_key.as_deref(),
            declared_columns: &info.columns,
            catalog,
        });
    }

    // Positional INSERT (no column list): bind values to the collection's
    // declared column order so named projections/predicates can find them.
    // No-op for named inserts and schemaless collections.
    let columns = resolve_insert_columns(columns, &info, rows_ast)?;

    // Vector-primary collection: bypass document encoding.
    //
    // The vector path never reaches `EngineRules::plan_insert`, which is where
    // every other engine hands its declared DEFAULTs on for expansion. It
    // materializes them here instead, through the same helper the key-value
    // path uses, and before coercion so a default is range-checked exactly like
    // a supplied literal.
    if info.primary == nodedb_types::PrimaryEngine::Vector
        && let Some(ref vpc) = info.vector_primary
    {
        let mut rows_parsed = convert_value_rows(&columns, rows_ast)?;
        let volatile_defaults =
            materialize_defaults_in_rows(&info.columns, &mut rows_parsed, catalog)?;
        coerce_and_check_rows(&info, &mut rows_parsed)?;
        return build_vector_primary_insert_plan(
            &table_name,
            vpc,
            &columns,
            rows_parsed,
            volatile_defaults,
        );
    }

    // All other engines: delegate to engine rules.
    let mut rows = convert_value_rows(&columns, rows_ast)?;
    coerce_and_check_rows(&info, &mut rows)?;
    let column_defaults: Vec<(String, String)> = info
        .columns
        .iter()
        .filter_map(|c| c.default.as_ref().map(|d| (c.name.clone(), d.clone())))
        .collect();
    let column_schema: Vec<(String, String)> = info
        .columns
        .iter()
        .filter_map(|c| c.raw_type.as_ref().map(|t| (c.name.clone(), t.clone())))
        .collect();
    let rules = engine_rules::resolve_engine_rules(info.engine);
    rules.plan_insert(InsertParams {
        collection: table_name,
        columns,
        rows,
        column_defaults,
        if_absent,
        column_schema,
        primary_key: info.primary_key.clone(),
    })
}

/// Plan an UPSERT statement (pre-processed from `UPSERT INTO` to `INSERT INTO`).
///
/// Same parsing as INSERT but routes through `engine_rules.plan_upsert()`.
pub fn plan_upsert(ins: &ast::Insert, catalog: &dyn SqlCatalog) -> Result<Vec<SqlPlan>> {
    let table_name = match &ins.table {
        ast::TableObject::TableName(name) => normalize_object_name_checked(name)?,
        ast::TableObject::TableFunction(_) => {
            return Err(SqlError::Unsupported {
                detail: "UPSERT INTO table function not supported".into(),
            });
        }
        // Oracle's `INSERT INTO (SELECT ...)`: the target is a subquery, so
        // there is no collection to resolve or route to an engine.
        ast::TableObject::TableQuery(_) => {
            return Err(SqlError::Unsupported {
                detail: "UPSERT INTO a subquery target is not supported".into(),
            });
        }
    };
    let info = catalog
        .get_collection(DatabaseId::DEFAULT, &table_name)?
        .ok_or_else(|| SqlError::UnknownTable {
            name: table_name.clone(),
        })?;

    let columns = insert_columns(&ins.columns, &target_scope(&table_name, &info)?)?;

    let source = ins.source.as_ref().ok_or_else(|| SqlError::Parse {
        detail: "UPSERT requires VALUES".into(),
    })?;

    let rows_ast = match &*source.body {
        ast::SetExpr::Values(values) => &values.rows,
        _ => {
            return Err(SqlError::Unsupported {
                detail: "UPSERT source must be VALUES".into(),
            });
        }
    };

    // KV: upsert is just a PUT (natural overwrite). Positional column
    // binding (below) does not apply here — see `plan_insert`.
    if info.engine == EngineType::KeyValue {
        return build_kv_insert_plan(KvInsertParams {
            collection: table_name,
            columns: &columns,
            rows_ast,
            intent: KvInsertIntent::Put,
            on_conflict_updates: Vec::new(),
            pk_col: info.primary_key.as_deref(),
            declared_columns: &info.columns,
            catalog,
        });
    }

    // Positional UPSERT (no column list): bind to the collection's declared
    // column order — see `plan_insert` for the full rationale.
    let columns = resolve_insert_columns(columns, &info, rows_ast)?;

    let mut rows = convert_value_rows(&columns, rows_ast)?;
    coerce_and_check_rows(&info, &mut rows)?;
    let column_defaults: Vec<(String, String)> = info
        .columns
        .iter()
        .filter_map(|c| c.default.as_ref().map(|d| (c.name.clone(), d.clone())))
        .collect();
    let column_schema: Vec<(String, String)> = info
        .columns
        .iter()
        .filter_map(|c| c.raw_type.as_ref().map(|t| (c.name.clone(), t.clone())))
        .collect();
    let rules = engine_rules::resolve_engine_rules(info.engine);
    rules.plan_upsert(engine_rules::UpsertParams {
        collection: table_name,
        columns,
        rows,
        column_defaults,
        on_conflict_updates: Vec::new(),
        column_schema,
        primary_key: info.primary_key.clone(),
    })
}

/// Plan an `INSERT ... ON CONFLICT DO UPDATE SET` statement.
fn plan_upsert_with_on_conflict(
    ins: &ast::Insert,
    catalog: &dyn SqlCatalog,
    mut on_conflict_updates: Vec<(String, SqlExpr)>,
) -> Result<Vec<SqlPlan>> {
    let table_name = match &ins.table {
        ast::TableObject::TableName(name) => normalize_object_name_checked(name)?,
        ast::TableObject::TableFunction(_) => {
            return Err(SqlError::Unsupported {
                detail: "INSERT ... ON CONFLICT on a table function is not supported".into(),
            });
        }
        // Oracle's `INSERT INTO (SELECT ...)`: the target is a subquery, so
        // there is no collection to resolve or route to an engine.
        ast::TableObject::TableQuery(_) => {
            return Err(SqlError::Unsupported {
                detail: "INSERT ... ON CONFLICT on a subquery target is not supported".into(),
            });
        }
    };
    let info = catalog
        .get_collection(DatabaseId::DEFAULT, &table_name)?
        .ok_or_else(|| SqlError::UnknownTable {
            name: table_name.clone(),
        })?;

    let columns = insert_columns(&ins.columns, &target_scope(&table_name, &info)?)?;

    let source = ins.source.as_ref().ok_or_else(|| SqlError::Parse {
        detail: "INSERT ... ON CONFLICT requires VALUES".into(),
    })?;
    let rows_ast = match &*source.body {
        ast::SetExpr::Values(values) => &values.rows,
        _ => {
            return Err(SqlError::Unsupported {
                detail: "INSERT ... ON CONFLICT source must be VALUES".into(),
            });
        }
    };

    // KV: `INSERT ... ON CONFLICT (key) DO UPDATE SET ...` is an opt-in
    // overwrite — same physical semantics as UPSERT, with the optional
    // per-row assignments carried through for the Data Plane to apply
    // against the existing row.
    if info.engine == EngineType::KeyValue {
        return build_kv_insert_plan(KvInsertParams {
            collection: table_name,
            columns: &columns,
            rows_ast,
            intent: KvInsertIntent::Put,
            on_conflict_updates,
            pk_col: info.primary_key.as_deref(),
            declared_columns: &info.columns,
            catalog,
        });
    }

    // Positional UPSERT (no column list): bind to the collection's declared
    // column order — see `plan_insert` for the full rationale.
    let columns = resolve_insert_columns(columns, &info, rows_ast)?;

    let mut rows = convert_value_rows(&columns, rows_ast)?;
    coerce_and_check_rows(&info, &mut rows)?;
    // `DO UPDATE SET col = <literal>` writes through the same path as the
    // inserted row, so its literals carry the same declared-type contract.
    coerce_assignments_to_declared_types(
        &info.columns,
        &mut on_conflict_updates,
        info.primary_key.as_deref(),
    )?;
    check_declared_int_ranges_in_assignments(&info.columns, &on_conflict_updates)?;
    check_declared_float_ranges_in_assignments(&info.columns, &on_conflict_updates)?;
    let column_defaults: Vec<(String, String)> = info
        .columns
        .iter()
        .filter_map(|c| c.default.as_ref().map(|d| (c.name.clone(), d.clone())))
        .collect();
    let column_schema: Vec<(String, String)> = info
        .columns
        .iter()
        .filter_map(|c| c.raw_type.as_ref().map(|t| (c.name.clone(), t.clone())))
        .collect();
    let rules = engine_rules::resolve_engine_rules(info.engine);
    rules.plan_upsert(engine_rules::UpsertParams {
        collection: table_name,
        columns,
        rows,
        column_defaults,
        on_conflict_updates,
        column_schema,
        primary_key: info.primary_key.clone(),
    })
}
