// SPDX-License-Identifier: Apache-2.0

//! INSERT, UPDATE, DELETE planning.

use nodedb_types::DatabaseId;
use sqlparser::ast::{self};

use super::dml_helpers::{
    build_kv_insert_plan, build_vector_primary_insert_plan,
    check_declared_float_ranges_in_assignments, check_declared_int_ranges_in_assignments,
    coerce_and_check_rows, convert_value_rows, resolve_insert_columns,
};
use crate::engine_rules::{self, InsertParams};
use crate::error::{Result, SqlError};
use crate::parser::normalize::{normalize_insert_column, normalize_object_name_checked};
use crate::planner::declared_type_coerce::coerce_assignments_to_declared_types;
use crate::resolver::expr::convert_expr;
use crate::types::*;

pub use dml_update_delete::{plan_delete, plan_truncate_stmt, plan_update};

#[path = "dml_update_delete.rs"]
mod dml_update_delete;

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

fn classify_on_conflict(ins: &ast::Insert) -> Result<OnConflict> {
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
                let expr = convert_expr(&a.value)?;
                pairs.push((name, expr));
            }
            Ok(OnConflict::DoUpdate(pairs))
        }
    }
}

/// PRIMARY KEY implies NOT NULL on every engine.
///
/// A row that omits the declared primary-key column or binds it to NULL
/// must raise `23502` (not_null_violation) rather than commit. On the
/// document path such a row used to fall through to "fresh surrogate" —
/// silently minting a new identity for what the caller declared as the row's
/// key, so several NULL-key rows coexisted and readback of them disagreed
/// between scan and aggregate paths. Collections whose key is synthetic
/// (`_rowid`, or a schemaless collection carrying only the auto-injected PK
/// column) legitimately mint fresh identities and are exempt.
fn enforce_pk_not_null(
    engine: EngineType,
    collection: &str,
    primary_key: Option<&str>,
    columns: &[ColumnInfo],
    rows: &[Vec<(String, SqlValue)>],
) -> Result<()> {
    let Some(pk) = primary_key else {
        return Ok(());
    };
    if pk == "_rowid" {
        return Ok(());
    }
    let closed = match engine {
        EngineType::DocumentSchemaless => columns.len() > 1,
        EngineType::KeyValue
        | EngineType::DocumentStrict
        | EngineType::Columnar
        | EngineType::Timeseries
        | EngineType::Spatial => true,
        EngineType::Array => false,
    };
    if !closed {
        return Ok(());
    }
    let pk_has_default = columns.iter().any(|c| c.name == pk && c.default.is_some());
    for row in rows {
        let cell = row.iter().find(|(name, _)| name == pk).map(|(_, v)| v);
        let missing = cell.is_none();
        let null = matches!(cell, Some(SqlValue::Null));
        // A declared DEFAULT materializes at conversion; only absence is
        // exempt, an explicit NULL still violates the constraint.
        if missing && pk_has_default {
            continue;
        }
        if missing || null {
            return Err(SqlError::NotNullViolation {
                table: collection.to_string(),
                column: pk.to_string(),
            });
        }
    }
    Ok(())
}

/// Plan an INSERT statement.
pub fn plan_insert(ins: &ast::Insert, catalog: &dyn SqlCatalog) -> Result<Vec<SqlPlan>> {
    // `INSERT ... ON CONFLICT DO UPDATE SET` reroutes to the upsert path
    // with the assignments carried through. `DO NOTHING` stays on the
    // INSERT path with `if_absent=true`.
    let if_absent = match classify_on_conflict(ins)? {
        OnConflict::None => false,
        OnConflict::DoNothing => true,
        OnConflict::DoUpdate(updates) => {
            return plan_upsert_with_on_conflict(ins, catalog, updates);
        }
    };
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

    let columns: Vec<String> = ins
        .columns
        .iter()
        .map(normalize_insert_column)
        .collect::<Result<_>>()?;

    // Check for INSERT...SELECT.
    if let Some(source) = &ins.source
        && let ast::SetExpr::Select(_select) = &*source.body
    {
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
        return build_kv_insert_plan(
            table_name,
            &columns,
            rows_ast,
            intent,
            Vec::new(),
            info.primary_key.as_deref(),
            &info.columns,
        );
    }

    // Positional INSERT (no column list): bind values to the collection's
    // declared column order so named projections/predicates can find them.
    // No-op for named inserts and schemaless collections.
    let columns = resolve_insert_columns(columns, &info, rows_ast)?;

    // Vector-primary collection: bypass document encoding.
    if info.primary == nodedb_types::PrimaryEngine::Vector
        && let Some(ref vpc) = info.vector_primary
    {
        let mut rows_parsed = convert_value_rows(&columns, rows_ast)?;
        coerce_and_check_rows(&info, &mut rows_parsed)?;
        return build_vector_primary_insert_plan(&table_name, vpc, &columns, rows_parsed);
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
    enforce_pk_not_null(
        info.engine,
        &table_name,
        info.primary_key.as_deref(),
        &info.columns,
        &rows,
    )?;
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

    let columns: Vec<String> = ins
        .columns
        .iter()
        .map(normalize_insert_column)
        .collect::<Result<_>>()?;

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
        return build_kv_insert_plan(
            table_name,
            &columns,
            rows_ast,
            KvInsertIntent::Put,
            Vec::new(),
            info.primary_key.as_deref(),
            &info.columns,
        );
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
    enforce_pk_not_null(
        info.engine,
        &table_name,
        info.primary_key.as_deref(),
        &info.columns,
        &rows,
    )?;
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

    let columns: Vec<String> = ins
        .columns
        .iter()
        .map(normalize_insert_column)
        .collect::<Result<_>>()?;

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
        return build_kv_insert_plan(
            table_name,
            &columns,
            rows_ast,
            KvInsertIntent::Put,
            on_conflict_updates,
            info.primary_key.as_deref(),
            &info.columns,
        );
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
    enforce_pk_not_null(
        info.engine,
        &table_name,
        info.primary_key.as_deref(),
        &info.columns,
        &rows,
    )?;
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
