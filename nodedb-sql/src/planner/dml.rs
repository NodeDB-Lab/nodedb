//! INSERT, UPDATE, DELETE planning.

use sqlparser::ast::{self};

use super::dml_helpers::{
    convert_value_rows, expr_to_sql_value, extract_point_keys,
    extract_table_name_from_table_with_joins,
};
use crate::engine_rules::{self, DeleteParams, InsertParams, UpdateParams};
use crate::error::{Result, SqlError};
use crate::parser::normalize::{normalize_ident, normalize_object_name};
use crate::resolver::expr::convert_expr;
use crate::types::*;

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
                    ast::AssignmentTarget::ColumnName(obj) => normalize_object_name(obj),
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
        ast::TableObject::TableName(name) => normalize_object_name(name),
        ast::TableObject::TableFunction(_) => {
            return Err(SqlError::Unsupported {
                detail: "INSERT INTO table function not supported".into(),
            });
        }
    };
    let info = catalog
        .get_collection(&table_name)?
        .ok_or_else(|| SqlError::UnknownTable {
            name: table_name.clone(),
        })?;

    let columns: Vec<String> = ins.columns.iter().map(normalize_ident).collect();

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
    if info.engine == EngineType::KeyValue {
        let intent = if if_absent {
            KvInsertIntent::InsertIfAbsent
        } else {
            KvInsertIntent::Insert
        };
        return build_kv_insert_plan(table_name, &columns, rows_ast, intent, Vec::new());
    }

    // Vector-primary collection: bypass document encoding.
    if info.primary == nodedb_types::PrimaryEngine::Vector
        && let Some(ref vpc) = info.vector_primary
    {
        let rows_parsed = convert_value_rows(&columns, rows_ast)?;
        return build_vector_primary_insert_plan(&table_name, vpc, &columns, rows_parsed);
    }

    // All other engines: delegate to engine rules.
    let rows = convert_value_rows(&columns, rows_ast)?;
    let column_defaults: Vec<(String, String)> = info
        .columns
        .iter()
        .filter_map(|c| c.default.as_ref().map(|d| (c.name.clone(), d.clone())))
        .collect();
    let rules = engine_rules::resolve_engine_rules(info.engine);
    rules.plan_insert(InsertParams {
        collection: table_name,
        columns,
        rows,
        column_defaults,
        if_absent,
    })
}

/// Build a `SqlPlan::VectorPrimaryInsert` from parsed rows.
///
/// Extracts the vector-field column into `vector: Vec<f32>` and collects
/// all remaining columns into `payload_fields`. Rows missing the vector
/// column are rejected.
fn build_vector_primary_insert_plan(
    collection: &str,
    vpc: &nodedb_types::VectorPrimaryConfig,
    _columns: &[String],
    rows: Vec<Vec<(String, SqlValue)>>,
) -> Result<Vec<SqlPlan>> {
    let mut result_rows = Vec::with_capacity(rows.len());
    for row in rows {
        let mut vector: Option<Vec<f32>> = None;
        let mut payload_fields = std::collections::HashMap::new();

        for (col, val) in row {
            if col == vpc.vector_field {
                match val {
                    SqlValue::Array(items) => {
                        let floats: Result<Vec<f32>> = items
                            .iter()
                            .map(|v| match v {
                                SqlValue::Float(f) => Ok(*f as f32),
                                SqlValue::Int(i) => Ok(*i as f32),
                                other => Err(SqlError::Parse {
                                    detail: format!(
                                        "vector field must contain numbers, got {other:?}"
                                    ),
                                }),
                            })
                            .collect();
                        vector = Some(floats?);
                    }
                    other => {
                        return Err(SqlError::Parse {
                            detail: format!(
                                "vector field '{}' must be an array literal, got {other:?}",
                                vpc.vector_field
                            ),
                        });
                    }
                }
            } else {
                payload_fields.insert(col, val);
            }
        }

        let vector = vector.ok_or_else(|| SqlError::Parse {
            detail: format!(
                "vector-primary INSERT missing required vector field '{}'",
                vpc.vector_field
            ),
        })?;

        result_rows.push(VectorPrimaryRow {
            surrogate: nodedb_types::Surrogate::ZERO,
            vector,
            payload_fields,
        });
    }

    Ok(vec![SqlPlan::VectorPrimaryInsert {
        collection: collection.to_string(),
        field: vpc.vector_field.clone(),
        quantization: vpc.quantization,
        payload_indexes: vpc.payload_indexes.clone(),
        rows: result_rows,
    }])
}

/// Plan an UPSERT statement (pre-processed from `UPSERT INTO` to `INSERT INTO`).
///
/// Same parsing as INSERT but routes through `engine_rules.plan_upsert()`.
pub fn plan_upsert(ins: &ast::Insert, catalog: &dyn SqlCatalog) -> Result<Vec<SqlPlan>> {
    let table_name = match &ins.table {
        ast::TableObject::TableName(name) => normalize_object_name(name),
        ast::TableObject::TableFunction(_) => {
            return Err(SqlError::Unsupported {
                detail: "UPSERT INTO table function not supported".into(),
            });
        }
    };
    let info = catalog
        .get_collection(&table_name)?
        .ok_or_else(|| SqlError::UnknownTable {
            name: table_name.clone(),
        })?;

    let columns: Vec<String> = ins.columns.iter().map(normalize_ident).collect();

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

    // KV: upsert is just a PUT (natural overwrite).
    if info.engine == EngineType::KeyValue {
        return build_kv_insert_plan(
            table_name,
            &columns,
            rows_ast,
            KvInsertIntent::Put,
            Vec::new(),
        );
    }

    let rows = convert_value_rows(&columns, rows_ast)?;
    let column_defaults: Vec<(String, String)> = info
        .columns
        .iter()
        .filter_map(|c| c.default.as_ref().map(|d| (c.name.clone(), d.clone())))
        .collect();
    let rules = engine_rules::resolve_engine_rules(info.engine);
    rules.plan_upsert(engine_rules::UpsertParams {
        collection: table_name,
        columns,
        rows,
        column_defaults,
        on_conflict_updates: Vec::new(),
    })
}

/// Plan an `INSERT ... ON CONFLICT DO UPDATE SET` statement. Identical to
/// `plan_upsert` except the assignments are carried onto the upsert plan
/// so the Data Plane can evaluate them against the existing row instead
/// of merging the would-be-inserted values.
fn plan_upsert_with_on_conflict(
    ins: &ast::Insert,
    catalog: &dyn SqlCatalog,
    on_conflict_updates: Vec<(String, SqlExpr)>,
) -> Result<Vec<SqlPlan>> {
    let table_name = match &ins.table {
        ast::TableObject::TableName(name) => normalize_object_name(name),
        ast::TableObject::TableFunction(_) => {
            return Err(SqlError::Unsupported {
                detail: "INSERT ... ON CONFLICT on a table function is not supported".into(),
            });
        }
    };
    let info = catalog
        .get_collection(&table_name)?
        .ok_or_else(|| SqlError::UnknownTable {
            name: table_name.clone(),
        })?;

    let columns: Vec<String> = ins.columns.iter().map(normalize_ident).collect();

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
        );
    }

    let rows = convert_value_rows(&columns, rows_ast)?;
    let column_defaults: Vec<(String, String)> = info
        .columns
        .iter()
        .filter_map(|c| c.default.as_ref().map(|d| (c.name.clone(), d.clone())))
        .collect();
    let rules = engine_rules::resolve_engine_rules(info.engine);
    rules.plan_upsert(engine_rules::UpsertParams {
        collection: table_name,
        columns,
        rows,
        column_defaults,
        on_conflict_updates,
    })
}

/// Build a `SqlPlan::KvInsert` from a VALUES clause. Shared by plain INSERT,
/// UPSERT, and `INSERT ... ON CONFLICT (key) DO UPDATE` — the three paths
/// differ only in `intent` and `on_conflict_updates`, never in how entries
/// are extracted from the row exprs.
fn build_kv_insert_plan(
    table_name: String,
    columns: &[String],
    rows_ast: &[Vec<ast::Expr>],
    intent: KvInsertIntent,
    on_conflict_updates: Vec<(String, SqlExpr)>,
) -> Result<Vec<SqlPlan>> {
    let key_idx = columns.iter().position(|c| c == "key");
    let ttl_idx = columns.iter().position(|c| c == "ttl");
    let mut entries = Vec::with_capacity(rows_ast.len());
    let mut ttl_secs: u64 = 0;
    for row_exprs in rows_ast {
        let key_val = match key_idx {
            Some(idx) => expr_to_sql_value(&row_exprs[idx])?,
            None => SqlValue::String(String::new()),
        };
        if let Some(idx) = ttl_idx {
            match expr_to_sql_value(&row_exprs[idx]) {
                Ok(SqlValue::Int(n)) => ttl_secs = n.max(0) as u64,
                Ok(SqlValue::Float(f)) => ttl_secs = f.max(0.0) as u64,
                _ => {}
            }
        }
        let value_cols: Vec<(String, SqlValue)> = columns
            .iter()
            .enumerate()
            .filter(|(i, _)| Some(*i) != key_idx && Some(*i) != ttl_idx)
            .map(|(i, col)| {
                let val = expr_to_sql_value(&row_exprs[i])?;
                Ok((col.clone(), val))
            })
            .collect::<Result<Vec<_>>>()?;
        entries.push((key_val, value_cols));
    }
    Ok(vec![SqlPlan::KvInsert {
        collection: table_name,
        entries,
        ttl_secs,
        intent,
        on_conflict_updates,
    }])
}

/// Plan an UPDATE statement.
pub fn plan_update(stmt: &ast::Statement, catalog: &dyn SqlCatalog) -> Result<Vec<SqlPlan>> {
    let ast::Statement::Update(update) = stmt else {
        return Err(SqlError::Parse {
            detail: "expected UPDATE statement".into(),
        });
    };

    let table_name = extract_table_name_from_table_with_joins(&update.table)?;
    let info = catalog
        .get_collection(&table_name)?
        .ok_or_else(|| SqlError::UnknownTable {
            name: table_name.clone(),
        })?;

    let assigns: Vec<(String, SqlExpr)> = update
        .assignments
        .iter()
        .map(|a| {
            let col = match &a.target {
                ast::AssignmentTarget::ColumnName(name) => normalize_object_name(name),
                ast::AssignmentTarget::Tuple(names) => names
                    .iter()
                    .map(normalize_object_name)
                    .collect::<Vec<_>>()
                    .join(","),
            };
            let val = convert_expr(&a.value)?;
            Ok((col, val))
        })
        .collect::<Result<_>>()?;

    let filters = match &update.selection {
        Some(expr) => super::select::convert_where_to_filters(expr)?,
        None => Vec::new(),
    };

    // Detect point updates (WHERE pk = literal).
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

/// Plan a DELETE statement.
pub fn plan_delete(stmt: &ast::Statement, catalog: &dyn SqlCatalog) -> Result<Vec<SqlPlan>> {
    let ast::Statement::Delete(delete) = stmt else {
        return Err(SqlError::Parse {
            detail: "expected DELETE statement".into(),
        });
    };

    let from_tables = match &delete.from {
        ast::FromTable::WithFromKeyword(tables) | ast::FromTable::WithoutKeyword(tables) => tables,
    };
    let table_name =
        extract_table_name_from_table_with_joins(from_tables.first().ok_or_else(|| {
            SqlError::Parse {
                detail: "DELETE requires a FROM table".into(),
            }
        })?)?;
    let info = catalog
        .get_collection(&table_name)?
        .ok_or_else(|| SqlError::UnknownTable {
            name: table_name.clone(),
        })?;

    let filters = match &delete.selection {
        Some(expr) => super::select::convert_where_to_filters(expr)?,
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
                collection: normalize_object_name(&t.name),
                restart_identity,
            })
        })
        .collect()
}

// ── Helpers extracted to `dml_helpers.rs` to keep this file under 500 lines.
