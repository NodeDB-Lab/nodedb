//! INSERT, UPDATE, DELETE planning.

use sqlparser::ast::{self};

use crate::error::{Result, SqlError};
use crate::parser::normalize::{normalize_ident, normalize_object_name};
use crate::resolver::expr::{convert_expr, convert_value};
use crate::types::*;

/// Plan an INSERT statement.
pub fn plan_insert(ins: &ast::Insert, catalog: &dyn SqlCatalog) -> Result<Vec<SqlPlan>> {
    let table_name = match &ins.table {
        ast::TableObject::TableName(name) => normalize_object_name(name),
        ast::TableObject::TableFunction(_) => {
            return Err(SqlError::Unsupported {
                detail: "INSERT INTO table function not supported".into(),
            });
        }
    };
    let info = catalog
        .get_collection(&table_name)
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

    // For timeseries, route to TimeseriesIngest.
    if info.engine == EngineType::Timeseries {
        let rows = convert_value_rows(&columns, rows_ast)?;
        return Ok(vec![SqlPlan::TimeseriesIngest {
            collection: table_name,
            rows,
        }]);
    }

    // KV engine: key (= document ID) and value are fundamentally separate.
    if info.engine == EngineType::KeyValue {
        let key_idx = columns.iter().position(|c| c == "key");
        let mut entries = Vec::with_capacity(rows_ast.len());
        for row_exprs in rows_ast {
            let key_val = match key_idx {
                Some(idx) => expr_to_sql_value(&row_exprs[idx])?,
                None => SqlValue::String(String::new()),
            };
            // Value columns = everything except key.
            let value_cols: Vec<(String, SqlValue)> = columns
                .iter()
                .enumerate()
                .filter(|(i, _)| Some(*i) != key_idx)
                .map(|(i, col)| {
                    let val = expr_to_sql_value(&row_exprs[i])?;
                    Ok((col.clone(), val))
                })
                .collect::<Result<Vec<_>>>()?;
            entries.push((key_val, value_cols));
        }
        return Ok(vec![SqlPlan::KvInsert {
            collection: table_name,
            entries,
        }]);
    }

    let rows = convert_value_rows(&columns, rows_ast)?;
    let column_defaults: Vec<(String, String)> = info
        .columns
        .iter()
        .filter_map(|c| c.default.as_ref().map(|d| (c.name.clone(), d.clone())))
        .collect();
    Ok(vec![SqlPlan::Insert {
        collection: table_name,
        engine: info.engine,
        rows,
        column_defaults,
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
        .get_collection(&table_name)
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

    Ok(vec![SqlPlan::Update {
        collection: table_name,
        engine: info.engine,
        assignments: assigns,
        filters,
        target_keys,
        returning: update.returning.is_some(),
    }])
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
        .get_collection(&table_name)
        .ok_or_else(|| SqlError::UnknownTable {
            name: table_name.clone(),
        })?;

    let filters = match &delete.selection {
        Some(expr) => super::select::convert_where_to_filters(expr)?,
        None => Vec::new(),
    };

    let target_keys = extract_point_keys(delete.selection.as_ref(), &info);

    Ok(vec![SqlPlan::Delete {
        collection: table_name,
        engine: info.engine,
        filters,
        target_keys,
    }])
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

// ── Helpers ──

fn convert_value_rows(
    columns: &[String],
    rows: &[Vec<ast::Expr>],
) -> Result<Vec<Vec<(String, SqlValue)>>> {
    rows.iter()
        .map(|row| {
            row.iter()
                .enumerate()
                .map(|(i, expr)| {
                    let col = columns.get(i).cloned().unwrap_or_else(|| format!("col{i}"));
                    let val = expr_to_sql_value(expr)?;
                    Ok((col, val))
                })
                .collect::<Result<Vec<_>>>()
        })
        .collect()
}

fn expr_to_sql_value(expr: &ast::Expr) -> Result<SqlValue> {
    match expr {
        ast::Expr::Value(v) => convert_value(&v.value),
        ast::Expr::UnaryOp {
            op: ast::UnaryOperator::Minus,
            expr: inner,
        } => {
            let val = expr_to_sql_value(inner)?;
            match val {
                SqlValue::Int(n) => Ok(SqlValue::Int(-n)),
                SqlValue::Float(f) => Ok(SqlValue::Float(-f)),
                _ => Err(SqlError::TypeMismatch {
                    detail: "cannot negate non-numeric value".into(),
                }),
            }
        }
        ast::Expr::Array(ast::Array { elem, .. }) => {
            let vals = elem.iter().map(expr_to_sql_value).collect::<Result<_>>()?;
            Ok(SqlValue::Array(vals))
        }
        ast::Expr::Function(func) => {
            let func_name = func
                .name
                .0
                .iter()
                .map(|p| match p {
                    ast::ObjectNamePart::Identifier(ident) => normalize_ident(ident),
                    _ => String::new(),
                })
                .collect::<Vec<_>>()
                .join(".")
                .to_lowercase();
            match func_name.as_str() {
                "st_point" => {
                    // ST_Point(lon, lat) → GeoJSON string at plan time.
                    let args = super::select::extract_func_args(func)?;
                    if args.len() >= 2 {
                        let lon = super::select::extract_float(&args[0])?;
                        let lat = super::select::extract_float(&args[1])?;
                        Ok(SqlValue::String(format!(
                            r#"{{"type":"Point","coordinates":[{lon},{lat}]}}"#
                        )))
                    } else {
                        Ok(SqlValue::String(format!("{expr}")))
                    }
                }
                "st_geomfromgeojson" => {
                    let args = super::select::extract_func_args(func)?;
                    if !args.is_empty() {
                        let s = super::select::extract_string_literal(&args[0])?;
                        Ok(SqlValue::String(s))
                    } else {
                        Ok(SqlValue::String(format!("{expr}")))
                    }
                }
                _ => {
                    // Other functions like now() — store as string for runtime eval.
                    Ok(SqlValue::String(format!("{expr}")))
                }
            }
        }
        _ => Err(SqlError::Unsupported {
            detail: format!("value expression: {expr}"),
        }),
    }
}

fn extract_table_name_from_table_with_joins(table: &ast::TableWithJoins) -> Result<String> {
    match &table.relation {
        ast::TableFactor::Table { name, .. } => Ok(normalize_object_name(name)),
        _ => Err(SqlError::Unsupported {
            detail: "non-table target in DML".into(),
        }),
    }
}

/// Extract point-operation keys from WHERE clause (WHERE pk = literal OR pk IN (...)).
fn extract_point_keys(selection: Option<&ast::Expr>, info: &CollectionInfo) -> Vec<SqlValue> {
    let pk = match &info.primary_key {
        Some(pk) => pk.clone(),
        None => return Vec::new(),
    };

    let expr = match selection {
        Some(e) => e,
        None => return Vec::new(),
    };

    let mut keys = Vec::new();
    collect_pk_equalities(expr, &pk, &mut keys);
    keys
}

fn collect_pk_equalities(expr: &ast::Expr, pk: &str, keys: &mut Vec<SqlValue>) {
    match expr {
        ast::Expr::BinaryOp {
            left,
            op: ast::BinaryOperator::Eq,
            right,
        } => {
            if is_column(left, pk)
                && let Ok(v) = expr_to_sql_value(right)
            {
                keys.push(v);
            } else if is_column(right, pk)
                && let Ok(v) = expr_to_sql_value(left)
            {
                keys.push(v);
            }
        }
        ast::Expr::BinaryOp {
            left,
            op: ast::BinaryOperator::Or,
            right,
        } => {
            collect_pk_equalities(left, pk, keys);
            collect_pk_equalities(right, pk, keys);
        }
        ast::Expr::InList {
            expr: inner,
            list,
            negated: false,
        } => {
            if is_column(inner, pk) {
                for item in list {
                    if let Ok(v) = expr_to_sql_value(item) {
                        keys.push(v);
                    }
                }
            }
        }
        _ => {}
    }
}

fn is_column(expr: &ast::Expr, name: &str) -> bool {
    match expr {
        ast::Expr::Identifier(ident) => normalize_ident(ident) == name,
        ast::Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            normalize_ident(&parts[1]) == name
        }
        _ => false,
    }
}
