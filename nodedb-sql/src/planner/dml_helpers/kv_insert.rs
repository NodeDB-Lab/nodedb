// SPDX-License-Identifier: Apache-2.0

//! Plan construction for the KV engine's `VALUES`-clause insert paths
//! (plain `INSERT`, `UPSERT`, and `INSERT ... ON CONFLICT DO UPDATE`).

use sqlparser::ast;

use super::range_check::{
    check_declared_float_ranges, check_declared_float_ranges_in_assignments,
    check_declared_int_ranges, check_declared_int_ranges_in_assignments,
};
use super::value_convert::expr_to_sql_value;
use crate::error::{Result, SqlError};
use crate::planner::declared_type_coerce::{
    coerce_assignments_to_declared_types, coerce_row_to_declared_types,
};
use crate::types::*;

/// Build a `SqlPlan::KvInsert` from a VALUES clause. Shared by plain INSERT,
/// UPSERT, and `INSERT ... ON CONFLICT (key) DO UPDATE` — the three paths
/// differ only in `intent` and `on_conflict_updates`, never in how entries
/// are extracted from the row exprs.
///
/// `pk_col` is the schema-defined primary-key column name from
/// `CollectionInfo::primary_key`.  When supplied, that column is used as
/// the KV key regardless of whether it is named `"key"`.  Falls back to
/// the literal name `"key"` when `pk_col` is `None` (legacy / generic
/// KV collections that use the built-in key/value column convention).
pub(crate) fn build_kv_insert_plan(
    table_name: String,
    columns: &[String],
    rows_ast: &[ast::Parens<Vec<ast::Expr>>],
    intent: KvInsertIntent,
    mut on_conflict_updates: Vec<(String, SqlExpr)>,
    pk_col: Option<&str>,
    declared_columns: &[ColumnInfo],
) -> Result<Vec<SqlPlan>> {
    // Positional KV insert (no column list): the key/value split below is
    // driven entirely by matching column *names* against `key_col_name`/
    // `"ttl"`. With an empty `columns` list there is no key to bind to, so
    // every row would silently become an empty-keyed, empty-valued entry
    // (all colliding). Reject rather than corrupt.
    if columns.is_empty() {
        return Err(SqlError::PositionalKvInsertUnsupported {
            collection: table_name,
        });
    }
    let key_col_name = pk_col.unwrap_or("key");
    // When using a named primary-key column (e.g. `k STRING PRIMARY KEY`), we
    // store the key bytes in the KV key slot AND also keep the column in the
    // value map.  This allows scan filters on the primary-key column (e.g.
    // `WHERE k = 'x'`) and projection (e.g. `SELECT k FROM ...`) to work
    // without teaching the KV scan handler to inspect the raw key bytes.
    // The only column we exclude from the value map is the built-in `"key"`
    // sentinel (used by raw key/value KV collections) and `"ttl"`.
    // Keyed by NAME rather than by position in the statement's column list:
    // a column materialized from its DEFAULT is appended to the row and has no
    // position in that list, so an index-keyed rule would silently stop
    // excluding it. The two rules must agree however the cell arrived.
    let excluded_from_value = |name: &str| {
        // Exclude the raw "key" sentinel column (not a named PK column).
        (key_col_name == "key" && name == "key") || name == "ttl"
    };
    // Resolve every row's literals once, then coerce each cell to its declared
    // column type. Unlike the strict, columnar, and timeseries engines, KV has
    // no typed write path: its engine stores the bytes it is handed and the
    // declared schema exists only here, in the catalog. Without this the
    // stored cell's type is whatever the literal happened to resolve to — a
    // fractional literal is an exact `Decimal`, which serializes as a msgpack
    // string — while `RowDescription` advertises the declared numeric type, and
    // the read path can only encode SQL NULL for it. See
    // `declared_type_coerce` for the full rationale.
    let mut coerced_rows: Vec<Vec<(String, SqlValue)>> = Vec::with_capacity(rows_ast.len());
    for row_exprs in rows_ast {
        let mut row: Vec<(String, SqlValue)> = Vec::with_capacity(columns.len());
        for (i, col) in columns.iter().enumerate() {
            let Some(expr) = row_exprs.get(i) else { break };
            row.push((col.clone(), expr_to_sql_value(expr)?));
        }
        materialize_declared_defaults(declared_columns, &mut row)?;
        // The key column is exempt — see `coerce_rows_to_declared_types`.
        coerce_row_to_declared_types(declared_columns, &mut row, Some(key_col_name))?;
        coerced_rows.push(row);
    }
    // KV returns early from every INSERT/UPSERT entry point, so the declared
    // width check lives here rather than at the call sites — otherwise a
    // fourth KV entry point could be added without it. It runs on the coerced
    // values so a literal that only becomes an integer through coercion is
    // still range-checked against its declared width.
    check_declared_int_ranges(declared_columns, &coerced_rows)?;
    check_declared_float_ranges(declared_columns, &coerced_rows)?;
    // `ON CONFLICT DO UPDATE SET col = <literal>` writes through the same
    // untyped KV path as the inserted row, so its literals need the same
    // declared-type coercion.
    coerce_assignments_to_declared_types(
        declared_columns,
        &mut on_conflict_updates,
        Some(key_col_name),
    )?;
    check_declared_int_ranges_in_assignments(declared_columns, &on_conflict_updates)?;
    check_declared_float_ranges_in_assignments(declared_columns, &on_conflict_updates)?;

    // PRIMARY KEY implies NOT NULL. A row that omits the key column or
    // binds it to NULL would commit an empty-keyed entry — unique against
    // nothing and unreadable — instead of raising. Defaults were already
    // materialized above, so a present NULL is an explicit violation and
    // an absent key means the column list never mentioned it.
    for row in &coerced_rows {
        let cell = row.iter().find(|(name, _)| name == key_col_name);
        if cell.is_none() || matches!(cell, Some((_, SqlValue::Null))) {
            return Err(SqlError::NotNullViolation {
                table: table_name.clone(),
                column: key_col_name.to_string(),
            });
        }
    }

    let mut entries = Vec::with_capacity(coerced_rows.len());
    let mut ttl_secs: u64 = 0;
    for row in &coerced_rows {
        // By name, for the same reason the exclusion rule above is: a
        // DEFAULT-materialized key column is appended to the row and has no
        // position in the statement's column list.
        let key_val = match row.iter().find(|(name, _)| name == key_col_name) {
            Some((_, value)) => value.clone(),
            None => SqlValue::String(String::new()),
        };
        if let Some((_, value)) = row.iter().find(|(name, _)| name == "ttl") {
            match value {
                SqlValue::Int(n) => ttl_secs = (*n).max(0) as u64,
                SqlValue::Float(f) => ttl_secs = f.max(0.0) as u64,
                _ => {}
            }
        }
        let value_cols: Vec<(String, SqlValue)> = row
            .iter()
            .filter(|(name, _)| !excluded_from_value(name))
            .cloned()
            .collect();
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

/// Fill in every declared column the statement omitted that carries a DEFAULT.
///
/// The key-value engine stores the bytes it is handed and has no typed write
/// path, so a DEFAULT that is not materialized HERE is materialized nowhere:
/// the catalog would keep the declaration and every read return nothing for it.
/// Documents and columnar rows expand theirs through the same
/// `evaluate_default_expr`, so one expression yields one value on every engine.
///
/// Two rules the ordering encodes:
///
/// - A column the statement SUPPLIED is never touched, and that includes an
///   explicit `NULL`. `NULL` is a value the author chose; overwriting it with
///   the default would make it impossible to store one.
/// - Materialized values are appended BEFORE the caller's declared-type
///   coercion and range checks, so a default is validated exactly like a
///   supplied literal. Filling them in afterwards would make `DEFAULT 999999`
///   on a `SMALLINT` column a way to store a value the same literal is
///   rejected for.
fn materialize_declared_defaults(
    declared_columns: &[ColumnInfo],
    row: &mut Vec<(String, SqlValue)>,
) -> Result<()> {
    for column in declared_columns {
        let Some(default_expr) = column.default.as_deref() else {
            continue;
        };
        if row.iter().any(|(name, _)| name == &column.name) {
            continue;
        }
        let evaluated =
            crate::planner::defaults::evaluate_default_expr(default_expr).map_err(|e| {
                SqlError::Parse {
                    detail: format!("default for column '{}' is invalid: {e}", column.name),
                }
            })?;
        let Some(evaluated) = evaluated else { continue };
        let value = nodedb_value_to_sql_value(&column.name, evaluated)?;
        row.push((column.name.clone(), value));
    }
    Ok(())
}

/// Convert an evaluated default back into the planner's literal type.
///
/// The inverse of `sql_value_to_ndb` in `planner::defaults`, which is the only
/// producer of these values — so every shape the evaluator can emit has an
/// exact counterpart here. Anything else is rejected rather than rendered
/// through `Debug`: a `DEFAULT` that silently stored `Uuid("…")` as its own
/// debug text would be the same class of defect as dropping it entirely, but
/// harder to notice because the column would look populated.
fn nodedb_value_to_sql_value(column: &str, value: nodedb_types::Value) -> Result<SqlValue> {
    Ok(match value {
        nodedb_types::Value::Null => SqlValue::Null,
        nodedb_types::Value::Bool(b) => SqlValue::Bool(b),
        nodedb_types::Value::Integer(i) => SqlValue::Int(i),
        nodedb_types::Value::Float(f) => SqlValue::Float(f),
        nodedb_types::Value::Decimal(d) => SqlValue::Decimal(d),
        nodedb_types::Value::String(s) => SqlValue::String(s),
        nodedb_types::Value::Bytes(b) => SqlValue::Bytes(b),
        nodedb_types::Value::NaiveDateTime(dt) => SqlValue::Timestamp(dt),
        nodedb_types::Value::DateTime(dt) => SqlValue::Timestamptz(dt),
        nodedb_types::Value::Array(items) => SqlValue::Array(
            items
                .into_iter()
                .map(|item| nodedb_value_to_sql_value(column, item))
                .collect::<Result<Vec<_>>>()?,
        ),
        other => {
            return Err(SqlError::Unsupported {
                detail: format!(
                    "default for column '{column}' evaluates to a value with no SQL literal \
                     form: {other:?}"
                ),
            });
        }
    })
}

#[cfg(test)]
mod kv_on_conflict_range_tests {
    use sqlparser::ast::{Expr, Value, ValueWithSpan};
    use sqlparser::tokenizer::Span;

    use super::*;
    use nodedb_types::columnar::{FloatWidth, IntWidth};

    fn string_column(name: &str) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            data_type: SqlDataType::String,
            nullable: true,
            is_primary_key: false,
            default: None,
            raw_type: None,
            int_width: None,
            float_width: None,
        }
    }

    fn int_column(name: &str, width: Option<IntWidth>) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            data_type: SqlDataType::Int64,
            nullable: true,
            is_primary_key: false,
            default: None,
            raw_type: None,
            int_width: width,
            float_width: None,
        }
    }

    fn float_column(name: &str, width: Option<FloatWidth>) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            data_type: SqlDataType::Float64,
            nullable: true,
            is_primary_key: false,
            default: None,
            raw_type: None,
            int_width: None,
            float_width: width,
        }
    }

    fn key_value_expr(key: &str) -> Expr {
        Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(key.to_string()),
            span: Span::empty(),
        })
    }

    /// Build a single-row `INSERT ... ON CONFLICT (key) DO UPDATE SET ...`
    /// plan against a `key TEXT PRIMARY KEY` collection with `n INT` and
    /// `r REAL` columns, and return the result of range-checking `updates`.
    fn plan_with_on_conflict(updates: Vec<(String, SqlExpr)>) -> Result<Vec<SqlPlan>> {
        let declared = [
            string_column("key"),
            int_column("n", Some(IntWidth::I32)),
            float_column("r", Some(FloatWidth::F32)),
        ];
        build_kv_insert_plan(
            "t".to_string(),
            &["key".to_string()],
            &[ast::Parens::with_empty_span(vec![key_value_expr("a")])],
            KvInsertIntent::Put,
            updates,
            Some("key"),
            &declared,
        )
    }

    #[test]
    fn on_conflict_int_beyond_declared_width_is_rejected() {
        let updates = vec![(
            "n".to_string(),
            SqlExpr::Literal(SqlValue::Int(9_876_543_210)),
        )];
        let err = plan_with_on_conflict(updates).expect_err("i32 column must reject overflow");
        assert!(matches!(err, SqlError::IntegerOutOfRange { .. }));
    }

    #[test]
    fn on_conflict_int_in_range_is_accepted() {
        let updates = vec![("n".to_string(), SqlExpr::Literal(SqlValue::Int(42)))];
        plan_with_on_conflict(updates).expect("in-range i32 literal must be accepted");
    }

    #[test]
    fn on_conflict_float_beyond_f32_range_is_rejected() {
        let updates = vec![("r".to_string(), SqlExpr::Literal(SqlValue::Float(1e300)))];
        let err = plan_with_on_conflict(updates).expect_err("real column must reject f32 overflow");
        assert!(matches!(err, SqlError::FloatOutOfRange { .. }));
    }

    #[test]
    fn on_conflict_float_rounding_is_accepted() {
        // Pinned per `check_declared_float_ranges`: narrowing to f32 rounds,
        // it does not reject, so a future change must not tighten this.
        let updates = vec![("r".to_string(), SqlExpr::Literal(SqlValue::Float(1.1)))];
        plan_with_on_conflict(updates).expect("rounding into f32 must not be rejected");
    }
}
