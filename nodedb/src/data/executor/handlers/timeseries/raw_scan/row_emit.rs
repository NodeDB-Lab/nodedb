// SPDX-License-Identifier: BUSL-1.1

//! Row emission helpers — build `rmpv::Value` directly, plus value conversions.

use std::collections::HashMap;

use nodedb_types::columnar::schema::TS_SYSTEM;

use crate::engine::timeseries::columnar_memtable::{ColumnData, ColumnType};

/// Extract the `_ts_system` value from an rmpv-encoded row for audit-log
/// ordering. Rows without the column sort first (treated as `i64::MIN`).
pub(super) fn rmpv_system_time(row: &rmpv::Value) -> i64 {
    let rmpv::Value::Map(entries) = row else {
        return i64::MIN;
    };
    for (k, v) in entries {
        if let rmpv::Value::String(s) = k
            && s.as_str() == Some(TS_SYSTEM)
            && let rmpv::Value::Integer(i) = v
        {
            return i.as_i64().unwrap_or(i64::MIN);
        }
    }
    i64::MIN
}

/// Emit the memtable rows at `row_indices`, in the order given.
///
/// The read-back a write's `RETURNING` projection uses. It deliberately goes
/// through [`emit_memtable_row`] — the same function `SELECT` uses — so the two
/// cannot disagree about how a stored cell renders. The rules that would
/// otherwise have to be restated are real: a float field the line omitted is
/// stored as `NaN` and must come back as SQL NULL, and so must a symbol whose
/// dictionary entry is missing. A projection written over the ingest-side
/// `ColumnValue`s would have had to repeat both, and repeating them is how two
/// shapers drift.
///
/// An index past the memtable's row count is skipped rather than panicking:
/// the caller reads indices recorded before a flush, and a flush in between
/// would invalidate them. Callers must project before flushing.
pub(in crate::data::executor) fn emit_memtable_rows_at(
    mt: &crate::engine::timeseries::columnar_memtable::ColumnarMemtable,
    row_indices: &[usize],
) -> Vec<rmpv::Value> {
    let schema = mt.schema().clone();
    let columns: Vec<_> = schema
        .columns
        .iter()
        .enumerate()
        .map(|(i, (name, ty))| (i, name, ty, mt.column(i)))
        .collect();
    let row_count = mt.row_count() as usize;
    row_indices
        .iter()
        .filter(|&&idx| idx < row_count)
        .map(|&idx| emit_memtable_row(mt, &columns, idx))
        .collect()
}

/// Emit a single row from the memtable as rmpv::Value::Map.
pub(super) fn emit_memtable_row(
    mt: &crate::engine::timeseries::columnar_memtable::ColumnarMemtable,
    columns: &[(usize, &String, &ColumnType, &ColumnData)],
    idx: usize,
) -> rmpv::Value {
    // Build raw msgpack bytes, then decode to rmpv::Value.
    let mut buf = Vec::with_capacity(columns.len() * 32);
    nodedb_query::msgpack_scan::write_map_header(&mut buf, columns.len());
    for (col_idx, col_name, col_type, col_data) in columns {
        nodedb_query::msgpack_scan::write_str(&mut buf, col_name);
        crate::data::executor::handlers::columnar_read::emit_column_value(
            &mut buf, mt, *col_idx, col_type, col_data, idx,
        );
    }
    crate::util::bounded_msgpack::read_value(&buf).unwrap_or(rmpv::Value::Nil)
}

/// Emit a single row from a disk partition as rmpv::Value::Map.
pub(super) fn emit_partition_row(
    schema: &[(String, ColumnType)],
    col_data: &[Option<ColumnData>],
    sym_dicts: &HashMap<usize, nodedb_types::timeseries::SymbolDictionary>,
    idx: usize,
) -> rmpv::Value {
    let mut fields: Vec<(rmpv::Value, rmpv::Value)> = Vec::with_capacity(schema.len());
    for (col_i, (col_name, col_type)) in schema.iter().enumerate() {
        // A column whose file could not be read is emitted as NULL, never
        // skipped. Skipping it changed the row's COLUMN SET rather than one
        // cell's value, so `SELECT *` on the same row returned different
        // columns before and after a flush — the memtable path below always
        // emits every column. A missing value is NULL; it is not a missing
        // column.
        let Some(data) = &col_data[col_i] else {
            fields.push((
                rmpv::Value::String(col_name.as_str().into()),
                rmpv::Value::Nil,
            ));
            continue;
        };
        let val = match col_type {
            ColumnType::Timestamp => rmpv::Value::Integer(data.as_timestamps()[idx].into()),
            ColumnType::Float64 => {
                let v = data.as_f64()[idx];
                if v.is_nan() {
                    rmpv::Value::Nil
                } else {
                    rmpv::Value::F64(v)
                }
            }
            ColumnType::Int64 => {
                if let ColumnData::Int64(vals) = data {
                    rmpv::Value::Integer(vals[idx].into())
                } else {
                    rmpv::Value::Nil
                }
            }
            ColumnType::Symbol => {
                if let ColumnData::Symbol(ids) = data {
                    sym_dicts
                        .get(&col_i)
                        .and_then(|dict| dict.get(ids[idx]))
                        .map(|s| rmpv::Value::String(s.into()))
                        .unwrap_or(rmpv::Value::Nil)
                } else {
                    rmpv::Value::Nil
                }
            }
        };
        fields.push((rmpv::Value::String(col_name.as_str().into()), val));
    }
    rmpv::Value::Map(fields)
}

/// Extract timestamp from a row (first integer field) for sort-merge.
pub(super) fn extract_timestamp(row: &rmpv::Value) -> i64 {
    if let rmpv::Value::Map(fields) = row {
        for (_, v) in fields {
            if let rmpv::Value::Integer(n) = v {
                return n.as_i64().unwrap_or(0);
            }
        }
    }
    0
}

/// Apply computed column expressions to an rmpv row.
///
/// Converts the row to `nodedb_types::Value` for expression evaluation,
/// then produces a new row containing only the computed columns.
/// When computed columns are present, the output contains ONLY
/// computed columns (matching Document engine behavior for projection).
pub(super) fn apply_computed_columns_rmpv(
    row: rmpv::Value,
    computed_cols: &[crate::bridge::expr_eval::ComputedColumn],
) -> crate::Result<rmpv::Value> {
    let doc = rmpv_to_nodedb_value(&row);
    let mut fields: Vec<(rmpv::Value, rmpv::Value)> = Vec::with_capacity(computed_cols.len());
    for cc in computed_cols {
        // A computed column is projection-shaped: a division/modulo-by-zero
        // fails the whole scan instead of silently materializing NULL into
        // the response.
        let result = cc.expr.eval(&doc)?;
        fields.push((
            rmpv::Value::String(cc.alias.as_str().into()),
            nodedb_value_to_rmpv(&result),
        ));
    }
    Ok(rmpv::Value::Map(fields))
}

/// Convert rmpv row to nodedb_types::Value for expression evaluation.
pub(super) fn rmpv_to_nodedb_value(row: &rmpv::Value) -> nodedb_types::Value {
    match row {
        rmpv::Value::Map(fields) => {
            let mut map = std::collections::HashMap::new();
            for (k, v) in fields {
                let key = match k {
                    rmpv::Value::String(s) => s.as_str().unwrap_or("").to_string(),
                    _ => continue,
                };
                let val = match v {
                    rmpv::Value::Integer(n) => {
                        nodedb_types::Value::Integer(n.as_i64().unwrap_or(0))
                    }
                    rmpv::Value::F64(f) => nodedb_types::Value::Float(*f),
                    rmpv::Value::String(s) => {
                        nodedb_types::Value::String(s.as_str().unwrap_or("").to_string())
                    }
                    rmpv::Value::Nil => nodedb_types::Value::Null,
                    rmpv::Value::Boolean(b) => nodedb_types::Value::Bool(*b),
                    _ => nodedb_types::Value::Null,
                };
                map.insert(key, val);
            }
            nodedb_types::Value::Object(map)
        }
        _ => nodedb_types::Value::Null,
    }
}

/// Convert nodedb_types::Value back to rmpv::Value for response encoding.
pub(super) fn nodedb_value_to_rmpv(v: &nodedb_types::Value) -> rmpv::Value {
    match v {
        nodedb_types::Value::Integer(n) => rmpv::Value::Integer((*n).into()),
        nodedb_types::Value::Float(f) => rmpv::Value::F64(*f),
        nodedb_types::Value::String(s) => rmpv::Value::String(s.as_str().into()),
        nodedb_types::Value::Bool(b) => rmpv::Value::Boolean(*b),
        nodedb_types::Value::Null => rmpv::Value::Nil,
        _ => rmpv::Value::Nil,
    }
}

/// Rescale every declared-instant cell of `rows` from the milliseconds the
/// memtable and the partitions store to the epoch microseconds a `TIMESTAMP`
/// cell carries on the wire.
///
/// The engine's own unit stays milliseconds: partition ranges, retention,
/// `time_bucket` and every scan predicate read it. The scale therefore runs
/// once, as rows leave the scan — after filtering, sorting and computed
/// columns — so nothing inside the engine sees the wire unit.
///
/// `instant_columns` comes from `CoreLoop::ts_instant_columns`, which lists
/// the columns declared `TIMESTAMP` or `TIMESTAMPTZ`. A `BIGINT TIME_KEY`
/// lives in the same millisecond column and is not in that list, so it keeps
/// the integer the client inserted.
///
/// SQL NULL cells pass through untouched. A stored value that cannot be
/// expressed in microseconds fails the read rather than wrapping.
pub(in crate::data::executor) fn scale_instant_cells(
    rows: &mut [rmpv::Value],
    instant_columns: &[String],
) -> crate::Result<()> {
    if instant_columns.is_empty() {
        return Ok(());
    }
    for row in rows.iter_mut() {
        let rmpv::Value::Map(fields) = row else {
            continue;
        };
        for (key, value) in fields.iter_mut() {
            let Some(name) = key.as_str() else { continue };
            if !instant_columns.iter().any(|c| c == name) {
                continue;
            }
            let rmpv::Value::Integer(stored) = value else {
                continue;
            };
            let millis = stored.as_i64().ok_or_else(|| crate::Error::Internal {
                detail: format!(
                    "timeseries column {name} holds {stored}, which is not a millisecond \
                     count an instant can be read from"
                ),
            })?;
            let micros = nodedb_types::NdbDateTime::from_millis(millis)
                .map_err(|e| crate::Error::Internal {
                    detail: format!("timeseries column {name} at {millis} ms: {e}"),
                })?
                .micros;
            *value = rmpv::Value::Integer(micros.into());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::scale_instant_cells;

    fn row(cells: &[(&str, i64)]) -> rmpv::Value {
        rmpv::Value::Map(
            cells
                .iter()
                .map(|(k, v)| {
                    (
                        rmpv::Value::String((*k).into()),
                        rmpv::Value::Integer((*v).into()),
                    )
                })
                .collect(),
        )
    }

    fn cell(row: &rmpv::Value, name: &str) -> Option<i64> {
        let rmpv::Value::Map(fields) = row else {
            return None;
        };
        fields
            .iter()
            .find(|(k, _)| k.as_str() == Some(name))
            .and_then(|(_, v)| v.as_i64())
    }

    /// A declared `TIMESTAMP` column is read as epoch microseconds, so the
    /// millisecond value storage holds is scaled on the way out. 2020-03-05
    /// stays 2020-03-05 instead of landing 50 years earlier.
    #[test]
    fn a_declared_instant_column_leaves_the_scan_in_microseconds() {
        let mut rows = vec![row(&[("captured_at", 1_583_402_400_000)])];
        scale_instant_cells(&mut rows, &["captured_at".to_string()]).expect("scale");
        assert_eq!(cell(&rows[0], "captured_at"), Some(1_583_402_400_000_000));
    }

    /// A `BIGINT TIME_KEY` shares the millisecond storage column but is not a
    /// declared instant, so its value is handed back exactly as inserted.
    #[test]
    fn a_column_that_is_not_a_declared_instant_keeps_its_value() {
        let mut rows = vec![row(&[("ts", 1000), ("n", 7)])];
        scale_instant_cells(&mut rows, &["other".to_string()]).expect("scale");
        assert_eq!(cell(&rows[0], "ts"), Some(1000));
        assert_eq!(cell(&rows[0], "n"), Some(7));
    }

    /// A NULL instant cell stays NULL — there is no instant to scale.
    #[test]
    fn a_null_instant_cell_passes_through() {
        let mut rows = vec![rmpv::Value::Map(vec![(
            rmpv::Value::String("captured_at".into()),
            rmpv::Value::Nil,
        )])];
        scale_instant_cells(&mut rows, &["captured_at".to_string()]).expect("scale");
        assert_eq!(cell(&rows[0], "captured_at"), None);
    }

    /// A stored millisecond count past the microsecond range fails the read.
    /// Wrapping it would hand back an instant that is not the stored one.
    #[test]
    fn a_millisecond_value_beyond_the_microsecond_range_fails_the_read() {
        let mut rows = vec![row(&[("captured_at", i64::MAX)])];
        let err = scale_instant_cells(&mut rows, &["captured_at".to_string()])
            .expect_err("i64::MAX ms cannot be expressed in microseconds");
        assert!(
            err.to_string().contains("captured_at"),
            "the error must name the column: {err}"
        );
    }
}
