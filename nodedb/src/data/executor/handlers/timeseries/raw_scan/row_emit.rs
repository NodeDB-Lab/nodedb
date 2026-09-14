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
