// SPDX-License-Identifier: BUSL-1.1

//! Value conversions: a decoded columnar row → its projected response
//! object, and timeseries columnar cell → raw msgpack for the timeseries scan
//! path.

use std::collections::HashMap;

use nodedb_types::value::Value;

/// Project a decoded columnar row into the scan's response object: column
/// projection, the forced `_ts_system` audit column, and computed
/// (scalar-expression) columns. Shared by the flushed-segment scan, the base
/// memtable scan loop, the in-transaction overlay merge
/// (`merge_overlay_into_columnar_scan`), and the predicate DML row read, so a
/// staged row's object is built identically to a base row's.
///
/// Every cell is carried as the `Value` the row reader typed it as, so a
/// declared `TIMESTAMP` / `TIMESTAMPTZ` cell is an instant here and is
/// written as the instant ext by `value_to_msgpack`, from the live memtable
/// and from a flushed segment alike. The bitemporal `_ts_system`,
/// `_ts_valid_from`, `_ts_valid_until` columns are declared `Int64` and stay
/// integers: they hold epoch milliseconds with `i64::MIN` / `i64::MAX` as
/// the unbounded sentinels, which no instant can carry.
pub(in crate::data::executor) fn row_to_projected_value(
    row: &[Value],
    schema: &nodedb_types::columnar::ColumnarSchema,
    projection: &[String],
    computed_cols: &[crate::bridge::expr_eval::ComputedColumn],
    all_versions: bool,
) -> crate::Result<Value> {
    let mut obj: HashMap<String, Value> = HashMap::with_capacity(schema.columns.len());
    for (i, col_def) in schema.columns.iter().enumerate() {
        let force_system_col =
            all_versions && col_def.name == nodedb_types::columnar::schema::TS_SYSTEM;
        if !projection.is_empty()
            && !force_system_col
            && !projection.iter().any(|p| p == &col_def.name)
            && !computed_cols.iter().any(|cc| cc.alias == col_def.name)
        {
            continue;
        }
        if let Some(cell) = row.get(i) {
            obj.insert(col_def.name.clone(), cell.clone());
        }
    }
    if !computed_cols.is_empty() {
        let doc_val = Value::Object(obj.clone());
        for cc in computed_cols {
            if matches!(obj.get(&cc.alias), Some(v) if !matches!(v, Value::Null)) {
                continue;
            }
            // A computed column is projection-shaped: a division/modulo-by-
            // zero fails the whole scan rather than silently materializing
            // NULL into the response row.
            let v = cc.expr.eval(&doc_val)?;
            obj.insert(cc.alias.clone(), v);
        }
        if !projection.is_empty() {
            obj.retain(|k, _| {
                projection.iter().any(|p| p == k)
                    || computed_cols.iter().any(|cc| &cc.alias == k)
                    || (all_versions && k == nodedb_types::columnar::schema::TS_SYSTEM)
            });
        }
    }
    Ok(Value::Object(obj))
}

/// Write a timeseries columnar memtable cell value directly as msgpack bytes.
///
/// Encodes the column value at the given row index directly into `buf`
/// without intermediate decoding. Used by timeseries raw_scan and aggregate
/// handlers that still use the internal `ColumnarMemtable`.
///
/// A time cell is written as its column's kind says: an instant column
/// yields a typed instant ext, a `Millis` column the integer stored. `Err`
/// when a stored millisecond count overflows the microsecond range an
/// instant carries; the cell is never written wrapped.
pub(in crate::data::executor) fn emit_column_value(
    buf: &mut Vec<u8>,
    mt: &crate::engine::timeseries::columnar_memtable::ColumnarMemtable,
    col_idx: usize,
    col_type: &crate::engine::timeseries::columnar_memtable::ColumnType,
    col_data: &crate::engine::timeseries::columnar_memtable::ColumnData,
    row_idx: usize,
) -> crate::Result<()> {
    use crate::engine::timeseries::columnar_memtable::{
        ColumnData as TsColumnData, ColumnType as TsColumnType,
    };
    match col_type {
        TsColumnType::Timestamp(kind) => {
            let millis = col_data.as_timestamps()[row_idx];
            write_time_cell(buf, *kind, millis)?;
        }
        TsColumnType::Float64 => {
            let v = col_data.as_f64()[row_idx];
            if v.is_finite() {
                nodedb_query::msgpack_scan::write_f64(buf, v);
            } else {
                nodedb_query::msgpack_scan::write_null(buf);
            }
        }
        TsColumnType::Symbol => {
            if let TsColumnData::Symbol(ids) = col_data {
                let sym_id = ids[row_idx];
                if let Some(s) = mt.symbol_dict(col_idx).and_then(|dict| dict.get(sym_id)) {
                    nodedb_query::msgpack_scan::write_str(buf, s);
                } else {
                    nodedb_query::msgpack_scan::write_null(buf);
                }
            } else {
                nodedb_query::msgpack_scan::write_null(buf);
            }
        }
        TsColumnType::Int64 => {
            if let TsColumnData::Int64(vals) = col_data {
                nodedb_query::msgpack_scan::write_i64(buf, vals[row_idx]);
            } else {
                nodedb_query::msgpack_scan::write_null(buf);
            }
        }
    }
    Ok(())
}

/// Epoch microseconds for a stored millisecond count.
///
/// `Err` when `millis * 1000` overflows `i64`: the stored value cannot be
/// expressed as an instant, and wrapping it would hand back a different one.
fn instant_micros(millis: i64) -> crate::Result<i64> {
    nodedb_types::NdbDateTime::from_millis(millis)
        .map(|dt| dt.micros)
        .map_err(|e| crate::Error::Internal {
            detail: format!("timeseries time cell at {millis} ms: {e}"),
        })
}

/// Write a stored millisecond time cell as the value its kind denotes.
pub(in crate::data::executor) fn write_time_cell(
    buf: &mut Vec<u8>,
    kind: crate::engine::timeseries::columnar_memtable::TimeKind,
    millis: i64,
) -> crate::Result<()> {
    use crate::engine::timeseries::columnar_memtable::TimeKind;
    match kind {
        TimeKind::Instant(k) => nodedb_types::write_instant(buf, k, instant_micros(millis)?),
        TimeKind::Millis => nodedb_query::msgpack_scan::write_i64(buf, millis),
    }
    Ok(())
}

/// The rmpv cell for a stored millisecond time value, typed by its kind.
///
/// An instant column yields the ten-byte instant ext, a `Millis` column the
/// integer stored.
pub(in crate::data::executor) fn rmpv_time_cell(
    kind: crate::engine::timeseries::columnar_memtable::TimeKind,
    millis: i64,
) -> crate::Result<rmpv::Value> {
    use crate::engine::timeseries::columnar_memtable::TimeKind;
    Ok(match kind {
        TimeKind::Instant(k) => {
            rmpv::Value::Ext(k.ext_type(), instant_micros(millis)?.to_be_bytes().to_vec())
        }
        TimeKind::Millis => rmpv::Value::Integer(millis.into()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::timeseries::columnar_memtable::{
        ColumnData, ColumnType, ColumnarMemtable, ColumnarMemtableConfig, ColumnarSchema, TimeKind,
    };
    use nodedb_types::InstantKind;

    const MS: i64 = 1_583_402_400_000;

    fn memtable(kind: TimeKind) -> ColumnarMemtable {
        let schema = ColumnarSchema {
            columns: vec![
                ("ts".into(), ColumnType::Timestamp(kind)),
                ("v".into(), ColumnType::Float64),
            ],
            timestamp_idx: 0,
            codecs: vec![nodedb_codec::ColumnCodec::Auto; 2],
        };
        ColumnarMemtable::new(schema, ColumnarMemtableConfig::default())
    }

    fn emit(kind: TimeKind, millis: i64) -> crate::Result<Vec<u8>> {
        let mt = memtable(kind);
        let data = ColumnData::Timestamp(vec![millis]);
        let mut buf = Vec::new();
        emit_column_value(&mut buf, &mt, 0, &ColumnType::Timestamp(kind), &data, 0)?;
        Ok(buf)
    }

    #[test]
    fn an_instant_column_emits_a_typed_instant_ext() {
        let buf = emit(TimeKind::Instant(InstantKind::Naive), MS).expect("emit");
        assert_eq!(
            nodedb_types::read_instant(&buf, 0),
            Some((InstantKind::Naive, MS * 1000))
        );
        let buf = emit(TimeKind::Instant(InstantKind::Utc), MS).expect("emit");
        assert_eq!(
            nodedb_types::read_instant(&buf, 0),
            Some((InstantKind::Utc, MS * 1000))
        );
    }

    #[test]
    fn a_millis_column_emits_the_integer_stored() {
        let buf = emit(TimeKind::Millis, MS).expect("emit");
        let mut expected = Vec::new();
        nodedb_query::msgpack_scan::write_i64(&mut expected, MS);
        assert_eq!(buf, expected);
    }

    /// A projected row carries a declared `TIMESTAMP` cell as the instant the
    /// row reader typed it, and the `_ts_system` audit column as the integer
    /// it is declared as.
    #[test]
    fn a_projected_row_keeps_instant_cells_and_integer_system_time() {
        use nodedb_types::NdbDateTime;
        use nodedb_types::columnar::schema::TS_SYSTEM;
        use nodedb_types::columnar::{ColumnDef, ColumnType};

        let schema = nodedb_types::columnar::ColumnarSchema::new(vec![
            ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
            ColumnDef::required("at", ColumnType::Timestamp),
            ColumnDef::required(TS_SYSTEM, ColumnType::Int64),
        ])
        .expect("valid schema");
        let at = Value::NaiveDateTime(NdbDateTime::from_micros(MS * 1000));
        let row = [Value::Integer(1), at.clone(), Value::Integer(MS)];

        let Value::Object(all) =
            row_to_projected_value(&row, &schema, &[], &[], false).expect("project")
        else {
            panic!("a projected row is an object");
        };
        assert_eq!(all.get("at"), Some(&at));
        assert_eq!(all.get(TS_SYSTEM), Some(&Value::Integer(MS)));

        let projection = ["at".to_string()];
        let Value::Object(audit) =
            row_to_projected_value(&row, &schema, &projection, &[], true).expect("project")
        else {
            panic!("a projected row is an object");
        };
        assert_eq!(audit.get("at"), Some(&at));
        assert_eq!(
            audit.get(TS_SYSTEM),
            Some(&Value::Integer(MS)),
            "an all-versions read forces the system-time column into the projection"
        );
        assert!(!audit.contains_key("id"));
    }

    #[test]
    fn a_millisecond_count_past_the_microsecond_range_is_an_error() {
        let err = emit(TimeKind::Instant(InstantKind::Naive), i64::MAX)
            .expect_err("i64::MAX ms cannot be expressed in microseconds");
        assert!(err.to_string().contains("time cell"), "{err}");
        assert!(emit(TimeKind::Millis, i64::MAX).is_ok());
    }
}
