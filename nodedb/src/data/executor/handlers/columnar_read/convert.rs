// SPDX-License-Identifier: BUSL-1.1

//! Value conversions: engine Value → JSON for response encoding, and
//! timeseries columnar cell → raw msgpack for the timeseries scan path.

/// Convert a `nodedb_types::Value` to `serde_json::Value` for response encoding.
pub(in crate::data::executor) fn value_to_json(
    val: &nodedb_types::value::Value,
) -> serde_json::Value {
    use nodedb_types::value::Value;
    match val {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Integer(i) => serde_json::Value::Number((*i).into()),
        Value::Float(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::String(s) => serde_json::Value::String(s.clone()),
        Value::DateTime(dt) | Value::NaiveDateTime(dt) => serde_json::Value::String(dt.to_string()),
        Value::Decimal(d) => serde_json::Value::String(d.to_string()),
        Value::Uuid(s) => serde_json::Value::String(s.clone()),
        Value::Bytes(b) => {
            use base64::Engine;
            serde_json::Value::String(base64::engine::general_purpose::STANDARD.encode(b))
        }
        Value::Array(arr) => serde_json::Value::Array(arr.iter().map(value_to_json).collect()),
        Value::Geometry(g) => serde_json::to_value(g).unwrap_or(serde_json::Value::Null),
        Value::Object(map) => {
            let obj: serde_json::Map<String, serde_json::Value> = map
                .iter()
                .map(|(k, v)| (k.clone(), value_to_json(v)))
                .collect();
            serde_json::Value::Object(obj)
        }
        _ => serde_json::Value::Null,
    }
}

/// Project a decoded columnar row into the scan's response JSON shape:
/// column projection, the forced `_ts_system` audit column, and computed
/// (scalar-expression) columns. Shared by the base memtable scan loop and
/// the in-transaction overlay merge (`merge_overlay_into_columnar_scan`) so
/// a staged row's JSON is built identically to a base row's.
pub(in crate::data::executor) fn row_to_projected_json(
    row: &[nodedb_types::value::Value],
    schema: &nodedb_types::columnar::ColumnarSchema,
    projection: &[String],
    computed_cols: &[crate::bridge::expr_eval::ComputedColumn],
    all_versions: bool,
) -> crate::Result<serde_json::Value> {
    let mut obj = serde_json::Map::new();
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
        if i < row.len() {
            obj.insert(col_def.name.clone(), value_to_json(&row[i]));
        }
    }
    if !computed_cols.is_empty() {
        let doc_val = nodedb_types::Value::from(serde_json::Value::Object(obj.clone()));
        for cc in computed_cols {
            let existing = obj.get(&cc.alias);
            if matches!(existing, Some(v) if !v.is_null()) {
                continue;
            }
            // A computed column is projection-shaped: a division/modulo-by-
            // zero fails the whole scan rather than silently materializing
            // NULL into the response row.
            let v = cc.expr.eval(&doc_val)?;
            obj.insert(cc.alias.clone(), serde_json::Value::from(v));
        }
        if !projection.is_empty() {
            obj.retain(|k, _| {
                projection.iter().any(|p| p == k)
                    || computed_cols.iter().any(|cc| &cc.alias == k)
                    || (all_versions && k == nodedb_types::columnar::schema::TS_SYSTEM)
            });
        }
    }
    Ok(serde_json::Value::Object(obj))
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

    #[test]
    fn a_millisecond_count_past_the_microsecond_range_is_an_error() {
        let err = emit(TimeKind::Instant(InstantKind::Naive), i64::MAX)
            .expect_err("i64::MAX ms cannot be expressed in microseconds");
        assert!(err.to_string().contains("time cell"), "{err}");
        assert!(emit(TimeKind::Millis, i64::MAX).is_ok());
    }
}
