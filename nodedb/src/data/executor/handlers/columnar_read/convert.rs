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
        Value::DateTime(dt) => serde_json::Value::String(dt.to_string()),
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

/// Write a timeseries columnar memtable cell value directly as msgpack bytes.
///
/// Encodes the column value at the given row index directly into `buf`
/// without intermediate decoding. Used by timeseries raw_scan and aggregate
/// handlers that still use the internal `ColumnarMemtable`.
pub(in crate::data::executor) fn emit_column_value(
    buf: &mut Vec<u8>,
    mt: &crate::engine::timeseries::columnar_memtable::ColumnarMemtable,
    col_idx: usize,
    col_type: &crate::engine::timeseries::columnar_memtable::ColumnType,
    col_data: &crate::engine::timeseries::columnar_memtable::ColumnData,
    row_idx: usize,
) {
    use crate::engine::timeseries::columnar_memtable::{
        ColumnData as TsColumnData, ColumnType as TsColumnType,
    };
    match col_type {
        TsColumnType::Timestamp => {
            nodedb_query::msgpack_scan::write_i64(buf, col_data.as_timestamps()[row_idx]);
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
}
