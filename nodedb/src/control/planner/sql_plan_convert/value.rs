//! Value conversion utilities: SqlValue ↔ nodedb_types::Value, msgpack encoding,
//! time range extraction, and column default evaluation.

use nodedb_sql::types::{Filter, FilterExpr, SqlExpr, SqlValue};

pub(super) fn sql_value_to_nodedb_value(v: &SqlValue) -> nodedb_types::Value {
    match v {
        SqlValue::Int(i) => nodedb_types::Value::Integer(*i),
        SqlValue::Float(f) => nodedb_types::Value::Float(*f),
        SqlValue::String(s) => nodedb_types::Value::String(s.clone()),
        SqlValue::Bool(b) => nodedb_types::Value::Bool(*b),
        SqlValue::Null => nodedb_types::Value::Null,
        SqlValue::Array(arr) => {
            nodedb_types::Value::Array(arr.iter().map(sql_value_to_nodedb_value).collect())
        }
        SqlValue::Bytes(b) => nodedb_types::Value::Bytes(b.clone()),
    }
}

pub(super) fn sql_value_to_string(v: &SqlValue) -> String {
    match v {
        SqlValue::String(s) => s.clone(),
        SqlValue::Int(i) => i.to_string(),
        SqlValue::Float(f) => f.to_string(),
        SqlValue::Bool(b) => b.to_string(),
        _ => String::new(),
    }
}

pub(super) fn sql_value_to_bytes(v: &SqlValue) -> Vec<u8> {
    match v {
        SqlValue::String(s) => s.as_bytes().to_vec(),
        SqlValue::Bytes(b) => b.clone(),
        SqlValue::Int(i) => i.to_string().as_bytes().to_vec(),
        _ => sql_value_to_string(v).into_bytes(),
    }
}

/// Encode a SQL value as standard msgpack for field-level updates.
pub(super) fn sql_value_to_msgpack(v: &SqlValue) -> Vec<u8> {
    let mut buf = Vec::with_capacity(16);
    write_msgpack_value(&mut buf, v);
    buf
}

/// Convert SqlValue to JSON — used only at wire boundaries (e.g. timeseries ingest).
/// For internal expression evaluation, use `sql_value_to_nodedb_value` instead.
fn sql_value_to_json(v: &SqlValue) -> serde_json::Value {
    match v {
        SqlValue::Int(i) => serde_json::Value::Number((*i).into()),
        SqlValue::Float(f) => serde_json::json!(*f),
        SqlValue::String(s) => serde_json::Value::String(s.clone()),
        SqlValue::Bool(b) => serde_json::Value::Bool(*b),
        SqlValue::Null => serde_json::Value::Null,
        SqlValue::Array(arr) => {
            serde_json::Value::Array(arr.iter().map(sql_value_to_json).collect())
        }
        SqlValue::Bytes(b) => serde_json::Value::String(base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            b,
        )),
    }
}

// ── Msgpack encoding ──

pub(super) fn row_to_msgpack(row: &[(String, SqlValue)]) -> crate::Result<Vec<u8>> {
    // Write standard msgpack map directly from SqlValue — no JSON or zerompk intermediary.
    let mut buf = Vec::with_capacity(row.len() * 32);
    write_msgpack_map_header(&mut buf, row.len());
    for (key, val) in row {
        write_msgpack_str(&mut buf, key);
        write_msgpack_value(&mut buf, val);
    }
    Ok(buf)
}

pub(super) fn write_msgpack_map_header(buf: &mut Vec<u8>, len: usize) {
    if len < 16 {
        buf.push(0x80 | len as u8);
    } else if len <= u16::MAX as usize {
        buf.push(0xDE);
        buf.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        buf.push(0xDF);
        buf.extend_from_slice(&(len as u32).to_be_bytes());
    }
}

pub(super) fn write_msgpack_str(buf: &mut Vec<u8>, s: &str) {
    let bytes = s.as_bytes();
    let len = bytes.len();
    if len < 32 {
        buf.push(0xA0 | len as u8);
    } else if len <= u8::MAX as usize {
        buf.push(0xD9);
        buf.push(len as u8);
    } else if len <= u16::MAX as usize {
        buf.push(0xDA);
        buf.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        buf.push(0xDB);
        buf.extend_from_slice(&(len as u32).to_be_bytes());
    }
    buf.extend_from_slice(bytes);
}

pub(super) fn write_msgpack_value(buf: &mut Vec<u8>, val: &SqlValue) {
    match val {
        SqlValue::Null => buf.push(0xC0),
        SqlValue::Bool(true) => buf.push(0xC3),
        SqlValue::Bool(false) => buf.push(0xC2),
        SqlValue::Int(i) => {
            let i = *i;
            if (0..=127).contains(&i) {
                buf.push(i as u8);
            } else if (-32..0).contains(&i) {
                buf.push(i as u8); // negative fixint
            } else if i >= i8::MIN as i64 && i <= i8::MAX as i64 {
                buf.push(0xD0);
                buf.push(i as i8 as u8);
            } else if i >= i16::MIN as i64 && i <= i16::MAX as i64 {
                buf.push(0xD1);
                buf.extend_from_slice(&(i as i16).to_be_bytes());
            } else if i >= i32::MIN as i64 && i <= i32::MAX as i64 {
                buf.push(0xD2);
                buf.extend_from_slice(&(i as i32).to_be_bytes());
            } else {
                buf.push(0xD3);
                buf.extend_from_slice(&i.to_be_bytes());
            }
        }
        SqlValue::Float(f) => {
            buf.push(0xCB);
            buf.extend_from_slice(&f.to_be_bytes());
        }
        SqlValue::String(s) => write_msgpack_str(buf, s),
        SqlValue::Array(arr) => {
            let len = arr.len();
            if len < 16 {
                buf.push(0x90 | len as u8);
            } else if len <= u16::MAX as usize {
                buf.push(0xDC);
                buf.extend_from_slice(&(len as u16).to_be_bytes());
            } else {
                buf.push(0xDD);
                buf.extend_from_slice(&(len as u32).to_be_bytes());
            }
            for item in arr {
                write_msgpack_value(buf, item);
            }
        }
        SqlValue::Bytes(b) => {
            let len = b.len();
            if len <= u8::MAX as usize {
                buf.push(0xC4);
                buf.push(len as u8);
            } else if len <= u16::MAX as usize {
                buf.push(0xC5);
                buf.extend_from_slice(&(len as u16).to_be_bytes());
            } else {
                buf.push(0xC6);
                buf.extend_from_slice(&(len as u32).to_be_bytes());
            }
            buf.extend_from_slice(b);
        }
    }
}

pub(super) fn assignments_to_bytes(
    assignments: &[(String, SqlExpr)],
) -> crate::Result<Vec<(String, Vec<u8>)>> {
    let mut result = Vec::new();
    for (field, expr) in assignments {
        let bytes = match expr {
            SqlExpr::Literal(v) => sql_value_to_msgpack(v),
            _ => {
                // Non-literal expression — encode as string.
                let mut buf = Vec::new();
                write_msgpack_str(&mut buf, &format!("{expr:?}"));
                buf
            }
        };
        result.push((field.clone(), bytes));
    }
    Ok(result)
}

pub(super) fn rows_to_msgpack_array(
    rows: &[&Vec<(String, SqlValue)>],
    column_defaults: &[(String, String)],
) -> crate::Result<Vec<u8>> {
    let arr: Vec<nodedb_types::Value> = rows
        .iter()
        .map(|row| {
            let mut map = std::collections::HashMap::new();
            for (key, val) in row.iter() {
                map.insert(key.clone(), sql_value_to_nodedb_value(val));
            }
            // Apply column defaults for missing fields.
            for (col_name, default_expr) in column_defaults {
                if !map.contains_key(col_name)
                    && let Some(val) = evaluate_default_expr(default_expr)
                {
                    map.insert(col_name.clone(), val);
                }
            }
            nodedb_types::Value::Object(map)
        })
        .collect();
    let val = nodedb_types::Value::Array(arr);
    nodedb_types::value_to_msgpack(&val).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("columnar row batch: {e}"),
    })
}

pub(super) fn serialize_ingest_rows(rows: &[Vec<(String, SqlValue)>]) -> crate::Result<Vec<u8>> {
    let json_rows: Vec<serde_json::Value> = rows
        .iter()
        .map(|row| {
            let mut map = serde_json::Map::new();
            for (key, val) in row {
                map.insert(key.clone(), sql_value_to_json(val));
            }
            serde_json::Value::Object(map)
        })
        .collect();
    sonic_rs::to_vec(&json_rows).map_err(|e| crate::Error::Serialization {
        format: "json".into(),
        detail: format!("timeseries ingest rows: {e}"),
    })
}

// ── Column default evaluation ──

/// Evaluate a column DEFAULT expression at insert time.
///
/// Supports ID generation functions and literal values:
///   - `UUID_V7` / `UUIDV7` → time-sortable UUID v7
///   - `UUID_V4` / `UUIDV4` / `UUID` → random UUID v4
///   - `ULID` → time-sortable ULID
///   - `CUID2` → collision-resistant unique ID
///   - `NANOID` → URL-friendly 21-char ID
///   - `NANOID(N)` → URL-friendly N-char ID
///   - Integer/float literals → numeric values
///   - Quoted strings → string values
fn evaluate_default_expr(expr: &str) -> Option<nodedb_types::Value> {
    let upper = expr.trim().to_uppercase();
    match upper.as_str() {
        "UUID_V7" | "UUIDV7" => Some(nodedb_types::Value::String(nodedb_types::id_gen::uuid_v7())),
        "UUID_V4" | "UUIDV4" | "UUID" => {
            Some(nodedb_types::Value::String(nodedb_types::id_gen::uuid_v4()))
        }
        "ULID" => Some(nodedb_types::Value::String(nodedb_types::id_gen::ulid())),
        "CUID2" => Some(nodedb_types::Value::String(nodedb_types::id_gen::cuid2())),
        "NANOID" => Some(nodedb_types::Value::String(nodedb_types::id_gen::nanoid())),
        _ => {
            // NANOID(N) — custom length
            if upper.starts_with("NANOID(") && upper.ends_with(')') {
                let len_str = &upper[7..upper.len() - 1];
                if let Ok(len) = len_str.parse::<usize>() {
                    return Some(nodedb_types::Value::String(
                        nodedb_types::id_gen::nanoid_with_length(len),
                    ));
                }
            }
            // CUID2(N) — custom length
            if upper.starts_with("CUID2(") && upper.ends_with(')') {
                let len_str = &upper[6..upper.len() - 1];
                if let Ok(len) = len_str.parse::<usize>() {
                    return Some(nodedb_types::Value::String(
                        nodedb_types::id_gen::cuid2_with_length(len),
                    ));
                }
            }
            // Numeric literal
            if let Ok(i) = expr.trim().parse::<i64>() {
                return Some(nodedb_types::Value::Integer(i));
            }
            if let Ok(f) = expr.trim().parse::<f64>() {
                return Some(nodedb_types::Value::Float(f));
            }
            // Quoted string literal
            let trimmed = expr.trim();
            if (trimmed.starts_with('\'') && trimmed.ends_with('\''))
                || (trimmed.starts_with('"') && trimmed.ends_with('"'))
            {
                return Some(nodedb_types::Value::String(
                    trimmed[1..trimmed.len() - 1].to_string(),
                ));
            }
            None
        }
    }
}

// ── Time range extraction ──

/// Extract (min_ts_ms, max_ts_ms) time bounds from WHERE filters on timestamp columns.
///
/// Recognizes patterns like `ts >= '2024-01-01' AND ts <= '2024-01-02'` and converts
/// timestamp strings to epoch milliseconds.
pub(super) fn extract_time_range(filters: &[Filter]) -> (i64, i64) {
    let mut min_ts = i64::MIN;
    let mut max_ts = i64::MAX;

    for filter in filters {
        extract_time_bounds_from_filter(&filter.expr, &mut min_ts, &mut max_ts);
    }

    (min_ts, max_ts)
}

fn extract_time_bounds_from_filter(expr: &FilterExpr, min_ts: &mut i64, max_ts: &mut i64) {
    match expr {
        FilterExpr::Comparison { field, op, value } if is_time_field(field) => {
            if let Some(ms) = sql_value_to_timestamp_ms(value) {
                match op {
                    nodedb_sql::types::CompareOp::Ge | nodedb_sql::types::CompareOp::Gt => {
                        if ms > *min_ts {
                            *min_ts = ms;
                        }
                    }
                    nodedb_sql::types::CompareOp::Le | nodedb_sql::types::CompareOp::Lt => {
                        if ms < *max_ts {
                            *max_ts = ms;
                        }
                    }
                    nodedb_sql::types::CompareOp::Eq => {
                        *min_ts = ms;
                        *max_ts = ms;
                    }
                    _ => {}
                }
            }
        }
        FilterExpr::Between { field, low, high } if is_time_field(field) => {
            if let Some(lo) = sql_value_to_timestamp_ms(low) {
                *min_ts = lo;
            }
            if let Some(hi) = sql_value_to_timestamp_ms(high) {
                *max_ts = hi;
            }
        }
        FilterExpr::And(children) => {
            for child in children {
                extract_time_bounds_from_filter(&child.expr, min_ts, max_ts);
            }
        }
        // Expr-based filters: walk the SqlExpr tree for timestamp comparisons.
        FilterExpr::Expr(sql_expr) => {
            extract_time_bounds_from_expr(sql_expr, min_ts, max_ts);
        }
        _ => {}
    }
}

fn extract_time_bounds_from_expr(expr: &SqlExpr, min_ts: &mut i64, max_ts: &mut i64) {
    let SqlExpr::BinaryOp { left, op, right } = expr else {
        return;
    };
    match op {
        nodedb_sql::types::BinaryOp::And => {
            extract_time_bounds_from_expr(left, min_ts, max_ts);
            extract_time_bounds_from_expr(right, min_ts, max_ts);
        }
        nodedb_sql::types::BinaryOp::Ge | nodedb_sql::types::BinaryOp::Gt => {
            if let Some(field) = expr_column_name(left)
                && is_time_field(&field)
                && let Some(ms) = expr_to_timestamp_ms(right)
                && ms > *min_ts
            {
                *min_ts = ms;
            }
        }
        nodedb_sql::types::BinaryOp::Le | nodedb_sql::types::BinaryOp::Lt => {
            if let Some(field) = expr_column_name(left)
                && is_time_field(&field)
                && let Some(ms) = expr_to_timestamp_ms(right)
                && ms < *max_ts
            {
                *max_ts = ms;
            }
        }
        _ => {}
    }
}

fn is_time_field(name: &str) -> bool {
    let lower = name.to_lowercase();
    lower == "ts"
        || lower == "timestamp"
        || lower == "time"
        || lower == "created_at"
        || lower.ends_with("_at")
        || lower.ends_with("_time")
        || lower.ends_with("_ts")
}

fn expr_column_name(expr: &SqlExpr) -> Option<String> {
    match expr {
        SqlExpr::Column { name, .. } => Some(name.clone()),
        _ => None,
    }
}

fn expr_to_timestamp_ms(expr: &SqlExpr) -> Option<i64> {
    match expr {
        SqlExpr::Literal(val) => sql_value_to_timestamp_ms(val),
        _ => None,
    }
}

fn sql_value_to_timestamp_ms(val: &SqlValue) -> Option<i64> {
    match val {
        SqlValue::Int(ms) => Some(*ms),
        SqlValue::String(s) => parse_timestamp_to_ms(s),
        _ => None,
    }
}

fn parse_timestamp_to_ms(s: &str) -> Option<i64> {
    // Try common timestamp formats.
    // "2024-01-01 00:00:00" → epoch ms
    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
        return Some(dt.and_utc().timestamp_millis());
    }
    // "2024-01-01T00:00:00" (ISO 8601)
    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S") {
        return Some(dt.and_utc().timestamp_millis());
    }
    // "2024-01-01" (date only)
    if let Ok(d) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        return Some(d.and_hms_opt(0, 0, 0)?.and_utc().timestamp_millis());
    }
    // Raw milliseconds as string
    s.parse::<i64>().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn row_to_msgpack_produces_standard_format() {
        let row = vec![
            ("count".to_string(), SqlValue::Int(1)),
            ("label".to_string(), SqlValue::String("homepage".into())),
        ];
        let bytes = row_to_msgpack(&row).unwrap();
        // First byte should be a fixmap header (0x82 = map with 2 entries).
        assert_eq!(
            bytes[0], 0x82,
            "expected fixmap(2), got 0x{:02X}. bytes={bytes:?}",
            bytes[0]
        );
        // Should be decodable by json_from_msgpack (standard msgpack parser).
        let json = nodedb_types::json_from_msgpack(&bytes).unwrap();
        let obj = json.as_object().unwrap();
        assert_eq!(obj["count"], 1);
        assert_eq!(obj["label"], "homepage");
    }

    #[test]
    fn write_msgpack_value_int() {
        let mut buf = Vec::new();
        write_msgpack_value(&mut buf, &SqlValue::Int(42));
        // 42 fits in positive fixint (0x00-0x7F).
        assert_eq!(buf, vec![42]);
    }

    #[test]
    fn write_msgpack_value_string() {
        let mut buf = Vec::new();
        write_msgpack_value(&mut buf, &SqlValue::String("hi".into()));
        // fixstr: 0xA0 | 2 = 0xA2, then "hi".
        assert_eq!(buf, vec![0xA2, b'h', b'i']);
    }

    #[test]
    fn aggregate_spec_preserves_alias_and_case_expression() {
        use super::super::aggregate::agg_expr_to_spec;
        use nodedb_sql::types::AggregateExpr;

        let agg = AggregateExpr {
            function: "sum".into(),
            args: vec![SqlExpr::Case {
                operand: None,
                when_then: vec![(
                    SqlExpr::BinaryOp {
                        left: Box::new(SqlExpr::Column {
                            table: None,
                            name: "category".into(),
                        }),
                        op: nodedb_sql::types::BinaryOp::Eq,
                        right: Box::new(SqlExpr::Literal(SqlValue::String("tools".into()))),
                    },
                    SqlExpr::Literal(SqlValue::Int(1)),
                )],
                else_expr: Some(Box::new(SqlExpr::Literal(SqlValue::Int(0)))),
            }],
            alias: "tools_count".into(),
            distinct: false,
        };

        let spec = agg_expr_to_spec(&agg);

        assert_eq!(spec.function, "sum");
        assert_eq!(spec.alias, "tools_count");
        assert_eq!(spec.field, "*");
        assert!(matches!(
            spec.expr,
            Some(crate::bridge::expr_eval::SqlExpr::Case { .. })
        ));
    }
}
