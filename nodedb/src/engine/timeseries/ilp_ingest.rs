//! ILP → columnar memtable ingestion bridge.
//!
//! Accumulates parsed ILP lines into batches and flushes them to the
//! columnar memtable. Handles schema inference on first write (auto-create).

use std::collections::HashMap;

use super::columnar_memtable::{ColumnType, ColumnValue, ColumnarMemtable, ColumnarSchema};
use super::ilp::{FieldValue, IlpLine};
use nodedb_types::timeseries::{IngestResult, SeriesId, SeriesKey};

/// Infers a columnar schema from a batch of ILP lines.
///
/// Scans all lines to discover tag keys and field keys, then builds
/// a schema: timestamp + tag columns (Symbol) + field columns (typed).
pub fn infer_schema(lines: &[IlpLine<'_>]) -> ColumnarSchema {
    // Collect all tag keys and field keys with their types.
    let mut tag_keys: Vec<String> = Vec::new();
    let mut field_keys: Vec<(String, ColumnType)> = Vec::new();
    let mut seen_tags: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut seen_fields: std::collections::HashSet<String> = std::collections::HashSet::new();

    for line in lines {
        for &(key, _) in &line.tags {
            if seen_tags.insert(key.to_string()) {
                tag_keys.push(key.to_string());
            }
        }
        for &(key, ref val) in &line.fields {
            if seen_fields.insert(key.to_string()) {
                let col_type = match val {
                    FieldValue::Float(_) => ColumnType::Float64,
                    FieldValue::Int(_) | FieldValue::UInt(_) => ColumnType::Int64,
                    FieldValue::Str(_) => ColumnType::Symbol,
                    FieldValue::Bool(_) => ColumnType::Float64,
                };
                field_keys.push((key.to_string(), col_type));
            }
        }
    }

    // Build schema: timestamp, then tags (Symbol), then fields.
    let mut columns = Vec::with_capacity(1 + tag_keys.len() + field_keys.len());
    columns.push(("timestamp".to_string(), ColumnType::Timestamp));
    for tag in &tag_keys {
        columns.push((tag.clone(), ColumnType::Symbol));
    }
    for (field, ty) in &field_keys {
        columns.push((field.clone(), *ty));
    }

    ColumnarSchema {
        timestamp_idx: 0,
        codecs: vec![nodedb_codec::ColumnCodec::Auto; columns.len()],
        columns,
    }
}

/// Detect new fields in an ILP batch and expand the memtable schema.
///
/// Scans all lines for tag keys and field keys not present in the current
/// schema. New columns are added with NULL backfill for existing rows.
/// Must be called BEFORE `ingest_batch` so the batch can map values to
/// the expanded schema.
pub fn evolve_schema(memtable: &mut ColumnarMemtable, lines: &[IlpLine<'_>]) {
    // Collect existing column names before mutating (avoids borrow conflict).
    let existing: std::collections::HashSet<String> = memtable
        .schema()
        .columns
        .iter()
        .map(|(n, _)| n.clone())
        .collect();

    // Collect all new columns to add (name, type).
    let mut new_columns: Vec<(String, ColumnType)> = Vec::new();
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();

    for line in lines {
        for &(key, _) in &line.tags {
            if !existing.contains(key) && seen.insert(key.to_string()) {
                new_columns.push((key.to_string(), ColumnType::Symbol));
            }
        }
        for &(key, ref val) in &line.fields {
            if !existing.contains(key) && seen.insert(key.to_string()) {
                let col_type = match val {
                    FieldValue::Float(_) => ColumnType::Float64,
                    FieldValue::Int(_) | FieldValue::UInt(_) => ColumnType::Int64,
                    FieldValue::Str(_) => ColumnType::Symbol,
                    FieldValue::Bool(_) => ColumnType::Float64,
                };
                new_columns.push((key.to_string(), col_type));
            }
        }
    }

    // Now mutate — no outstanding borrows.
    for (name, col_type) in new_columns {
        memtable.add_column(name, col_type);
    }
}

/// Ingest a batch of parsed ILP lines into a columnar memtable.
///
/// The memtable's schema must already be set. Tag/field values are mapped
/// to the schema's column order.
///
/// Returns (accepted_count, rejected_count).
pub fn ingest_batch(
    memtable: &mut ColumnarMemtable,
    lines: &[IlpLine<'_>],
    series_keys: &mut HashMap<SeriesId, SeriesKey>,
    default_timestamp_ms: i64,
) -> (usize, usize) {
    ingest_batch_with_lvc(memtable, lines, series_keys, default_timestamp_ms, None)
}

/// Ingest a batch of ILP lines with optional last-value cache update.
pub fn ingest_batch_with_lvc(
    memtable: &mut ColumnarMemtable,
    lines: &[IlpLine<'_>],
    series_keys: &mut HashMap<SeriesId, SeriesKey>,
    default_timestamp_ms: i64,
    mut lvc: Option<&mut super::last_value_cache::LastValueCache>,
) -> (usize, usize) {
    let schema = memtable.schema().clone();
    let mut accepted = 0;
    let mut rejected = 0;

    for line in lines {
        // Build SeriesKey from measurement + tags.
        let tags: Vec<(String, String)> = line
            .tags
            .iter()
            .map(|&(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let key = SeriesKey::new(line.measurement, tags);
        let series_id = key.to_series_id(0);
        series_keys.entry(series_id).or_insert(key);

        // Resolve timestamp.
        let ts_ms = line
            .timestamp_ns
            .map(|ns| ns / 1_000_000) // ns → ms
            .unwrap_or(default_timestamp_ms);

        // Build column values in schema order.
        let mut values: Vec<ColumnValue<'_>> = Vec::with_capacity(schema.columns.len());

        for (col_name, col_type) in &schema.columns {
            match col_type {
                ColumnType::Timestamp => {
                    values.push(ColumnValue::Timestamp(ts_ms));
                }
                ColumnType::Symbol => {
                    // Look up tag value first, then string field value.
                    let val = line
                        .tags
                        .iter()
                        .find(|&&(k, _)| k == col_name)
                        .map(|&(_, v)| v)
                        .or_else(|| find_field_str(&line.fields, col_name))
                        .unwrap_or("");
                    values.push(ColumnValue::Symbol(val));
                }
                ColumnType::Float64 => {
                    let val = find_field_f64(&line.fields, col_name);
                    values.push(ColumnValue::Float64(val));
                }
                ColumnType::Int64 => {
                    let val = find_field_i64(&line.fields, col_name);
                    values.push(ColumnValue::Int64(val));
                }
            }
        }

        match memtable.ingest_row(series_id, &values) {
            Ok(IngestResult::Rejected) => rejected += 1,
            Ok(_) => {
                accepted += 1;
                // Update last-value cache with the first float64 field value.
                if let Some(ref mut cache) = lvc {
                    let value = values
                        .iter()
                        .find_map(|v| match v {
                            ColumnValue::Float64(f) => Some(*f),
                            _ => None,
                        })
                        .unwrap_or(0.0);
                    cache.update(series_id, ts_ms, value);
                }
            }
            Err(_) => rejected += 1,
        }
    }

    (accepted, rejected)
}

fn find_field_str<'a>(fields: &[(&str, FieldValue<'a>)], name: &str) -> Option<&'a str> {
    for &(k, ref v) in fields {
        if k == name
            && let FieldValue::Str(s) = v
        {
            return Some(s);
        }
    }
    None
}

fn find_field_f64(fields: &[(&str, FieldValue<'_>)], name: &str) -> f64 {
    for &(k, ref v) in fields {
        if k == name {
            return match v {
                FieldValue::Float(f) => *f,
                FieldValue::Int(i) => *i as f64,
                FieldValue::UInt(u) => *u as f64,
                FieldValue::Bool(b) => {
                    if *b {
                        1.0
                    } else {
                        0.0
                    }
                }
                FieldValue::Str(_) => f64::NAN,
            };
        }
    }
    f64::NAN
}

fn find_field_i64(fields: &[(&str, FieldValue<'_>)], name: &str) -> i64 {
    for &(k, ref v) in fields {
        if k == name {
            return match v {
                FieldValue::Int(i) => *i,
                FieldValue::UInt(u) => *u as i64,
                FieldValue::Float(f) => *f as i64,
                FieldValue::Bool(b) => {
                    if *b {
                        1
                    } else {
                        0
                    }
                }
                FieldValue::Str(_) => 0,
            };
        }
    }
    0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::timeseries::columnar_memtable::ColumnarMemtableConfig;
    use crate::engine::timeseries::ilp::parse_batch;

    fn default_config() -> ColumnarMemtableConfig {
        ColumnarMemtableConfig {
            max_memory_bytes: 10 * 1024 * 1024,
            hard_memory_limit: 20 * 1024 * 1024,
            max_tag_cardinality: 10_000,
        }
    }

    #[test]
    fn infer_schema_from_ilp() {
        let input = "cpu,host=a,dc=us value=0.64,count=100i 1000000000\n\
                     cpu,host=b,dc=eu value=0.55,count=200i 2000000000";
        let lines: Vec<_> = parse_batch(input)
            .into_iter()
            .filter_map(|r| r.ok())
            .collect();
        let schema = infer_schema(&lines);

        // timestamp + 2 tags + 2 fields = 5 columns.
        assert_eq!(schema.columns.len(), 5);
        assert_eq!(
            schema.columns[0],
            ("timestamp".into(), ColumnType::Timestamp)
        );
        assert_eq!(schema.columns[1].1, ColumnType::Symbol); // host
        assert_eq!(schema.columns[2].1, ColumnType::Symbol); // dc
        assert_eq!(schema.columns[3].1, ColumnType::Float64); // value
        assert_eq!(schema.columns[4].1, ColumnType::Int64); // count
    }

    #[test]
    fn ingest_ilp_batch() {
        let input = "cpu,host=server01 usage=0.64 1434055562000000000\n\
                     cpu,host=server02 usage=0.55 1434055563000000000\n\
                     cpu,host=server01 usage=0.72 1434055564000000000";
        let lines: Vec<_> = parse_batch(input)
            .into_iter()
            .filter_map(|r| r.ok())
            .collect();
        let schema = infer_schema(&lines);

        let mut mt = ColumnarMemtable::new(schema, default_config());
        let mut series_keys = HashMap::new();

        let (accepted, rejected) = ingest_batch(&mut mt, &lines, &mut series_keys, 0);
        assert_eq!(accepted, 3);
        assert_eq!(rejected, 0);
        assert_eq!(mt.row_count(), 3);
        assert_eq!(series_keys.len(), 2); // server01 and server02
    }

    #[test]
    fn timestamp_ns_to_ms_conversion() {
        let input = "temp value=22.5 1704067200000000000"; // 2024-01-01 00:00:00 UTC in ns
        let lines: Vec<_> = parse_batch(input)
            .into_iter()
            .filter_map(|r| r.ok())
            .collect();
        let schema = infer_schema(&lines);

        let mut mt = ColumnarMemtable::new(schema, default_config());
        let mut series_keys = HashMap::new();
        ingest_batch(&mut mt, &lines, &mut series_keys, 0);

        let ts = mt.column(0).as_timestamps()[0];
        assert_eq!(ts, 1_704_067_200_000); // ms
    }

    #[test]
    fn missing_timestamp_uses_default() {
        let input = "temp value=22.5"; // no timestamp
        let lines: Vec<_> = parse_batch(input)
            .into_iter()
            .filter_map(|r| r.ok())
            .collect();
        let schema = infer_schema(&lines);

        let mut mt = ColumnarMemtable::new(schema, default_config());
        let mut series_keys = HashMap::new();
        let default_ts = 9999;
        ingest_batch(&mut mt, &lines, &mut series_keys, default_ts);

        let ts = mt.column(0).as_timestamps()[0];
        assert_eq!(ts, 9999);
    }

    #[test]
    fn mixed_field_types() {
        let input = "sensor temp=72.5,humidity=45i,active=true 1000000000";
        let lines: Vec<_> = parse_batch(input)
            .into_iter()
            .filter_map(|r| r.ok())
            .collect();
        let schema = infer_schema(&lines);

        let mut mt = ColumnarMemtable::new(schema, default_config());
        let mut series_keys = HashMap::new();
        ingest_batch(&mut mt, &lines, &mut series_keys, 0);
        assert_eq!(mt.row_count(), 1);
    }

    #[test]
    fn string_fields_stored_as_symbol() {
        let input =
            r#"dns,client=10.0.0.1 qname="bigquery.googleapis.com",elapsed_ms=12.5 1000000000"#;
        let lines: Vec<_> = parse_batch(input)
            .into_iter()
            .filter_map(|r| r.ok())
            .collect();
        let schema = infer_schema(&lines);

        // qname should be Symbol, not Float64.
        let qname_col = schema.columns.iter().find(|(n, _)| n == "qname").unwrap();
        assert_eq!(qname_col.1, ColumnType::Symbol);

        // Ingest and verify the string value is recoverable.
        let mut mt = ColumnarMemtable::new(schema.clone(), default_config());
        let mut series_keys = HashMap::new();
        ingest_batch(&mut mt, &lines, &mut series_keys, 0);
        assert_eq!(mt.row_count(), 1);

        // Find qname column index and resolve symbol.
        let col_idx = schema
            .columns
            .iter()
            .position(|(n, _)| n == "qname")
            .unwrap();
        let col_data = mt.column(col_idx);
        if let crate::engine::timeseries::columnar_memtable::ColumnData::Symbol(ids) = col_data {
            let dict = mt.symbol_dict(col_idx).unwrap();
            let resolved = dict.get(ids[0]).unwrap();
            assert_eq!(resolved, "bigquery.googleapis.com");
        } else {
            panic!("expected Symbol column data for qname");
        }
    }
}
