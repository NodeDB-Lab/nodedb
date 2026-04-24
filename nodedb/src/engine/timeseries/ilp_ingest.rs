//! ILP → columnar memtable ingestion bridge.
//!
//! Accumulates parsed ILP lines into batches and flushes them to the
//! columnar memtable. Schema inference / evolution lives in the sibling
//! `ilp_schema` module.

use std::collections::HashMap;

use super::columnar_memtable::{ColumnType, ColumnValue, ColumnarMemtable};
use super::ilp::{FieldValue, IlpLine};
use nodedb_types::timeseries::{IngestResult, SeriesId, SeriesKey};

pub use super::ilp_schema::{ensure_bitemporal_columns, evolve_schema, infer_schema};

/// Bitemporal stamps applied per-row on ingest. `system_ms` is always
/// engine-assigned (client-supplied values are ignored); the valid-time
/// pair is client-provided (via `_ts_valid_from` / `_ts_valid_until`
/// fields in the line's field set) or defaults to the open interval.
#[derive(Clone, Copy)]
pub struct BitempStamps {
    pub system_ms: i64,
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
    ingest_batch_with_lvc(
        memtable,
        lines,
        series_keys,
        default_timestamp_ms,
        None,
        None,
    )
}

/// Ingest a batch of ILP lines with optional last-value cache update.
///
/// When `bitemporal` is `Some`, rows are stamped with the provided
/// `system_ms` for the `_ts_system` reserved column. `_ts_valid_from` /
/// `_ts_valid_until` are pulled from the line's field set when present,
/// defaulting to the open interval `[i64::MIN, i64::MAX)`.
pub fn ingest_batch_with_lvc(
    memtable: &mut ColumnarMemtable,
    lines: &[IlpLine<'_>],
    series_keys: &mut HashMap<SeriesId, SeriesKey>,
    default_timestamp_ms: i64,
    mut lvc: Option<&mut super::last_value_cache::LastValueCache>,
    bitemporal: Option<BitempStamps>,
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
                    let val = match (bitemporal, col_name.as_str()) {
                        (Some(b), "_ts_system") => b.system_ms,
                        (Some(_), "_ts_valid_from") => {
                            find_field_i64_opt(&line.fields, col_name).unwrap_or(i64::MIN)
                        }
                        (Some(_), "_ts_valid_until") => {
                            find_field_i64_opt(&line.fields, col_name).unwrap_or(i64::MAX)
                        }
                        _ => find_field_i64(&line.fields, col_name),
                    };
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
    find_field_i64_opt(fields, name).unwrap_or(0)
}

fn find_field_i64_opt(fields: &[(&str, FieldValue<'_>)], name: &str) -> Option<i64> {
    for &(k, ref v) in fields {
        if k == name {
            return Some(match v {
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
            });
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::timeseries::columnar_memtable::{ColumnData, ColumnarMemtableConfig};
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
    fn bitemporal_ingest_stamps_reserved_columns() {
        // Late-arriving IoT backfill: an ILP line with a user-provided
        // `_ts_valid_from` reflecting when the measurement was taken, but
        // the server stamps `_ts_system` at ingest time. A subsequent
        // `AS OF SYSTEM TIME` query before `system_now` must exclude the
        // row; an `AS OF VALID TIME` query at the event time must find it.
        let input = "temp,sensor=s1 reading=22.5,_ts_valid_from=1000i,_ts_valid_until=2000i \
                     1500000000000000";
        let lines: Vec<_> = parse_batch(input)
            .into_iter()
            .filter_map(|r| r.ok())
            .collect();

        let mut schema = infer_schema(&lines);
        ensure_bitemporal_columns(&mut schema);
        // All three reserved columns must be present; `_ts_valid_from`
        // and `_ts_valid_until` may come from the line's field set (via
        // `infer_schema`) instead of being appended by
        // `ensure_bitemporal_columns`, so we check set-membership rather
        // than fixed tail-order.
        for name in ["_ts_system", "_ts_valid_from", "_ts_valid_until"] {
            assert!(
                schema.columns.iter().any(|(n, _)| n == name),
                "missing reserved column {name}"
            );
        }

        let mut mt = ColumnarMemtable::new(schema, default_config());
        let mut keys = HashMap::new();
        let stamps = Some(BitempStamps { system_ms: 5_000 });
        let (accepted, rejected) =
            ingest_batch_with_lvc(&mut mt, &lines, &mut keys, 0, None, stamps);
        assert_eq!((accepted, rejected), (1, 0));

        // Inspect the memtable row to verify the three reserved slots
        // carry the expected stamps.
        let schema = mt.schema().clone();
        let sys_idx = schema
            .columns
            .iter()
            .position(|(n, _)| n == "_ts_system")
            .unwrap();
        let vf_idx = schema
            .columns
            .iter()
            .position(|(n, _)| n == "_ts_valid_from")
            .unwrap();
        let vu_idx = schema
            .columns
            .iter()
            .position(|(n, _)| n == "_ts_valid_until")
            .unwrap();
        let rows: Vec<Vec<i64>> = (0..mt.row_count() as usize)
            .map(|r| {
                [sys_idx, vf_idx, vu_idx]
                    .iter()
                    .map(|&c| {
                        let col = mt.column(c);
                        if let ColumnData::Int64(vals) = col {
                            vals[r]
                        } else {
                            panic!("expected Int64 column at idx {c}")
                        }
                    })
                    .collect()
            })
            .collect();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], vec![5_000, 1_000, 2_000]);
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
