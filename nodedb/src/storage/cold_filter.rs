//! Parquet predicate pushdown: row-group pruning + row-level filtering.
//!
//! When reading Parquet files from cold storage (L2), this module applies
//! two levels of filtering to minimize I/O:
//!
//! 1. **Row-group pruning**: Uses Parquet column statistics (min/max per
//!    row group) to skip entire row groups that can't match the predicate.
//!    A row group with `max(age) = 25` is skipped for `age > 30`.
//!
//! 2. **Row-level filtering**: After row-group pruning, applies the full
//!    predicate to each row in the surviving row groups.
//!
//! ## Integration
//!
//! Called from `read_parquet_filtered()` which replaces `read_parquet_with_predicate`
//! for queries that have filter predicates.

use arrow::array::{Array, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::DataType;
use bytes::Bytes;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{ArrowPredicateFn, ParquetRecordBatchReaderBuilder, RowFilter};
use parquet::file::metadata::RowGroupMetaData;
use parquet::file::statistics::Statistics;
use tracing::debug;

use crate::bridge::scan_filter::ScanFilter;

/// Read a Parquet file with both row-group pruning and row-level filtering.
///
/// `filters` are the same `ScanFilter` predicates used for L0/L1 DocumentScan.
/// `projection` restricts which columns are read (empty = all columns).
///
/// Returns filtered RecordBatches.
pub fn read_parquet_filtered(
    parquet_bytes: &[u8],
    filters: &[ScanFilter],
    projection: &[String],
) -> crate::Result<Vec<RecordBatch>> {
    let reader_builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(
        parquet_bytes,
    ))
    .map_err(|e| crate::Error::ColdStorage {
        detail: format!("parquet reader init: {e}"),
    })?;

    let file_metadata = reader_builder.metadata().clone();
    let schema = reader_builder.schema().clone();

    // Step 1: Row-group pruning via column statistics.
    let row_groups = prune_row_groups(file_metadata.row_groups(), filters);

    if row_groups.is_empty() {
        debug!("all row groups pruned by statistics — zero rows returned");
        return Ok(Vec::new());
    }

    debug!(
        total_groups = file_metadata.num_row_groups(),
        surviving_groups = row_groups.len(),
        "row-group pruning complete"
    );

    // Step 2: Build reader with only surviving row groups and optional projection.
    let reader_builder =
        ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(parquet_bytes))
            .map_err(|e| crate::Error::ColdStorage {
                detail: format!("parquet reader reinit: {e}"),
            })?
            .with_row_groups(row_groups);

    let reader_builder = if projection.is_empty() {
        reader_builder
    } else {
        let indices: Vec<usize> = projection
            .iter()
            .filter_map(|name| schema.index_of(name).ok())
            .collect();
        let mask = ProjectionMask::leaves(reader_builder.parquet_schema(), indices);
        reader_builder.with_projection(mask)
    };

    // Step 3: Apply row-level filter if predicates exist.
    let reader = if filters.is_empty() {
        reader_builder
            .build()
            .map_err(|e| crate::Error::ColdStorage {
                detail: format!("build reader: {e}"),
            })?
    } else {
        let filter_schema = schema.clone();
        let filters_owned: Vec<ScanFilter> = filters.to_vec();

        let predicate = ArrowPredicateFn::new(ProjectionMask::all(), move |batch: RecordBatch| {
            let num_rows = batch.num_rows();
            let mask: Vec<bool> = (0..num_rows)
                .map(|row_idx| {
                    let doc = record_batch_row_to_json(&batch, row_idx, &filter_schema);
                    let msgpack = nodedb_types::json_msgpack::json_to_msgpack_or_empty(&doc);
                    filters_owned.iter().all(|f| f.matches_binary(&msgpack))
                })
                .collect();

            Ok(arrow::array::BooleanArray::from(mask))
        });

        let row_filter = RowFilter::new(vec![Box::new(predicate)]);
        reader_builder
            .with_row_filter(row_filter)
            .build()
            .map_err(|e| crate::Error::ColdStorage {
                detail: format!("build filtered reader: {e}"),
            })?
    };

    let batches: Vec<RecordBatch> =
        reader
            .collect::<Result<_, _>>()
            .map_err(|e| crate::Error::ColdStorage {
                detail: format!("read batches: {e}"),
            })?;
    Ok(batches)
}

/// Prune row groups whose column statistics prove the predicate can't match.
///
/// Returns indices of row groups that MIGHT contain matching rows.
fn prune_row_groups(row_groups: &[RowGroupMetaData], filters: &[ScanFilter]) -> Vec<usize> {
    if filters.is_empty() {
        return (0..row_groups.len()).collect();
    }

    let mut surviving = Vec::new();

    for (idx, rg) in row_groups.iter().enumerate() {
        let mut can_match = true;

        for filter in filters {
            if !row_group_might_match(rg, filter) {
                can_match = false;
                break;
            }
        }

        if can_match {
            surviving.push(idx);
        }
    }

    surviving
}

/// Check if a row group's column statistics allow the filter to match.
///
/// Returns `true` if the row group MIGHT contain matching rows (conservative).
/// Returns `false` only if statistics PROVE no match is possible.
fn row_group_might_match(rg: &RowGroupMetaData, filter: &ScanFilter) -> bool {
    // Find the column in the row group.
    let col_idx = rg
        .columns()
        .iter()
        .position(|c| c.column_descr().name() == filter.field);

    let Some(col_idx) = col_idx else {
        return true; // Column not in Parquet — can't prune.
    };

    let col = &rg.columns()[col_idx];
    let Some(stats) = col.statistics() else {
        return true; // No statistics — can't prune.
    };

    // Use min/max statistics to prune.
    use nodedb_query::scan_filter::FilterOp;
    let json_val: serde_json::Value = filter.value.clone().into();
    match &filter.op {
        FilterOp::Eq => stat_might_contain(stats, &json_val),
        FilterOp::Gt | FilterOp::Gte => stat_max_gte(stats, &json_val),
        FilterOp::Lt | FilterOp::Lte => stat_min_lte(stats, &json_val),
        _ => true, // Complex operators (contains, like, etc.) — can't prune.
    }
}

/// Check if column statistics allow an equality match.
fn stat_might_contain(stats: &Statistics, value: &serde_json::Value) -> bool {
    match stats {
        Statistics::Int64(s) => {
            let (Some(min), Some(max)) = (s.min_opt(), s.max_opt()) else {
                return true;
            };
            value.as_i64().is_none_or(|v| v >= *min && v <= *max)
        }
        Statistics::Float(s) => {
            let (Some(min), Some(max)) = (s.min_opt(), s.max_opt()) else {
                return true;
            };
            value
                .as_f64()
                .is_none_or(|v| v >= *min as f64 && v <= *max as f64)
        }
        Statistics::Double(s) => {
            let (Some(min), Some(max)) = (s.min_opt(), s.max_opt()) else {
                return true;
            };
            value.as_f64().is_none_or(|v| v >= *min && v <= *max)
        }
        Statistics::ByteArray(s) => {
            let (Some(min), Some(max)) = (s.min_opt(), s.max_opt()) else {
                return true;
            };
            if let Some(v) = value.as_str() {
                let min_str = std::str::from_utf8(min.data()).unwrap_or("");
                let max_str = std::str::from_utf8(max.data()).unwrap_or("");
                v >= min_str && v <= max_str
            } else {
                true
            }
        }
        _ => true,
    }
}

/// Check if the column max >= value (for gt/gte predicates).
fn stat_max_gte(stats: &Statistics, value: &serde_json::Value) -> bool {
    match stats {
        Statistics::Int64(s) => {
            let Some(max) = s.max_opt() else { return true };
            value.as_i64().is_none_or(|v| *max >= v)
        }
        Statistics::Double(s) => {
            let Some(max) = s.max_opt() else { return true };
            value.as_f64().is_none_or(|v| *max >= v)
        }
        _ => true,
    }
}

/// Check if the column min <= value (for lt/lte predicates).
fn stat_min_lte(stats: &Statistics, value: &serde_json::Value) -> bool {
    match stats {
        Statistics::Int64(s) => {
            let Some(min) = s.min_opt() else { return true };
            value.as_i64().is_none_or(|v| *min <= v)
        }
        Statistics::Double(s) => {
            let Some(min) = s.min_opt() else { return true };
            value.as_f64().is_none_or(|v| *min <= v)
        }
        _ => true,
    }
}

/// Convert a single row from a RecordBatch to a JSON Value for filter evaluation.
fn record_batch_row_to_json(
    batch: &RecordBatch,
    row_idx: usize,
    schema: &arrow::datatypes::Schema,
) -> serde_json::Value {
    let mut map = serde_json::Map::new();

    for (col_idx, field) in schema.fields().iter().enumerate() {
        if col_idx >= batch.num_columns() {
            continue;
        }
        let col = batch.column(col_idx);

        if col.is_null(row_idx) {
            map.insert(field.name().clone(), serde_json::Value::Null);
            continue;
        }

        let value = match field.data_type() {
            DataType::Utf8 => {
                let arr = col.as_any().downcast_ref::<StringArray>();
                arr.map(|a| serde_json::Value::String(a.value(row_idx).to_string()))
            }
            DataType::Int64 => {
                let arr = col.as_any().downcast_ref::<Int64Array>();
                arr.map(|a| serde_json::Value::Number(a.value(row_idx).into()))
            }
            DataType::Float64 => {
                let arr = col.as_any().downcast_ref::<Float64Array>();
                arr.and_then(|a| {
                    serde_json::Number::from_f64(a.value(row_idx)).map(serde_json::Value::Number)
                })
            }
            _ => Some(serde_json::Value::String(
                arrow::util::display::array_value_to_string(col, row_idx).unwrap_or_default(),
            )),
        };

        if let Some(v) = value {
            map.insert(field.name().clone(), v);
        }
    }

    serde_json::Value::Object(map)
}

/// Convert filtered RecordBatches to the same `(doc_id, value_bytes)` format
/// used by L0/L1 DocumentScan, enabling transparent merging.
pub fn batches_to_document_rows(batches: &[RecordBatch]) -> Vec<(String, Vec<u8>)> {
    let mut rows = Vec::new();

    for batch in batches {
        let id_col = batch
            .column_by_name("_id")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>());

        for row_idx in 0..batch.num_rows() {
            let doc_id = id_col
                .map(|c| c.value(row_idx).to_string())
                .unwrap_or_default();

            // Build raw msgpack map from Arrow columns — no serde_json intermediary.
            // First pass: count non-null, non-id fields.
            let mut field_count = 0usize;
            for (col_idx, field) in batch.schema().fields().iter().enumerate() {
                if field.name() == "_id" {
                    continue;
                }
                let col = batch.column(col_idx);
                if !col.is_null(row_idx) {
                    field_count += 1;
                }
            }
            let mut value = Vec::with_capacity(field_count * 32);
            nodedb_query::msgpack_scan::write_map_header(&mut value, field_count);
            for (col_idx, field) in batch.schema().fields().iter().enumerate() {
                if field.name() == "_id" {
                    continue;
                }
                let col = batch.column(col_idx);
                if col.is_null(row_idx) {
                    continue;
                }
                match field.data_type() {
                    DataType::Utf8 => {
                        if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
                            nodedb_query::msgpack_scan::write_kv_str(
                                &mut value,
                                field.name(),
                                arr.value(row_idx),
                            );
                        }
                    }
                    DataType::Int64 => {
                        if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                            nodedb_query::msgpack_scan::write_kv_i64(
                                &mut value,
                                field.name(),
                                arr.value(row_idx),
                            );
                        }
                    }
                    DataType::Float64 => {
                        if let Some(arr) = col.as_any().downcast_ref::<Float64Array>() {
                            nodedb_query::msgpack_scan::write_kv_f64(
                                &mut value,
                                field.name(),
                                arr.value(row_idx),
                            );
                        }
                    }
                    _ => {}
                }
            }
            rows.push((doc_id, value));
        }
    }

    rows
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::ArrayRef;
    use arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    fn make_test_parquet(rows: &[(&str, i64, &str)]) -> Vec<u8> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_id", DataType::Utf8, false),
            Field::new("age", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]));

        let ids: Vec<&str> = rows.iter().map(|(id, _, _)| *id).collect();
        let ages: Vec<i64> = rows.iter().map(|(_, age, _)| *age).collect();
        let names: Vec<&str> = rows.iter().map(|(_, _, name)| *name).collect();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(ids)) as ArrayRef,
                Arc::new(Int64Array::from(ages)) as ArrayRef,
                Arc::new(StringArray::from(names)) as ArrayRef,
            ],
        )
        .unwrap();

        let props = parquet::file::properties::WriterProperties::builder()
            .set_statistics_enabled(parquet::file::properties::EnabledStatistics::Page)
            .set_max_row_group_row_count(Some(2)) // Small row groups for testing pruning.
            .build();

        let mut buf: Vec<u8> = Vec::new();
        let mut writer =
            parquet::arrow::ArrowWriter::try_new(&mut buf, schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        buf
    }

    #[test]
    fn no_filters_returns_all_rows() {
        let parquet = make_test_parquet(&[("d1", 25, "alice"), ("d2", 30, "bob")]);
        let batches = read_parquet_filtered(&parquet, &[], &[]).unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }

    #[test]
    fn row_level_filter_applies() {
        let parquet = make_test_parquet(&[
            ("d1", 20, "alice"),
            ("d2", 30, "bob"),
            ("d3", 25, "charlie"),
        ]);

        let filters = vec![ScanFilter {
            field: "age".into(),
            op: "gt".into(),
            value: nodedb_types::Value::Integer(25),
            clauses: vec![],
            expr: None,
        }];

        let batches = read_parquet_filtered(&parquet, &filters, &[]).unwrap();
        let rows = batches_to_document_rows(&batches);

        // Only bob (age=30) should match age > 25.
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, "d2");
    }

    #[test]
    fn projection_limits_columns() {
        let parquet = make_test_parquet(&[("d1", 25, "alice")]);
        let batches = read_parquet_filtered(&parquet, &[], &["name".into()]).unwrap();
        assert!(!batches.is_empty());
        assert_eq!(batches[0].num_columns(), 1);
    }

    #[test]
    fn batches_to_rows_conversion() {
        let parquet = make_test_parquet(&[("d1", 25, "alice"), ("d2", 30, "bob")]);
        let batches = read_parquet_filtered(&parquet, &[], &[]).unwrap();
        let rows = batches_to_document_rows(&batches);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].0, "d1");
        assert_eq!(rows[1].0, "d2");

        // Verify the value is valid msgpack with the expected fields.
        let doc = nodedb_types::value_from_msgpack(&rows[0].1).unwrap();
        assert_eq!(
            doc.get("name"),
            Some(&nodedb_types::Value::String("alice".into()))
        );
        assert_eq!(doc.get("age"), Some(&nodedb_types::Value::Integer(25)));
    }
}
