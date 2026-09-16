// SPDX-License-Identifier: BUSL-1.1

//! Tag value autocomplete: `SHOW TAG VALUES FOR 'host' FROM metrics`.
//!
//! Returns all distinct values for a tag column by reading from the symbol
//! dictionary only — no data scan required. O(1) per partition since the
//! dictionary is stored as a small JSON file alongside the column data.
//!
//! For in-memory memtable data, reads directly from the per-column
//! SymbolDictionary.

use std::collections::BTreeSet;
use std::path::Path;

use super::columnar_memtable::ColumnarMemtable;
use super::columnar_segment::ColumnarSegmentReader;

/// Get all distinct tag values for a column from a partition on disk.
///
/// Reads the `.sym` file only — no column data decompression.
pub fn tag_values_from_partition(
    partition_dir: &Path,
    tag_column: &str,
) -> Result<Vec<String>, TagAutoError> {
    let dict = ColumnarSegmentReader::read_symbol_dict(partition_dir, tag_column, None)
        .map_err(|e| TagAutoError::Io(format!("read symbol dict: {e}")))?;

    let mut values = Vec::with_capacity(dict.len());
    for i in 0..dict.len() {
        if let Some(s) = dict.get(i as u32) {
            values.push(s.to_string());
        }
    }
    values.sort();
    Ok(values)
}

/// Get all distinct tag values from an in-memory memtable.
pub fn tag_values_from_memtable(memtable: &ColumnarMemtable, tag_column: &str) -> Vec<String> {
    let schema = memtable.schema();
    let col_idx = schema
        .columns
        .iter()
        .position(|(name, _)| name == tag_column);

    let Some(idx) = col_idx else {
        return Vec::new();
    };

    let Some(dict) = memtable.symbol_dict(idx) else {
        return Vec::new();
    };

    let mut values = Vec::with_capacity(dict.len());
    for i in 0..dict.len() {
        if let Some(s) = dict.get(i as u32) {
            values.push(s.to_string());
        }
    }
    values.sort();
    values
}

/// Merge tag values from multiple sources (partitions + memtable).
///
/// Returns sorted unique values.
pub fn merge_tag_values(sources: &[Vec<String>]) -> Vec<String> {
    let mut merged = BTreeSet::new();
    for source in sources {
        for val in source {
            merged.insert(val.clone());
        }
    }
    merged.into_iter().collect()
}

/// Get tag values with optional prefix filter for autocomplete UIs.
///
/// `prefix` filters to values starting with the given string (case-insensitive).
pub fn tag_values_with_prefix(values: &[String], prefix: &str) -> Vec<String> {
    let prefix_lower = prefix.to_lowercase();
    values
        .iter()
        .filter(|v| v.to_lowercase().starts_with(&prefix_lower))
        .cloned()
        .collect()
}

/// List all tag column names for a collection from the schema.
pub fn tag_column_names(memtable: &ColumnarMemtable) -> Vec<String> {
    use super::columnar_memtable::ColumnType;
    memtable
        .schema()
        .columns
        .iter()
        .filter(|(_, ty)| *ty == ColumnType::Symbol)
        .map(|(name, _)| name.clone())
        .collect()
}

/// Error type for tag autocomplete operations.
#[derive(thiserror::Error, Debug)]
pub enum TagAutoError {
    #[error("tag autocomplete: {0}")]
    Io(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::timeseries::columnar_memtable::{
        ColumnType, ColumnValue, ColumnarMemtable, ColumnarMemtableConfig, ColumnarSchema, TimeKind,
    };
    use crate::engine::timeseries::columnar_segment::ColumnarSegmentWriter;
    use tempfile::TempDir;

    fn test_config() -> ColumnarMemtableConfig {
        ColumnarMemtableConfig {
            max_memory_bytes: 10 * 1024 * 1024,
            hard_memory_limit: 20 * 1024 * 1024,
            max_tag_cardinality: 1000,
        }
    }

    fn make_tagged_memtable() -> ColumnarMemtable {
        let schema = ColumnarSchema {
            columns: vec![
                ("timestamp".into(), ColumnType::Timestamp(TimeKind::Millis)),
                ("value".into(), ColumnType::Float64),
                ("host".into(), ColumnType::Symbol),
                ("region".into(), ColumnType::Symbol),
            ],
            timestamp_idx: 0,
            codecs: vec![nodedb_codec::ColumnCodec::Auto; 4],
        };
        let mut mt = ColumnarMemtable::new(schema, test_config());
        let hosts = ["prod-web-01", "prod-web-02", "prod-api-01", "staging-01"];
        let regions = ["us-east-1", "us-west-2", "eu-west-1"];
        for i in 0..20 {
            mt.ingest_row(
                i as u64,
                &[
                    ColumnValue::Timestamp(1000 + i as i64),
                    ColumnValue::Float64(i as f64),
                    ColumnValue::Symbol(hosts[i % hosts.len()].to_string()),
                    ColumnValue::Symbol(regions[i % regions.len()].to_string()),
                ],
            )
            .unwrap();
        }
        mt
    }

    #[test]
    fn tag_values_from_memtable_basic() {
        let mt = make_tagged_memtable();
        let hosts = tag_values_from_memtable(&mt, "host");
        assert_eq!(hosts.len(), 4);
        assert_eq!(hosts[0], "prod-api-01"); // Sorted.
        assert_eq!(hosts[3], "staging-01");
    }

    #[test]
    fn tag_column_names_lists_symbols() {
        let mt = make_tagged_memtable();
        let names = tag_column_names(&mt);
        assert_eq!(names, vec!["host", "region"]);
    }

    #[test]
    fn tag_values_from_partition_roundtrip() {
        let tmp = TempDir::new().unwrap();
        let mt = make_tagged_memtable();
        let drain = {
            let mut mt = mt;
            mt.drain()
        };

        let writer = ColumnarSegmentWriter::new(tmp.path());
        writer
            .write_partition("ts-tags", &drain.view(), 86_400_000, 0, None)
            .unwrap();

        let part_dir = tmp.path().join("ts-tags");
        let hosts = tag_values_from_partition(&part_dir, "host").unwrap();
        assert_eq!(hosts.len(), 4);

        let regions = tag_values_from_partition(&part_dir, "region").unwrap();
        assert_eq!(regions.len(), 3);
    }

    #[test]
    fn merge_values() {
        let a = vec!["us-east-1".into(), "us-west-2".into()];
        let b = vec!["eu-west-1".into(), "us-east-1".into()]; // overlap
        let merged = merge_tag_values(&[a, b]);
        assert_eq!(merged, vec!["eu-west-1", "us-east-1", "us-west-2"]);
    }

    #[test]
    fn prefix_filter() {
        let values: Vec<String> = vec![
            "prod-web-01".into(),
            "prod-web-02".into(),
            "prod-api-01".into(),
            "staging-01".into(),
        ];
        let filtered = tag_values_with_prefix(&values, "prod-web");
        assert_eq!(filtered, vec!["prod-web-01", "prod-web-02"]);
    }

    #[test]
    fn nonexistent_column() {
        let mt = make_tagged_memtable();
        let values = tag_values_from_memtable(&mt, "nonexistent");
        assert!(values.is_empty());
    }
}
