// SPDX-License-Identifier: BUSL-1.1

//! `ColumnarMemtable` — per-column ingest buffer for timeseries data.
//!
//! NOT thread-safe — lives on a single Data Plane core (!Send by design).

use std::collections::HashMap;

use nodedb_types::timeseries::{SeriesId, SymbolDictionary};

use super::super::types::{ColumnData, ColumnType, ColumnarMemtableConfig, ColumnarSchema};

/// Columnar memtable: per-column vectors instead of per-series hash maps.
///
/// Each row is a flat tuple of (timestamp, value, tag1, tag2, ...).
/// Series identity is derived from the tag columns at query time.
/// This layout is SIMD-friendly: aggregation functions operate on
/// contiguous `&[f64]` or `&[i64]` slices.
pub struct ColumnarMemtable {
    pub(super) schema: ColumnarSchema,
    pub(super) columns: Vec<ColumnData>,
    /// Per-series row count for quick cardinality checks.
    pub(super) series_row_counts: HashMap<SeriesId, u64>,
    /// Per-tag-column symbol dictionary.
    pub(super) symbol_dicts: HashMap<usize, SymbolDictionary>,
    pub(super) row_count: u64,
    pub(super) memory_bytes: usize,
    pub(super) config: ColumnarMemtableConfig,
    pub(super) min_ts: i64,
    pub(super) max_ts: i64,
}

impl ColumnarMemtable {
    /// Create a new columnar memtable with the given schema.
    pub fn new(schema: ColumnarSchema, config: ColumnarMemtableConfig) -> Self {
        let columns: Vec<ColumnData> = schema
            .columns
            .iter()
            .map(|(_, ty)| ColumnData::new(*ty))
            .collect();

        // Initialize symbol dicts for tag columns.
        let mut symbol_dicts = HashMap::new();
        for (i, (_, ty)) in schema.columns.iter().enumerate() {
            if *ty == ColumnType::Symbol {
                symbol_dicts.insert(i, SymbolDictionary::new());
            }
        }

        Self {
            schema,
            columns,
            series_row_counts: HashMap::new(),
            symbol_dicts,
            row_count: 0,
            memory_bytes: 0,
            config,
            min_ts: i64::MAX,
            max_ts: i64::MIN,
        }
    }

    /// Create a simple metrics memtable (timestamp + f64 value, no tags).
    pub fn new_metric(config: ColumnarMemtableConfig) -> Self {
        Self::new(ColumnarSchema::metric_default(), config)
    }

    // -- Accessors --

    pub fn row_count(&self) -> u64 {
        self.row_count
    }

    /// Approximate memory usage. Uses incremental tracking with periodic
    /// recomputation from column capacities for accuracy.
    pub fn memory_bytes(&self) -> usize {
        let col_bytes: usize = self.columns.iter().map(|c| c.memory_bytes()).sum();
        let dict_bytes: usize = self.symbol_dicts.len() * 256; // rough estimate
        self.memory_bytes.max(col_bytes + dict_bytes)
    }

    pub fn min_ts(&self) -> i64 {
        self.min_ts
    }

    pub fn max_ts(&self) -> i64 {
        self.max_ts
    }

    pub fn series_count(&self) -> usize {
        self.series_row_counts.len()
    }

    pub fn schema(&self) -> &ColumnarSchema {
        &self.schema
    }

    /// Return the immutable admission configuration used to construct this
    /// memtable. Transaction undo uses this with a snapshot so restoration
    /// preserves the original limits even if live operator tuning changed.
    pub fn config(&self) -> ColumnarMemtableConfig {
        self.config.clone()
    }

    pub fn column(&self, idx: usize) -> &ColumnData {
        &self.columns[idx]
    }

    pub fn symbol_dict(&self, col_idx: usize) -> Option<&SymbolDictionary> {
        self.symbol_dicts.get(&col_idx)
    }

    pub fn is_empty(&self) -> bool {
        self.row_count == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_config() -> ColumnarMemtableConfig {
        ColumnarMemtableConfig {
            max_memory_bytes: 1024 * 1024,
            hard_memory_limit: 2 * 1024 * 1024,
            max_tag_cardinality: 1000,
        }
    }

    #[test]
    fn empty_memtable() {
        let mt = ColumnarMemtable::new_metric(default_config());
        assert_eq!(mt.row_count(), 0);
        assert!(mt.is_empty());
        assert_eq!(mt.series_count(), 0);
    }
}
