// SPDX-License-Identifier: BUSL-1.1

//! Taking rows out of a [`ColumnarMemtable`]: flush view, drain, truncate.

use nodedb_types::timeseries::SymbolDictionary;

use super::super::types::{
    ColumnData, ColumnType, ColumnarDrainResult, ColumnarFlushView, max_system_ts_of,
};
use super::table::ColumnarMemtable;

impl ColumnarMemtable {
    /// Borrow this memtable's live rows as the payload a flush would write.
    ///
    /// The read-only half of a flush: a segment can be encoded and landed from
    /// this view, and only then does [`Self::drain`] take the rows out. Nothing
    /// leaves memory before it is durable, so a failed segment write leaves the
    /// memtable exactly as it was.
    pub fn flush_view(&self) -> ColumnarFlushView<'_> {
        ColumnarFlushView {
            columns: &self.columns,
            schema: &self.schema,
            symbol_dicts: &self.symbol_dicts,
            row_count: self.row_count,
            min_ts: self.min_ts,
            max_ts: self.max_ts,
            max_system_ts: max_system_ts_of(&self.schema, &self.columns),
        }
    }

    /// Drain all data from the memtable, resetting it for reuse.
    ///
    /// Returns the column data, schema, symbol dicts, and stats.
    ///
    /// Callers that flush must write the segment from [`Self::flush_view`] FIRST
    /// and drain only once that write has committed — the rows here have no
    /// other copy but the WAL, and the checkpoint that calls the flush is what
    /// authorises deleting it.
    pub fn drain(&mut self) -> ColumnarDrainResult {
        let mut drained_columns = Vec::with_capacity(self.columns.len());
        for (col, (_, schema_type)) in self.columns.iter_mut().zip(self.schema.columns.iter()) {
            // DictEncoded columns are drained by swapping in a fresh Symbol
            // placeholder. The flusher converts Symbol → DictEncoded during
            // segment encoding once it has enough cardinality data.
            let col_type = match col {
                ColumnData::Timestamp(_) => *schema_type,
                ColumnData::Float64(_) => ColumnType::Float64,
                ColumnData::Int64(_) => ColumnType::Int64,
                ColumnData::Symbol(_) => ColumnType::Symbol,
                ColumnData::DictEncoded { .. } => ColumnType::Symbol,
            };
            let mut empty = ColumnData::new(col_type);
            std::mem::swap(col, &mut empty);
            drained_columns.push(empty);
        }

        let drained_dicts = std::mem::take(&mut self.symbol_dicts);
        // Reinitialize symbol dicts.
        for (i, (_, ty)) in self.schema.columns.iter().enumerate() {
            if *ty == ColumnType::Symbol {
                self.symbol_dicts.insert(i, SymbolDictionary::new());
            }
        }

        // Scan `_ts_system` column (if present) for retention's system-time axis.
        let max_system_ts = max_system_ts_of(&self.schema, &drained_columns);

        let result = ColumnarDrainResult {
            columns: drained_columns,
            schema: self.schema.clone(),
            symbol_dicts: drained_dicts,
            row_count: self.row_count,
            min_ts: self.min_ts,
            max_ts: self.max_ts,
            max_system_ts,
            series_row_counts: std::mem::take(&mut self.series_row_counts),
        };

        self.row_count = 0;
        self.memory_bytes = 0;
        self.min_ts = i64::MAX;
        self.max_ts = i64::MIN;

        result
    }

    /// Truncate this memtable back to `n` rows.
    ///
    /// Used during transaction rollback to reverse a `TimeseriesIngest` operation.
    /// All column vectors are truncated; aggregate stats are recomputed from the
    /// surviving rows. `series_row_counts` is rebuilt from scratch so per-series
    /// cardinality remains consistent.
    pub fn truncate_to(&mut self, n: u64) {
        if n >= self.row_count {
            return;
        }
        let n_usize = n as usize;
        let ts_idx = self.schema.timestamp_idx;
        for col in &mut self.columns {
            match col {
                ColumnData::Timestamp(v) | ColumnData::Int64(v) => v.truncate(n_usize),
                ColumnData::Float64(v) => v.truncate(n_usize),
                ColumnData::Symbol(v) => v.truncate(n_usize),
                ColumnData::DictEncoded { ids, valid, .. } => {
                    ids.truncate(n_usize);
                    valid.truncate(n_usize);
                }
            }
        }
        self.row_count = n;
        // Recompute ts range from surviving timestamps.
        if n == 0 {
            self.min_ts = i64::MAX;
            self.max_ts = i64::MIN;
            self.series_row_counts.clear();
        } else if let ColumnData::Timestamp(ts) = &self.columns[ts_idx] {
            self.min_ts = ts.iter().copied().min().unwrap_or(i64::MAX);
            self.max_ts = ts.iter().copied().max().unwrap_or(i64::MIN);
        }
        // Recompute memory_bytes estimate by re-summing column capacities.
        self.memory_bytes = self
            .columns
            .iter()
            .map(|c| match c {
                ColumnData::Timestamp(v) | ColumnData::Int64(v) => v.capacity() * 8,
                ColumnData::Float64(v) => v.capacity() * 8,
                ColumnData::Symbol(v) => v.capacity() * 4,
                ColumnData::DictEncoded {
                    ids,
                    valid,
                    dictionary,
                    ..
                } => ids.capacity() * 4 + valid.capacity() + dictionary.len() * 32,
            })
            .sum();
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::timeseries::MetricSample;

    use super::super::super::types::ColumnarMemtableConfig;
    use super::*;

    fn default_config() -> ColumnarMemtableConfig {
        ColumnarMemtableConfig {
            max_memory_bytes: 1024 * 1024,
            hard_memory_limit: 2 * 1024 * 1024,
            max_tag_cardinality: 1000,
        }
    }

    #[test]
    fn drain_returns_data_and_resets() {
        let mut mt = ColumnarMemtable::new_metric(default_config());
        for i in 0..50 {
            mt.ingest_metric(
                1,
                MetricSample {
                    timestamp_ms: 1000 + i,
                    value: i as f64,
                },
            );
        }
        assert_eq!(mt.row_count(), 50);

        let result = mt.drain();
        assert_eq!(result.row_count, 50);
        assert_eq!(result.min_ts, 1000);
        assert_eq!(result.max_ts, 1049);
        assert_eq!(result.columns.len(), 2);
        assert_eq!(result.columns[0].len(), 50);
        assert_eq!(result.columns[1].len(), 50);

        // Memtable is reset.
        assert_eq!(mt.row_count(), 0);
        assert!(mt.is_empty());
    }
}
