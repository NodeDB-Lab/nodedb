// SPDX-License-Identifier: BUSL-1.1

//! Taking rows out of a [`ColumnarMemtable`]: flush view, drain, truncate.

use nodedb_types::timeseries::{SeriesId, SeriesKey, SymbolDictionary};

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
                ColumnData::Symbol(_) | ColumnData::DictEncoded { .. } => *schema_type,
            };
            let mut empty = ColumnData::new(col_type);
            std::mem::swap(col, &mut empty);
            drained_columns.push(empty);
        }

        let drained_dicts = std::mem::take(&mut self.symbol_dicts);
        // Reinitialize symbol dicts.
        for (i, (_, ty)) in self.schema.columns.iter().enumerate() {
            if ty.is_dictionary() {
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

    /// Truncate this memtable back to its first `n` rows.
    ///
    /// Every column is truncated. Time range, memory and the per-series
    /// counts are recomputed from the rows that stay. `known` is each series
    /// the memtable counted with its catalog key: a row's series is derived
    /// from its tags as ingest derives it ([`Self::recount_series`]).
    pub fn truncate_to(&mut self, n: u64, known: &[(SeriesId, SeriesKey)]) {
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
        } else if let Some(ColumnData::Timestamp(ts)) = self.columns.get(ts_idx) {
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
            .sum::<usize>()
            + super::table::dictionary_string_bytes(&self.symbol_dicts);
        self.recount_series(known);
    }

    /// Remove the first `n` rows, the rows a committed flush holds, and keep
    /// the rows ingested after it.
    ///
    /// Time range, memory and the per-series counts are recomputed from the
    /// rows that stay. `known` is each series the memtable counted with its
    /// catalog key: a row's series is derived from its tags as ingest derives
    /// it ([`Self::recount_series`]).
    pub fn drain_front(&mut self, n: u64, known: &[(SeriesId, SeriesKey)]) {
        if n >= self.row_count {
            self.drain();
            return;
        }
        let n_usize = n as usize;
        for col in &mut self.columns {
            match col {
                ColumnData::Timestamp(v) | ColumnData::Int64(v) => {
                    *v = v.split_off(n_usize.min(v.len()));
                }
                ColumnData::Float64(v) => {
                    *v = v.split_off(n_usize.min(v.len()));
                }
                ColumnData::Symbol(v) => {
                    *v = v.split_off(n_usize.min(v.len()));
                }
                ColumnData::DictEncoded { ids, valid, .. } => {
                    *ids = ids.split_off(n_usize.min(ids.len()));
                    *valid = valid.split_off(n_usize.min(valid.len()));
                }
            }
        }
        self.row_count -= n;
        let ts_idx = self.schema.timestamp_idx;
        if let Some(ColumnData::Timestamp(ts)) = self.columns.get(ts_idx) {
            self.min_ts = ts.iter().copied().min().unwrap_or(i64::MAX);
            self.max_ts = ts.iter().copied().max().unwrap_or(i64::MIN);
        }
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
            .sum::<usize>()
            + super::table::dictionary_string_bytes(&self.symbol_dicts);
        self.recount_series(known);
    }
}

#[cfg(test)]
mod tests {
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
            use super::super::super::types::ColumnValue;
            mt.ingest_row(
                1,
                &[
                    ColumnValue::Timestamp(1000 + i),
                    ColumnValue::Float64(i as f64),
                ],
            )
            .expect("ingest");
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

    #[test]
    fn drain_front_keeps_the_later_rows() {
        use super::super::super::types::{ColumnType, ColumnValue, ColumnarSchema, TimeKind};

        let schema = ColumnarSchema {
            columns: vec![
                ("ts".into(), ColumnType::Timestamp(TimeKind::Millis)),
                ("host".into(), ColumnType::Symbol),
                ("value".into(), ColumnType::Float64),
            ],
            timestamp_idx: 0,
            codecs: Vec::new(),
        };
        let key = |host: &str| SeriesKey::new("cpu", vec![("host".into(), host.into())]);
        let known = vec![(1, key("a")), (2, key("b")), (3, key("c"))];
        let mut mt = ColumnarMemtable::new(schema, default_config());
        // Rows 0..4 are hosts a and b, rows 4..10 hosts b and c.
        let hosts = ["a", "a", "b", "a", "b", "c", "c", "b", "c", "c"];
        for (i, host) in hosts.iter().enumerate() {
            let series = match *host {
                "a" => 1,
                "b" => 2,
                _ => 3,
            };
            mt.ingest_row(
                series,
                &[
                    ColumnValue::Timestamp(1000 + i as i64),
                    ColumnValue::Symbol((*host).into()),
                    ColumnValue::Float64(i as f64),
                ],
            )
            .expect("ingest");
        }
        assert_eq!(mt.series_count(), 3);

        mt.drain_front(4, &known);
        assert_eq!(mt.row_count(), 6);
        assert_eq!((mt.min_ts(), mt.max_ts()), (1004, 1009));
        assert_eq!(mt.series_count(), 2, "host a has no row left");
        let mut ids = mt.series_ids();
        ids.sort_unstable();
        assert_eq!(ids, vec![2, 3]);
        let rest = mt.drain();
        assert_eq!(rest.columns[0].len(), 6);
        assert_eq!(rest.columns[0].as_timestamps()[0], 1004);
        assert_eq!(rest.series_row_counts.get(&2), Some(&2));
        assert_eq!(rest.series_row_counts.get(&3), Some(&4));

        let mut metric = ColumnarMemtable::new_metric(default_config());
        metric
            .ingest_row(1, &[ColumnValue::Timestamp(5), ColumnValue::Float64(0.0)])
            .expect("ingest");
        metric.drain_front(1, &[]);
        assert!(metric.is_empty());
        assert_eq!(metric.series_count(), 0);
    }

    #[test]
    fn truncate_to_recounts_the_series_of_the_rows_kept() {
        use super::super::super::types::{ColumnType, ColumnValue, ColumnarSchema, TimeKind};

        let schema = ColumnarSchema {
            columns: vec![
                ("ts".into(), ColumnType::Timestamp(TimeKind::Millis)),
                ("host".into(), ColumnType::Symbol),
                ("value".into(), ColumnType::Float64),
            ],
            timestamp_idx: 0,
            codecs: Vec::new(),
        };
        let key = |host: &str| SeriesKey::new("cpu", vec![("host".into(), host.into())]);
        let known = vec![(1, key("a")), (2, key("b"))];
        let mut mt = ColumnarMemtable::new(schema, default_config());
        for (i, (series, host)) in [(1, "a"), (1, "a"), (2, "b")].iter().enumerate() {
            mt.ingest_row(
                *series,
                &[
                    ColumnValue::Timestamp(1000 + i as i64),
                    ColumnValue::Symbol((*host).into()),
                    ColumnValue::Float64(0.0),
                ],
            )
            .expect("ingest");
        }
        assert_eq!(mt.series_count(), 2);

        mt.truncate_to(2, &known);
        assert_eq!(mt.row_count(), 2);
        assert_eq!((mt.min_ts(), mt.max_ts()), (1000, 1001));
        assert_eq!(mt.series_ids(), vec![1], "host b has no row left");

        mt.truncate_to(0, &known);
        assert!(mt.is_empty());
        assert_eq!(mt.series_count(), 0);
    }
}
