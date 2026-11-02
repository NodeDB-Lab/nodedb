// SPDX-License-Identifier: BUSL-1.1

//! Incremental refresh engine for continuous aggregates.
//!
//! Computes the partial state of every touched `(time_bucket, group)` from
//! one batch of source rows alone: a flush drain, or one sealed partition
//! read back. The work is O(rows in the batch), not O(total rows). The
//! caller merges the delta into the materialized state and rolls it up into
//! downstream tiers.
//!
//! Cells feed the partial state exactly as the ad-hoc aggregate scan feeds
//! its accumulator: an `Int64` or `Timestamp` cell as an exact integer, a
//! `Float64` cell as a float, and a symbol cell not at all. A GROUP BY cell
//! keys by its value (see [`GroupCell`]).

use std::collections::HashMap;

use nodedb_types::timeseries::SymbolDictionary;

use super::definition::ContinuousAggregateDef;
use super::partial::{ColumnLayout, GroupCell, PartialAggregate};
use crate::engine::timeseries::columnar_memtable::{
    ColumnData, ColumnType, ColumnarDrainResult, ColumnarSchema, TimeKind,
};
use crate::engine::timeseries::time_bucket;

/// Partial buckets keyed by `(bucket_ts, group_key)`.
pub type Buckets = HashMap<(i64, Vec<GroupCell>), PartialAggregate>;

/// One batch of source rows to fold: a flush drain or one sealed partition.
pub struct RefreshSource<'a> {
    pub schema: &'a ColumnarSchema,
    /// One column per schema column, in schema order.
    pub columns: &'a [ColumnData],
    /// Dictionary of each symbol column that has one, by column index.
    pub symbol_dicts: &'a HashMap<usize, SymbolDictionary>,
    pub row_count: usize,
}

impl<'a> RefreshSource<'a> {
    /// The rows a flush drained.
    pub fn of_drain(drain: &'a ColumnarDrainResult) -> Self {
        Self {
            schema: &drain.schema,
            columns: &drain.columns,
            symbol_dicts: &drain.symbol_dicts,
            row_count: usize::try_from(drain.row_count).unwrap_or(usize::MAX),
        }
    }

    /// The kind of the designated time column.
    pub fn time_kind(&self) -> Option<TimeKind> {
        match self.schema.columns.get(self.schema.timestamp_idx) {
            Some((_, ColumnType::Timestamp(kind))) => Some(*kind),
            _ => None,
        }
    }

    /// The `_ts_system` ordinal of each row, bounded by `row_count`. Empty
    /// when the schema has no such column.
    fn ordinals(&self) -> &'a [i64] {
        match self
            .schema
            .ts_system_idx()
            .and_then(|idx| self.columns.get(idx))
        {
            Some(ColumnData::Int64(v)) => &v[..v.len().min(self.row_count)],
            _ => &[],
        }
    }

    /// The designated time column's cells, bounded by `row_count`.
    fn timestamps(&self) -> &'a [i64] {
        match self.columns.get(self.schema.timestamp_idx) {
            Some(ColumnData::Timestamp(v)) => &v[..v.len().min(self.row_count)],
            _ => &[],
        }
    }
}

/// Result of folding one batch of rows into one aggregate's layout.
pub struct RefreshResult {
    /// Number of input rows processed.
    pub rows_processed: u64,
    /// Highest timestamp in the batch, or `i64::MIN` for an empty batch.
    pub max_ts: i64,
    /// Partial state of the batch alone, to merge into the materialized
    /// state.
    pub delta: Buckets,
}

/// Compute the partial state of `src`'s rows for `def`.
pub fn refresh_rows(def: &ContinuousAggregateDef, src: &RefreshSource<'_>) -> RefreshResult {
    refresh_rows_where(def, src, &|_, _| true)
}

/// Which rows of a batch a read at system-time ordinal `at` folds: those
/// written at or below `at` whose time-key value `keep_ts` accepts.
pub struct RowCut<'a> {
    pub at: i64,
    pub keep_ts: &'a dyn Fn(i64) -> bool,
}

/// Compute the partial state of the rows of `src` that `cut` selects. A row
/// with no `_ts_system` cell counts as written at ordinal `0`.
pub fn refresh_rows_at(
    def: &ContinuousAggregateDef,
    src: &RefreshSource<'_>,
    cut: &RowCut<'_>,
) -> RefreshResult {
    let ordinals = src.ordinals();
    refresh_rows_where(def, src, &|row, ts| {
        ordinals.get(row).copied().unwrap_or(0) <= cut.at && (cut.keep_ts)(ts)
    })
}

/// Compute the partial state of the rows of `src` that `keep(row, ts)`
/// accepts.
fn refresh_rows_where(
    def: &ContinuousAggregateDef,
    src: &RefreshSource<'_>,
    keep: &dyn Fn(usize, i64) -> bool,
) -> RefreshResult {
    let timestamps = src.timestamps();
    let bucket_ms = def.bucket_interval_ms;
    if bucket_ms <= 0 || timestamps.is_empty() {
        return RefreshResult {
            rows_processed: 0,
            max_ts: i64::MIN,
            delta: Buckets::new(),
        };
    }

    let layout = ColumnLayout::of(def);
    let column_index = |name: &str| {
        src.schema
            .columns
            .iter()
            .position(|(column, _)| column == name)
    };

    // Source column of each layout slot.
    let slot_columns: Vec<Option<&ColumnData>> = layout
        .columns()
        .iter()
        .map(|name| column_index(name).and_then(|idx| src.columns.get(idx)))
        .collect();

    // Source column and dictionary of each GROUP BY column.
    let group_columns: Vec<(Option<&ColumnData>, Option<&SymbolDictionary>)> = def
        .group_by
        .iter()
        .map(|name| match column_index(name) {
            Some(idx) => (src.columns.get(idx), src.symbol_dicts.get(&idx)),
            None => (None, None),
        })
        .collect();

    // The `_ts_system` ordinal of each row, which every timeseries row
    // carries. A bucket records the newest it holds.
    let ordinals = src.ordinals();

    let mut max_ts = i64::MIN;
    let mut delta = Buckets::new();
    let mut rows_processed = 0u64;

    for (row, &ts) in timestamps.iter().enumerate() {
        if !keep(row, ts) {
            continue;
        }
        rows_processed += 1;
        let bucket = time_bucket::time_bucket(bucket_ms, ts);
        max_ts = max_ts.max(ts);

        let group_key: Vec<GroupCell> = group_columns
            .iter()
            .map(|(column, dict)| GroupCell::of(*column, *dict, row))
            .collect();

        let partial = delta
            .entry((bucket, group_key))
            .or_insert_with_key(|(bucket, key)| {
                PartialAggregate::new(*bucket, key.clone(), &layout)
            });
        partial.count += 1;
        if let Some(&ordinal) = ordinals.get(row) {
            partial.max_ordinal = partial.max_ordinal.max(ordinal);
        }
        for (slot, column) in slot_columns.iter().enumerate() {
            let Some(state) = partial.columns.get_mut(slot) else {
                continue;
            };
            match column {
                Some(ColumnData::Float64(v)) => {
                    if let Some(x) = v.get(row) {
                        state.add_float(ts, *x);
                    }
                }
                Some(ColumnData::Int64(v)) | Some(ColumnData::Timestamp(v)) => {
                    if let Some(x) = v.get(row) {
                        state.add_int(ts, *x);
                    }
                }
                Some(ColumnData::Symbol(_)) | Some(ColumnData::DictEncoded { .. }) | None => {}
            }
        }
    }

    RefreshResult {
        rows_processed,
        max_ts,
        delta,
    }
}

/// Merge `delta` into `materialized`. Both hold buckets of `layout`.
/// `delta` stays intact, so the caller can roll it up further.
pub fn merge_delta(materialized: &mut Buckets, delta: &Buckets, layout: &ColumnLayout) {
    for ((bucket, key), partial) in delta {
        materialized
            .entry((*bucket, key.clone()))
            .or_insert_with(|| PartialAggregate::new(*bucket, key.clone(), layout))
            .merge(partial);
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::columnar::schema::TS_SYSTEM;

    use super::*;
    use crate::engine::timeseries::continuous_agg::test_support::{T0, exact_def};

    /// Each bucket records the newest `_ts_system` ordinal of the rows it
    /// folds, and a merge keeps the newer of two.
    #[test]
    fn a_bucket_records_its_newest_row_ordinal() {
        let schema = ColumnarSchema {
            columns: vec![
                ("timestamp".into(), ColumnType::Timestamp(TimeKind::Millis)),
                ("v".into(), ColumnType::Int64),
                (TS_SYSTEM.into(), ColumnType::Int64),
            ],
            timestamp_idx: 0,
            codecs: vec![nodedb_codec::ColumnCodec::Auto; 3],
        };
        let columns = vec![
            ColumnData::Timestamp(vec![T0, T0 + 1_000, T0 + 3_600_000]),
            ColumnData::Int64(vec![1, 2, 3]),
            ColumnData::Int64(vec![10, 30, 20]),
        ];
        let dicts = HashMap::new();
        let src = RefreshSource {
            schema: &schema,
            columns: &columns,
            symbol_dicts: &dicts,
            row_count: 3,
        };
        let result = refresh_rows(&exact_def("t", "s", "1m"), &src);
        let mut ordinals: Vec<(i64, i64)> = result
            .delta
            .values()
            .map(|bucket| (bucket.bucket_ts, bucket.max_ordinal))
            .collect();
        ordinals.sort_unstable();
        assert_eq!(
            ordinals.iter().map(|(_, o)| *o).collect::<Vec<_>>(),
            vec![30, 20]
        );

        let mut first =
            PartialAggregate::new(0, Vec::new(), &ColumnLayout::of(&exact_def("t", "s", "1m")));
        let mut second =
            PartialAggregate::new(0, Vec::new(), &ColumnLayout::of(&exact_def("t", "s", "1m")));
        first.max_ordinal = 5;
        second.max_ordinal = 9;
        first.merge(&second);
        assert_eq!(first.max_ordinal, 9);
    }
}
