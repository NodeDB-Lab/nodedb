// SPDX-License-Identifier: BUSL-1.1

//! Incremental refresh engine for continuous aggregates.
//!
//! Takes a `ColumnarDrainResult` (flushed data) and computes the partial
//! state of every touched `(time_bucket, group)` from those rows alone:
//! O(flushed_rows), not O(total_rows). The caller merges that delta into the
//! materialized state and rolls it up into downstream tiers.
//!
//! Cells feed the partial state exactly as the ad-hoc aggregate scan feeds
//! its accumulator: an `Int64` or `Timestamp` cell as an exact integer, a
//! `Float64` cell as a float, and a symbol cell not at all.

use std::collections::HashMap;

use super::definition::ContinuousAggregateDef;
use super::partial::{ColumnLayout, PartialAggregate};
use super::watermark::WatermarkState;
use crate::engine::timeseries::columnar_memtable::{ColumnData, ColumnarDrainResult};
use crate::engine::timeseries::time_bucket;

/// Partial buckets keyed by `(bucket_ts, group_key)`.
pub type Buckets = HashMap<(i64, Vec<u32>), PartialAggregate>;

/// Result of a single aggregate refresh.
pub struct RefreshResult {
    /// Number of input rows processed.
    pub rows_processed: u64,
    /// Highest timestamp seen in the flushed data.
    pub max_ts: i64,
    /// Oldest O3 timestamp (below watermark), if any.
    pub o3_min_ts: Option<i64>,
    /// Partial state of the flushed rows alone, to merge into the
    /// materialized state.
    pub delta: Buckets,
}

/// Compute the partial state of `drain`'s rows for `def`.
pub fn refresh_from_drain(
    def: &ContinuousAggregateDef,
    drain: &ColumnarDrainResult,
    watermark: &WatermarkState,
) -> RefreshResult {
    let bucket_ms = def.bucket_interval_ms;
    if bucket_ms <= 0 || drain.row_count == 0 {
        return RefreshResult {
            rows_processed: 0,
            max_ts: watermark.watermark_ts,
            o3_min_ts: None,
            delta: Buckets::new(),
        };
    }

    let layout = ColumnLayout::of(def);
    let ts_idx = drain.schema.timestamp_idx;
    let timestamps = drain.columns[ts_idx].as_timestamps();
    let column_index = |name: &str| {
        drain
            .schema
            .columns
            .iter()
            .position(|(column, _)| column == name)
    };

    // Drain column of each layout slot.
    let slot_columns: Vec<Option<&ColumnData>> = layout
        .columns()
        .iter()
        .map(|name| column_index(name).map(|idx| &drain.columns[idx]))
        .collect();

    // Drain column of each GROUP BY column.
    let group_columns: Vec<Option<&ColumnData>> = def
        .group_by
        .iter()
        .map(|name| column_index(name).map(|idx| &drain.columns[idx]))
        .collect();

    let current_watermark = watermark.watermark_ts;
    let mut max_ts = current_watermark;
    let mut o3_min: Option<i64> = None;
    let mut delta = Buckets::new();

    for row in 0..drain.row_count as usize {
        let ts = timestamps[row];
        let bucket = time_bucket::time_bucket(bucket_ms, ts);

        // O3 detection.
        if ts <= current_watermark && o3_min.is_none_or(|current| ts < current) {
            o3_min = Some(ts);
        }
        if ts > max_ts {
            max_ts = ts;
        }

        let group_key: Vec<u32> = group_columns
            .iter()
            .map(|column| match column {
                Some(ColumnData::Symbol(ids)) => ids[row],
                _ => 0,
            })
            .collect();

        let partial = delta
            .entry((bucket, group_key))
            .or_insert_with_key(|(bucket, key)| {
                PartialAggregate::new(*bucket, key.clone(), &layout)
            });
        partial.count += 1;
        for (slot, column) in slot_columns.iter().enumerate() {
            let state = &mut partial.columns[slot];
            match column {
                Some(ColumnData::Float64(v)) => state.add_float(ts, v[row]),
                Some(ColumnData::Int64(v)) | Some(ColumnData::Timestamp(v)) => {
                    state.add_int(ts, v[row])
                }
                Some(ColumnData::Symbol(_)) | Some(ColumnData::DictEncoded { .. }) | None => {}
            }
        }
    }

    RefreshResult {
        rows_processed: drain.row_count,
        max_ts,
        o3_min_ts: o3_min,
        delta,
    }
}

/// Merge `delta` into `materialized`. Both hold buckets of one layout.
pub fn merge_delta(materialized: &mut Buckets, delta: Buckets) {
    for (key, partial) in delta {
        match materialized.get_mut(&key) {
            Some(existing) => existing.merge(&partial),
            None => {
                materialized.insert(key, partial);
            }
        }
    }
}
