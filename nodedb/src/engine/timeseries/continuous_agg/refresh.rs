//! Incremental refresh engine for continuous aggregates.
//!
//! Takes a `ColumnarDrainResult` (flushed data) and computes aggregates
//! by time_bucket + GROUP BY columns. O(flushed_rows), not O(total_rows).

use std::collections::HashMap;

use super::definition::ContinuousAggregateDef;
use super::partial::PartialAggregate;
use super::watermark::WatermarkState;
use crate::engine::timeseries::columnar_memtable::{ColumnData, ColumnarDrainResult};
use crate::engine::timeseries::time_bucket;

/// Result of a single aggregate refresh.
pub struct RefreshResult {
    /// Number of input rows processed.
    pub rows_processed: u64,
    /// Highest timestamp seen in the flushed data.
    pub max_ts: i64,
    /// Oldest O3 timestamp (below watermark), if any.
    pub o3_min_ts: Option<i64>,
}

/// Incrementally refresh an aggregate from flushed data.
///
/// Merges new samples into the existing materialized state.
pub fn refresh_from_drain(
    def: &ContinuousAggregateDef,
    drain: &ColumnarDrainResult,
    watermark: &WatermarkState,
    materialized: &mut HashMap<(i64, Vec<u32>), PartialAggregate>,
) -> RefreshResult {
    let bucket_ms = def.bucket_interval_ms;
    if bucket_ms <= 0 || drain.row_count == 0 {
        return RefreshResult {
            rows_processed: 0,
            max_ts: watermark.watermark_ts,
            o3_min_ts: None,
        };
    }

    let ts_idx = drain.schema.timestamp_idx;
    let timestamps = drain.columns[ts_idx].as_timestamps();

    // Find value column index for the first aggregate expression.
    // All expressions reference columns from the same drain.
    let value_col_idx = def
        .aggregates
        .iter()
        .find(|e| e.source_column != "*")
        .and_then(|e| {
            drain
                .schema
                .columns
                .iter()
                .position(|(name, _)| name == &e.source_column)
        });

    // Find GROUP BY column indices.
    let group_col_indices: Vec<Option<usize>> = def
        .group_by
        .iter()
        .map(|col_name| {
            drain
                .schema
                .columns
                .iter()
                .position(|(name, _)| name == col_name)
        })
        .collect();

    let current_watermark = watermark.watermark_ts;
    let mut max_ts = current_watermark;
    let mut o3_min: Option<i64> = None;

    for row in 0..drain.row_count as usize {
        let ts = timestamps[row];
        let bucket = time_bucket::time_bucket(bucket_ms, ts);

        // O3 detection.
        if ts <= current_watermark {
            match o3_min {
                Some(current) if ts < current => o3_min = Some(ts),
                None => o3_min = Some(ts),
                _ => {}
            }
        }
        if ts > max_ts {
            max_ts = ts;
        }

        // Build group key.
        let group_key: Vec<u32> = group_col_indices
            .iter()
            .map(|opt_idx| match opt_idx {
                Some(idx) => match &drain.columns[*idx] {
                    ColumnData::Symbol(v) => v[row],
                    _ => 0,
                },
                None => 0,
            })
            .collect();

        // Get value.
        let val = value_col_idx
            .map(|idx| match &drain.columns[idx] {
                ColumnData::Float64(v) => v[row],
                ColumnData::Int64(v) => v[row] as f64,
                ColumnData::Timestamp(v) => v[row] as f64,
                ColumnData::Symbol(_) => 0.0,
            })
            .unwrap_or(1.0); // COUNT(*)

        // Merge into materialized state.
        let key = (bucket, group_key.clone());
        match materialized.get_mut(&key) {
            Some(partial) => partial.merge_sample(ts, val),
            None => {
                materialized.insert(key, PartialAggregate::new(bucket, group_key, ts, val));
            }
        }
    }

    RefreshResult {
        rows_processed: drain.row_count,
        max_ts,
        o3_min_ts: o3_min,
    }
}
