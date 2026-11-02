// SPDX-License-Identifier: BUSL-1.1

//! Split the rows of one memtable flush by partition interval.
//!
//! A flush writes one partition per interval its rows fall in. Every row of a
//! bucket lies in `[start, end)`. Rows keep their memtable order inside a
//! bucket. A bucket for an interval an earlier flush already sealed becomes a
//! new partition of that interval: a sealed partition is never rewritten.

use std::collections::BTreeMap;

use nodedb_types::timeseries::PartitionInterval;

use super::columnar_memtable::{ColumnData, ColumnarDrainResult, ColumnarFlushView};
use super::partition_registry::interval_bounds;

/// The rows of one flush that fall in one interval.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntervalBucket {
    /// Inclusive interval start, in milliseconds. `i64::MIN` for an interval
    /// that never splits.
    pub start: i64,
    /// Exclusive interval end, in milliseconds. `i64::MAX` for an interval
    /// that never splits.
    pub end: i64,
    /// Row indices into the flushed columns, ascending.
    pub rows: Vec<usize>,
}

impl IntervalBucket {
    /// The interval width in milliseconds a partition records, or `0` for an
    /// interval that never splits.
    pub fn interval_ms(&self) -> u64 {
        if self.start == i64::MIN || self.end == i64::MAX {
            return 0;
        }
        u64::try_from(self.end.saturating_sub(self.start)).unwrap_or(0)
    }
}

/// Group the rows of `view` by the interval of `interval` each time key falls
/// in, ordered by interval start. An interval that never splits, or a view
/// without a time column, yields one bucket holding every row.
pub fn split_by_interval(
    view: &ColumnarFlushView<'_>,
    interval: &PartitionInterval,
) -> Vec<IntervalBucket> {
    let every_row = || {
        vec![IntervalBucket {
            start: i64::MIN,
            end: i64::MAX,
            rows: (0..row_count(view)).collect(),
        }]
    };
    let Some(ColumnData::Timestamp(timestamps)) = view.columns.get(view.schema.timestamp_idx)
    else {
        return every_row();
    };
    let mut buckets: BTreeMap<(i64, i64), Vec<usize>> = BTreeMap::new();
    for (row, ts) in timestamps.iter().enumerate() {
        let Some(bounds) = interval_bounds(interval, *ts) else {
            return every_row();
        };
        buckets.entry(bounds).or_default().push(row);
    }
    buckets
        .into_iter()
        .map(|((start, end), rows)| IntervalBucket { start, end, rows })
        .collect()
}

/// A copy of the rows `bucket` names, as a drained payload the segment writer
/// and the continuous-aggregate fold both take.
pub fn gather_bucket(
    view: &ColumnarFlushView<'_>,
    bucket: &IntervalBucket,
) -> crate::Result<ColumnarDrainResult> {
    let columns = view
        .columns
        .iter()
        .map(|column| gather_column(column, &bucket.rows))
        .collect::<crate::Result<Vec<_>>>()?;
    let timestamps = match columns.get(view.schema.timestamp_idx) {
        Some(ColumnData::Timestamp(values)) => values.as_slice(),
        _ => &[],
    };
    let min_ts = timestamps.iter().copied().min().unwrap_or(view.min_ts);
    let max_ts = timestamps.iter().copied().max().unwrap_or(view.max_ts);
    let max_system_ts = super::columnar_memtable::max_system_ts_of(view.schema, &columns);
    Ok(ColumnarDrainResult {
        columns,
        schema: view.schema.clone(),
        symbol_dicts: view.symbol_dicts.clone(),
        row_count: bucket.rows.len() as u64,
        min_ts,
        max_ts,
        max_system_ts,
        series_row_counts: std::collections::HashMap::new(),
    })
}

/// The number of rows the view holds: the length of its time column, or of
/// its first column when it has none.
fn row_count(view: &ColumnarFlushView<'_>) -> usize {
    view.columns
        .get(view.schema.timestamp_idx)
        .or_else(|| view.columns.first())
        .map_or(0, ColumnData::len)
}

/// The values of `column` at `rows`, in order.
fn gather_column(column: &ColumnData, rows: &[usize]) -> crate::Result<ColumnData> {
    fn pick<T: Copy>(values: &[T], rows: &[usize]) -> crate::Result<Vec<T>> {
        rows.iter()
            .map(|&row| {
                values
                    .get(row)
                    .copied()
                    .ok_or_else(|| crate::Error::Internal {
                        detail: format!(
                            "flush split: row {row} is outside a column of {} rows",
                            values.len()
                        ),
                    })
            })
            .collect()
    }
    Ok(match column {
        ColumnData::Timestamp(v) => ColumnData::Timestamp(pick(v, rows)?),
        ColumnData::Float64(v) => ColumnData::Float64(pick(v, rows)?),
        ColumnData::Int64(v) => ColumnData::Int64(pick(v, rows)?),
        ColumnData::Symbol(v) => ColumnData::Symbol(pick(v, rows)?),
        ColumnData::DictEncoded {
            ids,
            dictionary,
            reverse,
            valid,
        } => ColumnData::DictEncoded {
            ids: pick(ids, rows)?,
            dictionary: dictionary.clone(),
            reverse: reverse.clone(),
            valid: pick(valid, rows)?,
        },
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::engine::timeseries::columnar_memtable::{ColumnType, ColumnarSchema, TimeKind};

    const HOUR: u64 = 3_600_000;

    fn schema() -> ColumnarSchema {
        ColumnarSchema {
            columns: vec![
                ("ts".into(), ColumnType::Timestamp(TimeKind::Millis)),
                ("value".into(), ColumnType::Float64),
            ],
            timestamp_idx: 0,
            codecs: Vec::new(),
        }
    }

    #[test]
    fn rows_split_at_each_interval_boundary() {
        let schema = schema();
        let columns = vec![
            ColumnData::Timestamp(vec![10, 3_600_005, 20, 7_200_000]),
            ColumnData::Float64(vec![1.0, 2.0, 3.0, 4.0]),
        ];
        let dicts = HashMap::new();
        let view = ColumnarFlushView {
            columns: &columns,
            schema: &schema,
            symbol_dicts: &dicts,
            row_count: 4,
            min_ts: 10,
            max_ts: 7_200_000,
            max_system_ts: 0,
        };
        let buckets = split_by_interval(&view, &PartitionInterval::Duration(HOUR));
        assert_eq!(buckets.len(), 3);
        assert_eq!(buckets[0].rows, vec![0, 2]);
        assert_eq!((buckets[0].start, buckets[0].end), (0, 3_600_000));
        assert_eq!(buckets[1].rows, vec![1]);
        assert_eq!(buckets[2].rows, vec![3]);
        assert_eq!(buckets[0].interval_ms(), HOUR);

        let first = gather_bucket(&view, &buckets[0]).expect("gather");
        assert_eq!(first.row_count, 2);
        assert_eq!((first.min_ts, first.max_ts), (10, 20));
        match &first.columns[1] {
            ColumnData::Float64(values) => assert_eq!(values, &vec![1.0, 3.0]),
            other => panic!("expected Float64, got {other:?}"),
        }
    }

    /// A continuous aggregate fed one flush split into hourly partitions
    /// equals the aggregate computed over every row at once.
    #[test]
    fn an_aggregate_over_a_split_flush_equals_the_ad_hoc_result() {
        use crate::engine::timeseries::continuous_agg::test_support::{
            T0, ad_hoc, exact_def, exact_rows, int_drain, loaded_manager, materialized,
        };

        let rows = exact_rows();
        let drain = int_drain(&rows);
        let view = drain.view();
        let buckets = split_by_interval(&view, &PartitionInterval::Duration(HOUR));
        assert!(buckets.len() >= 2, "the rows span two hours: {buckets:?}");

        let mut mgr = loaded_manager(vec![
            exact_def("v_1m", "metrics", "1m"),
            exact_def("v_1h", "v_1m", "1h"),
        ]);
        for (index, bucket) in buckets.iter().enumerate() {
            let part = gather_bucket(&view, bucket).expect("gather");
            mgr.on_flush(0, 1, "metrics", &format!("p{index}"), &part, T0);
        }
        assert_eq!(materialized(&mgr, "v_1m"), ad_hoc(&rows, 60_000));
        assert_eq!(materialized(&mgr, "v_1h"), ad_hoc(&rows, 3_600_000));
    }

    #[test]
    fn an_unbounded_interval_keeps_every_row_together() {
        let schema = schema();
        let columns = vec![
            ColumnData::Timestamp(vec![10, 99_999_999]),
            ColumnData::Float64(vec![1.0, 2.0]),
        ];
        let dicts = HashMap::new();
        let view = ColumnarFlushView {
            columns: &columns,
            schema: &schema,
            symbol_dicts: &dicts,
            row_count: 2,
            min_ts: 10,
            max_ts: 99_999_999,
            max_system_ts: 0,
        };
        let buckets = split_by_interval(&view, &PartitionInterval::Unbounded);
        assert_eq!(buckets.len(), 1);
        assert_eq!(buckets[0].rows, vec![0, 1]);
        assert_eq!(buckets[0].interval_ms(), 0);
    }
}
