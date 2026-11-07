// SPDX-License-Identifier: BUSL-1.1

//! Rebuilding a last-value cache from stored rows.
//!
//! The cache keeps, per series, the row a live update leaves: the
//! newest timestamp, and of two equal timestamps the later write. Every
//! stored row names its series in `_ts_series` and its write in
//! `_ts_system`, so a scan over the stored rows finds the same row: the
//! greatest `(timestamp, ordinal)`, and of two equal pairs the later row in
//! scan order. The caller scans partitions oldest first, then the memtable.
//!
//! The value is the row's first Float64 cell that is not NaN, `0.0` when
//! none: the value a live update records
//! ([`super::ilp_ingest::update_last_value`]).

use std::collections::HashMap;

use nodedb_types::timeseries::SeriesId;

use super::columnar_memtable::{ColumnData, ColumnType, ColumnarSchema};
use super::last_value_cache::LastValueCache;

/// The newest row of one series seen so far.
#[derive(Debug, Clone, Copy)]
struct Newest {
    ts: i64,
    ordinal: i64,
    value: f64,
}

/// The newest row of every series across the batches folded so far.
#[derive(Debug, Default)]
pub struct LastValueScan {
    newest: HashMap<SeriesId, Newest>,
}

impl LastValueScan {
    pub fn new() -> Self {
        Self::default()
    }

    /// Fold the first `row_count` rows of one batch: a partition or the
    /// memtable. `source` names the batch in an error.
    ///
    /// A batch with rows and no `_ts_series` column is an error: its rows
    /// name no series, so the cache cannot hold their values. A batch with no
    /// `_ts_system` column is an error too: every schema gains both columns
    /// together, and ordinal `0` would lose a tie to an older write. So is a
    /// column shorter than the batch, or a float column of another type.
    pub fn fold(
        &mut self,
        source: &str,
        schema: &ColumnarSchema,
        columns: &[ColumnData],
        row_count: usize,
    ) -> crate::Result<()> {
        if row_count == 0 {
            return Ok(());
        }
        let missing = |what: &str| crate::Error::SegmentCorrupted {
            detail: format!("last-value cache rebuild: {source} has no {what} column"),
        };
        let timestamps = match columns.get(schema.timestamp_idx) {
            Some(ColumnData::Timestamp(v)) => v.as_slice(),
            _ => return Err(missing("time")),
        };
        let series = match schema.ts_series_idx().and_then(|idx| columns.get(idx)) {
            Some(ColumnData::Int64(v)) => v.as_slice(),
            _ => return Err(missing("_ts_series")),
        };
        let ordinals = match schema.ts_system_idx().and_then(|idx| columns.get(idx)) {
            Some(ColumnData::Int64(v)) => v.as_slice(),
            _ => return Err(missing("_ts_system")),
        };
        let short = |what: &str| crate::Error::SegmentCorrupted {
            detail: format!(
                "last-value cache rebuild: {source} column '{what}' holds fewer than \
                 {row_count} rows"
            ),
        };
        let mut floats: Vec<&[f64]> = Vec::new();
        for ((name, ty), column) in schema.columns.iter().zip(columns) {
            if !matches!(ty, ColumnType::Float64) {
                continue;
            }
            match column {
                ColumnData::Float64(v) if v.len() >= row_count => floats.push(v.as_slice()),
                _ => return Err(short(name)),
            }
        }
        if timestamps.len() < row_count {
            return Err(short("time"));
        }
        if series.len() < row_count {
            return Err(short("_ts_series"));
        }
        if ordinals.len() < row_count {
            return Err(short("_ts_system"));
        }
        let rows = timestamps
            .iter()
            .zip(series)
            .zip(ordinals)
            .take(row_count)
            .enumerate();
        for (row, ((&ts, &id), &ordinal)) in rows {
            let id = SeriesId::from_ne_bytes(id.to_ne_bytes());
            // A row with no value cell records `0.0`, as a live update does.
            let value = floats
                .iter()
                .map(|column| column[row])
                .find(|value| !value.is_nan())
                .unwrap_or(0.0);
            let next = Newest { ts, ordinal, value };
            let slot = self.newest.entry(id).or_insert(next);
            if (ts, ordinal) >= (slot.ts, slot.ordinal) {
                *slot = next;
            }
        }
        Ok(())
    }

    /// The cache these rows make, built at ordinal `at`: a read below `at`
    /// is refused, since the cache holds no version older than its build.
    pub fn into_cache(self, at: i64) -> LastValueCache {
        let mut cache = LastValueCache::with_capacity(self.newest.len());
        for (id, newest) in self.newest {
            cache.update(id, newest.ts, newest.value, newest.ordinal);
        }
        cache.mark_rebuilt(at);
        cache
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::timeseries::columnar_memtable::TimeKind;
    use nodedb_types::columnar::schema::{TS_SERIES, TS_SYSTEM};

    fn schema() -> ColumnarSchema {
        ColumnarSchema {
            columns: vec![
                ("ts".into(), ColumnType::Timestamp(TimeKind::Millis)),
                ("a".into(), ColumnType::Float64),
                ("b".into(), ColumnType::Float64),
                (TS_SYSTEM.into(), ColumnType::Int64),
                (TS_SERIES.into(), ColumnType::Int64),
            ],
            timestamp_idx: 0,
            codecs: vec![nodedb_codec::ColumnCodec::Auto; 5],
        }
    }

    fn batch(rows: &[(i64, f64, f64, i64, i64)]) -> Vec<ColumnData> {
        vec![
            ColumnData::Timestamp(rows.iter().map(|r| r.0).collect()),
            ColumnData::Float64(rows.iter().map(|r| r.1).collect()),
            ColumnData::Float64(rows.iter().map(|r| r.2).collect()),
            ColumnData::Int64(rows.iter().map(|r| r.3).collect()),
            ColumnData::Int64(rows.iter().map(|r| r.4).collect()),
        ]
    }

    /// The newest timestamp wins, a later write wins a tie, and the value
    /// is the first float cell that is not NaN.
    #[test]
    fn the_rebuild_keeps_the_row_a_live_update_keeps() {
        let mut scan = LastValueScan::new();
        let first = batch(&[
            (200, 1.0, 9.0, 5, 1),
            (100, 2.0, 9.0, 6, 1),
            (50, 3.0, 9.0, 7, 2),
        ]);
        scan.fold("p1", &schema(), &first, 3).expect("fold");
        let second = batch(&[(200, f64::NAN, 4.0, 8, 1)]);
        scan.fold("memtable", &schema(), &second, 1).expect("fold");
        let cache = scan.into_cache(10);

        let one = cache.get(1).expect("series 1");
        assert_eq!((one.ts, one.value, one.ordinal), (200, 4.0, 8));
        let two = cache.get(2).expect("series 2");
        assert_eq!((two.ts, two.value, two.ordinal), (50, 3.0, 7));
        assert!(cache.all_at(9).is_err(), "no version older than the build");
        assert_eq!(cache.all_at(10).expect("reachable").len(), 2);
    }

    #[test]
    fn rows_without_a_series_column_are_refused() {
        let mut columns = batch(&[(1, 1.0, 1.0, 1, 1)]);
        columns.pop();
        let mut short = schema();
        short.columns.pop();
        short.codecs.pop();
        assert!(LastValueScan::new().fold("p", &short, &columns, 1).is_err());
    }

    /// Ordinal `0` would lose a tie to an older write, so a missing or short
    /// `_ts_system` column is an error and never a default.
    #[test]
    fn rows_without_their_ordinal_are_refused() {
        let mut no_ordinal_schema = schema();
        no_ordinal_schema.columns.remove(3);
        no_ordinal_schema.codecs.remove(3);
        let mut no_ordinal = batch(&[(1, 1.0, 1.0, 1, 1)]);
        no_ordinal.remove(3);
        let refused = LastValueScan::new().fold("p", &no_ordinal_schema, &no_ordinal, 1);
        assert!(matches!(
            refused,
            Err(crate::Error::SegmentCorrupted { .. })
        ));

        let rows = batch(&[(1, 1.0, 1.0, 1, 1)]);
        let refused = LastValueScan::new().fold("p", &schema(), &rows, 2);
        assert!(matches!(
            refused,
            Err(crate::Error::SegmentCorrupted { .. })
        ));
    }

    #[test]
    fn a_short_float_column_is_refused() {
        let mut rows = batch(&[(1, 1.0, 1.0, 1, 1), (2, 2.0, 2.0, 2, 1)]);
        rows[2] = ColumnData::Float64(vec![1.0]);
        let refused = LastValueScan::new().fold("p", &schema(), &rows, 2);
        assert!(matches!(
            refused,
            Err(crate::Error::SegmentCorrupted { .. })
        ));
    }
}
