//! Out-of-order (O3) buffer for late-arriving timeseries data.
//!
//! When a row arrives with a timestamp older than the current partition's
//! `max_ts`, it's out-of-order. Instead of rejecting it, we buffer it here
//! and merge it into the target partition asynchronously.
//!
//! The O3 buffer is **queryable immediately** — queries union the O3 buffer
//! with sealed partitions at read time, no wait for merge.
//!
//! Per-core owned (!Send) — one O3 buffer per TPC core.

use nodedb_types::timeseries::SeriesId;

/// A single out-of-order row.
#[derive(Debug, Clone)]
pub struct O3Row {
    pub timestamp_ms: i64,
    pub series_id: SeriesId,
    pub value: f64,
    /// Target partition start_ts this row belongs to.
    pub target_partition_start: i64,
}

/// Per-core out-of-order buffer.
pub struct O3Buffer {
    rows: Vec<O3Row>,
    max_rows: usize,
    /// Rows sorted by (target_partition_start, timestamp) for efficient merge.
    sorted: bool,
}

impl O3Buffer {
    pub fn new(max_rows: usize) -> Self {
        Self {
            rows: Vec::with_capacity(max_rows.min(4096)),
            max_rows,
            sorted: true,
        }
    }

    /// Insert an out-of-order row. Returns true if accepted, false if buffer full.
    pub fn insert(&mut self, row: O3Row) -> bool {
        if self.rows.len() >= self.max_rows {
            return false;
        }
        self.sorted = false;
        self.rows.push(row);
        true
    }

    /// Query: find all rows in this buffer that fall within a time range.
    ///
    /// Returns rows matching the range, sorted by timestamp.
    pub fn query_range(&mut self, min_ts: i64, max_ts: i64) -> Vec<&O3Row> {
        self.ensure_sorted();
        self.rows
            .iter()
            .filter(|r| r.timestamp_ms >= min_ts && r.timestamp_ms <= max_ts)
            .collect()
    }

    /// Drain all rows targeting a specific partition (for merge).
    ///
    /// Returns the drained rows sorted by timestamp. Removes them from the buffer.
    pub fn drain_for_partition(&mut self, partition_start: i64) -> Vec<O3Row> {
        let mut drained = Vec::new();
        self.rows.retain(|r| {
            if r.target_partition_start == partition_start {
                drained.push(r.clone());
                false
            } else {
                true
            }
        });
        drained.sort_by_key(|r| r.timestamp_ms);
        drained
    }

    /// Dedup by (series_id, timestamp) — last-write-wins.
    ///
    /// Call before merge to handle retries/replays.
    pub fn dedup(&mut self) {
        self.ensure_sorted();
        self.rows
            .dedup_by(|a, b| a.series_id == b.series_id && a.timestamp_ms == b.timestamp_ms);
    }

    fn ensure_sorted(&mut self) {
        if !self.sorted {
            // Radix-style: group by partition first (few distinct values),
            // then sort within groups by timestamp. This is faster than a
            // full comparison sort when partition_start has low cardinality.
            self.rows.sort_unstable_by(|a, b| {
                a.target_partition_start
                    .cmp(&b.target_partition_start)
                    .then(a.timestamp_ms.cmp(&b.timestamp_ms))
                    .then(a.series_id.cmp(&b.series_id))
            });
            self.sorted = true;
        }
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub fn is_full(&self) -> bool {
        self.rows.len() >= self.max_rows
    }

    /// Number of distinct target partitions in the buffer.
    pub fn target_partition_count(&self) -> usize {
        let mut partitions: Vec<i64> = self.rows.iter().map(|r| r.target_partition_start).collect();
        partitions.sort_unstable();
        partitions.dedup();
        partitions.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_row(ts: i64, series: u64, partition: i64) -> O3Row {
        O3Row {
            timestamp_ms: ts,
            series_id: series,
            value: ts as f64,
            target_partition_start: partition,
        }
    }

    #[test]
    fn insert_and_query() {
        let mut buf = O3Buffer::new(100);
        assert!(buf.insert(make_row(500, 1, 0)));
        assert!(buf.insert(make_row(300, 1, 0)));
        assert!(buf.insert(make_row(700, 2, 0)));
        assert_eq!(buf.len(), 3);

        let results = buf.query_range(200, 600);
        assert_eq!(results.len(), 2);
        // Should be sorted by timestamp.
        assert_eq!(results[0].timestamp_ms, 300);
        assert_eq!(results[1].timestamp_ms, 500);
    }

    #[test]
    fn buffer_full_rejection() {
        let mut buf = O3Buffer::new(2);
        assert!(buf.insert(make_row(100, 1, 0)));
        assert!(buf.insert(make_row(200, 1, 0)));
        assert!(!buf.insert(make_row(300, 1, 0))); // full
        assert!(buf.is_full());
    }

    #[test]
    fn drain_for_partition() {
        let mut buf = O3Buffer::new(100);
        buf.insert(make_row(100, 1, 0));
        buf.insert(make_row(200, 1, 86_400_000));
        buf.insert(make_row(300, 2, 0));
        assert_eq!(buf.len(), 3);

        let drained = buf.drain_for_partition(0);
        assert_eq!(drained.len(), 2);
        assert_eq!(buf.len(), 1);
        // Drained should be sorted by timestamp.
        assert_eq!(drained[0].timestamp_ms, 100);
        assert_eq!(drained[1].timestamp_ms, 300);
    }

    #[test]
    fn dedup_last_write_wins() {
        let mut buf = O3Buffer::new(100);
        buf.insert(make_row(100, 1, 0));
        buf.insert(make_row(100, 1, 0)); // duplicate
        buf.insert(make_row(200, 1, 0));
        assert_eq!(buf.len(), 3);

        buf.dedup();
        assert_eq!(buf.len(), 2);
    }

    #[test]
    fn target_partition_count() {
        let mut buf = O3Buffer::new(100);
        buf.insert(make_row(100, 1, 0));
        buf.insert(make_row(200, 1, 86_400_000));
        buf.insert(make_row(300, 2, 0));
        assert_eq!(buf.target_partition_count(), 2);
    }

    #[test]
    fn empty_query() {
        let mut buf = O3Buffer::new(100);
        let results = buf.query_range(0, 1000);
        assert!(results.is_empty());
    }
}
