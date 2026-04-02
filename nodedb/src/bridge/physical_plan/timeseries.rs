//! Timeseries engine operations dispatched to the Data Plane.

/// Timeseries engine physical operations.
#[derive(Debug, Clone)]
pub enum TimeseriesOp {
    /// Columnar partition scan with time-range pruning.
    ///
    /// Universal timeseries query path: handles raw scans, time-bucket
    /// aggregation, and generic GROUP BY. Reads from both the active
    /// memtable and sealed disk partitions.
    Scan {
        collection: String,
        /// `(min_ts_ms, max_ts_ms)`. (0, i64::MAX) = no time filter.
        time_range: (i64, i64),
        projection: Vec<String>,
        limit: usize,
        filters: Vec<u8>,
        /// time_bucket interval in milliseconds. 0 = no bucketing.
        bucket_interval_ms: i64,
        /// GROUP BY column names (empty = no grouping or whole-table agg).
        group_by: Vec<String>,
        /// Aggregate expressions: `(op, field)` e.g. `("count","*")`, `("avg","elapsed_ms")`.
        /// Empty = raw scan (no aggregation).
        aggregates: Vec<(String, String)>,
        /// RLS post-scan filters (applied after time-range pruning).
        rls_filters: Vec<u8>,
    },

    /// Write a batch of samples to the columnar memtable.
    Ingest {
        collection: String,
        payload: Vec<u8>,
        /// "ilp" for InfluxDB Line Protocol, "samples" for structured.
        format: String,
        /// WAL record LSN for deduplication. Set by the WAL catch-up task
        /// so the Data Plane can skip records that have already been ingested
        /// or flushed to disk. `None` for live ingest (always accepted).
        wal_lsn: Option<u64>,
    },
}
