//! Timeseries engine operations dispatched to the Data Plane.

/// Timeseries engine physical operations.
#[derive(Debug, Clone)]
pub enum TimeseriesOp {
    /// Columnar partition scan with time-range pruning.
    Scan {
        collection: String,
        /// `(min_ts_ms, max_ts_ms)`. (0, i64::MAX) = no time filter.
        time_range: (i64, i64),
        projection: Vec<String>,
        limit: usize,
        filters: Vec<u8>,
        /// time_bucket interval in milliseconds. 0 = no bucketing.
        bucket_interval_ms: i64,
        /// RLS post-scan filters (applied after time-range pruning).
        rls_filters: Vec<u8>,
    },

    /// Write a batch of samples to the columnar memtable.
    Ingest {
        collection: String,
        payload: Vec<u8>,
        /// "ilp" for InfluxDB Line Protocol, "samples" for structured.
        format: String,
    },
}
