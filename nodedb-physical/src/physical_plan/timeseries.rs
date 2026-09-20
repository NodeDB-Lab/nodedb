// SPDX-License-Identifier: Apache-2.0

//! Timeseries engine operations dispatched to the Data Plane.

use nodedb_types::{QualifiedCollection, RlsWriteCheck, Surrogate, SystemTimeScope};

use crate::physical_plan::document::ReturningSpec;

/// An unconstrained `(min_ts_ms, max_ts_ms)` envelope. The Control Plane
/// always plans unbounded — narrowing needs the `TIME_KEY` column, resolved
/// only in the Data Plane's registered schema.
pub const UNBOUNDED_TIME_RANGE: (i64, i64) = (i64::MIN, i64::MAX);

/// Timeseries engine physical operations.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum TimeseriesOp {
    /// Columnar partition scan with time-range pruning.
    ///
    /// Universal timeseries query path: handles raw scans, time-bucket
    /// aggregation, and generic GROUP BY. Reads from both the active
    /// memtable and sealed disk partitions.
    Scan {
        collection: QualifiedCollection,
        /// `(min_ts_ms, max_ts_ms)` pruning envelope. The Data Plane narrows
        /// it further using the query's bounds on the declared `TIME_KEY`;
        /// see [`UNBOUNDED_TIME_RANGE`].
        time_range: (i64, i64),
        projection: Vec<String>,
        limit: usize,
        filters: Vec<u8>,
        /// `ORDER BY` terms, in significance order, applied before `limit`.
        /// Empty = engine's natural order.
        sort_keys: Vec<crate::physical_plan::SortKeySpec>,
        /// time_bucket interval in milliseconds. 0 = no bucketing.
        bucket_interval_ms: i64,
        /// GROUP BY column names (empty = no grouping or whole-table agg).
        group_by: Vec<String>,
        /// Aggregate expressions: `(op, field)`, e.g. `("count","*")`. Empty
        /// = raw scan.
        aggregates: Vec<(String, String)>,
        /// Gap-fill strategy ("null"/"prev"/"linear"/literal), used only
        /// when `bucket_interval_ms > 0`. Empty = none.
        gap_fill: String,
        /// Serialized `Vec<ComputedColumn>` for scalar projections (e.g.
        /// `time_bucket('1h', timestamp)`), applied per-row in raw scan mode.
        computed_columns: Vec<u8>,
        /// RLS post-scan filters, applied after time-range pruning.
        rls_filters: Vec<u8>,
        /// `Current` / `AsOf(ms)` (block-skip + post-filter) / `AllVersions`
        /// (audit log, ascending). Meaningful only `WITH BITEMPORAL`.
        #[serde(default)]
        system_time: SystemTimeScope,
        /// Bitemporal valid-time point: only rows whose
        /// `[_ts_valid_from, _ts_valid_until)` contains it are returned.
        #[serde(default)]
        valid_at_ms: Option<i64>,
    },

    /// Write a batch of samples to the columnar memtable.
    Ingest {
        collection: QualifiedCollection,
        payload: Vec<u8>,
        /// "ilp" for InfluxDB Line Protocol, "samples" for structured.
        format: String,
        /// WAL record LSN for deduplication. Set by the WAL catch-up task
        /// so the Data Plane can skip records that have already been ingested
        /// or flushed to disk. `None` for live ingest (always accepted).
        #[serde(default)]
        wal_lsn: Option<u64>,
        /// Not consumed by the ingest handler: timeseries rows are identified
        /// by `series_id` (deterministic hash of measurement + tags), not
        /// cross-engine surrogates. Almost always `vec![]`; kept for
        /// plan-shape uniformity with the columnar `Insert` op.
        #[serde(default)]
        surrogates: Vec<Surrogate>,
        /// Sync provenance: originating peer and sequence, for idempotency.
        #[serde(default)]
        provenance: Option<nodedb_types::sync::wire::SyncProvenance>,
        /// Write policy evaluated against every parsed row before it reaches
        /// the memtable. Every ingest format normalizes into ILP first, so
        /// one gate covers all of them — including the raw ILP listener.
        rls_write_check: RlsWriteCheck,
        /// When `Some`, return the STORED post-image of each ingested point,
        /// read back through the ordinary scan projection. Never the
        /// submitted line. A batch that rejects any row FAILS when this is
        /// set — the `rejected` count has nowhere to go in a row set.
        #[serde(default)]
        returning: Option<ReturningSpec>,
        /// Read filters bounding `returning` to what a `SELECT` by the same
        /// principal would show — distinct from `rls_write_check`.
        #[serde(default)]
        rls_filters: Vec<u8>,
    },

    /// Read-only resolve pass for a governed [`TimeseriesOp::Ingest`]: a
    /// follower can't judge a live predicate, so this normalizes the payload
    /// into stamped ILP lines (memtable schema is Data-Plane-only, so
    /// normalization must happen here) and decides the policy without writing.
    ResolveIngest(Box<TimeseriesOp>),

    /// `TRUNCATE` of a timeseries collection: the memtable, every on-disk
    /// partition, the series catalog, and the last-value cache. Reports the
    /// number of rows that existed across memtable and partitions.
    Truncate {
        collection: QualifiedCollection,
        /// `TRUNCATE ... RESTART IDENTITY`: the Control Plane resets the
        /// collection's sequences after the Data Plane clears the rows.
        #[serde(default)]
        restart_identity: bool,
    },
}
