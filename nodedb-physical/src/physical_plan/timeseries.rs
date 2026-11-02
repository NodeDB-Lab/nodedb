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
        /// HAVING predicates (MessagePack `Vec<ScanFilter>`) over the
        /// finalized group rows, applied after gap-fill and before sort and
        /// limit. Empty on a raw scan.
        #[serde(default)]
        having: Vec<u8>,
        /// AUTO_TIER layout. `Some` routes each segment of the range to the
        /// raw rows or to a downsample tier's buckets.
        #[serde(default)]
        tiering: Option<crate::physical_plan::TieringPlan>,
        /// The output columns of an aggregating scan, in SELECT-list order.
        /// Each pair is `(encoded key, output name)`: the encoded key is
        /// `bucket`, a GROUP BY column, or an aggregate's canonical key. Each
        /// group row carries exactly these columns, renamed after HAVING,
        /// sort, and limit. Empty keeps every encoded key under its own name.
        #[serde(default)]
        output_columns: Vec<(String, String)>,
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

    /// Read-only resolve pass for a [`TimeseriesOp::Ingest`]: it normalizes
    /// the payload into stamped ILP lines (memtable schema is Data-Plane-only,
    /// so normalization must happen here), decides the policy, and resolves
    /// the lines to the rows they store, without writing.
    ResolveIngest(Box<TimeseriesResolve>),

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

    /// A refresh cut of one continuous aggregate rooted at `collection`.
    ///
    /// The cut replicates on the collection's data group and applies on
    /// every replica in log order. At apply the replica flushes the
    /// collection's memtable, so every row of every record before the cut
    /// sits in a partition and no row after it does. The aggregate then
    /// folds exactly the partitions present at that moment, on the
    /// maintenance tick, so every replica folds the same rows.
    RefreshAggregate {
        collection: QualifiedCollection,
        cut: RefreshCut,
    },

    /// Partition maintenance on the core that owns `collection`:
    /// `SHOW PARTITIONS FOR` lists its partitions, and `REWRITE PARTITIONS
    /// FOR` rewrites each sealed one. Neither changes a row, so neither is
    /// logged or replicated.
    Partitions {
        collection: QualifiedCollection,
        action: PartitionsAction,
    },

    /// The finalized rows of continuous aggregate `aggregate`, read on the
    /// core that owns its root source `collection`: one map per bucket and
    /// group, holding `bucket`, the GROUP BY columns and the output columns.
    /// The read completes the aggregate's backfill and pending refresh cuts
    /// first, so it never answers from partial buckets. It changes no row,
    /// so it is neither logged nor replicated.
    ///
    /// A read at a system-time cut (`TxnSnapshot`, `Pinned`, `AsOf`)
    /// returns the rows the aggregate held at the cut: a bucket that changed
    /// after the cut is rebuilt from the raw rows visible at the cut, and a
    /// read no source answers exactly is refused as a serialization conflict.
    ReadAggregate {
        collection: QualifiedCollection,
        aggregate: String,
        #[serde(default)]
        system_time: SystemTimeScope,
    },
}

/// What a [`TimeseriesOp::Partitions`] does.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(c_enum)]
pub enum PartitionsAction {
    /// List every queryable partition with its state, time range, row count,
    /// size and interval.
    List,
    /// Rewrite every sealed or merged partition with the collection's current
    /// schema and codecs. A column the schema no longer holds is dropped.
    Rewrite,
}

/// One continuous aggregate by name and incarnation.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct AggregateIncarnation {
    pub name: String,
    pub incarnation: nodedb_types::Hlc,
}

/// The cut a [`TimeseriesOp::RefreshAggregate`] makes in one continuous
/// aggregate.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct RefreshCut {
    /// Aggregate name.
    pub aggregate: String,
    /// Incarnation of the aggregate the cut was proposed for. A replica
    /// whose aggregate of that name has another incarnation never applies
    /// the cut to it.
    pub incarnation: nodedb_types::Hlc,
    /// Names the cut on every replica. A second apply of the same id is a
    /// no-op, and a waiter asks for completion by it. For a periodic cut it
    /// is the period slot: `now_ms / interval_ms`.
    pub refresh_id: u64,
    /// The periodic catch-up proposed for slot `refresh_id`. A replica
    /// applies only slots above the last one it applied.
    pub periodic: bool,
}

/// What a [`TimeseriesOp::ResolveIngest`] resolves.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct TimeseriesResolve {
    /// The ingest to resolve.
    pub ingest: TimeseriesOp,
    /// The encoded schema an earlier ingest of the same transaction into
    /// the same collection resolved to. The ingest resolves against it, not
    /// against the live schema. `None` resolves against the live schema.
    #[serde(default)]
    pub base: Option<Vec<u8>>,
}
