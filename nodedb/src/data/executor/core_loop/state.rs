// SPDX-License-Identifier: BUSL-1.1

//! `CoreLoop` struct definition — all fields for the per-core Data Plane loop.

use std::collections::HashMap;
use std::sync::Arc;

use nodedb_bridge::buffer::{Consumer, Producer};

use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};
use crate::control::array_catalog::ArrayCatalogHandle;
use crate::data::executor::spatial_key::SpatialIndexKey;
use crate::data::io::IoMetrics;
use crate::engine::array::ArrayEngine;
use crate::engine::crdt::tenant_state::TenantCrdtEngine;
use crate::engine::graph::edge_store::EdgeStore;
use crate::engine::sparse::btree::SparseEngine;
use crate::engine::sparse::doc_cache::DocCache;
use crate::engine::sparse::inverted::InvertedIndex;
use crate::engine::vector::collection::VectorCollection;
use crate::engine::vector::sparse::SparseInvertedIndex;
use crate::types::{Lsn, TenantId};
use nodedb_columnar::mutation::snapshot::FlushedSurrogateTable;
use nodedb_graph::ShardedCsrIndex;
use nodedb_types::{DatabaseId, OrdinalClock};

use super::checkpoint_floors::CheckpointFloors;
use super::priority_queues::PriorityQueues;

/// Per-core event loop for the Data Plane.
///
/// Each CPU core runs one `CoreLoop`. It owns:
/// - SPSC consumer for incoming requests from the Control Plane
/// - SPSC producer for outgoing responses to the Control Plane
/// - Per-core `SparseEngine` (redb) for point lookups and range scans
/// - Per-tenant `TenantCrdtEngine` instances (lazy-initialized) + task queue
///
/// This type is intentionally `!Send` — pinned to a single core.
pub struct CoreLoop {
    pub(in crate::data::executor) core_id: usize,

    /// SPSC channel: receives requests from Control Plane.
    pub(in crate::data::executor) request_rx: Consumer<BridgeRequest>,

    /// SPSC channel: sends responses to Control Plane.
    pub(crate) response_tx: Producer<BridgeResponse>,

    /// Three-tier priority task queue (Critical / High / Low).
    ///
    /// Drain budget per 14-slot cycle: 8 Critical : 4 High : 2 Low.
    /// Empty tiers donate unused slots to the next lower tier.
    pub(crate) task_queue: PriorityQueues,

    /// Position within the current 14-slot drain cycle.
    /// Passed by mutable reference to `PriorityQueues::pop_next` so the
    /// ratio is maintained across multiple calls inside a single `tick()`.
    pub(crate) drain_cycle: usize,

    /// Per-priority IO queue-depth and wait-latency metrics.
    ///
    /// Shared via `Arc` with the Control Plane Prometheus handler so the
    /// HTTP endpoint can read live values without crossing the plane boundary
    /// through `SystemMetrics`.
    pub(crate) io_metrics: Arc<IoMetrics>,

    /// Current watermark LSN for this core's shard data.
    pub(crate) watermark: Lsn,

    /// What this core is durable through OUTSIDE the WAL, per engine, plus the
    /// boot-restored replay floors. Grouped in `checkpoint_floors.rs` — every
    /// field there obeys the same rule, and the LSN this core reports to the
    /// checkpoint manager is a fold over them.
    pub(in crate::data::executor) floors: CheckpointFloors,

    /// redb-backed sparse/metadata engine for this core.
    pub(crate) sparse: SparseEngine,

    /// Per-tenant CRDT engines, lazily initialized on first access.
    pub(in crate::data::executor) crdt_engines: HashMap<(DatabaseId, TenantId), TenantCrdtEngine>,

    /// Per-collection vector collections, lazily initialized on first insert.
    /// Key: `(DatabaseId, TenantId, collection_key)` where `collection_key` is
    /// `collection` or `"{collection}:{field_name}"` for named fields.
    pub(in crate::data::executor) vector_collections:
        HashMap<(DatabaseId, TenantId, String), VectorCollection>,

    /// Background HNSW builder: send requests.
    pub(in crate::data::executor) build_tx: Option<crate::engine::vector::builder::BuildSender>,
    /// Background HNSW builder: receive completed builds.
    pub(in crate::data::executor) build_rx:
        Option<crate::engine::vector::builder::CompleteReceiver>,

    /// Per-collection HNSW parameters set via DDL. If a collection has no
    /// entry here, `HnswParams::default()` is used on first insert.
    /// Key: `(DatabaseId, TenantId, collection_key)` — same shape as `vector_collections`.
    pub(in crate::data::executor) vector_params:
        HashMap<(DatabaseId, TenantId, String), crate::engine::vector::hnsw::HnswParams>,

    /// Vector dimension declared by `CREATE VECTOR INDEX ... DIM <n>`, per
    /// index. Every vector written to the field is checked against it, so a
    /// pipeline emitting the wrong embedding width is rejected at the write
    /// rather than discovered as poor search results. Absent = never declared
    /// (a pre-existing index, or one created before DIM was enforced), in
    /// which case the index adopts the width of the first vector it sees.
    /// Key: `(DatabaseId, TenantId, collection_key)` — same shape as `vector_params`.
    pub(in crate::data::executor) declared_dims: HashMap<(DatabaseId, TenantId, String), usize>,

    /// redb-backed graph edge storage for this core.
    pub(in crate::data::executor) edge_store: EdgeStore,

    /// Strictly-monotonic ordinal clock for bitemporal `system_from` suffixes.
    /// Shared across all Data Plane cores so edge keys are globally ordered
    /// even under concurrent multi-core writes.
    pub(in crate::data::executor) hlc: Arc<OrdinalClock>,
    /// HLC watermark for `_ts_system` stamping (see `bitemporal_time.rs`).
    pub(in crate::data::executor) last_stamp_ms: std::sync::atomic::AtomicI64,

    /// Per-tenant in-memory CSR adjacency index, rebuilt from
    /// edge_store on startup. Each tenant's graph state lives in its
    /// own `CsrIndex` partition — no shared key space, no lexical
    /// `<tid>:` prefix anywhere in memory.
    pub(in crate::data::executor) csr: ShardedCsrIndex,

    /// Full-text inverted index (BM25), shares redb with sparse engine.
    pub(in crate::data::executor) inverted: InvertedIndex,

    /// Per-collection spatial R-tree indexes, keyed by
    /// (DatabaseId, TenantId, collection, field).
    /// Lazily initialized when a spatial query or geometry insert first targets a field.
    pub(in crate::data::executor) spatial_indexes:
        std::collections::HashMap<SpatialIndexKey, crate::engine::spatial::RTree>,

    /// Reverse map from R-tree entry ID → document ID,
    /// keyed by (DatabaseId, TenantId, collection, field, entry_id).
    pub(in crate::data::executor) spatial_doc_map:
        std::collections::HashMap<(DatabaseId, TenantId, String, String, u64), String>,

    /// Reverse map from an indexed document to the HNSW vector ID it produced,
    /// keyed by (DatabaseId, TenantId, collection, field, storage key). The
    /// storage key matches the key `apply_point_put` indexes the row under.
    /// Populated on every vector index insert; consulted by
    /// `apply_point_delete` to soft-delete the orphaned vector when its owning
    /// document is removed.
    pub(in crate::data::executor) vector_doc_map: std::collections::HashMap<
        (
            DatabaseId,
            TenantId,
            String,
            String,
            nodedb_types::StorageKey,
        ),
        u32,
    >,

    /// Base data directory for this core (used for sort spill temp files).
    pub(in crate::data::executor) data_dir: std::path::PathBuf,

    /// vShards that are paused for write operations (during Phase 3 migration cutover).
    pub(in crate::data::executor) paused_vshards: std::collections::HashSet<crate::types::VShardId>,

    /// Nodes explicitly deleted via PointDelete cascade, keyed per
    /// `(database, tenant)`. Used for edge referential integrity — an
    /// `EdgePut` to a deleted node is rejected with `RejectedDanglingEdge`.
    /// Cleared periodically or on compaction. Entries are raw user-visible
    /// node names, structurally tenant-partitioned like every graph concern.
    pub(in crate::data::executor) deleted_nodes:
        HashMap<(nodedb_types::DatabaseId, TenantId), std::collections::HashSet<String>>,

    /// Idempotency key deduplication: maps processed idempotency keys to
    /// whether they succeeded (true) or failed (false). Uses `VecDeque`
    /// for FIFO eviction order alongside `HashMap` for O(1) lookup.
    /// Bounded to 16,384 entries.
    pub(in crate::data::executor) idempotency_cache: HashMap<u64, bool>,
    /// FIFO order of idempotency keys for correct eviction (oldest first).
    pub(in crate::data::executor) idempotency_order: std::collections::VecDeque<u64>,

    /// Per-stream sync high-watermark: the last `seq` durably applied for each
    /// `(producer_id, stream_id)` pair. Populated from WAL replay on startup;
    /// advanced by `sync_commit` after WAL durability. Never shared — this map
    /// lives exclusively on the owning core.
    pub(in crate::data::executor) sync_hwm:
        HashMap<(u64 /* producer_id */, u64 /* stream_id */), u64 /* last applied seq */>,

    /// Per-producer epoch floor: the highest epoch seen for each `producer_id`.
    /// When a newer epoch arrives the floor is advanced immediately (monotonic
    /// and also persisted in the registry/WAL). Frames carrying an older epoch
    /// are fenced without state change.
    pub(in crate::data::executor) producer_epoch_floor:
        HashMap<u64 /* producer_id */, u64 /* highest epoch seen */>,

    /// Column statistics store for CBO. Shares redb with sparse engine.
    /// Updated incrementally on PointPut. Read by DataFusion optimizer.
    pub(in crate::data::executor) stats_store: crate::engine::sparse::stats::StatsStore,

    /// Incremental aggregate cache: maps `(tenant, rest)` →
    /// partial aggregate state. Updated on writes (PointPut increments counts/sums),
    /// cleared on schema change. Turns O(N) full-scan aggregates into O(1) cache
    /// lookups for repeated dashboard/analytics queries.
    ///
    /// Key: `(TenantId, "{collection}\0{group_by_fields}\0{agg_ops}")`.
    /// Value: cached result rows as JSON, stamped with the KV write epoch the
    /// collection was at when computed — see
    /// `handlers::aggregate::AggregateCacheEntry`. Document/columnar writes
    /// still evict explicitly via `invalidate_aggregate_cache_for_collection`;
    /// KV writes are caught by the epoch stamp instead, since a KV write has
    /// no equivalent per-write invalidation call site.
    pub(in crate::data::executor) aggregate_cache: HashMap<
        (DatabaseId, TenantId, String),
        super::super::handlers::aggregate::AggregateCacheEntry,
    >,

    /// Per-collection full index config (includes index_type, PQ params, IVF params).
    /// Stored alongside vector_params for collections that use non-default index types.
    /// Key: `(DatabaseId, TenantId, collection_key)` — same shape as `vector_collections`.
    pub(in crate::data::executor) index_configs:
        HashMap<(DatabaseId, TenantId, String), crate::engine::vector::index_config::IndexConfig>,

    /// IVF-PQ indexes for collections configured with `index_type = "ivf_pq"`.
    /// Key: `(DatabaseId, TenantId, collection_key)` — same shape as `vector_collections`.
    pub(in crate::data::executor) ivf_indexes:
        HashMap<(DatabaseId, TenantId, String), crate::engine::vector::ivf::IvfPqIndex>,

    /// Per-collection sparse vector inverted indexes, keyed by
    /// (DatabaseId, TenantId, collection, field).
    /// The field is `"_sparse"` when no named field is specified.
    pub(in crate::data::executor) sparse_vector_indexes:
        HashMap<(DatabaseId, TenantId, String, String), SparseInvertedIndex>,

    /// Per-core LRU document cache for O(1) hot-key point lookups.
    /// Invalidated write-through on PointPut/Delete/Update.
    pub(in crate::data::executor) doc_cache: DocCache,

    /// Per-collection columnar timeseries memtables (!Send, per-core owned).
    /// Key: (DatabaseId, TenantId, collection).
    pub(in crate::data::executor) columnar_memtables: HashMap<
        (DatabaseId, TenantId, String),
        crate::engine::timeseries::columnar_memtable::ColumnarMemtable,
    >,

    /// Live engine-memory reservation for each columnar timeseries memtable's
    /// resident footprint. Recharged via `recharge_ts_memtable_budget` after
    /// every ingest (so the Timeseries budget tracks the memtable's actual
    /// `memory_bytes()`) and dropped when `flush_ts_collection` drains the
    /// memtable — so the flush release balances the reservation instead of
    /// releasing bytes that were never reserved.
    /// Key: (DatabaseId, TenantId, collection).
    pub(in crate::data::executor) columnar_memtable_mem:
        HashMap<(DatabaseId, TenantId, String), nodedb_mem::ReservationToken>,

    /// Per-collection columnar mutation engines for plain/spatial profiles.
    /// Uses `nodedb-columnar`'s `MutationEngine` with full INSERT/UPDATE/DELETE.
    /// Key: (DatabaseId, TenantId, collection).
    pub(in crate::data::executor) columnar_engines:
        HashMap<(DatabaseId, TenantId, String), nodedb_columnar::MutationEngine>,

    /// Flushed columnar segment bytes, keyed by (DatabaseId, TenantId, collection).
    /// Each entry is a list of encoded segment buffers produced by `SegmentWriter`.
    /// Kept in memory so `scan_columnar` can read rows that were drained from the
    /// active memtable during a flush (otherwise those rows would be lost until a
    /// real on-disk segment reader is wired up).
    pub(in crate::data::executor) columnar_flushed_segments:
        HashMap<(DatabaseId, TenantId, String), Vec<Vec<u8>>>,

    /// Cross-engine surrogates for flushed plain-columnar segments, held in
    /// lockstep with `columnar_flushed_segments`: outer Vec index == segment
    /// Vec index (so segment_id == index + 1 holds identically); inner Vec is
    /// per-row, indexed by row position within the segment. `None` = a row
    /// flushed without a surrogate (test fixtures / pre-surrogate rows).
    /// In-memory only, exactly like the segment bytes it annotates.
    pub(in crate::data::executor) columnar_flushed_surrogates:
        HashMap<(DatabaseId, TenantId, String), FlushedSurrogateTable>,

    /// Per-collection replay stamps: the records whose rows a partition
    /// holds or a truncate removed, and the truncates that took effect. See
    /// `timeseries_checkpoint::stamp`. Key: (DatabaseId, TenantId, collection).
    pub(in crate::data::executor) ts_replay_stamps: HashMap<
        (DatabaseId, TenantId, String),
        crate::data::executor::timeseries_checkpoint::stamp::TsReplayStamp,
    >,

    /// While restart replay runs the timeseries pass: the LSN through which it
    /// has passed every record. A flush or truncate then stamps through it
    /// instead of the core stamp. `None` outside that pass.
    pub(in crate::data::executor) ts_replay_cursor: Option<u64>,

    /// Last time any timeseries ingest was processed on this core.
    /// Used by idle flush: if no ingest for 5 seconds, `maybe_run_maintenance`
    /// flushes all non-empty memtables to disk partitions.
    pub(in crate::data::executor) last_ts_ingest: Option<std::time::Instant>,

    /// Per-collection last-value caches for O(1) recent value lookup.
    /// Key: (DatabaseId, TenantId, collection).
    pub(in crate::data::executor) ts_last_value_caches: HashMap<
        (DatabaseId, TenantId, String),
        crate::engine::timeseries::last_value_cache::LastValueCache,
    >,

    /// Per-collection series catalogs — the only source of `SeriesId`.
    ///
    /// The catalog resolves hash collisions, so two distinct `SeriesKey`s never
    /// share an ID. It must live beside `ts_last_value_caches` and be torn down
    /// with it: those two plus the memtable's per-series row counts are the only
    /// consumers of `SeriesId`, they are all in-memory and per-collection, and a
    /// catalog that outlived them would hand out IDs for series they no longer
    /// hold. Key: (DatabaseId, TenantId, collection).
    pub(in crate::data::executor) ts_series_catalogs:
        HashMap<(DatabaseId, TenantId, String), nodedb_types::timeseries::SeriesCatalog>,

    /// Per-collection timeseries partition registries for this core.
    /// Key: (DatabaseId, TenantId, collection).
    pub(in crate::data::executor) ts_registries: HashMap<
        (DatabaseId, TenantId, String),
        crate::engine::timeseries::partition_registry::PartitionRegistry,
    >,

    /// Aside partition directories of committed timeseries truncates whose
    /// removal failed at batch finalize; the maintenance tick retries them.
    pub(in crate::data::executor) ts_truncate_backlog: Vec<std::path::PathBuf>,

    /// Continuous aggregate manager for this core. Fires on memtable flush.
    pub(in crate::data::executor) continuous_agg_mgr:
        crate::engine::timeseries::continuous_agg::ContinuousAggregateManager,

    /// Schedules incremental dirty-page flushing across engines between
    /// coordinated checkpoints, so a checkpoint never has to flush an engine's
    /// entire backlog in one stall. It is scheduling pressure only and carries
    /// no LSN — what this core is durable through is answered solely by the
    /// `*_durable_lsn` fields below and the fold in `execute_checkpoint`.
    pub(in crate::data::executor) checkpoint_coordinator:
        crate::storage::checkpoint::CheckpointCoordinator,

    /// Per-collection document index configurations.
    /// Maps (DatabaseId, TenantId, collection) → CollectionConfig.
    /// Populated via RegisterDocumentCollection plans.
    pub(in crate::data::executor) doc_configs:
        HashMap<(DatabaseId, TenantId, String), crate::engine::document::store::CollectionConfig>,

    /// Per-collection last chain hash for HASH_CHAIN collections.
    /// Maps (TenantId, collection) → last SHA-256 hash.
    pub(in crate::data::executor) chain_hashes: HashMap<(DatabaseId, TenantId, String), String>,

    /// Query execution tuning parameters (sort run size, stream chunk size, etc.).
    /// Set at core spawn time from config; never changed at runtime.
    pub(in crate::data::executor) query_tuning: nodedb_types::config::tuning::QueryTuning,

    /// Graph engine tuning parameters (max_visited, max_depth, LCC thresholds).
    /// Set at core spawn time from config; never changed at runtime.
    pub(in crate::data::executor) graph_tuning: nodedb_types::config::tuning::GraphTuning,

    /// Timeseries engine tuning (memtable soft/hard budgets, tag cardinality
    /// ceiling). Set at core spawn time from config; never changed at runtime.
    /// Read when a collection's `ColumnarMemtable` is created and by the ingest
    /// path's record-boundary admission gate.
    pub(in crate::data::executor) ts_tuning: nodedb_types::config::tuning::TimeseriesToning,

    /// Per-core KV engine: hash tables + expiry wheel. `!Send`.
    pub(in crate::data::executor) kv_engine: crate::engine::kv::KvEngine,

    /// Per-core ND-array engine. Owns one LSM store per registered
    /// array (`open_array`). The Control Plane allocates WAL LSNs and
    /// the engine just stamps the supplied LSN into the memtable —
    /// see `ArrayEngine::{put_cells, delete_cells, flush}`.
    pub(in crate::data::executor) array_engine: ArrayEngine,

    /// Shared array catalog handle — the Control Plane's registered
    /// array metadata. The Data Plane consults this (read-only) when
    /// resolving array names to `ArrayId` + schema digests during
    /// dispatch.
    pub(in crate::data::executor) array_catalog: ArrayCatalogHandle,

    /// Per-core io_uring reader for batched columnar segment reads.
    /// Initialized lazily; `None` if io_uring is not available.
    pub(in crate::data::executor) uring_reader: Option<crate::data::io::uring_reader::UringReader>,

    /// Per-engine segment/checkpoint at-rest encryption keys.
    pub(in crate::data::executor) segment_keks: super::SegmentKeks,

    /// Memory governor for per-engine budget enforcement.
    pub(in crate::data::executor) governor: Arc<nodedb_mem::MemoryGovernor>,

    /// Request intake level for this tick, folded from engine memory
    /// pressure and response-ring utilization. Drain depth and the suspend
    /// decision both read off it.
    pub(crate) throttle: super::pressure::SpscThrottle,

    /// Per-collection jemalloc arena registry.
    ///
    /// Shared with the Control Plane for stats queries. Vector-primary
    /// collections request a dedicated arena via `get_or_create`; other
    /// collections use the per-core arena from `nodedb_mem::arena`.
    /// `None` until wired by the server bootstrap or test harness.
    pub(in crate::data::executor) collection_arena_registry:
        Option<std::sync::Arc<nodedb_mem::CollectionArenaRegistry>>,

    /// Shared system metrics — Arc is safe for `!Send` since all fields are atomic.
    pub(in crate::data::executor) metrics: Option<Arc<crate::control::metrics::SystemMetrics>>,

    /// Event bus producer: emits WriteEvents to the Event Plane.
    /// One per core, `!Send` once pinned. `None` if Event Plane is disabled.
    pub(in crate::data::executor) event_producer: Option<crate::event::bus::EventProducer>,

    /// Monotonic sequence counter for events emitted by this core.
    /// Incremented on every successful event emission.
    pub(in crate::data::executor) event_sequence: u64,

    /// Shared collection-scoped scan-quiesce registry.
    ///
    /// When set, every scan handler on this core calls
    /// `quiesce.try_start_scan(tenant, collection)` at entry and holds
    /// the resulting `ScanGuard` across the row stream. A concurrent
    /// `PurgeCollection` post-apply flow calls `begin_drain` +
    /// `wait_until_drained` on the same registry, so the unlink pass
    /// only runs once every in-flight scan has released.
    ///
    /// `None` in test / no-cluster bringup: scans skip the gate. Boot wires
    /// the shared registry via `set_quiesce` after `SharedState::open`.
    pub(in crate::data::executor) quiesce:
        Option<std::sync::Arc<crate::bridge::quiesce::CollectionQuiesce>>,

    /// Shared quarantine registry for corrupt segments.
    ///
    /// `Arc` is `Send + Sync` so it is safe to hold on a `!Send` core.
    /// `None` until wired by the server bootstrap via `set_quarantine_registry`.
    pub(in crate::data::executor) quarantine_registry:
        Option<std::sync::Arc<crate::storage::quarantine::QuarantineRegistry>>,

    /// Compaction pacing, the maintenance CPU budget, and index rebuilds.
    pub(in crate::data::executor) maintenance: super::maintenance_state::MaintenanceState,

    /// Ambient deterministic timestamp for the current Calvin epoch.
    ///
    /// Set to `Some(ms)` by `execute_calvin_execute_static`,
    /// `execute_calvin_execute_active` and `execute_calvin_resolve` while they
    /// stage or resolve a transaction's plans, and by `execute_calvin_flush`
    /// while it renders the reply, then restored immediately after. Engine handlers that need "current time" (bitemporal sys_from,
    /// KV TTL expire_at, timeseries system_ms) call
    /// `self.epoch_system_ms.unwrap_or_else(<wall_clock_read>)` so that
    /// single-shard (non-Calvin) paths read the wall clock.
    ///
    /// Safety: this is safe because `CoreLoop` is `!Send` and single-threaded
    /// per core, and staging never recurses into another Calvin execute.
    pub(in crate::data::executor) epoch_system_ms: Option<i64>,

    /// Per-transaction staging overlay: not-yet-durable writes for each
    /// in-flight transaction on this core, keyed by `TxnId`. Populated by
    /// `MetaOp::StageWrite`, released by `MetaOp::DropTxnOverlay`.
    pub(in crate::data::executor) txn_overlays: HashMap<
        crate::types::TxnId,
        crate::data::executor::handlers::transaction::overlay::TxnOverlay,
    >,
    /// Parallel to `txn_overlays`, for GRAPH writes (edge identity is a
    /// string tuple, not a surrogate -- see `GraphTxnOverlay`). Same lifecycle.
    pub(in crate::data::executor) graph_txn_overlays: HashMap<
        crate::types::TxnId,
        crate::data::executor::handlers::transaction::overlay::GraphTxnOverlay,
    >,
    /// Parallel to `txn_overlays`, for ARRAY writes (cell identity is a
    /// coordinate tuple, not a surrogate -- see `ArrayTxnOverlay`). Same
    /// lifecycle.
    pub(in crate::data::executor) array_txn_overlays: HashMap<
        crate::types::TxnId,
        crate::data::executor::handlers::transaction::overlay::ArrayTxnOverlay,
    >,
    /// Columnar engines THIS txn newly created while staging; `DropTxnOverlay`
    /// drops still-empty entries (rollback) and leaves filled ones (commit).
    pub(in crate::data::executor) txn_created_columnar_engines:
        HashMap<crate::types::TxnId, std::collections::HashSet<(DatabaseId, TenantId, String)>>,

    /// Per-core last-write-LSN version index (per key + per collection),
    /// advanced by every committed write-apply. Type + GC in `write_index.rs`.
    pub(in crate::data::executor) write_index: super::write_index::WriteVersionIndex,

    /// Scratch map (surrogate → resolve-time bitemporal stamp) consulted ONLY
    /// by `apply_point_put` and `apply_point_delete`. Populated right before a
    /// bitemporal document apply scope — from a committing transaction's
    /// overlay sidecar (commit-time install) or a decoded stamped redo put or
    /// delete sub-record (WAL replay, committed-redo apply) — and cleared right
    /// after. When a surrogate has an entry, the put or tombstone is forced
    /// onto the versioned store at the carried system time rather than a fresh
    /// one, so every apply of the record agrees on the version key even when
    /// `doc_configs` is empty (the real replay-time boot state).
    pub(in crate::data::executor) active_bitemporal_stamps:
        HashMap<u32, crate::data::executor::handlers::transaction::overlay::BitemporalStamp>,

    /// Transaction-resolved graph system-time ordinal used for every edge
    /// mutation in the current live apply/replay scope.
    pub(in crate::data::executor) active_graph_system_from: Option<i64>,

    /// Staged Calvin transactions, the writes waiting on them, and the
    /// executing transaction's leader flag.
    pub(in crate::data::executor) calvin: super::calvin_state::CalvinCoreState,

    /// Core count and per-record scratch of the committed-redo apply.
    pub(in crate::data::executor) redo_apply:
        crate::data::executor::handlers::transaction::redo_apply::RedoApplyState,
    /// Set once this core's state is unknown. It then refuses every request.
    pub(in crate::data::executor) fail_stop: super::fail_stop::CoreFailStop,
}
