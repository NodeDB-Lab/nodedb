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
    /// keyed by (DatabaseId, TenantId, collection, field, doc_id). `doc_id` is
    /// the hex-encoded surrogate row key (matching the key `apply_point_put`
    /// indexes under). Populated on every vector index insert; consulted by
    /// `apply_point_delete` to soft-delete the orphaned vector when its owning
    /// document is removed.
    pub(in crate::data::executor) vector_doc_map:
        std::collections::HashMap<(DatabaseId, TenantId, String, String, String), u32>,

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
    /// Value: cached result rows as JSON.
    pub(in crate::data::executor) aggregate_cache: HashMap<(DatabaseId, TenantId, String), Vec<u8>>,

    /// Last time periodic maintenance (compaction, edge sweep) was run.
    pub(in crate::data::executor) last_maintenance: Option<std::time::Instant>,

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

    /// Compaction interval (how often `maybe_run_maintenance` triggers).
    pub(in crate::data::executor) compaction_interval: std::time::Duration,

    /// Tombstone ratio threshold for auto-compaction (0.0–1.0).
    pub(in crate::data::executor) compaction_tombstone_threshold: f64,

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

    /// Per-collection max WAL LSN that has been ingested into the memtable.
    /// Used by the WAL catch-up deduplication: if a catch-up record's LSN
    /// is <= this value, the Data Plane skips it (already ingested).
    /// Key: (DatabaseId, TenantId, collection).
    pub(in crate::data::executor) ts_max_ingested_lsn: HashMap<(DatabaseId, TenantId, String), u64>,

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

    /// Per-collection timeseries partition registries for this core.
    /// Key: (DatabaseId, TenantId, collection).
    pub(in crate::data::executor) ts_registries: HashMap<
        (DatabaseId, TenantId, String),
        crate::engine::timeseries::partition_registry::PartitionRegistry,
    >,

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

    /// L1 segment compaction config for the storage layer.
    pub(in crate::data::executor) segment_compaction_config:
        crate::storage::compaction::CompactionConfig,

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
    pub(in crate::data::executor) governor: Option<Arc<nodedb_mem::MemoryGovernor>>,

    /// Shared per-database maintenance CPU budget tracker.
    ///
    /// Used by all maintenance sites (`run_compaction` and friends) to gate
    /// per-database background work against the quota's `maintenance_cpu_pct`.
    /// Set by `set_maintenance_budget` after core spawn.
    pub(in crate::data::executor) maintenance_budget:
        Option<Arc<crate::control::maintenance::MaintenanceBudgetTracker>>,

    /// Current SPSC drain batch size, adjusted by memory pressure.
    ///
    /// Normal: 64.  Critical: halved (floor 1).  Emergency: 0 (new reads
    /// suspended until pressure clears).  Restored with hysteresis after
    /// `PRESSURE_NORMAL_HYSTERESIS` consecutive Normal/Warning iterations.
    pub(crate) spsc_read_depth: usize,

    /// When `true` the core loop does not drain new SPSC requests.
    /// Set on Emergency pressure; cleared when pressure drops to Critical
    /// or below (then normal hysteresis restores `spsc_read_depth`).
    pub(crate) pressure_suspend_reads: bool,

    /// Consecutive ticks at Normal/Warning pressure since last Critical/Emergency
    /// transition. Used for hysteresis before restoring `spsc_read_depth`.
    pub(crate) pressure_normal_ticks: u32,

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
    /// `None` in test / no-cluster bringup paths: callers then skip
    /// the gate and scan unconditionally (matching pre-quiesce
    /// behavior). In the server bootstrap path `main.rs` wires the
    /// shared registry via `set_quiesce` after `SharedState::open`.
    pub(in crate::data::executor) quiesce:
        Option<std::sync::Arc<crate::bridge::quiesce::CollectionQuiesce>>,

    /// Shared quarantine registry for corrupt segments.
    ///
    /// `Arc` is `Send + Sync` so it is safe to hold on a `!Send` core.
    /// `None` until wired by the server bootstrap via `set_quarantine_registry`.
    pub(in crate::data::executor) quarantine_registry:
        Option<std::sync::Arc<crate::storage::quarantine::QuarantineRegistry>>,

    /// In-flight concurrent index rebuilds, polled each tick.
    ///
    /// Each entry is a `(collection_key, receiver)` pair.  The receiver
    /// yields a `RebuildResult` once the background OS thread finishes
    /// the shadow build.  Only one rebuild per collection may be in
    /// progress at a time; `execute_rebuild_index` returns
    /// `ErrorCode::Conflict` when a second is attempted.
    pub(in crate::data::executor) pending_reindex:
        Vec<crate::data::executor::handlers::control::reindex::PendingReindex>,

    /// Ambient deterministic timestamp for the current Calvin epoch.
    ///
    /// Set to `Some(ms)` by `execute_calvin_execute_static` and
    /// `execute_calvin_execute_active` before dispatching the inner
    /// transaction batch, then reset to `None` immediately after.
    /// Engine handlers that need "current time" (bitemporal sys_from, KV TTL
    /// expire_at, timeseries system_ms) call
    /// `self.epoch_system_ms.unwrap_or_else(<wall_clock_read>)` so that
    /// single-shard (non-Calvin) paths continue working without change.
    ///
    /// Safety: this is safe because `CoreLoop` is `!Send` and single-threaded
    /// per core. Sub-plans inside `execute_transaction_batch` do not recurse
    /// back into a Calvin execute variant, so the reset after the batch is not
    /// premature.
    pub(in crate::data::executor) epoch_system_ms: Option<i64>,

    /// Whether THIS node is the leader of the data-group owning the vshard for
    /// the currently-executing Calvin transaction.
    ///
    /// Set to `true`/`false` by `execute_calvin_execute_static` and
    /// `execute_calvin_execute_active` (from the scheduler-stamped, per-node,
    /// non-replicated `MetaOp::is_group_leader`) before dispatching the inner
    /// transaction batch, then reset to `false` immediately after.
    ///
    /// OLLP determinism: the bulk-DML handlers run the optimistic-lock
    /// verification (`actual != predicted`) and emit `OllpRetryRequired` ONLY
    /// when this is `true`. Every replica — leader and follower alike — applies
    /// the carried `ollp_predicted_surrogates` set verbatim, so all replicas
    /// mutate the identical surrogate set regardless of any per-replica local
    /// scan drift.
    ///
    /// Defaults to `false` outside a Calvin execute (single-shard / non-Calvin
    /// paths never set predicted surrogates, so the flag is never read there).
    /// Safe for the same single-threaded `!Send` reasons as `epoch_system_ms`.
    pub(in crate::data::executor) ollp_is_group_leader: bool,

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
    /// Columnar engines THIS txn newly created while staging; `DropTxnOverlay`
    /// drops still-empty entries (rollback) and leaves filled ones (commit).
    pub(in crate::data::executor) txn_created_columnar_engines:
        HashMap<crate::types::TxnId, std::collections::HashSet<(DatabaseId, TenantId, String)>>,

    /// Per-core last-write-LSN version index (per key + per collection),
    /// advanced by every committed write-apply. Type + GC in `write_index.rs`.
    pub(in crate::data::executor) write_index: super::write_index::WriteVersionIndex,

    /// Scratch map (surrogate → resolve-time bitemporal stamp) consulted ONLY
    /// by `apply_point_put`. Populated right before a bitemporal document apply
    /// scope — from a committing transaction's overlay sidecar (commit-time
    /// install) or a decoded 8-tuple redo sub-record (WAL replay) — and cleared
    /// right after. When a surrogate has an entry, the put is forced onto the
    /// versioned store at the carried stamp rather than deriving a fresh one,
    /// so the base install and the redo agree on the version key even when
    /// `doc_configs` is empty (the real replay-time boot state).
    pub(in crate::data::executor) active_bitemporal_stamps:
        HashMap<u32, crate::data::executor::handlers::transaction::overlay::BitemporalStamp>,

    /// Staged Calvin write plans awaiting the local commit verdict, keyed by
    /// `(epoch, position, vshard)`. `CalvinExecuteStatic` validates a
    /// transaction and inserts its plans here WITHOUT mutating base; the
    /// verdict-driven `CalvinFlush` replays them through
    /// `execute_transaction_batch` to make them durable, and `CalvinDrop`
    /// discards them. Nothing staged here is observable in the base engines
    /// until a flush.
    ///
    /// The vShard is part of the key because vShards round-robin onto cores:
    /// several vShard slices of one multi-participant transaction — which share
    /// the same `(epoch, position)` — can land on the same core, and each must
    /// stage and flush its own slice independently rather than clobber a peer's.
    pub(in crate::data::executor) commit_pending:
        HashMap<(u64, u32, u32), super::commit_pending::PendingCommit>,

    /// `Some((epoch, position, vshard))` only during a distributed Calvin flush apply (staging gate).
    pub(in crate::data::executor) calvin_flush_key: Option<(u64, u32, u32)>,

    /// Per-`(epoch, position, vshard)` index-value tuples drained from a Calvin
    /// flush's undo log, awaiting the post-apply `RecordCalvinWriteVersions` op.
    pub(in crate::data::executor) calvin_flush_index_tuples:
        crate::data::executor::handlers::transaction::index_write_values::StagedCalvinIndexTuples,
}
