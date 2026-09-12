// SPDX-License-Identifier: Apache-2.0

//! Control plane meta-operations dispatched to the Data Plane.

use std::collections::BTreeMap;

use nodedb_types::calvin::{PassiveReadKey, VersionedReadEntry};
use nodedb_types::timeseries::continuous_agg::ContinuousAggregateDef;
use nodedb_types::{QualifiedCollection, TenantId, Value};

pub use super::meta_calvin::PassiveReadKeyId;

/// Meta / maintenance physical operations.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum MetaOp {
    /// WAL append (write path).
    WalAppend { payload: Vec<u8> },

    /// Cancellation signal. Data Plane stops the target at next safe point.
    Cancel {
        target_request_id: nodedb_types::id::RequestId,
    },

    /// Atomic transaction batch: execute all sub-plans atomically.
    ///
    /// `txn_id` identifies the committing session transaction whose staging
    /// overlay holds the resolve-time bitemporal stamps this install must reuse
    /// (so a `bitemporal=true` document put lands on the same version key the
    /// redo carries, not a fresh one). `None` for install paths with no session
    /// overlay to consult — Calvin (which threads its stamps in directly) and
    /// procedural/test callers. Wire-additive: defaults to `None` on decode of
    /// older entries.
    TransactionBatch {
        plans: Vec<super::PhysicalPlan>,
        #[serde(default)]
        txn_id: Option<nodedb_types::id::TxnId>,
    },

    /// Create a snapshot: export all engine state for this core.
    CreateSnapshot,

    /// On-demand compaction.
    Compact,

    /// Checkpoint: flush all engine state to disk, report LSN.
    Checkpoint,

    /// Register a continuous aggregate definition on this core's manager.
    RegisterContinuousAggregate { def: ContinuousAggregateDef },

    /// Remove a continuous aggregate from this core's manager.
    UnregisterContinuousAggregate { name: String },

    /// List all continuous aggregates from this core's manager.
    /// Returns JSON-serialized `Vec<AggregateInfo>`.
    ListContinuousAggregates,

    /// Convert a collection's storage mode.
    ///
    /// Scans all documents in the collection and re-encodes them for the
    /// target type. The catalog update happens on the Control Plane after
    /// the Data Plane confirms success.
    ///
    /// `target_type`: "document_schemaless", "document_strict", "kv".
    /// `schema_json`: for "document_strict"/"kv", JSON-serialized column definitions.
    /// `source_storage_mode`: the collection's storage mode BEFORE this
    /// conversion, read from the catalog by the Control Plane dispatcher.
    /// The Data Plane's own `doc_configs` cache still reflects the OLD mode
    /// at dispatch time (the catalog flip and re-register happen after this
    /// op returns), so the handler cannot resolve the source format from
    /// that cache. It must take it from the plan instead.
    ConvertCollection {
        collection: QualifiedCollection,
        target_type: String,
        schema_json: String,
        source_storage_mode: super::document::StorageMode,
    },

    /// Snapshot a tenant's data from the sparse engine.
    /// Returns serialized `(documents, indexes)` as JSON payload.
    CreateTenantSnapshot { tenant_id: u64 },

    /// Restore a tenant's data across all engines from a snapshot.
    /// `snapshot` is a MessagePack-serialized `TenantDataSnapshot`.
    ///
    /// `replace_mode` selects the collision policy for engines whose restore is
    /// otherwise fail-closed (columnar / vector / flushed-timeseries):
    /// - `false` (user RESTORE): refuse to overwrite keys already present.
    /// - `true` (Raft InstallSnapshot apply): OVERWRITE present keys, because a
    ///   Raft install must replace local state, not fail against it.
    RestoreTenantSnapshot {
        tenant_id: u64,
        snapshot: Vec<u8>,
        replace_mode: bool,
        /// vShard IDs whose state must be cleared before install (clear-then-install
        /// for a lagging follower). Empty = legacy install-over-present behavior.
        #[serde(default)]
        clear_vshards: Vec<u32>,
        /// (tenant_id, collection) pairs to clear before install — pre-resolved by the
        /// applier from the local catalog for the cleared vShards. Empty = no clear.
        #[serde(default)]
        collections_to_clear: Vec<(u64, String)>,
    },

    /// Purge ALL data for a tenant across every engine and cache.
    ///
    /// Deletes documents, indexes, vectors, graph edges, timeseries,
    /// KV entries, CRDT state, inverted index terms, and cache entries.
    /// Idempotent: safe to re-run after a crash.
    PurgeTenant { tenant_id: u64 },

    /// Purge a single collection across every engine on this core.
    ///
    /// Runs the per-collection equivalent of `PurgeTenant`: each
    /// engine (columnar, vector, FTS, spatial, document/strict/kv,
    /// graph, CRDT, timeseries) reclaims its L1 segment files,
    /// memtables, in-flight compactions, and snapshot references
    /// for this one collection, then a WAL `CollectionTombstoned`
    /// record is appended so replay after purge does not resurrect
    /// rows. The quiesce-drain primitive (see `bridge::quiesce`) is
    /// invoked first so in-flight scans complete before file unlink.
    ///
    /// `purge_lsn` is the metadata-raft commit LSN at which the
    /// `PurgeCollection` entry committed; used by the WAL tombstone
    /// filter and surfaced in the audit trail.
    ///
    /// Idempotent: re-running after partial completion picks up
    /// where the previous run left off.
    UnregisterCollection {
        tenant_id: u64,
        name: String,
        purge_lsn: u64,
        /// Whether this core must reclaim the collection's shared on-disk L1
        /// checkpoint/partition files. Those paths are keyed by
        /// `(database, tenant, collection)` — not by core — so the Control
        /// Plane sets this `true` for exactly one core (the collection's
        /// homing vshard) in the all-cores fan-out. Every core still evicts
        /// its own per-core in-memory state; only the shared-path unlink /
        /// `remove_dir_all` is single-cored, so concurrent cores cannot race
        /// on the same tree.
        reclaim_l1_files: bool,
    },

    /// Reclaim every local Data Plane resource for a single
    /// materialized view. Mirrors `UnregisterCollection` one level
    /// up: an MV has its own columnar segment files (populated by the
    /// CDC refresh loop) that outlive the MV's catalog row when the
    /// MV is dropped, unless reclaim runs on every follower. Drops
    /// the MV's in-memory refresh state + unlinks its segment files.
    ///
    /// Idempotent: missing in-memory state is a no-op; missing files
    /// are a no-op. Runs on every node.
    UnregisterMaterializedView { tenant_id: u64, name: String },

    /// Estimate the on-core data size (in bytes) for a single
    /// `(tenant_id, collection)` pair. Sums per-engine in-memory
    /// state: KV hash-table bytes, columnar flushed-segment byte
    /// count, vector-index byte count, and sparse-redb document
    /// range. Response payload is a u64 LE byte count.
    ///
    /// Used by `_system.dropped_collections.size_bytes_estimate` to
    /// surface "how much storage will a hard-delete reclaim?"
    /// without waiting for a purge cycle.
    QueryCollectionSize { tenant_id: u64, name: String },

    /// Enforce retention on a timeseries collection: drop segments older than
    /// the cutoff. Called by the retention policy enforcement loop.
    EnforceTimeseriesRetention {
        collection: QualifiedCollection,
        max_age_ms: i64,
    },

    /// Bitemporal audit-retention purge for the graph edge store.
    ///
    /// Deletes versioned edge rows (in `edge_store/temporal` layout) where
    /// `system_from_ms < cutoff_system_ms` AND a newer version of the same
    /// base key exists. Never deletes the single surviving (latest) version
    /// of a base key, even if it is older than the cutoff — "audit retain"
    /// reclaims only *superseded* history, not the current row.
    /// Emits one `RecordType::TemporalPurge` WAL record per purged batch.
    TemporalPurgeEdgeStore {
        tenant_id: u64,
        collection: QualifiedCollection,
        cutoff_system_ms: i64,
    },

    /// Bitemporal audit-retention purge for the DocumentStrict versioned
    /// tables (`documents_versioned` + `indexes_versioned`). Same semantics
    /// as `TemporalPurgeEdgeStore`: drop versions older than `cutoff_system_ms`
    /// when a newer version of the same `(tenant, coll, doc_id)` exists;
    /// never drop the single surviving version. Deletes index-version rows
    /// keyed to the purged document versions in the same transaction.
    TemporalPurgeDocumentStrict {
        tenant_id: u64,
        collection: QualifiedCollection,
        cutoff_system_ms: i64,
    },

    /// Bitemporal audit-retention purge for plain columnar collections.
    /// Reuses the timeseries max-system-ts axis on partition meta: any
    /// partition whose `max_system_ts < cutoff_system_ms` and whose max
    /// column version has been superseded by a later partition is removed.
    /// Non-bitemporal columnar collections are a no-op (collection is
    /// expected to be flagged bitemporal by the caller).
    TemporalPurgeColumnar {
        tenant_id: u64,
        collection: QualifiedCollection,
        cutoff_system_ms: i64,
    },

    /// Bitemporal audit-retention purge for CRDT (Loro-backed) collections.
    /// Drops archived row versions whose `_ts_system < cutoff_system_ms`
    /// from the per-collection bitemporal history sibling. Never removes
    /// the live row, so `AS OF now()` reads remain correct post-purge.
    TemporalPurgeCrdt {
        tenant_id: u64,
        collection: QualifiedCollection,
        cutoff_system_ms: i64,
    },

    /// Bitemporal audit-retention purge for the array engine.
    ///
    /// Drops superseded tile versions (those with `system_from_ms <
    /// cutoff_system_ms` where a newer version of the same tile key
    /// exists) from the array's bitemporal storage. The single surviving
    /// (latest) version of each tile is never removed. Arrays are
    /// globally-scoped — `tenant_id` carries the sentinel value `0`.
    TemporalPurgeArray {
        tenant_id: u64,
        /// Global array id from `ArrayCatalogEntry::name`. Arrays are
        /// not yet tenant-scoped.
        array_id: String,
        cutoff_system_ms: i64,
    },

    /// Alter the bitemporal retention policy of an array.
    ///
    /// All real work (catalog rewrite + registry update) is done on the
    /// Control Plane before this op is emitted. The Data Plane handler
    /// returns an 8-byte LE u64 acknowledgement (new `audit_retain_ms`
    /// if set, or 0). This variant exists so the alter command travels
    /// through the standard plan dispatch path and its permission is
    /// classified as `Admin`.
    ///
    /// Double-`Option` semantics:
    /// - `None`          = field omitted from SET clause; do not change.
    /// - `Some(None)`    = SET to NULL (unregister from retention registry).
    /// - `Some(Some(v))` = SET to v.
    AlterArray {
        /// Global array id (name). Arrays are not yet tenant-scoped.
        array_id: String,
        audit_retain_ms: Option<Option<i64>>,
        minimum_audit_retain_ms: Option<Option<u64>>,
    },

    /// Apply retention to continuous aggregate buckets managed by
    /// the aggregate manager. Drops materialized buckets older than
    /// each aggregate's configured retention_period_ms.
    ApplyContinuousAggRetention,

    /// Query the watermark for a named continuous aggregate.
    /// Returns JSON-serialized `WatermarkState`.
    QueryAggregateWatermark { aggregate_name: String },

    /// Query all entries from a collection's last-value cache.
    /// Returns JSON-serialized `Vec<(u64, i64, f64)>` — (series_id, ts, value).
    QueryLastValues { collection: QualifiedCollection },

    /// Query a single series from a collection's last-value cache.
    /// Returns JSON-serialized `Option<(i64, f64)>` — (ts, value).
    QueryLastValue {
        collection: QualifiedCollection,
        series_id: u64,
    },

    /// Calvin deterministic executor: static-set multi-shard transaction.
    ///
    /// The Calvin scheduler dispatches this variant after lock acquisition for
    /// transactions whose read/write set is fully known at submission time (the
    /// common case). The Data Plane handler executes `plans` atomically (same
    /// semantics as `TransactionBatch`) and the scheduler writes a
    /// `WalRecord::CalvinApplied` after a successful response.
    ///
    /// NOTE: This variant occupies the same msgpack positional tag as the
    /// original `CalvinExecute` variant it replaces, preserving wire
    /// compatibility with any in-flight log entries from before the rename.
    CalvinExecuteStatic {
        /// Sequencer epoch this transaction belongs to.
        epoch: u64,
        /// Zero-based position within the epoch batch.
        position: u32,
        /// Tenant scope for all plans in this batch.
        tenant_id: TenantId,
        /// Physical plans to execute atomically.
        plans: Vec<super::PhysicalPlan>,
        /// Wall-clock ms read once on the sequencer leader at epoch creation.
        /// Used by engine handlers as the deterministic time anchor (bitemporal
        /// sys_from, KV TTL expire_at, timeseries system_ms) instead of reading
        /// the wall clock independently. Wire-additive: defaults to 0 on decode
        /// of older entries.
        epoch_system_ms: i64,
        /// Whether THIS node is the leader of the data-group owning this vshard,
        /// stamped by the dispatching scheduler. OLLP verification (`actual !=
        /// predicted` → `OllpRetryRequired`) is leader-only; followers apply the
        /// carried `ollp_predicted_surrogates` verbatim so every replica mutates
        /// the identical set (Calvin determinism). Per-node, never replicated.
        /// Wire-additive: defaults to `false` on decode of older entries.
        is_group_leader: bool,
        /// The transaction's LSN-versioned read-set from the replicated `TxClass`.
        /// Each participant checks its own vShard's reads at apply and records
        /// whether they were still current, without gating apply. Wire-additive:
        /// defaults to empty on decode of older entries.
        #[serde(default)]
        versioned_reads: Vec<VersionedReadEntry>,
    },

    /// Calvin dependent-read executor: passive participant reads keys and
    /// returns values for broadcast.
    ///
    /// Dispatched by the scheduler to passive vshards (those holding only
    /// read keys, not write keys) for a dependent-read Calvin transaction.
    /// The Data Plane handler reads each key from the local engine and
    /// returns a msgpack-encoded `Vec<(PassiveReadKeyId, Value)>` payload.
    /// The scheduler then proposes a `ReplicatedWrite::CalvinReadResult`
    /// to the per-vshard Raft group so all replicas see the same values.
    CalvinExecutePassive {
        /// Sequencer epoch this transaction belongs to.
        epoch: u64,
        /// Zero-based position within the epoch batch.
        position: u32,
        /// Tenant scope.
        tenant_id: TenantId,
        /// Keys to read on this passive vshard.
        keys_to_read: Vec<PassiveReadKey>,
    },

    /// Calvin dependent-read executor: active participant writes with
    /// injected read values.
    ///
    /// Dispatched by the scheduler to active vshards (those holding write
    /// keys) once all passive read results have been received. The
    /// `injected_reads` map is keyed by `PassiveReadKeyId` and carries the
    /// values broadcast by the passive participants.
    ///
    /// OLLP verification: before writing, the handler checks whether the
    /// predicate match in the txn's read set still matches the actual rows
    /// in the engine. If mismatched, returns `OllpRetryRequired` status
    /// and does NOT write. The OLLP orchestrator on the Control Plane
    /// interprets this status and retries via `Inbox::submit`.
    CalvinExecuteActive {
        /// Sequencer epoch this transaction belongs to.
        epoch: u64,
        /// Zero-based position within the epoch batch.
        position: u32,
        /// Tenant scope.
        tenant_id: TenantId,
        /// Physical plans to execute atomically.
        plans: Vec<super::PhysicalPlan>,
        /// Read values injected from passive participants.
        /// `BTreeMap` for deterministic iteration order (determinism contract).
        injected_reads: BTreeMap<PassiveReadKeyId, Value>,
        /// Wall-clock ms read once on the sequencer leader at epoch creation.
        /// Same semantics as `CalvinExecuteStatic::epoch_system_ms`.
        epoch_system_ms: i64,
        /// Whether THIS node is the leader of the data-group owning this
        /// vshard. Same semantics as `CalvinExecuteStatic::is_group_leader`:
        /// gates the LEADER-ONLY OLLP verification + `OllpRetryRequired`; every
        /// replica applies the carried `ollp_predicted_surrogates` set verbatim
        /// for determinism. Per-node, non-replicated; wire-additive (defaults to
        /// `false`).
        is_group_leader: bool,
    },

    /// Rebuild all indexes (HNSW, FTS LSM, graph CSR) for a collection
    /// on this core in a shadow-build + atomic-swap manner.
    ///
    /// When `concurrent = true`, the build proceeds without blocking query
    /// handling: a background OS thread performs the rebuild and the Data
    /// Plane polls for completion on subsequent ticks, only swapping the
    /// live index in at cutover.  When `concurrent = false` the rebuild
    /// runs inline (same semantics as the legacy Checkpoint path).
    ///
    /// `index_name` narrows the rebuild to a single named index when set;
    /// `None` rebuilds all index types for the collection.
    ///
    /// Returns `Response::Ok` on successful cutover, or a typed error if:
    /// - another rebuild is already in progress for this collection
    ///   (`ErrorCode::Conflict`), or
    /// - the shadow build fails (`ErrorCode::Internal`).
    RebuildIndex {
        collection: QualifiedCollection,
        index_name: Option<String>,
        concurrent: bool,
    },

    /// Persist a synonym group to the FTS backend on this core.
    ///
    /// Called after the Control Plane has already written to the catalog and
    /// updated the in-memory registry. The Data Plane handler writes the group
    /// to the `FtsIndex` meta store so query-time expansion can find it.
    PutSynonymGroup {
        tenant_id: u64,
        /// Serialized `SynonymGroupRecord` (JSON).
        record_json: String,
    },

    /// Remove a synonym group from the FTS backend on this core.
    ///
    /// Called after the Control Plane has already removed it from the catalog.
    DeleteSynonymGroup { tenant_id: u64, name: String },

    /// Re-key all documents and secondary indexes for a collection from
    /// `old_collection` (db-qualified source name) to `new_collection`
    /// (db-qualified target name) in the local Data Plane sparse engine.
    ///
    /// Called after `MoveTenantCutover` applies so that physical data is
    /// accessible under the new database context.  Both `old_collection` and
    /// `new_collection` are the `db_qualified` strings used as the logical
    /// collection identifier in the sparse store
    /// (e.g. `"2/orders"` for database 2, collection `orders`).
    RenameCollection {
        tenant_id: u64,
        old_database_id: u64,
        new_database_id: u64,
        old_collection: QualifiedCollection,
        new_collection: QualifiedCollection,
    },

    /// Execute a point write at STATEMENT time by STAGING it into the
    /// per-transaction overlay, instead of buffering it for COMMIT.
    ///
    /// `plan` is a single point-write `DocumentOp` (`PointPut` / `PointInsert`
    /// / `PointDelete` / `PointUpdate`). The Data Plane handler evaluates the
    /// write against BASE ∪ OVERLAY: it raises constraint violations
    /// (unique / primary-key) immediately, computes the real affected-row
    /// count, and records the resulting body (or tombstone) in the overlay so
    /// a subsequent same-transaction read-modify-write observes it. It does
    /// NOT make the write durable — the buffered plan is still replayed
    /// through the real apply path inside the COMMIT `TransactionBatch`, which
    /// remains the sole durable apply. Keyed by the request's `txn_id`.
    StageWrite { plan: Box<super::PhysicalPlan> },

    /// Drop the per-transaction staging overlay for a completed (committed
    /// or rolled-back) transaction on this core.
    ///
    /// The overlay (`crate::data::executor::handlers::transaction::overlay::TxnOverlay`)
    /// holds not-yet-durable writes staged during a transaction; once the
    /// transaction resolves, its overlay entry must be released so it does
    /// not leak across the `CoreLoop`'s lifetime. `HashMap::remove` on an
    /// absent key is a no-op, so this is safe to dispatch even when no
    /// overlay was ever populated for the given `txn_id`.
    DropTxnOverlay { txn_id: nodedb_types::id::TxnId },

    /// Mark a savepoint in the per-transaction staging overlays.
    ///
    /// A single savepoint spans BOTH the value/TTL overlay and the parallel
    /// GRAPH overlay, which keep independent undo journals. The Data Plane
    /// returns a 16-byte composite marker — two little-endian `u64`s: the
    /// value overlay's journal length followed by the GRAPH overlay's — so the
    /// Control Plane can record both as the savepoint's rollback markers.
    /// In-memory only — savepoints append no WAL. Keyed by the request's
    /// `txn_id`.
    MarkSavepoint { txn_id: nodedb_types::id::TxnId },

    /// Roll the per-transaction staging overlays back to a savepoint.
    ///
    /// Replays BOTH overlays' undo journals from their ends down to their
    /// respective markers in reverse — restoring each recorded prior slot (or
    /// removing it when absent) in the value/TTL overlay to `value_marker` and
    /// in the GRAPH overlay to `graph_marker` — then truncates each journal to
    /// its marker. The transaction stays open. In-memory only. Keyed by
    /// `txn_id`.
    RollbackToSavepoint {
        txn_id: nodedb_types::id::TxnId,
        value_marker: u64,
        graph_marker: u64,
    },

    /// Record the per-key / per-collection write versions of a committed
    /// Calvin transaction's locally-applied write plans.
    ///
    /// A Calvin apply's committed WAL LSN is known only after the apply
    /// succeeds, so the apply itself cannot advance the version index. The
    /// scheduler stamps that LSN onto this op's `wal_lsn` and dispatches it back
    /// to the same core, which funnels `plans` through the shared write-version
    /// recorder at that LSN — landing in the same shard-local WAL-LSN space the
    /// single-shard fast path and read watermarks use. Records only: no base
    /// mutation, no WAL append, no event emission. Wire-additive (appended last)
    /// so older log entries decode unchanged.
    RecordCalvinWriteVersions {
        /// Tenant scope for all plans.
        tenant_id: TenantId,
        /// The locally-applied write plans whose keys' versions are recorded.
        plans: Vec<super::PhysicalPlan>,
        /// Calvin epoch of the applied transaction. With `position` and the
        /// request's vShard, keys the index-value tuples the flush staged so the
        /// core drains and records them at this op's applied LSN.
        epoch: u64,
        /// Calvin position within the epoch (see `epoch`).
        position: u32,
    },

    /// Flush the staged writes of a Calvin transaction to base storage.
    ///
    /// `CalvinExecuteStatic` validates and STAGES the transaction's plans into
    /// the per-core commit-pending buffer without mutating base. Once the local
    /// commit vote resolves to commit, the scheduler dispatches this op back to
    /// the same core, which pops the staged plans keyed by `(epoch, position)`
    /// and replays them through the durable apply funnel (base + side effects +
    /// version recording). Absent key (already flushed/dropped) is an idempotent
    /// no-op, not an error.
    CalvinFlush { epoch: u64, position: u32 },

    /// Discard the staged writes of a Calvin transaction.
    ///
    /// Dispatched by the scheduler when the local commit vote resolves to abort.
    /// The handler removes the staged plans keyed by `(epoch, position)` and
    /// fires nothing — no base mutation, no side effects. Absent key is an
    /// idempotent no-op.
    CalvinDrop { epoch: u64, position: u32 },

    /// Resolve a committing transaction's staged writes into ONE replayable
    /// redo record, WITHOUT mutating base.
    ///
    /// The Data Plane handler reads the per-transaction staging overlay
    /// (`CoreLoop::txn_overlays`) by shared reference and, for every staged
    /// post-image, emits the engine-native WAL sub-record shape that engine's
    /// autocommit path already produces. The encoded `RedoRecord` bytes are
    /// returned in the response payload; the Control Plane appends them as a
    /// single `RecordType::TransactionRedo` WAL record and a later install phase
    /// replays them. No base engine is touched during resolve — a crash before
    /// install loses only an unacknowledged commit.
    ///
    /// `plans` is the transaction's buffered write set: it is walked to classify
    /// every op and to discover which collections' overlay entries to serialize.
    /// An op class whose resolve serializer is not yet built raises a typed
    /// error rather than being silently omitted (a dropped op class would lose
    /// those rows on install). Wire-additive: appended last so older log entries
    /// decode unchanged.
    ResolveTxn {
        txn_id: nodedb_types::id::TxnId,
        plans: Vec<super::PhysicalPlan>,
    },

    /// Resolve a staged Calvin transaction's write plans into ONE replayable
    /// redo record, WITHOUT mutating base.
    ///
    /// Mirrors `ResolveTxn` exactly, but reads Calvin's own staging state
    /// instead of a session transaction's: the plans buffered in the core's
    /// `commit_pending` under `(epoch, position, vshard)` and the per-core
    /// staging overlay written under the corresponding synthetic `TxnId`
    /// (see `calvin_synthetic_txn_id`). Dispatched by the scheduler once the
    /// local commit vote resolves to commit, in place of (or ahead of)
    /// `CalvinFlush` — the flush path mutates base directly, while resolve
    /// produces a durable redo record for a later install phase instead. No
    /// base engine is touched during resolve. Wire-additive: appended last
    /// so older log entries decode unchanged.
    CalvinResolve { epoch: u64, position: u32 },
}
