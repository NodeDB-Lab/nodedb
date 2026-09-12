// SPDX-License-Identifier: Apache-2.0

use nodedb_types::{
    QualifiedCollection, RlsWriteCheck, Surrogate, SurrogateBitmap, SystemTimeScope,
};

use super::merge_types::MergeClauseOp;
use super::ollp_edge::OllpPredictedEdge;
use super::sum_target::ResolvedSumTarget;
use super::timeseries_schema::TimeseriesSchema;
use super::types::{EnforcementOptions, RegisteredIndex, ReturningSpec, StorageMode, UpdateValue};

/// Document engine physical operations (schemaless + strict + DML).
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum DocumentOp {
    /// Point lookup by document ID.
    PointGet {
        collection: QualifiedCollection,
        document_id: String,
        /// Catalog-bound identity for `(collection, document_id)`. Hex-encoded
        /// by the handler for the substrate row key — user-PK strings are
        /// never used for storage addressing here.
        surrogate: Surrogate,
        /// Raw primary-key bytes, for follower-side WAL decode to re-derive
        /// the surrogate via the catalog rev table.
        pk_bytes: Vec<u8>,
        /// RLS post-fetch filters (serialized `Vec<ScanFilter>`). Denial
        /// returns `NOT_FOUND`, never a distinguishable error (no info leak).
        rls_filters: Vec<u8>,
        /// System-time selection. `Current` = current state. Honored only by
        /// bitemporal collections; the planner rejects temporal point-gets on
        /// non-bitemporal collections. `AllVersions` is rejected for point-gets.
        system_time: SystemTimeScope,
        /// `FOR VALID_TIME CONTAINS <ms>` filter.
        valid_at_ms: Option<i64>,
    },

    /// Point write: insert/update a document.
    ///
    /// This variant is unconditional-overwrite (upsert semantics). Use
    /// [`DocumentOp::PointInsert`] for SQL `INSERT` where duplicate PKs must
    /// raise `unique_violation`.
    PointPut {
        collection: QualifiedCollection,
        document_id: String,
        value: Vec<u8>,
        /// Catalog-bound identity for `(collection, document_id)`.
        /// Hex-encoded by the handler to compute the substrate row key.
        surrogate: Surrogate,
        /// Raw primary-key bytes, used by follower-side WAL decode to
        /// re-derive the surrogate via the catalog rev table.
        pk_bytes: Vec<u8>,
        /// When `Some`, return the STORED post-image, per spec (generated
        /// columns evaluated, `_rowid` injected). Never the submitted body —
        /// an echo would report the request, not what landed.
        #[serde(default)]
        returning: Option<ReturningSpec>,
        /// Read filters bounding `returning` to what a `SELECT` by the same
        /// principal would show. See `PointDelete::rls_filters`.
        #[serde(default)]
        rls_filters: Vec<u8>,
        /// `(target collection, join-key value)` → target row surrogate,
        /// resolved by the Control Plane at plan time (Data Plane has no
        /// PK→surrogate map). Keyed on the pair: one source can drive two
        /// bindings on the same join column.
        #[serde(default)]
        resolved_sum_targets: Vec<ResolvedSumTarget>,
    },

    /// Point insert: write one document, fail on duplicate primary key
    /// unless `if_absent` (silent skip on conflict).
    ///
    /// Separate from [`DocumentOp::PointPut`]: the insert must probe
    /// `document_id` existence inside the same write txn.
    PointInsert {
        collection: QualifiedCollection,
        document_id: String,
        value: Vec<u8>,
        if_absent: bool,
        /// Stable cross-engine identity assigned by the CP-side
        /// `SurrogateAssigner` from `(collection, document_id_bytes)`.
        /// `Surrogate::ZERO` is reserved as a sentinel and only appears
        /// in test fixtures.
        surrogate: Surrogate,
        /// When `Some`, return the STORED post-image of the inserted row
        /// projected per spec — see `PointPut::returning`. A conflict skipped
        /// by `if_absent` inserts nothing and therefore returns no row.
        #[serde(default)]
        returning: Option<ReturningSpec>,
        /// Read filters gating the rows `returning` emits — see
        /// `PointDelete::rls_filters`.
        #[serde(default)]
        rls_filters: Vec<u8>,
        /// See `PointPut::resolved_sum_targets`.
        #[serde(default)]
        resolved_sum_targets: Vec<ResolvedSumTarget>,
        /// Sum-binding TARGET collections this write must not apply its own
        /// delta for — the Control Plane appended a separate
        /// [`ApplyBalanceDelta`](DocumentOp::ApplyBalanceDelta) task homed on
        /// each target's vShard instead. Empty when targets are co-resident
        /// or the collection has no binding.
        #[serde(default)]
        deferred_sum_targets: Vec<String>,
    },

    /// Point delete: remove a document.
    PointDelete {
        collection: QualifiedCollection,
        document_id: String,
        /// Catalog-bound identity for `(collection, document_id)`. The
        /// handler hex-encodes this for the substrate row key.
        surrogate: Surrogate,
        /// Raw primary-key bytes for follower WAL decode rebind.
        pk_bytes: Vec<u8>,
        /// When `Some`, return the pre-deletion document projected per spec.
        #[serde(default)]
        returning: Option<ReturningSpec>,
        /// Read filters bounding `returning` to what a `SELECT` by the same
        /// principal would show.
        #[serde(default)]
        rls_filters: Vec<u8>,
        /// Write policy gating the persist against the row's pre-image (this
        /// op has no post-image), or the reason none applies. Fails the whole
        /// statement with `RejectedAuthz` — never a silent skip. Separate slot
        /// from `rls_filters`, which is the read-side redaction gate.
        rls_write_check: RlsWriteCheck,
        /// See `PointPut::resolved_sum_targets`.
        #[serde(default)]
        resolved_sum_targets: Vec<ResolvedSumTarget>,
    },

    /// Point update: read-modify-write with field-level changes.
    PointUpdate {
        collection: QualifiedCollection,
        document_id: String,
        /// Catalog-bound identity for `(collection, document_id)`. The
        /// handler hex-encodes this for the substrate row key.
        surrogate: Surrogate,
        /// Raw primary-key bytes for follower WAL decode rebind.
        pk_bytes: Vec<u8>,
        /// Field name → assignment RHS (literal bytes or row-scope expression).
        updates: Vec<(String, UpdateValue)>,
        /// When `Some`, return the post-update document projected per spec.
        #[serde(default)]
        returning: Option<ReturningSpec>,
        /// Read filters gating `returning` — see `PointDelete::rls_filters`.
        #[serde(default)]
        rls_filters: Vec<u8>,
        /// Write policy gating the persist against the post-update image, or
        /// the reason none applies — see `PointDelete::rls_write_check`.
        rls_write_check: RlsWriteCheck,
        /// See `PointPut::resolved_sum_targets`.
        #[serde(default)]
        resolved_sum_targets: Vec<ResolvedSumTarget>,
        /// The collection's declared `PRIMARY KEY` column, `Some` only for a
        /// schemaless collection. The Data Plane refuses a post-image whose
        /// value at this field is absent or JSON null.
        #[serde(default)]
        declared_primary_key: Option<String>,
    },

    /// Full collection scan with filtering, sorting, and pagination.
    Scan {
        collection: QualifiedCollection,
        limit: usize,
        offset: usize,
        sort_keys: Vec<crate::physical_plan::SortKeySpec>,
        /// Filter predicates serialized as JSON.
        filters: Vec<u8>,
        distinct: bool,
        projection: Vec<String>,
        /// Serialized `Vec<ComputedColumn>`.
        computed_columns: Vec<u8>,
        /// Serialized `Vec<WindowFuncSpec>`.
        window_functions: Vec<u8>,
        /// `Current` / `AsOf(ms)` / `AllVersions` (audit log, ascending).
        /// Honored only on bitemporal collections; the planner rejects
        /// temporal scans on non-bitemporal ones at SQL plan time.
        system_time: SystemTimeScope,
        /// `FOR VALID_TIME CONTAINS <ms>` filter. `None` = no filter.
        valid_at_ms: Option<i64>,
        /// Optional surrogate prefilter injected by a cross-engine sub-plan.
        /// When present, the scan skips rows whose surrogate is absent from
        /// this bitmap. `None` = no prefilter; full collection is scanned.
        #[serde(default)]
        prefilter: Option<SurrogateBitmap>,
    },

    /// Batch insert documents in a single redb transaction.
    BatchInsert {
        collection: QualifiedCollection,
        /// (document_id, value_bytes) pairs.
        documents: Vec<(String, Vec<u8>)>,
        /// Per-row surrogates (parallel to `documents`). When non-empty and
        /// same length as `documents`, the handler uses these for FTS indexing.
        /// `Surrogate::ZERO` entries are silently skipped by the FTS path.
        surrogates: Vec<nodedb_types::Surrogate>,
        /// When `Some`, return one row per inserted document — the STORED
        /// post-image of each, in `documents` order — projected per spec.
        /// See `PointPut::returning`.
        #[serde(default)]
        returning: Option<ReturningSpec>,
        /// Read filters gating the rows `returning` emits — see
        /// `PointDelete::rls_filters`.
        #[serde(default)]
        rls_filters: Vec<u8>,
        /// See `PointPut::resolved_sum_targets`. One entry per distinct pair
        /// across `documents`.
        #[serde(default)]
        resolved_sum_targets: Vec<ResolvedSumTarget>,
        /// See `PointInsert::deferred_sum_targets`.
        #[serde(default)]
        deferred_sum_targets: Vec<String>,
    },

    /// Range scan on a sparse/metadata index.
    RangeScan {
        collection: QualifiedCollection,
        field: String,
        lower: Option<Vec<u8>>,
        upper: Option<Vec<u8>>,
        limit: usize,
        /// RLS filters applied post-fetch (no pushdown slot in storage) —
        /// same shape `KvOp::Get` and `DocumentOp::PointGet` use.
        #[serde(default)]
        rls_filters: Vec<u8>,
    },

    /// Register collection with secondary indexes and storage mode (DDL).
    Register {
        collection: QualifiedCollection,
        /// Full secondary-index specs (name, path, unique, case_insensitive,
        /// state). Replaces the old `Vec<String>` path-only payload so the
        /// write handler can enforce UNIQUE and skip Building indexes.
        indexes: Vec<RegisteredIndex>,
        crdt_enabled: bool,
        /// Storage encoding mode. Determines how documents are serialized.
        storage_mode: StorageMode,
        /// Collection enforcement options propagated from catalog (boxed to reduce enum size).
        enforcement: Box<EnforcementOptions>,
        /// Bitemporal storage: every write becomes a new version keyed by
        /// `system_from_ms`; reads use the versioned table and Ceiling
        /// resolver.
        bitemporal: bool,
        /// Durable CRDT conflict policy (JSON `CollectionPolicy`). `Some`
        /// rehydrates the per-core `PolicyRegistry` on reboot so `ALTER
        /// COLLECTION ... SET ON CONFLICT` survives a restart; `None` falls
        /// back to `CollectionPolicy::ephemeral()`.
        conflict_policy: Option<String>,
        /// Declared columns + `TIME_KEY` for a timeseries collection.
        /// `Some` only for `engine='timeseries'`; the Data Plane builds the
        /// memtable schema from this instead of inferring from ingest.
        timeseries: Option<Box<TimeseriesSchema>>,
        /// Vector-primary access-path config, `Some` only for
        /// `WITH (primary='vector')`. Both plain and vector-primary rows are
        /// legal MessagePack maps — this is the only way to tell them apart.
        vector_primary: Option<Box<nodedb_types::VectorPrimaryConfig>>,
    },

    /// Lookup documents by secondary index value.
    IndexLookup {
        collection: QualifiedCollection,
        path: String,
        value: String,
    },

    /// Fetch full document rows via a secondary index, for a SELECT with an
    /// equality predicate on an indexed field.
    ///
    /// `filters` is the residual predicate left after the indexed equality,
    /// applied to every fetched body. Sort / distinct / window functions
    /// fall back to a full scan upstream — never reach this variant.
    IndexedFetch {
        collection: QualifiedCollection,
        /// Indexed field path (e.g. `$.email`).
        path: String,
        /// Equality lookup value. COLLATE NOCASE rewrites normalize to
        /// lowercase before emission, so the handler does not need to.
        value: String,
        /// Remaining post-filters (serialized `Vec<ScanFilter>`).
        filters: Vec<u8>,
        /// Column names to include in each row (empty = all fields).
        projection: Vec<String>,
        limit: usize,
        offset: usize,
    },

    /// Drop all secondary index entries for a field.
    DropIndex {
        collection: QualifiedCollection,
        field: String,
    },

    /// Backfill a secondary index from existing rows (CREATE INDEX on a
    /// non-empty collection). Runs as one write transaction so the index is
    /// consistent when the Ready flip commits.
    BackfillIndex {
        collection: QualifiedCollection,
        /// JSON-path-like field (e.g. `$.email`).
        path: String,
        is_array: bool,
        unique: bool,
        case_insensitive: bool,
        /// Partial-index predicate (raw SQL text of the `WHERE` body)
        /// or `None` for full indexes. Rows where the predicate is
        /// false are skipped — not indexed, not UNIQUE-checked.
        #[serde(default)]
        predicate: Option<String>,
    },

    /// Truncate: delete ALL documents in a collection.
    /// If `restart_identity` is true, sequences attached to this collection's
    /// fields are reset to their start value after truncation.
    Truncate {
        collection: QualifiedCollection,
        restart_identity: bool,
        /// Sum-binding targets from a Control-Plane recon scan of the rows
        /// this statement removes. The Data-Plane leader re-derives the
        /// actual set and returns `ErrorCode::OllpRetryRequired` on
        /// divergence, before writing.
        #[serde(default)]
        resolved_sum_targets: Vec<ResolvedSumTarget>,
        /// See `PointUpdate::declared_primary_key`. Names the column each
        /// removed row's identity is read from for its event and redo entry.
        #[serde(default)]
        declared_primary_key: Option<String>,
    },

    /// Estimate count via HLL cardinality stats.
    EstimateCount {
        collection: QualifiedCollection,
        field: String,
    },

    /// INSERT ... SELECT: copy documents from source to target.
    InsertSelect {
        target_collection: QualifiedCollection,
        source_collection: QualifiedCollection,
        source_filters: Vec<u8>,
        source_limit: usize,
        /// zerompk-encoded `Vec<ComputedColumn>`: one entry per target column,
        /// its `alias` the target column name and its `expr` the source-row
        /// expression. Empty means copy each source row unchanged.
        column_map: Vec<u8>,
    },

    /// Upsert: insert or merge. When `on_conflict_updates` is non-empty,
    /// the conflict branch evaluates those assignments against the
    /// *existing* document instead of merging the inserted value —
    /// the `INSERT ... ON CONFLICT DO UPDATE SET ...` path.
    Upsert {
        collection: QualifiedCollection,
        document_id: String,
        value: Vec<u8>,
        on_conflict_updates: Vec<(String, UpdateValue)>,
        /// Stable cross-engine identity assigned by the CP-side
        /// `SurrogateAssigner`. `Surrogate::ZERO` only in test fixtures.
        surrogate: Surrogate,
        /// Write policy gating the persist against whichever body actually
        /// lands: the insert body when absent, the merged/conflict-updated
        /// row when present — see `PointDelete::rls_write_check`.
        rls_write_check: RlsWriteCheck,
        /// When `Some`, return the STORED post-image: merged row on conflict,
        /// inserted row otherwise. Never the submitted body.
        #[serde(default)]
        returning: Option<ReturningSpec>,
        /// See `PointDelete::rls_filters`.
        #[serde(default)]
        rls_filters: Vec<u8>,
        /// See `PointPut::resolved_sum_targets`.
        #[serde(default)]
        resolved_sum_targets: Vec<ResolvedSumTarget>,
    },

    /// Update target rows matched by a join with a source collection: scan
    /// source, build a merged doc per matching target row (source fields
    /// qualified `<alias>.<field>`), evaluate `updates`, write back.
    UpdateFromJoin {
        target_collection: QualifiedCollection,
        source_collection: QualifiedCollection,
        /// Qualifier used for source columns in assignment expressions.
        source_alias: String,
        /// Field in the target used for the equi-join.
        target_join_col: String,
        /// Field in the source used for the equi-join.
        source_join_col: String,
        /// SET field assignments; RHS expressions reference the merged document.
        updates: Vec<(String, UpdateValue)>,
        /// Additional WHERE predicates applying only to the target (msgpack).
        target_filters: Vec<u8>,
        #[serde(default)]
        returning: Option<ReturningSpec>,
        /// Control-Plane-shipped source rows for cross-core `UPDATE ... FROM`.
        /// When `Some`, the join-map is built from these pre-scanned
        /// `(source_doc_id, raw_stored_bytes)` rows instead of local storage —
        /// needed because source and target can live on different
        /// Data-Plane cores. `None` = legacy local-read path.
        #[serde(default)]
        source_rows: Option<Vec<(String, Vec<u8>)>>,
        /// See `PointDelete::rls_filters`; every returned row is a target row.
        #[serde(default)]
        rls_filters: Vec<u8>,
        /// Write policy of `target_collection`, evaluated against each
        /// matched target row's post-image — see `PointDelete::rls_write_check`.
        rls_write_check: RlsWriteCheck,
        /// See `PointPut::resolved_sum_targets`; resolved from a recon scan
        /// covering both sides of a join-key change.
        #[serde(default)]
        resolved_sum_targets: Vec<ResolvedSumTarget>,
        /// See `PointUpdate::declared_primary_key`.
        #[serde(default)]
        declared_primary_key: Option<String>,
    },

    /// Bulk update: scan + apply field updates to all matches.
    BulkUpdate {
        collection: QualifiedCollection,
        filters: Vec<u8>,
        updates: Vec<(String, UpdateValue)>,
        /// When `Some`, return updated documents projected per spec.
        #[serde(default)]
        returning: Option<ReturningSpec>,
        /// OLLP path: when `Some`, the executor verifies the actual matching
        /// surrogate set equals this sorted set before writing, else returns
        /// `ErrorCode::OllpRetryRequired`. `None` = no verification.
        #[serde(default)]
        ollp_predicted_surrogates: Option<Vec<u32>>,
        /// OLLP path, carried for symmetry with `BulkDelete`; the executor
        /// does not verify edge drift on this variant. `None` off OLLP.
        #[serde(default)]
        ollp_predicted_edges: Option<Vec<OllpPredictedEdge>>,
        /// See `PointDelete::rls_filters`.
        #[serde(default)]
        rls_filters: Vec<u8>,
        /// Write policy against each matched row's post-update image — see
        /// `PointDelete::rls_write_check`.
        rls_write_check: RlsWriteCheck,
        /// See `PointPut::resolved_sum_targets`; resolved from a recon scan
        /// covering both sides of a join-key change.
        #[serde(default)]
        resolved_sum_targets: Vec<ResolvedSumTarget>,
        /// See `PointUpdate::declared_primary_key`.
        #[serde(default)]
        declared_primary_key: Option<String>,
    },

    /// Bulk delete: scan + delete all matches.
    BulkDelete {
        collection: QualifiedCollection,
        filters: Vec<u8>,
        /// When `Some`, return pre-deletion documents projected per spec.
        #[serde(default)]
        returning: Option<ReturningSpec>,
        /// See `BulkUpdate::ollp_predicted_surrogates`.
        #[serde(default)]
        ollp_predicted_surrogates: Option<Vec<u32>>,
        /// OLLP path: when `Some`, the executor recomputes the actual edge set
        /// (`surrogate, _from, _to, _type` per matched edge doc) and returns
        /// `OllpRetryRequired` before writing on any divergence, closing the
        /// recon→execute TOCTOU. `None` off OLLP.
        #[serde(default)]
        ollp_predicted_edges: Option<Vec<OllpPredictedEdge>>,
        /// See `PointDelete::rls_filters`.
        #[serde(default)]
        rls_filters: Vec<u8>,
        /// Write policy against each matched row's pre-deletion image (the
        /// only image a delete has) — see `PointDelete::rls_write_check`.
        rls_write_check: RlsWriteCheck,
        /// See `PointPut::resolved_sum_targets`.
        #[serde(default)]
        resolved_sum_targets: Vec<ResolvedSumTarget>,
        /// See `PointUpdate::declared_primary_key`. Names the column each
        /// removed row's identity is read from when staged.
        #[serde(default)]
        declared_primary_key: Option<String>,
    },

    /// MERGE: join-based multi-action DML (INSERT/UPDATE/DELETE per WHEN
    /// arm). Builds a join map from the source, walks target rows applying
    /// the first matching `Matched` arm, then source rows with no target
    /// match applying `NotMatched` (INSERT), then optionally target rows
    /// with no source match applying `NotMatchedBySource`.
    Merge {
        target_collection: QualifiedCollection,
        source_collection: QualifiedCollection,
        /// Qualifier used for source columns in assignment expressions.
        source_alias: String,
        target_join_col: String,
        source_join_col: String,
        clauses: Vec<MergeClauseOp>,
        #[serde(default)]
        returning: Option<ReturningSpec>,
        /// Control-Plane-pre-assigned surrogates for NOT-MATCHED insert rows,
        /// keyed by source join value. When `Some`, the handler re-verifies
        /// the insert set against these keys (`OllpRetryRequired` on drift,
        /// no write) and applies every arm in one redb transaction. `None`
        /// is the unresolved shape every Control-Plane entry point
        /// intercepts — never reaches the Data Plane.
        #[serde(default)]
        resolved_inserts: Option<Vec<(String, u32)>>,
        /// See `UpdateFromJoin::source_rows`.
        #[serde(default)]
        source_rows: Option<Vec<(String, Vec<u8>)>>,
        /// See `PointDelete::rls_filters`; every returned row is a target row.
        #[serde(default)]
        rls_filters: Vec<u8>,
        /// Write policy of `target_collection`: post-image for an
        /// UPDATE/INSERT arm, pre-image for a DELETE arm — see
        /// `PointDelete::rls_write_check`.
        rls_write_check: RlsWriteCheck,
        /// See `PointPut::resolved_sum_targets`, resolved from the RESOLVE
        /// pass's classification. INSERT credits, DELETE debits, UPDATE
        /// applies the difference (both sides on a join-key rewrite).
        #[serde(default)]
        resolved_sum_targets: Vec<ResolvedSumTarget>,
        /// See `PointUpdate::declared_primary_key`.
        #[serde(default)]
        declared_primary_key: Option<String>,
    },

    /// Cursor-paginated scan for the clone materializer. Returns
    /// `(document_id, surrogate, value_bytes)` triples plus next-cursor as
    /// `[next_cursor: bin, entries: [[id, surrogate, value_bytes], ...]]`;
    /// `value_bytes` is always MessagePack, transcoded from the source
    /// encoding. `next_cursor` empty = scan complete.
    ///
    /// Honors `system_as_of_ms` (the clone's `as_of_lsn`) on bitemporal
    /// collections; ignored otherwise.
    MaterializeScan {
        collection: QualifiedCollection,
        cursor: Vec<u8>,
        count: usize,
        system_as_of_ms: Option<i64>,
        // Point-in-time snapshot: `AllVersions` is rejected upstream.
    },

    /// Add a signed amount to a materialized-sum balance on a TARGET row.
    ///
    /// Source and target usually live on different cores, so this rides as
    /// its own Control-Plane-appended task homed on the target's vShard
    /// (like an implicit graph edge); the pair commits atomically through
    /// Calvin. Applied as a normal read-modify-write on the Data Plane.
    ApplyBalanceDelta {
        /// TARGET collection this op's task homes on.
        collection: QualifiedCollection,
        /// Target row's storage key (hex-encoded surrogate).
        document_id: String,
        /// Target row's cross-engine identity, resolved from the join value.
        surrogate: Surrogate,
        /// The balance column this delta moves.
        column: String,
        /// Signed amount to add, as an exact decimal STRING — never `f64`,
        /// which loses precision a balance cannot afford.
        delta: String,
        /// Binding's join column — a missing target fails with the same
        /// typed error the co-resident path raises.
        join_column: String,
        /// Join value that resolved to `surrogate`.
        join_value: String,
        /// The TARGET collection's declared `PRIMARY KEY` column, when it
        /// has one. Names the target row in its event and redo entry.
        #[serde(default)]
        declared_primary_key: Option<String>,
    },

    /// Read-only resolve pass over the wrapped write op: runs its full
    /// classification and reports what it would write, without writing,
    /// indexing, or emitting events. The Control-Plane expander turns the
    /// answer into concrete point ops.
    ///
    /// Valid inner ops: [`DocumentOp::Merge`], [`DocumentOp::UpdateFromJoin`],
    /// [`DocumentOp::PointUpdate`], [`DocumentOp::PointDelete`],
    /// [`DocumentOp::Upsert`], [`DocumentOp::BulkUpdate`],
    /// [`DocumentOp::BulkDelete`] — any other is rejected.
    ///
    /// Carries no check slot: it persists nothing. The wrapped op's write
    /// predicate still governs each resolved image. Classified as a read
    /// everywhere — permission, write class, lock keys, CDC, Calvin routing.
    ResolveWrite(Box<DocumentOp>),

    /// Apply exactly the mutations a [`DocumentOp::ResolveWrite`] reported,
    /// then return the reply it reported alongside them — the one shape
    /// every governed point/bulk document write resolves to.
    ///
    /// Every mutation's `precondition` is checked before the first runs; a
    /// mismatch fails the whole write with `ErrorCode::OllpRetryRequired`
    /// and mutates nothing.
    ResolvedWrite {
        mutations: Vec<super::resolved_mutation::DocumentResolvedMutation>,
        /// The statement's reply, decided at resolve time. Every replica
        /// returns it unchanged rather than recomputing it.
        response_payload: Vec<u8>,
        /// Always `RlsWriteCheck::DecidedEarlierInRequest` — this request
        /// already admitted these row images; the slot stays so apply runs
        /// the same write gate every path runs.
        rls_write_check: RlsWriteCheck,
    },
}
