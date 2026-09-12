// SPDX-License-Identifier: BUSL-1.1
//! Append-only Raft write wire ABI; variants must never be reordered.

use super::aliases::{
    default_columnar_ingest_format, default_columnar_insert_intent, default_ivf_cells,
    default_ivf_nprobe, default_pq_m,
};
use super::wire_shapes::{
    ColumnarResolvedRow, ConstraintChangeOp, DocumentResolvedMutationWire, KvResolvedMutationWire,
    ReplicatedBatchEdge, ReplicatedSumTarget,
};
use nodedb_physical::physical_plan::{ColumnarInsertIntent, UpdateValue};
use nodedb_types::{PayloadIndexKind, VectorQuantization, VectorStorageDtype};

#[derive(
    Debug,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum ReplicatedWrite {
    PointPut {
        collection: String,
        document_id: String,
        value: Vec<u8>,
        surrogate: u32,
        /// Join-key value → target surrogate, resolved by the proposing node.
        /// No applying node can re-derive this from the source vShard alone.
        /// SUPERSEDED by `resolved_sum_target_bindings`; kept for wire ABI compat.
        #[serde(default)]
        resolved_sum_targets: Vec<(String, u32)>,
        /// Same resolution, keyed on `(target collection, join value)` — the
        /// authoritative slot. Needed when a source drives two bindings on the
        /// same join column into different targets; the untargeted slot above
        /// can't tell those apart.
        #[serde(default)]
        resolved_sum_target_bindings: Vec<ReplicatedSumTarget>,
        /// RETURNING projection spec (`ReturningSpec`), msgpack-encoded.
        /// `None` if pre-dating this field or no RETURNING clause.
        #[serde(default)]
        returning: Option<Vec<u8>>,
        /// Read filters gating what `returning` may show back.
        #[serde(default)]
        rls_filters: Vec<u8>,
    },
    PointInsert {
        collection: String,
        document_id: String,
        value: Vec<u8>,
        #[serde(default)]
        if_absent: bool,
        surrogate: u32,
        /// See `PointPut::resolved_sum_targets`.
        #[serde(default)]
        resolved_sum_targets: Vec<(String, u32)>,
        /// Target collections whose delta the proposing node split onto its own
        /// `ApplyBalanceDelta` entry because the target lives on a different
        /// vShard. Skipping this list double-counts the delta on apply.
        #[serde(default)]
        deferred_sum_targets: Vec<String>,
        /// See `PointPut::resolved_sum_target_bindings`.
        #[serde(default)]
        resolved_sum_target_bindings: Vec<ReplicatedSumTarget>,
        /// See `PointPut::returning`.
        #[serde(default)]
        returning: Option<Vec<u8>>,
        /// See `PointPut::rls_filters`.
        #[serde(default)]
        rls_filters: Vec<u8>,
    },
    PointDelete {
        collection: String,
        document_id: String,
        surrogate: u32,
        /// See `PointPut::resolved_sum_targets`.
        #[serde(default)]
        resolved_sum_targets: Vec<(String, u32)>,
        /// See `PointPut::resolved_sum_target_bindings`.
        #[serde(default)]
        resolved_sum_target_bindings: Vec<ReplicatedSumTarget>,
        /// See `PointPut::returning`.
        #[serde(default)]
        returning: Option<Vec<u8>>,
        /// See `PointPut::rls_filters`.
        #[serde(default)]
        rls_filters: Vec<u8>,
    },
    PointUpdate {
        collection: String,
        document_id: String,
        updates: Vec<(String, nodedb_physical::physical_plan::UpdateValue)>,
        surrogate: u32,
        /// See `PointPut::resolved_sum_targets`.
        #[serde(default)]
        resolved_sum_targets: Vec<(String, u32)>,
        /// See `PointPut::resolved_sum_target_bindings`.
        #[serde(default)]
        resolved_sum_target_bindings: Vec<ReplicatedSumTarget>,
        /// See `PointPut::returning`.
        #[serde(default)]
        returning: Option<Vec<u8>>,
        /// See `PointPut::rls_filters`.
        #[serde(default)]
        rls_filters: Vec<u8>,
        /// The collection's declared `PRIMARY KEY` column, so every applier
        /// enforces NOT NULL on the computed post-image, not only the proposer.
        #[serde(default)]
        declared_primary_key: Option<String>,
    },
    DocUpsert {
        collection: String,
        document_id: String,
        value: Vec<u8>,
        on_conflict_updates: Vec<(String, nodedb_physical::physical_plan::UpdateValue)>,
        surrogate: u32,
        /// See `PointPut::resolved_sum_targets`.
        #[serde(default)]
        resolved_sum_targets: Vec<(String, u32)>,
        /// See `PointPut::resolved_sum_target_bindings`.
        #[serde(default)]
        resolved_sum_target_bindings: Vec<ReplicatedSumTarget>,
        /// See `PointPut::returning`.
        #[serde(default)]
        returning: Option<Vec<u8>>,
        /// See `PointPut::rls_filters`.
        #[serde(default)]
        rls_filters: Vec<u8>,
    },
    DocBatchInsert {
        collection: String,
        documents: Vec<(String, Vec<u8>)>,
        surrogates: Vec<u32>,
        /// See `PointPut::resolved_sum_targets`.
        #[serde(default)]
        resolved_sum_targets: Vec<(String, u32)>,
        /// See `PointInsert::deferred_sum_targets`.
        #[serde(default)]
        deferred_sum_targets: Vec<String>,
        /// See `PointPut::resolved_sum_target_bindings`.
        #[serde(default)]
        resolved_sum_target_bindings: Vec<ReplicatedSumTarget>,
        /// See `PointPut::returning`.
        #[serde(default)]
        returning: Option<Vec<u8>>,
        /// See `PointPut::rls_filters`.
        #[serde(default)]
        rls_filters: Vec<u8>,
    },
    VectorInsert {
        collection: String,
        vector: Vec<f32>,
        dim: usize,
        #[serde(default)]
        field_name: String,
        surrogate: u32,
        #[serde(default)]
        pk_bytes: Option<Vec<u8>>,
        #[serde(default)]
        provenance: Option<Vec<u8>>,
    },
    VectorBatchInsert {
        collection: String,
        vectors: Vec<Vec<f32>>,
        dim: usize,
        surrogates: Vec<u32>,
    },
    VectorDelete {
        collection: String,
        vector_id: u32,
    },
    SetVectorParams {
        collection: String,
        #[serde(default)]
        field_name: String,
        #[serde(default)]
        dim: usize,
        m: usize,
        ef_construction: usize,
        metric: String,
        #[serde(default)]
        index_type: String,
        #[serde(default = "default_pq_m")]
        pq_m: usize,
        #[serde(default = "default_ivf_cells")]
        ivf_cells: usize,
        #[serde(default = "default_ivf_nprobe")]
        ivf_nprobe: usize,
    },
    SparseInsert {
        collection: String,
        field_name: String,
        doc_id: String,
        entries: Vec<(u32, f32)>,
    },
    SparseDelete {
        collection: String,
        field_name: String,
        doc_id: String,
    },
    MultiVectorInsert {
        collection: String,
        field_name: String,
        document_surrogate: u32,
        vectors: Vec<f32>,
        count: usize,
        dim: usize,
    },
    MultiVectorDelete {
        collection: String,
        field_name: String,
        document_surrogate: u32,
    },
    DeleteBySurrogate {
        collection: String,
        surrogate: u32,
        field_name: String,
        /// Sync provenance encoded as zerompk bytes.
        #[serde(default)]
        provenance: Option<Vec<u8>>,
    },
    DirectUpsert {
        collection: String,
        field: String,
        surrogate: u32,
        vector: Vec<f32>,
        payload: Vec<u8>,
        quantization: VectorQuantization,
        storage_dtype: VectorStorageDtype,
        payload_indexes: Vec<(String, PayloadIndexKind)>,
        /// See `ReplicatedWrite::PointPut::returning`.
        #[serde(default)]
        returning: Option<Vec<u8>>,
        /// See `ReplicatedWrite::PointPut::rls_filters`.
        #[serde(default)]
        rls_filters: Vec<u8>,
    },
    CrdtApply {
        collection: String,
        document_id: String,
        delta: Vec<u8>,
        peer_id: u64,
        #[serde(default)]
        provenance: Option<Vec<u8>>,
        /// Leader-admitted constraint fence; zero means no fence.
        #[serde(default)]
        constraint_version_required: u64,
        /// Leader-assigned surrogate for `document_id`, carried so every
        /// replica binds the SAME identity instead of each allocating its
        /// own on apply. `0` (the wire default) means a record written
        /// before this field existed — see `decode/crdt.rs::apply` for the
        /// legacy fallback.
        #[serde(default)]
        surrogate: u32,
    },
    ColumnarIngest {
        collection: String,
        payload: Vec<u8>,
        schema_bytes: Vec<u8>,
        /// Leader-assigned global surrogates, parallel to the rows in `payload`.
        surrogates: Vec<u32>,
        /// Sync provenance encoded as zerompk bytes.
        #[serde(default)]
        provenance: Option<Vec<u8>>,
        /// "json", "msgpack", or "ilp" — see `ColumnarOp::Insert::format`.
        /// Defaults to msgpack for a pre-field record.
        #[serde(default = "default_columnar_ingest_format")]
        format: String,
        /// INSERT / INSERT IF ABSENT / UPSERT — see `ColumnarInsertIntent`.
        #[serde(default = "default_columnar_insert_intent")]
        intent: ColumnarInsertIntent,
        /// `ON CONFLICT (pk) DO UPDATE SET field = expr` assignments.
        #[serde(default)]
        on_conflict_updates: Vec<(String, UpdateValue)>,
        /// RETURNING projection spec (`ReturningSpec`), msgpack-encoded.
        #[serde(default)]
        returning: Option<Vec<u8>>,
        /// Read filters (`Vec<ScanFilter>`) gating what `returning` may show.
        #[serde(default)]
        rls_filters: Vec<u8>,
    },
    TimeseriesIngest {
        collection: String,
        payload: Vec<u8>,
        format: String,
        /// Leader-assigned global surrogates, parallel to the rows in `payload`.
        surrogates: Vec<u32>,
        /// Sync provenance encoded as zerompk bytes.
        #[serde(default)]
        provenance: Option<Vec<u8>>,
        /// RETURNING projection spec (`ReturningSpec`), msgpack-encoded.
        #[serde(default)]
        returning: Option<Vec<u8>>,
        /// Read filters (`Vec<ScanFilter>`) gating what `returning` may show.
        #[serde(default)]
        rls_filters: Vec<u8>,
    },
    FtsIndex {
        collection: String,
        /// Leader-assigned global surrogate for the document.
        surrogate: u32,
        text: String,
        /// Sync provenance encoded as zerompk bytes.
        #[serde(default)]
        provenance: Option<Vec<u8>>,
    },
    FtsDelete {
        collection: String,
        /// Leader-assigned global surrogate for the document.
        surrogate: u32,
        /// Sync provenance encoded as zerompk bytes.
        #[serde(default)]
        provenance: Option<Vec<u8>>,
    },
    SpatialInsert {
        collection: String,
        field: String,
        /// Leader-assigned global surrogate for the row.
        surrogate: u32,
        geometry_bytes: Vec<u8>,
        /// Sync provenance encoded as zerompk bytes.
        #[serde(default)]
        provenance: Option<Vec<u8>>,
    },
    SpatialDelete {
        collection: String,
        field: String,
        /// Leader-assigned global surrogate for the row.
        surrogate: u32,
        /// Sync provenance encoded as zerompk bytes.
        #[serde(default)]
        provenance: Option<Vec<u8>>,
    },
    EdgePut {
        collection: String,
        src_id: String,
        label: String,
        dst_id: String,
        properties: Vec<u8>,
        /// Leader-assigned source surrogate.
        src_surrogate: u32,
        /// Leader-assigned global surrogate for the destination node (binding
        /// key = `dst_id.as_bytes()`).
        dst_surrogate: u32,
    },
    EdgeDelete {
        collection: String,
        src_id: String,
        label: String,
        dst_id: String,
        /// Leader-assigned source surrogate.
        src_surrogate: u32,
        /// Leader-assigned global surrogate for the destination node (binding
        /// key = `dst_id.as_bytes()`).
        dst_surrogate: u32,
    },
    SetNodeLabels {
        node_id: String,
        labels: Vec<String>,
    },
    RemoveNodeLabels {
        node_id: String,
        labels: Vec<String>,
    },
    EdgePutBatch {
        edges: Vec<ReplicatedBatchEdge>,
    },
    EdgeDeleteBatch {
        edges: Vec<ReplicatedBatchEdge>,
    },
    KvPut {
        collection: String,
        key: Vec<u8>,
        value: Vec<u8>,
        ttl_ms: u64,
        /// Leader-assigned global surrogate (binding key = `key` raw bytes).
        surrogate: u32,
        /// Leader-resolved expiry clock; `None` when `ttl_ms == 0`.
        resolved_now_ms: Option<u64>,
        /// See `ReplicatedWrite::PointPut::returning`.
        #[serde(default)]
        returning: Option<Vec<u8>>,
        /// See `ReplicatedWrite::PointPut::rls_filters`.
        #[serde(default)]
        rls_filters: Vec<u8>,
    },
    KvDelete {
        collection: String,
        keys: Vec<Vec<u8>>,
    },
    KvInsert {
        collection: String,
        key: Vec<u8>,
        value: Vec<u8>,
        ttl_ms: u64,
        /// Leader-assigned global surrogate (binding key = `key` raw bytes).
        surrogate: u32,
        /// See `KvPut::resolved_now_ms`.
        resolved_now_ms: Option<u64>,
        /// See `ReplicatedWrite::PointPut::returning`.
        #[serde(default)]
        returning: Option<Vec<u8>>,
        /// See `ReplicatedWrite::PointPut::rls_filters`.
        #[serde(default)]
        rls_filters: Vec<u8>,
    },
    KvInsertIfAbsent {
        collection: String,
        key: Vec<u8>,
        value: Vec<u8>,
        ttl_ms: u64,
        surrogate: u32,
        /// See `KvPut::resolved_now_ms`.
        resolved_now_ms: Option<u64>,
        /// See `ReplicatedWrite::PointPut::returning`.
        #[serde(default)]
        returning: Option<Vec<u8>>,
        /// See `ReplicatedWrite::PointPut::rls_filters`.
        #[serde(default)]
        rls_filters: Vec<u8>,
    },
    KvInsertOnConflictUpdate {
        collection: String,
        key: Vec<u8>,
        value: Vec<u8>,
        ttl_ms: u64,
        updates: Vec<(String, nodedb_physical::physical_plan::UpdateValue)>,
        surrogate: u32,
        /// See `KvPut::resolved_now_ms`.
        resolved_now_ms: Option<u64>,
        /// See `ReplicatedWrite::PointPut::returning`.
        #[serde(default)]
        returning: Option<Vec<u8>>,
        /// See `ReplicatedWrite::PointPut::rls_filters`.
        #[serde(default)]
        rls_filters: Vec<u8>,
    },
    KvBatchPut {
        collection: String,
        entries: Vec<(Vec<u8>, Vec<u8>)>,
        ttl_ms: u64,
        surrogates: Vec<u32>,
        /// See `KvPut::resolved_now_ms`.
        resolved_now_ms: Option<u64>,
        /// See `ReplicatedWrite::PointPut::returning`.
        #[serde(default)]
        returning: Option<Vec<u8>>,
        /// See `ReplicatedWrite::PointPut::rls_filters`.
        #[serde(default)]
        rls_filters: Vec<u8>,
    },
    KvExpire {
        collection: String,
        key: Vec<u8>,
        ttl_ms: u64,
        /// Leader-resolved expiry clock; always `Some` for EXPIRE.
        resolved_now_ms: Option<u64>,
    },
    KvPersist {
        collection: String,
        key: Vec<u8>,
    },
    KvIncr {
        collection: String,
        key: Vec<u8>,
        delta: i64,
        ttl_ms: u64,
        surrogate: u32,
        /// See `KvPut::resolved_now_ms`.
        resolved_now_ms: Option<u64>,
    },
    KvIncrFloat {
        collection: String,
        key: Vec<u8>,
        delta: f64,
        surrogate: u32,
    },
    KvCas {
        collection: String,
        key: Vec<u8>,
        expected: Vec<u8>,
        new_value: Vec<u8>,
        surrogate: u32,
    },
    KvGetSet {
        collection: String,
        key: Vec<u8>,
        new_value: Vec<u8>,
        surrogate: u32,
        /// Read filters gating the OLD value this op hands back — see
        /// `KvOp::GetSet::rls_filters`. `GetSet` has no RETURNING projection,
        /// only this read gate.
        #[serde(default)]
        rls_filters: Vec<u8>,
    },
    KvRegisterSortedIndex {
        collection: String,
        index_name: String,
        sort_columns: Vec<(String, String)>,
        key_column: String,
        window_type: String,
        window_timestamp_column: String,
        window_start_ms: u64,
        window_end_ms: u64,
    },
    KvDropSortedIndex {
        index_name: String,
    },
    KvRegisterIndex {
        collection: String,
        field: String,
        field_position: usize,
        backfill: bool,
    },
    KvDropIndex {
        collection: String,
        field: String,
    },
    KvFieldSet {
        collection: String,
        key: Vec<u8>,
        updates: Vec<(String, Vec<u8>)>,
        surrogate: u32,
    },
    KvTransfer {
        collection: String,
        source_key: Vec<u8>,
        dest_key: Vec<u8>,
        field: String,
        amount: f64,
        debit_surrogate: u32,
        credit_surrogate: u32,
    },
    KvTransferItem {
        source_collection: String,
        dest_collection: String,
        item_key: Vec<u8>,
        dest_key: Vec<u8>,
        surrogate: u32,
    },
    ArrayOp {
        array: String,
        op_bytes: Vec<u8>,
        schema_hlc_bytes: [u8; 18],
        #[serde(default)]
        provenance: Option<Vec<u8>>,
    },
    ArraySchema {
        array: String,
        snapshot_payload: Vec<u8>,
        schema_hlc_bytes: [u8; 18],
    },
    BulkDml {
        collection: String,
        filters: Vec<u8>,
        is_update: bool,
        updates: Vec<(String, nodedb_physical::physical_plan::UpdateValue)>,
        /// See `PointPut::resolved_sum_targets`. A replica re-derives which rows
        /// match, but not which target row each credits — that came from the
        /// proposing node's scan of the target collection's catalog.
        #[serde(default)]
        resolved_sum_targets: Vec<(String, u32)>,
        /// See `PointPut::resolved_sum_target_bindings`.
        #[serde(default)]
        resolved_sum_target_bindings: Vec<ReplicatedSumTarget>,
        /// See `PointPut::returning`.
        #[serde(default)]
        returning: Option<Vec<u8>>,
        /// See `PointPut::rls_filters`.
        #[serde(default)]
        rls_filters: Vec<u8>,
        /// See `PointUpdate::declared_primary_key`. `None` on the delete arm
        /// (`is_update = false`), which has no post-image to check.
        #[serde(default)]
        declared_primary_key: Option<String>,
    },
    ColumnarBulkDml {
        collection: String,
        filters: Vec<u8>,
        is_update: bool,
        updates: Vec<(String, Vec<u8>)>,
    },
    InsertSelect {
        target_collection: String,
        source_collection: String,
        source_filters: Vec<u8>,
        source_limit: usize,
        /// See `DocumentOp::InsertSelect::column_map`. Each replica shapes the
        /// rows it copies with the same bindings the leader used.
        column_map: Vec<u8>,
    },
    CrdtImportCollection {
        tenant_id: u64,
        collection: String,
        bytes: Vec<u8>,
    },
    CrdtListInsert {
        collection: String,
        document_id: String,
        list_path: String,
        index: u64,
        fields_json: String,
        /// Surrogate of the parent document hosting this block list. See
        /// `ReplicatedWrite::CrdtApply::surrogate`.
        #[serde(default)]
        surrogate: u32,
    },
    CrdtListDelete {
        collection: String,
        document_id: String,
        list_path: String,
        index: u64,
        /// See `ReplicatedWrite::CrdtListInsert::surrogate`.
        #[serde(default)]
        surrogate: u32,
    },
    CrdtListMove {
        collection: String,
        document_id: String,
        list_path: String,
        from_index: u64,
        to_index: u64,
        /// See `ReplicatedWrite::CrdtListInsert::surrogate`.
        #[serde(default)]
        surrogate: u32,
    },
    CrdtDocUpsert {
        collection: String,
        document_id: String,
        surrogate: u32,
        fields_json: String,
        partial: bool,
        /// See `ReplicatedWrite::PointPut::returning`.
        #[serde(default)]
        returning: Option<Vec<u8>>,
        /// See `ReplicatedWrite::PointPut::rls_filters`.
        #[serde(default)]
        rls_filters: Vec<u8>,
    },
    CrdtDocDelete {
        collection: String,
        document_id: String,
        surrogate: u32,
        /// See `ReplicatedWrite::PointPut::returning`.
        #[serde(default)]
        returning: Option<Vec<u8>>,
        /// See `ReplicatedWrite::PointPut::rls_filters`.
        #[serde(default)]
        rls_filters: Vec<u8>,
    },
    CalvinReadResult {
        epoch: u64,
        position: u32,
        passive_vshard: u32,
        tenant_id: u64,
        values: Vec<u8>,
    },
    DocTruncate {
        collection: String,
        restart_identity: bool,
        /// See `BulkDml::resolved_sum_targets` — every row in the truncated
        /// collection takes its contribution off a resolved target.
        #[serde(default)]
        resolved_sum_targets: Vec<(String, u32)>,
        /// See `PointPut::resolved_sum_target_bindings`.
        #[serde(default)]
        resolved_sum_target_bindings: Vec<ReplicatedSumTarget>,
        /// See `PointUpdate::declared_primary_key`. Names the column each
        /// removed row's identity is read from on every applier.
        #[serde(default)]
        declared_primary_key: Option<String>,
    },
    KvTruncate {
        collection: String,
    },
    ConstraintChange {
        collection: String,
        op: ConstraintChangeOp,
        constraint_version: u64,
        constraints: Vec<Vec<u8>>,
    },
    ArrayCellPut {
        array: String,
        cells_msgpack: Vec<u8>,
        #[serde(default)]
        provenance: Option<Vec<u8>>,
    },
    ArrayCellDelete {
        array: String,
        coords_msgpack: Vec<u8>,
        #[serde(default)]
        provenance: Option<Vec<u8>>,
    },

    /// Fenced CRDT apply.
    CrdtApplyFenced {
        collection: String,
        document_id: String,
        delta: Vec<u8>,
        peer_id: u64,
        /// Sync provenance; `None` for non-sync applies.
        provenance: Option<Vec<u8>>,
        /// Leader-admitted collection constraint fence.
        constraint_version_required: u64,
        /// Mandatory exact preview frontier; absent on legacy `CrdtApply`.
        expected_frontier_digest: [u8; 32],
        /// See `ReplicatedWrite::CrdtApply::surrogate`.
        #[serde(default)]
        surrogate: u32,
    },
    CrdtApplyAuthenticated {
        collection: String,
        document_id: String,
        delta: Vec<u8>,
        peer_id: u64,
        provenance: Option<Vec<u8>>,
        constraint_version_required: u64,
        expected_frontier_digest: Option<[u8; 32]>,
        auth_user_id: u64,
        auth_device_id: u64,
        auth_seq_no: u64,
        delta_signature: [u8; 32],
        signing_required: bool,
        /// See `ReplicatedWrite::CrdtApply::surrogate`.
        #[serde(default)]
        surrogate: u32,
    },

    /// Tear down one vector index on every replica — counterpart of
    /// `SetVectorParams`.
    DropVectorIndex {
        collection: String,
        #[serde(default)]
        field_name: String,
    },

    /// Moves a materialized-sum balance on a TARGET row on a different vShard
    /// than the source write. A DELTA, not an absolute balance — every replica
    /// applies it once under exactly-once, LSN-ordered Raft apply, so it stays
    /// correct even if a follower reaches it from a different prior balance.
    ApplyBalanceDelta {
        /// TARGET collection, db-qualified.
        collection: String,
        /// Target row's storage key — hex-encoded surrogate.
        document_id: String,
        /// Target row's global identity.
        surrogate: u32,
        /// The balance column this delta moves.
        column: String,
        /// Signed amount as an exact decimal string (not `f64`, to keep the
        /// precision a balance total needs).
        delta: String,
        /// Binding's join column, for the typed not-found error on apply.
        join_column: String,
        /// Join value that resolved to `surrogate`.
        join_value: String,
        /// See `PointUpdate::declared_primary_key`, for the TARGET collection.
        #[serde(default)]
        declared_primary_key: Option<String>,
    },

    /// Resolved-row-set form of a columnar predicate `UPDATE` / `DELETE` on a
    /// write-policy collection. The Control Plane already resolved the rows
    /// and decided the policy against them, so every replica applies exactly
    /// these rows and evaluates nothing.
    ColumnarBulkDmlResolved {
        collection: String,
        is_update: bool,
        rows: Vec<ColumnarResolvedRow>,
    },

    /// Resolved form of a state-dependent KV write on a write-policy
    /// collection: mutations and reply, already decided, not an operation to
    /// re-derive. `mutations` may span two collections for `TransferItem`.
    KvResolvedWrite {
        mutations: Vec<KvResolvedMutationWire>,
        /// Statement reply decided at resolve time; every replica returns it
        /// unchanged rather than recomputing from moved-on state.
        response_payload: Vec<u8>,
    },

    /// KV predicate `UPDATE` on a collection with NO write policy — the
    /// predicate travels and every replica re-scans it against its own state.
    /// A write-policy collection never reaches this shape.
    KvPredicateUpdate {
        collection: String,
        /// Serialized `Vec<ScanFilter>`. Empty matches every row.
        filters: Vec<u8>,
        updates: Vec<(String, Vec<u8>)>,
    },

    /// KV predicate `DELETE` on a collection with NO write policy — see
    /// [`ReplicatedWrite::KvPredicateUpdate`].
    KvPredicateDelete {
        collection: String,
        /// Serialized `Vec<ScanFilter>`. Empty matches every row.
        filters: Vec<u8>,
    },

    /// Resolved form of a deferred document write (`PointUpdate`,
    /// `PointDelete`, `Upsert`, `BulkUpdate`, `BulkDelete`) on a write-policy
    /// collection: mutations and reply, already decided against the live
    /// writing identity, not a predicate a follower could re-judge.
    DocumentResolvedWrite {
        mutations: Vec<DocumentResolvedMutationWire>,
        /// Statement reply decided at resolve time; every replica returns it
        /// unchanged.
        response_payload: Vec<u8>,
    },
}

#[cfg(test)]
mod tests {
    use super::super::replicated_entry::ReplicatedEntry;
    use super::*;

    #[test]
    fn constraint_change_roundtrip() {
        let entry = ReplicatedEntry::new(
            7,
            0,
            3,
            ReplicatedWrite::ConstraintChange {
                collection: "orders".into(),
                op: ConstraintChangeOp::Set,
                constraint_version: 9,
                constraints: vec![vec![1, 2, 3], vec![4, 5, 6]],
            },
        );
        let original_key = entry.idempotency_key;

        let bytes = entry.to_bytes();
        let decoded = ReplicatedEntry::from_bytes(&bytes).expect("decode failed");
        assert_eq!(decoded.tenant_id, 7);
        assert_eq!(decoded.vshard_id, 3);
        assert_eq!(decoded.idempotency_key, original_key);
        match decoded.write {
            ReplicatedWrite::ConstraintChange {
                collection,
                op,
                constraint_version,
                constraints,
            } => {
                assert_eq!(collection, "orders");
                assert_eq!(op, ConstraintChangeOp::Set);
                assert_eq!(constraint_version, 9);
                assert_eq!(constraints, vec![vec![1u8, 2, 3], vec![4u8, 5, 6]]);
            }
            other => panic!("expected ConstraintChange, got {other:?}"),
        }
    }

    #[test]
    fn constraint_change_encoding_is_deterministic() {
        let write = ReplicatedWrite::ConstraintChange {
            collection: "orders".into(),
            op: ConstraintChangeOp::Drop,
            constraint_version: 4,
            constraints: vec![vec![1, 2, 3], vec![4, 5, 6]],
        };
        let a = zerompk::to_msgpack_vec(&write).expect("encode a failed");
        let b = zerompk::to_msgpack_vec(&write).expect("encode b failed");
        assert_eq!(
            a, b,
            "encoding the same ConstraintChange must be byte-identical"
        );
    }

    #[test]
    fn all_write_variants_serialize() {
        let writes = vec![
            ReplicatedWrite::PointPut {
                collection: "c".into(),
                document_id: "d".into(),
                value: vec![1, 2, 3],
                surrogate: 1,
                resolved_sum_targets: vec![("acc-1".into(), 4242)],
                resolved_sum_target_bindings: vec![ReplicatedSumTarget {
                    target_collection: "accounts".into(),
                    join_value: "acc-1".into(),
                    surrogate: 4242,
                }],
                returning: None,
                rls_filters: Vec::new(),
            },
            ReplicatedWrite::PointDelete {
                collection: "c".into(),
                document_id: "d".into(),
                surrogate: 1,
                resolved_sum_targets: Vec::new(),
                resolved_sum_target_bindings: Vec::new(),
                returning: None,
                rls_filters: Vec::new(),
            },
            ReplicatedWrite::VectorInsert {
                collection: "v".into(),
                vector: vec![1.0, 2.0, 3.0],
                dim: 3,
                field_name: "embedding".into(),
                surrogate: 7,
                pk_bytes: Some(b"doc-1".to_vec()),
                provenance: None,
            },
            ReplicatedWrite::CrdtApply {
                collection: "c".into(),
                document_id: "d".into(),
                delta: vec![0xAB],
                peer_id: 7,
                provenance: None,
                constraint_version_required: 0,
                surrogate: 5,
            },
            ReplicatedWrite::CrdtApplyFenced {
                collection: "c".into(),
                document_id: "d".into(),
                delta: vec![0xAC],
                peer_id: 8,
                provenance: None,
                constraint_version_required: 1,
                expected_frontier_digest: [1; 32],
                surrogate: 6,
            },
            ReplicatedWrite::EdgePut {
                collection: "col".into(),
                src_id: "a".into(),
                label: "knows".into(),
                dst_id: "b".into(),
                properties: vec![],
                src_surrogate: 10,
                dst_surrogate: 20,
            },
            ReplicatedWrite::EdgeDelete {
                collection: "col".into(),
                src_id: "a".into(),
                label: "knows".into(),
                dst_id: "b".into(),
                src_surrogate: 10,
                dst_surrogate: 20,
            },
            ReplicatedWrite::ArrayOp {
                array: "genome".into(),
                op_bytes: vec![0xde, 0xad],
                schema_hlc_bytes: [0u8; 18],
                provenance: None,
            },
            ReplicatedWrite::ArraySchema {
                array: "genome".into(),
                snapshot_payload: vec![0xbe, 0xef],
                schema_hlc_bytes: [1u8; 18],
            },
            ReplicatedWrite::ConstraintChange {
                collection: "orders".into(),
                op: ConstraintChangeOp::Set,
                constraint_version: 1,
                constraints: vec![vec![1, 2, 3]],
            },
        ];

        for write in writes {
            let entry = ReplicatedEntry::new(1, 0, 0, write);
            let bytes = entry.to_bytes();
            let decoded = ReplicatedEntry::from_bytes(&bytes);
            assert!(decoded.is_some(), "failed to roundtrip: {entry:?}");
        }
    }
}
