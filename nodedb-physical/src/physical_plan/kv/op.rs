// SPDX-License-Identifier: Apache-2.0

//! The KV operation enum — the wire shape and nothing else.

use nodedb_types::{QualifiedCollection, RlsWriteCheck, Surrogate};

use super::resolved_mutation::KvResolvedMutation;
use crate::physical_plan::document::ReturningSpec;

/// KV engine physical operations.
///
/// All operations target a hash-indexed collection with O(1) point lookups.
/// Keys and values are serialized as Binary Tuples.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum KvOp {
    /// Point lookup by primary key. Returns Binary Tuple value or nil.
    Get {
        collection: QualifiedCollection,
        key: Vec<u8>,
        /// RLS post-fetch filters. Evaluated after fetching the value.
        /// Returns nil on denial (no info leak).
        rls_filters: Vec<u8>,
        /// Clone snapshot-isolation ceiling: an entry with surrogate above
        /// this is treated as not-found, so post-AS-OF source bindings don't
        /// leak through a delegated clone `Get`. `None` for normal gets.
        #[serde(default)]
        surrogate_ceiling: Option<u32>,
    },

    /// Insert or update (RESP SET / SQL UPSERT semantics). Writes a Binary
    /// Tuple value keyed by primary key, overwriting any existing row.
    ///
    /// If the collection has secondary indexes, they are maintained synchronously.
    /// If no secondary indexes, takes the zero-index fast path.
    Put {
        collection: QualifiedCollection,
        key: Vec<u8>,
        /// Binary Tuple encoded value (all value columns).
        value: Vec<u8>,
        /// Per-key TTL override in milliseconds. 0 = use collection default.
        ttl_ms: u64,
        /// Stable cross-engine identity assigned by the CP-side
        /// `SurrogateAssigner` from `(collection, key)`.
        /// `Surrogate::ZERO` only appears in test fixtures.
        surrogate: Surrogate,
        /// When `Some`, return the STORED post-image (row as `SELECT` would
        /// show it, `key` included). Never the caller's submitted body.
        #[serde(default)]
        returning: Option<ReturningSpec>,
        /// Read filters bounding `returning` to what a `SELECT` by the same
        /// principal would show.
        #[serde(default)]
        rls_filters: Vec<u8>,
    },

    /// SQL `INSERT` semantics: write only if the key does not already exist.
    /// Returns `unique_violation` (SQLSTATE 23505) on duplicate key. RESP
    /// `SET` and `UPSERT` continue to use `Put`.
    Insert {
        collection: QualifiedCollection,
        key: Vec<u8>,
        value: Vec<u8>,
        ttl_ms: u64,
        /// Stable cross-engine identity. `Surrogate::ZERO` only in tests.
        surrogate: Surrogate,
        /// See `Put::returning`.
        #[serde(default)]
        returning: Option<ReturningSpec>,
        /// See `Put::rls_filters`.
        #[serde(default)]
        rls_filters: Vec<u8>,
    },

    /// SQL `INSERT ... ON CONFLICT DO NOTHING` semantics: write if the key
    /// does not exist, silently no-op on duplicate. No error on conflict.
    InsertIfAbsent {
        collection: QualifiedCollection,
        key: Vec<u8>,
        value: Vec<u8>,
        ttl_ms: u64,
        /// Stable cross-engine identity. `Surrogate::ZERO` only in tests.
        surrogate: Surrogate,
        /// See `Put::returning`.
        #[serde(default)]
        returning: Option<ReturningSpec>,
        /// See `Put::rls_filters`.
        #[serde(default)]
        rls_filters: Vec<u8>,
    },

    /// SQL `INSERT ... ON CONFLICT (key) DO UPDATE SET ...` semantics:
    /// write if absent; on duplicate, read-modify-write — apply the
    /// `updates` (which may reference `EXCLUDED.col` on the incoming row)
    /// to the existing value and write the merged result. `value` is the
    /// would-be-inserted row, used both as the write target when absent
    /// and as `EXCLUDED` when the handler evaluates expressions.
    InsertOnConflictUpdate {
        collection: QualifiedCollection,
        key: Vec<u8>,
        value: Vec<u8>,
        ttl_ms: u64,
        updates: Vec<(String, crate::physical_plan::document::UpdateValue)>,
        /// Stable cross-engine identity. `Surrogate::ZERO` only in tests.
        surrogate: Surrogate,
        /// Write policy against the body actually persisted — insert branch
        /// or the conflict-merge, neither of which exists at plan time.
        /// Distinct from `rls_filters`: never conflate write gate and redaction.
        rls_write_check: RlsWriteCheck,
        /// When `Some`, return the STORED post-image: merged row on conflict,
        /// inserted row otherwise. Never the submitted body.
        #[serde(default)]
        returning: Option<ReturningSpec>,
        /// See `Put::rls_filters`.
        #[serde(default)]
        rls_filters: Vec<u8>,
    },

    /// Delete by primary key(s). Returns count of keys actually deleted.
    Delete {
        collection: QualifiedCollection,
        keys: Vec<Vec<u8>>,
        /// Compiled row-level-security WRITE predicate, evaluated in the Data
        /// Plane against the stored row being removed, or the reason no
        /// predicate is attached. Only `RlsWriteCheck::Predicate` makes the
        /// handler read the pre-image at all.
        rls_write_check: RlsWriteCheck,
    },

    /// Cursor-based scan with optional filter predicate.
    Scan {
        collection: QualifiedCollection,
        /// Opaque cursor from a previous scan. Empty = start from beginning.
        cursor: Vec<u8>,
        /// Maximum entries to return in this batch.
        count: usize,
        /// Optional filter predicates (same format as DocumentScan filters).
        filters: Vec<u8>,
        /// Optional glob pattern for key matching (e.g., "user:*").
        match_pattern: Option<String>,
        /// ORDER BY terms, each an expression, applied to the scan result
        /// before encoding. Empty = unsorted (engine native order).
        #[serde(default)]
        sort_keys: Vec<crate::physical_plan::SortKeySpec>,
        /// See `Get::surrogate_ceiling`; drops entries above the ceiling.
        #[serde(default)]
        surrogate_ceiling: Option<u32>,
        /// Output column names (same format as DocumentOp::Scan). Empty =
        /// return the whole row document.
        #[serde(default)]
        projection: Vec<String>,
        /// Serialized `Vec<ComputedColumn>` applied per row after the scan
        /// (same format as DocumentOp::Scan). Empty = none.
        #[serde(default)]
        computed_columns: Vec<u8>,
    },

    /// Set or update TTL on an existing key.
    Expire {
        collection: QualifiedCollection,
        key: Vec<u8>,
        /// TTL in milliseconds from now.
        ttl_ms: u64,
        /// Compiled row-level-security WRITE predicate, or the reason no
        /// predicate is attached. The body is unchanged by a TTL mutation, so
        /// the stored row is both the pre- and the post-image and the Data
        /// Plane decides it before touching the expiry metadata.
        rls_write_check: RlsWriteCheck,
    },

    /// Remove TTL from an existing key (make it persistent).
    Persist {
        collection: QualifiedCollection,
        key: Vec<u8>,
        /// Compiled row-level-security WRITE predicate — see `Expire`, which
        /// this mirrors: the row body does not change, so the stored row is
        /// the image the policy decides.
        rls_write_check: RlsWriteCheck,
    },

    /// Get remaining TTL for a key without fetching the value.
    ///
    /// Returns JSON `{"ttl_ms": N}` where N is:
    /// - `-2` — key does not exist
    /// - `-1` — key exists but has no TTL (persistent)
    /// - `>= 0` — remaining milliseconds until expiry
    GetTtl {
        collection: QualifiedCollection,
        key: Vec<u8>,
    },

    /// Batch get: fetch multiple keys in a single bridge round-trip.
    BatchGet {
        collection: QualifiedCollection,
        keys: Vec<Vec<u8>>,
        /// RLS filters applied post-fetch (no pushdown slot in storage) —
        /// same shape `KvOp::Get` and `DocumentOp::PointGet` use.
        #[serde(default)]
        rls_filters: Vec<u8>,
    },

    /// Batch put: insert/update multiple key-value pairs atomically.
    BatchPut {
        collection: QualifiedCollection,
        /// `(key, value)` pairs.
        entries: Vec<(Vec<u8>, Vec<u8>)>,
        /// Per-key TTL override in milliseconds. 0 = use collection default.
        ttl_ms: u64,
        /// Stable cross-engine identity for each entry, same order and
        /// length as `entries`, assigned by the CP-side `SurrogateAssigner`
        /// from `(collection, key)` -- the same mechanism `Put`/`Insert`
        /// use. `Surrogate::ZERO` only appears in test fixtures.
        #[serde(default)]
        surrogates: Vec<Surrogate>,
        /// When `Some`, return one row per written entry — the STORED
        /// post-image of each, in `entries` order — projected per spec.
        #[serde(default)]
        returning: Option<ReturningSpec>,
        /// Read filters gating the rows `returning` emits — see
        /// `Put::rls_filters`.
        #[serde(default)]
        rls_filters: Vec<u8>,
    },

    /// Register a secondary index on a value field (DDL).
    ///
    /// Dispatched when `CREATE INDEX idx ON kv_collection (field)` is executed.
    /// If `backfill` is true, scans all existing entries to populate the index.
    RegisterIndex {
        collection: QualifiedCollection,
        /// Field name to index (must match a column in the KV schema).
        field: String,
        /// Position of the field in the schema column list.
        field_position: usize,
        /// Whether to backfill the index with existing entries.
        backfill: bool,
    },

    /// Remove a secondary index from a value field (DDL).
    DropIndex {
        collection: QualifiedCollection,
        field: String,
    },

    /// Extract one or more fields from a key's value (HGET/HMGET).
    ///
    /// Deserializes the stored value, extracts the named fields, and returns
    /// them as a JSON object. O(1) key lookup + field extraction.
    FieldGet {
        collection: QualifiedCollection,
        key: Vec<u8>,
        /// Field names to extract.
        fields: Vec<String>,
        /// See `BatchGet::rls_filters`.
        #[serde(default)]
        rls_filters: Vec<u8>,
    },

    /// Update specific fields in a key's value (HSET): read-modify-write,
    /// merges field updates, maintains secondary indexes.
    FieldSet {
        collection: QualifiedCollection,
        key: Vec<u8>,
        /// Field name → new value (JSON-encoded bytes).
        updates: Vec<(String, Vec<u8>)>,
        /// Content-addressed identity on `(collection, key)`, threaded to the
        /// write-back so a field merge keeps the row's original surrogate.
        surrogate: Surrogate,
        /// Write policy evaluated against the merged body, which exists only
        /// after the stored row is read and updates applied.
        rls_write_check: RlsWriteCheck,
    },

    /// Truncate: delete ALL entries in a KV collection.
    Truncate { collection: QualifiedCollection },

    /// Atomic increment: init 0 if absent, `TypeMismatch` if not i64,
    /// `OverflowError` on wrap. `ttl_ms > 0` sets/resets TTL; `0` preserves it.
    Incr {
        collection: QualifiedCollection,
        key: Vec<u8>,
        delta: i64,
        /// TTL in milliseconds. 0 = preserve existing TTL.
        ttl_ms: u64,
        /// See `FieldSet::surrogate`.
        surrogate: Surrogate,
        /// Write policy evaluated against the computed post-increment image
        /// inside the engine, not guessed by the handler.
        rls_write_check: RlsWriteCheck,
    },

    /// Atomic float increment on a numeric value. Returns new value.
    ///
    /// Same semantics as `Incr` but for f64 values.
    /// If value is not f64, returns `TypeMismatch`.
    IncrFloat {
        collection: QualifiedCollection,
        key: Vec<u8>,
        delta: f64,
        /// Stable cross-engine identity. `Surrogate::ZERO` only in tests.
        surrogate: Surrogate,
        /// Compiled row-level-security WRITE predicate — see `Incr`, whose
        /// engine-internal compute-and-persist this mirrors.
        rls_write_check: RlsWriteCheck,
    },

    /// Compare-and-swap: set value to `new_value` only if current equals `expected`.
    ///
    /// Returns JSON `{"success": bool, "current_value": "<base64>"}`.
    /// If key doesn't exist and `expected` is empty, creates the key (create-if-not-exists).
    Cas {
        collection: QualifiedCollection,
        key: Vec<u8>,
        expected: Vec<u8>,
        new_value: Vec<u8>,
        /// Stable cross-engine identity. `Surrogate::ZERO` only in tests.
        surrogate: Surrogate,
        /// Compiled row-level-security WRITE predicate, evaluated against
        /// `new_value` before the swap is attempted, or the reason no
        /// predicate is attached.
        rls_write_check: RlsWriteCheck,
    },

    /// Atomic get-and-set: set new value, return old value.
    ///
    /// Returns the previous value (or null if key didn't exist).
    GetSet {
        collection: QualifiedCollection,
        key: Vec<u8>,
        new_value: Vec<u8>,
        /// Stable cross-engine identity. `Surrogate::ZERO` only in tests.
        surrogate: Surrogate,
        /// Row-level-security READ filters applied to the OLD value this op
        /// hands back. The reply is a row body, so a row the read policy hides
        /// must come back absent rather than being disclosed by the write that
        /// replaced it.
        #[serde(default)]
        rls_filters: Vec<u8>,
        /// Compiled row-level-security WRITE predicate, evaluated against
        /// `new_value` before the swap, or the reason no predicate is
        /// attached. Never an alias of `rls_filters`: one decides what may be
        /// shown, the other what may be written.
        rls_write_check: RlsWriteCheck,
    },

    // ── Atomic Transfer Operations ───────────────────────────────────
    /// Atomic fungible transfer: read-validate-write in one Data Plane pass.
    ///
    /// Reads source and dest values, validates source.field >= amount,
    /// then atomically writes both updated values. No TOCTOU race.
    Transfer {
        collection: QualifiedCollection,
        source_key: Vec<u8>,
        dest_key: Vec<u8>,
        field: String,
        /// Amount to transfer (encoded as f64 bytes).
        amount: f64,
        /// Debit (source) row's identity, content-addressed on `(collection,
        /// source_key)`, threaded to the write-back.
        debit_surrogate: Surrogate,
        /// Credit (dest) row's identity, content-addressed on `(collection,
        /// dest_key)` — distinct from `debit_surrogate` so the rows never
        /// collapse onto one identity.
        credit_surrogate: Surrogate,
        /// Write policy for the (single) collection both rows live in. Both
        /// post-images are decided against it before either persists, so a
        /// transfer cannot half-apply.
        rls_write_check: RlsWriteCheck,
    },

    /// Atomic non-fungible item transfer: verifies source owns the item,
    /// then atomically deletes from source and inserts at dest. `NotFound`
    /// if source doesn't own it.
    TransferItem {
        source_collection: QualifiedCollection,
        dest_collection: QualifiedCollection,
        item_key: Vec<u8>,
        dest_key: Vec<u8>,
        /// Moved row's identity at its destination, content-addressed on
        /// `(dest_collection, dest_key)`.
        surrogate: Surrogate,
        /// Write policy of the SOURCE collection, decided against the row
        /// being removed.
        source_rls_write_check: RlsWriteCheck,
        /// Write policy of the DESTINATION collection. Kept separate from
        /// the source check: an identity may give a row up but not receive it.
        dest_rls_write_check: RlsWriteCheck,
    },

    // ── Sorted Index (Leaderboard) Operations ──────────────────────────
    /// Register a sorted index on a KV collection (DDL).
    RegisterSortedIndex {
        collection: QualifiedCollection,
        index_name: String,
        /// Sort columns: (column_name, direction "ASC"/"DESC").
        sort_columns: Vec<(String, String)>,
        /// Primary key column name.
        key_column: String,
        /// Window type: "none", "daily", "weekly", "monthly", or "custom".
        window_type: String,
        /// Window timestamp column (empty if window_type == "none").
        window_timestamp_column: String,
        /// Custom window start (ms since epoch, 0 if N/A).
        window_start_ms: u64,
        /// Custom window end (ms since epoch, 0 if N/A).
        window_end_ms: u64,
    },

    /// Drop a sorted index.
    DropSortedIndex { index_name: String },

    /// Get the 1-based rank of a key in a sorted index.
    SortedIndexRank {
        index_name: String,
        primary_key: Vec<u8>,
    },

    /// Get the top K entries from a sorted index.
    SortedIndexTopK { index_name: String, k: u32 },

    /// Get entries in a score range from a sorted index.
    SortedIndexRange {
        index_name: String,
        score_min: Option<Vec<u8>>,
        score_max: Option<Vec<u8>>,
    },

    /// Get total count of entries in a sorted index.
    SortedIndexCount { index_name: String },

    /// Get the sort key (score) for a key in a sorted index (ZSCORE equivalent).
    SortedIndexScore {
        index_name: String,
        primary_key: Vec<u8>,
    },

    /// Cursor-paginated raw scan for the clone materializer.
    ///
    /// Unlike `Scan`, this returns raw `(key, value)` byte pairs **plus** the
    /// next-cursor in a single payload, so the materializer can drive the
    /// scan to completion in O(N / count) round-trips. The response payload
    /// is msgpack-encoded as a 2-element array:
    ///   `[ next_cursor: bytes, entries: [[key: bytes, value: bytes], ...] ]`
    /// `next_cursor` is empty when the scan is complete.
    MaterializeScan {
        collection: QualifiedCollection,
        cursor: Vec<u8>,
        count: usize,
    },

    // ── Resolve-before-propose (governed state-dependent writes) ────────
    /// Read-only: report every mutation the wrapped write would apply, and
    /// the response payload it would return, without applying anything.
    ///
    /// The wrapped op is the intercepted write verbatim, live write predicate
    /// included — the handler decides the policy against the exact images it
    /// computes here, where the writing identity is still available. A
    /// follower has none, so the predicate can never cross the Raft wire.
    ResolveWrite(Box<KvOp>),

    /// Apply exactly the mutations a [`KvOp::ResolveWrite`] reported, then
    /// return `response_payload` verbatim.
    ///
    /// No predicate is evaluated and no image is recomputed: the Control
    /// Plane already decided both, and `rls_write_check` carries the verdict
    /// (`RlsWriteCheck::DecidedEarlierInRequest`). Every mutation's
    /// `precondition` is checked against current state before the first one
    /// applies, so a resolution that drifted under a concurrent write applies
    /// nothing and asks for a retry.
    ResolvedWrite {
        mutations: Vec<KvResolvedMutation>,
        /// The statement's reply, decided while resolving. Applying nodes
        /// return it unchanged, so leader and follower report the same thing.
        response_payload: Vec<u8>,
        rls_write_check: RlsWriteCheck,
    },

    // ── Predicate DML (SQL UPDATE / DELETE with no resolvable primary key) ──
    /// Merge `updates` into every row matching `filters`.
    ///
    /// The predicate form of [`KvOp::FieldSet`]: the plan carries the
    /// predicate, not a key list, since `WHERE` selects rows only once the
    /// Data Plane scans current state. Uses the same merge as `FieldSet`.
    PredicateUpdate {
        collection: QualifiedCollection,
        /// Serialized `Vec<ScanFilter>` (MessagePack). Empty matches every row.
        filters: Vec<u8>,
        /// Field assignments: `(field_name, msgpack_value_bytes)`.
        updates: Vec<(String, Vec<u8>)>,
        /// Compiled row-level-security WRITE predicate, evaluated against each
        /// matched row's post-image once the assignments have been applied, or
        /// the reason no predicate is attached.
        rls_write_check: RlsWriteCheck,
    },

    /// Delete every row matching `filters`.
    ///
    /// The predicate form of [`KvOp::Delete`], which it delegates to once the
    /// scan has resolved the matching keys.
    PredicateDelete {
        collection: QualifiedCollection,
        /// Serialized `Vec<ScanFilter>` (MessagePack). Empty matches every row.
        filters: Vec<u8>,
        /// Compiled row-level-security WRITE predicate, evaluated against the
        /// pre-image of every row this removes.
        rls_write_check: RlsWriteCheck,
    },
}
