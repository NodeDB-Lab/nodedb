// SPDX-License-Identifier: Apache-2.0

//! [`TextFields`] struct definition.

use serde::{Deserialize, Serialize};

use crate::protocol::auth::AuthMethod;
use crate::protocol::batch::{BatchDocument, BatchVector};

/// Catch-all text fields used by most operations.
///
/// Each operation uses a subset; unused fields default to `None`/empty.
///
/// Wire format: MsgPack map with `u16` numeric field IDs. `None` fields
/// are omitted. Unknown keys are ignored by the decoder (forward-compat).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TextFields {
    // ── Auth ─────────────────────────────────────────────────
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth: Option<AuthMethod>,

    // ── SQL/DDL/Explain/Set/Show/Reset/CopyFrom ─────────────
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sql: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,

    // ── Collection + document targeting ──────────────────────
    #[serde(skip_serializing_if = "Option::is_none")]
    pub collection: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub document_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Vec<u8>>,

    // ── Vector search ────────────────────────────────────────
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_vector: Option<Vec<f32>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_k: Option<u32>,

    // ── Range scan ───────────────────────────────────────────
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u64>,

    // ── CRDT ─────────────────────────────────────────────────
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delta: Option<Vec<u8>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub peer_id: Option<u64>,

    // ── Graph RAG fusion ─────────────────────────────────────
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_top_k: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub edge_label: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub direction: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expansion_depth: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub final_top_k: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_k: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub graph_k: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_field: Option<String>,

    // ── Graph ops ────────────────────────────────────────────
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_node: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end_node: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub depth: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from_node: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub to_node: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub edge_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub properties: Option<serde_json::Value>,

    // ── Text/Hybrid search ───────────────────────────────────
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_weight: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fuzzy: Option<bool>,

    // ── Vector search tuning ────────────────────────────────
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ef_search: Option<u32>,
    /// Named vector field (for multi-field vector collections).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field_name: Option<String>,

    // ── Range scan bounds ───────────────────────────────────
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lower_bound: Option<Vec<u8>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upper_bound: Option<Vec<u8>>,

    // ── CRDT dedup ──────────────────────────────────────────
    /// Monotonic mutation ID for CRDT delta deduplication.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mutation_id: Option<u64>,

    // ── Batch operations ─────────────────────────────────────
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vectors: Option<Vec<BatchVector>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub documents: Option<Vec<BatchDocument>>,

    // ── Spatial scan ───────────────────────────────────────────
    /// Query geometry as GeoJSON bytes (for SpatialScan).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_geometry: Option<Vec<u8>>,
    /// Spatial predicate name: "dwithin", "contains", "intersects", "within".
    #[serde(skip_serializing_if = "Option::is_none")]
    pub spatial_predicate: Option<String>,
    /// Distance threshold in meters (for ST_DWithin).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub distance_meters: Option<f64>,

    // ── Timeseries ───────────────────────────────────────────
    /// ILP payload bytes (for TimeseriesIngest).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<Vec<u8>>,
    /// Ingest format (default: "ilp").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,
    /// Time range start (epoch ms, for TimeseriesScan).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_range_start: Option<i64>,
    /// Time range end (epoch ms, for TimeseriesScan).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_range_end: Option<i64>,
    /// Bucket interval string for time_bucket aggregation (e.g., "5m").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bucket_interval: Option<String>,

    // ── KV advanced ─────────────────────────────────────────
    /// TTL in milliseconds (for KvExpire, KvBatchPut).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ttl_ms: Option<u64>,
    /// Cursor bytes for KvScan pagination.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<Vec<u8>>,
    /// Glob pattern for KvScan key matching.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub match_pattern: Option<String>,
    /// Multiple keys for BatchGet / Delete.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub keys: Option<Vec<Vec<u8>>>,
    /// Key-value entries for BatchPut: [(key, value), ...].
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entries: Option<Vec<(Vec<u8>, Vec<u8>)>>,
    /// Field names for FieldGet.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fields: Option<Vec<String>>,

    // ── KV atomic operations ────────────────────────────────
    /// Integer delta for KvIncr.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub incr_delta: Option<i64>,
    /// Delta for KvIncrFloat, as the client's decimal text, so no digit is
    /// lost to an `f64` on the wire.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub incr_float_delta: Option<String>,
    /// Expected value for KvCas.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected: Option<Vec<u8>>,
    /// New value for KvCas / KvGetSet.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub new_value: Option<Vec<u8>>,

    // ── KV sorted index operations ──────────────────────────
    /// Sorted index name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_name: Option<String>,
    /// Sort columns: [(column_name, direction), ...].
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort_columns: Option<Vec<(String, String)>>,
    /// Primary key column for sorted index.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key_column: Option<String>,
    /// Window type for sorted index: "none", "daily", "weekly", "monthly", "custom".
    #[serde(skip_serializing_if = "Option::is_none")]
    pub window_type: Option<String>,
    /// Timestamp column for windowed sorted index.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub window_timestamp_column: Option<String>,
    /// Custom window start (ms since epoch).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub window_start_ms: Option<u64>,
    /// Custom window end (ms since epoch).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub window_end_ms: Option<u64>,
    /// Top-K value for SortedIndexTopK.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_k_count: Option<u32>,
    /// Score min for SortedIndexRange (encoded bytes).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub score_min: Option<Vec<u8>>,
    /// Score max for SortedIndexRange (encoded bytes).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub score_max: Option<Vec<u8>>,

    // ── Document advanced ───────────────────────────────────
    /// Field-level updates: [(field_name, value_bytes), ...].
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updates: Option<Vec<(String, Vec<u8>)>>,
    /// Serialized filter predicates (MessagePack).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filters: Option<Vec<u8>>,

    // ── Vector advanced ─────────────────────────────────────
    /// Single vector embedding (for VectorInsert).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector: Option<Vec<f32>>,
    /// Vector ID for deletion. Wire type is u64 to accommodate future range
    /// expansion; the server narrows to u32 via the surrogate space check.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_id: Option<u64>,

    // ── Collection policy ────────────────────────────────────
    #[serde(skip_serializing_if = "Option::is_none")]
    pub policy: Option<serde_json::Value>,

    // ── Graph algorithm/match ───────────────────────────────
    /// Algorithm name for GraphAlgo (e.g., "pagerank", "wcc", "sssp").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub algorithm: Option<String>,
    /// Cypher-subset MATCH query string for GraphMatch.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub match_query: Option<String>,
    /// Algorithm-specific parameters (JSON object).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub algo_params: Option<serde_json::Value>,

    // ── Document DDL ────────────────────────────────────────
    /// Index paths for DocumentRegister.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_paths: Option<Vec<String>>,
    /// Source collection for InsertSelect.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_collection: Option<String>,

    // ── KV DDL ──────────────────────────────────────────────
    /// Field position in tuple for KvRegisterIndex.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field_position: Option<u64>,
    /// Whether to backfill existing keys on index creation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub backfill: Option<bool>,

    // ── Vector DDL ──────────────────────────────────────────
    /// HNSW M parameter (max connections per layer).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub m: Option<u16>,
    /// HNSW ef_construction parameter.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ef_construction: Option<u16>,
    /// Distance metric name ("cosine", "euclidean", "dot").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metric: Option<String>,
    /// Index type ("hnsw", "hnsw_pq", "ivf_pq").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_type: Option<String>,
    /// Declared vector dimension, enforced against every vector written to
    /// the field. `None` = not declared; the index adopts the width of the
    /// first vector it sees.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_dim: Option<u32>,

    // ── Database context ─────────────────────────────────────
    /// Target database name sent in the `Auth` handshake frame.
    ///
    /// Field ID 83. `None` = server default (`DatabaseId::DEFAULT`).
    /// The server binds this to the session's `current_database` at
    /// handshake time so every subsequent operation runs in this
    /// database context without per-request overhead.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub database: Option<String>,

    // ── SQL bound parameters ────────────────────────────────
    /// Caller-supplied bound parameters for `$1`, `$2`, … placeholders
    /// in `sql`.
    ///
    /// Field ID 84. `None` (or empty) = no bound parameters; the
    /// server runs the SQL as-is. When `Some`, the server inlines each
    /// value as a SQL literal before planning. The wire encoding is
    /// `Vec<Value>` written via the generic zerompk array
    /// implementation — Value has its own hand-rolled
    /// `ToMessagePack`/`FromMessagePack` so the canonical scalar
    /// variants (Null, Bool, Integer, Float, String, Bytes) round-trip
    /// without a lossy intermediate JSON step.
    ///
    /// We carry `Vec<Value>` directly rather than `Vec<u8>` of a
    /// pre-serialised inner blob because zerompk's generic
    /// `ToMessagePack for Vec<T>` writes a MessagePack array (length
    /// header + each element via `T::write`), which round-trips
    /// cleanly with the symmetric `Vec::<Value>::read`. A
    /// pre-serialised `Vec<u8>` carried in this field would be
    /// re-encoded as an array-of-u8 on the way out and fail to decode
    /// as binary on the way in — exactly the silent-drop pattern this
    /// field is added to close.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sql_params: Option<Vec<crate::value::Value>>,

    // ── CRDT list operations ─────────────────────────────────
    /// Dot-addressed path to the movable list container within the
    /// document (for `CrdtListInsert` / `CrdtListDelete` / `CrdtListMove`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub list_path: Option<String>,
    /// Target index for `CrdtListInsert` / `CrdtListDelete`. Wire type is
    /// `u64`; the server narrows to `usize` and rejects out-of-range
    /// values with a typed error rather than defaulting to `0`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub list_index: Option<u64>,
    /// Source index for `CrdtListMove`. Distinct from `list_to_index` —
    /// the two must never be conflated.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub list_from_index: Option<u64>,
    /// Destination index for `CrdtListMove`. Distinct from
    /// `list_from_index` — the two must never be conflated.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub list_to_index: Option<u64>,
    /// JSON object of block fields to insert as a new list element (for
    /// `CrdtListInsert`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub list_fields_json: Option<String>,
}
