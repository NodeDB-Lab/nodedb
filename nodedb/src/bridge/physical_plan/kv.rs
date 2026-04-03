//! KV engine operations dispatched to the Data Plane.

/// KV engine physical operations.
///
/// All operations target a hash-indexed collection with O(1) point lookups.
/// Keys and values are serialized as Binary Tuples.
#[derive(Debug, Clone)]
pub enum KvOp {
    /// Point lookup by primary key. Returns Binary Tuple value or nil.
    Get {
        collection: String,
        key: Vec<u8>,
        /// RLS post-fetch filters. Evaluated after fetching the value.
        /// Returns nil on denial (no info leak).
        rls_filters: Vec<u8>,
    },

    /// Insert or update. Writes a Binary Tuple value keyed by primary key.
    ///
    /// If the collection has secondary indexes, they are maintained synchronously.
    /// If no secondary indexes, takes the zero-index fast path.
    Put {
        collection: String,
        key: Vec<u8>,
        /// Binary Tuple encoded value (all value columns).
        value: Vec<u8>,
        /// Per-key TTL override in milliseconds. 0 = use collection default.
        ttl_ms: u64,
    },

    /// Delete by primary key(s). Returns count of keys actually deleted.
    Delete {
        collection: String,
        keys: Vec<Vec<u8>>,
    },

    /// Cursor-based scan with optional filter predicate.
    Scan {
        collection: String,
        /// Opaque cursor from a previous scan. Empty = start from beginning.
        cursor: Vec<u8>,
        /// Maximum entries to return in this batch.
        count: usize,
        /// Optional filter predicates (same format as DocumentScan filters).
        filters: Vec<u8>,
        /// Optional glob pattern for key matching (e.g., "user:*").
        match_pattern: Option<String>,
    },

    /// Set or update TTL on an existing key.
    Expire {
        collection: String,
        key: Vec<u8>,
        /// TTL in milliseconds from now.
        ttl_ms: u64,
    },

    /// Remove TTL from an existing key (make it persistent).
    Persist { collection: String, key: Vec<u8> },

    /// Get remaining TTL for a key without fetching the value.
    ///
    /// Returns JSON `{"ttl_ms": N}` where N is:
    /// - `-2` — key does not exist
    /// - `-1` — key exists but has no TTL (persistent)
    /// - `>= 0` — remaining milliseconds until expiry
    GetTtl { collection: String, key: Vec<u8> },

    /// Batch get: fetch multiple keys in a single bridge round-trip.
    BatchGet {
        collection: String,
        keys: Vec<Vec<u8>>,
    },

    /// Batch put: insert/update multiple key-value pairs atomically.
    BatchPut {
        collection: String,
        /// `(key, value)` pairs.
        entries: Vec<(Vec<u8>, Vec<u8>)>,
        /// Per-key TTL override in milliseconds. 0 = use collection default.
        ttl_ms: u64,
    },

    /// Register a secondary index on a value field (DDL).
    ///
    /// Dispatched when `CREATE INDEX idx ON kv_collection (field)` is executed.
    /// If `backfill` is true, scans all existing entries to populate the index.
    RegisterIndex {
        collection: String,
        /// Field name to index (must match a column in the KV schema).
        field: String,
        /// Position of the field in the schema column list.
        field_position: usize,
        /// Whether to backfill the index with existing entries.
        backfill: bool,
    },

    /// Remove a secondary index from a value field (DDL).
    DropIndex { collection: String, field: String },

    /// Extract one or more fields from a key's value (HGET/HMGET).
    ///
    /// Deserializes the stored value, extracts the named fields, and returns
    /// them as a JSON object. O(1) key lookup + field extraction.
    FieldGet {
        collection: String,
        key: Vec<u8>,
        /// Field names to extract.
        fields: Vec<String>,
    },

    /// Update specific fields in a key's value (HSET).
    ///
    /// Read-modify-write: reads the current value, merges field updates,
    /// writes back. Maintains secondary indexes if any.
    FieldSet {
        collection: String,
        key: Vec<u8>,
        /// Field name → new value (JSON-encoded bytes).
        updates: Vec<(String, Vec<u8>)>,
    },

    /// Truncate: delete ALL entries in a KV collection.
    Truncate { collection: String },

    /// Atomic increment on a numeric value. Returns new value.
    ///
    /// If key doesn't exist, initializes to 0 then adds delta.
    /// If value is not i64, returns `TypeMismatch`.
    /// On overflow (i64::MAX + 1), returns `OverflowError`.
    /// TTL: if `ttl_ms > 0` and key is new, sets TTL; if key exists, resets TTL.
    /// If `ttl_ms == 0`, preserves existing TTL (no change).
    Incr {
        collection: String,
        key: Vec<u8>,
        delta: i64,
        /// TTL in milliseconds. 0 = preserve existing TTL.
        ttl_ms: u64,
    },

    /// Atomic float increment on a numeric value. Returns new value.
    ///
    /// Same semantics as `Incr` but for f64 values.
    /// If value is not f64, returns `TypeMismatch`.
    IncrFloat {
        collection: String,
        key: Vec<u8>,
        delta: f64,
    },

    /// Compare-and-swap: set value to `new_value` only if current equals `expected`.
    ///
    /// Returns JSON `{"success": bool, "current_value": "<base64>"}`.
    /// If key doesn't exist and `expected` is empty, creates the key (create-if-not-exists).
    Cas {
        collection: String,
        key: Vec<u8>,
        expected: Vec<u8>,
        new_value: Vec<u8>,
    },

    /// Atomic get-and-set: set new value, return old value.
    ///
    /// Returns the previous value (or null if key didn't exist).
    GetSet {
        collection: String,
        key: Vec<u8>,
        new_value: Vec<u8>,
    },

    // ── Sorted Index (Leaderboard) Operations ──────────────────────────
    /// Register a sorted index on a KV collection (DDL).
    RegisterSortedIndex {
        collection: String,
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
}
