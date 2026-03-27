//! KV engine operations dispatched to the Data Plane.

/// KV engine physical operations.
///
/// All operations target a hash-indexed collection with O(1) point lookups.
/// Keys and values are serialized as Binary Tuples.
#[derive(Debug, Clone)]
pub enum KvOp {
    /// Point lookup by primary key. Returns Binary Tuple value or nil.
    Get { collection: String, key: Vec<u8> },

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
}
