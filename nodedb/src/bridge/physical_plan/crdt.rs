//! CRDT engine operations dispatched to the Data Plane.

/// CRDT engine physical operations.
#[derive(Debug, Clone)]
pub enum CrdtOp {
    /// CRDT state read for a document.
    Read {
        collection: String,
        document_id: String,
    },

    /// CRDT delta application (write path).
    Apply {
        collection: String,
        document_id: String,
        delta: Vec<u8>,
        peer_id: u64,
        /// Per-mutation unique ID for deduplication and compensation tracking.
        mutation_id: u64,
    },

    /// Set conflict resolution policy for a CRDT collection (DDL).
    SetPolicy {
        collection: String,
        /// JSON-serialized `CollectionPolicy` from nodedb-crdt.
        policy_json: String,
    },
}
