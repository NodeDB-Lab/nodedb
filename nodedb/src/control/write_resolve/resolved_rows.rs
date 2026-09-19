// SPDX-License-Identifier: BUSL-1.1

//! The decided row set a governed predicate write resolved to.

use nodedb_physical::physical_plan::{
    DocumentResolvedMutation, KvResolvedMutation, VectorResolvedMutation,
};
use nodedb_types::Value;

/// Concrete rows a governed predicate `UPDATE`/`DELETE` resolved to, after the
/// Data Plane decided the write policy against each one's exact image.
/// Native `nodedb_types::Value` throughout — `Value::from(serde_json::Value)`
/// is lossy for several types, so a JSON-roundtripped row is not the row.
pub enum ResolvedRows {
    /// `(primary key, full post-image)` for every row the policy admitted.
    Update(Vec<(Value, Vec<Value>)>),
    /// Primary key of every row the policy admitted for removal.
    Delete(Vec<Value>),
    /// KV mutations plus the exact response payload — not row-set shaped, so
    /// e.g. a `CAS` miss owes a reply while writing nothing.
    Kv {
        mutations: Vec<KvResolvedMutation>,
        response_payload: Vec<u8>,
    },
    /// One shape for all five governed deferred document writes: a point op
    /// resolves to one mutation, a bulk op to N, plus the reply payload.
    Document {
        mutations: Vec<DocumentResolvedMutation>,
        response_payload: Vec<u8>,
    },
    /// Row mutations of a governed vector-primary write plus the exact
    /// response payload, decided against each row's stored sidecar.
    Vector {
        mutations: Vec<VectorResolvedMutation>,
        response_payload: Vec<u8>,
    },
    /// Canonical line-protocol lines a governed ingest resolved to, every
    /// timestamp stamped — an ingest has no rows until rewritten to lines.
    Timeseries { lines: Vec<String> },
    /// The governed edge delete's pre-image satisfied the policy; the delete
    /// already names its edge in full, so nothing else travels back.
    GraphEdgeDeleteAdmitted,
}
