// SPDX-License-Identifier: Apache-2.0

//! The decided mutation set a governed vector-primary write resolved to.
//!
//! A vector-primary `DELETE` / `UPDATE` / conflict-patching `UPSERT` on a
//! collection with a row-level write policy cannot cross the Raft wire
//! carrying the live predicate: a follower has no writing identity to decide
//! it against. The Control Plane resolves the write against the rows the
//! Data Plane holds, decides the policy there, and ships the row mutations
//! themselves.

use nodedb_types::Surrogate;

/// One row mutation a resolved vector-primary write applies.
///
/// Every variant carries the sidecar bytes the resolve read for its row —
/// the apply's drift check. A row's sidecar is its `zerompk` TAGGED payload
/// map and is never empty once written, so `Vec::new()` on `Delete` /
/// `Update` names a bound node with no sidecar row behind it, and the apply
/// compares it against an absent sidecar.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum VectorResolvedMutation {
    /// Remove `surrogate`'s HNSW node, bitmap entries, and sidecar.
    Delete {
        surrogate: Surrogate,
        /// Sidecar bytes the resolve read; the node must still be bound and
        /// hold exactly these bytes.
        old_payload: Vec<u8>,
    },
    /// Rewrite `surrogate`'s row: a `new_vector` rebuilds the HNSW node
    /// under the same surrogate, `merged_payload` replaces the sidecar and
    /// moves the bitmap entries.
    Update {
        surrogate: Surrogate,
        new_vector: Option<Vec<f32>>,
        /// The full post-image sidecar, already merged and lower-cased,
        /// in the sidecar's own `zerompk` TAGGED encoding.
        merged_payload: Vec<u8>,
        /// Sidecar bytes the resolve read; see `Delete::old_payload`.
        old_payload: Vec<u8>,
    },
    /// Store a whole row under `surrogate`, replacing any existing one.
    Upsert {
        surrogate: Surrogate,
        /// UTF-8 of the declared primary-key value. Followers bind the
        /// leader-assigned surrogate to this exact key.
        pk_bytes: Vec<u8>,
        vector: Vec<f32>,
        /// The sidecar to store, in its own `zerompk` TAGGED encoding.
        payload: Vec<u8>,
        /// `None` requires the surrogate to still be unbound; `Some(bytes)`
        /// requires a bound node whose sidecar holds exactly `bytes`.
        old_payload: Option<Vec<u8>>,
    },
}

impl VectorResolvedMutation {
    /// The surrogate this one mutation targets.
    pub fn surrogate(&self) -> Surrogate {
        match self {
            VectorResolvedMutation::Delete { surrogate, .. }
            | VectorResolvedMutation::Update { surrogate, .. }
            | VectorResolvedMutation::Upsert { surrogate, .. } => *surrogate,
        }
    }

    /// The vector this mutation stores, when it stores one.
    pub fn stored_vector(&self) -> Option<&[f32]> {
        match self {
            VectorResolvedMutation::Delete { .. } => None,
            VectorResolvedMutation::Update { new_vector, .. } => new_vector.as_deref(),
            VectorResolvedMutation::Upsert { vector, .. } => Some(vector.as_slice()),
        }
    }
}

/// What `VectorOp::ResolveDirectWrite` reports back: every row mutation the
/// intercepted write applies, plus the exact response payload the statement
/// returns once they all apply cleanly.
///
/// An empty `mutations` list is a legitimate outcome — a predicate that
/// matches no row writes nothing and still owes its `{"affected": 0}` reply.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct VectorResolveOutcome {
    pub mutations: Vec<VectorResolvedMutation>,
    pub response_payload: Vec<u8>,
}
