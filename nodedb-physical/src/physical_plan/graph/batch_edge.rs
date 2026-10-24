// SPDX-License-Identifier: Apache-2.0

//! `BatchEdge`: one edge in an `EdgePutBatch` / `EdgeDeleteBatch`.

use nodedb_types::{QualifiedCollection, RlsWriteCheck, Surrogate};

/// One edge in an `EdgePutBatch` / `EdgeDeleteBatch`.
///
/// `src_surrogate` / `dst_surrogate` carry the global row identity for the
/// edge endpoints (resolved at construction time via the surrogate assigner).
/// Neither is ever `Surrogate::ZERO`: an edge that carries it is refused.
///
/// `write_gate` is the edge's own write-policy check, the same slot
/// `GraphOp::EdgeDelete` carries as `rls_write_check`. An `EdgeDeleteBatch`
/// decides it per edge against the stored image, before any tombstone is
/// written. An `EdgePutBatch` ignores it: a put is decided at injection.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct BatchEdge {
    pub collection: QualifiedCollection,
    pub src_id: String,
    pub label: String,
    pub dst_id: String,
    pub src_surrogate: Surrogate,
    pub dst_surrogate: Surrogate,
    pub write_gate: RlsWriteCheck,
}
