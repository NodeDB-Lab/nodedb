// SPDX-License-Identifier: Apache-2.0

//! `BatchEdge`: one edge in an `EdgePutBatch` / `EdgeDeleteBatch`.

use nodedb_types::{QualifiedCollection, Surrogate};

/// One edge in an `EdgePutBatch` / `EdgeDeleteBatch`.
///
/// `src_surrogate` / `dst_surrogate` carry the global row identity for the
/// edge endpoints (resolved at construction time via the surrogate assigner).
/// Neither is ever `Surrogate::ZERO`: an edge that carries it is refused.
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
    /// Start of the edge's valid time, ms since epoch. `None` takes the
    /// write's wall time. A delete ignores it.
    pub valid_from_ms: Option<i64>,
    /// Exclusive end of the edge's valid time. `None` leaves it open. A
    /// delete ignores it.
    pub valid_until_ms: Option<i64>,
}
