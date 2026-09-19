// SPDX-License-Identifier: Apache-2.0

//! Which user collection a vector-primary direct write targets.
//!
//! Kept beside the operation enum rather than inside it so `op.rs` stays the
//! single declaration of the wire shape and nothing else.

use super::op::VectorOp;

impl VectorOp {
    /// The vector-primary collection a direct write targets, or `None` for
    /// every op outside that family. `ResolveDirectWrite` reports the wrapped
    /// write's collection so the resolve dispatch routes to the core that
    /// owns exactly the rows the write reads.
    pub fn direct_write_collection(&self) -> Option<&str> {
        match self {
            VectorOp::DirectUpsert { collection, .. }
            | VectorOp::DirectInsert { collection, .. }
            | VectorOp::DirectInsertIfAbsent { collection, .. }
            | VectorOp::DirectDelete { collection, .. }
            | VectorOp::DirectUpdate { collection, .. }
            | VectorOp::ResolvedDirectWrite { collection, .. } => Some(collection.as_str()),
            VectorOp::ResolveDirectWrite(inner) => inner.direct_write_collection(),
            VectorOp::Search { .. }
            | VectorOp::Insert { .. }
            | VectorOp::BatchInsert { .. }
            | VectorOp::MultiSearch { .. }
            | VectorOp::Delete { .. }
            | VectorOp::DeleteBySurrogate { .. }
            | VectorOp::SetParams { .. }
            | VectorOp::DropIndex { .. }
            | VectorOp::QueryStats { .. }
            | VectorOp::Seal { .. }
            | VectorOp::CompactIndex { .. }
            | VectorOp::Rebuild { .. }
            | VectorOp::SparseInsert { .. }
            | VectorOp::SparseSearch { .. }
            | VectorOp::SparseDelete { .. }
            | VectorOp::MultiVectorInsert { .. }
            | VectorOp::MultiVectorDelete { .. }
            | VectorOp::MultiVectorScoreSearch { .. } => None,
        }
    }
}
