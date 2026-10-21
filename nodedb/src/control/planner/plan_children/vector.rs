// SPDX-License-Identifier: BUSL-1.1

//! The sub-plan a vector op takes its prefilter from.

use nodedb_physical::physical_plan::{PhysicalPlan, VectorOp};

/// The resolved sub-plan a vector search takes its prefilter from, if any.
pub(super) fn vector_child(op: &VectorOp) -> Option<&PhysicalPlan> {
    match op {
        VectorOp::Search {
            inline_prefilter_plan: Some(child),
            ..
        } => Some(child),
        VectorOp::ResolveDirectWrite(inner) => vector_child(inner),
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
        | VectorOp::MultiVectorScoreSearch { .. }
        | VectorOp::DirectUpsert { .. }
        | VectorOp::DirectInsert { .. }
        | VectorOp::DirectInsertIfAbsent { .. }
        | VectorOp::DirectBatchWrite { .. }
        | VectorOp::DirectDelete { .. }
        | VectorOp::DirectTruncate { .. }
        | VectorOp::DirectUpdate { .. }
        | VectorOp::ResolvedDirectWrite { .. } => None,
    }
}

/// The resolved sub-plan a vector search takes its prefilter from, if any,
/// mutable.
pub(super) fn vector_child_mut(op: &mut VectorOp) -> Option<&mut PhysicalPlan> {
    match op {
        VectorOp::Search {
            inline_prefilter_plan: Some(child),
            ..
        } => Some(child),
        VectorOp::ResolveDirectWrite(inner) => vector_child_mut(inner),
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
        | VectorOp::MultiVectorScoreSearch { .. }
        | VectorOp::DirectUpsert { .. }
        | VectorOp::DirectInsert { .. }
        | VectorOp::DirectInsertIfAbsent { .. }
        | VectorOp::DirectBatchWrite { .. }
        | VectorOp::DirectDelete { .. }
        | VectorOp::DirectTruncate { .. }
        | VectorOp::DirectUpdate { .. }
        | VectorOp::ResolvedDirectWrite { .. } => None,
    }
}
