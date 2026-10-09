// SPDX-License-Identifier: BUSL-1.1

//! Vector-engine write keys.
//!
//! A single-row op locks the same `(collection, surrogate)` the
//! write-admission gate's `vector_point_key` locks.

#![deny(clippy::wildcard_enum_match_arm)]

use nodedb_physical::physical_plan::{VectorOp, VectorWriteTargets};

use super::plan::{not_a_write, unsequenced};
use super::set::WriteKeys;

/// Add the write keys of vector op `op` to `keys`.
pub(super) fn add_keys(keys: &mut WriteKeys, op: &VectorOp) -> crate::Result<()> {
    match op {
        VectorOp::Insert {
            collection,
            surrogate,
            ..
        }
        | VectorOp::DirectInsert {
            collection,
            surrogate,
            ..
        }
        | VectorOp::DirectInsertIfAbsent {
            collection,
            surrogate,
            ..
        }
        | VectorOp::DirectUpsert {
            collection,
            surrogate,
            ..
        } => keys.vector_rows(collection.as_str(), [surrogate.as_u32()]),
        // The document row a multi-vector field belongs to.
        VectorOp::MultiVectorInsert {
            collection,
            document_surrogate,
            ..
        }
        | VectorOp::MultiVectorDelete {
            collection,
            document_surrogate,
            ..
        } => keys.vector_rows(collection.as_str(), [document_surrogate.as_u32()]),
        // A key unbound in its database names no row: it locks the whole
        // collection.
        VectorOp::DeleteBySurrogate {
            collection,
            surrogate,
            ..
        } => match surrogate {
            Some(surrogate) => keys.vector_rows(collection.as_str(), [surrogate.as_u32()]),
            None => keys.whole_collection(collection.as_str()),
        },
        VectorOp::BatchInsert {
            collection,
            surrogates,
            ..
        } => keys.vector_rows(collection.as_str(), surrogates.iter().map(|s| s.as_u32())),
        VectorOp::DirectDelete {
            collection,
            targets,
            ..
        }
        | VectorOp::DirectUpdate {
            collection,
            targets,
            ..
        } => match targets {
            VectorWriteTargets::Surrogates(surrogates) => {
                keys.vector_rows(collection.as_str(), surrogates.iter().map(|s| s.as_u32()));
            }
            VectorWriteTargets::Predicate(_) => keys.whole_collection(collection.as_str()),
        },
        // No row surrogate on the plan: a node-id delete, a sparse entry
        // keyed by its document id, and a truncate.
        VectorOp::Delete { collection, .. }
        | VectorOp::SparseInsert { collection, .. }
        | VectorOp::SparseDelete { collection, .. }
        | VectorOp::DirectTruncate { collection, .. } => keys.whole_collection(collection.as_str()),
        VectorOp::ResolvedDirectWrite { .. } => {
            return Err(unsequenced(
                "a resolved governed vector write is proposed by the write-resolve orchestrator",
            ));
        }
        VectorOp::ResolveDirectWrite(_)
        | VectorOp::Search { .. }
        | VectorOp::MultiSearch { .. }
        | VectorOp::SetParams { .. }
        | VectorOp::DropIndex { .. }
        | VectorOp::QueryStats { .. }
        | VectorOp::Seal { .. }
        | VectorOp::CompactIndex { .. }
        | VectorOp::Rebuild { .. }
        | VectorOp::SparseSearch { .. }
        | VectorOp::MultiVectorScoreSearch { .. } => {
            return Err(not_a_write("a vector read or index DDL op"));
        }
    }
    Ok(())
}
