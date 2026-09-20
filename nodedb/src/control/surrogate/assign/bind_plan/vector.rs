// SPDX-License-Identifier: BUSL-1.1

//! Vector ops: a row with a primary key binds by it; a headless row self-keys
//! by its surrogate's own bytes, the key `assign_anonymous` binds under.

use nodedb_physical::physical_plan::{VectorOp, VectorResolvedMutation};

use super::binder::IdentityBinder;

pub(super) fn bind(binder: &IdentityBinder<'_>, op: &mut VectorOp) -> crate::Result<()> {
    match op {
        VectorOp::Insert {
            collection,
            surrogate,
            pk_bytes,
            ..
        } => match pk_bytes {
            Some(pk) => binder.resolve_in_place(collection.as_str(), pk, surrogate),
            None => binder.resolve_self_keyed_in_place(collection.as_str(), surrogate),
        },
        VectorOp::BatchInsert {
            collection,
            surrogates,
            ..
        } => {
            for surrogate in surrogates.iter_mut() {
                binder.resolve_self_keyed_in_place(collection.as_str(), surrogate)?;
            }
            Ok(())
        }
        VectorOp::MultiVectorInsert {
            collection,
            document_surrogate,
            ..
        } => binder.resolve_self_keyed_in_place(collection.as_str(), document_surrogate),
        VectorOp::DirectUpsert {
            collection,
            surrogate,
            pk_bytes,
            ..
        }
        | VectorOp::DirectInsert {
            collection,
            surrogate,
            pk_bytes,
            ..
        }
        | VectorOp::DirectInsertIfAbsent {
            collection,
            surrogate,
            pk_bytes,
            ..
        } => bind_keyed_or_self(binder, collection.as_str(), pk_bytes, surrogate),
        VectorOp::ResolveDirectWrite(inner) => bind(binder, inner),
        VectorOp::ResolvedDirectWrite {
            collection,
            mutations,
            ..
        } => {
            for mutation in mutations.iter_mut() {
                match mutation {
                    VectorResolvedMutation::Upsert {
                        surrogate,
                        pk_bytes,
                        ..
                    } => bind_keyed_or_self(binder, collection.as_str(), pk_bytes, surrogate)?,
                    // Named by a surrogate bound when the row was inserted.
                    VectorResolvedMutation::Delete { .. }
                    | VectorResolvedMutation::Update { .. } => {}
                }
            }
            Ok(())
        }
        // Deletes and updates name an existing surrogate; searches, sparse
        // rows (keyed by doc id in their own store) and index maintenance
        // create no identity.
        VectorOp::Delete { .. }
        | VectorOp::DeleteBySurrogate { .. }
        | VectorOp::MultiVectorDelete { .. }
        | VectorOp::DirectDelete { .. }
        | VectorOp::DirectTruncate { .. }
        | VectorOp::DirectUpdate { .. }
        | VectorOp::Search { .. }
        | VectorOp::MultiSearch { .. }
        | VectorOp::SetParams { .. }
        | VectorOp::DropIndex { .. }
        | VectorOp::QueryStats { .. }
        | VectorOp::Seal { .. }
        | VectorOp::CompactIndex { .. }
        | VectorOp::Rebuild { .. }
        | VectorOp::SparseInsert { .. }
        | VectorOp::SparseSearch { .. }
        | VectorOp::SparseDelete { .. }
        | VectorOp::MultiVectorScoreSearch { .. } => Ok(()),
    }
}

/// An empty `pk_bytes` is a headless row: self-key it.
fn bind_keyed_or_self(
    binder: &IdentityBinder<'_>,
    collection: &str,
    pk_bytes: &[u8],
    surrogate: &mut nodedb_types::Surrogate,
) -> crate::Result<()> {
    if pk_bytes.is_empty() {
        return binder.resolve_self_keyed_in_place(collection, surrogate);
    }
    binder.resolve_in_place(collection, pk_bytes, surrogate)
}
