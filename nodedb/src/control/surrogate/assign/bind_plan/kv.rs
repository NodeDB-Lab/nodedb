// SPDX-License-Identifier: BUSL-1.1

//! KV ops: every row is keyed by its raw `key` bytes.

use nodedb_physical::physical_plan::{KvOp, KvResolvedMutation};

use super::binder::{BindSlot, IdentityBinder};

pub(super) fn bind(binder: &IdentityBinder<'_>, op: &mut KvOp) -> crate::Result<()> {
    match op {
        KvOp::Put {
            collection,
            key,
            surrogate,
            ..
        }
        | KvOp::Insert {
            collection,
            key,
            surrogate,
            ..
        }
        | KvOp::InsertIfAbsent {
            collection,
            key,
            surrogate,
            ..
        }
        | KvOp::InsertOnConflictUpdate {
            collection,
            key,
            surrogate,
            ..
        }
        | KvOp::FieldSet {
            collection,
            key,
            surrogate,
            ..
        }
        | KvOp::Incr {
            collection,
            key,
            surrogate,
            ..
        }
        | KvOp::IncrFloat {
            collection,
            key,
            surrogate,
            ..
        }
        | KvOp::Cas {
            collection,
            key,
            surrogate,
            ..
        }
        | KvOp::GetSet {
            collection,
            key,
            surrogate,
            ..
        } => binder.resolve_in_place(binder.plan_key(collection.as_str())?, key, surrogate),
        KvOp::BatchPut {
            collection,
            entries,
            surrogates,
            ..
        } => {
            // `zip` truncates silently; a row with no identity is a
            // malformed plan, refused here.
            if entries.len() != surrogates.len() {
                return Err(crate::Error::Serialization {
                    format: "physical_plan".into(),
                    detail: format!(
                        "batch put into '{}' carries {} entries but {} surrogates; every row \
                         must carry its own surrogate",
                        collection.as_str(),
                        entries.len(),
                        surrogates.len(),
                    ),
                });
            }
            let collection_key = binder.plan_key(collection.as_str())?;
            let slots = entries
                .iter()
                .zip(surrogates.iter_mut())
                .map(|((key, _value), surrogate)| BindSlot::keyed(collection_key, key, surrogate))
                .collect();
            binder.resolve_slots_in_place(slots)
        }
        KvOp::Transfer {
            collection,
            source_key,
            dest_key,
            debit_surrogate,
            credit_surrogate,
            ..
        } => {
            let collection_key = binder.plan_key(collection.as_str())?;
            binder.resolve_slots_in_place(vec![
                BindSlot::keyed(collection_key, source_key, debit_surrogate),
                BindSlot::keyed(collection_key, dest_key, credit_surrogate),
            ])
        }
        // The moved item lands under `dest_key` in the destination collection.
        KvOp::TransferItem {
            dest_collection,
            dest_key,
            surrogate,
            ..
        } => binder.resolve_in_place(
            binder.plan_key(dest_collection.as_str())?,
            dest_key,
            surrogate,
        ),
        KvOp::ResolveWrite(inner) => bind(binder, inner),
        KvOp::ResolvedWrite { mutations, .. } => {
            let mut slots = Vec::with_capacity(mutations.len());
            for mutation in mutations.iter_mut() {
                match mutation {
                    KvResolvedMutation::Put {
                        collection,
                        key,
                        surrogate,
                        ..
                    } => slots.push(BindSlot::keyed(
                        binder.plan_key(collection.as_str())?,
                        key,
                        surrogate,
                    )),
                    // Named by key; the row's identity was bound when it was put.
                    KvResolvedMutation::Rewrite { .. }
                    | KvResolvedMutation::Delete { .. }
                    | KvResolvedMutation::Expire { .. }
                    | KvResolvedMutation::Persist { .. }
                    | KvResolvedMutation::Reap { .. } => {}
                }
            }
            binder.resolve_slots_in_place(slots)
        }
        // Key-named ops on existing rows, reads, index maintenance and
        // predicate DML carry no surrogate slot to install.
        KvOp::Delete { .. }
        | KvOp::Expire { .. }
        | KvOp::Persist { .. }
        | KvOp::Truncate { .. }
        | KvOp::PredicateUpdate { .. }
        | KvOp::PredicateDelete { .. }
        | KvOp::Get { .. }
        | KvOp::Scan { .. }
        | KvOp::GetTtl { .. }
        | KvOp::BatchGet { .. }
        | KvOp::RegisterIndex { .. }
        | KvOp::DropIndex { .. }
        | KvOp::FieldGet { .. }
        | KvOp::RegisterSortedIndex { .. }
        | KvOp::DropSortedIndex { .. }
        | KvOp::SortedIndexRank { .. }
        | KvOp::SortedIndexTopK { .. }
        | KvOp::SortedIndexRange { .. }
        | KvOp::SortedIndexCount { .. }
        | KvOp::SortedIndexScore { .. }
        | KvOp::SortedIndexTxnRead { .. }
        | KvOp::MaterializeScan { .. } => Ok(()),
    }
}
