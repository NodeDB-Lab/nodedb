// SPDX-License-Identifier: BUSL-1.1

//! Write metadata of KV ops: which KV rows a dispatched op changes.

use crate::control::change_stream::ChangeOperation;
use nodedb_physical::physical_plan::{KvOp, KvResolvedMutation};
use nodedb_types::RowIdentity;

use super::extract::{WriteChangeMeta, every_row};

/// A KV row's identity is its key bytes, rendered as text for the subscriber.
fn kv_identity(key: &[u8]) -> RowIdentity {
    RowIdentity::from_user_key(String::from_utf8_lossy(key))
}

/// One `(collection, identity, op)` tuple per row `op` changes. Empty for a
/// read or a catalog-only op. Exhaustive over [`KvOp`], so a new op is a
/// compile error here.
pub(super) fn kv_change_meta(op: &KvOp) -> Vec<WriteChangeMeta> {
    match op {
        KvOp::Put {
            collection, key, ..
        }
        | KvOp::Insert {
            collection, key, ..
        }
        | KvOp::InsertIfAbsent {
            collection, key, ..
        }
        | KvOp::InsertOnConflictUpdate {
            collection, key, ..
        } => vec![(
            collection.to_string(),
            kv_identity(key),
            ChangeOperation::Insert,
        )],
        KvOp::Delete { collection, .. }
        // Predicate keys are decided in the Data Plane, so this reports one event with "*".
        | KvOp::PredicateDelete { collection, .. } => {
            vec![(collection.to_string(), every_row(), ChangeOperation::Delete)]
        }
        KvOp::PredicateUpdate { collection, .. } => {
            vec![(collection.to_string(), every_row(), ChangeOperation::Update)]
        }
        KvOp::FieldSet {
            collection, key, ..
        }
        | KvOp::Incr {
            collection, key, ..
        }
        | KvOp::IncrFloat {
            collection, key, ..
        }
        | KvOp::Cas {
            collection, key, ..
        }
        | KvOp::GetSet {
            collection, key, ..
        }
        | KvOp::Expire {
            collection, key, ..
        }
        | KvOp::Persist {
            collection, key, ..
        } => vec![(
            collection.to_string(),
            kv_identity(key),
            ChangeOperation::Update,
        )],
        // One event per entry, named by its key, as one point put per row
        // reports. A multi-row SQL INSERT lowers to this op.
        KvOp::BatchPut {
            collection,
            entries,
            ..
        } => entries
            .iter()
            .map(|(key, _)| {
                (
                    collection.to_string(),
                    kv_identity(key),
                    ChangeOperation::Insert,
                )
            })
            .collect(),
        KvOp::Truncate { collection, .. } => {
            vec![(collection.to_string(), every_row(), ChangeOperation::Delete)]
        }
        // Debits + credits two keys in the same collection; not individually addressable,
        // so reported as one event with document_id="*" like other batch ops.
        KvOp::Transfer { collection, .. } => {
            vec![(collection.to_string(), every_row(), ChangeOperation::Update)]
        }
        // Spans two collections (delete source, insert dest) — the only write here
        // that can't be a single tuple, so it reports two.
        KvOp::TransferItem {
            source_collection,
            dest_collection,
            item_key,
            dest_key,
            ..
        } => vec![
            (
                source_collection.to_string(),
                kv_identity(item_key),
                ChangeOperation::Delete,
            ),
            (
                dest_collection.to_string(),
                kv_identity(dest_key),
                ChangeOperation::Insert,
            ),
        ],
        // Reports one event per mutation, naming every collection/key touched — can
        // span two collections (a resolved `TransferItem`).
        // A TTL reap reports no change: its apply can be a no-op, and an
        // expiry was never a row change event.
        KvOp::ResolvedWrite { mutations, .. } => mutations
            .iter()
            .filter_map(|mutation| {
                let operation = match mutation {
                    KvResolvedMutation::Reap { .. } => return None,
                    KvResolvedMutation::Delete { .. } => ChangeOperation::Delete,
                    // Precondition: absent row = insert, present row = update.
                    KvResolvedMutation::Put { precondition, .. } => match precondition {
                        Some(_) => ChangeOperation::Update,
                        None => ChangeOperation::Insert,
                    },
                    KvResolvedMutation::Rewrite { .. }
                    | KvResolvedMutation::Expire { .. }
                    | KvResolvedMutation::Persist { .. } => ChangeOperation::Update,
                };
                Some((
                    mutation.collection().to_string(),
                    kv_identity(mutation.key()),
                    operation,
                ))
            })
            .collect(),
        // Reads, catalog-only ops, and the read-only resolve probe: no row
        // changed.
        KvOp::Get { .. }
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
        | KvOp::MaterializeScan { .. }
        | KvOp::ResolveWrite(_) => Vec::new(),
    }
}
