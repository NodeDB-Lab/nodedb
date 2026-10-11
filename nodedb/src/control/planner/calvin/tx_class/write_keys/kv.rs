// SPDX-License-Identifier: BUSL-1.1

//! Key-Value-engine write keys.
//!
//! A single-key op locks the same `(collection, key)` the write-admission
//! gate's `kv_point_key` locks, so a fast-path write and a sequenced one
//! serialize on one key.

#![deny(clippy::wildcard_enum_match_arm)]

use nodedb_physical::physical_plan::KvOp;

use super::plan::{not_a_write, unsequenced};
use super::set::WriteKeys;

/// Add the write keys of KV op `op` to `keys`.
pub(super) fn add_keys(keys: &mut WriteKeys, op: &KvOp) -> crate::Result<()> {
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
        | KvOp::FieldSet {
            collection, key, ..
        }
        | KvOp::Expire {
            collection, key, ..
        }
        | KvOp::Persist {
            collection, key, ..
        } => keys.kv_keys(collection.as_str(), [key.clone()]),
        KvOp::Delete {
            collection,
            keys: deleted,
            ..
        } => keys.kv_keys(collection.as_str(), deleted.iter().cloned()),
        // Both keys the transfer moves value between.
        KvOp::Transfer {
            collection,
            source_key,
            dest_key,
            ..
        } => keys.kv_keys(collection.as_str(), [source_key.clone(), dest_key.clone()]),
        // Every key it writes.
        KvOp::BatchPut {
            collection,
            entries,
            ..
        } => keys.kv_keys(collection.as_str(), entries.iter().map(|(k, _)| k.clone())),
        KvOp::Truncate { collection, .. }
        | KvOp::PredicateUpdate { collection, .. }
        | KvOp::PredicateDelete { collection, .. } => keys.whole_collection(collection.as_str()),
        KvOp::TransferItem { .. } => {
            return Err(unsequenced(
                "a cross-collection KV transfer has no enforced source/target co-location",
            ));
        }
        KvOp::ResolvedWrite { .. } => {
            return Err(unsequenced(
                "a resolved governed KV write is proposed by the write-resolve orchestrator",
            ));
        }
        KvOp::ResolveWrite(_)
        | KvOp::Get { .. }
        | KvOp::Scan { .. }
        | KvOp::GetTtl { .. }
        | KvOp::BatchGet { .. }
        | KvOp::FieldGet { .. }
        | KvOp::MaterializeScan { .. }
        | KvOp::RegisterIndex { .. }
        | KvOp::DropIndex { .. }
        | KvOp::RegisterSortedIndex { .. }
        | KvOp::DropSortedIndex { .. }
        | KvOp::SortedIndexRank { .. }
        | KvOp::SortedIndexTopK { .. }
        | KvOp::SortedIndexRange { .. }
        | KvOp::SortedIndexCount { .. }
        | KvOp::SortedIndexScore { .. }
        | KvOp::SortedIndexTxnRead { .. } => {
            return Err(not_a_write("a KV read or index DDL op"));
        }
    }
    Ok(())
}
