// SPDX-License-Identifier: Apache-2.0

//! Which user collection a KV operation targets.
//!
//! Kept beside the operation enum rather than inside it so `op.rs` stays the
//! single declaration of the wire shape and nothing else.

use super::op::KvOp;

impl KvOp {
    /// The user collection this op targets, if any. Sorted-index ops keyed
    /// only by index name and `ResolvedWrite` (mutations may span two
    /// collections) return `None`. `TransferItem` reports its source.
    pub fn collection(&self) -> Option<&str> {
        match self {
            KvOp::Get { collection, .. }
            | KvOp::Put { collection, .. }
            | KvOp::Insert { collection, .. }
            | KvOp::InsertIfAbsent { collection, .. }
            | KvOp::InsertOnConflictUpdate { collection, .. }
            | KvOp::Delete { collection, .. }
            | KvOp::Scan { collection, .. }
            | KvOp::Expire { collection, .. }
            | KvOp::Persist { collection, .. }
            | KvOp::GetTtl { collection, .. }
            | KvOp::BatchGet { collection, .. }
            | KvOp::BatchPut { collection, .. }
            | KvOp::RegisterIndex { collection, .. }
            | KvOp::DropIndex { collection, .. }
            | KvOp::FieldGet { collection, .. }
            | KvOp::FieldSet { collection, .. }
            | KvOp::Truncate { collection, .. }
            | KvOp::Incr { collection, .. }
            | KvOp::IncrFloat { collection, .. }
            | KvOp::Cas { collection, .. }
            | KvOp::GetSet { collection, .. }
            | KvOp::Transfer { collection, .. }
            | KvOp::RegisterSortedIndex { collection, .. }
            | KvOp::PredicateUpdate { collection, .. }
            | KvOp::PredicateDelete { collection, .. }
            | KvOp::MaterializeScan { collection, .. }
            | KvOp::SortedIndexTxnRead { collection, .. } => Some(collection.as_str()),
            KvOp::TransferItem {
                source_collection, ..
            } => Some(source_collection.as_str()),
            // The wrapped op is the intercepted write verbatim, so the resolve
            // dispatch routes to the core that owns exactly the rows the write
            // reads.
            KvOp::ResolveWrite(inner) => inner.collection(),
            KvOp::ResolvedWrite { .. }
            | KvOp::DropSortedIndex { .. }
            | KvOp::SortedIndexRank { .. }
            | KvOp::SortedIndexTopK { .. }
            | KvOp::SortedIndexRange { .. }
            | KvOp::SortedIndexCount { .. }
            | KvOp::SortedIndexScore { .. } => None,
        }
    }
}
