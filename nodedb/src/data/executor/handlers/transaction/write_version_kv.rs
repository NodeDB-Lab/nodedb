// SPDX-License-Identifier: BUSL-1.1

//! `KvOp` write versions for the parity oracle in `write_version.rs`. Same
//! contract: no-op with no WAL LSN, one `note_write` per key a committed
//! KV write touched.

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::write_index::KeyRepr;
use crate::data::executor::core_loop::write_index::WriteStamp;
use crate::types::TenantId;
use nodedb_physical::physical_plan::KvOp;

impl CoreLoop {
    pub(super) fn record_kv_version(
        &mut self,
        db: crate::types::DatabaseId,
        tenant: TenantId,
        op: &KvOp,
        stamp: WriteStamp,
    ) {
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
            | KvOp::Expire {
                collection, key, ..
            }
            | KvOp::Persist {
                collection, key, ..
            }
            | KvOp::FieldSet {
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
            } => {
                self.note_write(
                    db,
                    tenant,
                    collection.as_str(),
                    Some(KeyRepr::KvKey(Box::from(key.as_slice()))),
                    stamp,
                );
            }
            KvOp::Delete {
                collection, keys, ..
            } => {
                for key in keys {
                    self.note_write(
                        db,
                        tenant,
                        collection.as_str(),
                        Some(KeyRepr::KvKey(Box::from(key.as_slice()))),
                        stamp,
                    );
                }
            }
            KvOp::BatchPut {
                collection,
                entries,
                ..
            } => {
                for (key, _value) in entries {
                    self.note_write(
                        db,
                        tenant,
                        collection.as_str(),
                        Some(KeyRepr::KvKey(Box::from(key.as_slice()))),
                        stamp,
                    );
                }
            }
            // Whole-collection mutations: key set is every row or predicate-
            // resolved at apply time, so only the collection floor applies.
            KvOp::Truncate { collection, .. }
            | KvOp::PredicateUpdate { collection, .. }
            | KvOp::PredicateDelete { collection, .. } => {
                self.note_write(db, tenant, collection.as_str(), None, stamp);
            }
            // Read-only: nothing written, no version to record.
            KvOp::Get { .. }
            | KvOp::Scan { .. }
            | KvOp::GetTtl { .. }
            | KvOp::BatchGet { .. }
            | KvOp::FieldGet { .. }
            | KvOp::MaterializeScan { .. }
            | KvOp::SortedIndexRank { .. }
            | KvOp::SortedIndexTopK { .. }
            | KvOp::SortedIndexRange { .. }
            | KvOp::SortedIndexCount { .. }
            | KvOp::SortedIndexScore { .. }
            | KvOp::SortedIndexTxnRead { .. } => {}
            // Index DDL: no row key written.
            KvOp::RegisterIndex { .. }
            | KvOp::DropIndex { .. }
            | KvOp::RegisterSortedIndex { .. }
            | KvOp::DropSortedIndex { .. } => {}
            // Resolve pass reads what a governed write depends on; the
            // decided write lands as ResolvedWrite once proposed. Transfer /
            // TransferItem resolve into ResolvedWrite before commit, so the
            // raw op never reaches this path. ResolvedWrite's mutations
            // aren't tracked here, matching `DocumentOp::ResolvedWrite`.
            KvOp::ResolveWrite(_)
            | KvOp::ResolvedWrite { .. }
            | KvOp::Transfer { .. }
            | KvOp::TransferItem { .. } => {}
        }
    }
}
