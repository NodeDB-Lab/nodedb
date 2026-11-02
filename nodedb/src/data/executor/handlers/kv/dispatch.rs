// SPDX-License-Identifier: BUSL-1.1

//! KV operation dispatch: routes `KvOp` variants to their handler methods.

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use nodedb_physical::physical_plan::KvOp;

impl CoreLoop {
    /// Dispatch a KV operation to the appropriate handler.
    pub(in crate::data::executor) fn execute_kv(
        &mut self,
        task: &ExecutionTask,
        did: u64,
        tid: u64,
        op: &KvOp,
    ) -> Response {
        match op {
            KvOp::Get {
                collection,
                key,
                rls_filters,
                surrogate_ceiling,
                system_time,
            } => self.execute_kv_get(
                task,
                super::crud::KvGetParams {
                    did,
                    tid,
                    collection: collection.as_str(),
                    key,
                    rls_filters,
                    surrogate_ceiling: *surrogate_ceiling,
                    system_time: *system_time,
                },
            ),
            KvOp::Put {
                collection,
                key,
                value,
                ttl_ms,
                surrogate,
                returning,
                rls_filters,
                provenance,
            } => {
                let params = super::crud::KvWriteParams {
                    did,
                    tid,
                    collection: collection.as_str(),
                    key,
                    value,
                    ttl_ms: *ttl_ms,
                    surrogate: *surrogate,
                    returning: returning.as_ref(),
                    rls_filters,
                };
                match provenance {
                    Some(prov) => self.execute_kv_sync_put(task, params, prov),
                    None => self.execute_kv_put(task, params),
                }
            }
            KvOp::Insert {
                collection,
                key,
                value,
                ttl_ms,
                surrogate,
                returning,
                rls_filters,
            } => self.execute_kv_insert(
                task,
                super::crud::KvWriteParams {
                    did,
                    tid,
                    collection: collection.as_str(),
                    key,
                    value,
                    ttl_ms: *ttl_ms,
                    surrogate: *surrogate,
                    returning: returning.as_ref(),
                    rls_filters,
                },
            ),
            KvOp::InsertIfAbsent {
                collection,
                key,
                value,
                ttl_ms,
                surrogate,
                returning,
                rls_filters,
            } => self.execute_kv_insert_if_absent(
                task,
                super::crud::KvWriteParams {
                    did,
                    tid,
                    collection: collection.as_str(),
                    key,
                    value,
                    ttl_ms: *ttl_ms,
                    surrogate: *surrogate,
                    returning: returning.as_ref(),
                    rls_filters,
                },
            ),
            KvOp::InsertOnConflictUpdate {
                collection,
                key,
                value,
                ttl_ms,
                updates,
                surrogate,
                rls_write_check,
                returning,
                rls_filters,
            } => self.execute_kv_insert_on_conflict_update(
                task,
                super::crud::KvInsertOnConflictUpdateParams {
                    did,
                    tid,
                    collection: collection.as_str(),
                    key,
                    value,
                    ttl_ms: *ttl_ms,
                    updates,
                    surrogate: *surrogate,
                    rls_write_check,
                    returning: returning.as_ref(),
                    rls_filters,
                },
            ),
            KvOp::Delete {
                collection,
                keys,
                rls_write_check,
                returning,
                rls_filters,
                provenance,
            } => {
                let params = super::crud::KvDeleteParams {
                    did,
                    tid,
                    collection: collection.as_str(),
                    keys,
                    rls_write_check,
                    returning: returning.as_ref(),
                    rls_filters,
                };
                match provenance {
                    Some(prov) => self.execute_kv_sync_delete(task, params, prov),
                    None => self.execute_kv_delete(task, params),
                }
            }
            KvOp::Scan { .. } => self.dispatch_kv_scan(task, did, tid, op),
            KvOp::Expire {
                collection,
                key,
                ttl_ms,
                rls_write_check,
            } => self.execute_kv_expire(
                task,
                super::ttl::KvTtlTarget {
                    did,
                    tid,
                    collection: collection.as_str(),
                    key,
                    rls_write_check,
                },
                *ttl_ms,
            ),
            KvOp::Persist {
                collection,
                key,
                rls_write_check,
            } => self.execute_kv_persist(
                task,
                super::ttl::KvTtlTarget {
                    did,
                    tid,
                    collection: collection.as_str(),
                    key,
                    rls_write_check,
                },
            ),
            KvOp::BatchGet {
                collection,
                keys,
                rls_filters,
                system_time,
            } => self.execute_kv_batch_get(
                task,
                super::batch::KvBatchGetArgs {
                    did,
                    tid,
                    collection: collection.as_str(),
                    keys,
                    rls_filters,
                    system_time: *system_time,
                },
            ),
            KvOp::BatchPut {
                collection,
                entries,
                ttl_ms,
                intent,
                surrogates,
                returning,
                rls_filters,
            } => self.execute_kv_batch_put(
                task,
                super::batch_put::KvBatchPutArgs {
                    did,
                    tid,
                    collection: collection.as_str(),
                    entries,
                    ttl_ms: *ttl_ms,
                    intent,
                    surrogates,
                    returning: returning.as_ref(),
                    rls_filters,
                },
            ),
            KvOp::RegisterIndex {
                collection,
                field,
                field_position,
                backfill,
            } => self.execute_kv_register_index(
                task,
                super::index::KvRegisterIndexParams {
                    did,
                    tid,
                    collection: collection.as_str(),
                    field,
                    field_position: *field_position,
                    backfill: *backfill,
                },
            ),
            KvOp::DropIndex { collection, field } => {
                self.execute_kv_drop_index(task, did, tid, collection.as_str(), field)
            }
            KvOp::FieldGet {
                collection,
                key,
                fields,
                rls_filters,
                system_time,
            } => self.execute_kv_field_get(
                task,
                super::field::KvFieldGetArgs {
                    did,
                    tid,
                    collection: collection.as_str(),
                    key,
                    fields,
                    rls_filters,
                    system_time: *system_time,
                },
            ),
            KvOp::FieldSet {
                collection,
                key,
                updates,
                surrogate,
                if_present,
                rls_write_check,
                returning,
                rls_filters,
            } => self.execute_kv_field_set(
                super::atomic::KvAtomicCtx {
                    task,
                    did,
                    tid,
                    collection: collection.as_str(),
                    key,
                    surrogate: *surrogate,
                    rls_write_check,
                },
                super::field::KvFieldSetArgs {
                    updates,
                    if_present: *if_present,
                    returning: returning.as_ref(),
                    rls_filters,
                },
            ),
            KvOp::GetTtl { collection, key } => {
                self.execute_kv_get_ttl(task, did, tid, collection.as_str(), key)
            }
            KvOp::Truncate { collection, .. } => {
                self.execute_kv_truncate(task, did, tid, collection.as_str())
            }
            KvOp::Incr {
                collection,
                key,
                delta,
                ttl_ms,
                surrogate,
                rls_write_check,
                shape,
            } => self.execute_kv_incr(
                super::atomic::KvAtomicCtx {
                    task,
                    did,
                    tid,
                    collection: collection.as_str(),
                    key,
                    surrogate: *surrogate,
                    rls_write_check,
                },
                *delta,
                *ttl_ms,
                shape,
            ),
            KvOp::IncrFloat {
                collection,
                key,
                delta,
                surrogate,
                rls_write_check,
                shape,
            } => self.execute_kv_incr_float(
                super::atomic::KvAtomicCtx {
                    task,
                    did,
                    tid,
                    collection: collection.as_str(),
                    key,
                    surrogate: *surrogate,
                    rls_write_check,
                },
                delta,
                shape,
            ),
            KvOp::Cas {
                collection,
                key,
                expected,
                new_value,
                surrogate,
                rls_write_check,
            } => self.execute_kv_cas(
                super::atomic::KvAtomicCtx {
                    task,
                    did,
                    tid,
                    collection: collection.as_str(),
                    key,
                    surrogate: *surrogate,
                    rls_write_check,
                },
                expected,
                new_value,
            ),
            KvOp::GetSet {
                collection,
                key,
                new_value,
                surrogate,
                rls_filters,
                rls_write_check,
            } => self.execute_kv_getset(
                super::atomic::KvAtomicCtx {
                    task,
                    did,
                    tid,
                    collection: collection.as_str(),
                    key,
                    surrogate: *surrogate,
                    rls_write_check,
                },
                new_value,
                rls_filters,
            ),
            KvOp::RegisterSortedIndex {
                collection,
                index_name,
                sort_columns,
                key_column,
                window_type,
                window_timestamp_column,
                window_start_ms,
                window_end_ms,
            } => self.execute_kv_register_sorted_index(
                task,
                super::sorted::KvRegisterSortedIndexParams {
                    did,
                    tid,
                    collection: collection.as_str(),
                    index_name,
                    sort_columns,
                    key_column,
                    window_type,
                    window_timestamp_column,
                    window_start_ms: *window_start_ms,
                    window_end_ms: *window_end_ms,
                },
            ),
            KvOp::DropSortedIndex { index_name } => {
                self.execute_kv_drop_sorted_index(task, did, tid, index_name)
            }
            // Every sorted-index read answers at its system-time scope.
            KvOp::SortedIndexRank { .. }
            | KvOp::SortedIndexTopK { .. }
            | KvOp::SortedIndexRange { .. }
            | KvOp::SortedIndexCount { .. }
            | KvOp::SortedIndexScore { .. } => self.dispatch_kv_sorted_read(task, did, tid, op),
            KvOp::SortedIndexTxnRead {
                collection,
                index_name,
                pending,
                read,
                system_time,
            } => self.execute_kv_sorted_index_txn_read(
                task,
                super::sorted_txn::SortedIndexTxnReadParams {
                    did,
                    tid,
                    collection: collection.as_str(),
                    index_name,
                    pending: pending.as_ref(),
                    read,
                    system_time,
                },
            ),
            KvOp::Transfer { .. } => self.dispatch_kv_transfer(task, did, tid, op),
            KvOp::TransferItem { .. } => self.dispatch_kv_transfer_item(task, did, tid, op),
            KvOp::MaterializeScan {
                collection,
                cursor,
                count,
                system_time,
                only_key,
            } => self.execute_kv_materialize_scan(
                task,
                super::materialize_scan::KvMaterializeScan {
                    did,
                    tid,
                    collection: collection.as_str(),
                    cursor,
                    count: *count,
                    system_time: *system_time,
                    only_key: only_key.as_deref(),
                },
            ),
            // A predicate write resolves to a mutation list before it is
            // logged, and a transaction stages it. It never applies directly.
            KvOp::PredicateUpdate { collection, .. } | KvOp::PredicateDelete { collection, .. } => {
                self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!(
                            "kv predicate UPDATE/DELETE on '{}' reached the apply path \
                             unresolved; it resolves to a mutation list first",
                            collection.as_str()
                        ),
                    },
                )
            }
            KvOp::ResolveWrite(inner) => self.execute_kv_resolve_write(task, did, tid, inner),
            KvOp::ResolvedWrite {
                mutations,
                response_payload,
                rls_write_check,
            } => self.execute_kv_resolved_write(
                task,
                did,
                tid,
                mutations,
                response_payload,
                rls_write_check,
            ),
        }
    }
}
