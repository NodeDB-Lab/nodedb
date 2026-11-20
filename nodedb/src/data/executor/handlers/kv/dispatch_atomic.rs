// SPDX-License-Identifier: BUSL-1.1

//! KV dispatch for the single-key read-modify-write ops: `FieldSet`, `Incr`,
//! `IncrFloat`, `Cas`, `GetSet`. Split from `dispatch.rs` by op family.
//!
//! Each op re-types the image it computes under the declared columns its
//! plan carries. An op whose columns never resolved is refused here.

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use nodedb_physical::physical_plan::KvOp;

use super::atomic::KvAtomicCtx;

impl CoreLoop {
    /// Run a `FieldSet`: merge field updates into one key's row.
    pub(super) fn dispatch_kv_field_set(
        &mut self,
        task: &ExecutionTask,
        did: u64,
        tid: u64,
        op: &KvOp,
    ) -> Response {
        let KvOp::FieldSet {
            collection,
            declared_columns,
            key,
            updates,
            surrogate,
            if_present,
            rls_write_check,
            returning,
            rls_filters,
        } = op
        else {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: "dispatch_kv_field_set: plan is not FieldSet".into(),
                },
            );
        };
        let declared = match self.kv_op_columns(task, declared_columns, collection.as_str()) {
            Ok(declared) => declared,
            Err(refusal) => return refusal,
        };
        self.execute_kv_field_set(
            KvAtomicCtx {
                task,
                did,
                tid,
                collection: collection.as_str(),
                key,
                surrogate: *surrogate,
                rls_write_check,
                declared,
            },
            super::field::KvFieldSetArgs {
                updates,
                if_present: *if_present,
                returning: returning.as_ref(),
                rls_filters,
            },
        )
    }

    /// Run an `Incr`, `IncrFloat`, `Cas` or `GetSet`.
    pub(super) fn dispatch_kv_atomic(
        &mut self,
        task: &ExecutionTask,
        did: u64,
        tid: u64,
        op: &KvOp,
    ) -> Response {
        match op {
            KvOp::Incr {
                collection,
                declared_columns,
                key,
                delta,
                ttl_ms,
                surrogate,
                rls_write_check,
                shape,
            } => {
                let declared = match self.kv_op_columns(task, declared_columns, collection.as_str())
                {
                    Ok(declared) => declared,
                    Err(refusal) => return refusal,
                };
                self.execute_kv_incr(
                    KvAtomicCtx {
                        task,
                        did,
                        tid,
                        collection: collection.as_str(),
                        key,
                        surrogate: *surrogate,
                        rls_write_check,
                        declared,
                    },
                    *delta,
                    *ttl_ms,
                    shape,
                )
            }
            KvOp::IncrFloat {
                collection,
                declared_columns,
                key,
                delta,
                surrogate,
                rls_write_check,
                shape,
            } => {
                let declared = match self.kv_op_columns(task, declared_columns, collection.as_str())
                {
                    Ok(declared) => declared,
                    Err(refusal) => return refusal,
                };
                self.execute_kv_incr_float(
                    KvAtomicCtx {
                        task,
                        did,
                        tid,
                        collection: collection.as_str(),
                        key,
                        surrogate: *surrogate,
                        rls_write_check,
                        declared,
                    },
                    delta,
                    shape,
                )
            }
            KvOp::Cas {
                collection,
                declared_columns,
                key,
                expected,
                new_value,
                surrogate,
                rls_write_check,
            } => {
                let declared = match self.kv_op_columns(task, declared_columns, collection.as_str())
                {
                    Ok(declared) => declared,
                    Err(refusal) => return refusal,
                };
                self.execute_kv_cas(
                    KvAtomicCtx {
                        task,
                        did,
                        tid,
                        collection: collection.as_str(),
                        key,
                        surrogate: *surrogate,
                        rls_write_check,
                        declared,
                    },
                    expected,
                    new_value,
                )
            }
            KvOp::GetSet {
                collection,
                declared_columns,
                key,
                new_value,
                surrogate,
                rls_filters,
                rls_write_check,
            } => {
                let declared = match self.kv_op_columns(task, declared_columns, collection.as_str())
                {
                    Ok(declared) => declared,
                    Err(refusal) => return refusal,
                };
                self.execute_kv_getset(
                    KvAtomicCtx {
                        task,
                        did,
                        tid,
                        collection: collection.as_str(),
                        key,
                        surrogate: *surrogate,
                        rls_write_check,
                        declared,
                    },
                    new_value,
                    rls_filters,
                )
            }
            // Every other op routes through `execute_kv`'s own arms.
            KvOp::Get { .. }
            | KvOp::Put { .. }
            | KvOp::Insert { .. }
            | KvOp::InsertIfAbsent { .. }
            | KvOp::InsertOnConflictUpdate { .. }
            | KvOp::Delete { .. }
            | KvOp::Scan { .. }
            | KvOp::Expire { .. }
            | KvOp::Persist { .. }
            | KvOp::GetTtl { .. }
            | KvOp::BatchGet { .. }
            | KvOp::BatchPut { .. }
            | KvOp::RegisterIndex { .. }
            | KvOp::DropIndex { .. }
            | KvOp::FieldGet { .. }
            | KvOp::FieldSet { .. }
            | KvOp::Truncate { .. }
            | KvOp::Transfer { .. }
            | KvOp::TransferItem { .. }
            | KvOp::RegisterSortedIndex { .. }
            | KvOp::DropSortedIndex { .. }
            | KvOp::SortedIndexRank { .. }
            | KvOp::SortedIndexTopK { .. }
            | KvOp::SortedIndexRange { .. }
            | KvOp::SortedIndexCount { .. }
            | KvOp::SortedIndexScore { .. }
            | KvOp::SortedIndexTxnRead { .. }
            | KvOp::MaterializeScan { .. }
            | KvOp::ResolveWrite(_)
            | KvOp::ResolvedWrite { .. }
            | KvOp::PredicateUpdate { .. }
            | KvOp::PredicateDelete { .. } => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: "dispatch_kv_atomic: plan is not an atomic KV op".into(),
                },
            ),
        }
    }
}
