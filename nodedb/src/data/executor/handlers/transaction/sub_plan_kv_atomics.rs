// SPDX-License-Identifier: BUSL-1.1

//! COMMIT-time execution of the KV read-modify-write atomics: `Incr`,
//! `IncrFloat`, `Cas`, `GetSet`.
//!
//! Split out of `sub_plan_kv_writes.rs` to keep that file under the per-file
//! line budget. All four share one shape: read the prior value, delegate to
//! the SAME live handler autocommit uses — so the row-level-security write
//! gate, the TTL resolution, and the event emission are the autocommit ones
//! rather than a second implementation — then record the prior value so a
//! later sibling sub-plan's failure can restore it.

use crate::bridge::envelope::{ErrorCode, Response, Status};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::engine::kv::current_ms;
use nodedb_physical::physical_plan::KvOp;

use super::undo::UndoEntry;

impl CoreLoop {
    /// Execute one KV atomic in a transaction context.
    ///
    /// Caller invariant: `op` is `Incr`, `IncrFloat`, `Cas`, or `GetSet` —
    /// `execute_tx_kv_write` routes nothing else here.
    pub(super) fn execute_tx_kv_atomic(
        &mut self,
        task: &ExecutionTask,
        did: u64,
        tid: u64,
        op: &KvOp,
        undo_log: &mut Vec<UndoEntry>,
    ) -> Result<Response, ErrorCode> {
        match op {
            KvOp::Incr {
                collection,
                key,
                delta,
                ttl_ms,
                surrogate,
                rls_write_check,
            } => {
                let now_ms = current_ms();
                let prior = self
                    .kv_engine
                    .entry_image(did, tid, collection.as_str(), key, now_ms);
                let resp = self.execute_kv_incr(
                    crate::data::executor::handlers::kv::atomic::KvAtomicCtx {
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
                );
                if resp.status == Status::Error {
                    return Err(resp.error_code.map(|c| *c).unwrap_or(ErrorCode::Internal {
                        detail: "kv incr failed".into(),
                    }));
                }
                undo_log.push(UndoEntry::KvPut {
                    collection: collection.to_string(),
                    key: key.clone(),
                    prior,
                });
                Ok(resp)
            }

            KvOp::IncrFloat {
                collection,
                key,
                delta,
                surrogate,
                rls_write_check,
            } => {
                let now_ms = current_ms();
                let prior = self
                    .kv_engine
                    .entry_image(did, tid, collection.as_str(), key, now_ms);
                let resp = self.execute_kv_incr_float(
                    crate::data::executor::handlers::kv::atomic::KvAtomicCtx {
                        task,
                        did,
                        tid,
                        collection: collection.as_str(),
                        key,
                        surrogate: *surrogate,
                        rls_write_check,
                    },
                    *delta,
                );
                if resp.status == Status::Error {
                    return Err(resp.error_code.map(|c| *c).unwrap_or(ErrorCode::Internal {
                        detail: "kv incr float failed".into(),
                    }));
                }
                undo_log.push(UndoEntry::KvPut {
                    collection: collection.to_string(),
                    key: key.clone(),
                    prior,
                });
                Ok(resp)
            }

            KvOp::Cas {
                collection,
                key,
                expected,
                new_value,
                surrogate,
                rls_write_check,
            } => {
                let now_ms = current_ms();
                let prior = self
                    .kv_engine
                    .entry_image(did, tid, collection.as_str(), key, now_ms);
                let resp = self.execute_kv_cas(
                    crate::data::executor::handlers::kv::atomic::KvAtomicCtx {
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
                );
                if resp.status == Status::Error {
                    return Err(resp.error_code.map(|c| *c).unwrap_or(ErrorCode::Internal {
                        detail: "kv cas failed".into(),
                    }));
                }
                // CAS only mutates on success (which we verified above).
                undo_log.push(UndoEntry::KvPut {
                    collection: collection.to_string(),
                    key: key.clone(),
                    prior,
                });
                Ok(resp)
            }

            KvOp::GetSet {
                collection,
                key,
                new_value,
                surrogate,
                rls_filters,
                rls_write_check,
            } => {
                let now_ms = current_ms();
                let prior = self
                    .kv_engine
                    .entry_image(did, tid, collection.as_str(), key, now_ms);
                let resp = self.execute_kv_getset(
                    crate::data::executor::handlers::kv::atomic::KvAtomicCtx {
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
                );
                if resp.status == Status::Error {
                    return Err(resp.error_code.map(|c| *c).unwrap_or(ErrorCode::Internal {
                        detail: "kv get-set failed".into(),
                    }));
                }
                undo_log.push(UndoEntry::KvPut {
                    collection: collection.to_string(),
                    key: key.clone(),
                    prior,
                });
                Ok(resp)
            }

            // Routed here only by `execute_tx_kv_write`'s atomic arm.
            other => Err(ErrorCode::Internal {
                detail: format!("execute_tx_kv_atomic called with a non-atomic KvOp: {other:?}"),
            }),
        }
    }
}
