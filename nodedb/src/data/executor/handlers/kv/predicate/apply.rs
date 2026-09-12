// SPDX-License-Identifier: BUSL-1.1

//! Live handlers for `KvOp::PredicateUpdate` / `KvOp::PredicateDelete`. Both
//! resolve their row set via [`CoreLoop::kv_predicate_matches`] and reuse the
//! keyed path verbatim — re-deriving either here is how a predicate write
//! would drift from the keyed one.

use nodedb_types::{RlsWriteCheck, Surrogate};
use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::kv::field_compute::merge_field_updates;
use crate::data::executor::handlers::kv::rls::admit_kv_row;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;
use crate::engine::kv::{KvPutParams, current_ms};

/// Routing and policy inputs shared by both predicate handlers.
pub(in crate::data::executor) struct KvPredicateCtx<'a> {
    pub did: u64,
    pub tid: u64,
    pub collection: &'a str,
    pub filters: &'a [u8],
    pub rls_write_check: &'a RlsWriteCheck,
}

impl CoreLoop {
    /// Merge `updates` into every row the predicate matches. Every post-image
    /// is decided against the write policy before the first row is written,
    /// so one rejected row leaves the whole statement without effect.
    pub(in crate::data::executor) fn execute_kv_predicate_update(
        &mut self,
        task: &ExecutionTask,
        ctx: KvPredicateCtx<'_>,
        updates: &[(String, Vec<u8>)],
    ) -> Response {
        let KvPredicateCtx {
            did,
            tid,
            collection,
            filters,
            rls_write_check,
        } = ctx;
        debug!(core = self.core_id, %collection, "kv predicate update");
        let now_ms = current_ms();

        let matched = match self.kv_predicate_matches(did, tid, collection, filters, now_ms) {
            Ok(rows) => rows,
            Err(e) => return self.response_error(task, e),
        };

        let mut writes: Vec<(Vec<u8>, Vec<u8>, Vec<u8>)> = Vec::with_capacity(matched.len());
        for (key, body) in matched {
            let computed = match merge_field_updates(Some(body.as_slice()), updates) {
                Ok(c) => c,
                Err(e) => return self.response_error(task, e),
            };
            if let Err(e) =
                admit_kv_row(rls_write_check, &computed.new_value, &key, tid, collection)
            {
                return self.response_error(task, e);
            }
            writes.push((key, body, computed.new_value));
        }

        for (key, old_body, new_value) in &writes {
            // `Surrogate::ZERO` leaves the row's existing bound identity alone.
            self.kv_engine.put(KvPutParams {
                database_id: did,
                tenant_id: tid,
                collection,
                key,
                value: new_value,
                // Mirrors `execute_kv_field_set`, whose keyed merge this is the
                // predicate form of: the merge writes no TTL of its own.
                ttl_ms: 0,
                now_ms,
                surrogate: Surrogate::ZERO,
            });
            if let Some(ref m) = self.metrics {
                m.record_kv_put();
            }
            let key_str = String::from_utf8_lossy(key);
            self.emit_write_event(
                task,
                collection,
                crate::event::WriteOp::Update,
                crate::engine::document::store::RowIdentity::from_user_key(key_str.as_ref()),
                Some(new_value),
                Some(old_body),
            );
            self.note_kv_write_lsn(task, did, tid, collection, key);
        }

        match response_codec::encode_count("affected", writes.len()) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }

    /// Delete every row the predicate matches. Resolved keys go through
    /// `execute_kv_delete`, so the gate, metrics, and reply are the keyed
    /// delete's, not a second copy.
    pub(in crate::data::executor) fn execute_kv_predicate_delete(
        &mut self,
        task: &ExecutionTask,
        ctx: KvPredicateCtx<'_>,
    ) -> Response {
        let KvPredicateCtx {
            did,
            tid,
            collection,
            filters,
            rls_write_check,
        } = ctx;
        debug!(core = self.core_id, %collection, "kv predicate delete");
        let now_ms = current_ms();

        let matched = match self.kv_predicate_matches(did, tid, collection, filters, now_ms) {
            Ok(rows) => rows,
            Err(e) => return self.response_error(task, e),
        };
        let keys: Vec<Vec<u8>> = matched.into_iter().map(|(key, _body)| key).collect();
        self.execute_kv_delete(task, did, tid, collection, &keys, rls_write_check)
    }
}
