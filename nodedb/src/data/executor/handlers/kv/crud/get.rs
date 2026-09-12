// SPDX-License-Identifier: BUSL-1.1

//! KV point `GET` handler, including read-your-own-writes overlay lookup.

use tracing::debug;

use super::types::KvGetParams;
use crate::bridge::envelope::Response;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::transaction::overlay::{Staged, StagedTtl};
use crate::data::executor::handlers::transaction::stage_write::kv_row_identity;
use crate::data::executor::task::ExecutionTask;
use crate::engine::kv::current_ms;
use crate::types::TenantId;

impl CoreLoop {
    pub(in crate::data::executor) fn execute_kv_get(
        &self,
        task: &ExecutionTask,
        params: KvGetParams<'_>,
    ) -> Response {
        let KvGetParams {
            did,
            tid,
            collection,
            key,
            rls_filters,
            surrogate_ceiling,
        } = params;
        debug!(core = self.core_id, %collection, "kv get");

        // Read-your-own-writes: an in-transaction get consults this
        // transaction's staging overlay before falling back to the base KV
        // engine, keyed by the same hex-encoded identity the staging path
        // uses for KV rows.
        if let Some(txn_id) = task.request.txn_id {
            // Read-your-own-writes refreshes the lease (see the reaper).
            self.touch_overlay(txn_id);
            let coll_key = (
                task.request.database_id,
                TenantId::new(tid),
                collection.to_string(),
            );
            let doc_id = kv_row_identity(key);
            if let Some(overlay) = self.txn_overlays.get(&txn_id) {
                // A staged EXPIRE with an already-past instant makes the row
                // appear absent to a same-transaction read -- independent of
                // whether the row's VALUE was also staged this transaction
                // (an `Expire` on a base-only row stages only the TTL delta,
                // never a `Staged::Put`).
                if matches!(
                    overlay.get_ttl_by_doc_id(&coll_key, &doc_id),
                    Some(StagedTtl::ExpireAt(t)) if t <= current_ms()
                ) {
                    return self.response_with_payload(task, Vec::new());
                }
                if let Some(staged) = overlay.get_by_doc_id(&coll_key, &doc_id) {
                    return match staged {
                        Staged::Put(body) => {
                            if !crate::data::executor::handlers::rls_eval::rls_check_msgpack_bytes(
                                rls_filters,
                                body,
                            ) {
                                self.response_with_payload(task, Vec::new())
                            } else {
                                self.response_with_payload(task, body.clone())
                            }
                        }
                        Staged::Tombstone => self.response_with_payload(task, Vec::new()),
                    };
                }
            }
        }

        let now_ms = current_ms();
        let fetched = match surrogate_ceiling {
            Some(ceiling) => {
                // Clone-delegated read: drop the row when its binding was
                // allocated AFTER the clone's AS-OF surrogate ceiling.
                // `Surrogate::ZERO` means the entry was created via an
                // internal RMW path that did not bind an identity; treat
                // it as pre-clone (visible) — internal rows do not
                // originate from user post-clone writes.
                self.kv_engine
                    .get_with_surrogate(did, tid, collection, key, now_ms)
                    .and_then(|(value, surrogate)| {
                        let s = surrogate.as_u32();
                        if s != 0 && s > ceiling {
                            None
                        } else {
                            Some(value)
                        }
                    })
            }
            None => self.kv_engine.get(did, tid, collection, key, now_ms),
        };
        match fetched {
            Some(value) => {
                // RLS post-fetch: evaluate filters against KV value.
                if !crate::data::executor::handlers::rls_eval::rls_check_msgpack_bytes(
                    rls_filters,
                    &value,
                ) {
                    return self.response_with_payload(task, Vec::new());
                }
                if let Some(ref m) = self.metrics {
                    m.record_kv_get();
                }
                // Value is already standard msgpack — pass through directly.
                self.response_with_payload(task, value)
            }
            None => self.response_with_payload(task, Vec::new()),
        }
    }
}
