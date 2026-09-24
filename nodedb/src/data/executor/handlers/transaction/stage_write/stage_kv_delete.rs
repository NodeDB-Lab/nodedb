// SPDX-License-Identifier: BUSL-1.1

//! Statement-time staging for the KV `Delete`.
//!
//! Split out of `stage_kv.rs` to keep that file under the per-file line
//! budget, the same way `stage_kv_atomic.rs` / `stage_kv_transfer.rs` /
//! `stage_kv_ttl.rs` were.
//!
//! A staged delete does two things a staged put does not: it resolves the
//! surrogate of the row it is tombstoning (via
//! [`CoreLoop::resolve_kv_stage_surrogate`], so the tombstone lands on the
//! same row the COMMIT-time replay will remove), and it decides that row
//! against the compiled RLS write predicate. The row being removed is the only image a
//! delete has, and it is resolved under BASE ∪ OVERLAY so a row this
//! transaction already changed is judged as it now stands rather than as it was
//! before the transaction began.

use crate::bridge::envelope::Response;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::transaction::overlay::Staged;
use crate::data::executor::task::ExecutionTask;
use crate::types::TxnId;

use super::stage_kv::kv_row_identity;

impl CoreLoop {
    /// Tombstone every present key in the overlay, after deciding each row it
    /// removes against the compiled write policy.
    pub(super) fn stage_kv_delete(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        txn_id: TxnId,
        collection: &str,
        keys: &[Vec<u8>],
        rls_write_check: &nodedb_types::RlsWriteCheck,
    ) -> Response {
        let did = task.request.database_id;
        let mut deleted = 0usize;
        for key in keys {
            let doc_id = kv_row_identity(key);
            let coll_key = (
                did,
                crate::types::TenantId::new(tid),
                collection.to_string(),
            );

            // An absent key (never present, already staged-deleted, or hidden
            // by a staged TRUNCATE) matches PostgreSQL/Document DELETE
            // semantics for a missing key: DELETE 0, not an error.
            let Some(surrogate) = self.resolve_kv_stage_surrogate(txn_id, &coll_key, key) else {
                continue;
            };

            // The row being removed is the image the write policy decides, and
            // it is resolved under BASE ∪ OVERLAY so a row staged earlier in
            // this transaction is judged as it now stands, not as it was.
            if !matches!(
                rls_write_check.decision(),
                nodedb_types::WriteGateDecision::AdmitAll
            ) {
                let current = match self
                    .txn_overlays
                    .get(&txn_id)
                    .and_then(|o| o.get_by_doc_id(&coll_key, &doc_id))
                {
                    Some(Staged::Put(body)) => Some(body.clone()),
                    Some(Staged::Tombstone) => None,
                    None => {
                        let now_ms = self.kv_read_now_ms();
                        self.kv_engine
                            .get(did.as_u64(), tid, collection, key, now_ms)
                    }
                };
                if let Some(body) = current
                    && let Err(e) = self.stage_admit_write(
                        rls_write_check,
                        &body,
                        &doc_id,
                        did.as_u64(),
                        tid,
                        collection,
                    )
                {
                    return self.response_error(task, e);
                }
            }

            self.txn_overlay_mut(txn_id)
                .insert_tombstone(coll_key, surrogate.0, &doc_id);
            deleted += 1;
        }
        self.stage_count_response(task, deleted)
    }
}
