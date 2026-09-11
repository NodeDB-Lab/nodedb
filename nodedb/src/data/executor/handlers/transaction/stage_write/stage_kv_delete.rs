// SPDX-License-Identifier: BUSL-1.1

//! Statement-time staging for the KV `Delete`.
//!
//! Split out of `stage_kv.rs` to keep that file under the per-file line
//! budget, the same way `stage_kv_atomic.rs` / `stage_kv_transfer.rs` /
//! `stage_kv_ttl.rs` were.
//!
//! A staged delete does two things a staged put does not: it resolves the
//! surrogate of the row it is tombstoning (from the overlay's own doc-id
//! binding when this transaction already staged the row, otherwise from the
//! base engine's key→surrogate map, so the tombstone lands on the same row the
//! COMMIT-time replay will remove), and it decides that row against the
//! compiled RLS write predicate. The row being removed is the only image a
//! delete has, and it is resolved under BASE ∪ OVERLAY so a row this
//! transaction already changed is judged as it now stands rather than as it was
//! before the transaction began.

use nodedb_types::Surrogate;

use crate::bridge::envelope::Response;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::transaction::overlay::Staged;
use crate::data::executor::task::ExecutionTask;
use crate::engine::kv::current_ms;
use crate::types::TxnId;

use super::stage_kv::hex_key;

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
            let doc_id = hex_key(key);
            let coll_key = (
                did,
                crate::types::TenantId::new(tid),
                collection.to_string(),
            );

            let overlay_staged = self
                .txn_overlays
                .get(&txn_id)
                .and_then(|o| o.get_by_doc_id(&coll_key, &doc_id))
                .cloned();

            let (surrogate, present) = match overlay_staged {
                // A staged put exists: resolve its bound surrogate through
                // the overlay's own doc_id -> surrogate map so the
                // tombstone lands on the same row.
                Some(Staged::Put(_)) => {
                    let s = self
                        .txn_overlays
                        .get(&txn_id)
                        .and_then(|o| o.surrogate_for_doc_id(&coll_key, &doc_id))
                        .unwrap_or(0);
                    (Surrogate::new(s), true)
                }
                // Already staged-deleted in this transaction: absent,
                // matching PostgreSQL/Document DELETE semantics for a
                // missing key (DELETE 0, not an error).
                Some(Staged::Tombstone) => (Surrogate::ZERO, false),
                // Nothing staged: resolve via the base KV engine's own
                // key -> surrogate binding.
                None => {
                    let now_ms = current_ms();
                    match self.kv_engine.get_with_surrogate(
                        did.as_u64(),
                        tid,
                        collection,
                        key,
                        now_ms,
                    ) {
                        Some((_, s)) => (s, true),
                        None => (Surrogate::ZERO, false),
                    }
                }
            };

            if !present {
                continue;
            }

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
                        let now_ms = current_ms();
                        self.kv_engine
                            .get(did.as_u64(), tid, collection, key, now_ms)
                    }
                };
                if let Some(body) = current
                    && let Err(e) = self.stage_admit_write(
                        rls_write_check,
                        &body,
                        &crate::engine::document::store::RowIdentity::from_user_key(
                            doc_id.as_str(),
                        ),
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
