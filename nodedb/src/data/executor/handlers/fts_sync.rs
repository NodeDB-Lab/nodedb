// SPDX-License-Identifier: BUSL-1.1

//! FTS sync ingest handlers: index/delete documents through the idempotency gate.
//!
//! Called by `dispatch_text` when the plan variant is
//! `TextOp::FtsIndexDoc` or `TextOp::FtsDeleteDoc` and the op carries
//! a `SyncProvenance`.
//!
//! Without provenance (local non-sync path) the handlers apply directly
//! as before — no gate overhead.

use tracing::warn;

use crate::bridge::envelope::Response;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::sync_gate::{SyncAdmit, ack_status_from_admit};
use crate::data::executor::task::ExecutionTask;
use nodedb_types::Surrogate;
use nodedb_types::sync::wire::{AckStatus, SyncProvenance};

impl CoreLoop {
    /// Index a document's `(field, text)` pairs into the inverted BM25
    /// indexes (whole-document and per field), optionally gating on the
    /// `SyncProvenance` for idempotent replay. Empty `fields` remove the
    /// document from every index.
    ///
    /// Without provenance behaves identically to the pre-gate implementation.
    /// With provenance: runs the idempotency gate (`sync_admit`) before
    /// writing; on `Apply` commits the HWM after the engine write succeeds;
    /// returns a msgpack-encoded `SyncAckResult` in `Response.payload`.
    pub(in crate::data::executor) fn execute_fts_index_doc(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
        surrogate: Surrogate,
        fields: &[(String, String)],
        provenance: Option<&SyncProvenance>,
    ) -> Response {
        if let Some(refusal) =
            super::unbound_surrogate::refuse_unbound("fts", collection, surrogate)
        {
            return self.response_error(task, refusal);
        }
        // ── Idempotency gate ────────────────────────────────────────────────
        if let Some(prov) = provenance {
            match self.sync_admit(prov) {
                SyncAdmit::Apply => {
                    // Fall through to engine write below.
                }
                admit @ (SyncAdmit::Duplicate | SyncAdmit::Fenced | SyncAdmit::Gap { .. }) => {
                    let applied_seq = self.sync_hwm_value(prov.producer_id, prov.stream_id);
                    return self.sync_ack_response(
                        task,
                        ack_status_from_admit(&admit),
                        applied_seq,
                    );
                }
            }
        }

        // ── Engine write ────────────────────────────────────────────────────
        let tenant_id = nodedb_types::TenantId::new(tid);
        let database_id = task.request.database_id.as_u64();
        let text = nodedb_fts::DocumentText::from_fields(fields.iter().cloned());
        match self
            .inverted
            .index_document(database_id, tenant_id, collection, surrogate, &text)
        {
            Ok(()) => {
                // Advance the collection floor for this committed FTS write.
                self.note_collection_write_lsn(task, collection);
                if let Some(prov) = provenance {
                    self.sync_commit(prov);
                    return self.sync_ack_response(task, AckStatus::Applied, prov.seq);
                }
                self.response_ok(task)
            }
            Err(e) => {
                warn!(
                    core = self.core_id,
                    %collection,
                    surrogate = surrogate.as_u32(),
                    error = %e,
                    "FtsIndexDoc: inverted index write failed"
                );
                self.response_error(task, e)
            }
        }
    }

    /// Remove a document from the inverted BM25 index, optionally gating on
    /// `SyncProvenance` for idempotent replay. `None` names a key its home
    /// never bound: the delete removes nothing and still commits the
    /// producer's sequence.
    pub(in crate::data::executor) fn execute_fts_delete_doc(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
        surrogate: Option<Surrogate>,
        provenance: Option<&SyncProvenance>,
    ) -> Response {
        // ── Idempotency gate ────────────────────────────────────────────────
        if let Some(prov) = provenance {
            match self.sync_admit(prov) {
                SyncAdmit::Apply => {}
                admit @ (SyncAdmit::Duplicate | SyncAdmit::Fenced | SyncAdmit::Gap { .. }) => {
                    let applied_seq = self.sync_hwm_value(prov.producer_id, prov.stream_id);
                    return self.sync_ack_response(
                        task,
                        ack_status_from_admit(&admit),
                        applied_seq,
                    );
                }
            }
        }

        let Some(surrogate) = surrogate else {
            if let Some(prov) = provenance {
                self.sync_commit(prov);
                return self.sync_ack_response(task, AckStatus::Applied, prov.seq);
            }
            return self.response_ok(task);
        };

        // ── Engine write ────────────────────────────────────────────────────
        let tenant_id = nodedb_types::TenantId::new(tid);
        let database_id = task.request.database_id.as_u64();
        match self
            .inverted
            .remove_document(database_id, tenant_id, collection, surrogate)
        {
            Ok(()) => {
                // Advance the collection floor for this committed FTS delete.
                self.note_collection_write_lsn(task, collection);
                if let Some(prov) = provenance {
                    self.sync_commit(prov);
                    return self.sync_ack_response(task, AckStatus::Applied, prov.seq);
                }
                self.response_ok(task)
            }
            Err(e) => {
                warn!(
                    core = self.core_id,
                    %collection,
                    surrogate = surrogate.as_u32(),
                    error = %e,
                    "FtsDeleteDoc: inverted index removal failed"
                );
                self.response_error(task, e)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::sync::wire::{SyncAckResult, SyncOutcome};

    use super::*;
    use crate::bridge::envelope::Status;
    use crate::data::executor::core_loop::tests::{make_core_with_dir, make_default_task};

    const TID: u64 = 1;

    fn prov(seq: u64) -> SyncProvenance {
        SyncProvenance {
            producer_id: 7,
            epoch: 1,
            stream_id: 42,
            seq,
        }
    }

    fn assert_applied(response: &Response, seq: u64) {
        assert_eq!(response.status, Status::Ok);
        let ack: SyncAckResult = zerompk::from_msgpack(&response.payload).expect("sync ack");
        assert_eq!(ack.outcome, SyncOutcome::Ack(AckStatus::Applied));
        assert_eq!(ack.applied_seq, seq);
    }

    /// A message body whose only string field is `body`.
    fn body(text: &str) -> Vec<(String, String)> {
        vec![("body".to_string(), text.to_string())]
    }

    /// Two documents under bound surrogates.
    fn index_documents(core: &mut CoreLoop, task: &ExecutionTask) {
        for surrogate in [Surrogate::new(5), Surrogate::new(6)] {
            let response = core.execute_fts_index_doc(
                task,
                TID,
                "notes",
                surrogate,
                &body("hello world"),
                None,
            );
            assert_eq!(response.status, Status::Ok);
        }
    }

    /// Ids of `notes` documents matching `query` in one index.
    fn hits(core: &CoreLoop, index: nodedb_fts::IndexScope<'_>, query: &str) -> Vec<u32> {
        let mut ids: Vec<u32> = core
            .inverted
            .search(
                nodedb_types::DatabaseId::DEFAULT.as_u64(),
                nodedb_types::TenantId::new(TID),
                index,
                nodedb_fts::FtsSearchParams {
                    query,
                    top_k: 10,
                    fuzzy_enabled: false,
                    mode: nodedb_fts::QueryMode::And,
                    prefilter: None,
                },
            )
            .expect("search")
            .into_iter()
            .map(|r| r.doc_id.as_u32())
            .collect();
        ids.sort_unstable();
        ids
    }

    /// A synced document is searchable per field, and a later message with no
    /// fields removes it from every index.
    #[test]
    fn synced_fields_index_per_field_and_empty_fields_remove() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        let task = make_default_task();
        let title = nodedb_fts::IndexScope::field("notes", "title").expect("field scope");
        let body_index = nodedb_fts::IndexScope::field("notes", "body").expect("field scope");
        let doc = vec![
            ("title".to_string(), "rust".to_string()),
            ("body".to_string(), "guide".to_string()),
        ];
        let response =
            core.execute_fts_index_doc(&task, TID, "notes", Surrogate::new(5), &doc, None);
        assert_eq!(response.status, Status::Ok);
        let response =
            core.execute_fts_index_doc(&task, TID, "notes", Surrogate::new(6), &body("rust"), None);
        assert_eq!(response.status, Status::Ok);

        assert_eq!(hits(&core, title, "rust"), vec![5]);
        assert_eq!(hits(&core, body_index, "rust"), vec![6]);
        assert_eq!(hits(&core, "notes".into(), "rust"), vec![5, 6]);

        let response =
            core.execute_fts_index_doc(&task, TID, "notes", Surrogate::new(5), &[], Some(&prov(1)));
        assert_applied(&response, 1);
        assert!(hits(&core, title, "rust").is_empty());
        assert!(hits(&core, body_index, "guide").is_empty());
        assert_eq!(hits(&core, "notes".into(), "rust"), vec![6]);
        assert_eq!(doc_count(&core), 1);
    }

    /// The indexed document count of `notes`.
    fn doc_count(core: &CoreLoop) -> u32 {
        core.inverted
            .corpus_stats(
                nodedb_types::DatabaseId::DEFAULT.as_u64(),
                nodedb_types::TenantId::new(TID),
                "notes",
            )
            .expect("corpus stats")
            .0
    }

    #[test]
    fn an_index_without_a_surrogate_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        let task = make_default_task();

        let response =
            core.execute_fts_index_doc(&task, TID, "notes", Surrogate::ZERO, &body("hello"), None);
        assert!(matches!(
            response.error_code.as_deref(),
            Some(crate::bridge::envelope::ErrorCode::RejectedPrevalidation { .. })
        ));
        assert_eq!(doc_count(&core), 0, "nothing is indexed");
    }

    #[test]
    fn unbound_delete_with_provenance_commits_the_sequence_and_removes_nothing() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        let task = make_default_task();
        index_documents(&mut core, &task);
        let before = doc_count(&core);
        assert_eq!(before, 2);

        let response = core.execute_fts_delete_doc(&task, TID, "notes", None, Some(&prov(1)));
        assert_applied(&response, 1);
        assert_eq!(core.sync_hwm_value(7, 42), 1, "the sequence commits");
        assert_eq!(doc_count(&core), before, "no document is removed");
    }

    #[test]
    fn unbound_delete_without_provenance_is_ok_and_removes_nothing() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        let task = make_default_task();
        index_documents(&mut core, &task);

        let response = core.execute_fts_delete_doc(&task, TID, "notes", None, None);
        assert_eq!(response.status, Status::Ok);
        assert!(response.payload.is_empty());
        assert_eq!(doc_count(&core), 2);
    }
}
