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
    /// `SyncProvenance` for idempotent replay. Only the collection's declared
    /// text fields are kept. No declared field left in `fields` removes the
    /// document from every index. A collection that declares no text field
    /// indexes nothing.
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
        let frame = SyncedFrame {
            collection,
            surrogate,
            fields,
            sys_from: None,
        };
        self.execute_fts_index_doc_at(task, tid, frame, provenance)
    }

    /// [`Self::execute_fts_index_doc`] for `frame`. A frame with a
    /// `sys_from` installs at that ordinal through
    /// [`Self::install_synced_text`].
    pub(in crate::data::executor) fn execute_fts_index_doc_at(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        frame: SyncedFrame<'_>,
        provenance: Option<&SyncProvenance>,
    ) -> Response {
        let SyncedFrame {
            collection,
            surrogate,
            fields,
            sys_from,
        } = frame;
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

        if let Some(refusal) =
            super::unbound_surrogate::refuse_unbound("fts", collection, surrogate)
        {
            if let Some(prov) = provenance.filter(|prov| prov.producer_id != 0) {
                return self.sync_reject_response(
                    task,
                    nodedb_types::sync::violation::ViolationType::SchemaViolation {
                        field: "doc_id".to_owned(),
                        reason: crate::error_from_data_plane::data_plane_code_to_public(refusal)
                            .to_string(),
                    },
                    prov,
                );
            }
            return self.response_error(task, refusal);
        }

        // ── Engine write ────────────────────────────────────────────────────
        let database_id = task.request.database_id.as_u64();
        let written = match sys_from {
            // The prior text version ends at the write's system time.
            None => {
                let base = self.bitemporal_ordinal();
                self.write_synced_text(database_id, tid, collection, surrogate, fields, base)
            }
            Some(ordinal) => {
                self.install_synced_text(database_id, tid, collection, surrogate, fields, ordinal)
            }
        };
        match written {
            Ok(()) => {
                // Note this committed FTS write.
                if let Err(e) = self.note_collection_write(task, collection) {
                    return self.response_error(task, e);
                }
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
        self.execute_fts_delete_doc_at(task, tid, collection, surrogate, provenance, None)
    }

    /// [`Self::execute_fts_delete_doc`] at `sys_from`. A delete with a
    /// `sys_from` installs its tombstone version at that ordinal through
    /// [`Self::install_synced_text`].
    pub(in crate::data::executor) fn execute_fts_delete_doc_at(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
        surrogate: Option<Surrogate>,
        provenance: Option<&SyncProvenance>,
        sys_from: Option<i64>,
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
        let database_id = task.request.database_id.as_u64();
        let written = match sys_from {
            // The text version ends at the delete's system time.
            None => {
                let base = self.bitemporal_ordinal();
                self.write_synced_text(database_id, tid, collection, surrogate, &[], base)
            }
            Some(ordinal) => {
                self.install_synced_text(database_id, tid, collection, surrogate, &[], ordinal)
            }
        };
        match written {
            Ok(()) => {
                // Note this committed FTS delete.
                if let Err(e) = self.note_collection_write(task, collection) {
                    return self.response_error(task, e);
                }
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

impl CoreLoop {
    /// Store a live frame's `fields` as the document's newest synced-text
    /// version and index the collection's declared fields of it, in one
    /// write transaction. Empty `fields` store a tombstone version and end
    /// the document's text.
    ///
    /// The version stamps `base`, raised one above the row's newest stored
    /// frame when that frame sits at or above `base`. A document version
    /// stamps by the same rule (`KeyStamps`). So two frames of one row
    /// under one Calvin ordinal store two versions, the second newest. A
    /// frame below a restored head stores above it. The live frame is
    /// always the row's newest version, so it holds the current text. The
    /// stamp folds into the shared clock.
    ///
    /// The frame indexes exactly what a local write would: the declared
    /// text fields. A collection that declares none indexes nothing, and
    /// still stores the frame. A full-text index created later rebuilds the
    /// document's text history from the stored versions when the document
    /// store holds no body.
    pub(in crate::data::executor) fn write_synced_text(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        surrogate: Surrogate,
        fields: &[(String, String)],
        base: i64,
    ) -> crate::Result<()> {
        let mut doc = crate::engine::sparse::inverted::IndexDocScope {
            database_id,
            tid: nodedb_types::TenantId::new(tid),
            collection,
            surrogate,
            ordinal: base,
        };
        let txn = self.sparse.begin_write()?;
        doc.ordinal = match self.inverted.synced_text_head_in_txn(&txn, doc)? {
            Some(head) if head.ordinal >= base => head.ordinal.checked_add(1).ok_or_else(|| {
                crate::Error::NumericValueOutOfRange {
                    detail: format!(
                        "synced text version ordinal for '{collection}' cannot advance beyond {}",
                        head.ordinal
                    ),
                }
            })?,
            _ => base,
        };
        self.inverted.put_synced_text_in_txn(&txn, doc, fields)?;
        self.replay_frame_text(&txn, doc, fields)?;
        txn.commit().map_err(|e| crate::Error::Storage {
            engine: "inverted".into(),
            detail: format!("synced text commit ({collection}): {e}"),
        })?;
        self.observe_bitemporal_ordinal(doc.ordinal);
        Ok(())
    }

    /// Store a frame's `fields` as the document's synced-text version at
    /// its source `ordinal`, as a RESTORE re-issues it. The frame indexes
    /// its declared fields only when the document store holds no version
    /// of the row. A row with a stored body takes its text from the body,
    /// as a text-index rebuild and a snapshot restore read it. A frame
    /// indexed at an older ordinal than the body would end the body's
    /// text version before it starts. A frame below the row's newest
    /// stored frame is only stored: the newer frame holds the current text.
    pub(in crate::data::executor) fn install_synced_text(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        surrogate: Surrogate,
        fields: &[(String, String)],
        ordinal: i64,
    ) -> crate::Result<()> {
        let doc = crate::engine::sparse::inverted::IndexDocScope {
            database_id,
            tid: nodedb_types::TenantId::new(tid),
            collection,
            surrogate,
            ordinal,
        };
        let bodied = self
            .sparse
            .versioned_latest_sys_from(
                database_id,
                tid,
                collection,
                &nodedb_types::StorageKey::for_surrogate(surrogate),
            )?
            .is_some();
        let txn = self.sparse.begin_write()?;
        // A version below the row's newest stored frame is history only.
        // The newer frame set the current text, as for a document version.
        let below = self
            .inverted
            .synced_text_head_in_txn(&txn, doc)?
            .is_some_and(|head| ordinal < head.ordinal);
        self.inverted.put_synced_text_in_txn(&txn, doc, fields)?;
        if !bodied && !below {
            self.replay_frame_text(&txn, doc, fields)?;
        }
        txn.commit().map_err(|e| crate::Error::Storage {
            engine: "inverted".into(),
            detail: format!("synced text install commit ({collection}): {e}"),
        })
    }
}

/// One sync frame an `FtsIndex` write carries.
pub(in crate::data::executor) struct SyncedFrame<'a> {
    /// The collection as the Data Plane stores it.
    pub collection: &'a str,
    pub surrogate: Surrogate,
    /// Every `(field, text)` pair the frame carried.
    pub fields: &'a [(String, String)],
    /// The source ordinal of a re-issued frame version. `None` for a live
    /// frame, which stamps above the row's newest stored frame.
    pub sys_from: Option<i64>,
}

#[cfg(test)]
#[path = "fts_sync_tests.rs"]
mod tests;
