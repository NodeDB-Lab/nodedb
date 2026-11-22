// SPDX-License-Identifier: BUSL-1.1

//! WAL replay for FTS engine startup recovery.
//!
//! Called once during startup, after `open()` but before the event loop.
//! Processes `FtsIndex` and `FtsDelete` records, routing each through the
//! same apply handler (`execute_fts_index_doc` / `execute_fts_delete_doc`)
//! that the live sync path uses so the idempotency gate fires on replay.
//!
//! ## Surrogate re-derivation on replay
//!
//! The WAL payload stores the document key as the hex-encoded surrogate
//! string produced by `StorageKey`'s `Display` impl (format `{:08x}`).
//! On replay we parse it back via `u32::from_str_radix(&doc_id, 16)` —
//! the same conversion used by the scan / prefilter paths.  This does not
//! require a catalog or surrogate-assigner round-trip: the 8-hex-char key
//! is already the stable `u32` surrogate identity.
//!
//! ## Why there is no replay floor or watermark here
//!
//! Every retained `FtsIndex` / `FtsDelete` record is fed back through the apply
//! handlers on every boot, including records a restored FTS state already
//! contains. That is safe because BOTH handlers are total overwrites keyed by
//! the surrogate, in all three tables the Origin index reads:
//!
//! * POSTINGS — each row is keyed by `(term, surrogate)`, so
//!   `write_index_data` overwrites the document's one row per term and a
//!   term's list cannot grow a duplicate however many times the record is
//!   applied.
//! * DOC_LENGTHS — the surrogate's entry is overwritten with the same length.
//! * STATS — the counters are the part that a keyed row overwrite does NOT
//!   make idempotent on its own, so `write_index_data` derives the deltas from
//!   the PRIOR DOC_LENGTHS entry read in the same write transaction: an
//!   already-counted surrogate re-indexes with `count_delta = 0` and a
//!   `total_delta` of (new length − prior length), which is zero for an
//!   unchanged replay. `remove_document` symmetrically decrements only when a
//!   prior length existed, so a repeated delete subtracts nothing.
//!
//! The Origin search path reads posting lists from POSTINGS alone
//! (`RedbFtsBackend::read_postings`) and these writes bypass the FTS memtable
//! entirely, so there is no second copy of a posting that a re-apply could
//! diverge from. The tests at the bottom of this file pin all of that.

use crate::bridge::envelope::{PhysicalPlan, Priority, Request};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::{ExecutionTask, TaskState};
use crate::types::{DatabaseId, ReadConsistency};
use nodedb_physical::physical_plan::TextOp;
use nodedb_types::Surrogate;
use nodedb_wal::record::RecordType;

impl CoreLoop {
    /// Build a synthetic `ExecutionTask` for FTS WAL replay.
    fn replay_fts_task(
        tenant_id: crate::types::TenantId,
        database_id: DatabaseId,
        vshard_id: crate::types::VShardId,
        plan: PhysicalPlan,
    ) -> ExecutionTask {
        ExecutionTask {
            request: Request {
                request_id: crate::types::RequestId::new(0),
                tenant_id,
                database_id,
                vshard_id,
                // A replay orders against no client connection.
                stream: crate::bridge::envelope::RequestStream::Internal,
                plan,
                deadline: std::time::Instant::now()
                    + crate::data::executor::deadline::REPLAY_DEADLINE,
                priority: Priority::Normal,
                trace_id: crate::types::TraceId::ZERO,
                consistency: ReadConsistency::Strong,
                idempotency_key: None,
                event_source: crate::event::EventSource::User,
                user_roles: Vec::new(),
                user_id: None,
                statement_digest: None,
                txn_id: None,
                wal_lsn: None,
                resolved_now_ms: None,
                commit_hlc: None,
                system_ordinal: None,
                config_pins: None,
                admission: crate::bridge::envelope::Admission::Exempt(
                    crate::bridge::envelope::ExemptReason::AlreadyOrdered,
                ),
            },
            state: TaskState::Running,
            wal_lsn: None,
            resolved_now_ms: None,
        }
    }

    /// Replay WAL FTS records to rebuild in-memory inverted indexes after crash.
    ///
    /// Processes `FtsIndex` and `FtsDelete` records in LSN order. Each record
    /// is decoded and routed through the apply handler so the idempotency gate
    /// runs on replay exactly as it does on the live ingest path.
    pub(crate) fn replay_fts_wal(
        &mut self,
        records: &[nodedb_wal::WalRecord],
        num_cores: usize,
        tombstones: &nodedb_wal::TombstoneSet,
        grouped_origins: &crate::wal::redo::replay_origin::GroupedReplayOrigins,
    ) {
        use nodedb_wal::record::{FtsDeletePayload, FtsIndexPayload};

        let mut indexed = 0usize;
        let mut deleted = 0usize;
        let mut skipped = 0usize;

        for record in records {
            if self.replay_halted() {
                break;
            }
            let logical_type = record.logical_record_type();
            let record_type = RecordType::from_raw(logical_type);

            let is_fts_index = record_type == Some(RecordType::FtsIndex);
            let is_fts_delete = record_type == Some(RecordType::FtsDelete);
            if !is_fts_index && !is_fts_delete {
                continue;
            }

            let vshard_id = record.header.vshard_id as usize;
            let target_core = if num_cores > 0 {
                vshard_id % num_cores
            } else {
                0
            };
            if target_core != self.core_id {
                skipped += 1;
                continue;
            }

            let tenant_id = record.header.tenant_id;
            let record_lsn = record.header.lsn;
            // Replayed writes land under the database recorded in the WAL header.
            let database_id = DatabaseId::new(record.header.database_id);

            if is_fts_index {
                let payload = match FtsIndexPayload::from_bytes(&record.payload) {
                    Ok(p) => p,
                    Err(e) => {
                        self.replay_record_unapplied(
                            "fts",
                            "decode_index",
                            record_lsn,
                            &format!("FtsIndexPayload could not be decoded: {e}"),
                        );
                        skipped += 1;
                        continue;
                    }
                };

                if tombstones.is_tombstoned(
                    database_id.as_u64(),
                    tenant_id,
                    &payload.collection,
                    record_lsn,
                ) {
                    skipped += 1;
                    continue;
                }

                // Re-derive surrogate from the hex doc_id stored in the WAL.
                let surrogate = match u32::from_str_radix(&payload.doc_id, 16) {
                    Ok(raw) => Surrogate::new(raw),
                    Err(e) => {
                        self.replay_record_unapplied(
                            "fts",
                            "doc_id",
                            record_lsn,
                            &format!(
                                "doc_id '{}' is not the hex surrogate the index path writes: {e}",
                                payload.doc_id
                            ),
                        );
                        skipped += 1;
                        continue;
                    }
                };

                let prov = payload.provenance;
                if self.claim_for_validation() {
                    continue;
                }
                if self.recording_redo_undo() {
                    let captured = self.capture_fts_doc_undo(
                        database_id,
                        tenant_id,
                        &payload.collection,
                        surrogate,
                        payload.sys_from,
                        Some(&prov),
                    );
                    if !self.record_redo_capture(captured) {
                        skipped += 1;
                        continue;
                    }
                }

                let Some(vshard) =
                    self.replay_vshard("fts", record_lsn, database_id, &payload.collection)
                else {
                    skipped += 1;
                    continue;
                };
                let Some(qualified) = self.replay_qualified(
                    "fts",
                    record_lsn,
                    database_id,
                    payload.collection.clone(),
                ) else {
                    skipped += 1;
                    continue;
                };
                let task = Self::replay_fts_task(
                    nodedb_types::TenantId::new(tenant_id),
                    database_id,
                    vshard,
                    PhysicalPlan::Text(TextOp::FtsIndexDoc {
                        collection: qualified,
                        surrogate,
                        fields: payload.fields.clone(),
                        provenance: Some(prov.clone()),
                    }),
                );

                let prior_hwm = (grouped_origins.contains(record_lsn)
                    && !self.applying_committed_redo())
                .then(|| self.capture_sync_hwm_undo(Some(&prov)))
                .flatten();

                // A frame version a RESTORE re-issues installs at its source
                // ordinal. A live frame stamps the apply's own ordinal.
                let response = self.execute_fts_index_doc_at(
                    &task,
                    tenant_id,
                    crate::data::executor::handlers::fts_sync::SyncedFrame {
                        collection: &payload.collection,
                        surrogate,
                        fields: &payload.fields,
                        sys_from: payload.sys_from,
                    },
                    Some(&prov),
                );

                if response.status != crate::bridge::envelope::Status::Ok {
                    self.restore_grouped_replay_sync_hwm(
                        prior_hwm,
                        database_id.as_u64(),
                        tenant_id,
                        record_lsn,
                    );
                    // Refused sync input can survive a crash before its abort marker.
                    if !grouped_origins.contains(record_lsn)
                        && !self.applying_committed_redo()
                        && matches!(
                            response.error_code.as_deref(),
                            Some(crate::bridge::envelope::ErrorCode::SyncRejected {
                                provenance: rejected, applied_seq, ..
                            }) if &prov == rejected && rejected.producer_id != 0 && *applied_seq >= rejected.seq
                        )
                    {
                        skipped += 1;
                        continue;
                    }
                    self.replay_record_unapplied(
                        "fts",
                        "index_handler",
                        record_lsn,
                        &format!(
                            "the FtsIndexDoc handler rejected a committed write into '{}': {:?}",
                            payload.collection, response.error_code
                        ),
                    );
                    skipped += 1;
                    continue;
                }
                indexed += 1;
            } else {
                // FtsDelete
                let payload = match FtsDeletePayload::from_bytes(&record.payload) {
                    Ok(p) => p,
                    Err(e) => {
                        self.replay_record_unapplied(
                            "fts",
                            "decode_delete",
                            record_lsn,
                            &format!("FtsDeletePayload could not be decoded: {e}"),
                        );
                        skipped += 1;
                        continue;
                    }
                };

                if tombstones.is_tombstoned(
                    database_id.as_u64(),
                    tenant_id,
                    &payload.collection,
                    record_lsn,
                ) {
                    skipped += 1;
                    continue;
                }

                // An absent id is a delete of a key its home never bound.
                let surrogate = match payload.doc_id.as_deref() {
                    None => None,
                    Some(doc_id) => match u32::from_str_radix(doc_id, 16) {
                        Ok(raw) => Some(Surrogate::new(raw)),
                        Err(e) => {
                            self.replay_record_unapplied(
                                "fts",
                                "doc_id",
                                record_lsn,
                                &format!(
                                    "doc_id '{doc_id}' is not the hex surrogate the delete path writes: {e}"
                                ),
                            );
                            skipped += 1;
                            continue;
                        }
                    },
                };

                let prov = payload.provenance;
                if self.claim_for_validation() {
                    continue;
                }
                if self.recording_redo_undo() {
                    let captured = match surrogate {
                        Some(surrogate) => self.capture_fts_doc_undo(
                            database_id,
                            tenant_id,
                            &payload.collection,
                            surrogate,
                            payload.sys_from,
                            Some(&prov),
                        ),
                        None => Ok(self
                            .capture_sync_hwm_undo(Some(&prov))
                            .into_iter()
                            .collect()),
                    };
                    if !self.record_redo_capture(captured) {
                        skipped += 1;
                        continue;
                    }
                }

                let Some(vshard) =
                    self.replay_vshard("fts", record_lsn, database_id, &payload.collection)
                else {
                    skipped += 1;
                    continue;
                };
                let Some(qualified) = self.replay_qualified(
                    "fts",
                    record_lsn,
                    database_id,
                    payload.collection.clone(),
                ) else {
                    skipped += 1;
                    continue;
                };
                let task = Self::replay_fts_task(
                    nodedb_types::TenantId::new(tenant_id),
                    database_id,
                    vshard,
                    PhysicalPlan::Text(TextOp::FtsDeleteDoc {
                        collection: qualified,
                        surrogate,
                        provenance: Some(prov.clone()),
                    }),
                );

                let prior_hwm = (grouped_origins.contains(record_lsn)
                    && !self.applying_committed_redo())
                .then(|| self.capture_sync_hwm_undo(Some(&prov)))
                .flatten();

                let response = self.execute_fts_delete_doc_at(
                    &task,
                    tenant_id,
                    &payload.collection,
                    surrogate,
                    Some(&prov),
                    payload.sys_from,
                );

                if response.status != crate::bridge::envelope::Status::Ok {
                    self.restore_grouped_replay_sync_hwm(
                        prior_hwm,
                        database_id.as_u64(),
                        tenant_id,
                        record_lsn,
                    );
                    // Refused sync input can survive a crash before its abort marker.
                    if !grouped_origins.contains(record_lsn)
                        && !self.applying_committed_redo()
                        && matches!(
                            response.error_code.as_deref(),
                            Some(crate::bridge::envelope::ErrorCode::SyncRejected {
                                provenance: rejected, applied_seq, ..
                            }) if &prov == rejected && rejected.producer_id != 0 && *applied_seq >= rejected.seq
                        )
                    {
                        skipped += 1;
                        continue;
                    }
                    self.replay_record_unapplied(
                        "fts",
                        "delete_handler",
                        record_lsn,
                        &format!(
                            "the FtsDeleteDoc handler rejected a committed delete in '{}': {:?}",
                            payload.collection, response.error_code
                        ),
                    );
                    skipped += 1;
                    continue;
                }
                deleted += 1;
            }
        }

        if indexed > 0 || deleted > 0 {
            tracing::info!(
                core = self.core_id,
                indexed,
                deleted,
                skipped,
                "WAL FTS replay complete"
            );
        }
    }
}

#[cfg(test)]
#[path = "wal_replay_fts_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "wal_replay_fts_sync_tests.rs"]
mod sync_tests;
