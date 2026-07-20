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
//! string produced by `surrogate_to_doc_id(surrogate)` (format `{:08x}`).
//! On replay we parse it back via `u32::from_str_radix(&doc_id, 16)` —
//! the same conversion used by the scan / prefilter paths.  This does not
//! require a catalog or surrogate-assigner round-trip: the 8-hex-char key
//! is already the stable `u32` surrogate identity.

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
                plan,
                deadline: std::time::Instant::now() + std::time::Duration::from_secs(60),
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
    pub fn replay_fts_wal(
        &mut self,
        records: &[nodedb_wal::WalRecord],
        num_cores: usize,
        tombstones: &nodedb_wal::TombstoneSet,
    ) {
        use nodedb_wal::record::{FtsDeletePayload, FtsIndexPayload};

        let mut indexed = 0usize;
        let mut deleted = 0usize;
        let mut skipped = 0usize;

        for record in records {
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
            // Pre-scoping records carry `database_id == 0`, which maps to
            // `DatabaseId::DEFAULT` — exactly where the migration placed legacy
            // rows, so old and replayed data co-locate correctly.
            let database_id = DatabaseId::new(record.header.database_id);

            if is_fts_index {
                let payload = match FtsIndexPayload::from_bytes(&record.payload) {
                    Ok(p) => p,
                    Err(e) => {
                        tracing::warn!(
                            core = self.core_id,
                            lsn = record_lsn,
                            error = %e,
                            "WAL FTS replay: failed to decode FtsIndexPayload; skipping"
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
                    Err(_) => {
                        tracing::warn!(
                            core = self.core_id,
                            lsn = record_lsn,
                            doc_id = %payload.doc_id,
                            "WAL FTS replay: doc_id is not a valid hex surrogate; skipping"
                        );
                        skipped += 1;
                        continue;
                    }
                };

                let prov = payload.provenance.clone();

                let vshard = crate::types::VShardId::from_collection_in_database(
                    database_id,
                    &payload.collection,
                );
                let task = Self::replay_fts_task(
                    nodedb_types::TenantId::new(tenant_id),
                    database_id,
                    vshard,
                    PhysicalPlan::Text(TextOp::FtsIndexDoc {
                        collection: payload.collection.clone(),
                        surrogate,
                        text: payload.text.clone(),
                        provenance: Some(prov.clone()),
                    }),
                );

                let response = self.execute_fts_index_doc(
                    &task,
                    tenant_id,
                    &payload.collection,
                    surrogate,
                    &payload.text,
                    Some(&prov),
                );

                if response.status != crate::bridge::envelope::Status::Ok {
                    tracing::warn!(
                        core = self.core_id,
                        collection = %payload.collection,
                        lsn = record_lsn,
                        "WAL FTS replay: FtsIndex handler returned error; skipping"
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
                        tracing::warn!(
                            core = self.core_id,
                            lsn = record_lsn,
                            error = %e,
                            "WAL FTS replay: failed to decode FtsDeletePayload; skipping"
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

                let surrogate = match u32::from_str_radix(&payload.doc_id, 16) {
                    Ok(raw) => Surrogate::new(raw),
                    Err(_) => {
                        tracing::warn!(
                            core = self.core_id,
                            lsn = record_lsn,
                            doc_id = %payload.doc_id,
                            "WAL FTS replay: doc_id is not a valid hex surrogate; skipping"
                        );
                        skipped += 1;
                        continue;
                    }
                };

                let prov = payload.provenance.clone();

                let vshard = crate::types::VShardId::from_collection_in_database(
                    database_id,
                    &payload.collection,
                );
                let task = Self::replay_fts_task(
                    nodedb_types::TenantId::new(tenant_id),
                    database_id,
                    vshard,
                    PhysicalPlan::Text(TextOp::FtsDeleteDoc {
                        collection: payload.collection.clone(),
                        surrogate,
                        provenance: Some(prov.clone()),
                    }),
                );

                let response = self.execute_fts_delete_doc(
                    &task,
                    tenant_id,
                    &payload.collection,
                    surrogate,
                    Some(&prov),
                );

                if response.status != crate::bridge::envelope::Status::Ok {
                    tracing::warn!(
                        core = self.core_id,
                        collection = %payload.collection,
                        lsn = record_lsn,
                        "WAL FTS replay: FtsDelete handler returned error; skipping"
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
