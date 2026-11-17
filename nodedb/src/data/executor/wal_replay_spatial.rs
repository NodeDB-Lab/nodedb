// SPDX-License-Identifier: BUSL-1.1

//! WAL replay for Spatial engine startup recovery.
//!
//! Called once during startup, after `open()` but before the event loop.
//! Processes `SpatialPut` and `SpatialDelete` records in LSN order, routing
//! each through the same apply handler that the live sync path uses so the
//! idempotency gate fires on replay.
//!
//! ## Surrogate re-derivation on replay
//!
//! The WAL payload `doc_id` field holds the hex-encoded surrogate produced by
//! `StorageKey`'s `Display` impl (format `{:08x}`). On replay we parse it
//! back via `u32::from_str_radix(&doc_id, 16)` — no catalog round-trip needed.
//!
//! ## Geometry decode on replay
//!
//! `SpatialPutPayload.geometry_bytes` carries msgpack-encoded
//! `nodedb_types::geometry::SridGeometry` (the same format stored in
//! `SpatialInsertMsg.geometry_bytes`). On replay we decode it with
//! `zerompk::from_msgpack` and pass the `&SridGeometry` to
//! `execute_spatial_insert`.
//!
//! ## Why there is no replay floor or watermark here
//!
//! Every retained `SpatialPut` / `SpatialDelete` record is fed back through the
//! apply handlers on every boot, including records a restored checkpoint
//! already contains. That is safe because both halves of the spatial state are
//! keyed by the record's own surrogate and are rewritten wholesale:
//!
//! * The sparse document body is a `put` under the hex surrogate — an absolute
//!   overwrite, and the delete is a `remove` that reports absence as `Ok`.
//! * The R-tree is the half that is NOT idempotent on its own: `RTree::insert`
//!   appends without deduplicating, so a bare re-insert would leave two entries
//!   with one id. `execute_spatial_insert` therefore deletes the surrogate's
//!   entry first whenever `spatial_doc_map` says one exists, and the two maps
//!   are only ever written together — including by the checkpoint loader, which
//!   installs an R-tree and its docmap as one generation or neither. So a
//!   re-applied put replaces its own entry instead of duplicating it, and a
//!   re-applied delete removes nothing the second time.
//!
//! The tests at the bottom of this file pin that, entry counts included.

use crate::bridge::envelope::{PhysicalPlan, Priority, Request};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::spatial_sync::SpatialInsertExec;
use crate::data::executor::task::{ExecutionTask, TaskState};
use crate::types::{DatabaseId, ReadConsistency};
use nodedb_physical::physical_plan::SpatialOp;
use nodedb_types::Surrogate;
use nodedb_wal::record::RecordType;

impl CoreLoop {
    /// In the install pass of a committed-redo apply, record the undo of one
    /// spatial write before it runs: the row's pre-image and the sync
    /// high-water mark. Returns `false` when the pre-image cannot be read;
    /// the error is kept on the apply and the write is skipped. A delete of a
    /// key its home never bound (`None`) has no row pre-image: only the high-
    /// water mark is recorded.
    fn record_spatial_row_undo(
        &mut self,
        database_id: DatabaseId,
        tenant_id: u64,
        (collection, field): (&str, &str),
        surrogate: Option<Surrogate>,
        provenance: &nodedb_types::sync::wire::SyncProvenance,
    ) -> bool {
        if !self.recording_redo_undo() {
            return true;
        }
        let hwm = self.capture_sync_hwm_undo(Some(provenance));
        let captured = match surrogate {
            Some(surrogate) => self
                .capture_spatial_row_undo(database_id, tenant_id, collection, field, surrogate)
                .map(|row| std::iter::once(row).chain(hwm).collect::<Vec<_>>()),
            None => Ok(hwm.into_iter().collect()),
        };
        self.record_redo_capture(captured)
    }

    /// Build a synthetic `ExecutionTask` for Spatial WAL replay.
    fn replay_spatial_task(
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
                admission: crate::bridge::envelope::Admission::Exempt(
                    crate::bridge::envelope::ExemptReason::AlreadyOrdered,
                ),
            },
            state: TaskState::Running,
            wal_lsn: None,
            resolved_now_ms: None,
        }
    }

    /// Replay WAL Spatial records to rebuild in-memory R-tree indexes after crash.
    ///
    /// Processes `SpatialPut` and `SpatialDelete` records in LSN order. Each
    /// record is routed through the apply handler so the idempotency gate runs
    /// on replay exactly as it does on the live ingest path.
    pub(crate) fn replay_spatial_wal(
        &mut self,
        records: &[nodedb_wal::WalRecord],
        num_cores: usize,
        tombstones: &nodedb_wal::TombstoneSet,
        grouped_origins: &crate::wal::redo::replay_origin::GroupedReplayOrigins,
    ) {
        use nodedb_wal::record::{SpatialDeletePayload, SpatialPutPayload};

        let mut inserted = 0usize;
        let mut deleted = 0usize;
        let mut skipped = 0usize;

        for record in records {
            if self.replay_halted() {
                break;
            }
            let logical_type = record.logical_record_type();
            let record_type = RecordType::from_raw(logical_type);

            let is_spatial_put = record_type == Some(RecordType::SpatialPut);
            let is_spatial_delete = record_type == Some(RecordType::SpatialDelete);
            if !is_spatial_put && !is_spatial_delete {
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
            let database_id = DatabaseId::new(record.header.database_id);
            let record_lsn = record.header.lsn;

            if is_spatial_put {
                let payload = match SpatialPutPayload::from_bytes(&record.payload) {
                    Ok(p) => p,
                    Err(e) => {
                        self.replay_record_unapplied(
                            "spatial",
                            "decode_put",
                            record_lsn,
                            &format!("SpatialPutPayload could not be decoded: {e}"),
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
                    Err(e) => {
                        self.replay_record_unapplied(
                            "spatial",
                            "doc_id",
                            record_lsn,
                            &format!(
                                "doc_id '{}' is not the hex surrogate the insert path writes: {e}",
                                payload.doc_id
                            ),
                        );
                        skipped += 1;
                        continue;
                    }
                };

                // Decode geometry from msgpack bytes stored in the WAL payload.
                let geometry: nodedb_types::geometry::SridGeometry =
                    match zerompk::from_msgpack(&payload.geometry_bytes) {
                        Ok(g) => g,
                        Err(e) => {
                            self.replay_record_unapplied(
                                "spatial",
                                "geometry",
                                record_lsn,
                                &format!(
                                    "the geometry committed into '{}' could not be decoded: {e}",
                                    payload.collection
                                ),
                            );
                            skipped += 1;
                            continue;
                        }
                    };

                let prov = payload.provenance.clone();
                if self.claim_for_validation() {
                    continue;
                }
                if !self.record_spatial_row_undo(
                    database_id,
                    tenant_id,
                    (&payload.collection, &payload.field),
                    Some(surrogate),
                    &prov,
                ) {
                    skipped += 1;
                    continue;
                }

                let Some(vshard) =
                    self.replay_vshard("spatial", record_lsn, database_id, &payload.collection)
                else {
                    skipped += 1;
                    continue;
                };
                let Some(qualified) = self.replay_qualified(
                    "spatial",
                    record_lsn,
                    database_id,
                    payload.collection.clone(),
                ) else {
                    skipped += 1;
                    continue;
                };
                let task = Self::replay_spatial_task(
                    nodedb_types::TenantId::new(tenant_id),
                    database_id,
                    vshard,
                    PhysicalPlan::Spatial(SpatialOp::Insert {
                        collection: qualified,
                        field: payload.field.clone(),
                        surrogate,
                        geometry: geometry.clone(),
                        provenance: Some(prov.clone()),
                    }),
                );

                let prior_hwm = (grouped_origins.contains(record_lsn)
                    && !self.applying_committed_redo())
                .then(|| self.capture_sync_hwm_undo(Some(&prov)))
                .flatten();
                let response = self.execute_spatial_insert(SpatialInsertExec {
                    task: &task,
                    tid: tenant_id,
                    collection: &payload.collection,
                    field: &payload.field,
                    surrogate,
                    geometry: &geometry,
                    provenance: Some(&prov),
                });

                if response.status != crate::bridge::envelope::Status::Ok {
                    self.restore_grouped_replay_sync_hwm(
                        prior_hwm,
                        database_id.as_u64(),
                        tenant_id,
                        record_lsn,
                    );
                    if !grouped_origins.contains(record_lsn)
                        && !self.applying_committed_redo()
                        && matches!(response.error_code.as_deref(), Some(crate::bridge::envelope::ErrorCode::SyncRejected {
                            provenance: rejected, applied_seq, ..
                        }) if &prov == rejected && rejected.producer_id != 0 && *applied_seq >= rejected.seq)
                    {
                        skipped += 1;
                        continue;
                    }
                    self.replay_record_unapplied(
                        "spatial",
                        "insert_handler",
                        record_lsn,
                        &format!(
                            "the SpatialInsert handler rejected a committed write into '{}': {:?}",
                            payload.collection, response.error_code
                        ),
                    );
                    skipped += 1;
                    continue;
                }
                inserted += 1;
            } else {
                // SpatialDelete
                let payload = match SpatialDeletePayload::from_bytes(&record.payload) {
                    Ok(p) => p,
                    Err(e) => {
                        self.replay_record_unapplied(
                            "spatial",
                            "decode_delete",
                            record_lsn,
                            &format!("SpatialDeletePayload could not be decoded: {e}"),
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
                                "spatial",
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

                let prov = payload.provenance.clone();
                if self.claim_for_validation() {
                    continue;
                }
                if !self.record_spatial_row_undo(
                    database_id,
                    tenant_id,
                    (&payload.collection, &payload.field),
                    surrogate,
                    &prov,
                ) {
                    skipped += 1;
                    continue;
                }

                let Some(vshard) =
                    self.replay_vshard("spatial", record_lsn, database_id, &payload.collection)
                else {
                    skipped += 1;
                    continue;
                };
                let Some(qualified) = self.replay_qualified(
                    "spatial",
                    record_lsn,
                    database_id,
                    payload.collection.clone(),
                ) else {
                    skipped += 1;
                    continue;
                };
                let task = Self::replay_spatial_task(
                    nodedb_types::TenantId::new(tenant_id),
                    database_id,
                    vshard,
                    PhysicalPlan::Spatial(SpatialOp::Delete {
                        collection: qualified,
                        field: payload.field.clone(),
                        surrogate,
                        provenance: Some(prov.clone()),
                    }),
                );

                let prior_hwm = (grouped_origins.contains(record_lsn)
                    && !self.applying_committed_redo())
                .then(|| self.capture_sync_hwm_undo(Some(&prov)))
                .flatten();
                let response = self.execute_spatial_delete(
                    &task,
                    tenant_id,
                    &payload.collection,
                    &payload.field,
                    surrogate,
                    Some(&prov),
                );

                if response.status != crate::bridge::envelope::Status::Ok {
                    self.restore_grouped_replay_sync_hwm(
                        prior_hwm,
                        database_id.as_u64(),
                        tenant_id,
                        record_lsn,
                    );
                    if !grouped_origins.contains(record_lsn)
                        && !self.applying_committed_redo()
                        && matches!(response.error_code.as_deref(), Some(crate::bridge::envelope::ErrorCode::SyncRejected {
                            provenance: rejected, applied_seq, ..
                        }) if &prov == rejected && rejected.producer_id != 0 && *applied_seq >= rejected.seq)
                    {
                        skipped += 1;
                        continue;
                    }
                    self.replay_record_unapplied(
                        "spatial",
                        "delete_handler",
                        record_lsn,
                        &format!(
                            "the SpatialDelete handler rejected a committed delete in '{}': {:?}",
                            payload.collection, response.error_code
                        ),
                    );
                    skipped += 1;
                    continue;
                }
                deleted += 1;
            }
        }

        if inserted > 0 || deleted > 0 {
            tracing::info!(
                core = self.core_id,
                inserted,
                deleted,
                skipped,
                "WAL Spatial replay complete"
            );
        }
    }
}

#[cfg(test)]
#[path = "wal_replay_spatial_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "wal_replay_spatial_sync_tests.rs"]
mod sync_tests;
