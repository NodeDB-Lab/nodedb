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
//! `surrogate_to_doc_id(surrogate)` (format `{:08x}`).  On replay we parse it
//! back via `u32::from_str_radix(&doc_id, 16)` — no catalog round-trip needed.
//!
//! ## Geometry decode on replay
//!
//! `SpatialPutPayload.geometry_bytes` carries msgpack-encoded
//! `nodedb_types::geometry::Geometry` (the same format stored in
//! `SpatialInsertMsg.geometry_bytes`).  On replay we decode it with
//! `zerompk::from_msgpack` and pass the `&Geometry` to `execute_spatial_insert`.

use crate::bridge::envelope::{PhysicalPlan, Priority, Request};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::spatial_sync::SpatialInsertExec;
use crate::data::executor::task::{ExecutionTask, TaskState};
use crate::types::{DatabaseId, ReadConsistency};
use nodedb_physical::physical_plan::SpatialOp;
use nodedb_types::Surrogate;
use nodedb_wal::record::RecordType;

impl CoreLoop {
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

    /// Replay WAL Spatial records to rebuild in-memory R-tree indexes after crash.
    ///
    /// Processes `SpatialPut` and `SpatialDelete` records in LSN order. Each
    /// record is routed through the apply handler so the idempotency gate runs
    /// on replay exactly as it does on the live ingest path.
    pub fn replay_spatial_wal(
        &mut self,
        records: &[nodedb_wal::WalRecord],
        num_cores: usize,
        tombstones: &nodedb_wal::TombstoneSet,
    ) {
        use nodedb_wal::record::{SpatialDeletePayload, SpatialPutPayload};

        let mut inserted = 0usize;
        let mut deleted = 0usize;
        let mut skipped = 0usize;

        for record in records {
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
                        tracing::warn!(
                            core = self.core_id,
                            lsn = record_lsn,
                            error = %e,
                            "WAL Spatial replay: failed to decode SpatialPutPayload; skipping"
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
                            "WAL Spatial replay: doc_id is not a valid hex surrogate; skipping"
                        );
                        skipped += 1;
                        continue;
                    }
                };

                // Decode geometry from msgpack bytes stored in the WAL payload.
                let geometry: nodedb_types::geometry::Geometry =
                    match zerompk::from_msgpack(&payload.geometry_bytes) {
                        Ok(g) => g,
                        Err(e) => {
                            tracing::warn!(
                                core = self.core_id,
                                lsn = record_lsn,
                                collection = %payload.collection,
                                error = %e,
                                "WAL Spatial replay: failed to decode geometry; skipping"
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
                let task = Self::replay_spatial_task(
                    nodedb_types::TenantId::new(tenant_id),
                    database_id,
                    vshard,
                    PhysicalPlan::Spatial(SpatialOp::Insert {
                        collection: payload.collection.clone(),
                        field: payload.field.clone(),
                        surrogate,
                        geometry: geometry.clone(),
                        provenance: Some(prov.clone()),
                    }),
                );

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
                    tracing::warn!(
                        core = self.core_id,
                        collection = %payload.collection,
                        lsn = record_lsn,
                        "WAL Spatial replay: SpatialPut handler returned error; skipping"
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
                        tracing::warn!(
                            core = self.core_id,
                            lsn = record_lsn,
                            error = %e,
                            "WAL Spatial replay: failed to decode SpatialDeletePayload; skipping"
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
                            "WAL Spatial replay: doc_id is not a valid hex surrogate; skipping"
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
                let task = Self::replay_spatial_task(
                    nodedb_types::TenantId::new(tenant_id),
                    database_id,
                    vshard,
                    PhysicalPlan::Spatial(SpatialOp::Delete {
                        collection: payload.collection.clone(),
                        field: payload.field.clone(),
                        surrogate,
                        provenance: Some(prov.clone()),
                    }),
                );

                let response = self.execute_spatial_delete(
                    &task,
                    tenant_id,
                    &payload.collection,
                    &payload.field,
                    surrogate,
                    Some(&prov),
                );

                if response.status != crate::bridge::envelope::Status::Ok {
                    tracing::warn!(
                        core = self.core_id,
                        collection = %payload.collection,
                        lsn = record_lsn,
                        "WAL Spatial replay: SpatialDelete handler returned error; skipping"
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
