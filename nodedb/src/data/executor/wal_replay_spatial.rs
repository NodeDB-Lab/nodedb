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
//! `nodedb_types::geometry::Geometry` (the same format stored in
//! `SpatialInsertMsg.geometry_bytes`).  On replay we decode it with
//! `zerompk::from_msgpack` and pass the `&Geometry` to `execute_spatial_insert`.
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
use crate::data::executor::replay_abort::abort_replay;
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
                    Err(e) => abort_replay(
                        "spatial",
                        "decode_put",
                        self.core_id,
                        record_lsn,
                        &format!("SpatialPutPayload could not be decoded: {e}"),
                    ),
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
                    Err(e) => abort_replay(
                        "spatial",
                        "doc_id",
                        self.core_id,
                        record_lsn,
                        &format!(
                            "doc_id '{}' is not the hex surrogate the insert path writes: {e}",
                            payload.doc_id
                        ),
                    ),
                };

                // Decode geometry from msgpack bytes stored in the WAL payload.
                let geometry: nodedb_types::geometry::Geometry =
                    match zerompk::from_msgpack(&payload.geometry_bytes) {
                        Ok(g) => g,
                        Err(e) => abort_replay(
                            "spatial",
                            "geometry",
                            self.core_id,
                            record_lsn,
                            &format!(
                                "the geometry committed into '{}' could not be decoded: {e}",
                                payload.collection
                            ),
                        ),
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
                        collection: nodedb_types::QualifiedCollection::from_stored(
                            payload.collection.clone(),
                        ),
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
                    abort_replay(
                        "spatial",
                        "insert_handler",
                        self.core_id,
                        record_lsn,
                        &format!(
                            "the SpatialInsert handler rejected a committed write into '{}'",
                            payload.collection
                        ),
                    );
                }
                inserted += 1;
            } else {
                // SpatialDelete
                let payload = match SpatialDeletePayload::from_bytes(&record.payload) {
                    Ok(p) => p,
                    Err(e) => abort_replay(
                        "spatial",
                        "decode_delete",
                        self.core_id,
                        record_lsn,
                        &format!("SpatialDeletePayload could not be decoded: {e}"),
                    ),
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
                    Err(e) => abort_replay(
                        "spatial",
                        "doc_id",
                        self.core_id,
                        record_lsn,
                        &format!(
                            "doc_id '{}' is not the hex surrogate the insert path writes: {e}",
                            payload.doc_id
                        ),
                    ),
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
                        collection: nodedb_types::QualifiedCollection::from_stored(
                            payload.collection.clone(),
                        ),
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
                    abort_replay(
                        "spatial",
                        "delete_handler",
                        self.core_id,
                        record_lsn,
                        &format!(
                            "the SpatialDelete handler rejected a committed delete in '{}'",
                            payload.collection
                        ),
                    );
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
mod tests {
    use super::*;
    use crate::util::fnv1a_hash;
    use nodedb_types::TenantId;
    use nodedb_types::geometry::Geometry;
    use nodedb_types::sync::wire::SyncProvenance;
    use nodedb_wal::record::WalRecordArgs;
    use std::sync::Arc;

    const DB: u64 = 0;
    const TENANT: u64 = 7;
    const COLLECTION: &str = "places";
    const FIELD: &str = "geom";
    const SURROGATE: u32 = 0x2a;

    /// Holds the bridge endpoints + tempdir alive for the core's lifetime. The
    /// tests drive `replay_spatial_wal` directly and never tick the event loop,
    /// so the far ends are unused — they just must not be dropped.
    struct CoreHarness {
        core: CoreLoop,
        _req_tx: nodedb_bridge::buffer::Producer<crate::bridge::dispatch::BridgeRequest>,
        _resp_rx: nodedb_bridge::buffer::Consumer<crate::bridge::dispatch::BridgeResponse>,
        _dir: tempfile::TempDir,
    }

    fn make_core() -> CoreHarness {
        use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};
        use nodedb_bridge::buffer::RingBuffer;

        let dir = tempfile::tempdir().expect("tempdir");
        let (req_tx, req_rx) = RingBuffer::channel::<BridgeRequest>(64);
        let (resp_tx, resp_rx) = RingBuffer::channel::<BridgeResponse>(64);
        let core = CoreLoop::open(
            0,
            req_rx,
            resp_tx,
            dir.path(),
            Arc::new(nodedb_types::OrdinalClock::new()),
            crate::data::executor::core_loop::test_governor(),
        )
        .expect("open core");
        CoreHarness {
            core,
            _req_tx: req_tx,
            _resp_rx: resp_rx,
            _dir: dir,
        }
    }

    fn storage_key() -> nodedb_types::StorageKey {
        nodedb_types::StorageKey::for_surrogate(nodedb_types::Surrogate::new(SURROGATE))
    }

    fn point(x: f64, y: f64) -> Geometry {
        Geometry::Point {
            coordinates: [x, y],
        }
    }

    fn wal_record(record_type: RecordType, lsn: u64, payload: Vec<u8>) -> nodedb_wal::WalRecord {
        nodedb_wal::WalRecord::new(WalRecordArgs {
            record_type: record_type as u32,
            lsn,
            tenant_id: TENANT,
            vshard_id: 0,
            database_id: DB,
            payload,
            encryption_key: None,
            preamble_bytes: None,
        })
        .expect("wal record")
    }

    /// A put record carrying the local/unidentified producer sentinel, which is
    /// what a non-sync write records. It deliberately bypasses the sync
    /// idempotency gate, so these tests exercise the engine's OWN idempotency
    /// rather than the gate's — the property replay actually depends on.
    fn put_record(lsn: u64, geometry: &Geometry) -> nodedb_wal::WalRecord {
        let geometry_bytes = zerompk::to_msgpack_vec(geometry).expect("encode geometry");
        let payload = nodedb_wal::record::SpatialPutPayload::new(
            SyncProvenance::default(),
            COLLECTION,
            FIELD,
            storage_key().to_string(),
            geometry_bytes,
        )
        .to_bytes()
        .expect("encode SpatialPutPayload");
        wal_record(RecordType::SpatialPut, lsn, payload)
    }

    fn delete_record(lsn: u64) -> nodedb_wal::WalRecord {
        let payload = nodedb_wal::record::SpatialDeletePayload::new(
            SyncProvenance::default(),
            COLLECTION,
            FIELD,
            storage_key().to_string(),
        )
        .to_bytes()
        .expect("encode SpatialDeletePayload");
        wal_record(RecordType::SpatialDelete, lsn, payload)
    }

    fn replay(core: &mut CoreLoop, record: &nodedb_wal::WalRecord) {
        core.replay_spatial_wal(
            std::slice::from_ref(record),
            1,
            &nodedb_wal::TombstoneSet::new(),
        );
    }

    /// Both halves of the spatial state for the test's surrogate: the R-tree
    /// entry count (the half that would silently double), the docmap binding,
    /// and the decoded sparse document body. The body is compared decoded
    /// rather than as raw bytes because the map it is built from does not fix
    /// a field order — the VALUE is what must be stable, not the encoding.
    fn spatial_state(core: &CoreLoop) -> (usize, Option<String>, Option<nodedb_types::Value>) {
        let db = DatabaseId::new(DB);
        let tid = TenantId::new(TENANT);
        let entry_id = fnv1a_hash(storage_key().to_string().as_bytes());
        let entries = core
            .spatial_indexes
            .get(&(db, tid, COLLECTION.to_string(), FIELD.to_string()))
            .map(|rtree| rtree.len())
            .unwrap_or(0);
        let mapped = core
            .spatial_doc_map
            .get(&(db, tid, COLLECTION.to_string(), FIELD.to_string(), entry_id))
            .cloned();
        let body = core
            .sparse
            .get(DB, TENANT, COLLECTION, &storage_key())
            .expect("sparse read")
            .map(|bytes| nodedb_types::value_from_msgpack(&bytes).expect("decode body"));
        (entries, mapped, body)
    }

    /// The property spatial replay relies on instead of a floor: applying the
    /// SAME `SpatialPut` record a second time must leave one R-tree entry, not
    /// two. `RTree::insert` never deduplicates, so without the docmap-guarded
    /// delete-first this leaves a second entry with the same id that no delete
    /// can ever fully remove (`RTree::delete` removes one match per call).
    #[test]
    fn replaying_a_put_record_twice_leaves_one_entry() {
        let mut h = make_core();
        let record = put_record(10, &point(1.0, 2.0));

        replay(&mut h.core, &record);
        let after_first = spatial_state(&h.core);
        assert_eq!(after_first.0, 1, "one geometry indexed");
        assert_eq!(
            after_first.1.as_deref(),
            Some(storage_key().to_string().as_str())
        );
        assert!(after_first.2.is_some(), "the document body must be written");

        replay(&mut h.core, &record);
        assert_eq!(
            spatial_state(&h.core),
            after_first,
            "re-applying a durable SpatialPut record must be a no-op"
        );
    }

    /// A put whose geometry genuinely moved is not a replay and must take
    /// effect — the idempotency above must come from replacing the surrogate's
    /// entry, not from ignoring repeat writes to it.
    #[test]
    fn a_moved_geometry_still_takes_effect() {
        let mut h = make_core();
        replay(&mut h.core, &put_record(10, &point(1.0, 2.0)));
        let before = spatial_state(&h.core);

        replay(&mut h.core, &put_record(11, &point(50.0, 60.0)));
        let after = spatial_state(&h.core);
        assert_eq!(after.0, 1, "a move must replace the entry, not add one");
        assert_ne!(after.2, before.2, "the stored geometry must be the new one");
    }

    /// The delete arm carries the same obligation: a repeated `SpatialDelete`
    /// must remove nothing the second time, in the R-tree, the docmap, and the
    /// document store alike.
    #[test]
    fn replaying_a_delete_record_twice_leaves_nothing_behind() {
        let mut h = make_core();
        replay(&mut h.core, &put_record(10, &point(1.0, 2.0)));

        let record = delete_record(11);
        replay(&mut h.core, &record);
        let after_first = spatial_state(&h.core);
        assert_eq!(after_first, (0, None, None), "the geometry is fully gone");

        replay(&mut h.core, &record);
        assert_eq!(
            spatial_state(&h.core),
            after_first,
            "re-applying a durable SpatialDelete record must be a no-op"
        );
    }
}
