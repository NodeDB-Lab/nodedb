// SPDX-License-Identifier: BUSL-1.1

//! WAL replay for vector engine startup recovery.

use crate::bridge::envelope::PhysicalPlan;
use crate::types::DatabaseId;

use super::core_loop::CoreLoop;
use super::wal_replay_vector_redo::RedoVectorWrite;

impl CoreLoop {
    /// Replay WAL vector records to rebuild in-memory HNSW indexes after crash.
    ///
    /// Called once during startup, after `open()` but before the event loop.
    /// Processes `VectorPut` and `VectorDelete` records, ignoring records
    /// for other vShards (each core only replays records routed to it).
    ///
    /// Records are replayed in LSN order (WAL guarantees this). For batch
    /// inserts, the payload contains multiple vectors in a single record.
    pub fn replay_vector_wal(
        &mut self,
        records: &[nodedb_wal::WalRecord],
        num_cores: usize,
        tombstones: &nodedb_wal::TombstoneSet,
    ) {
        use crate::engine::vector::collection::VectorCollection;
        use crate::engine::vector::hnsw::HnswParams;
        use nodedb_wal::record::RecordType;

        let mut inserted = 0usize;
        let mut deleted = 0usize;
        let mut skipped = 0usize;

        for record in records {
            let logical_type = record.logical_record_type();

            let record_type = RecordType::from_raw(logical_type);
            let is_vector_put = record_type == Some(RecordType::VectorPut);
            let is_vector_delete = record_type == Some(RecordType::VectorDelete);
            let is_vector_params = record_type == Some(RecordType::VectorParams);
            let is_index_drop = record_type == Some(RecordType::VectorIndexDrop);
            if !is_vector_put && !is_vector_delete && !is_vector_params && !is_index_drop {
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
            let database_id = record.header.database_id;
            let record_lsn = record.header.lsn;
            let tombstones = tombstones.for_database(database_id);

            // A committed redo record carries no index DDL; left unclaimed,
            // the validate pass refuses a record that does.
            if (is_index_drop || is_vector_params) && self.applying_committed_redo() {
                continue;
            }

            // The restored vector checkpoint holds this put, delete or index
            // drop. Its stamp names exactly the records the live core applied
            // before the checkpoint, so every other record replays, in LSN
            // order, on top of it: a lower-LSN record still in flight at the
            // checkpoint applies here as it applied live, after the higher
            // ones. `VectorParams` is configuration the catalog seeds at boot,
            // not index content the checkpoint holds, so it always replays.
            if !is_vector_params && self.vector_replay_skips(record_lsn) {
                skipped += 1;
                continue;
            }

            if is_index_drop {
                // Applied in LSN order, so it wipes the params / puts that
                // preceded it and leaves a later re-CREATE to rebuild.
                self.restore_vector_index_drop_record(
                    database_id,
                    tenant_id,
                    &record.payload,
                    &mut skipped,
                );
                continue;
            }

            if is_vector_params {
                self.restore_vector_params_record(
                    database_id,
                    tenant_id,
                    record_lsn,
                    &record.payload,
                    &tombstones,
                    &mut skipped,
                );
                continue;
            }

            if is_vector_put {
                // Try the newest shape first (7 elements with trailing provenance),
                // then the 5-element shape (surrogate, no provenance),
                // then legacy 3-element shapes. The 7-element arm threads
                // provenance into `execute_vector_insert` so the idempotency
                // gate runs on replay exactly as it does on the live path.
                if let Ok((
                    collection,
                    vector,
                    dim,
                    field_name,
                    doc_id,
                    surrogate_u32,
                    provenance,
                )) = zerompk::from_msgpack::<(
                    String,
                    Vec<f32>,
                    usize,
                    String,
                    Option<String>,
                    u32,
                    Option<nodedb_types::sync::wire::SyncProvenance>,
                )>(&record.payload)
                {
                    if tombstones.is_tombstoned(tenant_id, &collection, record_lsn) {
                        skipped += 1;
                        continue;
                    }
                    if vector.len() != dim {
                        // `dim` and the vector both come out of the SAME
                        // payload, so a disagreement is not a schema change the
                        // record predates — it is a record whose two halves
                        // cannot both be what the writer wrote.
                        self.replay_record_unapplied(
                            "vector",
                            "dim",
                            record_lsn,
                            &format!(
                                "record for '{collection}' declares dim {dim} but carries {} \
                                 components",
                                vector.len()
                            ),
                        );
                        skipped += 1;
                        continue;
                    }
                    let insert_index_key = CoreLoop::vector_index_key(
                        database_id,
                        tenant_id,
                        &collection,
                        &field_name,
                    );
                    let surrogate = nodedb_types::Surrogate::new(surrogate_u32);
                    if !self.redo_vector_prelude(
                        RedoVectorWrite {
                            index_key: &insert_index_key,
                            tid: tenant_id,
                            collection: &collection,
                            dim,
                            surrogates: &[surrogate],
                            ids: &[],
                            sidecars: false,
                        },
                        provenance.as_ref(),
                        record_lsn,
                    ) {
                        skipped += 1;
                        continue;
                    }
                    // Local replay rebinds by the carried surrogate; the
                    // compat doc-id slot (always `None` on this write path)
                    // maps straight through to `pk_bytes` for fidelity.
                    let pk_bytes = doc_id.as_ref().map(|d| d.as_bytes().to_vec());
                    let vshard = crate::types::VShardId::from_collection_in_database(
                        DatabaseId::new(database_id),
                        &collection,
                    );
                    let task = Self::replay_vector_task(
                        nodedb_types::TenantId::new(tenant_id),
                        DatabaseId::new(database_id),
                        vshard,
                        PhysicalPlan::Vector(nodedb_physical::physical_plan::VectorOp::Insert {
                            collection: nodedb_types::QualifiedCollection::from_stored(
                                collection.clone(),
                            ),
                            vector: vector.clone(),
                            dim,
                            field_name: field_name.clone(),
                            surrogate,
                            pk_bytes,
                            provenance: provenance.clone(),
                        }),
                    );
                    let response = self.execute_vector_insert(
                        crate::data::executor::handlers::vector::VectorInsertParams {
                            task: &task,
                            tid: tenant_id,
                            collection: &collection,
                            vector: &vector,
                            dim,
                            field_name: &field_name,
                            surrogate,
                            provenance: provenance.as_ref(),
                        },
                    );
                    if response.status != crate::bridge::envelope::Status::Ok {
                        self.replay_record_unapplied(
                            "vector",
                            "insert_handler",
                            record_lsn,
                            &format!(
                                "the vector insert handler rejected a committed write into \
                                 '{collection}': {:?}",
                                response.error_code
                            ),
                        );
                        skipped += 1;
                        continue;
                    }
                    inserted += 1;
                } else if let Ok((collection, vector, dim, field_name, doc_id)) =
                    zerompk::from_msgpack::<(String, Vec<f32>, usize, String, Option<String>)>(
                        &record.payload,
                    )
                {
                    // A committed redo record never carries this shape; left
                    // unclaimed, the validate pass refuses the record.
                    if self.applying_committed_redo() {
                        continue;
                    }
                    if tombstones.is_tombstoned(tenant_id, &collection, record_lsn) {
                        skipped += 1;
                        continue;
                    }
                    if vector.len() != dim {
                        // `dim` and the vector both come out of the SAME
                        // payload, so a disagreement is not a schema change the
                        // record predates — it is a record whose two halves
                        // cannot both be what the writer wrote.
                        self.replay_record_unapplied(
                            "vector",
                            "dim",
                            record_lsn,
                            &format!(
                                "record for '{collection}' declares dim {dim} but carries {} \
                                 components",
                                vector.len()
                            ),
                        );
                        skipped += 1;
                        continue;
                    }
                    let index_key = CoreLoop::vector_index_key(
                        database_id,
                        tenant_id,
                        &collection,
                        &field_name,
                    );
                    let params = self
                        .vector_params
                        .get(&index_key)
                        .cloned()
                        .unwrap_or_else(|| {
                            tracing::debug!(
                                core = self.core_id,
                                %collection,
                                "no VectorParams found during WAL replay; using defaults"
                            );
                            HnswParams::default()
                        });
                    let index = self
                        .vector_collections
                        .entry(index_key)
                        .or_insert_with(|| VectorCollection::new(dim, params));
                    // Unlike the record-internal check above, this compares the
                    // record against a LIVE index whose width the collection
                    // may legitimately have changed since the record was
                    // written (an index rebuilt at a new dimension). The record
                    // is genuinely inapplicable to the current index rather
                    // than malformed, so this stays a skip: aborting here would
                    // wedge the boot on a retained pre-rebuild tail.
                    if index.dim() != dim {
                        let index_dim = index.dim();
                        self.replay_record_rejected(
                            "vector",
                            record_lsn,
                            None,
                            &format!(
                                "vector record for '{collection}' has dim {dim}, the index has \
                                 dim {index_dim}"
                            ),
                        );
                        continue;
                    }
                    // WAL replay rebinds vectors on the local node;
                    // surrogate identity is restored via the dedicated
                    // `SurrogateBind` replay path. Engine inserts here are
                    // local-id-only and bind to `Surrogate::ZERO`.
                    let _ = doc_id;
                    index.insert_with_surrogate(vector, nodedb_types::Surrogate::ZERO);
                    inserted += 1;
                } else if let Ok((collection, vector, dim)) =
                    zerompk::from_msgpack::<(String, Vec<f32>, usize)>(&record.payload)
                {
                    // A committed redo record never carries this shape; left
                    // unclaimed, the validate pass refuses the record.
                    if self.applying_committed_redo() {
                        continue;
                    }
                    if tombstones.is_tombstoned(tenant_id, &collection, record_lsn) {
                        skipped += 1;
                        continue;
                    }
                    if vector.len() != dim {
                        // `dim` and the vector both come out of the SAME
                        // payload, so a disagreement is not a schema change the
                        // record predates — it is a record whose two halves
                        // cannot both be what the writer wrote.
                        self.replay_record_unapplied(
                            "vector",
                            "dim",
                            record_lsn,
                            &format!(
                                "record for '{collection}' declares dim {dim} but carries {} \
                                 components",
                                vector.len()
                            ),
                        );
                        skipped += 1;
                        continue;
                    }
                    let index_key =
                        CoreLoop::vector_index_key(database_id, tenant_id, &collection, "");
                    let params = self
                        .vector_params
                        .get(&index_key)
                        .cloned()
                        .unwrap_or_else(|| {
                            tracing::debug!(
                                core = self.core_id,
                                %collection,
                                "no VectorParams found during WAL replay; using defaults"
                            );
                            HnswParams::default()
                        });
                    let index = self
                        .vector_collections
                        .entry(index_key)
                        .or_insert_with(|| VectorCollection::new(dim, params));
                    // Unlike the record-internal check above, this compares the
                    // record against a LIVE index whose width the collection
                    // may legitimately have changed since the record was
                    // written (an index rebuilt at a new dimension). The record
                    // is genuinely inapplicable to the current index rather
                    // than malformed, so this stays a skip: aborting here would
                    // wedge the boot on a retained pre-rebuild tail.
                    if index.dim() != dim {
                        let index_dim = index.dim();
                        self.replay_record_rejected(
                            "vector",
                            record_lsn,
                            None,
                            &format!(
                                "vector record for '{collection}' has dim {dim}, the index has \
                                 dim {index_dim}"
                            ),
                        );
                        continue;
                    }
                    index.insert(vector);
                    inserted += 1;
                } else if let Ok((collection, vectors, dim)) =
                    zerompk::from_msgpack::<(String, Vec<Vec<f32>>, usize)>(&record.payload)
                {
                    if tombstones.is_tombstoned(tenant_id, &collection, record_lsn) {
                        skipped += 1;
                        continue;
                    }
                    let index_key =
                        CoreLoop::vector_index_key(database_id, tenant_id, &collection, "");
                    if !self.redo_vector_prelude(
                        RedoVectorWrite {
                            index_key: &index_key,
                            tid: tenant_id,
                            collection: &collection,
                            dim,
                            surrogates: &[],
                            ids: &[],
                            sidecars: false,
                        },
                        None,
                        record_lsn,
                    ) {
                        skipped += 1;
                        continue;
                    }
                    let params = self
                        .vector_params
                        .get(&index_key)
                        .cloned()
                        .unwrap_or_else(|| {
                            tracing::debug!(
                                core = self.core_id,
                                %collection,
                                "no VectorParams found for batch replay; using defaults"
                            );
                            HnswParams::default()
                        });
                    let index = self
                        .vector_collections
                        .entry(index_key)
                        .or_insert_with(|| VectorCollection::new(dim, params));
                    for vector in vectors {
                        index.insert(vector);
                    }
                    inserted += 1;
                }
            } else if is_vector_delete {
                if self.replay_vector_delete_record(
                    &record.payload,
                    tenant_id,
                    database_id,
                    record_lsn,
                    &tombstones,
                ) {
                    deleted += 1;
                } else {
                    skipped += 1;
                }
            }
        }

        if inserted > 0 || deleted > 0 {
            tracing::info!(
                core = self.core_id,
                inserted,
                deleted,
                skipped,
                collections = self.vector_collections.len(),
                "WAL vector replay complete"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::executor::applied_prefix::ReplayStamp;
    use crate::data::executor::applied_prefix::stamp::LsnRange;
    use crate::engine::vector::collection::VectorCollection;
    use crate::engine::vector::hnsw::HnswParams;
    use std::sync::Arc;

    /// Holds the bridge endpoints + tempdir alive for the core's lifetime. The
    /// tests drive `replay_vector_wal` directly and never tick the event loop,
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

    /// A bare (unfielded) `VectorPut` WAL record at `lsn` — decodes through the
    /// 3-element replay arm.
    fn vector_put_record(
        lsn: u64,
        tenant_id: u64,
        collection: &str,
        vector: Vec<f32>,
    ) -> nodedb_wal::WalRecord {
        let dim = vector.len();
        let payload =
            zerompk::to_msgpack_vec(&(collection, vector, dim)).expect("encode vector put");
        nodedb_wal::WalRecord::new(nodedb_wal::record::WalRecordArgs {
            record_type: nodedb_wal::record::RecordType::VectorPut as u32,
            lsn,
            tenant_id,
            vshard_id: 0,
            database_id: 0,
            payload,
            encryption_key: None,
            preamble_bytes: None,
        })
        .expect("wal record")
    }

    /// Simulate `load_vector_checkpoints` restoring a checkpoint that holds
    /// one vector of `collection` and names the records in `stamp`.
    fn restore_checkpoint(
        core: &mut CoreLoop,
        tenant_id: u64,
        collection: &str,
        vector: Vec<f32>,
        stamp: ReplayStamp,
    ) {
        let dim = vector.len();
        let mut coll = VectorCollection::new(dim, HnswParams::default());
        coll.insert(vector);
        let key = CoreLoop::vector_index_key(0, tenant_id, collection, "");
        core.vector_collections.insert(key, coll);
        core.floors.replay_floors.vector.set(stamp);
    }

    fn coll_len(core: &CoreLoop, tenant_id: u64, collection: &str) -> Option<usize> {
        let key = CoreLoop::vector_index_key(0, tenant_id, collection, "");
        core.vector_collections.get(&key).map(|c| c.len())
    }

    fn replay(core: &mut CoreLoop, records: &[nodedb_wal::WalRecord]) {
        core.replay_vector_wal(records, 1, &nodedb_wal::TombstoneSet::new());
    }

    /// A record the restored checkpoint's stamp names is not replayed: an HNSW
    /// insert never dedups, so replaying it appends a second node.
    #[test]
    fn a_record_the_stamp_names_is_not_reapplied() {
        let mut h = make_core();
        restore_checkpoint(
            &mut h.core,
            7,
            "emb",
            vec![1.0, 2.0, 3.0],
            ReplayStamp::through(10),
        );
        replay(
            &mut h.core,
            &[vector_put_record(10, 7, "emb", vec![1.0, 2.0, 3.0])],
        );
        assert_eq!(coll_len(&h.core, 7, "emb"), Some(1));
    }

    /// A record above the stamp's prefix and outside its applied ranges is the
    /// WAL tail the checkpoint does not hold, and it replays.
    #[test]
    fn a_record_the_stamp_does_not_name_replays() {
        let mut h = make_core();
        restore_checkpoint(
            &mut h.core,
            7,
            "emb",
            vec![1.0, 2.0, 3.0],
            ReplayStamp::through(10),
        );
        replay(
            &mut h.core,
            &[vector_put_record(11, 7, "emb", vec![4.0, 5.0, 6.0])],
        );
        assert_eq!(coll_len(&h.core, 7, "emb"), Some(2));
    }

    /// Record 7 was still on its way when record 10 applied and the checkpoint
    /// was written. The checkpoint holds 10 and not 7. A stamp at the highest
    /// applied LSN would skip 7 and lose its vector.
    #[test]
    fn a_record_in_flight_below_an_applied_one_replays_once() {
        let mut h = make_core();
        let stamp = ReplayStamp {
            prefix: 5,
            applied_above: vec![LsnRange { start: 10, end: 10 }],
        };
        restore_checkpoint(&mut h.core, 7, "emb", vec![1.0, 2.0, 3.0], stamp);
        replay(
            &mut h.core,
            &[
                vector_put_record(7, 7, "emb", vec![4.0, 5.0, 6.0]),
                vector_put_record(10, 7, "emb", vec![1.0, 2.0, 3.0]),
            ],
        );
        assert_eq!(
            coll_len(&h.core, 7, "emb"),
            Some(2),
            "record 7 applies once and record 10 does not apply again"
        );
    }
}
