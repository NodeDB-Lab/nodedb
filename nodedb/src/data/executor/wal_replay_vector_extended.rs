// SPDX-License-Identifier: BUSL-1.1

//! WAL replay for the vector-engine write ops that live outside the plain
//! HNSW put/delete path: vector-primary direct upsert, sparse-vector
//! insert/delete, and multi-vector (ColBERT-style) insert/delete.
//!
//! Runs during startup after `replay_vector_wal` (so `VectorParams` are
//! already registered) and after the vector / sparse checkpoints are loaded
//! (so the per-collection checkpoint watermark gates re-application).
//!
//! ## Watermark discipline
//!
//! Direct-upsert and multi-vector writes append HNSW nodes, which are **not**
//! idempotent under double replay (`insert_with_surrogate` /
//! `insert_multi_vector` never dedup). They are therefore gated by the
//! per-collection `checkpoint_wal_lsn`: a restored checkpoint already contains
//! every write at or below its watermark, so records at/below it are skipped;
//! records above it are the WAL tail the checkpoint has not absorbed and are
//! applied, after which the watermark is advanced.
//!
//! Sparse insert/delete need no watermark: the sparse index upserts by
//! `doc_id` (a re-inserted document replaces its own postings) and delete of
//! an absent document is a no-op, so full re-application over a restored
//! checkpoint reproduces the exact same state.

use nodedb_physical::physical_plan::VectorOp;
use nodedb_wal::record::RecordType;

use crate::bridge::envelope::{PhysicalPlan, Status};
use crate::data::executor::core_loop::CoreLoop;
use crate::types::DatabaseId;

impl CoreLoop {
    /// Replay the extended vector-engine write records (direct upsert, sparse
    /// insert/delete, multi-vector insert/delete) to rebuild in-memory state
    /// after a crash.
    pub fn replay_vector_extended_wal(
        &mut self,
        records: &[nodedb_wal::WalRecord],
        num_cores: usize,
        tombstones: &nodedb_wal::TombstoneSet,
    ) {
        let mut applied = 0usize;
        let mut skipped = 0usize;

        for record in records {
            let record_type = RecordType::from_raw(record.logical_record_type());
            let is_target = matches!(
                record_type,
                Some(RecordType::VectorDirectUpsert)
                    | Some(RecordType::SparseVectorPut)
                    | Some(RecordType::SparseVectorDelete)
                    | Some(RecordType::MultiVectorPut)
                    | Some(RecordType::MultiVectorDelete)
            );
            if !is_target {
                continue;
            }

            // Per-core routing: each core only replays records for its vShards.
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

            let did_apply = match record_type {
                Some(RecordType::VectorDirectUpsert) => self.replay_direct_upsert(
                    &record.payload,
                    tenant_id,
                    database_id,
                    record_lsn,
                    tombstones,
                ),
                Some(RecordType::MultiVectorPut) => self.replay_multi_vector_put(
                    &record.payload,
                    tenant_id,
                    database_id,
                    record_lsn,
                    tombstones,
                ),
                Some(RecordType::MultiVectorDelete) => self.replay_multi_vector_delete(
                    &record.payload,
                    tenant_id,
                    database_id,
                    record_lsn,
                    tombstones,
                ),
                Some(RecordType::SparseVectorPut) => self.replay_sparse_put(
                    &record.payload,
                    tenant_id,
                    database_id,
                    record_lsn,
                    tombstones,
                ),
                Some(RecordType::SparseVectorDelete) => self.replay_sparse_delete(
                    &record.payload,
                    tenant_id,
                    database_id,
                    record_lsn,
                    tombstones,
                ),
                _ => false,
            };
            if did_apply {
                applied += 1;
            } else {
                skipped += 1;
            }
        }

        if applied > 0 {
            tracing::info!(
                core = self.core_id,
                applied,
                skipped,
                "WAL extended-vector replay complete"
            );
        }
    }

    /// Replay one `VectorDirectUpsert` record. Decodes the shape produced by
    /// `encode_vector_direct_upsert_payload` and routes to the live handler so
    /// the HNSW node, payload bitmap indexes, sparse-store body, and collection
    /// config are all restored identically to the live path.
    fn replay_direct_upsert(
        &mut self,
        payload: &[u8],
        tenant_id: u64,
        database_id: u64,
        record_lsn: u64,
        tombstones: &nodedb_wal::TombstoneSet,
    ) -> bool {
        let tombstones = tombstones.for_database(database_id);
        let Ok((
            collection,
            field,
            surrogate_u32,
            vector,
            payload_bytes,
            quantization,
            storage_dtype,
            payload_indexes,
        )) = zerompk::from_msgpack::<(
            String,
            String,
            u32,
            Vec<f32>,
            Vec<u8>,
            nodedb_types::VectorQuantization,
            nodedb_types::VectorStorageDtype,
            Vec<(String, nodedb_types::PayloadIndexKind)>,
        )>(payload)
        else {
            return false;
        };
        if tombstones.is_tombstoned(tenant_id, &collection, record_lsn) {
            return false;
        }
        let index_key = CoreLoop::vector_index_key(database_id, tenant_id, &collection, &field);
        // Watermark gate: a restored checkpoint already holds every write at or
        // below its watermark; re-applying would append a duplicate HNSW node.
        if let Some(existing) = self.vector_collections.get(&index_key)
            && record_lsn <= existing.checkpoint_wal_lsn()
        {
            return false;
        }
        let surrogate = nodedb_types::Surrogate::new(surrogate_u32);
        let vshard = crate::types::VShardId::from_collection_in_database(
            DatabaseId::new(database_id),
            &collection,
        );
        let task = Self::replay_vector_task(
            nodedb_types::TenantId::new(tenant_id),
            DatabaseId::new(database_id),
            vshard,
            PhysicalPlan::Vector(VectorOp::DirectUpsert {
                collection: collection.clone(),
                field: field.clone(),
                surrogate,
                vector: vector.clone(),
                payload: payload_bytes.clone(),
                quantization,
                storage_dtype,
                payload_indexes: payload_indexes.clone(),
            }),
        );
        let response = self.execute_vector_direct_upsert(
            crate::data::executor::handlers::vector_upsert::VectorDirectUpsertParams {
                task: &task,
                tid: tenant_id,
                collection: &collection,
                field: &field,
                surrogate,
                vector: &vector,
                payload: &payload_bytes,
                quantization,
                storage_dtype,
                payload_indexes: &payload_indexes,
            },
        );
        if response.status != Status::Ok {
            tracing::warn!(
                core = self.core_id,
                %collection,
                lsn = record_lsn,
                "WAL replay: direct-upsert handler returned error; skipping"
            );
            return false;
        }
        // Advance the (possibly freshly created) collection's watermark so the
        // next checkpoint records this replayed write and a later restart does
        // not re-apply it.
        if let Some(coll) = self.vector_collections.get_mut(&index_key) {
            coll.note_checkpoint_lsn(record_lsn);
        }
        true
    }

    /// Replay one `MultiVectorPut` record.
    fn replay_multi_vector_put(
        &mut self,
        payload: &[u8],
        tenant_id: u64,
        database_id: u64,
        record_lsn: u64,
        tombstones: &nodedb_wal::TombstoneSet,
    ) -> bool {
        let tombstones = tombstones.for_database(database_id);
        let Ok((collection, field_name, doc_surrogate_u32, vectors_flat, count, dim)) =
            zerompk::from_msgpack::<(String, String, u32, Vec<f32>, usize, usize)>(payload)
        else {
            return false;
        };
        if tombstones.is_tombstoned(tenant_id, &collection, record_lsn) {
            return false;
        }
        let index_key =
            CoreLoop::vector_index_key(database_id, tenant_id, &collection, &field_name);
        if let Some(existing) = self.vector_collections.get(&index_key)
            && record_lsn <= existing.checkpoint_wal_lsn()
        {
            return false;
        }
        let document_surrogate = nodedb_types::Surrogate::new(doc_surrogate_u32);
        let vshard = crate::types::VShardId::from_collection_in_database(
            DatabaseId::new(database_id),
            &collection,
        );
        let task = Self::replay_vector_task(
            nodedb_types::TenantId::new(tenant_id),
            DatabaseId::new(database_id),
            vshard,
            PhysicalPlan::Vector(VectorOp::MultiVectorInsert {
                collection: collection.clone(),
                field_name: field_name.clone(),
                document_surrogate,
                vectors: vectors_flat.clone(),
                count,
                dim,
            }),
        );
        let response = self.execute_multi_vector_insert(
            crate::data::executor::handlers::vector_multi::MultiVectorInsertParams {
                task: &task,
                tid: tenant_id,
                collection: &collection,
                field_name: &field_name,
                document_surrogate,
                vectors_flat: &vectors_flat,
                count,
                dim,
            },
        );
        if response.status != Status::Ok {
            tracing::warn!(
                core = self.core_id,
                %collection,
                lsn = record_lsn,
                "WAL replay: multi-vector insert handler returned error; skipping"
            );
            return false;
        }
        if let Some(coll) = self.vector_collections.get_mut(&index_key) {
            coll.note_checkpoint_lsn(record_lsn);
        }
        true
    }

    /// Replay one `MultiVectorDelete` record. Delete is idempotent (an absent
    /// document is a no-op), so it is not treated as an error when nothing is
    /// removed; the watermark is still advanced to gate lower records.
    fn replay_multi_vector_delete(
        &mut self,
        payload: &[u8],
        tenant_id: u64,
        database_id: u64,
        record_lsn: u64,
        tombstones: &nodedb_wal::TombstoneSet,
    ) -> bool {
        let tombstones = tombstones.for_database(database_id);
        let Ok((collection, field_name, doc_surrogate_u32)) =
            zerompk::from_msgpack::<(String, String, u32)>(payload)
        else {
            return false;
        };
        if tombstones.is_tombstoned(tenant_id, &collection, record_lsn) {
            return false;
        }
        let index_key =
            CoreLoop::vector_index_key(database_id, tenant_id, &collection, &field_name);
        if let Some(existing) = self.vector_collections.get(&index_key)
            && record_lsn <= existing.checkpoint_wal_lsn()
        {
            return false;
        }
        let document_surrogate = nodedb_types::Surrogate::new(doc_surrogate_u32);
        let vshard = crate::types::VShardId::from_collection_in_database(
            DatabaseId::new(database_id),
            &collection,
        );
        let task = Self::replay_vector_task(
            nodedb_types::TenantId::new(tenant_id),
            DatabaseId::new(database_id),
            vshard,
            PhysicalPlan::Vector(VectorOp::MultiVectorDelete {
                collection: collection.clone(),
                field_name: field_name.clone(),
                document_surrogate,
            }),
        );
        // Ignore the NotFound response for an already-absent document — the
        // apply is idempotent. Advance the watermark regardless so a later
        // checkpoint records the removal and gates the record on the next run.
        let _ = self.execute_multi_vector_delete(
            &task,
            tenant_id,
            &collection,
            &field_name,
            document_surrogate,
        );
        if let Some(coll) = self.vector_collections.get_mut(&index_key) {
            coll.note_checkpoint_lsn(record_lsn);
        }
        true
    }

    /// Replay one `SparseVectorPut` record. Idempotent upsert-by-`doc_id`, so
    /// no watermark gate is required.
    fn replay_sparse_put(
        &mut self,
        payload: &[u8],
        tenant_id: u64,
        database_id: u64,
        record_lsn: u64,
        tombstones: &nodedb_wal::TombstoneSet,
    ) -> bool {
        let tombstones = tombstones.for_database(database_id);
        let Ok((collection, field_name, doc_id, entries)) =
            zerompk::from_msgpack::<(String, String, String, Vec<(u32, f32)>)>(payload)
        else {
            return false;
        };
        if tombstones.is_tombstoned(tenant_id, &collection, record_lsn) {
            return false;
        }
        let vshard = crate::types::VShardId::from_collection_in_database(
            DatabaseId::new(database_id),
            &collection,
        );
        let task = Self::replay_vector_task(
            nodedb_types::TenantId::new(tenant_id),
            DatabaseId::new(database_id),
            vshard,
            PhysicalPlan::Vector(VectorOp::SparseInsert {
                collection: collection.clone(),
                field_name: field_name.clone(),
                doc_id: doc_id.clone(),
                entries: entries.clone(),
            }),
        );
        let response = self.execute_sparse_insert(
            &task,
            tenant_id,
            &collection,
            &field_name,
            &doc_id,
            &entries,
        );
        response.status == Status::Ok
    }

    /// Replay one `SparseVectorDelete` record. Idempotent (an absent document
    /// is a no-op), so no watermark gate is required.
    fn replay_sparse_delete(
        &mut self,
        payload: &[u8],
        tenant_id: u64,
        database_id: u64,
        record_lsn: u64,
        tombstones: &nodedb_wal::TombstoneSet,
    ) -> bool {
        let tombstones = tombstones.for_database(database_id);
        let Ok((collection, field_name, doc_id)) =
            zerompk::from_msgpack::<(String, String, String)>(payload)
        else {
            return false;
        };
        if tombstones.is_tombstoned(tenant_id, &collection, record_lsn) {
            return false;
        }
        let vshard = crate::types::VShardId::from_collection_in_database(
            DatabaseId::new(database_id),
            &collection,
        );
        let task = Self::replay_vector_task(
            nodedb_types::TenantId::new(tenant_id),
            DatabaseId::new(database_id),
            vshard,
            PhysicalPlan::Vector(VectorOp::SparseDelete {
                collection: collection.clone(),
                field_name: field_name.clone(),
                doc_id: doc_id.clone(),
            }),
        );
        // An absent document yields NotFound; that is an expected idempotent
        // no-op on replay, not a failure.
        let _ = self.execute_sparse_delete(&task, tenant_id, &collection, &field_name, &doc_id);
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use crate::control::server::wal_dispatch::wal_append_if_write;
    use crate::engine::vector::collection::VectorCollection;
    use crate::engine::vector::hnsw::HnswParams;
    use crate::types::{DatabaseId, TenantId, VShardId};
    use crate::wal::manager::WalManager;
    use nodedb_types::Surrogate;

    /// Holds the bridge endpoints + tempdir alive for the core's lifetime. The
    /// tests drive the replay methods directly and never tick the event loop.
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
        )
        .expect("open core");
        CoreHarness {
            core,
            _req_tx: req_tx,
            _resp_rx: resp_rx,
            _dir: dir,
        }
    }

    const TID: u64 = 1;

    /// Append each plan through the **production autocommit WAL path**
    /// (`wal_append_if_write`), then read the records back. Asserts every plan
    /// produced a durable record (`Some(lsn)`) — the exact assertion that fails
    /// on the pre-fix code, where these ops hit the catch-all `_ => None` and no
    /// record was written.
    fn append_via_autocommit(plans: &[PhysicalPlan]) -> Vec<nodedb_wal::WalRecord> {
        let dir = tempfile::tempdir().expect("wal tempdir");
        let wal = WalManager::open_for_testing(&dir.path().join("wal")).expect("open wal");
        for plan in plans {
            let outcome = wal_append_if_write(
                &wal,
                TenantId::new(TID),
                VShardId::new(0),
                DatabaseId::DEFAULT,
                plan,
            )
            .expect("wal append");
            assert!(
                outcome.lsn.is_some(),
                "every one of these vector writes must produce a durable WAL record"
            );
        }
        wal.sync().expect("wal sync");
        wal.replay().expect("wal replay read")
    }

    fn direct_upsert_plan(surrogate: u32, vector: Vec<f32>) -> PhysicalPlan {
        PhysicalPlan::Vector(VectorOp::DirectUpsert {
            collection: "vp".into(),
            field: "emb".into(),
            surrogate: Surrogate::new(surrogate),
            vector,
            payload: Vec::new(),
            quantization: nodedb_types::VectorQuantization::None,
            storage_dtype: nodedb_types::VectorStorageDtype::F32,
            payload_indexes: Vec::new(),
        })
    }

    fn du_index_key() -> (DatabaseId, TenantId, String) {
        CoreLoop::vector_index_key(DatabaseId::DEFAULT.as_u64(), TID, "vp", "emb")
    }

    /// The vector-primary regression: an INSERT into a `primary='vector'`
    /// collection (a `DirectUpsert`) must survive a WAL replay into a fresh
    /// engine. Fails on the old path — no record was ever written.
    #[test]
    fn direct_upsert_survives_wal_replay() {
        let records = append_via_autocommit(&[direct_upsert_plan(42, vec![1.0, 2.0, 3.0])]);
        let mut h = make_core();
        h.core
            .replay_vector_extended_wal(&records, 1, &nodedb_wal::TombstoneSet::new());
        let coll = h
            .core
            .vector_collections
            .get(&du_index_key())
            .expect("vector-primary collection rebuilt from WAL");
        assert_eq!(coll.len(), 1, "the upserted vector must be recovered");
        assert!(
            coll.local_for_surrogate(Surrogate::new(42)).is_some(),
            "the cross-engine surrogate must be rebound on recovery"
        );
    }

    /// A `DirectUpsert` record at or below the collection's restored checkpoint
    /// watermark must NOT be re-applied (no duplicate HNSW node); one above it
    /// must replay.
    #[test]
    fn direct_upsert_watermark_gates_replay() {
        // Record LSNs are 1 (below/at) and 2 (above) after two appends.
        let records = append_via_autocommit(&[
            direct_upsert_plan(1, vec![1.0, 2.0, 3.0]),
            direct_upsert_plan(2, vec![4.0, 5.0, 6.0]),
        ]);
        assert_eq!(records.len(), 2);

        let mut h = make_core();
        // Simulate a restored checkpoint holding the first write, watermarked at
        // its LSN (1). The second write (LSN 2) is the un-absorbed WAL tail.
        let mut coll = VectorCollection::new(3, HnswParams::default());
        coll.insert_with_surrogate(vec![1.0, 2.0, 3.0], Surrogate::new(1));
        coll.note_checkpoint_lsn(records[0].header.lsn);
        // Round-trip through a checkpoint so the persisted watermark becomes the
        // replay gate (`checkpoint_wal_lsn`): a live `note_checkpoint_lsn` only
        // feeds the applied watermark, which save folds into the gate and load
        // restores — the faithful shape of a restored checkpoint.
        let bytes = coll.checkpoint_to_bytes(None).unwrap();
        let coll = VectorCollection::from_checkpoint(&bytes, None).expect("decode checkpoint");
        h.core.vector_collections.insert(du_index_key(), coll);

        h.core
            .replay_vector_extended_wal(&records, 1, &nodedb_wal::TombstoneSet::new());

        let coll = h
            .core
            .vector_collections
            .get(&du_index_key())
            .expect("collection present");
        assert_eq!(
            coll.len(),
            2,
            "record at/below watermark skipped, record above replayed (no duplicate)"
        );
    }

    fn sparse_key(field: &str) -> (DatabaseId, TenantId, String, String) {
        let field = if field.is_empty() { "_sparse" } else { field };
        (
            DatabaseId::DEFAULT,
            TenantId::new(TID),
            "sc".into(),
            field.into(),
        )
    }

    #[test]
    fn sparse_insert_survives_wal_replay() {
        let plan = PhysicalPlan::Vector(VectorOp::SparseInsert {
            collection: "sc".into(),
            field_name: "sv".into(),
            doc_id: "d1".into(),
            entries: vec![(10, 0.5), (20, 0.8)],
        });
        let records = append_via_autocommit(&[plan]);
        let mut h = make_core();
        h.core
            .replay_vector_extended_wal(&records, 1, &nodedb_wal::TombstoneSet::new());
        let idx = h
            .core
            .sparse_vector_indexes
            .get(&sparse_key("sv"))
            .expect("sparse index rebuilt from WAL");
        assert_eq!(idx.doc_count(), 1, "the sparse document must be recovered");
    }

    #[test]
    fn sparse_delete_survives_wal_replay() {
        let insert = PhysicalPlan::Vector(VectorOp::SparseInsert {
            collection: "sc".into(),
            field_name: "sv".into(),
            doc_id: "d1".into(),
            entries: vec![(10, 0.5)],
        });
        let delete = PhysicalPlan::Vector(VectorOp::SparseDelete {
            collection: "sc".into(),
            field_name: "sv".into(),
            doc_id: "d1".into(),
        });
        let records = append_via_autocommit(&[insert, delete]);
        let mut h = make_core();
        h.core
            .replay_vector_extended_wal(&records, 1, &nodedb_wal::TombstoneSet::new());
        let doc_count = h
            .core
            .sparse_vector_indexes
            .get(&sparse_key("sv"))
            .map(|i| i.doc_count())
            .unwrap_or(0);
        assert_eq!(
            doc_count, 0,
            "the deleted sparse document must stay deleted"
        );
    }

    fn mv_index_key() -> (DatabaseId, TenantId, String) {
        CoreLoop::vector_index_key(DatabaseId::DEFAULT.as_u64(), TID, "mc", "mv")
    }

    #[test]
    fn multi_vector_insert_survives_wal_replay() {
        let plan = PhysicalPlan::Vector(VectorOp::MultiVectorInsert {
            collection: "mc".into(),
            field_name: "mv".into(),
            document_surrogate: Surrogate::new(7),
            vectors: vec![1.0, 2.0, 3.0, 4.0], // 2 vectors of dim 2
            count: 2,
            dim: 2,
        });
        let records = append_via_autocommit(&[plan]);
        let mut h = make_core();
        h.core
            .replay_vector_extended_wal(&records, 1, &nodedb_wal::TombstoneSet::new());
        let coll = h
            .core
            .vector_collections
            .get(&mv_index_key())
            .expect("multi-vector collection rebuilt from WAL");
        assert_eq!(coll.len(), 2, "both document vectors must be recovered");
        assert!(
            coll.multi_doc_map.contains_key(&Surrogate::new(7)),
            "the multi-vector document grouping must be reconstructed"
        );
    }

    #[test]
    fn multi_vector_delete_survives_wal_replay() {
        let insert = PhysicalPlan::Vector(VectorOp::MultiVectorInsert {
            collection: "mc".into(),
            field_name: "mv".into(),
            document_surrogate: Surrogate::new(7),
            vectors: vec![1.0, 2.0, 3.0, 4.0],
            count: 2,
            dim: 2,
        });
        let delete = PhysicalPlan::Vector(VectorOp::MultiVectorDelete {
            collection: "mc".into(),
            field_name: "mv".into(),
            document_surrogate: Surrogate::new(7),
        });
        let records = append_via_autocommit(&[insert, delete]);
        let mut h = make_core();
        h.core
            .replay_vector_extended_wal(&records, 1, &nodedb_wal::TombstoneSet::new());
        let coll = h
            .core
            .vector_collections
            .get(&mv_index_key())
            .expect("collection present");
        assert!(
            !coll.multi_doc_map.contains_key(&Surrogate::new(7)),
            "the deleted multi-vector document must stay deleted"
        );
    }
}
