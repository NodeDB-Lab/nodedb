// SPDX-License-Identifier: BUSL-1.1

//! WAL replay for the vector-engine write ops that live outside the plain
//! HNSW put/delete path: vector-primary direct upsert, sparse-vector
//! insert/delete, and multi-vector (ColBERT-style) insert/delete.
//!
//! Runs during startup after `replay_vector_wal` (so `VectorParams` are
//! already registered) and after the vector and sparse checkpoints are loaded
//! (so their replay stamps gate re-application).
//!
//! ## Stamp discipline
//!
//! Direct-upsert and multi-vector writes append HNSW nodes, which are not
//! idempotent under double replay (`insert_with_surrogate` /
//! `insert_multi_vector` never dedup). A record the restored vector
//! checkpoint's stamp names is skipped (`vector_replay_skips`). Every other
//! record replays, including a lower-LSN record that was still in flight when
//! the checkpoint was written.
//!
//! Sparse insert and delete are gated the same way by the sparse-vector
//! checkpoint's stamp (`sparse_vector_replay_skips`).

use nodedb_physical::physical_plan::VectorOp;
use nodedb_wal::record::RecordType;

use crate::bridge::envelope::{PhysicalPlan, Status};
use crate::control::server::wal_dispatch::VectorDirectUpsertRecord;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::wal_replay_vector_redo::RedoVectorWrite;
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
                    | Some(RecordType::VectorDirectDelete)
                    | Some(RecordType::VectorDirectUpdate)
                    | Some(RecordType::VectorDirectTruncate)
                    | Some(RecordType::VectorResolvedDirectWrite)
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
                Some(RecordType::VectorDirectDelete) => self.replay_direct_delete(
                    &record.payload,
                    tenant_id,
                    database_id,
                    record_lsn,
                    tombstones,
                ),
                Some(RecordType::VectorDirectUpdate) => self.replay_direct_update(
                    &record.payload,
                    tenant_id,
                    database_id,
                    record_lsn,
                    tombstones,
                ),
                Some(RecordType::VectorDirectTruncate) => self.replay_direct_truncate(
                    &record.payload,
                    tenant_id,
                    database_id,
                    record_lsn,
                    tombstones,
                ),
                Some(RecordType::VectorResolvedDirectWrite) => self
                    .replay_vector_resolved_direct_write(
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
            pk_bytes,
            vector,
            payload_bytes,
            quantization,
            storage_dtype,
            payload_indexes,
            intent,
            on_conflict_updates,
        )) = zerompk::from_msgpack::<VectorDirectUpsertRecord>(payload)
        else {
            self.replay_record_unapplied(
                "vector",
                "direct_upsert_decode",
                record_lsn,
                "VectorDirectUpsert payload does not decode",
            );
            return false;
        };
        if tombstones.is_tombstoned(tenant_id, &collection, record_lsn) {
            return false;
        }
        let index_key = CoreLoop::vector_index_key(database_id, tenant_id, &collection, &field);
        // The restored checkpoint holds this write; re-applying it appends a
        // duplicate HNSW node.
        if self.vector_replay_skips(record_lsn) {
            return false;
        }
        let surrogate = nodedb_types::Surrogate::new(surrogate_u32);
        if !self.redo_vector_prelude(
            RedoVectorWrite {
                index_key: &index_key,
                tid: tenant_id,
                collection: &collection,
                dim: vector.len(),
                surrogates: &[surrogate],
                ids: &[],
                sidecars: true,
            },
            None,
            record_lsn,
        ) {
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
            PhysicalPlan::Vector(VectorOp::DirectUpsert {
                collection: nodedb_types::QualifiedCollection::from_stored(collection.clone()),
                field: field.clone(),
                surrogate,
                pk_bytes,
                vector: vector.clone(),
                payload: payload_bytes.clone(),
                quantization,
                storage_dtype,
                payload_indexes: payload_indexes.clone(),
                // Replay re-applies a durable record; the statement that asked
                // for rows is long gone, and its write policy was decided when
                // the record was written.
                returning: None,
                rls_filters: Vec::new(),
                on_conflict_updates: on_conflict_updates.clone(),
                rls_write_check: nodedb_types::RlsWriteCheck::already_decided_elsewhere(),
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
                intent,
                on_conflict_updates: &on_conflict_updates,
                rls_write_check: &nodedb_types::RlsWriteCheck::already_decided_elsewhere(),
                returning: None,
                rls_filters: &[],
            },
        );
        if response.status != Status::Ok {
            self.replay_record_rejected(
                "vector",
                record_lsn,
                response.error_code,
                &format!("vector direct upsert into '{collection}' failed"),
            );
            return false;
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
            self.replay_record_unapplied(
                "vector",
                "multi_vector_put_decode",
                record_lsn,
                "MultiVectorPut payload does not decode",
            );
            return false;
        };
        if tombstones.is_tombstoned(tenant_id, &collection, record_lsn) {
            return false;
        }
        let index_key =
            CoreLoop::vector_index_key(database_id, tenant_id, &collection, &field_name);
        if self.vector_replay_skips(record_lsn) {
            return false;
        }
        let document_surrogate = nodedb_types::Surrogate::new(doc_surrogate_u32);
        if !self.redo_vector_prelude(
            RedoVectorWrite {
                index_key: &index_key,
                tid: tenant_id,
                collection: &collection,
                dim,
                surrogates: &[document_surrogate],
                ids: &[],
                sidecars: false,
            },
            None,
            record_lsn,
        ) {
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
            PhysicalPlan::Vector(VectorOp::MultiVectorInsert {
                collection: nodedb_types::QualifiedCollection::from_stored(collection.clone()),
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
            self.replay_record_rejected(
                "vector",
                record_lsn,
                response.error_code,
                &format!("multi-vector insert into '{collection}' failed"),
            );
            return false;
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
            self.replay_record_unapplied(
                "vector",
                "multi_vector_delete_decode",
                record_lsn,
                "MultiVectorDelete payload does not decode",
            );
            return false;
        };
        if tombstones.is_tombstoned(tenant_id, &collection, record_lsn) {
            return false;
        }
        let index_key =
            CoreLoop::vector_index_key(database_id, tenant_id, &collection, &field_name);
        if self.vector_replay_skips(record_lsn) {
            return false;
        }
        let document_surrogate = nodedb_types::Surrogate::new(doc_surrogate_u32);
        if !self.redo_vector_prelude(
            RedoVectorWrite {
                index_key: &index_key,
                tid: tenant_id,
                collection: &collection,
                dim: 0,
                surrogates: &[document_surrogate],
                ids: &[],
                sidecars: false,
            },
            None,
            record_lsn,
        ) {
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
            PhysicalPlan::Vector(VectorOp::MultiVectorDelete {
                collection: nodedb_types::QualifiedCollection::from_stored(collection.clone()),
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
    use nodedb_types::{QualifiedCollection, Surrogate};

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
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "vp"),
            field: "emb".into(),
            surrogate: Surrogate::new(surrogate),
            pk_bytes: Vec::new(),
            vector,
            payload: Vec::new(),
            quantization: nodedb_types::VectorQuantization::None,
            storage_dtype: nodedb_types::VectorStorageDtype::F32,
            payload_indexes: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
            on_conflict_updates: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::decided_earlier_in_request(),
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

    /// A `DirectUpsert` record the restored checkpoint's stamp names is not
    /// re-applied (no duplicate HNSW node); one it does not name replays.
    #[test]
    fn direct_upsert_stamp_gates_replay() {
        // Record LSNs are 1 (below/at) and 2 (above) after two appends.
        let records = append_via_autocommit(&[
            direct_upsert_plan(1, vec![1.0, 2.0, 3.0]),
            direct_upsert_plan(2, vec![4.0, 5.0, 6.0]),
        ]);
        assert_eq!(records.len(), 2);

        let mut h = make_core();
        // A restored checkpoint holding the first write, whose stamp names its
        // LSN. The second write is the WAL tail the checkpoint does not hold.
        let mut coll = VectorCollection::new(3, HnswParams::default());
        coll.insert_with_surrogate(vec![1.0, 2.0, 3.0], Surrogate::new(1));
        h.core.vector_collections.insert(du_index_key(), coll);
        h.core.floors.replay_floors.vector.set(
            crate::data::executor::applied_prefix::ReplayStamp::through(records[0].header.lsn),
        );

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
            "the record the stamp names is skipped and the other replays (no duplicate)"
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
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "sc"),
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

    /// A sparse insert the restored sparse-vector checkpoint's stamp names is
    /// not replayed.
    #[test]
    fn a_sparse_insert_the_stamp_names_is_skipped() {
        let plan = PhysicalPlan::Vector(VectorOp::SparseInsert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "sc"),
            field_name: "sv".into(),
            doc_id: "d1".into(),
            entries: vec![(10, 0.5)],
        });
        let records = append_via_autocommit(&[plan]);
        let mut h = make_core();
        h.core.floors.replay_floors.sparse_vector.set(
            crate::data::executor::applied_prefix::ReplayStamp::through(records[0].header.lsn),
        );
        h.core
            .replay_vector_extended_wal(&records, 1, &nodedb_wal::TombstoneSet::new());
        assert!(
            !h.core.sparse_vector_indexes.contains_key(&sparse_key("sv")),
            "the checkpoint holds the insert, so replay does not apply it again"
        );
    }

    #[test]
    fn sparse_delete_survives_wal_replay() {
        let insert = PhysicalPlan::Vector(VectorOp::SparseInsert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "sc"),
            field_name: "sv".into(),
            doc_id: "d1".into(),
            entries: vec![(10, 0.5)],
        });
        let delete = PhysicalPlan::Vector(VectorOp::SparseDelete {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "sc"),
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
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "mc"),
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
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "mc"),
            field_name: "mv".into(),
            document_surrogate: Surrogate::new(7),
            vectors: vec![1.0, 2.0, 3.0, 4.0],
            count: 2,
            dim: 2,
        });
        let delete = PhysicalPlan::Vector(VectorOp::MultiVectorDelete {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "mc"),
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
