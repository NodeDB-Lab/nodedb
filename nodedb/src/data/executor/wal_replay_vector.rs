// SPDX-License-Identifier: BUSL-1.1

//! WAL replay for vector engine startup recovery.

use crate::bridge::envelope::{PhysicalPlan, Priority, Request};
use crate::data::executor::task::{ExecutionTask, TaskState};
use crate::types::{DatabaseId, ReadConsistency};

use super::core_loop::CoreLoop;

impl CoreLoop {
    /// Build a synthetic `ExecutionTask` for WAL replay.
    ///
    /// Mirrors the equivalent helper in `timeseries_wal.rs`. The task carries
    /// no meaningful request semantics — it is only needed so that the handler
    /// methods can return a typed `Response`.
    pub(in crate::data::executor) fn replay_vector_task(
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
            if !is_vector_put && !is_vector_delete && !is_vector_params {
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

            if is_vector_params {
                // Newer records append the vector field name as the 9th
                // element; older records have 8 (quantization params, no field
                // name) or 4 (no quantization params). Try the full shape, fall
                // back to the legacy 4-tuple with the default (unnamed) field.
                let decoded = zerompk::from_msgpack::<(
                    String,
                    usize,
                    usize,
                    String,
                    String,
                    usize,
                    usize,
                    usize,
                    String,
                )>(&record.payload)
                .ok()
                .map(|(c, m, ef, metric, _it, _pq, _ic, _ip, field)| (c, m, ef, metric, field))
                .or_else(|| {
                    zerompk::from_msgpack::<(String, usize, usize, String)>(&record.payload)
                        .ok()
                        .map(|(c, m, ef, metric)| (c, m, ef, metric, String::new()))
                });
                if let Some((collection, m, ef_construction, metric, field_name)) = decoded {
                    if tombstones.is_tombstoned(tenant_id, &collection, record_lsn) {
                        skipped += 1;
                        continue;
                    }
                    let index_key = CoreLoop::vector_index_key(
                        database_id,
                        tenant_id,
                        &collection,
                        &field_name,
                    );
                    use crate::engine::vector::distance::DistanceMetric;
                    let metric_enum = match metric.as_str() {
                        "l2" | "euclidean" => DistanceMetric::L2,
                        "cosine" => DistanceMetric::Cosine,
                        "inner_product" | "ip" | "dot" => DistanceMetric::InnerProduct,
                        "manhattan" | "l1" => DistanceMetric::Manhattan,
                        "chebyshev" | "linf" => DistanceMetric::Chebyshev,
                        "hamming" => DistanceMetric::Hamming,
                        "jaccard" => DistanceMetric::Jaccard,
                        "pearson" => DistanceMetric::Pearson,
                        _ => DistanceMetric::Cosine,
                    };
                    let params = HnswParams {
                        m,
                        m0: m * 2,
                        ef_construction,
                        metric: metric_enum,
                        dtype: nodedb_types::vector_dtype::VectorStorageDtype::F32,
                    };
                    self.vector_params.insert(index_key, params);
                    tracing::debug!(
                        core = self.core_id,
                        %collection,
                        field = %field_name,
                        m,
                        ef_construction,
                        %metric,
                        "WAL replay: restored vector params"
                    );
                }
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
                        tracing::warn!(
                            core = self.core_id,
                            %collection,
                            expected = dim,
                            actual = vector.len(),
                            "skipping WAL vector record: dimension mismatch"
                        );
                        continue;
                    }
                    // Checkpoint watermark gate: a restored checkpoint already
                    // contains every write at or below its `checkpoint_wal_lsn`.
                    // Re-applying a straddling-segment record would append a
                    // duplicate HNSW node (`insert_with_surrogate` never dedups),
                    // so skip it. Records above the watermark are the WAL tail
                    // the checkpoint has not yet absorbed and must replay.
                    let insert_index_key = CoreLoop::vector_index_key(
                        database_id,
                        tenant_id,
                        &collection,
                        &field_name,
                    );
                    if let Some(existing) = self.vector_collections.get(&insert_index_key)
                        && record_lsn <= existing.checkpoint_wal_lsn()
                    {
                        skipped += 1;
                        continue;
                    }
                    let surrogate = nodedb_types::Surrogate::new(surrogate_u32);
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
                            collection: collection.clone(),
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
                        tracing::warn!(
                            core = self.core_id,
                            %collection,
                            lsn = record_lsn,
                            "WAL vector replay: insert handler returned error; skipping"
                        );
                        skipped += 1;
                        continue;
                    }
                    // Advance the (possibly freshly created) collection's
                    // watermark so the next checkpoint records this replayed
                    // write and a subsequent restart does not re-apply it.
                    if let Some(coll) = self.vector_collections.get_mut(&insert_index_key) {
                        coll.note_checkpoint_lsn(record_lsn);
                    }
                    inserted += 1;
                } else if let Ok((collection, vector, dim, field_name, doc_id)) =
                    zerompk::from_msgpack::<(String, Vec<f32>, usize, String, Option<String>)>(
                        &record.payload,
                    )
                {
                    if tombstones.is_tombstoned(tenant_id, &collection, record_lsn) {
                        skipped += 1;
                        continue;
                    }
                    if vector.len() != dim {
                        tracing::warn!(
                            core = self.core_id,
                            %collection,
                            expected = dim,
                            actual = vector.len(),
                            "skipping WAL vector record: dimension mismatch"
                        );
                        continue;
                    }
                    let index_key = CoreLoop::vector_index_key(
                        database_id,
                        tenant_id,
                        &collection,
                        &field_name,
                    );
                    // Checkpoint watermark gate (see the surrogate arm above).
                    if let Some(existing) = self.vector_collections.get(&index_key)
                        && record_lsn <= existing.checkpoint_wal_lsn()
                    {
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
                                "no VectorParams found during WAL replay; using defaults"
                            );
                            HnswParams::default()
                        });
                    let index = self
                        .vector_collections
                        .entry(index_key)
                        .or_insert_with(|| VectorCollection::new(dim, params));
                    if index.dim() != dim {
                        tracing::warn!(
                            core = self.core_id,
                            %collection,
                            index_dim = index.dim(),
                            record_dim = dim,
                            "skipping WAL vector record: index dimension mismatch"
                        );
                        continue;
                    }
                    // WAL replay rebinds vectors on the local node;
                    // surrogate identity is restored via the dedicated
                    // `SurrogateBind` replay path. Engine inserts here are
                    // local-id-only and bind to `Surrogate::ZERO`.
                    let _ = doc_id;
                    index.insert_with_surrogate(vector, nodedb_types::Surrogate::ZERO);
                    index.note_checkpoint_lsn(record_lsn);
                    inserted += 1;
                } else if let Ok((collection, vector, dim)) =
                    zerompk::from_msgpack::<(String, Vec<f32>, usize)>(&record.payload)
                {
                    if tombstones.is_tombstoned(tenant_id, &collection, record_lsn) {
                        skipped += 1;
                        continue;
                    }
                    if vector.len() != dim {
                        tracing::warn!(
                            core = self.core_id,
                            %collection,
                            expected = dim,
                            actual = vector.len(),
                            "skipping WAL vector record: dimension mismatch"
                        );
                        continue;
                    }
                    let index_key =
                        CoreLoop::vector_index_key(database_id, tenant_id, &collection, "");
                    // Checkpoint watermark gate (see the surrogate arm above).
                    if let Some(existing) = self.vector_collections.get(&index_key)
                        && record_lsn <= existing.checkpoint_wal_lsn()
                    {
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
                                "no VectorParams found during WAL replay; using defaults"
                            );
                            HnswParams::default()
                        });
                    let index = self
                        .vector_collections
                        .entry(index_key)
                        .or_insert_with(|| VectorCollection::new(dim, params));
                    if index.dim() != dim {
                        tracing::warn!(
                            core = self.core_id,
                            %collection,
                            index_dim = index.dim(),
                            record_dim = dim,
                            "skipping WAL vector record: index dimension mismatch"
                        );
                        continue;
                    }
                    index.insert(vector);
                    index.note_checkpoint_lsn(record_lsn);
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
                    // Checkpoint watermark gate (see the surrogate arm above).
                    if let Some(existing) = self.vector_collections.get(&index_key)
                        && record_lsn <= existing.checkpoint_wal_lsn()
                    {
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
                    index.note_checkpoint_lsn(record_lsn);
                    inserted += 1;
                }
            } else if is_vector_delete {
                // Decode order (longest shape first for backward compatibility):
                //
                //   4-element: (collection, surrogate_u32, field_name, Option<SyncProvenance>)
                //     → sync-path delete-by-surrogate; routes through the handler so the
                //       idempotency gate fires on replay.
                //
                //   3-element: (collection, vector_id, Option<SyncProvenance>)
                //     → local delete-by-node-id with provenance (discarded here).
                //
                //   2-element: (collection, vector_id)
                //     → legacy shape; direct node-id deletion.
                if let Ok((collection, surrogate_u32, field_name, provenance)) =
                    zerompk::from_msgpack::<(
                        String,
                        u32,
                        String,
                        Option<nodedb_types::sync::wire::SyncProvenance>,
                    )>(&record.payload)
                {
                    if tombstones.is_tombstoned(tenant_id, &collection, record_lsn) {
                        skipped += 1;
                        continue;
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
                        PhysicalPlan::Vector(
                            nodedb_physical::physical_plan::VectorOp::DeleteBySurrogate {
                                collection: collection.clone(),
                                surrogate,
                                field_name: field_name.clone(),
                                provenance: provenance.clone(),
                            },
                        ),
                    );
                    let response = self.execute_vector_delete_by_surrogate(
                        &task,
                        tenant_id,
                        &collection,
                        surrogate,
                        &field_name,
                        provenance.as_ref(),
                    );
                    if response.status != crate::bridge::envelope::Status::Ok {
                        tracing::warn!(
                            core = self.core_id,
                            %collection,
                            lsn = record_lsn,
                            "WAL vector replay: delete-by-surrogate handler returned error; skipping"
                        );
                        skipped += 1;
                        continue;
                    }
                    deleted += 1;
                } else {
                    // Legacy: 3-element (with discarded provenance) or 2-element.
                    let delete_decoded = zerompk::from_msgpack::<(
                        String,
                        u32,
                        Option<nodedb_types::sync::wire::SyncProvenance>,
                    )>(&record.payload)
                    .map(|(c, id, _prov)| (c, id))
                    .or_else(|_| zerompk::from_msgpack::<(String, u32)>(&record.payload));
                    if let Ok((collection, vector_id)) = delete_decoded {
                        if tombstones.is_tombstoned(tenant_id, &collection, record_lsn) {
                            skipped += 1;
                            continue;
                        }
                        let index_key =
                            CoreLoop::vector_index_key(database_id, tenant_id, &collection, "");
                        if let Some(index) = self.vector_collections.get_mut(&index_key) {
                            index.delete(vector_id);
                            deleted += 1;
                        }
                    }
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

    /// Simulate `load_vector_checkpoints` restoring a checkpoint that already
    /// contains one vector, stamped with watermark `lsn`.
    fn restore_checkpoint(
        core: &mut CoreLoop,
        tenant_id: u64,
        collection: &str,
        vector: Vec<f32>,
        lsn: u64,
    ) {
        let dim = vector.len();
        let mut coll = VectorCollection::new(dim, HnswParams::default());
        coll.insert(vector);
        coll.note_checkpoint_lsn(lsn);
        // Round-trip through a checkpoint so the persisted watermark becomes the
        // replay gate (`checkpoint_wal_lsn`): save folds the applied watermark
        // into it, and load exposes it — faithfully simulating a restored
        // checkpoint (the gate is set only by load/save, never by a live
        // `note_checkpoint_lsn`, which feeds the separate applied watermark).
        let bytes = coll.checkpoint_to_bytes(None).unwrap();
        let coll = VectorCollection::from_checkpoint(&bytes, None).expect("decode checkpoint");
        let key = CoreLoop::vector_index_key(0, tenant_id, collection, "");
        core.vector_collections.insert(key, coll);
    }

    fn coll_len(core: &CoreLoop, tenant_id: u64, collection: &str) -> Option<usize> {
        let key = CoreLoop::vector_index_key(0, tenant_id, collection, "");
        core.vector_collections.get(&key).map(|c| c.len())
    }

    /// The regression: a WAL record at LSN N whose write the restored checkpoint
    /// (watermark N) already absorbed must NOT be replayed — otherwise the
    /// straddling segment's record appends a duplicate HNSW node. Before the
    /// checkpoint-LSN gate this left TWO copies.
    #[test]
    fn straddling_record_not_reapplied_over_checkpoint() {
        let mut h = make_core();
        restore_checkpoint(&mut h.core, 7, "emb", vec![1.0, 2.0, 3.0], 10);
        let rec = vector_put_record(10, 7, "emb", vec![1.0, 2.0, 3.0]);
        h.core.replay_vector_wal(
            std::slice::from_ref(&rec),
            1,
            &nodedb_wal::TombstoneSet::new(),
        );
        assert_eq!(
            coll_len(&h.core, 7, "emb"),
            Some(1),
            "a record at/below the restored checkpoint watermark must be skipped exactly once"
        );
    }

    /// A record above the restored watermark is the genuine WAL tail the
    /// checkpoint has not absorbed and MUST replay.
    #[test]
    fn record_above_watermark_still_replays() {
        let mut h = make_core();
        restore_checkpoint(&mut h.core, 7, "emb", vec![1.0, 2.0, 3.0], 10);
        let rec = vector_put_record(11, 7, "emb", vec![4.0, 5.0, 6.0]);
        h.core.replay_vector_wal(
            std::slice::from_ref(&rec),
            1,
            &nodedb_wal::TombstoneSet::new(),
        );
        assert_eq!(
            coll_len(&h.core, 7, "emb"),
            Some(2),
            "a record above the watermark is the WAL tail and must replay"
        );
    }

    /// A checkpoint restored for collection A must not suppress replay of a
    /// record for collection B, even when B's record LSN is below A's watermark.
    #[test]
    fn checkpoint_watermark_is_per_collection() {
        let mut h = make_core();
        restore_checkpoint(&mut h.core, 7, "col_a", vec![1.0, 2.0, 3.0], 10);
        // Collection B has no checkpoint; its record at LSN 5 (below A's
        // watermark of 10) must still replay.
        let rec = vector_put_record(5, 7, "col_b", vec![7.0, 8.0, 9.0]);
        h.core.replay_vector_wal(
            std::slice::from_ref(&rec),
            1,
            &nodedb_wal::TombstoneSet::new(),
        );
        assert_eq!(
            coll_len(&h.core, 7, "col_b"),
            Some(1),
            "collection A's watermark must not gate collection B's records"
        );
        assert_eq!(
            coll_len(&h.core, 7, "col_a"),
            Some(1),
            "collection A must be untouched by B's replay"
        );
    }
}
