// SPDX-License-Identifier: BUSL-1.1

//! Snapshot, checkpoint, WAL append, cancel, range scan, and collection policy handlers.

use sonic_rs;
use tracing::{debug, info, warn};

use crate::bridge::envelope::{ErrorCode, Response};
use crate::types::RequestId;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

/// Parameters for a document range scan (`field` within `[lower, upper)`).
pub(in crate::data::executor) struct RangeScanArgs<'a> {
    pub tid: u64,
    pub collection: &'a str,
    pub field: &'a str,
    pub lower: Option<&'a [u8]>,
    pub upper: Option<&'a [u8]>,
    pub limit: usize,
}

impl CoreLoop {
    pub(in crate::data::executor) fn execute_wal_append(
        &self,
        task: &ExecutionTask,
        payload: &[u8],
    ) -> Response {
        debug!(core = self.core_id, len = payload.len(), "wal append");
        self.response_ok(task)
    }

    pub(in crate::data::executor) fn execute_cancel(
        &mut self,
        task: &ExecutionTask,
        target_request_id: RequestId,
    ) -> Response {
        debug!(core = self.core_id, %target_request_id, "cancel");
        let pos = self
            .task_queue
            .iter()
            .position(|t| t.request_id() == target_request_id);
        if let Some(pos) = pos {
            self.task_queue.remove(pos);
        }
        self.response_ok(task)
    }

    /// Read the conflict resolution policy for a collection and return it as JSON.
    /// Returns the ephemeral default when no explicit policy has been registered.
    pub(in crate::data::executor) fn execute_get_collection_policy(
        &mut self,
        task: &ExecutionTask,
        collection: &str,
    ) -> Response {
        debug!(core = self.core_id, %collection, "get collection policy");
        let tenant_id = task.request.tenant_id;
        let engine = match self.get_crdt_engine(task.request.database_id, tenant_id) {
            Ok(e) => e,
            Err(e) => {
                warn!(core = self.core_id, error = %e, "failed to create CRDT engine for get policy");
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };
        let policy = engine.get_collection_policy(collection);
        match sonic_rs::to_string(&policy) {
            Ok(json) => self.response_with_payload(task, json.into_bytes()),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("failed to serialize policy: {e}"),
                },
            ),
        }
    }

    pub(in crate::data::executor) fn execute_set_collection_policy(
        &mut self,
        task: &ExecutionTask,
        collection: &str,
        policy_json: &str,
    ) -> Response {
        debug!(core = self.core_id, %collection, "set collection policy");
        let tenant_id = task.request.tenant_id;
        let engine = match self.get_crdt_engine(task.request.database_id, tenant_id) {
            Ok(e) => e,
            Err(e) => {
                warn!(core = self.core_id, error = %e, "failed to create CRDT engine");
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };
        match engine.set_collection_policy(collection, policy_json) {
            Ok(()) => self.response_ok(task),
            Err(e) => {
                warn!(core = self.core_id, error = %e, "set collection policy failed");
                self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                )
            }
        }
    }

    pub(in crate::data::executor) fn execute_range_scan(
        &mut self,
        task: &ExecutionTask,
        args: RangeScanArgs<'_>,
    ) -> Response {
        // Bitemporal collections keep every write on the versioned redb table;
        // their plain INDEXES / DOCUMENTS tables are empty, so the index probe
        // + plain-table fallback below would return ZERO rows. Route them to
        // the versioned current-state scan instead (full-scan + range filter,
        // matching `execute_document_scan`).
        if self.is_bitemporal(task.request.database_id.as_u64(), args.tid, args.collection) {
            return self.execute_range_scan_bitemporal(task, args);
        }

        let RangeScanArgs {
            tid,
            collection,
            field,
            lower,
            upper,
            limit,
        } = args;
        debug!(core = self.core_id, %collection, %field, limit, "range scan");

        // Try index-backed range scan first.
        let results =
            match self
                .sparse
                .range_scan(crate::engine::sparse::btree_index::RangeScanParams {
                    database_id: task.request.database_id.as_u64(),
                    tenant_id: tid,
                    collection,
                    field,
                    lower,
                    upper,
                    limit,
                }) {
                Ok(r) => r,
                Err(e) => {
                    warn!(core = self.core_id, error = %e, "sparse range scan failed");
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    );
                }
            };

        // If the index returned nothing, fall back to full scan + sort.
        // This handles collections without a secondary index on `field`.
        if results.is_empty() {
            let scan_result = self.scan_collection(
                task.request.database_id.as_u64(),
                tid,
                collection,
                limit.max(1000),
            );
            match scan_result {
                Ok(mut docs) => {
                    if let Err(e) = super::super::document::sort::sort_rows(
                        &mut docs,
                        &[(field.to_string(), true)],
                    ) {
                        return self.response_error(
                            task,
                            ErrorCode::Internal {
                                detail: format!("in-memory sort failed: {e}"),
                            },
                        );
                    }
                    docs.truncate(limit);
                    // Raw msgpack passthrough — no decode/re-encode.
                    let rows: Vec<_> = docs
                        .iter()
                        .map(|(id, val)| {
                            let mp = super::super::super::doc_format::json_to_msgpack(val);
                            (id.clone(), mp)
                        })
                        .collect();
                    match super::super::super::response_codec::encode_raw_document_rows(&rows) {
                        Ok(payload) => return self.response_with_payload(task, payload),
                        Err(e) => {
                            return self.response_error(
                                task,
                                ErrorCode::Internal {
                                    detail: e.to_string(),
                                },
                            );
                        }
                    }
                }
                Err(e) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    );
                }
            }
        }

        match super::super::super::response_codec::encode(&results) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => {
                warn!(core = self.core_id, error = %e, "range scan serialization failed");
                self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                )
            }
        }
    }

    /// Execute a snapshot creation request: export all engine state as bytes.
    ///
    /// Returns the serialized `CoreSnapshot` as the response payload.
    /// The Control Plane collects these from all cores and writes to disk.
    pub(in crate::data::executor) fn execute_create_snapshot(
        &self,
        task: &ExecutionTask,
    ) -> Response {
        match self.export_snapshot() {
            Ok(snapshot) => match snapshot.to_bytes() {
                Ok(bytes) => {
                    info!(
                        core = self.core_id,
                        watermark = snapshot.watermark,
                        documents = snapshot.sparse_documents.len(),
                        vectors = snapshot.hnsw_indexes.len(),
                        size_bytes = bytes.len(),
                        "snapshot exported"
                    );
                    self.response_with_payload(task, bytes)
                }
                Err(e) => {
                    warn!(core = self.core_id, error = %e, "snapshot serialization failed");
                    self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    )
                }
            },
            Err(e) => {
                warn!(core = self.core_id, error = %e, "snapshot export failed");
                self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                )
            }
        }
    }

    /// Execute a coordinated checkpoint: flush all engine state to disk
    /// and return this core's checkpoint LSN.
    ///
    /// 1. Checkpoint KV collections (hash tables → disk files).
    /// 2. Checkpoint sparse vector indexes (inverted indexes → disk files).
    /// 3. Checkpoint the sync idempotency gate (HWM + epoch maps → disk file).
    /// 4. Checkpoint columnar collections (memtables + PK indexes + delete
    ///    bitmaps + in-memory flushed segment bytes → disk files).
    /// 5. Checkpoint the CSR graph node labels (label bitsets → disk file).
    /// 6. Checkpoint the array engines (memtables → on-disk tile segments).
    /// 7. Checkpoint the timeseries engines (columnar memtables → on-disk L1
    ///    partitions).
    /// 8. Checkpoint vector indexes (HNSW segments → disk files).
    /// 9. Export CRDT snapshots (Loro docs → disk files).
    /// 10. Flush spatial R-tree indexes to disk.
    /// 11. redb sparse engine is already ACID — no action needed. Neither is the
    ///     full-text `inverted` index, for the same reason and in the same
    ///     store: both its write paths commit POSTINGS / DOC_LENGTHS / STATS
    ///     into that redb `Database`, so it has nothing memory-only to flush.
    /// 12. Graph EDGES are committed to the redb edge store at apply time and
    ///     rebuilt into the CSR on startup — no action needed. Their node
    ///     LABELS are not (step 5).
    /// 13. Return the LSN below which ALL of this core's state is durable.
    ///
    /// ## The LSN this returns is a deletion authority, not a progress report
    ///
    /// The checkpoint manager takes the minimum of every core's reply and hands
    /// it to `WalManager::truncate_before`, which unlinks the sealed segments
    /// below it. So the returned LSN must satisfy one rule:
    ///
    /// > every byte of this core's state at or below the returned LSN is
    /// > recoverable WITHOUT the WAL — either flushed by this checkpoint, or
    /// > held in a durable store that is not the WAL.
    ///
    /// Reporting the watermark unconditionally violated that rule for any engine
    /// whose flush was partial or absent, and deleted the only copy of its
    /// state. The LSN is therefore computed as `min(watermark, every engine's
    /// reported durable LSN)`: a flush that fails clamps the LSN instead of
    /// silently widening the deletion.
    pub(in crate::data::executor) fn execute_checkpoint(
        &mut self,
        task: &ExecutionTask,
    ) -> Response {
        // Every engine on this core whose flush can fail contributes the LSN it
        // is durable through. The watermark is the ceiling — no engine can be
        // durable past writes this core has not seen — and every contribution
        // can only pull it DOWN.
        //
        // No engine is left contributing nothing on the grounds that its flush
        // reports no LSN; that gap is closed. The only state on this core with
        // no contributor is state that needs none because the WAL is not its
        // only other copy, and there are exactly three such cases:
        //
        //   - the redb `sparse` document store, which is ACID at apply time;
        //   - the full-text `inverted` index, which is the SAME redb database —
        //     both its write paths bypass the LSM memtable and commit postings
        //     with the document write, so it has nothing memory-only to flush
        //     and no flush to fail;
        //   - graph EDGES, committed to the redb `EdgeStore` at apply time and
        //     rebuilt into the CSR in `CoreLoop::open` (their node LABELS have
        //     no such store, hence step 3c).
        //
        // A partial rebuild does NOT earn an engine a place on that list. Both
        // vector and spatial have one — the HNSW re-indexes documents from
        // `sparse`, the R-tree re-derives columnar geometry from restored rows —
        // and both still contribute below, because each also holds state its
        // rebuild cannot see (vectors inserted without a document; a document
        // collection's geometry). An engine is only safe if the rebuild covers
        // ALL of it.
        // 1. Flush KV collections to disk, seeding the fold.
        let mut durable_lsns: Vec<crate::types::Lsn> = vec![self.checkpoint_kv_durable_lsn()];

        // 2. Flush the sparse-vector indexes. Same shape as KV — in-memory
        //    state whose only durable copy was the WAL. Its flush used to hang
        //    off an INDEPENDENT timer in `data/runtime.rs`, which is why it had
        //    to move here: a flush that is not ordered against the truncation
        //    it authorises is not a checkpoint, whatever its period.
        durable_lsns.push(self.checkpoint_sparse_vector_durable_lsn());

        // 3. Flush the sync idempotency gate. Rebuilt at boot only from the
        //    `SyncSeqAdvance` WAL records, so truncating them reset it and
        //    already-applied sync frames were admitted a second time.
        durable_lsns.push(self.checkpoint_sync_hwm_durable_lsn());

        // 3b. Flush the columnar engines. Memory-only on both halves — the live
        //     memtables in `columnar_engines` AND the encoded segment bytes in
        //     `columnar_flushed_segments`, which no code path writes to disk.
        //     Unlike the three above, columnar replay is not idempotent
        //     (`ColumnarOp::Update` is delete-old-PK + insert-new-row), so this
        //     LSN also becomes the restored replay floor.
        durable_lsns.push(self.checkpoint_columnar_durable_lsn());

        // 3c. Flush the CSR graph node labels. Edges are redb-backed and
        //     rebuilt into the CSR from the `EdgeStore` at boot, so they are not
        //     exported; the label bitset has no store behind it at all, which is
        //     why `wal_replay_graph_labels.rs` exists as a standalone pass — and
        //     why truncating the records it replays lost the labels outright
        //     while the nodes and edges around them survived.
        durable_lsns.push(self.checkpoint_graph_label_durable_lsn());

        // 3d. Flush the array engines. Same memory-only memtable as columnar,
        //     but the durable form already exists: this calls the very
        //     `ArrayEngine::flush` an `NDARRAY_FLUSH` calls, whose segments the
        //     boot path already mmaps. The hazard was that nothing but that
        //     explicit user command ever invoked it.
        durable_lsns.push(self.checkpoint_array_durable_lsn());

        // 3e. Flush the timeseries engines. Same memory-only columnar memtable
        //     as the plain columnar profile, but — like the array engine — the
        //     durable form already exists: this calls the very
        //     `flush_ts_collection` the ingest path's 64 MiB threshold and the
        //     idle timer in `handlers/compact/maintenance.rs` call, whose L1
        //     partitions `load_ts_registries` reads back at boot. The hazard was
        //     that a threshold and a timer are not ordered against the
        //     truncation this checkpoint authorises: a collection ingesting
        //     steadily below the threshold had every row of the last idle window
        //     in memory only.
        durable_lsns.push(self.checkpoint_ts_durable_lsn());

        // 4. Flush the vector indexes. `rebuild_vector_indexes_from_store`
        //    re-indexes every document of a vector-indexed collection from redb
        //    at boot, so document-borne vectors survive a lost checkpoint. It
        //    is not a full backstop: `VectorOp::Insert` writes a bare vector
        //    with no document behind it, and that path never touches `sparse`,
        //    so the boot scan cannot see those vectors at all.
        durable_lsns.push(self.checkpoint_vector_durable_lsn());

        // 5. Flush the CRDT engines. No store of any kind behind the `LoroDoc`s
        //    — `load_crdt_checkpoints` plus the WAL deltas above it are the only
        //    two sources — so this is the KV situation exactly, with a quieter
        //    failure: the documents come back at the last checkpoint's version
        //    and every edit since it is simply missing.
        durable_lsns.push(self.checkpoint_crdt_durable_lsn());

        // 6. Flush the spatial R-trees. The geometry of a columnar-family
        //    (`engine='spatial'`) collection does not depend on this file
        //    surviving: `columnar_checkpoint/geometry_restore.rs` rebuilds it
        //    from the restored columnar rows at boot. A DOCUMENT collection's
        //    geometry is rebuilt at boot by WAL redo of its document `Put`s (the
        //    same `apply_point_put_spatial` side-effect as the live write), but
        //    only for `Put`s still in the WAL — nothing re-derives it from the
        //    redb `sparse` store, so this checkpoint is the R-tree's only copy of
        //    any row whose `Put` the WAL has already truncated past.
        durable_lsns.push(self.checkpoint_spatial_durable_lsn());

        // 7. Compact CSR write buffers into dense arrays for clean state. This
        //    is an in-memory layout change ONLY — it merges each partition's
        //    write buffer into its dense adjacency arrays and touches no file.
        //    It contributes nothing to the durable LSN and must not be read as
        //    if it did: what makes the CSR survive a restart is the redb
        //    `EdgeStore` for edges, and step 3c's checkpoint for node labels.
        //    Its failure is therefore only a missed optimisation.
        if let Err(e) = self.csr.compact_all() {
            tracing::warn!(error = %e, "CSR compaction rejected by memory governor during snapshot; skipping");
        }

        // 8. Clamp the watermark down to every engine's durable LSN. `min` and
        //    not `max`: the reported LSN authorises deletion, so where the
        //    engines disagree the WAL must keep whatever the least-durable one
        //    still needs.
        let checkpoint_lsn = durable_lsns
            .iter()
            .fold(self.watermark, |acc, lsn| acc.min(*lsn))
            .as_u64();

        // The checkpoint coordinator is deliberately NOT told about this LSN.
        // It schedules flushes; it does not record durability. Its per-engine
        // dirty-page counters were already settled by the contributors above,
        // which are the only places that know whether the flush they ran landed.
        info!(
            core = self.core_id,
            checkpoint_lsn,
            watermark = self.watermark.as_u64(),
            kv_durable_lsn = self.floors.kv_durable_lsn.as_u64(),
            sparse_vector_durable_lsn = self.floors.sparse_vector_durable_lsn.as_u64(),
            sync_hwm_durable_lsn = self.floors.sync_hwm_durable_lsn.as_u64(),
            columnar_durable_lsn = self.floors.columnar_durable_lsn.as_u64(),
            graph_label_durable_lsn = self.floors.graph_label_durable_lsn.as_u64(),
            array_durable_lsn = self.floors.array_durable_lsn.as_u64(),
            ts_durable_lsn = self.floors.ts_durable_lsn.as_u64(),
            vector_durable_lsn = self.floors.vector_durable_lsn.as_u64(),
            crdt_durable_lsn = self.floors.crdt_durable_lsn.as_u64(),
            spatial_durable_lsn = self.floors.spatial_durable_lsn.as_u64(),
            dirty_pages = self.checkpoint_coordinator.total_dirty_pages(),
            "core checkpoint complete"
        );

        // Return the checkpoint LSN as the response payload.
        let payload = checkpoint_lsn.to_le_bytes().to_vec();
        self.response_with_payload(task, payload)
    }
}
