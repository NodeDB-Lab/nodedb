// SPDX-License-Identifier: BUSL-1.1

//! Per-engine durable-LSN contributors for the coordinated checkpoint.
//!
//! One method per engine whose state is memory-only with the WAL as its ONLY
//! other durable copy. Each flushes its engine and answers the single question
//! `execute_checkpoint` folds into the LSN it reports:
//!
//! > what is the highest LSN whose effects on this engine are recoverable
//! > WITHOUT the WAL?
//!
//! They share one shape, and it is the shape the data-loss bug came from
//! violating. On success the engine's `*_durable_lsn` advances to the flushed
//! point and that point is returned. On FAILURE the error is surfaced — logged
//! at `warn` naming the clamp it caused, never swallowed — and the LAST-KNOWN
//! durable LSN is returned instead of the watermark. The reported LSN authorises
//! `WalManager::truncate_before` to unlink segments below it, so a flush that
//! failed must never widen that authority over the very state it failed to
//! write. Clamping costs WAL growth until the next cycle succeeds; not clamping
//! costs the data.
//!
//! Every engine whose flush can fail now has a contributor here. The one engine
//! with state on this core and NO contributor is full-text search, and it needs
//! none: `InvertedIndex` is not flushed at all, because it is not memory-only.
//! Both of its write paths (`index_document`, `index_document_in_txn`) write
//! POSTINGS / DOC_LENGTHS / STATS straight into the same redb `Database` the
//! `sparse` engine commits to — bypassing the LSM memtable precisely so the
//! index is atomic with the document write. redb commits durably, so an FTS
//! write at or below the watermark is already on stable storage in a store that
//! is not the WAL. There is nothing to flush and therefore nothing to clamp.

use tracing::warn;

use crate::data::executor::core_loop::CoreLoop;
use crate::types::Lsn;

impl CoreLoop {
    /// Flush the KV engine and return the LSN it is durable through.
    ///
    /// `KvEngine` is pure in-memory state with no redb store behind it, so
    /// before its checkpoint existed the WAL held the only copy of every KV row
    /// and truncation destroyed it outright.
    pub(super) fn checkpoint_kv_durable_lsn(&mut self) -> Lsn {
        match self.checkpoint_kv_engines() {
            Ok(lsn) => {
                self.floors.kv_durable_lsn = lsn;
                lsn
            }
            Err(e) => {
                warn!(
                    core = self.core_id,
                    error = %e,
                    clamped_to = self.floors.kv_durable_lsn.as_u64(),
                    watermark = self.watermark.as_u64(),
                    "KV checkpoint flush failed; clamping this core's checkpoint LSN to the \
                     last LSN KV is durable through so WAL truncation cannot delete the only \
                     copy of unflushed KV state"
                );
                self.floors.kv_durable_lsn
            }
        }
    }

    /// Flush the sparse-vector indexes and return the LSN they are durable
    /// through.
    ///
    /// Same shape as KV — in-memory indexes whose only durable copy is the
    /// `SparseVectorPut` / `SparseVectorDelete` records. Clamping here keeps the
    /// segments holding the records this flush failed to write out.
    pub(super) fn checkpoint_sparse_vector_durable_lsn(&mut self) -> Lsn {
        match self.checkpoint_sparse_vector_indexes() {
            Ok(lsn) => {
                self.floors.sparse_vector_durable_lsn = lsn;
                lsn
            }
            Err(e) => {
                warn!(
                    core = self.core_id,
                    error = %e,
                    clamped_to = self.floors.sparse_vector_durable_lsn.as_u64(),
                    watermark = self.watermark.as_u64(),
                    "sparse vector checkpoint flush failed; clamping this core's checkpoint \
                     LSN to the last LSN the sparse-vector indexes are durable through so \
                     WAL truncation cannot delete the only copy of unflushed sparse-vector \
                     state"
                );
                self.floors.sparse_vector_durable_lsn
            }
        }
    }

    /// Flush the sync idempotency gate and return the LSN it is durable through.
    ///
    /// Guards a different failure from the other two: the gate's only other
    /// durable copy is the `SyncSeqAdvance` records, and deleting those does not
    /// lose a row — it resets every high-watermark to zero, after which frames a
    /// producer has already had applied and acknowledged are admitted and
    /// applied AGAIN.
    pub(super) fn checkpoint_sync_hwm_durable_lsn(&mut self) -> Lsn {
        match self.checkpoint_sync_hwm() {
            Ok(lsn) => {
                self.floors.sync_hwm_durable_lsn = lsn;
                lsn
            }
            Err(e) => {
                warn!(
                    core = self.core_id,
                    error = %e,
                    clamped_to = self.floors.sync_hwm_durable_lsn.as_u64(),
                    watermark = self.watermark.as_u64(),
                    "sync HWM checkpoint flush failed; clamping this core's checkpoint LSN \
                     to the last LSN the idempotency gate is durable through so WAL \
                     truncation cannot delete the SyncSeqAdvance records that are its only \
                     other copy — losing them re-applies already-acknowledged sync frames"
                );
                self.floors.sync_hwm_durable_lsn
            }
        }
    }

    /// Flush the columnar engines and return the LSN they are durable through.
    ///
    /// Columnar is memory-only on BOTH halves: `columnar_engines` holds the live
    /// memtable, PK index and delete bitmaps, and `columnar_flushed_segments`
    /// holds the encoded bytes of every flushed segment in a `HashMap` that was
    /// never written to disk. Neither has a store behind it, so before this
    /// checkpoint the WAL was the only copy of every columnar row while columnar
    /// writes advanced the watermark that authorised deleting it.
    ///
    /// Clamping matters more here than for the engines above, because columnar
    /// replay is not idempotent: `ColumnarOp::Update` is delete-old-PK +
    /// insert-new-row. So the reported LSN both authorises truncation AND, via
    /// the restored floor, decides which records replay. Overstating it would
    /// not merely delete rows — it would gate the records that would have
    /// rebuilt them.
    pub(super) fn checkpoint_columnar_durable_lsn(&mut self) -> Lsn {
        match self.checkpoint_columnar_engines() {
            Ok(lsn) => {
                self.floors.columnar_durable_lsn = lsn;
                lsn
            }
            Err(e) => {
                warn!(
                    core = self.core_id,
                    error = %e,
                    clamped_to = self.floors.columnar_durable_lsn.as_u64(),
                    watermark = self.watermark.as_u64(),
                    "columnar checkpoint flush failed; clamping this core's checkpoint LSN \
                     to the last LSN the columnar engines are durable through so WAL \
                     truncation cannot delete the only copy of unflushed columnar rows — \
                     both the live memtables and the flushed segment bytes held only in \
                     memory"
                );
                self.floors.columnar_durable_lsn
            }
        }
    }

    /// Flush the CSR graph node labels and return the LSN they are durable
    /// through.
    ///
    /// Narrower than the engines above: graph EDGES are durable without this,
    /// committed to the redb `EdgeStore` at apply time and rebuilt into the CSR
    /// from it in `CoreLoop::open`. The node-label bitset is the part with no
    /// store behind it, so a `GraphNodeLabelSet` record is its only durable
    /// copy while a label write advances the watermark that authorised deleting
    /// it. Its failure is quiet: the node and its edges come back, only the
    /// label is gone, so `MATCH (a:Person)` silently stops matching a node it
    /// matched before the restart.
    pub(super) fn checkpoint_graph_label_durable_lsn(&mut self) -> Lsn {
        match self.checkpoint_graph_labels() {
            Ok(lsn) => {
                self.floors.graph_label_durable_lsn = lsn;
                lsn
            }
            Err(e) => {
                warn!(
                    core = self.core_id,
                    error = %e,
                    clamped_to = self.floors.graph_label_durable_lsn.as_u64(),
                    watermark = self.watermark.as_u64(),
                    "graph node-label checkpoint flush failed; clamping this core's \
                     checkpoint LSN to the last LSN the labels are durable through so WAL \
                     truncation cannot delete the GraphNodeLabelSet records that are \
                     their only other copy"
                );
                self.floors.graph_label_durable_lsn
            }
        }
    }

    /// Flush the array engine and return the LSN it is durable through.
    ///
    /// Unlike the engines above this writes no checkpoint file of its own: the
    /// array engine's durable form IS its on-disk segments, and
    /// `checkpoint_array_engines` calls the same `ArrayEngine::flush` an
    /// `NDARRAY_FLUSH` does. The bug it closes is that the flush was reachable
    /// ONLY by that explicit command, so every cell written since the last one a
    /// user happened to run sat in a memory-only memtable while its `ArrayPut`
    /// records were truncated out from under it.
    pub(super) fn checkpoint_array_durable_lsn(&mut self) -> Lsn {
        match self.checkpoint_array_engines() {
            Ok(lsn) => {
                self.floors.array_durable_lsn = lsn;
                lsn
            }
            Err(e) => {
                warn!(
                    core = self.core_id,
                    error = %e,
                    clamped_to = self.floors.array_durable_lsn.as_u64(),
                    watermark = self.watermark.as_u64(),
                    "array checkpoint flush failed; clamping this core's checkpoint LSN \
                     to the last LSN the array engine is durable through so WAL \
                     truncation cannot delete the only copy of cells still held in a \
                     memory-only memtable"
                );
                self.floors.array_durable_lsn
            }
        }
    }

    /// Flush the timeseries memtables and return the LSN they are durable
    /// through.
    ///
    /// Writes no checkpoint file either: like the array engine, timeseries
    /// already has a durable form — the L1 partitions `flush_ts_collection`
    /// encodes — and a boot path that reads them back (`load_ts_registries`).
    /// The bug it closes is that the only things that ever called that flush
    /// were the ingest path's 64 MiB threshold and a 5-second idle timer, so a
    /// collection ingesting steadily below the threshold kept every row of the
    /// last idle window in a memory-only memtable while the checkpoint deleted
    /// the `TimeseriesBatch` records that were their only copy.
    pub(super) fn checkpoint_ts_durable_lsn(&mut self) -> Lsn {
        match self.checkpoint_timeseries_memtables() {
            Ok(lsn) => {
                self.floors.ts_durable_lsn = lsn;
                lsn
            }
            Err(e) => {
                warn!(
                    core = self.core_id,
                    error = %e,
                    clamped_to = self.floors.ts_durable_lsn.as_u64(),
                    watermark = self.watermark.as_u64(),
                    "timeseries checkpoint flush failed; clamping this core's checkpoint LSN \
                     to the last LSN the timeseries engine is durable through so WAL \
                     truncation cannot delete the only copy of rows still held in a \
                     memory-only memtable"
                );
                self.floors.ts_durable_lsn
            }
        }
    }

    /// Flush the vector indexes and return the LSN they are durable through.
    ///
    /// Narrower than KV, and narrower than it looks. Most of the HNSW has a
    /// genuine rebuild behind it: `rebuild_vector_indexes_from_store` re-indexes
    /// every document of every `CREATE VECTOR INDEX` collection from the durable
    /// redb `sparse` store at boot, so a vector that arrived inside a document
    /// comes back with or without this checkpoint.
    ///
    /// What that rebuild cannot see is a vector that never was a document.
    /// `VectorOp::Insert` carries `(vector, dim, field, surrogate, pk_bytes)`
    /// and writes only into `vector_collections`; no row lands in `sparse`, so
    /// the boot scan finds nothing to re-index. For those vectors the checkpoint
    /// file and the `VectorOp::Insert` records are the only two copies, which is
    /// exactly the KV situation and takes the KV answer.
    ///
    /// The coordinator's dirty-page counter is settled here rather than by the
    /// caller because only this arm knows the flush actually landed: a failed
    /// flush wrote nothing and must leave the pages dirty so the next
    /// maintenance tick retries them.
    pub(super) fn checkpoint_vector_durable_lsn(&mut self) -> Lsn {
        match self.checkpoint_vector_indexes() {
            Ok(outcome) => {
                self.checkpoint_coordinator
                    .record_flush("vector", outcome.files_written);
                self.floors.vector_durable_lsn = outcome.durable_lsn;
                outcome.durable_lsn
            }
            Err(e) => {
                warn!(
                    core = self.core_id,
                    error = %e,
                    clamped_to = self.floors.vector_durable_lsn.as_u64(),
                    watermark = self.watermark.as_u64(),
                    "vector checkpoint flush failed; clamping this core's checkpoint LSN to \
                     the last LSN the vector indexes are durable through so WAL truncation \
                     cannot delete the VectorOp::Insert records that are the only other copy \
                     of every vector written without a document behind it"
                );
                self.floors.vector_durable_lsn
            }
        }
    }

    /// Flush the CRDT tenant engines and return the LSN they are durable
    /// through.
    ///
    /// The purest form of the KV shape: `TenantCrdtEngine` is in-memory
    /// `LoroDoc`s with no store of any kind behind them, restored at boot ONLY
    /// by `load_crdt_checkpoints` plus the WAL deltas above it. Its failure is
    /// quiet in the way the graph labels' is — nothing errors at read time. The
    /// documents come back at the version of the last checkpoint that actually
    /// landed, and every edit made since it is simply not there.
    pub(super) fn checkpoint_crdt_durable_lsn(&mut self) -> Lsn {
        match self.checkpoint_crdt_engines() {
            Ok(outcome) => {
                self.checkpoint_coordinator
                    .record_flush("crdt", outcome.files_written);
                self.floors.crdt_durable_lsn = outcome.durable_lsn;
                outcome.durable_lsn
            }
            Err(e) => {
                warn!(
                    core = self.core_id,
                    error = %e,
                    clamped_to = self.floors.crdt_durable_lsn.as_u64(),
                    watermark = self.watermark.as_u64(),
                    "CRDT checkpoint flush failed; clamping this core's checkpoint LSN to the \
                     last LSN the CRDT engines are durable through so WAL truncation cannot \
                     delete the delta records that are the only other copy of the Loro state \
                     this flush failed to write"
                );
                self.floors.crdt_durable_lsn
            }
        }
    }

    /// Flush the spatial R-trees and return the LSN they are durable through.
    ///
    /// Scoped, like the graph labels, to the half with no rebuild independent of
    /// this file. `spatial_indexes` is fed by two paths:
    /// `index_columnar_geometry_columns` for a columnar-family (`engine='spatial'`)
    /// collection, whose entries `restore_columnar_geometry_indexes` re-derives
    /// from the rows the columnar checkpoint restored; and `apply_point_put_spatial`
    /// for a DOCUMENT collection's geometry field.
    ///
    /// A document collection's entries ARE rebuilt at boot — the same
    /// `apply_point_put_spatial` side-effect runs on the WAL redo path, so every
    /// document `Put` still in the WAL re-indexes into the R-tree. But nothing
    /// re-derives them from the redb `sparse` store, so once the WAL is truncated
    /// below a row's `Put` this checkpoint is that row's only surviving R-tree
    /// copy. The reported LSN gates that truncation, so for the document half this
    /// checkpoint and the un-truncated `Put` records are the only two copies and
    /// the reported LSN must respect that.
    ///
    /// No dirty-page accounting here, unlike the vector and CRDT arms: no write
    /// handler marks a "spatial" engine dirty, so the coordinator does not track
    /// one and has no counter for a flush to work off. Reporting a flush against
    /// an engine nothing ever marks dirty would be bookkeeping with no reader.
    pub(super) fn checkpoint_spatial_durable_lsn(&mut self) -> Lsn {
        match self.checkpoint_spatial_indexes() {
            Ok(outcome) => {
                self.floors.spatial_durable_lsn = outcome.durable_lsn;
                outcome.durable_lsn
            }
            Err(e) => {
                warn!(
                    core = self.core_id,
                    error = %e,
                    clamped_to = self.floors.spatial_durable_lsn.as_u64(),
                    watermark = self.watermark.as_u64(),
                    "spatial checkpoint flush failed; clamping this core's checkpoint LSN to \
                     the last LSN the R-trees are durable through so WAL truncation cannot \
                     delete the only other copy of the geometry entries this flush failed to \
                     write — losing them stops spatial predicates matching rows a full scan \
                     still returns"
                );
                self.floors.spatial_durable_lsn
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use nodedb_array::types::ArrayId;
    use nodedb_bridge::buffer::RingBuffer;

    use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};
    use crate::data::executor::core_loop::CoreLoop;
    use crate::types::Lsn;

    fn open_core(dir: &std::path::Path) -> CoreLoop {
        let (_req_tx, req_rx) = RingBuffer::channel::<BridgeRequest>(64);
        let (resp_tx, _resp_rx) = RingBuffer::channel::<BridgeResponse>(64);
        CoreLoop::open(
            0,
            req_rx,
            resp_tx,
            dir,
            std::sync::Arc::new(nodedb_types::OrdinalClock::new()),
        )
        .expect("CoreLoop::open")
    }

    /// The clamp is the whole point of this module: on flush failure the
    /// contributor must return the LAST-KNOWN durable LSN, never the watermark.
    /// Returning the watermark would widen `WalManager::truncate_before` over
    /// exactly the state the flush just failed to write.
    #[test]
    fn columnar_flush_failure_clamps_to_the_last_known_durable_lsn() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut core = open_core(dir.path());
        core.watermark = Lsn::new(900);

        // Occupy the checkpoint directory's path with a FILE so the flush's
        // `create_dir_all` fails and it can publish nothing.
        std::fs::write(dir.path().join("columnar-ckpt"), b"not a directory")
            .expect("write blocking file");

        assert_eq!(
            core.checkpoint_columnar_durable_lsn(),
            Lsn::ZERO,
            "a fresh core has flushed nothing, so a failed flush must clamp to \
             zero rather than authorise truncating up to the watermark"
        );
        assert_eq!(core.floors.columnar_durable_lsn, Lsn::ZERO);
    }

    /// A successful flush advances the contributor's field AND returns the same
    /// LSN — the two must not drift, since the field is what a later failure
    /// clamps back to.
    #[test]
    fn columnar_flush_success_advances_and_returns_the_watermark() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut core = open_core(dir.path());
        core.watermark = Lsn::new(750);

        assert_eq!(core.checkpoint_columnar_durable_lsn(), Lsn::new(750));
        assert_eq!(core.floors.columnar_durable_lsn, Lsn::new(750));
    }

    /// Same clamp for the graph node labels: a failed flush must not authorise
    /// deleting the `GraphNodeLabelSet` records that are the labels' only other
    /// copy.
    #[test]
    fn graph_label_flush_failure_clamps_to_the_last_known_durable_lsn() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut core = open_core(dir.path());
        core.watermark = Lsn::new(900);
        core.csr_partition_mut(0, 7)
            .add_node_label("alice", "Person")
            .expect("label a node so the flush has state to lose");

        // Occupy the checkpoint directory's path with a FILE so the flush's
        // `create_dir_all` fails and it can publish nothing.
        std::fs::write(dir.path().join("graph-label-ckpt"), b"not a directory")
            .expect("write blocking file");

        assert_eq!(
            core.checkpoint_graph_label_durable_lsn(),
            Lsn::ZERO,
            "a fresh core has flushed nothing, so a failed flush must clamp to \
             zero rather than authorise truncating up to the watermark"
        );
        assert_eq!(core.floors.graph_label_durable_lsn, Lsn::ZERO);
    }

    #[test]
    fn graph_label_flush_success_advances_and_returns_the_watermark() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut core = open_core(dir.path());
        core.watermark = Lsn::new(750);

        assert_eq!(core.checkpoint_graph_label_durable_lsn(), Lsn::new(750));
        assert_eq!(core.floors.graph_label_durable_lsn, Lsn::new(750));
    }

    /// Open an array holding one un-flushed cell, and return its id and the
    /// engine-root directory whose loss makes the next flush fail.
    fn open_array_with_a_pending_cell(core: &mut CoreLoop) -> (ArrayId, std::path::PathBuf) {
        use nodedb_array::schema::ArraySchemaBuilder;
        use nodedb_array::schema::attr_spec::{AttrSpec, AttrType};
        use nodedb_array::schema::dim_spec::{DimSpec, DimType};
        use nodedb_array::types::cell_value::value::CellValue;
        use nodedb_array::types::coord::value::CoordValue;
        use nodedb_array::types::domain::{Domain, DomainBound};

        let schema = ArraySchemaBuilder::new("grid")
            .dim(DimSpec::new(
                "x",
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(15)),
            ))
            .attr(AttrSpec::new("v", AttrType::Int64, true))
            .tile_extents(vec![4])
            .build()
            .expect("build schema");
        let id = ArrayId::new(nodedb_types::TenantId::new(1), "grid");
        core.array_engine
            .open_array(id.clone(), std::sync::Arc::new(schema), 0xA55E7)
            .expect("open array");
        core.array_engine
            .put_cells(
                &id,
                vec![crate::engine::array::wal::ArrayPutCell {
                    coord: vec![CoordValue::Int64(1)],
                    attrs: vec![CellValue::Int64(7)],
                    surrogate: nodedb_types::Surrogate::ZERO,
                    system_from_ms: 1,
                    valid_from_ms: 0,
                    valid_until_ms: i64::MAX,
                }],
                10,
            )
            .expect("put a cell so the memtable is non-empty");
        let root = core.array_engine.config().root.clone();
        (id, root)
    }

    /// The array engine's durable form is its segments, so a flush that cannot
    /// write one must clamp exactly like the file-based checkpoints: the cell it
    /// failed to persist still lives only in the memtable and the WAL.
    #[test]
    fn array_flush_failure_clamps_to_the_last_known_durable_lsn() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut core = open_core(dir.path());
        core.watermark = Lsn::new(900);
        let (_id, root) = open_array_with_a_pending_cell(&mut core);

        // Remove the engine root out from under the open store so the segment
        // write has nowhere to land.
        std::fs::remove_dir_all(&root).expect("remove array engine root");

        assert_eq!(
            core.checkpoint_array_durable_lsn(),
            Lsn::ZERO,
            "a flush that could not write its segment must clamp to zero rather \
             than authorise truncating the ArrayPut records for the cell it lost"
        );
        assert_eq!(core.floors.array_durable_lsn, Lsn::ZERO);
    }

    #[test]
    fn array_flush_success_advances_and_returns_the_watermark() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut core = open_core(dir.path());
        core.watermark = Lsn::new(750);
        open_array_with_a_pending_cell(&mut core);

        assert_eq!(core.checkpoint_array_durable_lsn(), Lsn::new(750));
        assert_eq!(core.floors.array_durable_lsn, Lsn::new(750));
    }

    const TS_COLLECTION: &str = "metrics";

    /// Put one un-flushed row in a timeseries memtable and return its key.
    fn ts_memtable_with_a_pending_row(
        core: &mut CoreLoop,
    ) -> (
        nodedb_types::DatabaseId,
        nodedb_types::TenantId,
        &'static str,
    ) {
        use crate::engine::timeseries::columnar_memtable::{
            ColumnarMemtable, ColumnarMemtableConfig,
        };

        let db = nodedb_types::DatabaseId::DEFAULT;
        let tid = nodedb_types::TenantId::new(1);
        let mut mt = ColumnarMemtable::new_metric(ColumnarMemtableConfig::default());
        mt.ingest_metric(
            1,
            nodedb_types::timeseries::MetricSample {
                timestamp_ms: 1_000,
                value: 42.0,
            },
        );
        core.columnar_memtables
            .insert((db, tid, TS_COLLECTION.to_string()), mt);
        (db, tid, TS_COLLECTION)
    }

    /// The timeseries engine's durable form is its L1 partitions, so a flush
    /// that cannot write one must clamp exactly like the file-based checkpoints:
    /// the row it failed to persist still lives only in the memtable and the
    /// WAL.
    #[test]
    fn timeseries_flush_failure_clamps_to_the_last_known_durable_lsn() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut core = open_core(dir.path());
        core.watermark = Lsn::new(900);
        let (db, tid, collection) = ts_memtable_with_a_pending_row(&mut core);

        // Occupy the collection's segment directory path with a FILE so the
        // partition write has nowhere to land.
        let tenant_dir = dir
            .path()
            .join("ts")
            .join(db.as_u64().to_string())
            .join(tid.as_u64().to_string());
        std::fs::create_dir_all(&tenant_dir).expect("create tenant dir");
        std::fs::write(tenant_dir.join(collection), b"not a directory")
            .expect("write blocking file");

        assert_eq!(
            core.checkpoint_ts_durable_lsn(),
            Lsn::ZERO,
            "a flush that could not write its partition must clamp to zero rather \
             than authorise truncating the TimeseriesBatch records for the rows it lost"
        );
        assert_eq!(core.floors.ts_durable_lsn, Lsn::ZERO);
    }

    #[test]
    fn timeseries_flush_success_advances_and_returns_the_watermark() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut core = open_core(dir.path());
        core.watermark = Lsn::new(750);
        ts_memtable_with_a_pending_row(&mut core);

        assert_eq!(core.checkpoint_ts_durable_lsn(), Lsn::new(750));
        assert_eq!(core.floors.ts_durable_lsn, Lsn::new(750));
    }

    const VECTOR_TENANT: u64 = 1;

    /// Put one un-flushed vector in a collection so the flush has state to lose.
    ///
    /// Inserted with a surrogate and no document behind it — the shape a
    /// `VectorOp::Insert` produces, which is exactly the state
    /// `rebuild_vector_indexes_from_store` cannot rebuild, since its boot scan
    /// only ever sees vectors that arrived inside a redb `sparse` document.
    fn vector_collection_with_a_pending_vector(core: &mut CoreLoop) {
        use crate::engine::vector::collection::VectorCollection;
        use crate::engine::vector::hnsw::HnswParams;

        let mut coll = VectorCollection::new(4, HnswParams::default());
        coll.insert_with_surrogate(vec![0.1, 0.2, 0.3, 0.4], nodedb_types::Surrogate::new(1));
        core.vector_collections.insert(
            (
                nodedb_types::DatabaseId::DEFAULT,
                nodedb_types::TenantId::new(VECTOR_TENANT),
                "docs:emb".to_string(),
            ),
            coll,
        );
    }

    /// Same clamp for the vector indexes: a failed flush must not authorise
    /// deleting the `VectorOp::Insert` records that are the only other copy of
    /// every vector written without a document.
    #[test]
    fn vector_flush_failure_clamps_to_the_last_known_durable_lsn() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut core = open_core(dir.path());
        core.watermark = Lsn::new(900);
        vector_collection_with_a_pending_vector(&mut core);

        // Occupy the checkpoint directory's path with a FILE so the flush's
        // `create_dir_all` fails and it can publish nothing.
        std::fs::write(dir.path().join("vector-ckpt"), b"not a directory")
            .expect("write blocking file");

        assert_eq!(
            core.checkpoint_vector_durable_lsn(),
            Lsn::ZERO,
            "a fresh core has flushed nothing, so a failed flush must clamp to \
             zero rather than authorise truncating up to the watermark"
        );
        assert_eq!(core.floors.vector_durable_lsn, Lsn::ZERO);
    }

    #[test]
    fn vector_flush_success_advances_and_returns_the_watermark() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut core = open_core(dir.path());
        core.watermark = Lsn::new(750);
        vector_collection_with_a_pending_vector(&mut core);

        assert_eq!(core.checkpoint_vector_durable_lsn(), Lsn::new(750));
        assert_eq!(core.floors.vector_durable_lsn, Lsn::new(750));
    }

    /// Same clamp for the CRDT engines: a failed flush must not authorise
    /// deleting the delta records that are the only other copy of the Loro
    /// state it did not write.
    #[test]
    fn crdt_flush_failure_clamps_to_the_last_known_durable_lsn() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut core = open_core(dir.path());
        core.watermark = Lsn::new(900);
        core.get_crdt_engine(
            nodedb_types::DatabaseId::DEFAULT,
            nodedb_types::TenantId::new(1),
        )
        .expect("create a CRDT engine so the flush has a tenant to export");

        // Occupy the checkpoint directory's path with a FILE so the flush's
        // `create_dir_all` fails and it can publish nothing.
        std::fs::write(dir.path().join("crdt-ckpt"), b"not a directory")
            .expect("write blocking file");

        assert_eq!(
            core.checkpoint_crdt_durable_lsn(),
            Lsn::ZERO,
            "a fresh core has flushed nothing, so a failed flush must clamp to \
             zero rather than authorise truncating up to the watermark"
        );
        assert_eq!(core.floors.crdt_durable_lsn, Lsn::ZERO);
    }

    #[test]
    fn crdt_flush_success_advances_and_returns_the_watermark() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut core = open_core(dir.path());
        core.watermark = Lsn::new(750);
        core.get_crdt_engine(
            nodedb_types::DatabaseId::DEFAULT,
            nodedb_types::TenantId::new(1),
        )
        .expect("create a CRDT engine");

        assert_eq!(core.checkpoint_crdt_durable_lsn(), Lsn::new(750));
        assert_eq!(core.floors.crdt_durable_lsn, Lsn::new(750));
    }

    /// Put one un-flushed entry in an R-tree so the flush has state to lose.
    fn spatial_index_with_a_pending_entry(core: &mut CoreLoop) {
        let mut rtree = crate::engine::spatial::RTree::new();
        rtree.insert(crate::engine::spatial::RTreeEntry {
            id: 1,
            bbox: nodedb_types::BoundingBox::new(0.0, 0.0, 1.0, 1.0),
        });
        core.spatial_indexes.insert(
            (
                nodedb_types::DatabaseId::DEFAULT,
                nodedb_types::TenantId::new(1),
                "places".to_string(),
                "geom".to_string(),
            ),
            rtree,
        );
    }

    /// Same clamp for the spatial R-trees. Nothing rebuilds a document
    /// collection's geometry entries, so a failed flush that still reported the
    /// watermark would delete their only other copy — and the loss is silent:
    /// the rows stay, only the predicate stops matching them.
    #[test]
    fn spatial_flush_failure_clamps_to_the_last_known_durable_lsn() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut core = open_core(dir.path());
        core.watermark = Lsn::new(900);
        spatial_index_with_a_pending_entry(&mut core);

        // Occupy the checkpoint directory's path with a FILE so the flush's
        // `create_dir_all` fails and it can publish nothing.
        std::fs::write(dir.path().join("spatial-ckpt"), b"not a directory")
            .expect("write blocking file");

        assert_eq!(
            core.checkpoint_spatial_durable_lsn(),
            Lsn::ZERO,
            "a fresh core has flushed nothing, so a failed flush must clamp to \
             zero rather than authorise truncating up to the watermark"
        );
        assert_eq!(core.floors.spatial_durable_lsn, Lsn::ZERO);
    }

    #[test]
    fn spatial_flush_success_advances_and_returns_the_watermark() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut core = open_core(dir.path());
        core.watermark = Lsn::new(750);
        spatial_index_with_a_pending_entry(&mut core);

        assert_eq!(core.checkpoint_spatial_durable_lsn(), Lsn::new(750));
        assert_eq!(core.floors.spatial_durable_lsn, Lsn::new(750));
    }

    /// FTS is the one engine on this core declared safe with no contributor,
    /// and this is the proof the declaration rests on: index a document, drop
    /// the core, and reopen it having written NO checkpoint file of any kind and
    /// replayed no WAL. The postings come back because `index_document` commits
    /// them to the same redb database the documents live in, so there is no
    /// flush that could fail and no LSN that could be overstated.
    ///
    /// If this test ever fails, FTS has acquired memory-only state and needs a
    /// contributor in this module like every other engine.
    #[test]
    fn fts_postings_survive_a_reopen_with_no_checkpoint_file_and_no_wal() {
        use nodedb_fts::FtsSearchParams;
        use nodedb_fts::posting::QueryMode;

        let dir = tempfile::tempdir().expect("tempdir");
        let db = nodedb_types::DatabaseId::DEFAULT.as_u64();
        let tid = nodedb_types::TenantId::new(1);

        {
            let core = open_core(dir.path());
            core.inverted
                .index_document(
                    db,
                    tid,
                    "docs",
                    nodedb_types::Surrogate::new(7),
                    "quick brown fox",
                )
                .expect("index a document");
        }

        // The whole claim is that no flush was needed: nothing may have written
        // an FTS checkpoint, because there is no such thing to write.
        assert!(
            !dir.path().join("fts-ckpt").exists(),
            "FTS must have no checkpoint directory — its durability is redb's, \
             not a checkpoint file's"
        );

        let core = open_core(dir.path());
        let hits = core
            .inverted
            .search(
                db,
                tid,
                "docs",
                FtsSearchParams {
                    query: "brown",
                    top_k: 10,
                    fuzzy_enabled: false,
                    mode: QueryMode::And,
                    prefilter: None,
                },
            )
            .expect("search the reopened index");

        assert_eq!(
            hits.len(),
            1,
            "the posting must survive a reopen with no checkpoint and no WAL \
             replay — it was committed to redb with the document write"
        );
    }
}
