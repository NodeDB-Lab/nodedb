// SPDX-License-Identifier: BUSL-1.1

//! Physical capture of one core's durable state.
//!
//! Capture forces a full checkpoint first, then reads every file the core's
//! engines own. It runs on the core's own thread, so nothing applies between
//! the checkpoint and the last file read.
//!
//! Per component:
//!
//! - Sparse and graph redb stores: a whole-file image under a held write
//!   transaction. redb has no immutable segments, and a logical dump
//!   needs every table's types. The sparse store also holds full-text
//!   postings, column statistics, and hash-chain heads, so those come with it.
//! - KV, sparse vector, columnar, vector, CRDT, and spatial: the MANIFEST and
//!   the one generation directory it names. A checkpoint publishes a new
//!   generation and commits it with one atomic MANIFEST write, so published
//!   files never change in place. Superseded generations are left behind.
//! - Sync gate and graph labels: the single STATE file each publishes.
//! - Array: each array's manifest and the segments it names. The forced flush
//!   moves every memtable into tile segments, and a written segment never
//!   changes.
//! - Timeseries: the registered, undeleted partitions of the collections this
//!   core owns. The forced flush moves every memtable into partitions, and a
//!   partition is committed by its `partition.meta` and never changes.
//!
//! HNSW indexes are captured, not rebuilt: a vector inserted without a
//! document has no other source to rebuild from. The CSR adjacency is not
//! captured: `CoreLoop::open` rebuilds it from the graph edge store, and that
//! rebuild is deterministic.

use tracing::{info, warn};

use super::layout;
use super::live_files::GENERATION_COMPONENTS;
use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::data::snapshot::{CoreSnapshot, SnapshotComponent, SnapshotFile};
use crate::storage::snapshot_files::{read_redb_image, rel_path_string};

impl CoreLoop {
    /// Answer `CreateSnapshot` with this core's encoded [`CoreSnapshot`].
    pub(in crate::data::executor) fn execute_create_snapshot(
        &mut self,
        task: &ExecutionTask,
    ) -> Response {
        let encoded = self
            .capture_core_snapshot()
            .and_then(|snapshot| snapshot.to_bytes().map(|bytes| (snapshot, bytes)));
        match encoded {
            Ok((snapshot, bytes)) => {
                info!(
                    core = self.core_id,
                    replay_floor = snapshot.replay_floor(),
                    applied_high_lsn = snapshot.applied_high_lsn(),
                    files = snapshot.files.len(),
                    size_bytes = bytes.len(),
                    "core snapshot captured"
                );
                self.response_with_payload(task, bytes)
            }
            Err(e) => {
                warn!(core = self.core_id, error = %e, "core snapshot capture failed");
                self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                )
            }
        }
    }

    /// Checkpoint every engine, then read every file this core's engines own.
    ///
    /// Fails when any engine flush fails: the capture then misses the
    /// state that flush left in memory.
    pub(in crate::data::executor) fn capture_core_snapshot(
        &mut self,
    ) -> crate::Result<CoreSnapshot> {
        let checkpoint_lsn = self.checkpoint_engines();
        let stamp = self.floors.applied_prefix.stamp()?;
        if checkpoint_lsn < stamp.prefix {
            return Err(crate::Error::Storage {
                engine: "snapshot".into(),
                detail: format!(
                    "core {} checkpoint is durable only through lsn {checkpoint_lsn}, below \
                     its floor {}: an engine flush failed, so a capture would miss its state. \
                     The failed flush is logged by the checkpoint; retry once it succeeds",
                    self.core_id, stamp.prefix
                ),
            });
        }

        let mut files = vec![
            self.redb_file(SnapshotComponent::Sparse, self.sparse.db())?,
            self.redb_file(SnapshotComponent::Graph, self.edge_store.db())?,
        ];
        let mut dirs = Vec::new();
        for component in GENERATION_COMPONENTS {
            self.collect_live_generation(component, &mut files, &mut dirs)?;
        }
        self.collect_state_files(&mut files)?;
        self.collect_live_arrays(&mut files)?;
        self.collect_owned_timeseries(&mut files)?;

        Ok(CoreSnapshot { stamp, files, dirs })
    }

    fn redb_file(
        &self,
        component: SnapshotComponent,
        db: &redb::Database,
    ) -> crate::Result<SnapshotFile> {
        let rel = layout::component_root(component, self.core_id);
        Ok(SnapshotFile {
            component,
            path: rel_path_string(&rel)?,
            bytes: read_redb_image(db, &self.data_dir.join(&rel))?,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Arc;

    use nodedb_bridge::buffer::RingBuffer;
    use nodedb_types::{Surrogate, TenantId};

    use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};
    use crate::data::executor::core_loop::CoreLoop;
    use crate::data::snapshot::CoreSnapshot;
    use crate::types::{DatabaseId, Lsn};

    const DB: DatabaseId = DatabaseId::DEFAULT;

    fn tid() -> TenantId {
        TenantId::new(1)
    }

    fn open_core(dir: &Path) -> CoreLoop {
        let (_req_tx, req_rx) = RingBuffer::channel::<BridgeRequest>(64);
        let (resp_tx, _resp_rx) = RingBuffer::channel::<BridgeResponse>(64);
        CoreLoop::open(
            0,
            req_rx,
            resp_tx,
            dir,
            Arc::new(nodedb_types::OrdinalClock::new()),
            crate::data::executor::core_loop::test_governor(),
        )
        .expect("CoreLoop::open")
    }

    /// Every record through `lsn` has an outcome, as a running node reports.
    fn settle(core: &mut CoreLoop, lsn: u64) {
        core.watermark = Lsn::new(lsn);
        core.floors
            .applied_prefix
            .observe_outcome_floor(Lsn::new(lsn));
    }

    fn capture(mut core: CoreLoop) -> CoreSnapshot {
        settle(&mut core, 750);
        let snapshot = core.capture_core_snapshot().expect("capture");
        drop(core);
        // Through the wire encoding, as the bridge carries it.
        CoreSnapshot::from_bytes(&snapshot.to_bytes().expect("encode")).expect("decode")
    }

    /// Restore into a fresh, empty directory and boot a core over it exactly as
    /// `spawn_core` does before WAL replay.
    fn restore_and_boot(snapshot: &CoreSnapshot) -> (tempfile::TempDir, CoreLoop) {
        let target = tempfile::tempdir().expect("tempdir");
        crate::storage::snapshot_files::require_empty_dir(target.path()).expect("empty");
        crate::storage::snapshot_executor::restore_core_snapshot(target.path(), 0, snapshot)
            .expect("restore");
        let mut core = open_core(target.path());
        crate::data::runtime::load_boot_checkpoints(&mut core).expect("boot load");
        (target, core)
    }

    #[test]
    fn documents_and_full_text_postings_round_trip() {
        use nodedb_fts::FtsSearchParams;
        use nodedb_fts::posting::QueryMode;

        let dir = tempfile::tempdir().unwrap();
        let core = open_core(dir.path());
        core.sparse.put_raw("1:docs:d1", b"hello").unwrap();
        core.inverted
            .index_document(
                DB.as_u64(),
                tid(),
                "docs",
                Surrogate::new(7),
                &crate::engine::sparse::inverted::test_support::body("quick brown fox"),
            )
            .unwrap();

        let (_dir, restored) = restore_and_boot(&capture(core));
        assert_eq!(
            restored.sparse.get_raw("1:docs:d1").unwrap().as_deref(),
            Some(b"hello".as_slice())
        );
        let hits = restored
            .inverted
            .search(
                DB.as_u64(),
                tid(),
                "docs",
                FtsSearchParams {
                    query: "brown",
                    top_k: 10,
                    fuzzy_enabled: false,
                    mode: QueryMode::And,
                    prefilter: None,
                },
            )
            .unwrap();
        assert_eq!(hits.len(), 1, "full-text postings live in the sparse store");
    }

    #[test]
    fn graph_edges_round_trip() {
        use crate::engine::graph::edge_store::EdgeRef;

        let dir = tempfile::tempdir().unwrap();
        let core = open_core(dir.path());
        core.edge_store
            .put_edge_versioned(
                EdgeRef::new(DB, tid(), "people", "a", "knows", "b"),
                b"{}",
                100,
                100,
                i64::MAX,
            )
            .unwrap();

        let (_dir, restored) = restore_and_boot(&capture(core));
        assert_eq!(restored.edge_store.export_edges().unwrap().len(), 1);
    }

    #[test]
    fn kv_rows_round_trip() {
        use crate::engine::kv::KvPutParams;

        let dir = tempfile::tempdir().unwrap();
        let mut core = open_core(dir.path());
        core.kv_engine
            .put(KvPutParams {
                database_id: DB.as_u64(),
                tenant_id: tid().as_u64(),
                collection: "sessions",
                key: b"k1",
                value: b"v1",
                ttl_ms: 0,
                now_ms: 1_000,
                surrogate: Surrogate::new(1),
            })
            .expect("a bound row writes");

        let (_dir, restored) = restore_and_boot(&capture(core));
        assert_eq!(
            restored
                .kv_engine
                .get(DB.as_u64(), tid().as_u64(), "sessions", b"k1", 1_000)
                .as_deref(),
            Some(b"v1".as_slice())
        );
    }

    #[test]
    fn sparse_vector_index_round_trips() {
        use crate::engine::vector::sparse::SparseInvertedIndex;

        let dir = tempfile::tempdir().unwrap();
        let mut core = open_core(dir.path());
        let mut index = SparseInvertedIndex::new();
        let vector = nodedb_types::SparseVector::from_entries(vec![(1, 0.5), (7, 0.25)]).unwrap();
        index.insert("doc-a", &vector);
        let key = (DB, tid(), "docs".to_string(), "emb".to_string());
        core.sparse_vector_indexes.insert(key.clone(), index);

        let (_dir, restored) = restore_and_boot(&capture(core));
        let restored_index = restored
            .sparse_vector_indexes
            .get(&key)
            .expect("index restored");
        assert_eq!(restored_index.doc_count(), 1);
    }

    #[test]
    fn sync_gate_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let mut core = open_core(dir.path());
        core.sync_hwm.insert((1, 5), 3);
        core.producer_epoch_floor.insert(1, 2);

        let (_dir, restored) = restore_and_boot(&capture(core));
        assert_eq!(restored.sync_hwm.get(&(1, 5)), Some(&3));
        assert_eq!(restored.producer_epoch_floor.get(&1), Some(&2));
    }

    #[test]
    fn columnar_rows_round_trip() {
        use nodedb_columnar::MutationEngine;
        use nodedb_types::columnar::{ColumnDef, ColumnType, ColumnarSchema};
        use nodedb_types::value::Value;

        let dir = tempfile::tempdir().unwrap();
        let mut core = open_core(dir.path());
        let schema = ColumnarSchema::new(vec![
            ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
            ColumnDef::required("name", ColumnType::String),
        ])
        .unwrap();
        let mut engine = MutationEngine::new("events".to_string(), schema);
        for (id, surrogate) in [(1, 101), (2, 102)] {
            engine
                .insert_with_surrogate(
                    &[Value::Integer(id), Value::String(format!("row-{id}"))],
                    Surrogate::new(surrogate),
                )
                .unwrap();
        }
        let key = (DB, tid(), "events".to_string());
        core.columnar_engines.insert(key.clone(), engine);

        let (_dir, restored) = restore_and_boot(&capture(core));
        let restored_engine = restored
            .columnar_engines
            .get(&key)
            .expect("engine restored");
        assert_eq!(restored_engine.live_row_count(), 2);
    }

    #[test]
    fn graph_labels_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let mut core = open_core(dir.path());
        core.csr_partition_mut(DB.as_u64(), tid().as_u64())
            .add_node_label("ghost", "Person")
            .unwrap();

        let (_dir, restored) = restore_and_boot(&capture(core));
        let csr = restored
            .csr_partition(DB.as_u64(), tid().as_u64())
            .expect("partition restored");
        let id = csr.node_id("ghost").expect("node restored");
        assert!(csr.node_has_label(id.raw(csr.partition_tag()), "Person"));
    }

    fn grid_schema() -> nodedb_array::schema::ArraySchema {
        use nodedb_array::schema::ArraySchemaBuilder;
        use nodedb_array::schema::attr_spec::{AttrSpec, AttrType};
        use nodedb_array::schema::dim_spec::{DimSpec, DimType};
        use nodedb_array::types::domain::{Domain, DomainBound};

        ArraySchemaBuilder::new("grid")
            .dim(DimSpec::new(
                "x",
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(15)),
            ))
            .attr(AttrSpec::new("v", AttrType::Int64, true))
            .tile_extents(vec![4])
            .build()
            .unwrap()
    }

    const GRID_SCHEMA_HASH: u64 = 0xA55E7;

    #[test]
    fn array_cells_round_trip() {
        use nodedb_array::types::ArrayId;
        use nodedb_array::types::cell_value::value::CellValue;
        use nodedb_array::types::coord::value::CoordValue;

        let dir = tempfile::tempdir().unwrap();
        let mut core = open_core(dir.path());
        let id = ArrayId::new(tid(), "grid");
        core.array_engine
            .open_array(id.clone(), Arc::new(grid_schema()), GRID_SCHEMA_HASH)
            .unwrap();
        core.array_engine
            .put_cells(
                &id,
                vec![crate::engine::array::wal::ArrayPutCell {
                    coord: vec![CoordValue::Int64(1)],
                    attrs: vec![CellValue::Int64(7)],
                    surrogate: Surrogate::new(1),
                    system_from_ms: 1,
                    valid_from_ms: 0,
                    valid_until_ms: i64::MAX,
                }],
                10,
            )
            .unwrap();

        let (_dir, mut restored) = restore_and_boot(&capture(core));
        restored
            .array_engine
            .open_array(id.clone(), Arc::new(grid_schema()), GRID_SCHEMA_HASH)
            .unwrap();
        assert!(
            restored
                .array_engine
                .contains_cell(&id, &[CoordValue::Int64(1)])
                .unwrap(),
            "the flushed tile segment must come back"
        );
    }

    #[test]
    fn timeseries_partitions_round_trip() {
        use crate::engine::timeseries::columnar_memtable::{
            ColumnarMemtable, ColumnarMemtableConfig,
        };

        let dir = tempfile::tempdir().unwrap();
        let mut core = open_core(dir.path());
        let mut memtable = ColumnarMemtable::new_metric(ColumnarMemtableConfig::default());
        memtable.ingest_metric(
            1,
            nodedb_types::timeseries::MetricSample {
                timestamp_ms: 1_000,
                value: 42.0,
            },
        );
        let key = (DB, tid(), "metrics".to_string());
        core.columnar_memtables.insert(key.clone(), memtable);

        let snapshot = capture(core);
        assert!(
            snapshot
                .files
                .iter()
                .any(|f| f.path.starts_with("ts/0/1/metrics/")),
            "the owned collection's partitions are captured"
        );
        let (_dir, restored) = restore_and_boot(&snapshot);
        let registry = restored.ts_registries.get(&key).expect("registry restored");
        assert!(registry.partition_count() > 0);
    }

    #[test]
    fn vector_index_round_trips() {
        use crate::engine::vector::collection::VectorCollection;
        use crate::engine::vector::hnsw::HnswParams;

        let dir = tempfile::tempdir().unwrap();
        let mut core = open_core(dir.path());
        let mut collection = VectorCollection::new(4, HnswParams::default());
        collection
            .insert_with_surrogate(vec![0.1, 0.2, 0.3, 0.4], Surrogate::new(1))
            .unwrap();
        let key = (DB, tid(), "docs:emb".to_string());
        core.vector_collections.insert(key.clone(), collection);

        let (_dir, restored) = restore_and_boot(&capture(core));
        let restored_collection = restored
            .vector_collections
            .get(&key)
            .expect("collection restored");
        assert_eq!(
            restored_collection.len(),
            1,
            "a vector with no document is restored only from the capture"
        );
    }

    #[test]
    fn crdt_state_round_trips() {
        use loro::LoroValue;

        let dir = tempfile::tempdir().unwrap();
        let mut core = open_core(dir.path());
        core.get_crdt_engine(DB, tid())
            .unwrap()
            .doc_upsert("orders", "row-1", &[("qty", LoroValue::I64(2))])
            .unwrap();

        let (_dir, restored) = restore_and_boot(&capture(core));
        let engine = restored
            .crdt_engines
            .get(&(DB, tid()))
            .expect("engine restored");
        let collections: Vec<String> = engine
            .export_all_snapshots()
            .unwrap()
            .into_iter()
            .map(|(collection, _)| collection)
            .collect();
        assert_eq!(collections, ["orders"]);
    }

    #[test]
    fn spatial_index_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let mut core = open_core(dir.path());
        let memory = nodedb_mem::ScopedMemory::new(
            core.governor.clone(),
            DB,
            tid(),
            nodedb_mem::EngineId::Spatial,
        );
        let mut rtree = crate::engine::spatial::RTree::new(memory);
        rtree.insert(crate::engine::spatial::RTreeEntry {
            id: 1,
            bbox: nodedb_types::BoundingBox::new(0.0, 0.0, 1.0, 1.0),
        });
        let key = (DB, tid(), "places".to_string(), "geom".to_string());
        core.spatial_indexes.insert(key.clone(), rtree);

        let (_dir, restored) = restore_and_boot(&capture(core));
        assert_eq!(restored.spatial_indexes.get(&key).map(|t| t.len()), Some(1));
    }

    #[test]
    fn the_capture_names_the_records_it_holds() {
        let dir = tempfile::tempdir().unwrap();
        let mut core = open_core(dir.path());
        core.floors
            .applied_prefix
            .observe_outcome_floor(Lsn::new(10));
        core.floors.applied_prefix.note_applied(Lsn::new(30));
        core.watermark = Lsn::new(30);

        let snapshot = core.capture_core_snapshot().unwrap();
        assert_eq!(snapshot.replay_floor(), 10);
        assert_eq!(
            snapshot.applied_high_lsn(),
            30,
            "record 30 applied, so the capture holds its effect"
        );
        assert!(!snapshot.stamp.skips(20), "record 20 is still on its way");
    }

    #[test]
    fn a_failed_flush_fails_the_capture() {
        let dir = tempfile::tempdir().unwrap();
        let mut core = open_core(dir.path());
        settle(&mut core, 900);
        // A file where the directory must go: the columnar flush cannot publish.
        std::fs::write(dir.path().join("columnar-ckpt"), b"not a directory").unwrap();

        assert!(core.capture_core_snapshot().is_err());
    }

    #[test]
    fn timeseries_ownership_follows_the_replay_route() {
        let dir = tempfile::tempdir().unwrap();
        let mut core = open_core(dir.path());
        core.set_num_cores(crate::types::VShardId::COUNT as usize);
        let owned = nodedb_types::CollectionKey::from_bare(DB, "metrics")
            .vshard()
            .as_u32() as usize;
        assert_eq!(
            core.owns_collection(DB, "metrics").unwrap(),
            owned == core.core_id
        );
        core.set_num_cores(1);
        assert!(core.owns_collection(DB, "metrics").unwrap());
    }

    fn paths(snapshot: &CoreSnapshot) -> Vec<&str> {
        snapshot.files.iter().map(|f| f.path.as_str()).collect()
    }

    /// Only the generation the MANIFEST names is captured. A superseded
    /// generation left on disk stays behind.
    #[test]
    fn superseded_generations_are_not_captured() {
        use crate::engine::kv::KvPutParams;

        let dir = tempfile::tempdir().unwrap();
        let mut core = open_core(dir.path());
        core.kv_engine
            .put(KvPutParams {
                database_id: DB.as_u64(),
                tenant_id: tid().as_u64(),
                collection: "sessions",
                key: b"k1",
                value: b"v1",
                ttl_ms: 0,
                now_ms: 1_000,
                surrogate: Surrogate::new(1),
            })
            .expect("a bound row writes");
        settle(&mut core, 750);
        core.capture_core_snapshot().unwrap();
        let stale = dir.path().join("kv-ckpt/core-0/gen-999");
        std::fs::create_dir_all(&stale).unwrap();
        std::fs::write(stale.join("old.ckpt"), b"superseded").unwrap();

        let snapshot = core.capture_core_snapshot().unwrap();
        let kv: Vec<&str> = snapshot
            .files
            .iter()
            .filter(|f| f.component == crate::data::snapshot::SnapshotComponent::Kv)
            .map(|f| f.path.as_str())
            .collect();
        assert!(kv.contains(&"kv-ckpt/core-0/MANIFEST"));
        assert!(kv.iter().all(|p| !p.contains("gen-999")), "{kv:?}");
        let generations: std::collections::BTreeSet<&str> = kv
            .iter()
            .filter_map(|p| p.split('/').nth(2))
            .filter(|part| part.starts_with("gen-"))
            .collect();
        assert_eq!(generations.len(), 1, "exactly the live generation: {kv:?}");
    }

    /// A segment no array manifest names, and a dropped array's tombstone
    /// directory, stay behind.
    #[test]
    fn unreferenced_array_files_are_not_captured() {
        use nodedb_array::types::ArrayId;
        use nodedb_array::types::cell_value::value::CellValue;
        use nodedb_array::types::coord::value::CoordValue;

        let dir = tempfile::tempdir().unwrap();
        let mut core = open_core(dir.path());
        let id = ArrayId::new(tid(), "grid");
        core.array_engine
            .open_array(id.clone(), Arc::new(grid_schema()), GRID_SCHEMA_HASH)
            .unwrap();
        core.array_engine
            .put_cells(
                &id,
                vec![crate::engine::array::wal::ArrayPutCell {
                    coord: vec![CoordValue::Int64(1)],
                    attrs: vec![CellValue::Int64(7)],
                    surrogate: Surrogate::new(1),
                    system_from_ms: 1,
                    valid_from_ms: 0,
                    valid_until_ms: i64::MAX,
                }],
                10,
            )
            .unwrap();
        settle(&mut core, 750);
        let first = core.capture_core_snapshot().unwrap();
        let manifest = paths(&first)
            .into_iter()
            .find(|p| p.ends_with("manifest.ndam"))
            .expect("the array manifest is captured")
            .to_string();
        let array_dir = dir.path().join(&manifest).parent().unwrap().to_path_buf();
        std::fs::write(array_dir.join("stray.ndas"), b"unreferenced").unwrap();
        let tombstone = array_dir.parent().unwrap().join(".gone.drop-pending");
        std::fs::create_dir_all(&tombstone).unwrap();
        std::fs::write(tombstone.join("manifest.ndam"), b"dropped").unwrap();

        let second = core.capture_core_snapshot().unwrap();
        let captured = paths(&second);
        assert!(captured.contains(&manifest.as_str()));
        assert!(
            captured
                .iter()
                .all(|p| !p.contains("stray") && !p.contains("drop-pending")),
            "{captured:?}"
        );
        assert!(
            captured.iter().any(|p| p.ends_with(".ndas")),
            "the segment the manifest names is captured"
        );
    }

    /// A partition directory the registry does not hold stays behind.
    #[test]
    fn unregistered_timeseries_partitions_are_not_captured() {
        use crate::engine::timeseries::columnar_memtable::{
            ColumnarMemtable, ColumnarMemtableConfig,
        };

        let dir = tempfile::tempdir().unwrap();
        let mut core = open_core(dir.path());
        let mut memtable = ColumnarMemtable::new_metric(ColumnarMemtableConfig::default());
        memtable.ingest_metric(
            1,
            nodedb_types::timeseries::MetricSample {
                timestamp_ms: 1_000,
                value: 42.0,
            },
        );
        core.columnar_memtables
            .insert((DB, tid(), "metrics".to_string()), memtable);
        settle(&mut core, 750);
        core.capture_core_snapshot().unwrap();
        let stray = dir.path().join("ts/0/1/metrics/ts-stray");
        std::fs::create_dir_all(&stray).unwrap();
        std::fs::write(stray.join("partition.meta"), b"not registered").unwrap();

        let snapshot = core.capture_core_snapshot().unwrap();
        let captured = paths(&snapshot);
        assert!(captured.iter().any(|p| p.starts_with("ts/0/1/metrics/ts-")));
        assert!(
            captured.iter().all(|p| !p.contains("ts-stray")),
            "{captured:?}"
        );
    }
}
