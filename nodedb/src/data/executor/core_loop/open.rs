// SPDX-License-Identifier: BUSL-1.1

//! `CoreLoop` constructors: `open` and `open_with_array_catalog`.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use nodedb_bridge::buffer::{Consumer, Producer};

use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};
use crate::control::array_catalog::ArrayCatalogHandle;
use crate::data::io::IoMetrics;
use crate::engine::array::{ArrayEngine, ArrayEngineConfig};
use crate::engine::graph::edge_store::EdgeStore;
use crate::engine::sparse::btree::SparseEngine;
use crate::engine::sparse::doc_cache::DocCache;
use crate::engine::sparse::inverted::InvertedIndex;
use crate::types::Lsn;
use nodedb_types::OrdinalClock;

use super::priority_queues::PriorityQueues;
use super::state::CoreLoop;

impl CoreLoop {
    /// Create a core loop with its SPSC channel endpoints and engine storage.
    ///
    /// `data_dir` is the base data directory; each core gets its own redb file
    /// at `{data_dir}/sparse/core-{core_id}.redb`.
    pub fn open(
        core_id: usize,
        request_rx: Consumer<BridgeRequest>,
        response_tx: Producer<BridgeResponse>,
        data_dir: &Path,
        hlc: Arc<OrdinalClock>,
        governor: Arc<nodedb_mem::MemoryGovernor>,
    ) -> crate::Result<Self> {
        Self::open_with_array_catalog(
            core_id,
            request_rx,
            response_tx,
            data_dir,
            hlc,
            governor,
            crate::control::array_catalog::ArrayCatalog::handle(),
        )
    }

    /// Variant that accepts a pre-built [`ArrayCatalogHandle`]. The
    /// server bootstrap loads the catalog from disk once and passes the
    /// same handle into every core so Data-Plane dispatch and
    /// Control-Plane DDL share one registry.
    pub fn open_with_array_catalog(
        core_id: usize,
        request_rx: Consumer<BridgeRequest>,
        response_tx: Producer<BridgeResponse>,
        data_dir: &Path,
        hlc: Arc<OrdinalClock>,
        governor: Arc<nodedb_mem::MemoryGovernor>,
        array_catalog: ArrayCatalogHandle,
    ) -> crate::Result<Self> {
        let sparse_path =
            crate::data::executor::snapshot::layout::sparse_store_path(data_dir, core_id);
        let sparse = SparseEngine::open(&sparse_path)?;

        let graph_path =
            crate::data::executor::snapshot::layout::graph_store_path(data_dir, core_id);
        let edge_store = EdgeStore::open(&graph_path)?;
        let csr = crate::engine::graph::csr::rebuild::rebuild_sharded_from_store(
            &edge_store,
            Arc::clone(&governor),
        )?;

        // Inverted index shares the sparse engine's redb database.
        let inverted = InvertedIndex::open(sparse.db().clone(), Arc::clone(&governor))?;

        // Column statistics store shares the sparse engine's redb database.
        let stats_store = crate::engine::sparse::stats::StatsStore::open(sparse.db().clone())?;

        // Rehydrate the per-collection hash-chain heads written by previous
        // runs. Without this every restart restarts every chain at
        // `GENESIS_HASH`, so `VERIFY_HASH_CHAIN` breaks at the first row
        // inserted after a restart and blames an untampered row.
        //
        // This reads the persisted heads and never rescans the collections:
        // `VERIFY_HASH_CHAIN` compares the last row against the head, so a head
        // rebuilt from the rows cannot detect rows removed from the end.
        let chain_hashes = sparse.load_chain_heads()?;

        let array_root = crate::data::executor::snapshot::layout::array_root(data_dir, core_id);
        let array_engine = ArrayEngine::new(ArrayEngineConfig::new(array_root)).map_err(|e| {
            crate::Error::Internal {
                detail: format!("open array engine: {e}"),
            }
        })?;

        Ok(Self {
            core_id,
            // A core opened without a node fires only actions armed for every
            // node. The server bootstrap scopes it with `set_fail_scope`.
            fail_scope: nodedb_types::fail_point::FailScope::Any,
            request_rx,
            response_tx,
            task_queue: PriorityQueues::new(),
            drain_cycle: 0,
            io_metrics: Arc::new(IoMetrics::new()),
            watermark: Lsn::ZERO,
            sparse,
            crdt_engines: HashMap::new(),
            vector_collections: HashMap::new(),
            vector_builds: super::vector_build_queue::VectorBuildQueue::spawn(core_id),
            vector_params: HashMap::new(),
            declared_dims: HashMap::new(),
            edge_store,
            hlc,
            last_stamp_ms: std::sync::atomic::AtomicI64::new(0),
            csr,
            inverted,
            data_dir: data_dir.to_path_buf(),
            paused_vshards: std::collections::HashSet::new(),
            deleted_nodes: HashMap::new(),
            idempotency: super::idempotency::IdempotencyCache::default(),
            sync_hwm: HashMap::new(),
            producer_epoch_floor: HashMap::new(),
            stats_store,
            aggregate_cache: HashMap::new(),
            maintenance: super::maintenance_state::MaintenanceState::new(),
            index_configs: HashMap::new(),
            sparse_vector_indexes: HashMap::new(),
            doc_cache: DocCache::new(
                nodedb_types::config::tuning::QueryTuning::default().doc_cache_entries,
            ),
            columnar_memtables: HashMap::new(),
            columnar_memtable_mem: HashMap::new(),
            columnar_engines: HashMap::new(),
            columnar_flushed_segments: HashMap::new(),
            columnar_flushed_surrogates: HashMap::new(),
            ts_replay_stamps: HashMap::new(),
            ts_replay_cursor: None,
            last_ts_ingest: None,
            ts_last_value_caches: HashMap::new(),
            ts_series_catalogs: HashMap::new(),
            ts_registries: HashMap::new(),
            ts_truncate_backlog: Vec::new(),
            continuous_agg_mgr:
                crate::engine::timeseries::continuous_agg::ContinuousAggregateManager::new(),
            checkpoint_coordinator: crate::storage::checkpoint::CheckpointCoordinator::new(
                crate::storage::checkpoint::CheckpointConfig::default(),
            ),
            spatial_indexes: std::collections::HashMap::new(),
            spatial_doc_map: std::collections::HashMap::new(),
            vector_doc_map: std::collections::HashMap::new(),
            doc_configs: HashMap::new(),
            chain_hashes,
            chain_intents: HashMap::new(),
            query_tuning: nodedb_types::config::tuning::QueryTuning::default(),
            graph_tuning: nodedb_types::config::tuning::GraphTuning::default(),
            ts_tuning: nodedb_types::config::tuning::TimeseriesToning::default(),
            vector_tuning: nodedb_types::config::tuning::VectorTuning::default(),
            kv_engine: crate::engine::kv::KvEngine::from_tuning(
                crate::engine::kv::current_ms(),
                &nodedb_types::config::tuning::KvTuning::default(),
            ),
            // A fresh core has restored nothing and flushed nothing, so every
            // engine is durable through nothing and no replay floor is set —
            // see `CheckpointFloors::new` for what each zero costs and why.
            floors: crate::data::executor::core_loop::checkpoint_floors::CheckpointFloors::new(),
            array_engine,
            array_catalog,
            uring_reader: crate::data::io::uring_reader::UringReader::new(),
            segment_keks: crate::data::executor::core_loop::SegmentKeks {
                vector_checkpoint_kek: None,
                spatial_checkpoint_kek: None,
                columnar_segment_kek: None,
                array_segment_kek: None,
                ts_segment_kek: None,
            },
            governor,
            throttle: super::pressure::SpscThrottle::new(),
            collection_arena_registry: None,
            metrics: None,
            events: super::event_outlet::EventOutlet::new(),
            quiesce: None,
            quarantine_registry: None,
            epoch_system_ms: None,
            txn_overlays: HashMap::new(),
            graph_txn_overlays: HashMap::new(),
            array_txn_overlays: HashMap::new(),
            txn_savepoints: HashMap::new(),
            txn_created_columnar_engines: HashMap::new(),
            ts_resolve_holds: HashMap::new(),
            write_index: super::write_index::WriteVersionIndex::new(),
            calvin: super::calvin_state::CalvinCoreState::new(),
            apply_scope: super::apply_scope::ApplyScope::default(),
            redo_apply:
                crate::data::executor::handlers::transaction::redo_apply::RedoApplyState::new(),
            fail_stop: super::fail_stop::CoreFailStop::default(),
            write_set_journal: super::write_set_journal::WriteSetJournalState::default(),
        })
    }
}
