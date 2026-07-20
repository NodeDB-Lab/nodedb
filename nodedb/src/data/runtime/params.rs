// SPDX-License-Identifier: BUSL-1.1

//! Everything one Data Plane core needs to open, recover, and start serving.

use std::path::Path;
use std::sync::Arc;

use nodedb_bridge::buffer::{Consumer, Producer};

use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};

use super::config::CoreCompactionConfig;

/// Parameters for [`spawn_core`](super::spawn_core).
pub struct SpawnCoreParams<'a> {
    pub core_id: usize,
    pub request_rx: Consumer<BridgeRequest>,
    pub response_tx: Producer<BridgeResponse>,
    pub data_dir: &'a Path,
    pub wal_records: Arc<[nodedb_wal::WalRecord]>,
    pub tombstones: nodedb_wal::TombstoneSet,
    pub num_cores: usize,
    pub compaction_config: CoreCompactionConfig,
    pub system_metrics: Option<Arc<crate::control::metrics::SystemMetrics>>,
    pub event_producer: Option<crate::event::bus::EventProducer>,
    pub governor: Arc<nodedb_mem::MemoryGovernor>,
    pub quiesce: Option<Arc<crate::bridge::quiesce::CollectionQuiesce>>,
    pub hlc: Arc<nodedb_types::OrdinalClock>,
    pub array_catalog: crate::control::array_catalog::ArrayCatalogHandle,
    pub quarantine_registry: Arc<crate::storage::quarantine::QuarantineRegistry>,
    pub maintenance_budget: Arc<crate::control::maintenance::MaintenanceBudgetTracker>,
    /// Catalog-sourced `doc_configs` seed, applied before `replay_all_wal`
    /// so strict (Binary Tuple) collections redo-replay through their real
    /// schema instead of falling through to the raw-MessagePack fallback.
    pub doc_config_seed: Arc<Vec<crate::data::executor::core_loop::DocConfigSeedEntry>>,
    /// Catalog-sourced vector-index build parameters, applied before
    /// `replay_all_wal` (seed `vector_params` + `index_configs`) and again
    /// after it (rebuild the HNSW from the durable store), so vector search
    /// survives a hard crash that emptied the WAL.
    pub vector_index_param_seed: Arc<Vec<nodedb_types::StoredVectorIndexParams>>,
    /// Catalog-sourced schema for every columnar-family (`columnar` /
    /// `timeseries` / `spatial`) collection, applied before `replay_all_wal`
    /// so a fresh `MutationEngine` is pre-registered with its real schema
    /// instead of the WAL redo path inferring one from the first replayed
    /// row (which loses declared types like Geometry, Timestamp, Decimal).
    pub columnar_schema_seed: Arc<
        Vec<(
            crate::types::DatabaseId,
            crate::types::TenantId,
            String,
            nodedb_types::columnar::ColumnarSchema,
        )>,
    >,
    /// One-shot signal fired once this core finishes `replay_all_wal` (before
    /// it enters its event loop). Boot awaits every core's signal before
    /// opening the client gateway, so `/healthz` never reports ready while a
    /// core is still rebuilding its in-memory indexes (HNSW, etc.) from the
    /// WAL — which would otherwise let a just-restarted node serve half-rebuilt
    /// results. Dropped without firing if the core panics during open/replay,
    /// which surfaces to boot as a failed readiness gate.
    // no-plane-separation: passive one-shot readiness Sender, fired once (`send(())`, synchronous and runtime-free) at the end of WAL replay to signal Boot; no tokio runtime or tasks run in the Data Plane.
    pub replay_done: tokio::sync::oneshot::Sender<()>,
}
