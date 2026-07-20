// SPDX-License-Identifier: BUSL-1.1

//! Data Plane core spawning, array catalog initialization, and document
//! schema registry seeding.

use std::sync::Arc;

use tracing::info;

use crate::ServerConfig;
use crate::bridge::dispatch::{CoreChannelDataSide, Dispatcher};
use crate::bridge::quiesce::CollectionQuiesce;
use crate::control::array_catalog::ArrayCatalog;
use crate::control::metrics::SystemMetrics;
use crate::control::server::shared::ddl::neutral::collection::register::{
    build_doc_config_from_stored, derive_auto_indexes, extend_with_catalog_indexes,
};
use crate::data::eventfd::EventFdNotifier;
use crate::data::runtime::{CoreCompactionConfig, SpawnCoreParams, spawn_core};
use crate::event::EventProducer;
use crate::storage::quarantine::QuarantineRegistry;
use crate::types::{DatabaseId, TenantId};

/// Load the persisted ND-array catalog from redb into the shared in-memory handle.
pub fn load_array_catalog(
    config: &ServerConfig,
) -> crate::control::array_catalog::ArrayCatalogHandle {
    let array_catalog = ArrayCatalog::handle();
    let catalog_path = config.catalog_path();
    match crate::control::security::catalog::SystemCatalog::open(&catalog_path) {
        Ok(catalog) => match catalog.load_all_arrays() {
            Ok(entries) => {
                let mut guard = array_catalog
                    .write()
                    .expect("array catalog lock poisoned at startup");
                for entry in entries {
                    if let Err(e) = guard.register(entry) {
                        tracing::warn!(error = %e, "failed to register array at startup");
                    }
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "failed to load _system.arrays at startup");
            }
        },
        Err(e) => {
            tracing::warn!(error = %e, "could not open system catalog to load arrays");
        }
    }
    array_catalog
}

/// Load every active collection's `CollectionConfig` from the durable
/// catalog, keyed the same way `doc_configs` is keyed, so each Data Plane
/// core can seed its schema registry synchronously before WAL redo replay
/// runs.
///
/// WAL replay happens on the core's own thread before that core ever
/// drains an SPSC request — including the `DocumentOp::Register`
/// broadcasts that normally populate `doc_configs` post-boot via
/// [`crate::bootstrap::schema_rehydrate::rehydrate_schema_registry`].
/// Without this, strict (Binary Tuple) document collections replay
/// through the schemaless fallback and get re-persisted as raw
/// MessagePack. Mirrors [`load_array_catalog`]'s fail-open pattern: a
/// catalog open/load failure at this boot phase logs a warning and
/// yields an empty seed rather than aborting startup.
pub fn load_doc_config_registry(
    config: &ServerConfig,
) -> Vec<crate::data::executor::core_loop::DocConfigSeedEntry> {
    let catalog_path = config.catalog_path();
    let catalog = match crate::control::security::catalog::SystemCatalog::open(&catalog_path) {
        Ok(catalog) => catalog,
        Err(e) => {
            tracing::warn!(error = %e, "could not open system catalog to seed doc_configs");
            return Vec::new();
        }
    };

    let all = match crate::bootstrap::constraint_reconcile::load_collections(&catalog) {
        Ok(all) => all,
        Err(e) => {
            tracing::warn!(error = %e, "failed to load collections to seed doc_configs");
            return Vec::new();
        }
    };

    all.into_iter()
        .filter(|(_, coll)| coll.is_active)
        .map(|(database_id, coll)| {
            let tenant_id = TenantId::new(coll.tenant_id);
            let mut indexes = derive_auto_indexes(coll.fields.iter().map(|(n, _)| n.as_str()));
            extend_with_catalog_indexes(&mut indexes, &coll);
            let config = build_doc_config_from_stored(&catalog, tenant_id, &coll, &indexes);
            let key = (database_id, tenant_id, config.name.clone());
            (key, config)
        })
        .collect()
}

/// Load every persisted `CREATE VECTOR INDEX`'s build parameters from the
/// durable catalog so each Data Plane core can seed its in-memory
/// vector-index config (`vector_params` + `index_configs`) and rebuild the
/// HNSW from the durable document store on boot.
///
/// The WAL `VectorParams` record is not crash-durable — a `kill -9` before
/// the WAL group-commit flush loses it, so on reopen the core would not know
/// the collection carries a vector index and post-restart search would return
/// empty. Mirrors [`load_doc_config_registry`]'s fail-open pattern: a catalog
/// open/load failure at this boot phase logs a warning and yields an empty
/// seed rather than aborting startup.
pub fn load_vector_index_param_seed(
    config: &ServerConfig,
) -> Vec<nodedb_types::StoredVectorIndexParams> {
    let catalog_path = config.catalog_path();
    let catalog = match crate::control::security::catalog::SystemCatalog::open(&catalog_path) {
        Ok(catalog) => catalog,
        Err(e) => {
            tracing::warn!(error = %e, "could not open system catalog to seed vector index params");
            return Vec::new();
        }
    };
    match catalog.list_all_vector_index_params() {
        Ok(entries) => entries,
        Err(e) => {
            tracing::warn!(error = %e, "failed to load vector index params to seed cores");
            Vec::new()
        }
    }
}

/// Load every active columnar-family collection's (tenant, name, schema)
/// from the durable catalog so each Data Plane core can pre-register the
/// real `MutationEngine` schema BEFORE `replay_all_wal`.
///
/// `replay_columnar_payload` replays redo records against `schema_bytes:
/// &[]`; on a fresh `columnar_engines` map, `ensure_columnar_engine_schema`
/// then falls back to inferring the schema from the first replayed row
/// (`infer_schema_from_value`), which recognizes only Float/Int/Bool/String
/// — losing declared types like Geometry, Timestamp, Decimal, Bytes, and
/// Uuid. For a spatial collection this silently degrades the geometry
/// column to `String`, so the R-tree crash-recovery rebuild (which filters
/// on `column_type == Geometry`) never runs.
///
/// Pre-registering the engine here means `ensure_columnar_engine_schema`
/// finds an existing engine and returns its (real, catalog-sourced) schema
/// instead of ever inferring — the same fix shape as
/// [`load_doc_config_registry`] for strict document collections. Mirrors
/// its fail-open pattern: a catalog open/load failure at this boot phase
/// logs a warning and yields an empty seed rather than aborting startup.
pub fn load_columnar_schema_seed(
    config: &ServerConfig,
) -> Vec<(
    DatabaseId,
    TenantId,
    String,
    nodedb_types::columnar::ColumnarSchema,
)> {
    let catalog_path = config.catalog_path();
    let catalog = match crate::control::security::catalog::SystemCatalog::open(&catalog_path) {
        Ok(catalog) => catalog,
        Err(e) => {
            tracing::warn!(error = %e, "could not open system catalog to seed columnar schemas");
            return Vec::new();
        }
    };

    let all = match crate::bootstrap::constraint_reconcile::load_collections(&catalog) {
        Ok(all) => all,
        Err(e) => {
            tracing::warn!(error = %e, "failed to load collections to seed columnar schemas");
            return Vec::new();
        }
    };

    all.into_iter()
        .filter(|(_, coll)| coll.is_active && coll.collection_type.is_columnar_family())
        .filter_map(|(database_id, coll)| {
            let schema = crate::control::planner::sql_plan_convert::dml::build_columnar_schema(
                &coll.fields,
            )?;
            Some((
                database_id,
                TenantId::new(coll.tenant_id),
                coll.name.clone(),
                schema,
            ))
        })
        .collect()
}

/// Result of [`spawn_data_plane_cores`]: the core thread handles plus each
/// core's WAL-replay-completion signal.
pub struct SpawnedDataPlaneCores {
    /// Held only to keep the Data Plane core threads alive; never read.
    pub handles: Vec<std::thread::JoinHandle<()>>,
    /// Per-core one-shot that resolves when the core finishes `replay_all_wal`
    /// (before entering its event loop). Boot awaits every one before opening
    /// the client gateway.
    pub replay_done: Vec<tokio::sync::oneshot::Receiver<()>>,
}

/// Shared Arc resources passed to each Data Plane core at spawn time.
pub struct CoreSharedResources {
    pub governor: Arc<nodedb_mem::MemoryGovernor>,
    pub quiesce: Arc<CollectionQuiesce>,
    pub hlc: Arc<nodedb_types::OrdinalClock>,
    pub array_catalog: crate::control::array_catalog::ArrayCatalogHandle,
    pub quarantine_registry: Arc<QuarantineRegistry>,
    pub system_metrics: Arc<SystemMetrics>,
    pub maintenance_budget: Arc<crate::control::maintenance::MaintenanceBudgetTracker>,
    pub doc_config_seed: Arc<Vec<crate::data::executor::core_loop::DocConfigSeedEntry>>,
    pub vector_index_param_seed: Arc<Vec<nodedb_types::StoredVectorIndexParams>>,
    pub columnar_schema_seed: Arc<
        Vec<(
            DatabaseId,
            TenantId,
            String,
            nodedb_types::columnar::ColumnarSchema,
        )>,
    >,
}

/// Spawn all Data Plane cores, wire dispatcher notifiers, and return core
/// handles plus a per-core WAL-replay-completion signal.
///
/// Each returned `oneshot::Receiver<()>` resolves when its core finishes
/// `replay_all_wal` (before it enters its event loop). Boot must await every
/// one before opening the client gateway so `/healthz` cannot report ready
/// while a core is still rebuilding in-memory indexes from the WAL.
pub fn spawn_data_plane_cores(
    config: &ServerConfig,
    data_sides: Vec<CoreChannelDataSide>,
    event_producers: Vec<EventProducer>,
    wal_records: Arc<[nodedb_wal::WalRecord]>,
    replay_tombstones: nodedb_wal::TombstoneSet,
    dispatcher: &mut Dispatcher,
    resources: CoreSharedResources,
) -> anyhow::Result<SpawnedDataPlaneCores> {
    let CoreSharedResources {
        governor,
        quiesce,
        hlc,
        array_catalog,
        quarantine_registry,
        system_metrics,
        maintenance_budget,
        doc_config_seed,
        vector_index_param_seed,
        columnar_schema_seed,
    } = resources;
    let num_cores = config.server.data_plane_cores;
    let compaction_cfg = CoreCompactionConfig {
        interval: config.checkpoint.compaction_interval(),
        tombstone_threshold: config.checkpoint.compaction_tombstone_threshold,
        query: config.tuning.query.clone(),
        graph: config.tuning.graph.clone(),
        timeseries: config.tuning.timeseries.clone(),
        checkpoint_interval: std::time::Duration::from_secs(config.checkpoint.interval_secs),
    };

    let mut core_handles = Vec::with_capacity(num_cores);
    let mut replay_done_rxs = Vec::with_capacity(num_cores);
    let mut notifiers: Vec<(usize, EventFdNotifier)> = Vec::with_capacity(num_cores);

    for (core_id, (data_side, event_producer)) in
        data_sides.into_iter().zip(event_producers).enumerate()
    {
        let (replay_done_tx, replay_done_rx) = tokio::sync::oneshot::channel();
        let (handle, notifier) = spawn_core(SpawnCoreParams {
            core_id,
            request_rx: data_side.request_rx,
            response_tx: data_side.response_tx,
            data_dir: &config.server.data_dir,
            wal_records: Arc::clone(&wal_records),
            tombstones: replay_tombstones.clone(),
            num_cores,
            compaction_config: compaction_cfg.clone(),
            system_metrics: Some(Arc::clone(&system_metrics)),
            event_producer: Some(event_producer),
            governor: Arc::clone(&governor),
            quiesce: Some(Arc::clone(&quiesce)),
            hlc: Arc::clone(&hlc),
            array_catalog: Arc::clone(&array_catalog),
            quarantine_registry: Arc::clone(&quarantine_registry),
            maintenance_budget: Arc::clone(&maintenance_budget),
            doc_config_seed: Arc::clone(&doc_config_seed),
            vector_index_param_seed: Arc::clone(&vector_index_param_seed),
            columnar_schema_seed: Arc::clone(&columnar_schema_seed),
            replay_done: replay_done_tx,
        })?;
        core_handles.push(handle);
        replay_done_rxs.push(replay_done_rx);
        notifiers.push((core_id, notifier));
    }

    for (core_id, notifier) in &notifiers {
        dispatcher.set_notifier(*core_id, *notifier);
    }

    info!(num_cores, "data plane cores running (eventfd-driven)");
    Ok(SpawnedDataPlaneCores {
        handles: core_handles,
        replay_done: replay_done_rxs,
    })
}
