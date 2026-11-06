// SPDX-License-Identifier: BUSL-1.1

//! Data Plane core spawning, array catalog initialization, and document
//! schema registry seeding.

use std::sync::Arc;

use tracing::info;

use crate::ServerConfig;
use crate::bootstrap::catalog_open::CatalogForRead;
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
///
/// The catalog is filled while this function owns it, and only then wrapped
/// in the shared lock. No other thread can reach it before that, so no lock
/// is taken here.
///
/// `Err` when a catalog exists and its arrays do not read. An empty handle
/// looks like a first boot and hides every array the catalog declares.
pub fn load_array_catalog(
    config: &ServerConfig,
) -> Result<crate::control::array_catalog::ArrayCatalogHandle, CatalogSeedError> {
    load_array_catalog_at(&config.catalog_path())
}

/// [`load_array_catalog`] against an explicit catalog path.
fn load_array_catalog_at(
    catalog_path: &std::path::Path,
) -> Result<crate::control::array_catalog::ArrayCatalogHandle, CatalogSeedError> {
    const SEED: &str = "ND-array definitions";
    let mut array_catalog = ArrayCatalog::new();
    if let Some(catalog) = open_for_seed(SEED, catalog_path)? {
        crate::control::array_catalog::persist::register_loaded(
            &mut array_catalog,
            catalog.load_all_arrays(),
        )
        .map_err(|source| CatalogSeedError::load(SEED, catalog_path, source))?;
    }
    Ok(Arc::new(std::sync::RwLock::new(array_catalog)))
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
/// MessagePack, so a catalog that exists and does not read refuses boot.
pub fn load_doc_config_registry(
    config: &ServerConfig,
) -> Result<Vec<crate::data::executor::core_loop::DocConfigSeedEntry>, CatalogSeedError> {
    load_doc_config_registry_at(&config.catalog_path())
}

/// [`load_doc_config_registry`] against an explicit catalog path.
///
/// Exists because the boot path and the integration-test harness reconstruct
/// cores through different entry points but must reconstruct them the SAME way:
/// the harness has the catalog path but no `ServerConfig`, and without this it
/// silently spawned cores with no seed at all. A harness that skips the seed
/// does not reproduce a restart — it reproduces a restart with the schema
/// registry missing, which is a state production never reaches, and every
/// restart test written against it is weaker than it appears.
pub fn load_doc_config_registry_at(
    catalog_path: &std::path::Path,
) -> Result<Vec<crate::data::executor::core_loop::DocConfigSeedEntry>, CatalogSeedError> {
    const SEED: &str = "collection schemas";
    let Some(catalog) = open_for_seed(SEED, catalog_path)? else {
        return Ok(Vec::new());
    };
    load_doc_config_registry_from(&catalog)
        .map_err(|source| CatalogSeedError::load(SEED, catalog_path, source))
}

/// [`load_doc_config_registry`] against an ALREADY-OPEN catalog.
///
/// The path-taking variants above open the catalog themselves, which only works
/// for a caller that holds no handle to it yet. redb is single-writer: a second
/// open while another handle is alive fails, and that failure refuses boot. So
/// any caller that already has the catalog open must pass it here instead of
/// handing over a path and re-opening behind its own lock.
///
/// `Err` when the catalog's collections do not read.
pub fn load_doc_config_registry_from<S>(
    catalog: &S,
) -> crate::Result<Vec<crate::data::executor::core_loop::DocConfigSeedEntry>>
where
    S: crate::bootstrap::constraint_reconcile::CollectionSource + ?Sized,
{
    let all = crate::bootstrap::constraint_reconcile::load_collections(catalog)?;

    Ok(all
        .into_iter()
        .filter(|(_, coll)| coll.is_active)
        .map(|(database_id, coll)| {
            let tenant_id = TenantId::new(coll.tenant_id);
            let mut indexes = derive_auto_indexes(coll.fields.iter().map(|(n, _)| n.as_str()));
            extend_with_catalog_indexes(&mut indexes, &coll);
            let config = build_doc_config_from_stored(catalog, tenant_id, &coll, &indexes);
            let key = (database_id, tenant_id, config.name.clone());
            (key, config)
        })
        .collect())
}

/// Load every persisted `CREATE VECTOR INDEX`'s build parameters from the
/// durable catalog so each Data Plane core can seed its in-memory
/// vector-index config (`vector_params` + `index_configs`) and rebuild the
/// HNSW from the durable document store on boot.
///
/// The WAL `VectorParams` record is not crash-durable — a `kill -9` before
/// the WAL group-commit flush loses it, so on reopen the core would not know
/// the collection carries a vector index and post-restart search would return
/// empty. An empty seed also makes the boot rebuild skip every index, so a
/// catalog that exists and does not read refuses boot.
pub fn load_vector_index_param_seed(
    config: &ServerConfig,
) -> Result<Vec<nodedb_types::StoredVectorIndexParams>, CatalogSeedError> {
    load_vector_index_param_seed_at(&config.catalog_path())
}

/// [`load_vector_index_param_seed`] against an explicit catalog path.
fn load_vector_index_param_seed_at(
    catalog_path: &std::path::Path,
) -> Result<Vec<nodedb_types::StoredVectorIndexParams>, CatalogSeedError> {
    const SEED: &str = "vector index parameters";
    let Some(catalog) = open_for_seed(SEED, catalog_path)? else {
        return Ok(Vec::new());
    };
    catalog
        .list_all_vector_index_params()
        .map_err(|source| CatalogSeedError::load(SEED, catalog_path, source))
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
/// [`load_doc_config_registry`] for strict document collections. A catalog
/// that exists and does not read refuses boot.
pub fn load_columnar_schema_seed(
    config: &ServerConfig,
) -> Result<ColumnarSchemaSeed, CatalogSeedError> {
    load_columnar_schema_seed_at(&config.catalog_path())
}

/// One columnar-family collection's identity and declared schema, per entry.
pub type ColumnarSchemaSeed = Vec<(
    DatabaseId,
    TenantId,
    String,
    nodedb_types::columnar::ColumnarSchema,
)>;

/// [`load_columnar_schema_seed`] against an explicit catalog path.
fn load_columnar_schema_seed_at(
    catalog_path: &std::path::Path,
) -> Result<ColumnarSchemaSeed, CatalogSeedError> {
    const SEED: &str = "columnar schemas";
    let Some(catalog) = open_for_seed(SEED, catalog_path)? else {
        return Ok(Vec::new());
    };
    let all = crate::bootstrap::constraint_reconcile::load_collections(&catalog)
        .map_err(|source| CatalogSeedError::load(SEED, catalog_path, source))?;

    Ok(all
        .into_iter()
        .filter(|(_, coll)| coll.is_active && coll.collection_type.is_columnar_family())
        .filter_map(|(database_id, coll)| {
            let schema = crate::control::planner::sql_plan_convert::dml::build_columnar_schema(
                &coll.fields,
                coll.declared_primary_key.as_deref().unwrap_or(
                    crate::control::planner::sql_plan_convert::dml::DEFAULT_IDENTITY_COLUMN,
                ),
            );
            // Empty fields are a schemaless collection. Other fields that build no schema are a bad catalog row.
            if schema.is_none() && !coll.fields.is_empty() {
                tracing::error!(
                    collection = %coll.name,
                    tenant_id = coll.tenant_id,
                    "columnar schema does not build from its catalog fields; \
                     the collection has no seeded schema, so check its column names and types"
                );
            }
            let schema = schema?;
            Some((
                database_id,
                TenantId::new(coll.tenant_id),
                coll.name.clone(),
                schema,
            ))
        })
        .collect())
}

/// Why boot refuses to seed the cores from the catalog.
#[derive(Debug, thiserror::Error)]
pub enum CatalogSeedError {
    /// The catalog file exists and did not open.
    #[error(
        "boot refused: catalog {} exists and did not open, so the {seed} are unknown: \
         {detail}; stop any other process holding the file, or restore the catalog from \
         a backup, then start again",
        .path.display()
    )]
    CatalogOpen {
        seed: &'static str,
        path: std::path::PathBuf,
        detail: String,
    },
    /// The catalog opened and the seed's rows did not load.
    #[error(
        "boot refused: the {seed} in catalog {} did not load: {source}; restore the \
         catalog from a backup, then start again",
        .path.display()
    )]
    CatalogLoad {
        seed: &'static str,
        path: std::path::PathBuf,
        #[source]
        source: Box<crate::Error>,
    },
}

impl CatalogSeedError {
    fn load(seed: &'static str, path: &std::path::Path, source: crate::Error) -> Self {
        Self::CatalogLoad {
            seed,
            path: path.to_path_buf(),
            source: Box::new(source),
        }
    }
}

impl From<CatalogSeedError> for crate::Error {
    fn from(e: CatalogSeedError) -> Self {
        Self::Config {
            detail: e.to_string(),
        }
    }
}

/// Open the catalog for one seed. `None` when no catalog file exists: a first
/// boot, the one case with nothing to seed. A file that does not open is
/// locked, corrupt, or unreadable, and an empty seed hides that.
fn open_for_seed(
    seed: &'static str,
    catalog_path: &std::path::Path,
) -> Result<Option<CatalogForRead>, CatalogSeedError> {
    CatalogForRead::open(catalog_path).map_err(|error| CatalogSeedError::CatalogOpen {
        seed,
        path: catalog_path.to_path_buf(),
        detail: error.to_string(),
    })
}

/// Load the user-defined CRS set from the durable catalog so each Data Plane
/// core resolves user-defined SRIDs from its first request, WAL replay
/// included. A committed `CREATE` or `DROP SPATIAL_REF_SYS` reaches a running
/// core through `MetaOp::SetCustomCrsSet`; this seed covers the set as of
/// boot.
///
/// `Err` when a catalog exists and its CRS set does not read. Replay with an
/// empty set fails or misreads every geometry in a user-defined SRID, so boot
/// stops.
pub fn load_crs_set_seed(
    config: &ServerConfig,
) -> Result<Arc<nodedb_crs::CustomCrsSet>, CatalogSeedError> {
    load_crs_set_seed_at(&config.catalog_path())
}

/// [`load_crs_set_seed`] against an explicit catalog path.
fn load_crs_set_seed_at(
    catalog_path: &std::path::Path,
) -> Result<Arc<nodedb_crs::CustomCrsSet>, CatalogSeedError> {
    const SEED: &str = "user-defined CRS definitions";
    let Some(catalog) = open_for_seed(SEED, catalog_path)? else {
        return Ok(Arc::new(nodedb_crs::CustomCrsSet::new()));
    };
    let set = catalog
        .load_custom_crs_set()
        .map_err(|source| CatalogSeedError::load(SEED, catalog_path, source))?;
    Ok(Arc::new(set))
}

/// Result of [`spawn_data_plane_cores`]: the core thread handles plus each
/// core's WAL-replay-completion signal.
pub struct SpawnedDataPlaneCores {
    /// Held only to keep the Data Plane core threads alive; never read.
    pub handles: Vec<std::thread::JoinHandle<()>>,
    /// Per-core one-shot that resolves when the core finishes `replay_all_wal`
    /// (before entering its event loop), carrying the replay's outcome. Boot
    /// awaits every one before opening the client gateway.
    pub replay_done: Vec<tokio::sync::oneshot::Receiver<crate::Result<()>>>,
}

/// Shared Arc resources passed to each Data Plane core at spawn time.
pub struct CoreSharedResources {
    pub governor: Arc<nodedb_mem::MemoryGovernor>,
    pub quiesce: Arc<CollectionQuiesce>,
    pub event_interest: Arc<crate::event::interest::EventInterest>,
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
    pub crs_set_seed: Arc<nodedb_crs::CustomCrsSet>,
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
        event_interest,
        hlc,
        array_catalog,
        quarantine_registry,
        system_metrics,
        maintenance_budget,
        doc_config_seed,
        vector_index_param_seed,
        columnar_schema_seed,
        crs_set_seed,
    } = resources;
    let num_cores = config.server.data_plane_cores;
    let node_id = crate::control::cluster::configured_node_id(config);
    let compaction_cfg = CoreCompactionConfig {
        interval: config.checkpoint.compaction_interval(),
        tombstone_threshold: config.checkpoint.compaction_tombstone_threshold,
        query: config.tuning.query.clone(),
        graph: config.tuning.graph.clone(),
        timeseries: config.tuning.timeseries.clone(),
        vector: config.tuning.vector.clone(),
        checkpoint_interval: std::time::Duration::from_secs(config.checkpoint.interval_secs),
        commit_snapshot_window: config.tuning.network.snapshot_max_hold(),
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
            node_id,
            request_rx: data_side.request_rx,
            response_tx: data_side.response_tx,
            data_dir: &config.server.data_dir,
            wal_records: Arc::clone(&wal_records),
            tombstones: replay_tombstones.clone(),
            num_cores,
            compaction_config: compaction_cfg.clone(),
            system_metrics: Some(Arc::clone(&system_metrics)),
            event_producer: Some(event_producer),
            event_interest: Arc::clone(&event_interest),
            governor: Arc::clone(&governor),
            quiesce: Some(Arc::clone(&quiesce)),
            hlc: Arc::clone(&hlc),
            array_catalog: Arc::clone(&array_catalog),
            quarantine_registry: Arc::clone(&quarantine_registry),
            maintenance_budget: Arc::clone(&maintenance_budget),
            doc_config_seed: Arc::clone(&doc_config_seed),
            vector_index_param_seed: Arc::clone(&vector_index_param_seed),
            columnar_schema_seed: Arc::clone(&columnar_schema_seed),
            crs_set_seed: Arc::clone(&crs_set_seed),
            replay_done: replay_done_tx,
        })?;
        core_handles.push(handle);
        replay_done_rxs.push(replay_done_rx);
        notifiers.push((core_id, notifier));
    }

    for (core_id, notifier) in notifiers {
        dispatcher.set_notifier(core_id, notifier);
    }

    info!(num_cores, "data plane cores running (eventfd-driven)");
    Ok(SpawnedDataPlaneCores {
        handles: core_handles,
        replay_done: replay_done_rxs,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every loader against one catalog path, as `seed: outcome`.
    fn every_seed(path: &std::path::Path) -> Vec<(&'static str, Result<(), CatalogSeedError>)> {
        vec![
            ("crs", load_crs_set_seed_at(path).map(|_| ())),
            ("array", load_array_catalog_at(path).map(|_| ())),
            ("doc", load_doc_config_registry_at(path).map(|_| ())),
            ("vector", load_vector_index_param_seed_at(path).map(|_| ())),
            ("columnar", load_columnar_schema_seed_at(path).map(|_| ())),
        ]
    }

    /// A first boot has no catalog file. Every seed is empty and boot goes on.
    #[test]
    fn an_absent_catalog_seeds_every_loader_empty() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("system.redb");

        for (seed, outcome) in every_seed(&path) {
            assert!(outcome.is_ok(), "{seed}: a first boot goes on: {outcome:?}");
        }
        assert!(load_doc_config_registry_at(&path).expect("doc").is_empty());
        assert!(
            load_vector_index_param_seed_at(&path)
                .expect("vector")
                .is_empty()
        );
        assert!(
            load_columnar_schema_seed_at(&path)
                .expect("columnar")
                .is_empty()
        );
    }

    /// A catalog with no rows is a real catalog with nothing to seed.
    #[test]
    fn an_empty_catalog_seeds_every_loader() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("system.redb");
        drop(crate::control::security::catalog::SystemCatalog::open(&path).expect("catalog"));

        for (seed, outcome) in every_seed(&path) {
            assert!(outcome.is_ok(), "{seed}: {outcome:?}");
        }
    }

    /// A catalog that does not open stops boot in every loader. An empty seed
    /// looks like a first boot, and hides what the catalog declares.
    #[test]
    fn a_catalog_that_does_not_open_refuses_boot_in_every_loader() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("system.redb");
        std::fs::write(&path, vec![0xAB; 8192]).expect("write garbage catalog");

        for (seed, outcome) in every_seed(&path) {
            let error = outcome.expect_err(seed);
            assert!(
                matches!(error, CatalogSeedError::CatalogOpen { .. }),
                "{seed}: {error}"
            );
            let message = error.to_string();
            assert!(message.contains("system.redb"), "names the file: {message}");
            assert!(
                message.contains("did not open"),
                "names the cause: {message}"
            );
            assert!(
                message.contains("then start again"),
                "names the action: {message}"
            );
            assert!(matches!(
                crate::Error::from(error),
                crate::Error::Config { .. }
            ));
        }
    }

    /// A catalog whose collections do not read refuses the seed.
    struct UnreadableCollections;

    impl crate::bootstrap::constraint_reconcile::CollectionSource for UnreadableCollections {
        fn list_database_ids(&self) -> crate::Result<Vec<DatabaseId>> {
            Ok(Vec::new())
        }

        fn collections_in(
            &self,
            _database_id: DatabaseId,
        ) -> crate::Result<Vec<crate::control::security::catalog::StoredCollection>> {
            Err(crate::Error::Internal {
                detail: "collections table unreadable".into(),
            })
        }

        fn collections_for_tenant(
            &self,
            _database_id: DatabaseId,
            _tenant_id: u64,
        ) -> crate::Result<Vec<crate::control::security::catalog::StoredCollection>> {
            Ok(Vec::new())
        }
    }

    #[test]
    fn a_read_error_after_the_open_is_an_error() {
        assert!(load_doc_config_registry_from(&UnreadableCollections).is_err());
    }

    #[test]
    fn a_load_error_names_the_seed_file_and_action() {
        let error = CatalogSeedError::load(
            "columnar schemas",
            std::path::Path::new("/data/system.redb"),
            crate::Error::Internal {
                detail: "table unreadable".into(),
            },
        );
        let message = error.to_string();
        for part in [
            "columnar schemas",
            "/data/system.redb",
            "table unreadable",
            "then start again",
        ] {
            assert!(message.contains(part), "{part}: {message}");
        }
    }

    /// A read error refuses the array seed.
    #[test]
    fn the_array_registration_reports_a_read_error() {
        let mut registry = ArrayCatalog::new();
        let failed = crate::control::array_catalog::persist::register_loaded(
            &mut registry,
            Err(crate::Error::Internal {
                detail: "arrays table unreadable".into(),
            }),
        );
        assert!(failed.is_err());
        crate::control::array_catalog::persist::register_loaded(&mut registry, Ok(Vec::new()))
            .expect("nothing to register");
    }
}
