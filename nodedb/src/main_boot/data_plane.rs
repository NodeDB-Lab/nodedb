// SPDX-License-Identifier: BUSL-1.1

//! Memory governor, WAL, SPSC bridge, Event Bus, and Data Plane core
//! spawn — the boot phase that stands up everything the Data Plane
//! needs before `SharedState::open`.

use std::sync::Arc;

use nodedb::ServerConfig;
use nodedb::bootstrap;
use nodedb::bridge::dispatch::{DATA_PLANE_QUEUE_CAPACITY, Dispatcher};

/// Everything downstream boot phases need from Data Plane bootstrap,
/// bundled so the call site doesn't juggle 15 separate `let`s.
pub(crate) struct DataPlaneBootstrap {
    pub(crate) dispatcher: Dispatcher,
    pub(crate) wal: Arc<nodedb::wal::WalManager>,
    pub(crate) wal_records: Arc<[nodedb_wal::WalRecord]>,
    /// Collection tombstones recovered alongside the WAL tail. Shared with the
    /// surrogate-replay pass so a dropped collection's `(pk → surrogate)` binds
    /// are not resurrected into the catalog.
    pub(crate) replay_tombstones: Arc<nodedb_wal::TombstoneSet>,
    pub(crate) num_cores: usize,
    pub(crate) event_consumers: Vec<nodedb::event::bus::EventConsumerRx>,
    pub(crate) system_metrics: Arc<nodedb::control::metrics::SystemMetrics>,
    pub(crate) quiesce: Arc<nodedb::bridge::quiesce::CollectionQuiesce>,
    /// The collections some Event Plane consumer reads. Every core holds it.
    /// The Control Plane registries' slices are installed once
    /// `SharedState` exists.
    pub(crate) event_interest: Arc<nodedb::event::interest::EventInterest>,
    pub(crate) array_catalog: nodedb::control::array_catalog::ArrayCatalogHandle,
    pub(crate) quarantine_registry: Arc<nodedb::storage::quarantine::QuarantineRegistry>,
    pub(crate) maintenance_budget: Arc<nodedb::control::maintenance::MaintenanceBudgetTracker>,
    pub(crate) governor: Arc<nodedb_mem::governor::MemoryGovernor>,
    pub(crate) watermark_store: Arc<nodedb::event::watermark::WatermarkStore>,
    pub(crate) trigger_dlq: Arc<std::sync::Mutex<nodedb::event::trigger::TriggerDlq>>,
    /// The cluster handle. A server with no `[cluster]` section runs a
    /// synthesized one-node cluster, so every server has one.
    pub(crate) cluster_handle: Arc<nodedb::control::cluster::ClusterHandle>,
    // Held only to keep the Data Plane core threads alive; never read.
    pub(crate) _core_handles: Vec<std::thread::JoinHandle<()>>,
    /// Per-core WAL-replay-completion signals. Boot awaits every one before
    /// firing the gateway readiness gate so `/healthz` reports ready only once
    /// every core has rebuilt its in-memory indexes from the WAL.
    pub(crate) replay_done: Vec<tokio::sync::oneshot::Receiver<nodedb::Result<()>>>,
}

/// Run the full Data Plane bootstrap phase. Runs between WAL init and
/// `SharedState::open`, kept out of `main()` for readability.
pub(crate) async fn bootstrap_data_plane(
    config: &ServerConfig,
    wal_gate: &nodedb::control::startup::ReadyGate,
) -> anyhow::Result<DataPlaneBootstrap> {
    // Initialize memory governor (per-engine budgets + global ceiling).
    let byte_budgets = config.engines.to_byte_budgets(config.server.memory_limit);
    let governor = nodedb::memory::init_governor(config.server.memory_limit, &byte_budgets)?;

    // Open WAL, validate, replay, and load tombstone set.
    // The WAL key comes from the source `[encryption]` names: a key file,
    // Vault or AWS KMS. A Vault request goes through the `[egress]` policy.
    let wal_key = match &config.encryption {
        Some(settings) => {
            let key = nodedb::control::security::keystore::load_wal_key(
                settings,
                Arc::new(config.egress.checked_policy()?),
            )
            .await?;
            tracing::info!(source = settings.source()?.kind(), "WAL key loaded");
            Some(key)
        }
        None => None,
    };
    let (wal, wal_records, replay_tombstones) = bootstrap::wal_init::init_wal(config, wal_key)?;
    wal_gate.fire();

    // Create SPSC bridge: Dispatcher (Control Plane) + CoreChannelDataSide (Data Plane).
    let num_cores = config.server.data_plane_cores;
    let (mut dispatcher, data_sides) = Dispatcher::new(num_cores, DATA_PLANE_QUEUE_CAPACITY);

    // Create Event Bus: per-core ring buffers (Data Plane → Event Plane).
    let (event_producers, event_consumers) = nodedb::event::bus::create_event_bus(num_cores);

    // Start Data Plane cores on dedicated OS threads (thread-per-core).
    // Each core gets: jemalloc arena pinning + eventfd-driven wake + WAL replay + event producer.
    // Sized for the configured core count so every core's `core_id` indexes a
    // heartbeat slot the stall monitor can sample.
    let system_metrics = Arc::new(nodedb::control::metrics::SystemMetrics::with_cores(
        num_cores,
    ));

    // Create the shared scan-quiesce registry up front so every Data
    // Plane core and (below) `SharedState::open` reference the same
    // instance. The registry is the integration point between Control
    // Plane purge-time `begin_drain` and per-core scan-time
    // `try_start_scan` — splitting it would make drain a no-op.
    let quiesce = nodedb::bridge::quiesce::CollectionQuiesce::new();

    // One set for every core. It holds no collection until the Control
    // Plane registries are installed, so WAL replay emits no on-demand event.
    let event_interest = nodedb::event::interest::EventInterest::new();

    // Load the persisted ND-array catalog once, before spawning cores.
    let array_catalog = bootstrap::data_plane::load_array_catalog(config);

    // Load every active collection's schema from the durable catalog once,
    // before spawning cores, so each core can seed `doc_configs` ahead of
    // its own WAL redo replay (see `load_doc_config_registry` docs).
    let doc_config_seed = Arc::new(bootstrap::data_plane::load_doc_config_registry(config));

    // Load every persisted `CREATE VECTOR INDEX`'s build parameters once,
    // before spawning cores, so each core can seed its vector-index config and
    // rebuild the HNSW from the durable document store on boot (the WAL
    // `VectorParams` record is not crash-durable).
    let vector_index_param_seed =
        Arc::new(bootstrap::data_plane::load_vector_index_param_seed(config));

    // Seed each core with every columnar-family collection's real catalog
    // schema so a fresh `MutationEngine` created during WAL redo replay is
    // pre-registered with its declared types (Geometry, Timestamp, Decimal,
    // etc.) instead of falling back to lossy inference from the first
    // replayed row.
    let columnar_schema_seed = Arc::new(bootstrap::data_plane::load_columnar_schema_seed(config));

    // Seed each core with the catalog's user-defined CRS set, so WAL replay
    // and the first request resolve user-defined SRIDs.
    let crs_set_seed = bootstrap::data_plane::load_crs_set_seed(config);

    // Create the quarantine registry before spawning cores.
    let quarantine_registry =
        std::sync::Arc::new(nodedb::storage::quarantine::QuarantineRegistry::new());

    // Create once and share with both Data Plane cores and SharedState so
    // ALTER DATABASE SET QUOTA updates live caps immediately for all cores.
    let maintenance_budget =
        Arc::new(nodedb::control::maintenance::MaintenanceBudgetTracker::new());

    let bootstrap::data_plane::SpawnedDataPlaneCores {
        handles: _core_handles,
        replay_done,
    } = bootstrap::data_plane::spawn_data_plane_cores(
        config,
        data_sides,
        event_producers,
        Arc::clone(&wal_records),
        replay_tombstones.clone(),
        &mut dispatcher,
        bootstrap::data_plane::CoreSharedResources {
            governor: Arc::clone(&governor),
            quiesce: Arc::clone(&quiesce),
            event_interest: Arc::clone(&event_interest),
            hlc: wal.ordinal_clock(),
            array_catalog: Arc::clone(&array_catalog),
            quarantine_registry: Arc::clone(&quarantine_registry),
            system_metrics: Arc::clone(&system_metrics),
            maintenance_budget: Arc::clone(&maintenance_budget),
            doc_config_seed: Arc::clone(&doc_config_seed),
            vector_index_param_seed: Arc::clone(&vector_index_param_seed),
            columnar_schema_seed: Arc::clone(&columnar_schema_seed),
            crs_set_seed: Arc::clone(&crs_set_seed),
        },
    )?;

    // Event Plane resources (spawned after SharedState is created — needs it for trigger dispatch).
    let watermark_store = Arc::new(
        nodedb::event::watermark::WatermarkStore::open(&config.server.data_dir)
            .expect("failed to open event plane watermark store"),
    );
    let trigger_dlq = Arc::new(std::sync::Mutex::new(
        nodedb::event::trigger::TriggerDlq::open(&config.server.data_dir)
            .expect("failed to open trigger DLQ"),
    ));

    // Every server runs a cluster handle. `[cluster]` selects a real
    // deployment. Without it, the server synthesizes a one-node cluster so
    // the sequencer and per-vShard schedulers run, and cross-core
    // transactions take the deterministic Calvin path.
    let handle = if let Some(ref cluster_cfg) = config.cluster {
        cluster_cfg
            .validate()
            .map_err(|e| anyhow::anyhow!("cluster config: {e}"))?;
        nodedb::control::cluster::init_cluster(
            cluster_cfg,
            &config.server.data_dir,
            &config.tuning.cluster_transport,
        )
        .await?
    } else {
        nodedb::control::cluster::init_single_node_calvin(
            &config.server.data_dir,
            &config.tuning.cluster_transport,
        )
        .await?
    };
    let cluster_handle = Arc::new(handle);

    Ok(DataPlaneBootstrap {
        dispatcher,
        wal,
        wal_records,
        replay_tombstones: Arc::new(replay_tombstones),
        num_cores,
        event_consumers,
        system_metrics,
        quiesce,
        event_interest,
        array_catalog,
        quarantine_registry,
        maintenance_budget,
        governor,
        watermark_store,
        trigger_dlq,
        cluster_handle,
        _core_handles,
        replay_done,
    })
}
