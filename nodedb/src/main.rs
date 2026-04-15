#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tracing::info;
use tracing_subscriber::EnvFilter;

use nodedb::ServerConfig;
use nodedb::bridge::dispatch::Dispatcher;
use nodedb::config::server::apply_env_overrides;
use nodedb::control::startup::{StartupPhase, StartupSequencer};
use nodedb::control::state::SharedState;
use nodedb::data::runtime::spawn_core;
use nodedb::wal::WalManager;

fn build_tls_acceptor(
    tls: &nodedb::config::server::TlsSettings,
) -> anyhow::Result<pgwire::tokio::TlsAcceptor> {
    use std::fs::File;
    use std::io::BufReader;

    let cert_file = File::open(&tls.cert_path)
        .map_err(|e| anyhow::anyhow!("failed to open TLS cert {}: {e}", tls.cert_path.display()))?;
    let key_file = File::open(&tls.key_path)
        .map_err(|e| anyhow::anyhow!("failed to open TLS key {}: {e}", tls.key_path.display()))?;

    let certs: Vec<_> = rustls_pemfile::certs(&mut BufReader::new(cert_file))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| anyhow::anyhow!("failed to parse TLS certs: {e}"))?;

    let key = rustls_pemfile::private_key(&mut BufReader::new(key_file))
        .map_err(|e| anyhow::anyhow!("failed to parse TLS key: {e}"))?
        .ok_or_else(|| anyhow::anyhow!("no private key found in {}", tls.key_path.display()))?;

    let server_config = tokio_rustls::rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| anyhow::anyhow!("TLS config error: {e}"))?;

    Ok(pgwire::tokio::TlsAcceptor::from(Arc::new(server_config)))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Resolve config file path.
    // Priority: CLI arg (highest) > NODEDB_CONFIG env var > default.
    let config_path: Option<PathBuf> = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .or_else(|| std::env::var("NODEDB_CONFIG").ok().map(PathBuf::from));

    // Load config first (needed for log format).
    // Environment variable overrides are applied after tracing is initialised
    // (see below) so that info!/warn! messages are actually emitted.
    let mut config = match config_path {
        Some(ref path) => ServerConfig::from_file(path)?,
        None => ServerConfig::default(),
    };

    // Apply env overrides once now (before tracing) so that log_format is
    // correct in case NODEDB_DATA_DIR / NODEDB_MEMORY_LIMIT also affect it.
    // The overrides are re-applied silently here; the real log messages
    // will be emitted by the second call after the subscriber is registered.
    apply_env_overrides(&mut config);

    // Initialize tracing with format from config.
    // Default to warn level for clean startup. Use RUST_LOG=info for verbose.
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"));
    if config.log_format == "json" {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_writer(std::io::stderr)
            .json()
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_writer(std::io::stderr)
            .init();
    }

    // Re-apply env overrides now that tracing is initialised so that
    // info!/warn! messages are actually emitted for operators.
    apply_env_overrides(&mut config);

    match &config_path {
        None => info!("no config file provided, using defaults"),
        Some(path)
            if std::env::var("NODEDB_CONFIG").is_ok() && std::env::args().nth(1).is_none() =>
        {
            info!(
                path = %path.display(),
                "config file loaded from NODEDB_CONFIG"
            );
        }
        Some(_) => {}
    }

    info!(
        host = %config.host,
        native_port = config.ports.native,
        cores = config.data_plane_cores,
        memory_limit = config.memory_limit,
        "starting nodedb"
    );

    // Validate engine config.
    config.engines.validate()?;

    // Construct the gate-based startup sequencer. Gates for each phase are
    // registered before the subsystem that owns that phase begins its work,
    // and fired immediately after it reports ready. The `startup_gate` is
    // installed on `SharedState` after `open()` returns so every code path
    // that calls `await_phase` can observe phase transitions in real time.
    let (startup_seq, startup_gate) = StartupSequencer::new();

    // Register all gates up-front so the sequencer knows every phase has
    // an owner. Phases that have no concurrent sub-tasks get a single gate
    // that is fired inline.
    let wal_gate = startup_seq.register_gate(StartupPhase::WalRecovery, "wal");
    let catalog_gate =
        startup_seq.register_gate(StartupPhase::ClusterCatalogOpen, "cluster-catalog");
    let raft_gate =
        startup_seq.register_gate(StartupPhase::RaftMetadataReplay, "raft-metadata-replay");
    let schema_gate =
        startup_seq.register_gate(StartupPhase::SchemaCacheWarmup, "schema-cache-warmup");
    let sanity_gate =
        startup_seq.register_gate(StartupPhase::CatalogSanityCheck, "catalog-sanity-check");
    let data_groups_gate =
        startup_seq.register_gate(StartupPhase::DataGroupsReplay, "data-groups-replay");
    let transport_gate = startup_seq.register_gate(StartupPhase::TransportBind, "transport-bind");
    let warm_peers_gate = startup_seq.register_gate(StartupPhase::WarmPeers, "warm-peers");
    let health_loop_gate = startup_seq.register_gate(StartupPhase::HealthLoopStart, "health-loop");
    let gateway_enable_gate =
        startup_seq.register_gate(StartupPhase::GatewayEnable, "gateway-enable");

    // Initialize memory governor (per-engine budgets + global ceiling).
    let byte_budgets = config.engines.to_byte_budgets(config.memory_limit);
    let governor = nodedb::memory::init_governor(config.memory_limit, &byte_budgets)?;

    // Open WAL (with optional encryption at rest).
    let wal_segment_target = config.checkpoint.wal_segment_target_bytes();
    let wal = {
        let mut mgr = WalManager::open_with_tuning(
            &config.wal_dir(),
            false,
            wal_segment_target,
            &config.tuning.wal,
        )?;
        if let Some(ref enc) = config.encryption {
            let key = nodedb_wal::crypto::WalEncryptionKey::from_file(&enc.key_path)
                .map_err(nodedb::Error::Wal)?;
            mgr.set_encryption_ring(nodedb_wal::crypto::KeyRing::new(key));
            info!(key_path = %enc.key_path.display(), "WAL encryption enabled");
        }
        Arc::new(mgr)
    };
    info!(next_lsn = %wal.next_lsn(), "WAL ready");

    // Strict integrity check: any non-empty segment that contains no valid
    // WAL records is treated as fatal corruption. This fires before wal_gate
    // so the sequencer never reaches GatewayEnable on a corrupted WAL.
    if let Err(e) = wal.validate_for_startup() {
        tracing::error!(
            error = %e,
            "StartupError: WAL validation failed — cannot start with corrupted WAL segments"
        );
        std::process::exit(1);
    }

    wal_gate.fire();

    // Replay WAL records for crash recovery (shared across all cores).
    let wal_records: Arc<[nodedb_wal::WalRecord]> = match wal.replay() {
        Ok(records) => {
            if !records.is_empty() {
                info!(records = records.len(), "WAL records loaded for replay");
            }
            Arc::from(records.into_boxed_slice())
        }
        Err(e) => {
            tracing::error!(
                error = %e,
                "StartupError: WAL replay failed — cannot start with a corrupt or unreadable WAL"
            );
            std::process::exit(1);
        }
    };

    // Create SPSC bridge: Dispatcher (Control Plane) + CoreChannelDataSide (Data Plane).
    let num_cores = config.data_plane_cores;
    let (mut dispatcher, data_sides) = Dispatcher::new(num_cores, 1024);

    // Create Event Bus: per-core ring buffers (Data Plane → Event Plane).
    let (event_producers, event_consumers) = nodedb::event::bus::create_event_bus(num_cores);

    // Start Data Plane cores on dedicated OS threads (thread-per-core).
    // Each core gets: jemalloc arena pinning + eventfd-driven wake + WAL replay + event producer.
    let compaction_cfg = nodedb::data::runtime::CoreCompactionConfig {
        interval: config.checkpoint.compaction_interval(),
        tombstone_threshold: config.checkpoint.compaction_tombstone_threshold,
        query: config.tuning.query.clone(),
    };
    let system_metrics = Arc::new(nodedb::control::metrics::SystemMetrics::new());
    let mut core_handles = Vec::with_capacity(num_cores);
    let mut notifiers = Vec::with_capacity(num_cores);
    for (core_id, (data_side, event_producer)) in
        data_sides.into_iter().zip(event_producers).enumerate()
    {
        let (handle, notifier) = spawn_core(
            core_id,
            data_side.request_rx,
            data_side.response_tx,
            &config.data_dir,
            Arc::clone(&wal_records),
            num_cores,
            compaction_cfg.clone(),
            Some(Arc::clone(&system_metrics)),
            Some(event_producer),
            Arc::clone(&governor),
        )?;
        core_handles.push(handle);
        notifiers.push((core_id, notifier));
    }

    // Wire notifiers into the dispatcher so it signals cores after pushing requests.
    for (core_id, notifier) in &notifiers {
        dispatcher.set_notifier(*core_id, *notifier);
    }

    info!(num_cores, "data plane cores running (eventfd-driven)");

    // Event Plane resources (spawned after SharedState is created — needs it for trigger dispatch).
    let watermark_store = Arc::new(
        nodedb::event::watermark::WatermarkStore::open(&config.data_dir)
            .expect("failed to open event plane watermark store"),
    );
    let trigger_dlq = Arc::new(std::sync::Mutex::new(
        nodedb::event::trigger::TriggerDlq::open(&config.data_dir)
            .expect("failed to open trigger DLQ"),
    ));

    // Initialize cluster mode if configured.
    let cluster_handle = if let Some(ref cluster_cfg) = config.cluster {
        cluster_cfg
            .validate()
            .map_err(|e| anyhow::anyhow!("cluster config: {e}"))?;
        let handle = nodedb::control::cluster::init_cluster(
            cluster_cfg,
            &config.data_dir,
            &config.tuning.cluster_transport,
        )
        .await?;
        Some(handle)
    } else {
        None
    };

    // Create shared state with persistent system catalog.
    let mut shared = SharedState::open(
        dispatcher,
        Arc::clone(&wal),
        &config.catalog_path(),
        &config.auth,
        config.tuning.clone(),
    )?;

    // Install the real startup gate on SharedState so listeners and health
    // checks read live phase transitions. The placeholder gate created
    // inside `SharedState::open` is discarded here.
    if let Some(state) = Arc::get_mut(&mut shared) {
        state.startup = Arc::clone(&startup_gate);
    }

    // System catalog (redb) is open — fire the ClusterCatalogOpen gate.
    catalog_gate.fire();

    // Wire cluster handles into SharedState so that every code path
    // which checks `state.cluster_topology` / `state.cluster_transport`
    // (pgwire routing, scatter-gather, CDC transport, /health, etc.)
    // actually sees the live cluster. Without this the fields stay
    // None and cluster-mode features silently degrade to single-node
    // behaviour. Arc::get_mut is valid here because no clones exist yet.
    if let Some(ref handle) = cluster_handle
        && let Some(state) = Arc::get_mut(&mut shared)
    {
        state.node_id = handle.node_id;
        state.cluster_topology = Some(Arc::clone(&handle.topology));
        state.cluster_routing = Some(Arc::clone(&handle.routing));
        state.cluster_transport = Some(Arc::clone(&handle.transport));
        // Share the metadata cache + applied-index watcher that
        // cluster::init_cluster allocated, so the MetadataCommitApplier
        // (installed in start_raft) writes into the same cache readers
        // observe via `SharedState::metadata_cache`.
        state.metadata_cache = Arc::clone(&handle.metadata_cache);
        state.metadata_applied_index_watcher = Arc::clone(&handle.applied_index_watcher);
    }

    // Initialise JWKS registry if JWT providers are configured.
    if let Some(ref jwt_config) = config.auth.jwt
        && !jwt_config.providers.is_empty()
        && let Some(state) = Arc::get_mut(&mut shared)
    {
        let registry = tokio::runtime::Handle::current().block_on(
            nodedb::control::security::jwks::registry::JwksRegistry::init(jwt_config.clone()),
        );
        state.jwks_registry = Some(std::sync::Arc::new(registry));
        info!(
            "JWKS registry initialised with {} providers",
            jwt_config.providers.len()
        );
    }

    // Initialise cold storage (L2 tiering) if configured.
    // Arc::get_mut is valid here because no clones of `shared` exist yet.
    if let Some(ref cold_settings) = config.cold_storage {
        let cold_config = cold_settings.to_cold_storage_config();
        match nodedb::storage::cold::ColdStorage::new(cold_config) {
            Ok(cold) => {
                if let Some(state) = Arc::get_mut(&mut shared) {
                    state.cold_storage = Some(Arc::new(cold));
                    info!("cold storage (L2 tiering) initialised");
                } else {
                    tracing::warn!(
                        "cold storage: Arc::get_mut failed (unexpected clone), skipping"
                    );
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "cold storage init failed, tiering disabled");
            }
        }
    }

    // Wire memory governor into SharedState for observability.
    if let Some(state) = Arc::get_mut(&mut shared) {
        state.governor = Some(Arc::clone(&governor));
    }

    // Construct the gateway and install it (plus its DDL invalidator) on
    // SharedState. Must happen after cluster topology is wired and before
    // listeners bind. Arc::get_mut is valid here because no listener has
    // cloned `shared` yet.
    {
        // Clone before the mutable borrow so the Gateway can hold its own Arc.
        let shared_for_gateway = Arc::clone(&shared);
        if let Some(state) = Arc::get_mut(&mut shared) {
            let gateway =
                std::sync::Arc::new(nodedb::control::gateway::Gateway::new(shared_for_gateway));
            let invalidator = std::sync::Arc::new(
                nodedb::control::gateway::PlanCacheInvalidator::new(&gateway.plan_cache),
            );
            state.gateway = Some(Arc::clone(&gateway));
            state.gateway_invalidator = Some(invalidator);
        }
    }

    // Bootstrap credentials.
    let auth_mode = config.auth.mode.clone();
    match config.auth.resolve_superuser_password() {
        Ok(Some(password)) => {
            shared
                .credentials
                .bootstrap_superuser(&config.auth.superuser_name, &password)?;
            info!(
                user = config.auth.superuser_name,
                mode = ?auth_mode,
                "superuser bootstrapped"
            );
        }
        Ok(None) => {
            // Trust mode — no credentials needed.
            info!(mode = ?auth_mode, "auth mode: trust (no authentication)");
        }
        Err(e) => {
            return Err(e.into());
        }
    }

    // All shutdown signals flow through the canonical
    // `ShutdownWatch` held on `SharedState`. The local
    // `shutdown_rx` binding below is a raw-receiver view of
    // that same watch, preserved so the existing listener APIs
    // (`PgListener::run`, `HttpServer::run`, `IlpListener::run`,
    // `RespListener::run`, `spawn_cold_storage_loop`,
    // `spawn_checkpoint_loop`, and the lease renewal loop)
    // keep their `watch::Receiver<bool>` parameter unchanged.
    // New code SHOULD use `shared.shutdown.subscribe()`.
    let shutdown_rx = shared.shutdown.raw_receiver();

    // Unified shutdown bus: phased drain with per-phase 500 ms budgets.
    // `ShutdownBus::initiate()` signals the flat `ShutdownWatch` so all
    // existing `watch::Receiver<bool>` subscribers wake up as well.
    let (shutdown_bus, _shutdown_bus_handle) =
        nodedb::control::shutdown::ShutdownBus::new(Arc::clone(&shared.shutdown));
    // Wire system metrics so the bus records `shutdown_last_duration_ms{phase}`
    // for each phase transition during graceful shutdown.
    shutdown_bus.set_metrics(Arc::clone(&system_metrics));

    // Test-only injection: if NODEDB_TEST_SLOW_DRAIN_TASK=1, register a drain
    // task that sleeps for 2s without calling report_drained, to verify the
    // offender-abort path in integration tests. This code path is guarded
    // by an env var so it is never activated in production.
    if std::env::var("NODEDB_TEST_SLOW_DRAIN_TASK").as_deref() == Ok("1") {
        let mut guard = shutdown_bus.register_task(
            nodedb::control::shutdown::ShutdownPhase::DrainingListeners,
            "test_slow_task",
            None,
        );
        tokio::spawn(async move {
            guard.await_signal().await;
            // Intentionally do NOT call report_drained — tests the offender path.
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            drop(guard); // This will log the "dropped without report_drained" warning.
        });
    }

    // Start cluster Raft loop if in cluster mode. The returned
    // receiver flips to `true` after the metadata raft group has
    // applied its first entry on this node — see
    // `nodedb-cluster::RaftLoop::subscribe_ready`. We hold on to it
    // and await it just before binding client-facing listeners so
    // the first DDL after process start cannot race against an
    // uninitialized metadata group.
    let raft_ready_rx: Option<tokio::sync::watch::Receiver<bool>> =
        if let Some(ref handle) = cluster_handle {
            Some(nodedb::control::cluster::start_raft(
                handle,
                Arc::clone(&shared),
                &config.data_dir,
                shutdown_rx.clone(),
                &config.tuning.cluster_transport,
            )?)
        } else {
            None
        };

    // Spawn the descriptor lease renewal loop. Returns None on
    // single-node clusters (no metadata raft handle wired) — the
    // returned JoinHandle is dropped on the floor because the loop
    // subscribes to `shutdown_rx` and exits cleanly on Ctrl+C.
    let _lease_renewal = nodedb::control::lease::LeaseRenewalLoop::spawn(
        Arc::clone(&shared),
        &config.tuning.cluster_transport,
        shutdown_rx.clone(),
    );

    // Start response poller: routes Data Plane responses to
    // waiting sessions.
    //
    // Adaptive backoff strategy: under load we use `yield_now()`
    // for microsecond-level responsiveness (tokio's timer wheel
    // has 1ms granularity, so sleep(100us) actually sleeps ~1ms,
    // adding 1ms to every request's latency). When the poller
    // observes an idle streak we ramp the wait up so an idle
    // server does not peg an entire tokio worker at 100% CPU.
    //
    // - Active (response just routed OR within the last ~256 yields):
    //   yield_now() — sub-millisecond latency for bursts.
    // - Idle for 256+ iterations: sleep 1ms (still responsive,
    //   matches the timer wheel minimum).
    // - Idle for 1024+ iterations (~1s of true idleness): sleep
    //   10ms — bounds idle CPU to ~0.1% of one core.
    let shared_poller = Arc::clone(&shared);
    nodedb::control::shutdown::spawn_loop(
        &shared.loop_registry,
        &shared.shutdown,
        "response_poller",
        move |shutdown| async move {
            let mut idle_iters: u32 = 0;
            loop {
                if shutdown.is_cancelled() {
                    break;
                }
                let routed = shared_poller.poll_and_route_responses();
                if routed > 0 {
                    idle_iters = 0;
                    tokio::task::yield_now().await;
                    continue;
                }
                idle_iters = idle_iters.saturating_add(1);
                if idle_iters <= 256 {
                    tokio::task::yield_now().await;
                } else if idle_iters <= 1024 {
                    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                } else {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
            }
        },
    );

    // WAL catch-up is handled at startup by replay_timeseries_wal().
    // No continuous catch-up task is needed: the ILP listener's dispatch
    // to the Data Plane is synchronous (waits for SPSC capacity), so
    // all WAL-acknowledged data always reaches the Data Plane.
    // The catch-up task only runs in test harnesses that simulate SPSC drops.

    // Event trigger processor: evaluates DEFINE EVENT triggers on writes.
    nodedb::control::event_trigger::spawn_event_trigger_processor(Arc::clone(&shared));

    // Wire webhook manager with Arc<SharedState> so it can spawn delivery tasks.
    shared.webhook_manager.set_state(Arc::clone(&shared));

    // Spawn Event Plane: one consumer Tokio task per Data Plane core.
    // Kept alive until process exit — Drop impl aborts consumer tasks.
    // CdcRouter lives in SharedState (shared with DDL handlers for drop cleanup).
    let _event_plane = nodedb::event::EventPlane::spawn(
        event_consumers,
        Arc::clone(&wal),
        watermark_store,
        Arc::clone(&shared),
        trigger_dlq,
        Arc::clone(&shared.cdc_router),
        Arc::clone(&shared.shutdown),
    );
    info!(num_cores, "event plane running");

    // Tenant rate counter reset (1-second timer).
    let shared_rate = Arc::clone(&shared);
    nodedb::control::shutdown::spawn_loop(
        &shared.loop_registry,
        &shared.shutdown,
        "tenant_rate_reset",
        move |mut shutdown| async move {
            let mut tick = tokio::time::interval(Duration::from_secs(1));
            loop {
                tokio::select! {
                    _ = shutdown.wait_cancelled() => break,
                    _ = tick.tick() => shared_rate.reset_tenant_rate_counters(),
                }
            }
        },
    );

    // Audit log flush (10-second timer).
    let shared_audit = Arc::clone(&shared);
    nodedb::control::shutdown::spawn_loop(
        &shared.loop_registry,
        &shared.shutdown,
        "audit_log_flush",
        move |mut shutdown| async move {
            let mut tick = tokio::time::interval(Duration::from_secs(10));
            loop {
                tokio::select! {
                    _ = shutdown.wait_cancelled() => break,
                    _ = tick.tick() => shared_audit.flush_audit_log(),
                }
            }
        },
    );

    // Tenant memory estimation (30-second timer).
    let shared_mem = Arc::clone(&shared);
    nodedb::control::shutdown::spawn_loop(
        &shared.loop_registry,
        &shared.shutdown,
        "tenant_memory_estimate",
        move |mut shutdown| async move {
            let mut tick = tokio::time::interval(Duration::from_secs(30));
            loop {
                tokio::select! {
                    _ = shutdown.wait_cancelled() => break,
                    _ = tick.tick() => shared_mem.update_tenant_memory_estimates(),
                }
            }
        },
    );

    // Checkpoint manager: periodic engine flush + WAL truncation.
    let shared_ckpt = Arc::clone(&shared);
    let shutdown_rx_ckpt = shutdown_rx.clone();
    nodedb::control::checkpoint_manager::spawn_checkpoint_task(
        shared_ckpt,
        num_cores,
        config.checkpoint.to_manager_config(),
        shutdown_rx_ckpt,
    );

    // Usage metering flush: drain per-core counters → UsageStore every 60 seconds.
    // JoinHandle intentionally held — task runs for the lifetime of the process.
    let _metering_flush = nodedb::control::security::metering::counter::spawn_flush_task(
        Arc::clone(&shared.usage_counter),
        Arc::clone(&shared.usage_store),
        60,
    );

    // Cold tier task: upload old L1 segments to L2 cold storage (if configured).
    if let Some(ref cold_settings) = config.cold_storage {
        let shared_cold = Arc::clone(&shared);
        let cold_settings_clone = cold_settings.clone();
        let data_dir_clone = config.data_dir.clone();
        let shutdown_rx_cold = shutdown_rx.clone();
        nodedb::control::cold_tier::spawn_cold_tier_task(
            shared_cold,
            cold_settings_clone,
            data_dir_clone,
            shutdown_rx_cold,
        );
        info!("cold tier task spawned");
    }

    // Create shared connection semaphore — enforced across all listeners.
    let conn_semaphore = Arc::new(tokio::sync::Semaphore::new(config.max_connections));
    info!(
        max_connections = config.max_connections,
        "connection limit configured"
    );

    // Bind all listeners before starting accept loops.
    let listener = nodedb::control::server::listener::Listener::bind(config.native_addr()).await?;
    let pg_listener =
        nodedb::control::server::pgwire::listener::PgListener::bind(config.pgwire_addr()).await?;
    let ilp_listener = if let Some(ilp_addr) = config.ilp_addr() {
        Some(nodedb::control::server::ilp_listener::IlpListener::bind(ilp_addr).await?)
    } else {
        None
    };
    let resp_listener = if let Some(resp_addr) = config.resp_addr() {
        Some(nodedb::control::server::resp::RespListener::bind(resp_addr).await?)
    } else {
        None
    };

    // Startup banner.
    eprintln!();
    eprintln!("  NodeDB v{}", env!("CARGO_PKG_VERSION"));
    eprintln!("  ─────────────────────────────────────");
    eprintln!("  Host            : {}", config.host);
    eprintln!("  Native protocol : {}", config.native_addr());
    eprintln!("  PostgreSQL wire : {}", config.pgwire_addr());
    eprintln!("  HTTP API        : {}", config.http_addr());
    if let Some(addr) = config.resp_addr() {
        eprintln!("  RESP (KV)       : {addr}");
    }
    if let Some(addr) = config.ilp_addr() {
        eprintln!("  ILP ingest      : {addr}");
    }
    eprintln!("  Data Plane cores: {}", config.data_plane_cores);
    eprintln!("  Data directory  : {}", config.data_dir.display());
    eprintln!("  Auth mode       : {:?}", config.auth.mode);
    eprintln!();
    eprintln!("  Press Ctrl+C to stop.");
    eprintln!();

    // Handle Ctrl+C and SIGTERM with phased shutdown via ShutdownBus.
    //
    // The first SIGTERM or Ctrl+C initiates the shutdown bus, which:
    //   1. Signals the flat ShutdownWatch (all watch::Receiver<bool> loops wake)
    //   2. Advances through shutdown phases with 500ms per-phase budgets
    //   3. Awaits loop_registry for any loops that don't participate in phased drain
    //
    // Second Ctrl+C or SIGTERM (only after the first has been fully received and
    // initiate() called) force-exits immediately. We use a oneshot to ensure the
    // force-stop handler only arms itself after the graceful handler has received
    // the first signal — this eliminates the race where both handlers receive the
    // same SIGTERM delivery, the force-stop handler fires first, and exits with
    // code 1 before the graceful path runs.
    let (force_stop_tx, force_stop_rx) = tokio::sync::oneshot::channel::<()>();
    let max_conns = config.max_connections;
    let sem_clone = Arc::clone(&conn_semaphore);
    let shared_signal = Arc::clone(&shared);
    let bus_for_signal = shutdown_bus.clone();
    tokio::spawn(async move {
        // Wait for first Ctrl+C or SIGTERM — whichever arrives first.
        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};
            let mut sigterm =
                signal(SignalKind::terminate()).expect("failed to install SIGTERM handler");
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {},
                _ = sigterm.recv() => {},
            }
        }
        #[cfg(not(unix))]
        {
            tokio::signal::ctrl_c().await.ok();
        }

        let active = max_conns - sem_clone.available_permits();
        if active > 0 {
            eprintln!();
            eprintln!(
                "  {} active connection(s). Draining (30s timeout)...",
                active
            );
            eprintln!("  Press Ctrl+C again to force stop.");
        } else {
            eprintln!("\n  Shutting down...");
        }
        // Persist shape registry before shutdown.
        let shapes = shared_signal.shape_registry.export_all();
        if !shapes.is_empty() {
            tracing::info!(shapes = shapes.len(), "persisting shape subscriptions");
        }

        // Release every descriptor lease held by this node via a
        // single batched raft entry. Best-effort — bounded to 2
        // seconds so a wedged cluster doesn't block shutdown. On
        // failure, leases drain via TTL.
        nodedb::control::lease::shutdown_release::release_all_local_leases(
            Arc::clone(&shared_signal),
            nodedb::control::lease::shutdown_release::DEFAULT_SHUTDOWN_RELEASE_TIMEOUT,
        )
        .await;

        // Initiate phased shutdown. This also signals the flat ShutdownWatch
        // so all existing watch::Receiver<bool> subscribers wake up. The
        // returned JoinHandle resolves when the sequencer has walked every
        // phase (including offender-abort-at-budget logging) — we MUST
        // await it before `process::exit(0)` or the sequencer gets killed
        // mid-phase and offender aborts never fire.
        let sequencer_handle = bus_for_signal.initiate();

        // Arm the force-stop handler now that we have received the first
        // signal and called initiate(). Any *subsequent* signal will be
        // a genuine user request for an immediate stop.
        let _ = force_stop_tx.send(());

        // Also await the flat loop_registry for any loops registered via
        // spawn_loop that are not in the phased bus. Both paths converge:
        // the bus signals the flat watch, which the loop_registry loops
        // observe. shutdown_all awaits their join handles.
        let report = shared_signal
            .loop_registry
            .shutdown_all(shared_signal.tuning.shutdown.deadline())
            .await;
        if report.is_clean() {
            tracing::info!(
                clean = report.exited_clean.len(),
                total = ?report.total,
                "all background loops exited cleanly"
            );
        } else {
            tracing::error!(
                clean = report.exited_clean.len(),
                laggards = ?report.laggards,
                total = ?report.total,
                "background loops exceeded shutdown deadline"
            );
        }

        // Await the phased-bus sequencer so offender-abort-at-budget logs
        // get written before the process dies. Bounded to 2s as a safety
        // net — the per-phase 500ms budget × 7 phases should never exceed
        // ~3.5s, but we cap at 2s because a wedged bus shouldn't block
        // shutdown indefinitely. If it hits the cap, log and exit anyway.
        match tokio::time::timeout(std::time::Duration::from_secs(2), sequencer_handle).await {
            Ok(Ok(())) => {}
            Ok(Err(join_err)) => {
                tracing::error!(error = %join_err, "shutdown sequencer task panicked");
            }
            Err(_) => {
                tracing::error!("shutdown sequencer exceeded 2s cap — forcing exit");
            }
        }

        std::process::exit(0);
    });

    // Force-exit on a SECOND Ctrl+C or SIGTERM (only after the first has been
    // received and initiate() called). The oneshot `force_stop_rx` is sent by
    // the graceful handler above after it calls `bus.initiate()`, so this task
    // never races with the first signal delivery.
    tokio::spawn(async move {
        // Wait until the graceful handler has armed us (i.e., received the
        // first signal). This prevents the race where both tasks receive the
        // same OS signal delivery and this task calls process::exit(1) before
        // the graceful path can complete.
        let _ = force_stop_rx.await;

        // Now listen for a second signal (genuine user override during drain).
        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};
            let mut sigterm =
                signal(SignalKind::terminate()).expect("failed to install second SIGTERM handler");
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {},
                _ = sigterm.recv() => {},
            }
        }
        #[cfg(not(unix))]
        {
            tokio::signal::ctrl_c().await.ok();
        }
        eprintln!("  Force stop.");
        std::process::exit(1);
    });

    // Build shared TLS acceptor if configured. Per-protocol flags control
    // which listeners actually use it — `tls_for(flag)` returns None when
    // the flag is false, disabling TLS on that protocol.
    let base_acceptor: Option<tokio_rustls::TlsAcceptor> = match &config.tls {
        Some(tls) => {
            let check_interval = Duration::from_secs(tls.cert_reload_interval_secs.unwrap_or(3600));
            let (_tls_rx, _tls_tx) = nodedb::control::server::tls_reload::start_tls_reloader(
                tls,
                check_interval,
                Arc::clone(&shared),
            )?;
            let acceptor: tokio_rustls::TlsAcceptor = build_tls_acceptor(tls)?;
            info!(
                reload_interval_secs = check_interval.as_secs(),
                "TLS enabled with hot rotation"
            );
            Some(acceptor)
        }
        None => None,
    };

    // Per-protocol TLS: returns the acceptor only if the protocol flag is true.
    let tls_for = |enabled: bool| -> Option<tokio_rustls::TlsAcceptor> {
        if enabled { base_acceptor.clone() } else { None }
    };
    let tls_flags = config.tls.as_ref();
    let pgwire_tls_enabled = tls_flags.is_some_and(|t| t.pgwire);
    let http_tls_enabled = tls_flags.is_some_and(|t| t.http);
    let resp_tls_enabled = tls_flags.is_some_and(|t| t.resp);
    let ilp_tls_enabled = tls_flags.is_some_and(|t| t.ilp);
    let native_tls_enabled = tls_flags.is_some_and(|t| t.native);

    // Boot-time readiness gate: in cluster mode, wait until the
    // metadata raft group has applied its first entry on this node
    // before opening any client-facing listener. This eliminates the
    // restart-window race where the first DDL would observe
    // `metadata propose: not leader` because election had not yet
    // completed.
    if let Some(mut ready_rx) = raft_ready_rx {
        const RAFT_READY_TIMEOUT: Duration = Duration::from_secs(30);
        match tokio::time::timeout(RAFT_READY_TIMEOUT, ready_rx.wait_for(|v| *v)).await {
            Ok(Ok(_)) => {
                info!("metadata raft group ready — opening client listeners");
            }
            Ok(Err(_)) => {
                raft_gate.fail("raft readiness watch dropped before signalling ready");
                return Err(anyhow::anyhow!(
                    "raft readiness watch dropped before signalling ready"
                ));
            }
            Err(_) => {
                raft_gate.fail(format!(
                    "raft readiness timeout after {RAFT_READY_TIMEOUT:?}"
                ));
                return Err(anyhow::anyhow!(
                    "raft readiness timeout after {RAFT_READY_TIMEOUT:?} — \
                     metadata group failed to apply first entry"
                ));
            }
        }
    }
    // Metadata raft group has applied its first entry (or we're
    // in single-node mode with no raft). The post-apply hooks
    // have rebuilt in-memory registries from redb.
    raft_gate.fire();
    schema_gate.fire();

    // Catalog sanity check: applied-index gate, redb
    // cross-table integrity, and in-memory registry ⇔ redb
    // verification. Any unrepairable divergence or any redb
    // integrity violation aborts startup.
    let verify_report = nodedb::control::cluster::verify_and_repair(&shared).await?;
    if verify_report.is_acceptable() {
        info!(report = %verify_report, "catalog sanity check passed");
    } else {
        sanity_gate.fail(format!("catalog sanity check failed: {verify_report}"));
        return Err(anyhow::anyhow!(
            "catalog sanity check failed: {verify_report}"
        ));
    }
    sanity_gate.fire();
    data_groups_gate.fire();
    transport_gate.fire();

    // Warm the QUIC peer cache so the first replicated request
    // after boot doesn't pay a cold dial.
    if let (Some(transport), Some(topology)) = (
        shared.cluster_transport.as_ref(),
        shared.cluster_topology.as_ref(),
    ) {
        // Clone the topology snapshot so the read guard is dropped
        // before awaiting — clippy::await_holding_lock.
        let topo_snapshot = {
            let guard = topology.read().unwrap_or_else(|p| p.into_inner());
            guard.clone()
        };
        let warm_report = nodedb::control::cluster::warm_known_peers(
            transport,
            &topo_snapshot,
            shared.node_id,
            Duration::from_secs(2),
        )
        .await;
        if warm_report.attempted > 0 {
            info!(report = %warm_report, "peer cache warm-up complete");
            if !warm_report.is_complete() {
                for (id, err) in &warm_report.failed {
                    tracing::warn!(node_id = id, error = %err, "peer warm failed");
                }
            }
        }
    }
    warm_peers_gate.fire();
    health_loop_gate.fire();
    gateway_enable_gate.fire();

    // Run pgwire listener in a separate task.
    let shared_pg = Arc::clone(&shared);
    let conn_sem_pg = Arc::clone(&conn_semaphore);
    let pgwire_tls = tls_for(pgwire_tls_enabled);
    let startup_gate_pg = Arc::clone(&startup_gate);
    let bus_pg = shutdown_bus.clone();
    tokio::spawn(async move {
        if let Err(e) = pg_listener
            .run(
                shared_pg,
                auth_mode,
                pgwire_tls,
                conn_sem_pg,
                startup_gate_pg,
                bus_pg,
            )
            .await
        {
            tracing::error!(error = %e, "pgwire listener failed");
        }
    });

    // Run HTTP API server.
    // HTTP is NOT gated at the accept-loop level: /healthz must respond
    // during startup (k8s readiness probe requirement). Instead, a
    // startup-gate middleware on the router rejects non-health routes
    // with 503 until `GatewayEnable` fires.
    let shared_http = Arc::clone(&shared);
    let http_auth_mode = config.auth.mode.clone();
    let http_listen = config.http_addr();
    // HTTP uses TlsSettings directly (axum-server loads certs itself).
    let http_tls = if http_tls_enabled {
        config.tls.clone()
    } else {
        None
    };
    let bus_http = shutdown_bus.clone();
    tokio::spawn(async move {
        if let Err(e) = nodedb::control::server::http::server::run(
            http_listen,
            shared_http,
            http_auth_mode,
            http_tls.as_ref(),
            bus_http,
        )
        .await
        {
            tracing::error!(error = %e, "HTTP API server failed");
        }
    });

    // Run ILP TCP listener for timeseries ingest (if configured).
    if let Some(ilp) = ilp_listener {
        let shared_ilp = Arc::clone(&shared);
        let conn_sem_ilp = Arc::clone(&conn_semaphore);
        let ilp_tls = tls_for(ilp_tls_enabled);
        let startup_gate_ilp = Arc::clone(&startup_gate);
        let bus_ilp = shutdown_bus.clone();
        tokio::spawn(async move {
            if let Err(e) = ilp
                .run(shared_ilp, conn_sem_ilp, ilp_tls, startup_gate_ilp, bus_ilp)
                .await
            {
                tracing::error!(error = %e, "ILP listener failed");
            }
        });
    }

    // Run RESP (Redis-compatible) listener for KV access (if configured).
    if let Some(resp) = resp_listener {
        let shared_resp = Arc::clone(&shared);
        let conn_sem_resp = Arc::clone(&conn_semaphore);
        let resp_tls = tls_for(resp_tls_enabled);
        let startup_gate_resp = Arc::clone(&startup_gate);
        let bus_resp = shutdown_bus.clone();
        tokio::spawn(async move {
            if let Err(e) = resp
                .run(
                    shared_resp,
                    conn_sem_resp,
                    resp_tls,
                    startup_gate_resp,
                    bus_resp,
                )
                .await
            {
                tracing::error!(error = %e, "RESP listener failed");
            }
        });
    }

    // Start sync WebSocket listener for NodeDB-Lite clients.
    let sync_config = nodedb::control::server::sync::listener::SyncListenerConfig::default();
    match nodedb::control::server::sync::listener::start_sync_listener(
        sync_config,
        Some(Arc::clone(&shared)),
    )
    .await
    {
        Ok(sync_state) => {
            info!(
                addr = %sync_state.config.listen_addr,
                max_sessions = sync_state.config.max_sessions,
                "sync WebSocket listener started"
            );
        }
        Err(e) => {
            tracing::warn!(error = %e, "sync listener failed to start (non-fatal)");
        }
    }

    // Native protocol TLS.
    let native_tls = tls_for(native_tls_enabled);

    // Signal readiness to systemd (Type=notify) and, in cluster
    // mode, transition the lifecycle tracker to `Ready`. By this
    // point every listener task has been spawned (pgwire, HTTP,
    // ILP, RESP, sync WebSocket) and the Raft loop is running. The
    // native listener is about to start accepting connections
    // below — when systemctl returns from `start`, this is the
    // state it observes.
    if let Some(ref handle) = cluster_handle {
        let nodes = handle.topology.read().map(|t| t.node_count()).unwrap_or(1);
        handle.lifecycle.to_ready(nodes);
    }
    nodedb_cluster::readiness::notify_ready();

    // Run native listener on main task.
    let native_auth_mode = config.auth.mode.clone();
    listener
        .run(
            shared,
            native_auth_mode,
            native_tls,
            conn_semaphore,
            Arc::clone(&startup_gate),
            shutdown_bus.clone(),
        )
        .await?;

    info!("server shutting down");
    nodedb_cluster::readiness::notify_stopping();

    // The native listener returned because the phased shutdown bus signaled
    // DrainingListeners. The signal handler task is concurrently awaiting
    // the bus sequencer to walk every phase (including offender-abort at
    // budget). If we `exit(0)` here, the signal handler gets killed
    // mid-sequence and offender-abort logs never get emitted.
    //
    // Wait for the bus to reach `Closed` before exiting. The signal handler
    // also calls `exit(0)` after its sequencer await — whichever reaches
    // it first wins the race, and both paths guarantee the sequencer has
    // completed first.
    shutdown_bus
        .handle()
        .await_phase(nodedb::control::shutdown::ShutdownPhase::Closed)
        .await;

    // Data Plane cores run on std::thread (not Tokio) and block in an
    // infinite eventfd poll loop. They have no shutdown signal — they
    // rely on process exit. Explicitly exit so they don't keep the
    // process alive after the Control Plane has drained.
    std::process::exit(0);
}
