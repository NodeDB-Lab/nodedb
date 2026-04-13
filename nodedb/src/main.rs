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
            .json()
            .init();
    } else {
        tracing_subscriber::fmt().with_env_filter(filter).init();
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

    // Replay WAL records for crash recovery (shared across all cores).
    let wal_records: Arc<[nodedb_wal::WalRecord]> = match wal.replay() {
        Ok(records) => {
            if !records.is_empty() {
                info!(records = records.len(), "WAL records loaded for replay");
            }
            Arc::from(records.into_boxed_slice())
        }
        Err(e) => {
            tracing::warn!(error = %e, "WAL replay failed, starting with empty state");
            Arc::from(Vec::new().into_boxed_slice())
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

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

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

    // Start response poller: routes Data Plane responses to waiting sessions.
    // Uses yield_now() instead of sleep() because Tokio's timer wheel has 1ms
    // minimum granularity — sleep(100us) actually sleeps ~1ms, adding 1ms to
    // every request's latency. yield_now() yields to the scheduler without a
    // timer, polling on every scheduler cycle (microsecond-level).
    let shared_poller = Arc::clone(&shared);
    tokio::spawn(async move {
        loop {
            shared_poller.poll_and_route_responses();
            tokio::task::yield_now().await;
        }
    });

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
    );
    info!(num_cores, "event plane running");

    // Tenant rate counter reset (1-second timer).
    let shared_rate = Arc::clone(&shared);
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            shared_rate.reset_tenant_rate_counters();
        }
    });

    // Audit log flush (10-second timer).
    let shared_audit = Arc::clone(&shared);
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(10)).await;
            shared_audit.flush_audit_log();
        }
    });

    // Tenant memory estimation (30-second timer).
    let shared_mem = Arc::clone(&shared);
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(30)).await;
            shared_mem.update_tenant_memory_estimates();
        }
    });

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

    // Handle Ctrl+C with two-stage shutdown.
    let max_conns = config.max_connections;
    let sem_clone = Arc::clone(&conn_semaphore);
    let shared_signal = Arc::clone(&shared);
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();

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

        let _ = shutdown_tx.send(true);

        // Second Ctrl+C: force exit immediately.
        tokio::signal::ctrl_c().await.ok();
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
                return Err(anyhow::anyhow!(
                    "raft readiness watch dropped before signalling ready"
                ));
            }
            Err(_) => {
                return Err(anyhow::anyhow!(
                    "raft readiness timeout after {RAFT_READY_TIMEOUT:?} — \
                     metadata group failed to apply first entry"
                ));
            }
        }
    }

    // Run pgwire listener in a separate task.
    let shared_pg = Arc::clone(&shared);
    let shutdown_rx_pg = shutdown_rx.clone();
    let conn_sem_pg = Arc::clone(&conn_semaphore);
    let pgwire_tls = tls_for(pgwire_tls_enabled);
    tokio::spawn(async move {
        if let Err(e) = pg_listener
            .run(
                shared_pg,
                auth_mode,
                pgwire_tls,
                conn_sem_pg,
                shutdown_rx_pg,
            )
            .await
        {
            tracing::error!(error = %e, "pgwire listener failed");
        }
    });

    // Run HTTP API server.
    let shared_http = Arc::clone(&shared);
    let http_auth_mode = config.auth.mode.clone();
    let http_listen = config.http_addr();
    // HTTP uses TlsSettings directly (axum-server loads certs itself).
    let http_tls = if http_tls_enabled {
        config.tls.clone()
    } else {
        None
    };
    let shutdown_rx_http = shutdown_rx.clone();
    tokio::spawn(async move {
        if let Err(e) = nodedb::control::server::http::server::run(
            http_listen,
            shared_http,
            http_auth_mode,
            http_tls.as_ref(),
            shutdown_rx_http,
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
        let shutdown_rx_ilp = shutdown_rx.clone();
        tokio::spawn(async move {
            if let Err(e) = ilp
                .run(shared_ilp, conn_sem_ilp, ilp_tls, shutdown_rx_ilp)
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
        let shutdown_rx_resp = shutdown_rx.clone();
        tokio::spawn(async move {
            if let Err(e) = resp
                .run(shared_resp, conn_sem_resp, resp_tls, shutdown_rx_resp)
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
            shutdown_rx,
        )
        .await?;

    info!("server shutting down");
    nodedb_cluster::readiness::notify_stopping();

    // Data Plane cores run on std::thread (not Tokio) and block in an
    // infinite eventfd poll loop. They have no shutdown signal — they
    // rely on process exit. Explicitly exit so they don't keep the
    // process alive after the Control Plane has drained.
    std::process::exit(0);
}
