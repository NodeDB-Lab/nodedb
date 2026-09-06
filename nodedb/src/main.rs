// SPDX-License-Identifier: BUSL-1.1

#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

mod main_boot;

use std::path::PathBuf;
use std::sync::Arc;

use nodedb::ServerConfig;
use nodedb::config::server::apply_env_overrides;
use nodedb::control::startup::StartupSequencer;
use tracing::info;

use main_boot::{
    background, data_plane, gates, listeners, post_open, shared_state, shutdown_wiring, startup_log,
};

fn main() -> anyhow::Result<()> {
    // Nothing may precede this. The black-box recorder's crash monitor is a
    // re-exec of this same binary; when this process is the monitor it serves
    // minidumps here and exits, and any startup work done first would be the
    // server starting a second time. In a normal process it is one
    // environment-variable check.
    if nodedb::bootstrap::diagnostics::run_crash_monitor_if_env() {
        return Ok(());
    }

    // This is deliberately the first application action: Tokio may create
    // worker threads while building its runtime, and the default panic hook
    // renders arbitrary panic payloads.
    nodedb::bootstrap::panic_hook::install();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    if let Err(error) = runtime.block_on(server_main()) {
        // Mirror the success path's std::process::exit(0) at main.rs's end:
        // returning Err from main() is not enough. Data Plane cores block in
        // an infinite eventfd poll loop with no shutdown signal, so a boot
        // failure that only unwinds server_main() leaves them running with
        // every listener port closed — the process looks alive but serves
        // nothing.
        //
        // A boot error can happen before tracing is initialised (e.g. a
        // malformed config file, read before init_tracing() runs), so
        // tracing::error! alone would silently drop it. exit(1) also skips
        // the anyhow Debug print the runtime would have made on a returned
        // Err. eprintln! with "{error:?}" is the only channel guaranteed to
        // reach the operator in both cases — it prints the full anyhow
        // context chain, not just the top-level message — so it runs
        // unconditionally; tracing::error! runs alongside it for
        // subscribers already wired up when the failure happened later in
        // boot.
        eprintln!("StartupError: server failed to start — aborting startup\n{error:?}");
        tracing::error!(error = %error, "StartupError: server failed to start — aborting startup");
        std::process::exit(1);
    }
    Ok(())
}

async fn server_main() -> anyhow::Result<()> {
    // Operator subcommand dispatch (L.4): handled before config load
    // + tracing init so `nodedb regen-certs`, `nodedb rotate-ca`,
    // `nodedb join-token` exit cleanly without spinning up the
    // server's global allocator arenas or file locks. A first arg
    // that doesn't match a known subcommand is treated as a config
    // file path and falls through to the normal server bootstrap.
    let cli_args: Vec<String> = std::env::args().skip(1).collect();
    match nodedb::ctl::parse_subcommand(&cli_args) {
        Ok(Some(cmd)) => std::process::exit(nodedb::ctl::run_subcommand(cmd)),
        Ok(None) => {}
        Err(e) => {
            eprintln!("error: {e}");
            std::process::exit(2);
        }
    }

    // Resolve config file path.
    // Priority: CLI arg (highest) > NODEDB_CONFIG env var > default.
    let config_path: Option<PathBuf> = cli_args
        .iter()
        .find(|a| !a.starts_with("--"))
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
    // correct in case NODEDB_DATA_DIR / NODEDB_MEMORY_LIMIT also affect it,
    // and so a malformed override aborts the boot before any subsystem
    // starts. The info! messages this call emits reach no subscriber yet;
    // the second call below re-applies them once tracing is registered so
    // the operator still sees them logged.
    apply_env_overrides(&mut config)?;

    // Own the black-box recorder before the subscriber is built: the panic hook
    // it installs chains in front of the one above, and the reports directory
    // has to be resolved from the data dir the overrides just settled. Only
    // this binary calls `init`, and only once — the WAL and every other library
    // layer emit into whatever it configures here.
    nodedb::bootstrap::diagnostics::init(&config);

    // Initialize tracing subscriber (format + filter from config / RUST_LOG).
    nodedb::bootstrap::tracing_init::init_tracing(&config);

    // Root span: entered for the lifetime of the process. Provides structured
    // context fields (service name, version, host, pid, node_id) on every log
    // event. node_id starts at 0 for single-node; cluster wiring records the
    // real value below once the cluster handle is resolved.
    let root_span = tracing::info_span!(
        "service",
        service.name = "nodedb",
        service.version = nodedb::version::VERSION,
        host = %nodedb::version::hostname(),
        pid = std::process::id(),
        node_id = 0u64,
    );
    // Use enter() (borrows) rather than entered() (consumes) so that root_span
    // remains accessible for the late record() call after cluster wiring.
    let _root_guard = root_span.enter();

    // Re-apply env overrides after tracing initializes so that
    // info!/warn! messages are actually emitted for operators.
    apply_env_overrides(&mut config)?;

    let cluster_mode_str = startup_log::log_boot_banner(&config_path, &config);

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
    let gates::StartupGates {
        wal_gate,
        catalog_gate,
        raft_gate,
        schema_gate,
        sanity_gate,
        data_groups_gate,
        transport_gate,
        warm_peers_gate,
        health_loop_gate,
        gateway_enable_gate,
    } = gates::register_startup_gates(&startup_seq);

    let data_plane::DataPlaneBootstrap {
        dispatcher,
        wal,
        wal_records,
        replay_tombstones,
        num_cores,
        event_consumers,
        system_metrics,
        quiesce,
        array_catalog,
        quarantine_registry,
        maintenance_budget,
        governor,
        watermark_store,
        trigger_dlq,
        cluster_handle,
        _core_handles,
        replay_done,
    } = data_plane::bootstrap_data_plane(&config, &wal_gate).await?;

    let shared = shared_state::open_and_wire_state(
        &config,
        shared_state::SharedStateInputs {
            dispatcher,
            wal: Arc::clone(&wal),
            quiesce,
            array_catalog,
            quarantine_registry,
            governor,
            system_metrics: Arc::clone(&system_metrics),
            maintenance_budget,
            cluster_handle: cluster_handle.as_deref(),
            startup_gate: &startup_gate,
            root_span: &root_span,
        },
    )
    .await?;

    post_open::run(
        &shared,
        &wal_records,
        &replay_tombstones,
        &config,
        &catalog_gate,
    )
    .await?;

    let (shutdown_rx, shutdown_bus, _loop_registry_shutdown) =
        shutdown_wiring::wire_shutdown_bus(&shared, &system_metrics);

    let background::BackgroundLoops {
        raft_ready_rx,
        _lease_renewal,
        _event_plane_shutdown,
    } = background::spawn(
        &shared,
        &config,
        shutdown_rx.clone(),
        shutdown_bus.clone(),
        background::BackgroundLoopsInputs {
            cluster_handle: cluster_handle.as_deref(),
            wal: Arc::clone(&wal),
            event_consumers,
            watermark_store,
            trigger_dlq,
            num_cores,
        },
    )?;

    let listeners::ListenerSetup {
        conn_semaphore,
        admission_registry,
        listener,
        pg_listener,
        http_listener,
        sync_listener,
        ilp_listener,
        resp_listener,
        base_acceptor,
        native_tls_enabled,
    } = listeners::setup(
        &shared,
        &config,
        cluster_mode_str,
        &shutdown_bus,
        cluster_handle.clone(),
    )
    .await?;

    // Per-protocol TLS: returns the acceptor only if the protocol flag is true.
    let tls_for = |enabled: bool| -> Option<tokio_rustls::TlsAcceptor> {
        if enabled { base_acceptor.clone() } else { None }
    };

    // Wait for raft readiness, run catalog sanity check, warm peer cache, fire gates.
    nodedb::bootstrap::cluster_ready::await_cluster_ready(
        &shared,
        raft_ready_rx,
        replay_done,
        nodedb::bootstrap::cluster_ready::ClusterReadyGates {
            raft_gate,
            schema_gate,
            sanity_gate,
            data_groups_gate,
            transport_gate,
            warm_peers_gate,
            health_loop_gate,
            gateway_enable_gate,
        },
    )
    .await?;

    // Persisted quotas become live here: the catalog has caught up, and no
    // listener has accepted a connection that could bypass a cap.
    nodedb::bootstrap::quota_replay::replay_quotas(&shared);

    // Spawn all non-native protocol listeners.
    nodedb::bootstrap::listeners::spawn_protocol_listeners(
        nodedb::bootstrap::listeners::ProtocolListeners {
            pg_listener,
            http_listener,
            sync_listener,
            ilp_listener,
            resp_listener,
        },
        Arc::clone(&shared),
        &config,
        nodedb::bootstrap::listeners::ListenerInfra {
            conn_semaphore: Arc::clone(&conn_semaphore),
            startup_gate: Arc::clone(&startup_gate),
            shutdown_bus: shutdown_bus.clone(),
        },
        base_acceptor.clone(),
        &cluster_handle,
    )
    .await;

    // Native protocol TLS.
    let native_tls = tls_for(native_tls_enabled);

    // Run native listener on main task.
    let native_auth_mode = config.auth.mode.clone();
    listener
        .run(nodedb::control::server::listener::ListenerRunParams {
            state: shared,
            auth_mode: native_auth_mode,
            tls_acceptor: native_tls,
            conn_semaphore,
            startup_gate: Arc::clone(&startup_gate),
            bus: shutdown_bus.clone(),
            admission: admission_registry,
        })
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
