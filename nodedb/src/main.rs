#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tracing::info;
use tracing_subscriber::EnvFilter;

use nodedb::ServerConfig;
use nodedb::bridge::dispatch::Dispatcher;
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
    // Initialize tracing.
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    // Load config.
    let config = match std::env::args().nth(1) {
        Some(path) => ServerConfig::from_file(&PathBuf::from(path))?,
        None => {
            info!("no config file provided, using defaults");
            ServerConfig::default()
        }
    };

    info!(
        listen = %config.listen,
        cores = config.data_plane_cores,
        memory_limit = config.memory_limit,
        "starting nodedb"
    );

    // Validate engine config.
    config.engines.validate()?;

    // Initialize memory governor.
    let byte_budgets = config.engines.to_byte_budgets(config.memory_limit);
    let _governor = nodedb::memory::init_governor(config.memory_limit, &byte_budgets)?;

    // Open WAL (with optional encryption at rest).
    let wal = if let Some(ref enc) = config.encryption {
        Arc::new(WalManager::open_encrypted(
            &config.wal_dir(),
            false,
            &enc.key_path,
        )?)
    } else {
        Arc::new(WalManager::open(&config.wal_dir(), false)?)
    };
    info!(next_lsn = %wal.next_lsn(), "WAL ready");

    // Create SPSC bridge: Dispatcher (Control Plane) + CoreChannelDataSide (Data Plane).
    let num_cores = config.data_plane_cores;
    let (mut dispatcher, data_sides) = Dispatcher::new(num_cores, 1024);

    // Start Data Plane cores on dedicated OS threads (thread-per-core).
    // Each core gets: jemalloc arena pinning + eventfd-driven wake.
    let mut core_handles = Vec::with_capacity(num_cores);
    let mut notifiers = Vec::with_capacity(num_cores);
    for (core_id, data_side) in data_sides.into_iter().enumerate() {
        let (handle, notifier) = spawn_core(
            core_id,
            data_side.request_rx,
            data_side.response_tx,
            &config.data_dir,
        )?;
        core_handles.push(handle);
        notifiers.push((core_id, notifier));
    }

    // Wire notifiers into the dispatcher so it signals cores after pushing requests.
    for (core_id, notifier) in &notifiers {
        dispatcher.set_notifier(*core_id, *notifier);
    }

    info!(num_cores, "data plane cores running (eventfd-driven)");

    // Create shared state with persistent system catalog.
    let shared = SharedState::open(
        dispatcher,
        Arc::clone(&wal),
        &config.catalog_path(),
        &config.auth,
    )?;

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

    // Start response poller: routes Data Plane responses to waiting sessions.
    let shared_poller = Arc::clone(&shared);
    tokio::spawn(async move {
        loop {
            shared_poller.poll_and_route_responses();
            tokio::time::sleep(Duration::from_micros(100)).await;
        }
    });

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

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    // Bind both listeners before starting accept loops.
    let listener = nodedb::control::server::listener::Listener::bind(config.listen).await?;
    let pg_listener =
        nodedb::control::server::pgwire::listener::PgListener::bind(config.pg_listen).await?;

    // Handle Ctrl+C.
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        info!("shutdown signal received");
        let _ = shutdown_tx.send(true);
    });

    // Build TLS acceptor if configured.
    let tls_acceptor = match &config.tls {
        Some(tls) => {
            let acceptor = build_tls_acceptor(tls)?;
            info!("pgwire TLS enabled");
            Some(acceptor)
        }
        None => None,
    };

    // Run pgwire listener in a separate task.
    let shared_pg = Arc::clone(&shared);
    let shutdown_rx_pg = shutdown_rx.clone();
    tokio::spawn(async move {
        if let Err(e) = pg_listener
            .run(shared_pg, auth_mode, tls_acceptor, shutdown_rx_pg)
            .await
        {
            tracing::error!(error = %e, "pgwire listener failed");
        }
    });

    // Run HTTP API server.
    let shared_http = Arc::clone(&shared);
    let http_auth_mode = config.auth.mode.clone();
    let http_listen = config.http_listen;
    let http_tls = config.tls.clone();
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

    // Build native TLS acceptor if configured (reuses same cert/key as pgwire).
    let native_tls: Option<tokio_rustls::TlsAcceptor> = match &config.tls {
        Some(tls) => {
            // pgwire::tokio::TlsAcceptor is tokio_rustls::TlsAcceptor — build one directly.
            let acceptor = build_tls_acceptor(tls)?;
            // Convert pgwire TlsAcceptor back to tokio_rustls TlsAcceptor.
            // They're the same type underneath.
            Some(acceptor)
        }
        None => None,
    };

    // Run native listener on main task.
    let native_auth_mode = config.auth.mode.clone();
    listener
        .run(shared, native_auth_mode, native_tls, shutdown_rx)
        .await?;

    Ok(())
}
