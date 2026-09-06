// SPDX-License-Identifier: BUSL-1.1

//! The lowest-level cluster-node spawn body: pre-binds QUIC transport,
//! opens WAL + credentials, wires cluster handles into `SharedState`,
//! starts every Data-Plane core, the Event Plane, Raft, the descriptor
//! lease loop, the gateway, and the pgwire/native listeners, then
//! connects a `tokio_postgres::Client`.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use nodedb::bridge::dispatch::Dispatcher;
use nodedb::config::auth::AuthMode;
use nodedb::config::server::ClusterSettings;
use nodedb::control::server::pgwire::listener::PgListener;
use nodedb::control::state::SharedState;
use nodedb::event::{EventPlane, EventPlaneConfig, create_event_bus};
use nodedb::wal::WalManager;

use crate::cluster_harness::cluster::ClusterSpawnConfig;

use super::client_slot::ClusterTestClient;
use super::types::{DataDir, HARNESS_SUPERUSER, TestClusterNode};

impl TestClusterNode {
    /// [`Self::spawn_with_full_config`] entry point used by every spawn
    /// variant that does not need to reopen an existing data directory: mints
    /// a fresh `tempdir()` this node owns and deletes on drop.
    pub(crate) async fn spawn_with_full_config(
        node_id: u64,
        seed_nodes: Vec<SocketAddr>,
        config: &ClusterSpawnConfig,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Self::spawn_with_full_config_at(node_id, seed_nodes, config, None).await
    }

    /// Lowest-level cluster-node spawn. In addition to the tuning knobs of
    /// [`Self::spawn_with_tuning_graph_query_and_cores`], this accepts the
    /// Raft `log_compaction_threshold`: when `Some(n)`, every Raft group on
    /// this node auto-compacts its log once it has more than `n` applied
    /// entries past the snapshot index. A low value forces the leader's
    /// data-group log to compact past the start after a handful of writes,
    /// which is what makes a freshly-joined learner unreachable via
    /// `AppendEntries` and forces a real `InstallSnapshot`.
    ///
    /// `replication_factor` controls how many nodes HRW placement assigns to
    /// each Raft group (`take = min(replication_factor, node_count)`). Tests
    /// that need EVERY node to host EVERY group deterministically (e.g. the
    /// InstallSnapshot end-to-end test, which asserts on a learner's LOCAL
    /// hosting state rather than a forwardable pgwire query) must pass the
    /// post-join node count here — otherwise placement may never assign the
    /// joining node to the collection's data group at all.
    ///
    /// Every other spawn entry point delegates here with `None` /
    /// `replication_factor = 3`.
    ///
    /// `data_dir_path_override`: `None` mints a fresh `tempdir()` this node
    /// owns and deletes on drop (the historical behaviour, via
    /// [`Self::spawn_with_full_config`]). `Some(path)` roots the node at a
    /// CALLER-supplied directory instead — used by `..._on_path` spawn
    /// variants that reopen a previous node's data directory (a WAL-only
    /// restart). In both cases the WAL at `<dir>/test.wal` is replayed and
    /// threaded into every Data-Plane core via `CoreLoopSpawn::replay`: on a
    /// fresh empty WAL this replays zero records (no behaviour change from
    /// before this parameter existed); on a reopened directory it rebuilds
    /// in-memory-only structures (e.g. the vector HNSW index) from the
    /// persisted `TransactionRedo` / `Put` / etc. records.
    pub(crate) async fn spawn_with_full_config_at(
        node_id: u64,
        seed_nodes: Vec<SocketAddr>,
        config: &ClusterSpawnConfig,
        data_dir_path_override: Option<PathBuf>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        // Every cluster node funnels through here, so installing tracing at
        // this one point means no test has to opt in to see server-side logs.
        crate::test_tracing::init();

        let tuning = &config.tuning;
        let graph_tuning = &config.graph_tuning;
        let query_tuning = &config.query_tuning;
        let num_cores = config.num_cores;
        let log_compaction_threshold = config.log_compaction_threshold;
        let replication_factor = config.replication_factor;

        let (data_dir_handle, data_dir_path): (DataDir, PathBuf) = match data_dir_path_override {
            Some(p) => (DataDir::Borrowed, p),
            None => {
                let dir = tempfile::tempdir()?;
                let path = dir.path().to_path_buf();
                (DataDir::Owned(dir), path)
            }
        };

        // Open WAL + dispatcher + event bus. Replay whatever is already on
        // disk (empty for a fresh directory) so every core can rebuild its
        // in-memory-only structures before it starts ticking.
        let wal = Arc::new(WalManager::open_for_testing(
            &data_dir_path.join("test.wal"),
        )?);
        let wal_records: Arc<[nodedb_wal::WalRecord]> = Arc::from(wal.replay()?.into_boxed_slice());
        let replay_tombstones = nodedb_wal::extract_tombstones(&wal_records).unwrap();
        let (dispatcher, data_sides) = Dispatcher::new(num_cores, 1024);
        let (event_producers, event_consumers) = create_event_bus(num_cores);

        // Credential store backed by the system catalog — required for
        // CREATE COLLECTION to exercise the full persistence path.
        let credentials = Arc::new(
            nodedb::control::security::credential::store::CredentialStore::open(
                &data_dir_path.join("system.redb"),
            )?,
        );
        // Mirror production/pgwire-harness bootstrap: both listeners run in
        // `AuthMode::Trust`, which resolves — but never fabricates — a durable
        // stored identity. Without this every node rejects the harness connect
        // with `trust auth: user 'nodedb' does not exist`.
        credentials.bootstrap_trust_superuser(HARNESS_SUPERUSER)?;
        // Static, deployment-time surrogate-registry mode — same predicate as
        // production's `config.cluster.is_some()`. `single_node_calvin` here
        // takes the `init_single_node_calvin` synthesis below (never joinable,
        // mirrors production's default standalone path); every other branch
        // builds a real `ClusterSettings` and joins/bootstraps a genuine Raft
        // group, so it must start in `Cluster` mode from construction — this
        // node was never going to promote into it later.
        let is_cluster = !config.single_node_calvin;
        let mut shared = SharedState::new_with_credentials(
            dispatcher,
            Arc::clone(&wal),
            credentials,
            is_cluster,
        )?;

        // Acquire the cluster handle. The single-node-Calvin path drives the
        // production `init_single_node_calvin` synthesis (which binds its own
        // loopback transport); every multi-node path pre-binds a transport and
        // builds explicit `ClusterSettings`.
        let (handle, listen_addr) = if config.single_node_calvin {
            let handle =
                nodedb::control::cluster::init_single_node_calvin(&data_dir_path, tuning).await?;
            let listen_addr = handle.transport.local_addr();
            (handle, listen_addr)
        } else {
            // Pre-bind the QUIC transport on a random port so we know the
            // listen address before wiring seeds / cluster settings.
            let transport = Arc::new(nodedb_cluster::NexarTransport::new(
                node_id,
                "127.0.0.1:0".parse()?,
                nodedb_cluster::TransportCredentials::Insecure,
            )?);
            let listen_addr = transport.local_addr();

            // Build cluster settings. Empty `seed_nodes` → single-node
            // bootstrap by listing only our own address.
            let seeds = if seed_nodes.is_empty() {
                vec![listen_addr]
            } else {
                seed_nodes
            };
            let cluster_settings = ClusterSettings {
                node_id,
                listen: listen_addr,
                seed_nodes: seeds,
                num_groups: 2,
                replication_factor,
                force_bootstrap: false,
                tls: None,
                max_active_sessions: 0,
                login_attempts_per_ip_per_min: 30,
                login_attempts_per_user_per_min: 10,
                insecure_transport: true,
                log_compaction_threshold,
                join_retry_max_attempts: 8,
                join_retry_max_backoff_secs: 32,
            };

            // Initialise the cluster using the pre-bound transport.
            let handle = nodedb::control::cluster::init_cluster_with_transport(
                &cluster_settings,
                transport.clone(),
                &data_dir_path,
                tuning,
            )
            .await?;
            (handle, listen_addr)
        };

        // Wire cluster handles into SharedState (mirrors main.rs).
        // `Arc::get_mut` is valid here: `shared` has not been cloned.
        if let Some(state) = Arc::get_mut(&mut shared) {
            state.node_id = handle.node_id;
            state.cluster_topology = Some(Arc::clone(&handle.topology));
            state.cluster_routing = Some(Arc::clone(&handle.routing));
            state.cluster_transport = Some(Arc::clone(&handle.transport));
            state.metadata_cache = Arc::clone(&handle.metadata_cache);
            state.group_watchers = Arc::clone(&handle.group_watchers);
            // Wire the cross-shard event SENDER (mirrors production
            // `bootstrap::state_wiring::wire_state`) so an AFTER-trigger body
            // writing to a remote-homed collection is dispatched to the owning
            // node instead of silently mis-written locally. Must be set BEFORE
            // `EventPlane::spawn` below, whose gate spawns the dispatcher drain
            // task only when these fields are `Some`. The receiver builds its
            // own HWM store at Raft group setup, so `hwm_store` stays `None`.
            let cross_shard_metrics =
                Arc::new(nodedb::event::cross_shard::CrossShardMetrics::new());
            state.cross_shard_dispatcher = Some(Arc::new(
                nodedb::event::cross_shard::CrossShardDispatcher::new(
                    handle.node_id,
                    Arc::clone(&cross_shard_metrics),
                ),
            ));
            state.cross_shard_dlq = Some(Arc::new(std::sync::Mutex::new(
                nodedb::event::cross_shard::CrossShardDlq::open(&data_dir_path)?,
            )));
            state.cross_shard_metrics = Some(cross_shard_metrics);
            // Fixed test KEK so backup tests produce encrypted envelopes.
            state.backup_kek = Some(Arc::new([0x42u8; 32]));
            // Durable producer registry, sharing the credential store's
            // already-open catalog (mirrors production `SharedState::open`).
            // Required for sync handshake fencing to replicate via the
            // metadata Raft group on cluster nodes.
            let catalog = state.credentials.catalog().clone();
            match nodedb::control::sync_producer::registry::SyncProducerRegistry::open(Arc::new(
                catalog,
            )) {
                Ok(reg) => state.producer_registry = Some(Arc::new(reg)),
                Err(e) => {
                    return Err(
                        format!("SyncProducerRegistry::open failed in test harness: {e}").into(),
                    );
                }
            }
        } else {
            return Err("SharedState already cloned before cluster wire-up".into());
        }

        // Start one Data-Plane core loop per core. Each core gets its own SPSC
        // data side and event producer; per-core stores live under the shared
        // data dir keyed by `idx` (graph/core-{idx}.redb, etc.).
        let mut core_stop_txs = Vec::with_capacity(num_cores);
        let mut core_handles = Vec::with_capacity(num_cores);
        for (idx, (data_side, event_producer)) in
            data_sides.into_iter().zip(event_producers).enumerate()
        {
            let (core_stop_tx, core_stop_rx) = std::sync::mpsc::channel::<()>();
            let replay = (!wal_records.is_empty()).then(|| crate::core_loop_runner::WalReplay {
                records: Arc::clone(&wal_records),
                tombstones: replay_tombstones.clone(),
                // Cluster node may host multiple cores; replay must route each
                // record to `vshard % num_cores` matching the live node.
                num_cores,
            });
            let core_handle =
                crate::core_loop_runner::spawn_core_loop(crate::core_loop_runner::CoreLoopSpawn {
                    idx,
                    data_side,
                    core_dir: data_dir_path.clone(),
                    core_array_catalog: shared.array_catalog.clone(),
                    event_producer,
                    core_metrics: shared.system_metrics.clone(),
                    governor: shared.governor.clone(),
                    replay,
                    graph_tuning: graph_tuning.clone(),
                    query_tuning: query_tuning.clone(),
                    // Seeded from the SAME durable catalog production reads, so a
                    // harness restart reconstructs cores the way a real one does.
                    // An empty catalog yields an empty seed, which is exactly what
                    // production gets booting a fresh data dir.
                    doc_config_seed: nodedb::bootstrap::data_plane::load_doc_config_registry_from(
                        shared.credentials.catalog(),
                    ),
                    stop_rx: core_stop_rx,
                });
            core_stop_txs.push(core_stop_tx);
            core_handles.push(core_handle);
        }

        // Response poller (Data Plane → control plane routing).
        let shared_poller = Arc::clone(&shared);
        let (poller_shutdown_tx, mut poller_shutdown_rx) = tokio::sync::watch::channel(false);
        let poller_handle = tokio::spawn(async move {
            loop {
                shared_poller.poll_and_route_responses();
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_millis(1)) => {}
                    _ = poller_shutdown_rx.changed() => break,
                }
            }
        });

        // Event Plane (triggers, CDC, scheduler).
        let watermark_store = Arc::new(nodedb::event::watermark::WatermarkStore::open(
            &data_dir_path,
        )?);
        let trigger_dlq = Arc::new(std::sync::Mutex::new(
            nodedb::event::trigger::TriggerDlq::open(&data_dir_path)?,
        ));
        let (pg_shutdown_bus, _) =
            nodedb::control::shutdown::ShutdownBus::new(Arc::clone(&shared.shutdown));
        let event_plane = EventPlane::spawn(EventPlaneConfig {
            consumers_rx: event_consumers,
            wal: Arc::clone(&wal),
            watermark_store,
            shared_state: Arc::clone(&shared),
            trigger_dlq,
            cdc_router: Arc::clone(&shared.cdc_router),
            shutdown: Arc::clone(&shared.shutdown),
            shutdown_bus: pg_shutdown_bus.clone(),
        });

        // Start Raft + install MetadataCommitApplier.
        let (cluster_shutdown_tx, cluster_shutdown_rx) = tokio::sync::watch::channel(false);
        nodedb::control::cluster::start_raft(&handle, Arc::clone(&shared), &data_dir_path, tuning)?;

        // `start_raft` spawns the cluster subsystems (SWIM, reachability,
        // decommission, rebalancer) and stashes the resulting
        // `RunningCluster` on `handle.running_cluster`. Take it out now —
        // `handle` itself is not otherwise retained past this point — so
        // `graceful_shutdown_wal_only` can stop and join those tasks
        // before returning (see `nodedb_cluster::RunningCluster`'s doc:
        // the host must call `shutdown_all` during orderly shutdown).
        let running_cluster = handle
            .running_cluster
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take();

        // CRDT constraint reconcile loop (leader-gated). The production server
        // spawns this from `spawn_background_loops`, which the harness does not
        // call; wire it directly so cluster tests exercise constraint delivery
        // to every replica's validator. Registered on `shared.loop_registry`,
        // so cluster shutdown stops it with the other loops.
        // The interval comes from the same startup gate the server uses, so a
        // test that sets `NODEDB_CONSTRAINT_RECONCILE_INTERVAL_MS` to suppress
        // the loop is honoured here exactly as it is in production. Parsing it
        // a second way here is what would let the two drift apart.
        let mut reconcile_config = nodedb::ServerConfig::default();
        nodedb::config::server::apply_env_overrides(&mut reconcile_config)
            .map_err(|e| format!("environment override rejected: {e}"))?;
        nodedb::bootstrap::constraint_reconcile::spawn_constraint_reconcile(
            Arc::clone(&shared),
            reconcile_config
                .tuning
                .maintenance
                .constraint_reconcile_interval_ms,
        );

        // Spawn the descriptor lease renewal loop on the same
        // shutdown channel as raft so cluster shutdown stops it
        // cleanly. Returns None on single-node clusters that
        // never wired metadata_raft (the harness always wires it,
        // so this returns Some in practice for cluster tests).
        let lease_renewal_handle = nodedb::control::lease::LeaseRenewalLoop::spawn(
            Arc::clone(&shared),
            tuning,
            cluster_shutdown_rx,
        )
        .map(|(join, metrics)| {
            shared.loop_metrics_registry.register(metrics);
            join
        });

        // Construct the gateway and install it (plus its DDL invalidator) on
        // SharedState, mirroring what main.rs does before listeners bind. The
        // fields are `OnceLock`s, set through `&self`, so no `Arc::get_mut`
        // (which `shared` being already cloned would defeat) or raw-pointer
        // write is needed.
        {
            let gateway = Arc::new(nodedb::control::gateway::Gateway::new(Arc::clone(&shared)));
            let invalidator = Arc::new(nodedb::control::gateway::PlanCacheInvalidator::new(
                &gateway.plan_cache,
            ));
            let _ = shared.gateway.set(gateway);
            let _ = shared.gateway_invalidator.set(invalidator);
        }

        // pgwire listener.
        // In the test harness, use the startup gate already on SharedState
        // (a pre-fired placeholder from `new_inner`). This means the listener
        // accepts immediately without a startup-phase delay.
        let pg_listener = PgListener::bind("127.0.0.1:0".parse()?).await?;
        let pg_addr = pg_listener.local_addr();
        let shared_pg = Arc::clone(&shared);
        let test_startup_gate = Arc::clone(&shared.startup);
        let bus_pg = pg_shutdown_bus.clone();
        let pg_handle = tokio::spawn(async move {
            let _ = pg_listener
                .run(
                    shared_pg,
                    AuthMode::Trust,
                    None,
                    Arc::new(tokio::sync::Semaphore::new(128)),
                    test_startup_gate,
                    bus_pg,
                )
                .await;
        });

        // Native (MessagePack) listener — same SharedState, ephemeral port,
        // trust-mode auth. Uses the pre-fired startup gate so it accepts
        // immediately without a startup-phase wait (same as the pgwire listener).
        let native_listener =
            nodedb::control::server::listener::Listener::bind("127.0.0.1:0".parse()?)
                .await
                .map_err(|e| format!("bind native listener: {e}"))?;
        let native_port = native_listener.local_addr().port();
        let shared_native = Arc::clone(&shared);
        let native_startup_gate = Arc::clone(&shared.startup);
        let bus_native = pg_shutdown_bus.clone();
        // The registry `SharedState` carries, not a fresh one: quota DDL
        // installs its caps there. A private registry leaves the listener
        // enforcing no caps at all.
        let native_admission = Arc::clone(&shared.admission_registry);
        let native_handle = tokio::spawn(async move {
            let _ = native_listener
                .run(nodedb::control::server::listener::ListenerRunParams {
                    state: shared_native,
                    auth_mode: AuthMode::Trust,
                    tls_acceptor: None,
                    conn_semaphore: Arc::new(tokio::sync::Semaphore::new(128)),
                    startup_gate: native_startup_gate,
                    bus: bus_native,
                    admission: native_admission,
                })
                .await;
        });

        // Give the listeners a moment to start accepting.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Connect tokio_postgres client.
        let conn_str = format!(
            "host=127.0.0.1 port={} user=nodedb dbname=default",
            pg_addr.port()
        );
        let (client, connection) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
            .await
            .map_err(|e| format!("pgwire connect failed: {e}"))?;
        let conn_handle = tokio::spawn(async move {
            let _ = connection.await;
        });

        Ok(Self {
            node_id,
            listen_addr,
            pg_addr,
            native_port,
            client: ClusterTestClient::new(client),
            shared,
            _data_dir: data_dir_handle,
            _conn_handle: Some(conn_handle),
            pg_shutdown_bus,
            poller_shutdown_tx,
            cluster_shutdown_tx,
            core_stop_txs,
            _pg_handle: Some(pg_handle),
            _native_handle: Some(native_handle),
            _poller_handle: Some(poller_handle),
            _core_handles: core_handles,
            _event_plane: Some(event_plane),
            _lease_renewal_handle: lease_renewal_handle,
            _running_cluster: running_cluster,
        })
    }
}
