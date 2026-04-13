//! Single cluster node: SharedState + cluster + Data Plane + pgwire.
//!
//! A `TestClusterNode` owns one full NodeDB server instance configured
//! for cluster mode. Spawn via [`TestClusterNode::spawn`], passing the
//! node id and the seed list (empty for the bootstrap node; otherwise
//! a list of already-running peer addresses). On return the node has:
//!
//! - Pre-bound QUIC transport (so the listen address is known before
//!   peers need it).
//! - `SharedState` wired with credentials, metadata_cache, and the
//!   cluster handles (topology / routing / transport / applied-index
//!   watcher).
//! - Data Plane core running on a spawn_blocking task.
//! - Response poller running on a tokio task.
//! - Event Plane spawned.
//! - Raft loop + QUIC RPC server started via `start_raft` (installs
//!   the production `MetadataCommitApplier` with `Weak<SharedState>`
//!   so committed `CollectionDdl::Create` entries trigger Data Plane
//!   registers on every node).
//! - pgwire listener bound on an ephemeral port.
//! - `tokio_postgres::Client` connected to that listener.
//!
//! Shutdown flips every shutdown channel, aborts background tasks,
//! and drops the TempDir.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use nodedb::bridge::dispatch::Dispatcher;
use nodedb::config::auth::AuthMode;
use nodedb::config::server::ClusterSettings;
use nodedb::control::server::pgwire::listener::PgListener;
use nodedb::control::state::SharedState;
use nodedb::data::executor::core_loop::CoreLoop;
use nodedb::event::{EventPlane, create_event_bus};
use nodedb::wal::WalManager;
use nodedb_types::config::tuning::ClusterTransportTuning;

/// Running cluster node.
pub struct TestClusterNode {
    pub node_id: u64,
    pub listen_addr: SocketAddr,
    pub pg_addr: SocketAddr,
    pub client: tokio_postgres::Client,
    pub shared: Arc<SharedState>,
    _data_dir: tempfile::TempDir,
    _conn_handle: tokio::task::JoinHandle<()>,
    pg_shutdown_tx: tokio::sync::watch::Sender<bool>,
    poller_shutdown_tx: tokio::sync::watch::Sender<bool>,
    cluster_shutdown_tx: tokio::sync::watch::Sender<bool>,
    core_stop_tx: std::sync::mpsc::Sender<()>,
    _pg_handle: tokio::task::JoinHandle<()>,
    _poller_handle: tokio::task::JoinHandle<()>,
    _core_handle: tokio::task::JoinHandle<()>,
    _event_plane: EventPlane,
}

impl TestClusterNode {
    /// Spawn a cluster node.
    ///
    /// - `node_id` — non-zero unique id within the cluster.
    /// - `seed_nodes` — empty for the bootstrap node; otherwise the
    ///   pre-bound listen address of at least one already-running
    ///   peer (typically node 1).
    pub async fn spawn(
        node_id: u64,
        seed_nodes: Vec<SocketAddr>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let data_dir = tempfile::tempdir()?;
        let data_dir_path: PathBuf = data_dir.path().to_path_buf();

        // Pre-bind the QUIC transport on a random port so we know the
        // listen address before wiring seeds / cluster settings.
        let transport = Arc::new(nodedb_cluster::NexarTransport::new(
            node_id,
            "127.0.0.1:0".parse()?,
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
            replication_factor: 3,
            force_bootstrap: false,
        };

        // Open WAL + dispatcher + event bus.
        let wal = Arc::new(WalManager::open_for_testing(
            &data_dir_path.join("test.wal"),
        )?);
        let (dispatcher, data_sides) = Dispatcher::new(1, 64);
        let (event_producers, event_consumers) = create_event_bus(1);

        // Credential store backed by the system catalog — required for
        // CREATE COLLECTION to exercise the full persistence path.
        let credentials = Arc::new(
            nodedb::control::security::credential::store::CredentialStore::open(
                &data_dir_path.join("system.redb"),
            )?,
        );
        let mut shared =
            SharedState::new_with_credentials(dispatcher, Arc::clone(&wal), credentials);

        // Initialise the cluster using the pre-bound transport.
        let handle = nodedb::control::cluster::init_cluster_with_transport(
            &cluster_settings,
            transport.clone(),
            &data_dir_path,
        )
        .await?;

        // Wire cluster handles into SharedState (mirrors main.rs).
        // `Arc::get_mut` is valid here: `shared` has not been cloned.
        if let Some(state) = Arc::get_mut(&mut shared) {
            state.node_id = handle.node_id;
            state.cluster_topology = Some(Arc::clone(&handle.topology));
            state.cluster_routing = Some(Arc::clone(&handle.routing));
            state.cluster_transport = Some(Arc::clone(&handle.transport));
            state.metadata_cache = Arc::clone(&handle.metadata_cache);
            state.metadata_applied_index_watcher = Arc::clone(&handle.applied_index_watcher);
        } else {
            return Err("SharedState already cloned before cluster wire-up".into());
        }

        // Start Data Plane core.
        let data_side = data_sides.into_iter().next().expect("data side");
        let event_producer = event_producers.into_iter().next().expect("event producer");
        let core_dir = data_dir_path.clone();
        let (core_stop_tx, core_stop_rx) = std::sync::mpsc::channel::<()>();
        let core_handle = tokio::task::spawn_blocking(move || {
            let mut core =
                CoreLoop::open(0, data_side.request_rx, data_side.response_tx, &core_dir)
                    .expect("core open");
            core.set_event_producer(event_producer);
            while core_stop_rx.try_recv().is_err() {
                core.tick();
                std::thread::sleep(Duration::from_millis(1));
            }
        });

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
        let event_plane = EventPlane::spawn(
            event_consumers,
            Arc::clone(&wal),
            watermark_store,
            Arc::clone(&shared),
            trigger_dlq,
            Arc::clone(&shared.cdc_router),
        );

        // Start Raft + install MetadataCommitApplier.
        let (cluster_shutdown_tx, cluster_shutdown_rx) = tokio::sync::watch::channel(false);
        let tuning = ClusterTransportTuning::default();
        nodedb::control::cluster::start_raft(
            &handle,
            Arc::clone(&shared),
            &data_dir_path,
            cluster_shutdown_rx,
            &tuning,
        )?;

        // pgwire listener.
        let pg_listener = PgListener::bind("127.0.0.1:0".parse()?).await?;
        let pg_addr = pg_listener.local_addr();
        let (pg_shutdown_tx, pg_shutdown_rx) = tokio::sync::watch::channel(false);
        let shared_pg = Arc::clone(&shared);
        let pg_handle = tokio::spawn(async move {
            let _ = pg_listener
                .run(
                    shared_pg,
                    AuthMode::Trust,
                    None,
                    Arc::new(tokio::sync::Semaphore::new(128)),
                    pg_shutdown_rx,
                )
                .await;
        });

        // Give the listener a moment to start accepting.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Connect tokio_postgres client.
        let conn_str = format!(
            "host=127.0.0.1 port={} user=nodedb dbname=nodedb",
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
            client,
            shared,
            _data_dir: data_dir,
            _conn_handle: conn_handle,
            pg_shutdown_tx,
            poller_shutdown_tx,
            cluster_shutdown_tx,
            core_stop_tx,
            _pg_handle: pg_handle,
            _poller_handle: poller_handle,
            _core_handle: core_handle,
            _event_plane: event_plane,
        })
    }

    /// Number of nodes currently visible in this node's topology view.
    pub fn topology_size(&self) -> usize {
        self.shared
            .cluster_topology
            .as_ref()
            .map(|t| t.read().unwrap_or_else(|p| p.into_inner()).node_count())
            .unwrap_or(0)
    }

    /// Number of active collections visible on this node (read through
    /// the local `SystemCatalog` redb — populated by the
    /// `MetadataCommitApplier` on every node via
    /// `CatalogEntry::apply_to`).
    pub fn cached_collection_count(&self) -> usize {
        let Some(catalog) = self.shared.credentials.catalog() else {
            return 0;
        };
        // `load_collections_for_tenant` filters out `is_active = false`
        // records, so a deactivated collection drops out of the count.
        catalog
            .load_collections_for_tenant(1)
            .map(|v| v.len())
            .unwrap_or(0)
    }

    /// Number of sequences visible in this node's in-memory
    /// `sequence_registry`. After the applier spawns its
    /// post-apply side effect for a `PutSequence`, the registry
    /// should see the new record on every node.
    pub fn sequence_count(&self, tenant_id: u32) -> usize {
        self.shared.sequence_registry.list(tenant_id).len()
    }

    /// Check whether a sequence with the given name exists in this
    /// node's in-memory registry.
    pub fn has_sequence(&self, tenant_id: u32, name: &str) -> bool {
        self.shared.sequence_registry.exists(tenant_id, name)
    }

    /// Execute a simple query; returns an error message on SQL error.
    pub async fn exec(&self, sql: &str) -> Result<(), String> {
        match self.client.simple_query(sql).await {
            Ok(_) => Ok(()),
            Err(e) => Err(pg_error_detail(&e)),
        }
    }

    /// Cooperatively shut down every background task this node owns.
    pub async fn shutdown(self) {
        let _ = self.pg_shutdown_tx.send(true);
        let _ = self.cluster_shutdown_tx.send(true);
        let _ = self.poller_shutdown_tx.send(true);
        let _ = self.core_stop_tx.send(());
        // Give tokio a chance to drop the task futures before TempDir
        // is dropped — otherwise redb file locks can linger.
        for _ in 0..32 {
            tokio::task::yield_now().await;
        }
    }
}

fn pg_error_detail(e: &tokio_postgres::Error) -> String {
    if let Some(db_err) = e.as_db_error() {
        format!(
            "{}: {} (SQLSTATE {})",
            db_err.severity(),
            db_err.message(),
            db_err.code().code()
        )
    } else {
        format!("{e:?}")
    }
}
