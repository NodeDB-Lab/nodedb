// SPDX-License-Identifier: BUSL-1.1

//! A `SharedState` served by a one-node cluster that runs on its own thread.
//!
//! Some tests drive the Control Plane directly: they call DDL dispatch, an
//! HTTP router, or a listener's accept loop against a `SharedState`. A DDL
//! proposes through the metadata group, so the state needs a booted cluster.
//! [`BootedState::boot`] boots one through [`crate::single_node`], with a Data
//! Plane core, a response poller and, unless the options turn it off, an
//! Event Plane, on a dedicated thread
//! and runtime. The call is synchronous, so a sync fixture can return it.
//! Dropping the handle stops the node and removes its directory.

use std::sync::Arc;
use std::time::Duration;

use nodedb::bridge::dispatch::Dispatcher;
use nodedb::control::state::SharedState;
use nodedb::event::{EventPlane, EventPlaneConfig, create_event_bus};
use nodedb::wal::WalManager;

use crate::single_node::{BootError, OneNodeRaft};

/// Stack size of the node runtime's worker threads. The metadata applier and
/// the post-apply steps run there, with deep call stacks in debug builds.
const NODE_THREAD_STACK: usize = 16 * 1024 * 1024;

/// How the booted node's state is set up before anything shares it.
pub struct BootOptions {
    /// Create the built-in `default` database in the catalog.
    pub default_database: bool,
    /// Spawn the node's Event Plane. A node runs exactly one Event Plane: a
    /// test that spawns its own plane on the state boots with `false`, so its
    /// plane is the node's only one.
    pub event_plane: bool,
    /// Applied to the state before the gateway install. The installed
    /// gateway holds a back-reference, so no field is settable afterward.
    pub configure: Box<dyn FnOnce(&mut SharedState) + Send>,
}

impl Default for BootOptions {
    fn default() -> Self {
        Self {
            default_database: true,
            event_plane: true,
            configure: Box::new(|_| {}),
        }
    }
}

/// A running one-node cluster and the state it serves.
pub struct BootedState {
    state: Arc<SharedState>,
    stop_tx: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl std::ops::Deref for BootedState {
    type Target = Arc<SharedState>;

    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

impl BootedState {
    /// Boot a one-node cluster in a fresh directory and return its state
    /// once it serves requests.
    ///
    /// Panics when the boot fails.
    pub fn boot(options: BootOptions) -> Self {
        let (state_tx, state_rx) = std::sync::mpsc::channel::<Result<Arc<SharedState>, String>>();
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
        let thread = std::thread::Builder::new()
            .name("booted-state".into())
            .spawn(move || run_node(options, state_tx, stop_rx))
            .expect("spawn the booted-state thread");
        let state = match state_rx.recv() {
            Ok(Ok(state)) => state,
            Ok(Err(error)) => panic!("one-node cluster boot failed: {error}"),
            Err(_) => panic!("one-node cluster boot thread exited before it booted"),
        };
        Self {
            state,
            stop_tx: Some(stop_tx),
            thread: Some(thread),
        }
    }
}

impl Drop for BootedState {
    fn drop(&mut self) {
        if let Some(stop_tx) = self.stop_tx.take() {
            let _ = stop_tx.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// Everything the node thread stops on shutdown.
struct Node {
    state: Arc<SharedState>,
    shutdown_bus: nodedb::control::shutdown::ShutdownBus,
    poller_shutdown_tx: tokio::sync::watch::Sender<bool>,
    poller: tokio::task::JoinHandle<()>,
    core_stop_tx: std::sync::mpsc::Sender<()>,
    core: tokio::task::JoinHandle<()>,
    /// `None` when the test spawns the node's Event Plane itself.
    event_plane: Option<EventPlane>,
    raft: OneNodeRaft,
    _dir: tempfile::TempDir,
}

/// The node thread: boot, hand the state over, wait for the stop signal,
/// then shut down.
fn run_node(
    options: BootOptions,
    state_tx: std::sync::mpsc::Sender<Result<Arc<SharedState>, String>>,
    stop_rx: tokio::sync::oneshot::Receiver<()>,
) {
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_stack_size(NODE_THREAD_STACK)
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            let _ = state_tx.send(Err(format!("build the node runtime: {error}")));
            return;
        }
    };
    runtime.block_on(async move {
        let node = match boot_node(options).await {
            Ok(node) => node,
            Err(error) => {
                let _ = state_tx.send(Err(error.to_string()));
                return;
            }
        };
        if state_tx.send(Ok(Arc::clone(&node.state))).is_err() {
            node.shutdown().await;
            return;
        }
        // A dropped sender stops the node too.
        let _ = stop_rx.await;
        node.shutdown().await;
    });
}

async fn boot_node(options: BootOptions) -> Result<Node, BootError> {
    let BootOptions {
        default_database,
        event_plane: spawn_event_plane,
        configure,
    } = options;
    let dir = tempfile::tempdir()?;
    let wal = Arc::new(WalManager::open_for_testing(&dir.path().join("test.wal"))?);
    let credentials = Arc::new(
        nodedb::control::security::credential::store::CredentialStore::open(
            &dir.path().join("system.redb"),
        )?,
    );
    if default_database {
        credentials.catalog().bootstrap_default_database()?;
    }
    let (dispatcher, data_sides) = Dispatcher::new(1, 64);
    let (event_producers, event_consumers) = create_event_bus(1);
    let mut state =
        SharedState::new_with_credentials(dispatcher, Arc::clone(&wal), credentials, false)?;
    let cluster = crate::single_node::init(dir.path()).await?;
    {
        let wired = Arc::get_mut(&mut state).ok_or("shared state is cloned before wiring")?;
        crate::single_node::wire(wired, &cluster, dir.path())?;
        configure(wired);
    }
    nodedb::bootstrap::state_wiring::install_gateway(&state)?;

    let data_side = data_sides.into_iter().next().ok_or("no data side")?;
    let event_producer = event_producers
        .into_iter()
        .next()
        .ok_or("no event producer")?;
    let (core_stop_tx, core_stop_rx) = std::sync::mpsc::channel::<()>();
    let core = crate::core_loop_runner::spawn_core_loop(crate::core_loop_runner::CoreLoopSpawn {
        idx: 0,
        node_id: state.node_id,
        num_cores: 1,
        data_side,
        core_dir: dir.path().to_path_buf(),
        core_array_catalog: state.array_catalog.clone(),
        event_producer,
        core_metrics: state.system_metrics.clone(),
        governor: state.governor.clone(),
        replay: None,
        graph_tuning: nodedb_types::config::tuning::GraphTuning::default(),
        query_tuning: nodedb_types::config::tuning::QueryTuning::default(),
        timeseries_tuning: nodedb_types::config::tuning::TimeseriesToning::default(),
        doc_config_seed: nodedb::bootstrap::data_plane::load_doc_config_registry_from(
            state.credentials.catalog(),
        ),
        event_interest: crate::core_loop_runner::event_interest_for(&state),
        stop_rx: core_stop_rx,
    });

    let poll_state = Arc::clone(&state);
    let (poller_shutdown_tx, mut poller_shutdown_rx) = tokio::sync::watch::channel(false);
    let poller = tokio::spawn(async move {
        loop {
            poll_state.poll_and_route_responses();
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(1)) => {}
                _ = poller_shutdown_rx.changed() => break,
            }
        }
    });

    let (shutdown_bus, _) =
        nodedb::control::shutdown::ShutdownBus::new(Arc::clone(&state.shutdown));
    let event_plane = if spawn_event_plane {
        let watermark_store = Arc::new(nodedb::event::watermark::WatermarkStore::open(dir.path())?);
        let trigger_dlq = Arc::new(std::sync::Mutex::new(
            nodedb::event::trigger::TriggerDlq::open(dir.path())?,
        ));
        Some(EventPlane::spawn(EventPlaneConfig {
            consumers_rx: event_consumers,
            wal: Arc::clone(&wal),
            watermark_store,
            shared_state: Arc::clone(&state),
            trigger_dlq,
            cdc_router: Arc::clone(&state.cdc_router),
            shutdown: Arc::clone(&state.shutdown),
            shutdown_bus: shutdown_bus.clone(),
        }))
    } else {
        // The core's events have no consumer here. The test's own plane
        // consumes the events it emits on its own bus.
        drop(event_consumers);
        None
    };

    let raft = crate::single_node::start(&cluster, &state, dir.path()).await?;
    Ok(Node {
        state,
        shutdown_bus,
        poller_shutdown_tx,
        poller,
        core_stop_tx,
        core,
        event_plane,
        raft,
        _dir: dir,
    })
}

impl Node {
    async fn shutdown(self) {
        let Node {
            state,
            shutdown_bus,
            poller_shutdown_tx,
            poller,
            core_stop_tx,
            core,
            event_plane,
            mut raft,
            _dir,
        } = self;
        shutdown_bus.initiate();
        let _ = poller_shutdown_tx.send(true);
        let _ = poller.await;
        let _ = core_stop_tx.send(());
        let _ = core.await;
        if let Some(event_plane) = event_plane {
            event_plane.shutdown_and_join().await;
        }
        state
            .loop_registry
            .shutdown_all(Duration::from_secs(5))
            .await;
        raft.shutdown(&state).await;
    }
}
