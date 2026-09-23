// SPDX-License-Identifier: BUSL-1.1

//! Calvin-aware test node that adds a sequencer Raft group to the
//! standard `TestNode` harness.
//!
//! Three things beyond the plain `TestNode`:
//!
//! 1. The sequencer Raft group (`SEQUENCER_GROUP_ID`) is registered on
//!    every node's `MultiRaft` **before the Raft tasks for that node are
//!    spawned**.  The 150 ms election timeout gives a comfortable window:
//!    all nodes register the group within microseconds of `start_cluster`
//!    completing, so every peer has the group registered long before the
//!    first vote request arrives.
//!
//! 2. A [`CalvinApplier`] is installed on the `RaftLoop` in place of
//!    `NoopApplier`.  It routes committed entries for
//!    `SEQUENCER_GROUP_ID` to the `SequencerStateMachine` and ignores
//!    entries for all other groups.
//!
//! 3. The node exposes:
//!    - `is_sequencer_leader()` — check if this node leads the sequencer
//!      Raft group.
//!    - `last_applied_epoch()` — read the state machine's applied epoch
//!      counter.
//!    - `start_sequencer_service(inbox_receiver, config)` — start the
//!      epoch ticker task on this node; returns a `watch::Sender<bool>`
//!      for shutdown.
//!    - `add_vshard_sender(vshard_id, sender)` — wire a per-vshard channel
//!      into the state machine so tests can assert fan-out.
//!    - `try_recv_txn(rx)` — read the next sequenced txn a fan-out channel
//!      holds.

#![allow(dead_code)] // Not every test file uses every helper.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use nodedb_cluster::{
    CacheApplier, ClusterCatalog, ClusterConfig, ClusterLifecycleTracker, ClusterTopology,
    MetadataCache, MultiRaft, NexarTransport, RaftLoop, TransportCredentials,
    calvin::{
        CalvinCompletionRegistry, SEQUENCER_GROUP_ID,
        sequencer::{
            InboxReceiver, SequencerConfig, SequencerReceivers, SequencerService,
            SequencerStateMachine, new_reservation_inbox, service::SequencerMetrics,
            state_machine::StateMachineMetrics,
        },
        types::{SchedulerInput, SequencedTxn},
    },
    start_cluster,
};
use nodedb_raft::message::LogEntry;
use tempfile::TempDir;
use tokio::sync::{mpsc, watch};

/// `CommitApplier` that routes sequencer-group entries to the
/// `SequencerStateMachine` and no-ops everything else.
pub struct CalvinApplier {
    state_machine: Arc<Mutex<SequencerStateMachine>>,
}

impl CalvinApplier {
    pub fn new(state_machine: Arc<Mutex<SequencerStateMachine>>) -> Self {
        Self { state_machine }
    }
}

impl nodedb_cluster::CommitApplier for CalvinApplier {
    fn apply_committed(&self, group_id: u64, entries: &[LogEntry]) -> u64 {
        if group_id == SEQUENCER_GROUP_ID {
            let mut sm = self.state_machine.lock().unwrap_or_else(|p| p.into_inner());
            for entry in entries {
                if !entry.data.is_empty() {
                    sm.apply(entry.index, &entry.data);
                }
            }
        }
        entries.last().map(|e| e.index).unwrap_or(0)
    }
}

/// A cluster test node extended with sequencer Raft group support.
pub struct CalvinTestNode {
    pub _data_dir: Option<TempDir>,
    pub data_dir_path: PathBuf,
    pub transport: Arc<NexarTransport>,
    pub topology: Arc<RwLock<ClusterTopology>>,
    pub lifecycle: ClusterLifecycleTracker,
    pub node_id: u64,
    pub metadata_cache: Arc<RwLock<MetadataCache>>,
    pub multi_raft: Arc<Mutex<MultiRaft>>,
    raft_loop: Arc<RaftLoop<CalvinApplier>>,
    pub state_machine: Arc<Mutex<SequencerStateMachine>>,
    pub sm_metrics: Arc<StateMachineMetrics>,
    shutdown_tx: watch::Sender<bool>,
    serve_handle: tokio::task::JoinHandle<()>,
    run_handle: tokio::task::JoinHandle<()>,
}

impl CalvinTestNode {
    /// Whether this node believes itself to be the sequencer Raft group leader.
    pub fn is_sequencer_leader(&self) -> bool {
        for status in self.raft_loop.group_statuses() {
            if status.group_id == SEQUENCER_GROUP_ID {
                return status.leader_id == self.node_id;
            }
        }
        false
    }

    /// The last applied epoch on this node's sequencer state machine,
    /// or `None` if no epoch has been applied yet.
    pub fn last_applied_epoch(&self) -> Option<u64> {
        self.state_machine
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .last_applied_epoch()
    }

    /// The number of epochs applied on this node.
    pub fn epochs_applied(&self) -> u64 {
        self.state_machine
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .metrics
            .epochs_applied
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Register a per-vshard output sender so the state machine can
    /// fan out sequenced transactions to the receiving test code.
    ///
    /// The state machine sends into `sender` itself, inside `apply`, before
    /// it advances `last_applied_epoch`. A test that waits for the epoch can
    /// then read the fan-out with `try_recv_txn` and no further wait.
    pub fn add_vshard_sender(&self, vshard_id: u32, sender: mpsc::Sender<SchedulerInput>) {
        self.state_machine
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .set_vshard_sender(vshard_id, sender);
    }

    /// Start the sequencer service epoch-ticker task on this node.
    ///
    /// Returns `(watch::Sender<bool>, Arc<SequencerMetrics>, JoinHandle<()>)`:
    /// - Send `true` on the watch to stop the task.
    /// - The metrics arc can be polled to observe admission counters.
    /// - The `JoinHandle` must be kept and awaited at test teardown so a
    ///   panic inside `service.run` surfaces as a test failure instead of
    ///   silently disappearing into the runtime.
    pub fn start_sequencer_service(
        &self,
        inbox_receiver: InboxReceiver,
        config: SequencerConfig,
    ) -> (
        watch::Sender<bool>,
        std::sync::Arc<SequencerMetrics>,
        tokio::task::JoinHandle<()>,
    ) {
        // Pair the registry's verdict signal channel with the service's
        // receiver so a completed vote tally would reach the leader's propose
        // arm (inert for now — nothing reads the verdict).
        let (verdict_tx, verdict_rx) = tokio::sync::mpsc::channel(512);
        // No test in this file exercises hot-key reservations; the receiver is
        // wired for arity only and its paired `ReservationInbox` is dropped.
        let (_reservation_inbox, reservation_receiver) = new_reservation_inbox(64);
        let mut service = SequencerService::new(
            config,
            self.node_id,
            self.multi_raft.clone(),
            SequencerReceivers {
                inbox: inbox_receiver,
                reservations: reservation_receiver,
            },
            // The service derives its own epoch seed once the sequencer group
            // has replayed its log, so it takes the state machine rather than a
            // pre-read epoch that could predate replay.
            self.state_machine.clone(),
            CalvinCompletionRegistry::new(verdict_tx),
            verdict_rx,
        );

        let metrics = service.metrics.clone();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle = tokio::spawn(async move {
            service.run(shutdown_rx).await;
        });
        (shutdown_tx, metrics, handle)
    }

    pub fn listen_addr(&self) -> SocketAddr {
        self.transport.local_addr()
    }

    pub async fn shutdown(self) {
        self.raft_loop.begin_shutdown();
        let _ = self.shutdown_tx.send(true);
        self.serve_handle.abort();
        self.run_handle.abort();
        let _ = self.serve_handle.await;
        let _ = self.run_handle.await;
        for _ in 0..32 {
            tokio::task::yield_now().await;
        }
    }
}

/// Spawn a single `CalvinTestNode`.
///
/// `all_node_ids` is the full 3-node voter set; `node_idx` is the index of
/// this node in that list.  Registers the sequencer group **before** spawning
/// background Raft tasks.
///
/// `seed_nodes` must be empty for the bootstrap node (index 0) and contain
/// the bootstrap node's address for nodes 1 and 2.
async fn spawn_one_calvin_node(
    node_idx: usize,
    all_node_ids: &[u64],
    seed_nodes: Vec<SocketAddr>,
) -> Result<CalvinTestNode, Box<dyn std::error::Error + Send + Sync>> {
    let node_id = all_node_ids[node_idx];
    let transport = Arc::new(NexarTransport::with_timeout(
        node_id,
        "127.0.0.1:0".parse().unwrap(),
        Duration::from_secs(4),
        TransportCredentials::Insecure,
    )?);

    let data_dir = tempfile::tempdir()?;
    let data_dir_path = data_dir.path().to_path_buf();
    let catalog = Arc::new(ClusterCatalog::open(&data_dir_path.join("cluster.redb"))?);

    let listen_addr = transport.local_addr();
    let seeds = if seed_nodes.is_empty() {
        vec![listen_addr]
    } else {
        seed_nodes
    };

    let config = ClusterConfig {
        node_id,
        listen_addr,
        seed_nodes: seeds,
        num_groups: 2,
        replication_factor: 3,
        data_dir: data_dir_path.clone(),
        force_bootstrap: false,
        join_retry: nodedb_cluster::JoinRetryPolicy {
            max_attempts: 8,
            max_backoff_secs: 2,
        },
        swim_udp_addr: None,
        election_timeout_min: Duration::from_millis(150),
        election_timeout_max: Duration::from_millis(300),
        install_snapshot_chunk_bytes: 4 * 1024 * 1024,
        orphan_partial_max_age_secs: 300,
        log_compaction_threshold: None,
    };

    let lifecycle = ClusterLifecycleTracker::new();
    let state = start_cluster(&config, &catalog, Arc::clone(&transport), &lifecycle).await?;
    lifecycle.to_ready(state.topology.read().map(|t| t.node_count()).unwrap_or(0));

    // Unwrap MultiRaft immediately — no other Arc clones of it yet.
    let mut multi_raft_value = Arc::try_unwrap(state.multi_raft)
        .unwrap_or_else(|_| {
            panic!("MultiRaft should have no extra strong Arc owners in Calvin test setup")
        })
        .into_inner()
        .unwrap_or_else(|p| p.into_inner());

    // Register the sequencer group before spawning any tasks.
    // This is the race-fix: we do this synchronously here so the 150 ms
    // election timeout cannot fire before the group is registered on
    // any node.
    //
    // A joining node already reconstructed the sequencer group as a learner
    // from its `JoinResponse` during `start_cluster` above (the join path now
    // owns sequencer membership, including learner→voter promotion). Re-adding
    // it here would open the group's redb storage a second time and fail with a
    // lock error, so only the bootstrap node (which has no reconstructed group)
    // creates it explicitly — mirroring the `contains_group` guard in
    // `start_raft`.
    if !multi_raft_value.contains_group(SEQUENCER_GROUP_ID) {
        let peers: Vec<u64> = all_node_ids
            .iter()
            .filter(|&&id| id != node_id)
            .copied()
            .collect();
        multi_raft_value.add_group(SEQUENCER_GROUP_ID, peers)?;
    }

    let topology = state.topology.clone();
    let metadata_cache = Arc::new(RwLock::new(MetadataCache::new()));
    let metadata_applier: Arc<dyn nodedb_cluster::MetadataApplier> =
        Arc::new(CacheApplier::new(metadata_cache.clone()));

    let sm_metrics = StateMachineMetrics::new();
    let state_machine = Arc::new(Mutex::new(SequencerStateMachine::new(
        HashMap::new(),
        CalvinCompletionRegistry::new_detached(),
    )));
    let applier = CalvinApplier::new(state_machine.clone());

    let raft_loop = Arc::new(
        RaftLoop::new(
            multi_raft_value,
            transport.clone(),
            topology.clone(),
            applier,
        )
        .with_metadata_applier(metadata_applier)
        .with_catalog(catalog.clone()),
    );
    let multi_raft = raft_loop.multi_raft_handle();

    let (shutdown_tx, shutdown_rx_serve) = watch::channel(false);
    let shutdown_rx_run = shutdown_tx.subscribe();

    let transport_for_serve = transport.clone();
    let handler_for_serve = raft_loop.clone();
    let serve_handle = tokio::spawn(async move {
        let _ = transport_for_serve
            .serve(handler_for_serve, shutdown_rx_serve)
            .await;
    });

    let raft_loop_for_run = raft_loop.clone();
    let run_handle = tokio::spawn(async move {
        raft_loop_for_run.run(shutdown_rx_run).await;
    });

    drop(catalog);

    Ok(CalvinTestNode {
        _data_dir: Some(data_dir),
        data_dir_path,
        transport,
        topology,
        lifecycle,
        node_id,
        metadata_cache,
        multi_raft,
        raft_loop,
        state_machine,
        sm_metrics,
        shutdown_tx,
        serve_handle,
        run_handle,
    })
}

/// Spin up a 3-node `CalvinTestNode` cluster.
///
/// Node 1 bootstraps first; nodes 2 and 3 join via node 1.  Each node
/// registers the sequencer Raft group before its background Raft tasks are
/// spawned, so vote requests from the first elected leader always find the
/// group registered on every peer.
///
/// `all_node_ids` must be exactly 3 entries, e.g. `vec![1, 2, 3]`.
/// Returns nodes in the order of `all_node_ids`.
pub async fn spawn_with_sequencer(
    all_node_ids: Vec<u64>,
) -> Result<Vec<CalvinTestNode>, Box<dyn std::error::Error + Send + Sync>> {
    assert_eq!(
        all_node_ids.len(),
        3,
        "spawn_with_sequencer requires exactly 3 nodes"
    );

    // Node 0: bootstrap (empty seeds — single-node start).
    let node0 = spawn_one_calvin_node(0, &all_node_ids, vec![]).await?;
    let seed_addr = node0.listen_addr();

    // Small yield to let node 0's serve loop become ready for inbound RPCs.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Nodes 1 and 2: join via node 0.
    let node1 = spawn_one_calvin_node(1, &all_node_ids, vec![seed_addr]).await?;
    let node2 = spawn_one_calvin_node(2, &all_node_ids, vec![seed_addr]).await?;

    Ok(vec![node0, node1, node2])
}

/// Wait for any of the provided nodes to become the sequencer leader.
///
/// Returns the index of the leader node in `nodes`.
pub async fn wait_for_sequencer_leader(
    nodes: &[CalvinTestNode],
    deadline: Duration,
    step: Duration,
) -> usize {
    // no-determinism: test timing observability
    let start = std::time::Instant::now();
    loop {
        for (i, node) in nodes.iter().enumerate() {
            if node.is_sequencer_leader() {
                return i;
            }
        }
        if start.elapsed() >= deadline {
            panic!(
                "timed out after {:?} waiting for sequencer leader election",
                deadline
            );
        }
        tokio::time::sleep(step).await;
    }
}

/// The next sequenced txn `rx` holds, skipping any other scheduler input.
/// `None` when the channel holds no txn.
pub fn try_recv_txn(rx: &mut mpsc::Receiver<SchedulerInput>) -> Option<SequencedTxn> {
    while let Ok(input) = rx.try_recv() {
        if let SchedulerInput::Txn(txn) = input {
            return Some(txn);
        }
    }
    None
}
