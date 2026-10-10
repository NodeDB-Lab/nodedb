// SPDX-License-Identifier: BUSL-1.1

//! A one-node cluster for unit tests that propose DDL or writes.
//!
//! It boots the way a server with no `[cluster]` section boots:
//! `init_single_node_calvin`, `wire_cluster_handle`, `start_raft`, then the
//! metadata-group readiness wait. Its one Data Plane core acknowledges every
//! request, unless the test supplies its own ([`boot_with_core`]). A test
//! that serves no request and proposes nothing builds a bare `SharedState`
//! instead.
//!
//! The boot runs on any tokio runtime flavor.

use std::sync::Arc;
use std::time::Duration;

use nodedb_types::config::tuning::{ClusterTransportTuning, RaftReadyTimeout};

use crate::bridge::dispatch::{CoreChannelDataSide, Dispatcher};
use crate::control::security::credential::CredentialStore;
use crate::control::state::SharedState;
use crate::wal::WalManager;

/// The metadata-group stall bound for this harness: fail fast instead of
/// inheriting the backlog-tolerant production default. The harness boots a
/// bare single-voter cluster, so a stall here is a test bug, not a slow
/// replay, and a short bound reports it sooner.
const TEST_RAFT_READY_TIMEOUT: RaftReadyTimeout = RaftReadyTimeout(Duration::from_secs(30));

/// How long each shutdown step can take.
const SHUTDOWN_STEP: Duration = Duration::from_secs(5);

/// A running one-node cluster and the state it serves.
pub(crate) struct OneNodeCluster {
    pub(crate) state: Arc<SharedState>,
    core: tokio::task::JoinHandle<()>,
    running: Option<nodedb_cluster::RunningCluster>,
    _dir: tempfile::TempDir,
}

/// Election timeouts for a group whose only voter is this node. A sole voter
/// never waits on a peer, so a short timeout elects it at once.
fn one_node_tuning() -> ClusterTransportTuning {
    ClusterTransportTuning {
        election_timeout_min_ms: 150,
        election_timeout_max_ms: 300,
        ..ClusterTransportTuning::default()
    }
}

/// Boot a one-node cluster in a fresh directory and wait until its metadata
/// group applied its first entry.
pub(crate) async fn boot() -> OneNodeCluster {
    boot_with(|_| {}).await
}

/// [`boot`], with `configure` run on the state before it is shared.
pub(crate) async fn boot_with(configure: impl FnOnce(&mut SharedState)) -> OneNodeCluster {
    boot_with_core(configure, |state, side| {
        tokio::spawn(crate::control::state::test_core::acknowledge_every_request(
            state, side,
        ))
    })
    .await
}

/// [`boot_with`], with `core` spawning the task that answers the Data
/// Plane side before the cluster starts. The node applies its Raft entries
/// through that core, so a test's core sees and answers them.
pub(crate) async fn boot_with_core(
    configure: impl FnOnce(&mut SharedState),
    core: impl FnOnce(Arc<SharedState>, CoreChannelDataSide) -> tokio::task::JoinHandle<()>,
) -> OneNodeCluster {
    let dir = tempfile::tempdir().expect("create test directory");
    let wal = Arc::new(
        WalManager::open_for_testing(&dir.path().join("test.wal")).expect("open test WAL"),
    );
    let credentials = Arc::new(
        CredentialStore::open(&dir.path().join("system.redb")).expect("open credential store"),
    );
    credentials
        .catalog()
        .bootstrap_default_database()
        .expect("bootstrap the default database");
    let (dispatcher, mut data_sides) = Dispatcher::new(1, 64);
    let mut state = SharedState::new_with_credentials(dispatcher, wal, credentials, false)
        .expect("construct shared state");

    let tuning = one_node_tuning();
    let handle = super::init_single_node_calvin(dir.path(), &tuning)
        .await
        .expect("init the one-node cluster");
    {
        let wired = Arc::get_mut(&mut state).expect("state is not shared yet");
        configure(wired);
        crate::bootstrap::state_wiring::wire_cluster_handle(wired, &handle, dir.path())
            .expect("wire the cluster handle");
    }
    super::metadata_applier::seed_host_tables(&state).expect("seed host tables");
    crate::bootstrap::state_wiring::install_gateway(&state).expect("install gateway");

    let side = data_sides.pop().expect("one data side");
    let core = core(Arc::clone(&state), side);

    let ready = super::start_raft(&handle, Arc::clone(&state), dir.path(), &tuning)
        .await
        .expect("start raft");
    let running = handle
        .running_cluster
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .take();
    crate::bootstrap::cluster_ready::await_raft_ready(&state, ready, TEST_RAFT_READY_TIMEOUT)
        .await
        .expect("the metadata group applies its first entry");

    OneNodeCluster {
        state,
        core,
        running,
        _dir: dir,
    }
}

impl OneNodeCluster {
    /// Stop every task the cluster started and close its transport.
    pub(crate) async fn shutdown(mut self) {
        self.state.shutdown.signal();
        if let Some(running) = self.running.take() {
            let errors = running.shutdown_all(SHUTDOWN_STEP).await;
            assert!(errors.is_empty(), "cluster subsystem shutdown: {errors:?}");
        }
        if let Some(transport) = self.state.cluster_transport.clone() {
            // A one-node cluster has no peer to acknowledge the close.
            let _ = transport.close(SHUTDOWN_STEP).await;
        }
        self.core.abort();
    }
}
