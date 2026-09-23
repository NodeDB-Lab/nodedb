// SPDX-License-Identifier: BUSL-1.1

//! Spawn a Data Plane [`CoreLoop`] for tests on a dedicated OS thread with a
//! large stack.
//!
//! [`CoreLoop::open_with_array_catalog`] has deep initialization call stacks
//! that overflow the OS-default 8 MiB blocking thread stack in debug builds;
//! 32 MiB is sufficient. Tokio's `spawn_blocking` worker stack cannot be
//! configured per-task, so the runner uses `std::thread::Builder` inside
//! `spawn_blocking` to get the larger stack while still surrendering the
//! blocking-pool slot when the loop exits.
//!
//! The runner also centralises the tick loop's exit semantics: continue
//! ticking only while the stop channel is `Empty`. Both `Ok(())` (explicit
//! stop) and `Disconnected` (sender dropped, e.g. owning harness dropped
//! mid-panic) terminate the loop. `spawn_blocking` threads cannot be aborted,
//! so a loop that continued on `Disconnected` would block tokio runtime
//! shutdown indefinitely and force nextest to kill the test process at
//! `slow-timeout` (~2 minutes of wasted CI time per flaky test).
//!
//! All four production call sites (three pgwire harness variants plus the
//! cluster harness) share this single implementation — variations are
//! expressed by the [`CoreLoopSpawn`] fields.

use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use nodedb::bridge::dispatch::CoreChannelDataSide;
use nodedb::control::array_catalog::ArrayCatalog;
use nodedb::control::metrics::SystemMetrics;
use nodedb::data::executor::core_loop::CoreLoop;
use nodedb::event::EventProducer;
use nodedb_mem::MemoryGovernor;
use nodedb_types::OrdinalClock;
use nodedb_wal::{TombstoneSet, WalRecord};

/// Stack size for the dedicated CoreLoop thread. 32 MiB covers the deepest
/// observed init stack in debug builds with a comfortable margin.
const CORE_LOOP_STACK_SIZE: usize = 32 * 1024 * 1024;

/// One inter-tick sleep duration. Matches the historical value used at every
/// site this helper replaces; centralising it avoids drift.
const TICK_INTERVAL: Duration = Duration::from_millis(1);

/// WAL replay payload threaded through to the new core before the tick loop
/// starts. Used only by the `start_on_dir` variant of the pgwire harness; the
/// cluster harness and the fresh-start variants pass `None`.
pub struct WalReplay {
    pub records: Arc<[WalRecord]>,
    pub tombstones: TombstoneSet,
    /// Total number of Data-Plane cores in this node. WAL replay routes each
    /// record to `vshard_id % num_cores`, so this MUST equal the live node's
    /// core count or records replay onto the wrong core (a multi-core node
    /// with `num_cores = 1` would replay everything onto core 0 while reads
    /// route to `vshard % real_core_count`).
    pub num_cores: usize,
}

/// Configuration for one CoreLoop spawn.
pub struct CoreLoopSpawn {
    /// Core index within the data plane (0-based).
    pub idx: usize,
    /// Total number of Data-Plane cores in this node. The committed-redo apply
    /// routes each record to `vshard_id % num_cores`, the same rule the
    /// dispatcher routes requests by.
    pub num_cores: usize,
    /// SPSC bridge endpoints for this core.
    pub data_side: CoreChannelDataSide,
    /// Storage directory shared with the rest of the harness.
    pub core_dir: PathBuf,
    /// Per-core array catalog handle from `SharedState::array_catalog`.
    pub core_array_catalog: Arc<RwLock<ArrayCatalog>>,
    /// Event Plane producer endpoint for this core.
    pub event_producer: EventProducer,
    /// Optional system metrics handle wired via `core.set_metrics`.
    pub core_metrics: Option<Arc<SystemMetrics>>,
    /// Memory governor, supplied to `CoreLoop::open_with_array_catalog` at
    /// construction. Production (`data::runtime`) always wires a real one.
    pub governor: Arc<MemoryGovernor>,
    /// WAL replay payload, or `None` for fresh-start cores.
    pub replay: Option<WalReplay>,
    /// Graph engine tuning wired via `core.set_graph_tuning`. Production
    /// (`data::runtime`) wires this from `config.tuning.graph`; the harness
    /// passes `GraphTuning::default()` (100k caps) unless a test overrides the
    /// variable-length expansion caps to drive truncation on small graphs.
    pub graph_tuning: nodedb_types::config::tuning::GraphTuning,
    /// Query execution tuning wired via `core.set_query_tuning`. Production
    /// (`data::runtime`) wires this from `config.tuning.query`; the harness
    /// passes `QueryTuning::default()` (65_536-row columnar flush threshold)
    /// unless a test overrides e.g. `columnar_flush_threshold` to drive flush
    /// on a small dataset.
    pub query_tuning: nodedb_types::config::tuning::QueryTuning,
    /// Catalog-sourced `doc_configs` seed, applied BEFORE WAL replay exactly as
    /// production's `seed_catalog_state` does.
    ///
    /// Without it a replayed collection has no declared schema to resolve, so
    /// every engine that consults `doc_configs` during replay falls back to
    /// inference: strict documents re-persist as raw MessagePack, and a
    /// timeseries collection's declared `TIME_KEY` is renamed to the inferred
    /// `timestamp`. That made every restart test here quietly weaker than it
    /// looked — none of them exercised the seeded path production always takes.
    pub doc_config_seed: Vec<nodedb::data::executor::core_loop::DocConfigSeedEntry>,
    /// Stop signal for the tick loop. Sender lives in the harness shutdown path.
    pub stop_rx: std::sync::mpsc::Receiver<()>,
}

/// Spawn a `CoreLoop` for one Data Plane core inside `tokio::spawn_blocking`.
///
/// Returns the `JoinHandle` for the blocking task. The inner OS thread is
/// joined inside the blocking task so no thread leaks if the JoinHandle is
/// awaited on shutdown.
pub fn spawn_core_loop(spawn: CoreLoopSpawn) -> tokio::task::JoinHandle<()> {
    let CoreLoopSpawn {
        idx,
        num_cores,
        data_side,
        core_dir,
        core_array_catalog,
        event_producer,
        core_metrics,
        governor,
        replay,
        graph_tuning,
        query_tuning,
        doc_config_seed,
        stop_rx,
    } = spawn;

    tokio::task::spawn_blocking(move || {
        std::thread::Builder::new()
            .name(format!("nodedb-data-core-{idx}"))
            .stack_size(CORE_LOOP_STACK_SIZE)
            .spawn(move || {
                let mut core = CoreLoop::open_with_array_catalog(
                    idx,
                    data_side.request_rx,
                    data_side.response_tx,
                    &core_dir,
                    Arc::new(OrdinalClock::new()),
                    governor,
                    core_array_catalog,
                )
                .expect("CoreLoop::open_with_array_catalog");
                core.set_event_producer(event_producer);
                core.set_num_cores(num_cores);
                core.set_query_tuning(query_tuning);
                core.set_graph_tuning(graph_tuning);
                if let Some(m) = core_metrics {
                    core.set_metrics(m);
                }
                // Before replay, never after: replay is what consults
                // `doc_configs`, and production seeds it in the same order
                // (`seed_catalog_state` -> `replay_wal_and_rebuild_indexes`).
                core.seed_doc_configs(&doc_config_seed);
                if let Some(WalReplay {
                    records,
                    tombstones,
                    num_cores: replay_num_cores,
                }) = replay
                {
                    core.replay_all_wal(&records, replay_num_cores, &tombstones);
                }
                while matches!(
                    stop_rx.try_recv(),
                    Err(std::sync::mpsc::TryRecvError::Empty)
                ) {
                    core.tick();
                    std::thread::sleep(TICK_INTERVAL);
                }
            })
            .expect("spawn nodedb-data-core thread")
            .join()
            .expect("nodedb-data-core thread panicked");
    })
}
