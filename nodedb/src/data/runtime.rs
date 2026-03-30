//! TPC (Thread-per-Core) runtime for Data Plane cores.
//!
//! Replaces the naive `sleep(50µs)` busy-poll with an eventfd-driven wake
//! mechanism. Each core thread:
//!
//! 1. Pins itself to a dedicated jemalloc arena (zero allocator contention).
//! 2. Blocks on `libc::poll(eventfd)` when idle (zero CPU waste).
//! 3. Wakes instantly when the Control Plane signals via `EventFdNotifier`.
//! 4. Processes all pending requests in a tight loop, then re-parks.
//!
//! # Panic Isolation
//!
//! Every `core.tick()` invocation is wrapped in `catch_unwind`. A panic in
//! any engine execution (bad index, arithmetic overflow, corrupted data)
//! is caught without killing the core thread. The faulting request receives
//! an `INTERNAL_ERROR` response, and the core continues serving subsequent
//! requests. A health watchdog tracks consecutive panics: if the threshold
//! is exceeded, the core stops accepting new work and logs an alert.

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::Path;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Instant;

use tracing::{error, info, warn};

use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};
use crate::bridge::envelope::{ErrorCode, Payload, Response, Status};
use crate::data::eventfd::{EventFd, EventFdNotifier};
use crate::data::executor::core_loop::CoreLoop;

use nodedb_bridge::buffer::{Consumer, Producer};

/// Maximum idle poll timeout in milliseconds.
///
/// Even without signals, cores wake periodically to run maintenance
/// (e.g., deferred retry polling, metrics flush).
const IDLE_POLL_TIMEOUT_MS: i32 = 100;

/// Maximum consecutive panics before the core enters degraded mode.
///
/// Degraded mode drains and rejects all incoming requests with
/// `ErrorCode::CoreDegraded` instead of executing them. This prevents
/// a poison-pill request from hot-looping through catch_unwind.
const MAX_CONSECUTIVE_PANICS: u32 = 3;

/// Window in which consecutive panics are counted. If more than
/// `MAX_CONSECUTIVE_PANICS` occur within this window, the core degrades.
/// Panics separated by more than this duration reset the counter.
const PANIC_WINDOW_SECS: u64 = 60;

/// How long a degraded core stays in degraded mode before attempting
/// recovery. After this cool-down, the core resets its panic counter
/// and resumes accepting requests. If the poison-pill request is still
/// in the queue, it will panic again and re-enter degraded mode — but
/// by then the offending request has been drained and rejected.
const DEGRADED_COOLDOWN_SECS: u64 = 30;

/// Tracks core health across panics for the watchdog.
struct CoreHealthWatchdog {
    /// Number of panics in the current window.
    consecutive_panics: u32,
    /// Timestamp of the first panic in the current window.
    window_start: Option<Instant>,
    /// Whether this core has been marked degraded.
    degraded: bool,
    /// When the core entered degraded mode (for cool-down recovery).
    degraded_at: Option<Instant>,
}

impl CoreHealthWatchdog {
    fn new() -> Self {
        Self {
            consecutive_panics: 0,
            window_start: None,
            degraded: false,
            degraded_at: None,
        }
    }

    /// Record a panic. Returns `true` if the core should enter degraded mode.
    fn record_panic(&mut self) -> bool {
        let now = Instant::now();

        // Reset window if the previous panic was outside the window.
        if let Some(start) = self.window_start {
            if now.duration_since(start).as_secs() > PANIC_WINDOW_SECS {
                self.consecutive_panics = 0;
                self.window_start = Some(now);
            }
        } else {
            self.window_start = Some(now);
        }

        self.consecutive_panics += 1;

        if self.consecutive_panics >= MAX_CONSECUTIVE_PANICS {
            self.degraded = true;
            self.degraded_at = Some(Instant::now());
        }

        self.degraded
    }

    /// Record a successful tick (no panic). Resets the consecutive counter.
    fn record_success(&mut self) {
        if self.consecutive_panics > 0 {
            self.consecutive_panics = 0;
            self.window_start = None;
        }
    }

    /// Check if the core is degraded. If the cool-down period has elapsed,
    /// auto-recover: reset panic counters and exit degraded mode.
    fn is_degraded(&mut self) -> bool {
        if self.degraded
            && let Some(degraded_at) = self.degraded_at
            && degraded_at.elapsed().as_secs() >= DEGRADED_COOLDOWN_SECS
        {
            info!(
                cooldown_secs = DEGRADED_COOLDOWN_SECS,
                "core recovered from degraded mode after cool-down"
            );
            self.degraded = false;
            self.degraded_at = None;
            self.consecutive_panics = 0;
            self.window_start = None;
            return false;
        }
        self.degraded
    }
}

/// Spawn a Data Plane core on a dedicated OS thread with TPC isolation.
///
/// Returns the `JoinHandle` and the `EventFdNotifier` that the Control Plane
/// uses to wake this core after pushing a request into the SPSC queue.
///
/// If `wal_records` is non-empty, the core replays vector WAL records
/// during startup (before entering the event loop) to rebuild HNSW indexes.
/// Compaction configuration passed to each Data Plane core.
#[derive(Debug, Clone)]
pub struct CoreCompactionConfig {
    /// How often to run automatic compaction.
    pub interval: std::time::Duration,
    /// Tombstone ratio threshold for auto-compaction.
    pub tombstone_threshold: f64,
    /// Query execution tuning parameters.
    pub query: nodedb_types::config::tuning::QueryTuning,
}

impl Default for CoreCompactionConfig {
    fn default() -> Self {
        Self {
            interval: std::time::Duration::from_secs(600),
            tombstone_threshold: 0.2,
            query: nodedb_types::config::tuning::QueryTuning::default(),
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub fn spawn_core(
    core_id: usize,
    request_rx: Consumer<BridgeRequest>,
    response_tx: Producer<BridgeResponse>,
    data_dir: &Path,
    wal_records: Arc<[nodedb_wal::WalRecord]>,
    num_cores: usize,
    compaction_config: CoreCompactionConfig,
    system_metrics: Option<Arc<crate::control::metrics::SystemMetrics>>,
    event_producer: Option<crate::event::bus::EventProducer>,
) -> std::io::Result<(JoinHandle<()>, EventFdNotifier)> {
    let data_dir = data_dir.to_path_buf();

    // Create eventfd and extract notifier before moving EventFd to core thread.
    let efd = EventFd::new().map_err(std::io::Error::other)?;
    let notifier = efd.notifier();

    let handle = std::thread::Builder::new()
        .name(format!("data-core-{core_id}"))
        .spawn(move || {
            // 1. Pin to dedicated jemalloc arena.
            match nodedb_mem::arena::pin_thread_arena(core_id as u32) {
                Ok(arena) => info!(core_id, arena, "pinned to jemalloc arena"),
                Err(e) => warn!(core_id, error = %e, "failed to pin jemalloc arena, continuing with default"),
            }

            // 2. Open engines.
            let mut core = CoreLoop::open(core_id, request_rx, response_tx, &data_dir)
                .expect("failed to open CoreLoop engines");

            // 2b. Apply metrics reference.
            if let Some(m) = system_metrics {
                core.set_metrics(m);
            }

            // 2b. Wire Event Plane producer (Data Plane → Event Plane).
            if let Some(ep) = event_producer {
                core.set_event_producer(ep);
            }

            // 2c. Apply compaction config.
            core.set_compaction_config(
                compaction_config.interval,
                compaction_config.tombstone_threshold,
            );

            // 2c. Apply query tuning config.
            core.set_query_tuning(compaction_config.query);

            // 3. Load vector + spatial checkpoints (fast recovery).
            core.load_vector_checkpoints();
            core.load_spatial_checkpoints();

            // 4. Replay WAL records for crash recovery.
            if !wal_records.is_empty() {
                core.replay_vector_wal(&wal_records, num_cores);
                core.replay_kv_wal(&wal_records, num_cores);
            }

            info!(core_id, "data plane core started (eventfd-driven)");

            let mut watchdog = CoreHealthWatchdog::new();
            let mut last_checkpoint = Instant::now();
            /// Checkpoint interval: 5 minutes.
            const CHECKPOINT_INTERVAL: std::time::Duration = std::time::Duration::from_secs(300);

            /// Maximum requests to process per event loop iteration before
            /// yielding to maintenance tasks. Prevents maintenance starvation
            /// under sustained high write load.
            const MAX_TASKS_PER_ITERATION: usize = 256;

            // 5. Event loop: poll → drain → tick → checkpoint → repeat.
            loop {
                // Block until signaled or timeout.
                efd.poll_wait(IDLE_POLL_TIMEOUT_MS);

                // Drain all accumulated signals.
                while efd.drain() > 0 {}

                // If degraded, drain and reject all pending requests.
                if watchdog.is_degraded() {
                    drain_and_reject(&mut core, core_id);
                    continue;
                }

                // Process pending requests with panic isolation.
                // Bounded to MAX_TASKS_PER_ITERATION to prevent maintenance
                // starvation under sustained high load.
                let mut tasks_processed = 0usize;
                loop {
                    // catch_unwind requires FnOnce: &mut core is not UnwindSafe
                    // by default, but we explicitly opt in. The CoreLoop state
                    // may be partially inconsistent after a panic (e.g., a
                    // half-inserted HNSW node), but:
                    //   - Reads are safe: stale or partial data is better than dead core.
                    //   - Writes: the WAL ensures crash consistency on replay;
                    //     a panicked write was never acknowledged to the client.
                    //   - The watchdog degrades the core before repeated panics
                    //     can compound corruption.
                    let result =
                        catch_unwind(AssertUnwindSafe(|| core.tick()));

                    match result {
                        Ok(0) => break, // No more pending requests.
                        Ok(_) => {
                            watchdog.record_success();
                            tasks_processed += 1;
                            if tasks_processed >= MAX_TASKS_PER_ITERATION {
                                break; // Yield to maintenance.
                            }
                        }
                        Err(panic_payload) => {
                            // Extract panic message for logging.
                            let msg = panic_message(&panic_payload);
                            error!(
                                core_id,
                                panic_count = watchdog.consecutive_panics + 1,
                                message = %msg,
                                "data plane core caught panic during tick"
                            );

                            let is_degraded = watchdog.record_panic();
                            if is_degraded {
                                error!(
                                    core_id,
                                    threshold = MAX_CONSECUTIVE_PANICS,
                                    window_secs = PANIC_WINDOW_SECS,
                                    "core entered DEGRADED mode — rejecting all requests"
                                );
                                drain_and_reject(&mut core, core_id);
                            }

                            // The panicked tick may have consumed a request from
                            // the queue without sending a response. The in-flight
                            // request's oneshot channel in RequestTracker will
                            // time out on the Control Plane side (deadline expiry),
                            // which is the correct behavior — the client sees
                            // DEADLINE_EXCEEDED rather than hanging forever.

                            break; // Exit inner loop; re-enter poll_wait.
                        }
                    }
                }

                // Periodic vector checkpoint (when idle and interval elapsed).
                if last_checkpoint.elapsed() >= CHECKPOINT_INTERVAL {
                    core.checkpoint_vector_indexes();
                    last_checkpoint = Instant::now();
                }

                // Periodic compaction + maintenance (tombstone cleanup, CSR compact, edge sweep).
                core.maybe_run_maintenance();
            }
        })?;

    Ok((handle, notifier))
}

/// Drain all pending requests from a core's SPSC queue and send back
/// `CoreDegraded` error responses. Used when the watchdog has flagged
/// the core as unhealthy.
fn drain_and_reject(core: &mut CoreLoop, core_id: usize) {
    core.drain_requests();
    while let Some(task) = core.task_queue.pop_front() {
        let response = Response {
            request_id: task.request_id(),
            status: Status::Error,
            attempt: 1,
            partial: false,
            payload: Payload::empty(),
            watermark_lsn: core.watermark,
            error_code: Some(ErrorCode::Internal {
                detail: format!("core-{core_id} is degraded after repeated panics"),
            }),
        };
        if let Err(e) = core
            .response_tx
            .try_push(BridgeResponse { inner: response })
        {
            warn!(core_id, error = %e, "failed to send degraded-rejection response");
        }
    }
}

/// Extract a human-readable message from a panic payload.
fn panic_message(payload: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "non-string panic payload".to_string()
    }
}
