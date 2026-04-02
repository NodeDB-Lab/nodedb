//! Distributed apply machinery — tracks pending Raft proposals and applies
//! committed entries to the local Data Plane.
//!
//! See [`crate::control::wal_replication`] for the full write flow description.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, oneshot};
use tracing::{debug, warn};

use nodedb_cluster::raft_loop::CommitApplier;
use nodedb_raft::message::LogEntry;

use crate::bridge::envelope::{Priority, Request, Status};
use crate::control::state::SharedState;
use crate::control::wal_replication::from_replicated_entry;
use crate::types::{ReadConsistency, RequestId};

// ── Propose tracker ─────────────────────────────────────────────────

/// Response payload sent back to the proposer after commit + execution.
pub type ProposeResult = std::result::Result<Vec<u8>, crate::Error>;

/// Tracks pending proposals awaiting Raft commit.
///
/// Keyed by `(group_id, log_index)`. The proposer registers a oneshot
/// receiver before calling `propose()`, and awaits it. When the entry
/// is committed and executed, the result is sent through the channel.
pub struct ProposeTracker {
    waiters: Mutex<HashMap<(u64, u64), oneshot::Sender<ProposeResult>>>,
}

impl Default for ProposeTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl ProposeTracker {
    pub fn new() -> Self {
        Self {
            waiters: Mutex::new(HashMap::new()),
        }
    }

    /// Register a waiter for a proposed entry. Returns a receiver that
    /// resolves when the entry is committed and executed.
    pub fn register(&self, group_id: u64, log_index: u64) -> oneshot::Receiver<ProposeResult> {
        let (tx, rx) = oneshot::channel();
        let mut waiters = self.waiters.lock().unwrap_or_else(|p| p.into_inner());
        waiters.insert((group_id, log_index), tx);
        rx
    }

    /// Complete a waiter after the entry has been committed and executed.
    /// Returns false if no waiter was registered (follower path).
    pub fn complete(&self, group_id: u64, log_index: u64, result: ProposeResult) -> bool {
        let mut waiters = self.waiters.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(tx) = waiters.remove(&(group_id, log_index)) {
            let _ = tx.send(result);
            true
        } else {
            false
        }
    }
}

// ── Distributed applier ─────────────────────────────────────────────

/// Queued entry for the background apply loop.
pub struct ApplyBatch {
    pub(crate) group_id: u64,
    pub(crate) entries: Vec<LogEntry>,
}

/// CommitApplier that queues committed entries for async Data Plane execution.
///
/// Uses a bounded tokio mpsc channel: `apply_committed()` is called from the
/// sync Raft tick loop and pushes non-blockingly. A background async task
/// reads from the channel, dispatches each write to the Data Plane, and
/// notifies any waiting proposers.
pub struct DistributedApplier {
    apply_tx: mpsc::Sender<ApplyBatch>,
    tracker: Arc<ProposeTracker>,
}

impl DistributedApplier {
    pub fn new(apply_tx: mpsc::Sender<ApplyBatch>, tracker: Arc<ProposeTracker>) -> Self {
        Self { apply_tx, tracker }
    }

    /// Access the tracker (for registering propose waiters).
    pub fn tracker(&self) -> &Arc<ProposeTracker> {
        &self.tracker
    }
}

impl CommitApplier for DistributedApplier {
    fn apply_committed(&self, group_id: u64, entries: &[LogEntry]) -> u64 {
        let last_index = entries.last().map(|e| e.index).unwrap_or(0);

        // Filter to non-empty entries (skip no-op entries from leader election).
        let real_entries: Vec<LogEntry> = entries
            .iter()
            .filter(|e| !e.data.is_empty())
            .cloned()
            .collect();

        if real_entries.is_empty() {
            // Still notify waiters for no-op entries so proposers don't hang.
            for entry in entries {
                self.tracker.complete(group_id, entry.index, Ok(vec![]));
            }
            return last_index;
        }

        // Push to background task. If the channel is full, log a warning
        // but don't block the tick loop.
        if let Err(e) = self.apply_tx.try_send(ApplyBatch {
            group_id,
            entries: real_entries,
        }) {
            warn!(group_id, error = %e, "apply queue full, entries will be retried on next tick");
            // Don't advance applied index — entries will be re-delivered.
            return 0;
        }

        last_index
    }
}

// ── Background apply loop ───────────────────────────────────────────

static APPLY_REQUEST_COUNTER: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(2_000_000_000);

/// Run the background loop that applies committed Raft entries to the local Data Plane.
///
/// This task reads from the apply channel, deserializes each entry, dispatches
/// the write to the Data Plane via SPSC, and notifies proposers.
pub async fn run_apply_loop(
    mut apply_rx: mpsc::Receiver<ApplyBatch>,
    state: Arc<SharedState>,
    tracker: Arc<ProposeTracker>,
) {
    while let Some(batch) = apply_rx.recv().await {
        for entry in &batch.entries {
            let (tenant_id, vshard_id, plan) = match from_replicated_entry(&entry.data) {
                Some(t) => t,
                None => {
                    // Couldn't deserialize — might be a different format or corrupted.
                    debug!(
                        group_id = batch.group_id,
                        index = entry.index,
                        "skipping non-ReplicatedEntry commit"
                    );
                    tracker.complete(batch.group_id, entry.index, Ok(vec![]));
                    continue;
                }
            };

            let request_id = RequestId::new(
                APPLY_REQUEST_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            );

            let request = Request {
                request_id,
                tenant_id,
                vshard_id,
                plan,
                deadline: Instant::now() + Duration::from_secs(30),
                priority: Priority::Normal,
                trace_id: 0,
                consistency: ReadConsistency::Strong,
                idempotency_key: None,
                event_source: crate::event::EventSource::User,
                user_roles: Vec::new(),
            };

            let rx = state.tracker.register_oneshot(request_id);

            let dispatch_result = match state.dispatcher.lock() {
                Ok(mut d) => d.dispatch(request),
                Err(poisoned) => poisoned.into_inner().dispatch(request),
            };

            if let Err(e) = dispatch_result {
                warn!(
                    group_id = batch.group_id,
                    index = entry.index,
                    error = %e,
                    "failed to dispatch committed write"
                );
                tracker.complete(
                    batch.group_id,
                    entry.index,
                    Err(crate::Error::Internal {
                        detail: format!("dispatch failed: {e}"),
                    }),
                );
                continue;
            }

            // Await Data Plane response.
            match tokio::time::timeout(Duration::from_secs(30), rx).await {
                Ok(Ok(resp)) => {
                    let payload = resp.payload.to_vec();
                    if resp.status == Status::Error {
                        let err_msg = resp
                            .error_code
                            .as_ref()
                            .map(|c| format!("{c:?}"))
                            .unwrap_or_else(|| "execution error".into());
                        tracker.complete(
                            batch.group_id,
                            entry.index,
                            Err(crate::Error::Internal { detail: err_msg }),
                        );
                    } else {
                        tracker.complete(batch.group_id, entry.index, Ok(payload));
                    }
                }
                Ok(Err(_)) => {
                    tracker.complete(
                        batch.group_id,
                        entry.index,
                        Err(crate::Error::Internal {
                            detail: "response channel closed".to_string(),
                        }),
                    );
                }
                Err(_) => {
                    tracker.complete(
                        batch.group_id,
                        entry.index,
                        Err(crate::Error::Internal {
                            detail: "deadline exceeded applying committed write".to_string(),
                        }),
                    );
                }
            }
        }
    }
}

/// Create a DistributedApplier and the channel for the background apply loop.
///
/// Returns (applier, receiver). Spawn `run_apply_loop` with the receiver.
pub fn create_distributed_applier(
    tracker: Arc<ProposeTracker>,
) -> (DistributedApplier, mpsc::Receiver<ApplyBatch>) {
    let (tx, rx) = mpsc::channel(1024);
    let applier = DistributedApplier::new(tx, tracker);
    (applier, rx)
}
