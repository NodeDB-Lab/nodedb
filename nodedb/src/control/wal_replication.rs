//! Distributed WAL write path — propose writes through Raft, apply after commit.
//!
//! Write flow:
//! 1. Handler serializes write as [`ReplicatedWrite`]
//! 2. Handler proposes to Raft via [`RaftLoop::propose`]
//! 3. Handler registers a waiter in [`ProposeTracker`] keyed by (group_id, log_index)
//! 4. Raft replicates to quorum and commits
//! 5. [`DistributedApplier`] receives committed entries, queues for async execution
//! 6. Background task dispatches each write to the local Data Plane
//! 7. If a waiter exists (leader path), sends the response; otherwise just applies (follower)

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, oneshot};
use tracing::{debug, warn};

use nodedb_cluster::raft_loop::CommitApplier;
use nodedb_raft::message::LogEntry;

use crate::bridge::envelope::{PhysicalPlan, Priority, Request, Status};

/// Type alias for the Raft propose callback.
///
/// Takes `(vshard_id, serialized_entry)` and returns `(group_id, log_index)`.
pub type RaftProposer =
    dyn Fn(u16, Vec<u8>) -> std::result::Result<(u64, u64), String> + Send + Sync;
use crate::control::state::SharedState;
use crate::types::{ReadConsistency, RequestId, TenantId, VShardId};

// ── Replicated write envelope ───────────────────────────────────────

/// A write operation serialized for Raft replication.
///
/// Mirrors the write variants of [`PhysicalPlan`] but uses only types that
/// are trivially serializable (no `Arc`, no `Instant`).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum ReplicatedWrite {
    PointPut {
        collection: String,
        document_id: String,
        value: Vec<u8>,
    },
    PointDelete {
        collection: String,
        document_id: String,
    },
    PointUpdate {
        collection: String,
        document_id: String,
        updates: Vec<(String, Vec<u8>)>,
    },
    VectorInsert {
        collection: String,
        vector: Vec<f32>,
        dim: usize,
    },
    VectorBatchInsert {
        collection: String,
        vectors: Vec<Vec<f32>>,
        dim: usize,
    },
    VectorDelete {
        collection: String,
        vector_id: u32,
    },
    SetVectorParams {
        collection: String,
        m: usize,
        ef_construction: usize,
        metric: String,
    },
    CrdtApply {
        collection: String,
        document_id: String,
        delta: Vec<u8>,
        peer_id: u64,
    },
    EdgePut {
        src_id: String,
        label: String,
        dst_id: String,
        properties: Vec<u8>,
    },
    EdgeDelete {
        src_id: String,
        label: String,
        dst_id: String,
    },
}

/// Metadata carried alongside the write for routing on the receiving node.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ReplicatedEntry {
    pub tenant_id: u32,
    pub vshard_id: u16,
    pub write: ReplicatedWrite,
}

impl ReplicatedEntry {
    /// Serialize to bytes for Raft log entry data.
    pub fn to_bytes(&self) -> Vec<u8> {
        rmp_serde::to_vec(self).expect("ReplicatedEntry serialization cannot fail")
    }

    /// Deserialize from Raft log entry data bytes.
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        rmp_serde::from_slice(data).ok()
    }
}

/// Try to convert a PhysicalPlan write variant into a ReplicatedEntry.
///
/// Returns `None` for read operations or system commands.
pub fn to_replicated_entry(
    tenant_id: TenantId,
    vshard_id: VShardId,
    plan: &PhysicalPlan,
) -> Option<ReplicatedEntry> {
    let write = match plan {
        PhysicalPlan::PointPut {
            collection,
            document_id,
            value,
        } => ReplicatedWrite::PointPut {
            collection: collection.clone(),
            document_id: document_id.clone(),
            value: value.clone(),
        },
        PhysicalPlan::PointDelete {
            collection,
            document_id,
        } => ReplicatedWrite::PointDelete {
            collection: collection.clone(),
            document_id: document_id.clone(),
        },
        PhysicalPlan::PointUpdate {
            collection,
            document_id,
            updates,
        } => ReplicatedWrite::PointUpdate {
            collection: collection.clone(),
            document_id: document_id.clone(),
            updates: updates.clone(),
        },
        PhysicalPlan::VectorInsert {
            collection,
            vector,
            dim,
            field_name: _,
        } => ReplicatedWrite::VectorInsert {
            collection: collection.clone(),
            vector: vector.clone(),
            dim: *dim,
        },
        PhysicalPlan::VectorBatchInsert {
            collection,
            vectors,
            dim,
        } => ReplicatedWrite::VectorBatchInsert {
            collection: collection.clone(),
            vectors: vectors.clone(),
            dim: *dim,
        },
        PhysicalPlan::VectorDelete {
            collection,
            vector_id,
        } => ReplicatedWrite::VectorDelete {
            collection: collection.clone(),
            vector_id: *vector_id,
        },
        PhysicalPlan::SetVectorParams {
            collection,
            m,
            ef_construction,
            metric,
        } => ReplicatedWrite::SetVectorParams {
            collection: collection.clone(),
            m: *m,
            ef_construction: *ef_construction,
            metric: metric.clone(),
        },
        PhysicalPlan::CrdtApply {
            collection,
            document_id,
            delta,
            peer_id,
            mutation_id: _,
        } => ReplicatedWrite::CrdtApply {
            collection: collection.clone(),
            document_id: document_id.clone(),
            delta: delta.clone(),
            peer_id: *peer_id,
        },
        PhysicalPlan::EdgePut {
            src_id,
            label,
            dst_id,
            properties,
        } => ReplicatedWrite::EdgePut {
            src_id: src_id.clone(),
            label: label.clone(),
            dst_id: dst_id.clone(),
            properties: properties.clone(),
        },
        PhysicalPlan::EdgeDelete {
            src_id,
            label,
            dst_id,
        } => ReplicatedWrite::EdgeDelete {
            src_id: src_id.clone(),
            label: label.clone(),
            dst_id: dst_id.clone(),
        },
        // Not a write — reads, system ops, etc.
        _ => return None,
    };

    Some(ReplicatedEntry {
        tenant_id: tenant_id.as_u32(),
        vshard_id: vshard_id.as_u16(),
        write,
    })
}

/// Convert a ReplicatedWrite back into a PhysicalPlan for Data Plane execution.
fn to_physical_plan(write: &ReplicatedWrite) -> PhysicalPlan {
    match write {
        ReplicatedWrite::PointPut {
            collection,
            document_id,
            value,
        } => PhysicalPlan::PointPut {
            collection: collection.clone(),
            document_id: document_id.clone(),
            value: value.clone(),
        },
        ReplicatedWrite::PointDelete {
            collection,
            document_id,
        } => PhysicalPlan::PointDelete {
            collection: collection.clone(),
            document_id: document_id.clone(),
        },
        ReplicatedWrite::PointUpdate {
            collection,
            document_id,
            updates,
        } => PhysicalPlan::PointUpdate {
            collection: collection.clone(),
            document_id: document_id.clone(),
            updates: updates.clone(),
        },
        ReplicatedWrite::VectorInsert {
            collection,
            vector,
            dim,
        } => PhysicalPlan::VectorInsert {
            collection: collection.clone(),
            vector: vector.clone(),
            dim: *dim,
            field_name: String::new(),
        },
        ReplicatedWrite::VectorBatchInsert {
            collection,
            vectors,
            dim,
        } => PhysicalPlan::VectorBatchInsert {
            collection: collection.clone(),
            vectors: vectors.clone(),
            dim: *dim,
        },
        ReplicatedWrite::VectorDelete {
            collection,
            vector_id,
        } => PhysicalPlan::VectorDelete {
            collection: collection.clone(),
            vector_id: *vector_id,
        },
        ReplicatedWrite::SetVectorParams {
            collection,
            m,
            ef_construction,
            metric,
        } => PhysicalPlan::SetVectorParams {
            collection: collection.clone(),
            m: *m,
            ef_construction: *ef_construction,
            metric: metric.clone(),
        },
        ReplicatedWrite::CrdtApply {
            collection,
            document_id,
            delta,
            peer_id,
        } => PhysicalPlan::CrdtApply {
            collection: collection.clone(),
            document_id: document_id.clone(),
            delta: delta.clone(),
            peer_id: *peer_id,
            mutation_id: 0,
        },
        ReplicatedWrite::EdgePut {
            src_id,
            label,
            dst_id,
            properties,
        } => PhysicalPlan::EdgePut {
            src_id: src_id.clone(),
            label: label.clone(),
            dst_id: dst_id.clone(),
            properties: properties.clone(),
        },
        ReplicatedWrite::EdgeDelete {
            src_id,
            label,
            dst_id,
        } => PhysicalPlan::EdgeDelete {
            src_id: src_id.clone(),
            label: label.clone(),
            dst_id: dst_id.clone(),
        },
    }
}

// ── Propose tracker ─────────────────────────────────────────────────

/// Response payload sent back to the proposer after commit + execution.
pub type ProposeResult = std::result::Result<Vec<u8>, String>;

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
    group_id: u64,
    entries: Vec<LogEntry>,
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
            let replicated = match ReplicatedEntry::from_bytes(&entry.data) {
                Some(r) => r,
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

            let plan = to_physical_plan(&replicated.write);
            let tenant_id = TenantId::new(replicated.tenant_id);
            let vshard_id = VShardId::new(replicated.vshard_id);
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
            };

            let rx = state.tracker.register(request_id);

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
                    Err(format!("dispatch failed: {e}")),
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
                        tracker.complete(batch.group_id, entry.index, Err(err_msg));
                    } else {
                        tracker.complete(batch.group_id, entry.index, Ok(payload));
                    }
                }
                Ok(Err(_)) => {
                    tracker.complete(
                        batch.group_id,
                        entry.index,
                        Err("response channel closed".into()),
                    );
                }
                Err(_) => {
                    tracker.complete(
                        batch.group_id,
                        entry.index,
                        Err("deadline exceeded applying committed write".into()),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replicated_entry_roundtrip() {
        let entry = ReplicatedEntry {
            tenant_id: 1,
            vshard_id: 42,
            write: ReplicatedWrite::PointPut {
                collection: "users".into(),
                document_id: "u1".into(),
                value: b"alice".to_vec(),
            },
        };

        let bytes = entry.to_bytes();
        let decoded = ReplicatedEntry::from_bytes(&bytes).unwrap();
        assert_eq!(decoded.tenant_id, 1);
        assert_eq!(decoded.vshard_id, 42);
        match decoded.write {
            ReplicatedWrite::PointPut {
                collection,
                document_id,
                value,
            } => {
                assert_eq!(collection, "users");
                assert_eq!(document_id, "u1");
                assert_eq!(value, b"alice");
            }
            other => panic!("expected PointPut, got {other:?}"),
        }
    }

    #[test]
    fn all_write_variants_serialize() {
        let writes = vec![
            ReplicatedWrite::PointPut {
                collection: "c".into(),
                document_id: "d".into(),
                value: vec![1, 2, 3],
            },
            ReplicatedWrite::PointDelete {
                collection: "c".into(),
                document_id: "d".into(),
            },
            ReplicatedWrite::VectorInsert {
                collection: "v".into(),
                vector: vec![1.0, 2.0, 3.0],
                dim: 3,
            },
            ReplicatedWrite::CrdtApply {
                collection: "c".into(),
                document_id: "d".into(),
                delta: vec![0xAB],
                peer_id: 7,
            },
            ReplicatedWrite::EdgePut {
                src_id: "a".into(),
                label: "knows".into(),
                dst_id: "b".into(),
                properties: vec![],
            },
            ReplicatedWrite::EdgeDelete {
                src_id: "a".into(),
                label: "knows".into(),
                dst_id: "b".into(),
            },
        ];

        for write in writes {
            let entry = ReplicatedEntry {
                tenant_id: 1,
                vshard_id: 0,
                write,
            };
            let bytes = entry.to_bytes();
            let decoded = ReplicatedEntry::from_bytes(&bytes);
            assert!(decoded.is_some(), "failed to roundtrip: {entry:?}");
        }
    }

    #[test]
    fn propose_tracker_register_and_complete() {
        let tracker = ProposeTracker::new();
        let mut rx = tracker.register(1, 5);

        // Complete the waiter.
        assert!(tracker.complete(1, 5, Ok(b"result".to_vec())));

        // Should receive the result.
        let result = rx.try_recv().unwrap();
        assert_eq!(result.unwrap(), b"result");
    }

    #[test]
    fn propose_tracker_no_waiter_returns_false() {
        let tracker = ProposeTracker::new();
        assert!(!tracker.complete(1, 99, Ok(vec![])));
    }

    #[test]
    fn to_replicated_entry_writes_only() {
        let tenant = TenantId::new(1);
        let vshard = VShardId::new(0);

        // Write — should produce Some.
        let plan = PhysicalPlan::PointPut {
            collection: "c".into(),
            document_id: "d".into(),
            value: vec![],
        };
        assert!(to_replicated_entry(tenant, vshard, &plan).is_some());

        // Read — should produce None.
        let plan = PhysicalPlan::PointGet {
            collection: "c".into(),
            document_id: "d".into(),
        };
        assert!(to_replicated_entry(tenant, vshard, &plan).is_none());
    }
}
