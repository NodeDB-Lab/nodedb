//! CommitApplier for committed data-group Raft entries.
//!
//! When a Raft entry is committed across the quorum, the leader's
//! applier dispatches the write to the Data Plane via the same SPSC
//! path used for single-node writes. The entry data is a serialized
//! `ReplicatedEntry`.

use std::sync::Arc;

use crate::control::state::SharedState;

/// Dispatches committed data-group Raft entries to the SPSC bridge.
pub struct SpscCommitApplier {
    pub(super) shared: Arc<SharedState>,
}

impl SpscCommitApplier {
    pub fn new(shared: Arc<SharedState>) -> Self {
        Self { shared }
    }
}

impl nodedb_cluster::CommitApplier for SpscCommitApplier {
    fn apply_committed(&self, _group_id: u64, entries: &[nodedb_raft::message::LogEntry]) -> u64 {
        let mut last_applied = 0u64;

        for entry in entries {
            if entry.data.is_empty() {
                // No-op entry (leader election marker) — nothing to apply.
                last_applied = entry.index;
                continue;
            }

            match crate::control::wal_replication::from_replicated_entry(&entry.data) {
                Some((tenant_id, vshard_id, plan)) => {
                    let request = crate::bridge::envelope::Request {
                        request_id: crate::types::RequestId::new(entry.index),
                        tenant_id,
                        vshard_id,
                        plan,
                        deadline: std::time::Instant::now() + std::time::Duration::from_secs(30),
                        priority: crate::bridge::envelope::Priority::Normal,
                        trace_id: 0,
                        consistency: crate::types::ReadConsistency::Strong,
                        idempotency_key: Some(entry.index),
                        event_source: crate::event::EventSource::RaftFollower,
                        user_roles: Vec::new(),
                    };

                    match self.shared.dispatcher.lock() {
                        Ok(mut d) => {
                            if let Err(e) = d.dispatch(request) {
                                tracing::warn!(
                                    index = entry.index,
                                    error = %e,
                                    "failed to dispatch committed entry to data plane"
                                );
                            }
                        }
                        Err(p) => {
                            if let Err(e) = p.into_inner().dispatch(request) {
                                tracing::warn!(
                                    index = entry.index,
                                    error = %e,
                                    "failed to dispatch committed entry (poisoned lock)"
                                );
                            }
                        }
                    }
                }
                None => {
                    // ConfChange or unrecognized entry — skip (ConfChanges are
                    // handled by the RaftLoop before calling the applier).
                    tracing::debug!(
                        index = entry.index,
                        data_len = entry.data.len(),
                        "skipping non-data entry"
                    );
                }
            }

            last_applied = entry.index;
        }

        last_applied
    }
}
