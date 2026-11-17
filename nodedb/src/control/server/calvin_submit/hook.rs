// SPDX-License-Identifier: BUSL-1.1

//! `RegistryCalvinSubmit` — bridges the cluster `SubmitCalvinTxn` trigger to a
//! node-local Calvin submit-and-await.
//!
//! `nodedb-cluster` cannot depend on `nodedb` (circular), so the submit logic
//! lives here and is exposed to the transport via the
//! [`nodedb_cluster::CalvinSubmit`] hook. The `RaftLoop` is built
//! `with_calvin_submit(Arc::new(RegistryCalvinSubmit { .. }))`.
//!
//! # What it does
//!
//! This handler only ever runs on the SEQUENCER-GROUP leader (the coordinator
//! routes the `SubmitCalvinTxnRequest` there precisely so the submit lands where
//! the sequencer service assigns and the local registry receives the replicated
//! completion ack). On `on_submit_calvin_txn`, the leader:
//! 1. decodes `tx_class_bytes` (msgpack) into a `TxClass` and re-derives its
//!    cached participating-vshard set via `restore_derived`;
//! 2. submits it to this node's Calvin sequencer inbox and awaits assignment +
//!    completion through `submit_and_await_calvin_until`, ending at the
//!    deadline the forwarded `deadline_remaining_ms` names, so it cannot
//!    outlive the coordinator's remaining budget. A spent budget is refused
//!    unsubmitted, a definite refusal;
//! 3. maps `Ok(())` → `error: None` and `Err` → a typed cluster error that
//!    keeps the Data-Plane verdict's code — never a silent drop.
//!
//! # Plane discipline
//!
//! This runs on the leader's Control Plane (the Tokio transport reactor). The
//! submit-and-await blocks only on the assignment / completion oneshot channels;
//! the transaction execution itself happens on the Data Plane via the sequencer
//! service / per-vshard schedulers. This hook never touches storage I/O or
//! io_uring directly.

use std::sync::Arc;

use nodedb_cluster::calvin::types::TxClass;
use nodedb_cluster::{SubmitCalvinTxnRequest, SubmitCalvinTxnResponse, TypedClusterError};

use crate::control::cluster::calvin::refusal::{RefusedSequences, encode_refused_sequences};
use crate::control::cluster::data_plane_error_wire::execution_error_to_typed;
use crate::control::cluster::forwarded_budget::hook_deadline;
use crate::control::planner::calvin::refused_sequences::collecting;
use crate::control::planner::calvin::submit_and_await_calvin_until;
use crate::control::state::SharedState;

/// `nodedb`-side implementation of [`nodedb_cluster::CalvinSubmit`].
///
/// Holds the node's [`SharedState`] so it can reach the Calvin sequencer inbox
/// and completion registry. The coordinator only routes here when this node is
/// the sequencer-group leader, so the submit-and-await is the one that actually
/// completes.
pub struct RegistryCalvinSubmit {
    /// Shared node state — the source of the local sequencer inbox + registry.
    state: Arc<SharedState>,
}

impl RegistryCalvinSubmit {
    /// Build a Calvin-submit hook over `state`.
    pub fn new(state: Arc<SharedState>) -> Self {
        Self { state }
    }
}

#[async_trait::async_trait]
impl nodedb_cluster::CalvinSubmit for RegistryCalvinSubmit {
    async fn on_submit_calvin_txn(&self, req: SubmitCalvinTxnRequest) -> SubmitCalvinTxnResponse {
        // A spent budget is refused before any work: nothing is submitted.
        let deadline = match hook_deadline(req.deadline_remaining_ms) {
            Ok(deadline) => deadline,
            Err(refusal) => {
                return SubmitCalvinTxnResponse {
                    error: Some(refusal),
                    payload_bytes: None,
                    refused_sequences: None,
                };
            }
        };

        let mut tx_class: TxClass = match zerompk::from_msgpack(&req.tx_class_bytes) {
            Ok(tc) => tc,
            Err(e) => {
                return SubmitCalvinTxnResponse {
                    error: Some(TypedClusterError::Internal {
                        code: 0,
                        message: format!("calvin-submit: failed to decode TxClass: {e}"),
                    }),
                    payload_bytes: None,
                    refused_sequences: None,
                };
            }
        };
        // Re-derive the participating-vshard set skipped during serialization
        // (the wire bytes carry only the read/write sets).
        if let Err(e) = tx_class.restore_derived() {
            return SubmitCalvinTxnResponse {
                error: Some(TypedClusterError::Internal {
                    code: 0,
                    message: format!("calvin-submit: TxClass participants underivable: {e}"),
                }),
                payload_bytes: None,
                refused_sequences: None,
            };
        }

        // This leader runs no statement of the writer's, so it collects the
        // sequence results a refused `RETURNING` evaluation drew, and the
        // reply carries them to the coordinator.
        let (outcome, refused) = collecting(submit_and_await_calvin_until(
            &self.state,
            tx_class,
            deadline,
        ))
        .await;
        match outcome {
            // Forward the applied RETURNING payload (drained from this leader's
            // local sidecar) back to the remote coordinator over the non-Raft
            // RPC response; `None` for a plain write with no rows to surface.
            Ok(applied) => SubmitCalvinTxnResponse {
                error: None,
                payload_bytes: applied.map(|r| r.payload.to_vec()),
                refused_sequences: None,
            },
            // The awaited apply verdict is a Data-Plane verdict: carry its
            // code so the coordinator renders the SQLSTATE a local write
            // renders, instead of an uncoded internal error.
            Err(e) => SubmitCalvinTxnResponse {
                error: Some(execution_error_to_typed(e)),
                payload_bytes: None,
                refused_sequences: refused.as_ref().and_then(encoded_sequences),
            },
        }
    }
}

/// The wire form of `sequences`. Results with no wire form are logged and
/// dropped: the error still reaches the coordinator.
fn encoded_sequences(sequences: &RefusedSequences) -> Option<Vec<u8>> {
    match encode_refused_sequences(sequences) {
        Ok(bytes) => Some(bytes),
        Err(error) => {
            tracing::warn!(
                %error,
                "calvin-submit: the sequence results of a refusal have no wire form"
            );
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use nodedb_cluster::CalvinSubmit;

    use super::*;
    use crate::bridge::dispatch::Dispatcher;
    use crate::bridge::envelope::ErrorCode;
    use crate::wal::WalManager;

    /// A forward with a spent budget is refused unsubmitted. The coordinator
    /// sees a definite refusal, never a 1 ms wait that ends unknown.
    #[tokio::test]
    async fn a_spent_budget_is_refused_before_the_submit() {
        let dir = tempfile::tempdir().unwrap();
        let wal = Arc::new(WalManager::open_for_testing(&dir.path().join("test.wal")).unwrap());
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).unwrap();

        let reply = RegistryCalvinSubmit::new(state)
            .on_submit_calvin_txn(SubmitCalvinTxnRequest {
                tx_class_bytes: Vec::new(),
                deadline_remaining_ms: 0,
                trace_id: [0u8; 16],
            })
            .await;
        let refusal = reply.error.expect("a spent budget is refused");
        assert!(
            matches!(
                crate::Error::from(refusal),
                crate::Error::DataPlane(ErrorCode::ExpiredBeforeExecution)
            ),
            "a spent budget must come back as ExpiredBeforeExecution"
        );
        assert!(reply.payload_bytes.is_none());
    }
}
