// SPDX-License-Identifier: BUSL-1.1

//! `SyncSession::handle_row_push_reject`: a Lite peer refused a row push.
//!
//! Origin does not re-send a row push, so a refused row never reaches the
//! peer on its own. The refusal goes to the sync dead-letter queue with the
//! peer's reason, the way a refused delta does, so an operator can inspect
//! it and repair the peer.

use std::sync::Arc;

use tracing::{error, warn};

use crate::control::state::SharedState;

use super::super::dlq::{DlqEnqueueParams, ViolationType};
use super::super::wire::{RowPushRefusal, RowPushRejectMsg};
use super::state::SyncSession;

impl SyncSession {
    /// Record a row push the peer refused.
    ///
    /// Without `SharedState` there is no dead-letter queue to hold it, so
    /// the refusal is only logged.
    pub fn handle_row_push_reject(
        &mut self,
        msg: &RowPushRejectMsg,
        shared: Option<&Arc<SharedState>>,
    ) {
        // Origin sends row pushes only to authenticated sessions, so an
        // unauthenticated refusal names no row this session received.
        if !self.authenticated {
            warn!(
                session = %self.session_id,
                collection = %msg.collection,
                "row push refusal from an unauthenticated session ignored"
            );
            return;
        }
        error!(
            session = %self.session_id,
            collection = %msg.collection,
            document_id = %msg.document_id,
            sequence = msg.sequence,
            refusal = %msg.refusal,
            "sync peer refused a row push; the row is not on that peer"
        );
        let Some(shared) = shared else {
            return;
        };
        let entry = self.row_push_reject_entry(msg);
        let mut dlq = shared.sync_dlq.lock().unwrap_or_else(|p| p.into_inner());
        dlq.enqueue(entry);
    }

    /// The dead-letter entry for a refused row push.
    fn row_push_reject_entry(&self, msg: &RowPushRejectMsg) -> DlqEnqueueParams {
        let violation_type = match &msg.refusal {
            RowPushRefusal::Malformed { detail } => ViolationType::MalformedDelta {
                detail: detail.clone(),
            },
            RowPushRefusal::ApplyFailed { detail } => ViolationType::ConstraintViolation {
                detail: detail.clone(),
            },
        };
        DlqEnqueueParams {
            session_id: self.session_id.clone(),
            tenant_id: self.tenant_id.map(|t| t.as_u64()).unwrap_or(0),
            username: self.username.clone().unwrap_or_default(),
            collection: msg.collection.clone(),
            document_id: msg.document_id.clone(),
            mutation_id: msg.sequence,
            peer_id: msg.peer_id,
            delta: Vec::new(),
            violation_type,
            compensation: None,
            device_metadata: self.device_metadata.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reject(refusal: RowPushRefusal) -> RowPushRejectMsg {
        RowPushRejectMsg {
            collection: "cfg".into(),
            document_id: "k1".into(),
            sequence: 3,
            peer_id: 9,
            refusal,
        }
    }

    #[test]
    fn a_malformed_row_is_dead_lettered_as_a_malformed_delta() {
        let session = SyncSession::new("row-push-reject".to_string());
        let entry = session.row_push_reject_entry(&reject(RowPushRefusal::Malformed {
            detail: "not a row map".into(),
        }));
        assert_eq!(entry.collection, "cfg");
        assert_eq!(entry.document_id, "k1");
        assert_eq!(entry.mutation_id, 3);
        assert_eq!(entry.peer_id, 9);
        assert_eq!(
            entry.violation_type,
            ViolationType::MalformedDelta {
                detail: "not a row map".into()
            }
        );
    }

    #[test]
    fn a_failed_apply_is_dead_lettered_as_a_constraint_violation() {
        let session = SyncSession::new("row-push-reject".to_string());
        let entry = session.row_push_reject_entry(&reject(RowPushRefusal::ApplyFailed {
            detail: "storage full".into(),
        }));
        assert_eq!(
            entry.violation_type,
            ViolationType::ConstraintViolation {
                detail: "storage full".into()
            }
        );
    }
}
