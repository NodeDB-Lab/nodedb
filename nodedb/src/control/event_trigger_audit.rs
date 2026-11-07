// SPDX-License-Identifier: BUSL-1.1

//! Audit rows of a DEFINE EVENT THEN action.
//!
//! An action runs on the Event Plane, which does no storage I/O.
//! So both rows go through the Control Plane recorder, which does the WAL work.

use std::sync::Arc;

use tracing::error;

use crate::control::event_action_error::TriggerActionError;
use crate::control::security::audit::{AuditEvent, PendingAudit};
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId};

/// Record the action's intent, durable before the action runs.
///
/// A failed record refuses the action. It applied nothing, so the retry queue runs it again.
/// Each run writes one intent row, so a retry writes another.
pub(super) async fn record_action_intent(
    shared: &Arc<SharedState>,
    database_id: DatabaseId,
    tenant_id: TenantId,
    trigger_name: &str,
    sql: &str,
) -> Result<(), TriggerActionError> {
    let intent = PendingAudit {
        event: AuditEvent::ActionIntent,
        tenant_id: Some(tenant_id),
        database_id: Some(database_id),
        source: "event_trigger".to_owned(),
        detail: format!("event '{trigger_name}' runs: {sql}"),
    };
    shared
        .audit_recorder()
        .record(vec![intent])
        .await
        .map_err(|failure| TriggerActionError::AuditIntent {
            source: crate::Error::Storage {
                engine: "audit_log".into(),
                detail: format!("the record of event '{trigger_name}' is not durable: {failure:?}"),
            },
        })
}

/// Record the action's result. This row is best-effort: the intent row is the durable one.
pub(super) async fn record_action_result(
    shared: &Arc<SharedState>,
    database_id: DatabaseId,
    tenant_id: TenantId,
    source: &str,
    detail: String,
) {
    let row = PendingAudit {
        event: AuditEvent::AdminAction,
        tenant_id: Some(tenant_id),
        database_id: Some(database_id),
        source: source.to_owned(),
        detail,
    };
    if let Err(failure) = shared.audit_recorder().record(vec![row]).await {
        error!(?failure, source, "event action result row is not durable");
    }
}
