// SPDX-License-Identifier: BUSL-1.1

//! Sync dispatch that returns a full [`Response`].
//!
//! Used by the columnar, timeseries, FTS, spatial, and vector sync handlers,
//! which need the raw `Response` to extract the payload themselves.

use crate::bridge::envelope::{PhysicalPlan, Response, Status};
use crate::control::server::dispatch_utils::RecordOwner;
use crate::control::server::shared::authorization::AuthorizedTask;
use crate::control::state::SharedState;
use crate::event::EventSource;
use crate::types::{DatabaseId, Lsn, TenantId, VShardId};

use super::admission_guard::reject_unadmitted_crdt_apply;
use super::propose::propose_sync_plan;

/// Trusted-internal sync-shaped dispatch. The entry's apply appends the
/// write's records on every replica.
pub(crate) async fn dispatch_trusted_internal_sync_response(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    vshard_id: VShardId,
    plan: PhysicalPlan,
    event_source: EventSource,
) -> crate::Result<Response> {
    let owner = RecordOwner {
        tenant_id,
        database_id,
        vshard_id,
    };
    let payload = sync_write(state, owner, &plan, event_source).await?;
    Ok(ok_response(state, payload))
}

/// Sync-path convenience: proposes `authorized` tagged
/// [`EventSource::CrdtSync`] and returns the payload bytes.
pub(crate) async fn dispatch_sync_payload(
    state: &SharedState,
    authorized: AuthorizedTask,
) -> crate::Result<Vec<u8>> {
    let task = authorized.into_physical_task();
    let owner = RecordOwner {
        tenant_id: task.tenant_id,
        database_id: task.database_id,
        vshard_id: task.vshard_id,
    };
    sync_write(state, owner, &task.plan, EventSource::CrdtSync).await
}

/// Refuse an unadmitted CRDT apply, then propose `plan`. The gate verdict
/// travels in the returned payload.
async fn sync_write(
    state: &SharedState,
    owner: RecordOwner,
    plan: &PhysicalPlan,
    event_source: EventSource,
) -> crate::Result<Vec<u8>> {
    reject_unadmitted_crdt_apply(plan)?;
    propose_sync_plan(state, owner, plan, event_source).await
}

/// The applied payload wrapped in `Status::Ok`. A non-`Ok` status means a
/// protocol error, not a gate rejection: the gate verdict is in the payload.
fn ok_response(state: &SharedState, payload: Vec<u8>) -> Response {
    Response {
        request_id: state.next_request_id(),
        status: Status::Ok,
        attempt: 1,
        partial: false,
        payload: payload.into(),
        watermark_lsn: Lsn::ZERO,
        error_code: None,
        stage_vote: None,
        read_version_lsn: Lsn::ZERO,
        write_set: Vec::new(),
    }
}

/// Build the loud error every `NoOp*Dispatcher` returns when a sync op reaches
/// a path lacking `SharedState` — such a path will ACK the client while
/// silently dropping the write. `op` names the operation for the diagnostic.
pub fn noop_dispatch_error(op: &str) -> crate::Error {
    crate::Error::Internal {
        detail: format!(
            "{op} routed through path lacking SharedState; \
             check listener wiring — {op} was ACKed but NOT applied"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::super::durability_test_support::{applying_proposer, authorized_write, fixture};
    use super::dispatch_sync_payload;

    /// The payload a peer is acked off is the applied entry's payload.
    #[tokio::test]
    async fn the_payload_is_the_applied_entrys() {
        let (state, _side, _directory) = fixture();
        crate::control::vshard_admission::install_async_raft_proposer(
            &state,
            crate::control::vshard_admission::applying_submit(applying_proposer()),
        )
        .expect("install proposer");
        let authorized = authorized_write(&state);

        let payload = dispatch_sync_payload(&state, authorized)
            .await
            .expect("the proposal applies");

        assert_eq!(payload, b"applied".to_vec());
    }

    /// A state `start_raft` never ran on refuses the write.
    #[tokio::test]
    async fn a_state_without_a_proposer_refuses_the_write() {
        let (state, _side, _directory) = fixture();
        let authorized = authorized_write(&state);

        let result = dispatch_sync_payload(&state, authorized).await;

        assert!(
            matches!(result, Err(crate::Error::Internal { .. })),
            "got {result:?}"
        );
    }
}
