// SPDX-License-Identifier: BUSL-1.1

//! Sync dispatch that returns raw payload bytes, used by the CRDT delta path.

use crate::bridge::envelope::PhysicalPlan;
use crate::control::server::dispatch_utils::RecordOwner;
use crate::control::server::shared::authorization::AuthorizedTask;
use crate::control::server::shared::session::StatementDeadline;
use crate::control::state::SharedState;
use crate::event::EventSource;

use super::admission_guard::reject_unadmitted_crdt_apply;
use super::outcome::SyncDispatchOutcome;
use super::propose::propose_sync_plan;

/// Dispatch a sync write and return the apply payload plus what CRDT admission
/// measured about the delta. A CRDT apply runs its admission. Every other
/// plan is proposed through Raft.
pub async fn dispatch_sync_bytes(
    state: &SharedState,
    collection: &str,
    authorized: AuthorizedTask,
    deadline: StatementDeadline,
    event_source: EventSource,
    policy: &dyn crate::control::crdt_admission::CrdtPostImagePolicy,
) -> crate::Result<SyncDispatchOutcome> {
    // Sync inbound envelope carries no session database, so scoped to the default database.
    if matches!(
        authorized.plan(),
        PhysicalPlan::Crdt(
            nodedb_physical::physical_plan::CrdtOp::Apply { .. }
                | nodedb_physical::physical_plan::CrdtOp::ApplyAuthenticated { .. }
        )
    ) {
        let outcome =
            crate::control::crdt_admission::dispatch_authorized_crdt_apply_admitted_outcome(
                state,
                crate::control::crdt_admission::AuthorizedCrdtApplyAdmissionRequest {
                    authorized,
                    collection,
                    deadline,
                    event_source,
                    policy,
                },
            )
            .await?;
        return Ok(SyncDispatchOutcome {
            payload: outcome.payload,
            trimmed_ops: outcome.trimmed_ops,
        });
    }
    // The Raft entry's apply owns durability.
    dispatch_write_replicated(state, collection, authorized, deadline, event_source)
        .await
        .map(SyncDispatchOutcome::untrimmed)
}

/// Dispatch a write so it is quorum-durable: propose it through Raft and
/// block until it applied locally. The entry's apply appends the write's redo
/// record on every replica.
pub(crate) async fn dispatch_write_replicated(
    state: &SharedState,
    collection: &str,
    authorized: AuthorizedTask,
    deadline: StatementDeadline,
    event_source: EventSource,
) -> crate::Result<Vec<u8>> {
    let user_roles = authorized.identity().role_names();
    let task = authorized.into_physical_task();
    let tenant_id = task.tenant_id;
    let database_id = task.database_id;
    let vshard_id = task.vshard_id;
    let plan = task.plan;
    let owner = RecordOwner {
        tenant_id,
        database_id,
        vshard_id,
    };
    reject_unadmitted_crdt_apply(&plan)?;
    if vshard_id != nodedb_types::CollectionKey::from_bare(database_id, collection).vshard() {
        return Err(crate::Error::Internal {
            detail: "authorized sync task vShard does not match collection".into(),
        });
    }
    // The lease gates a sync write on a drained collection like any other
    // write, and lives until the write's outcome.
    let _lease = crate::control::server::shared::clone_write::write_lease(
        state,
        tenant_id,
        database_id,
        &plan,
    )
    .await?;
    // The apply's write funnel records the write's mark on every replica.
    propose_sync_plan(state, owner, plan, event_source, deadline, user_roles).await
}

#[cfg(test)]
mod tests {
    use super::super::durability_test_support::{
        COLLECTION, applying_proposer, authorized_write, fixture,
    };
    use super::dispatch_write_replicated;
    use crate::control::server::shared::session::StatementDeadline;
    use crate::event::EventSource;

    /// A proposed write answers with the applied entry's payload.
    #[tokio::test]
    async fn a_proposed_write_answers_with_the_applied_payload() {
        let (state, _side, _directory) = fixture();
        crate::control::vshard_admission::install_async_raft_proposer(
            &state,
            crate::control::vshard_admission::applying_submit(applying_proposer()),
        )
        .expect("install proposer");
        let authorized = authorized_write(&state);

        let payload = dispatch_write_replicated(
            &state,
            COLLECTION,
            authorized,
            StatementDeadline::of_statement(&state),
            EventSource::CrdtSync,
        )
        .await
        .expect("the proposal applies");

        assert_eq!(payload, b"applied".to_vec());
    }

    /// A state `start_raft` never ran on has no proposer, so the write is
    /// refused.
    #[tokio::test]
    async fn a_state_without_a_proposer_refuses_the_write() {
        let (state, _side, _directory) = fixture();
        let authorized = authorized_write(&state);

        let result = dispatch_write_replicated(
            &state,
            COLLECTION,
            authorized,
            StatementDeadline::of_statement(&state),
            EventSource::CrdtSync,
        )
        .await;

        assert!(
            matches!(result, Err(crate::Error::Internal { .. })),
            "got {result:?}"
        );
    }
}
