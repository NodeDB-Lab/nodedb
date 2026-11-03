// SPDX-License-Identifier: BUSL-1.1

//! Single-write dispatch for the protocol-neutral collection DML: a write
//! plan on the statement's transaction, the staging requests a transaction
//! sends, and the `RETURNING` rows a write answers with.

use std::sync::Arc;

use crate::bridge::envelope::{PhysicalPlan, Response};
use crate::control::security::audit::ArcAuditEmitter;
use crate::control::security::identity::{AuthenticatedIdentity, Permission};
use crate::control::sequence::SessionSequenceAccess;
use crate::control::server::pgwire::types::error_to_sqlstate;
use crate::control::server::response_shape::compose::shape_returning_materialized;
use crate::control::server::response_shape::redaction::QueryRedaction;
use crate::control::server::response_shape::request::MaterializedShapeRequest;
use crate::control::server::response_shape::schema::OutputSchema;
use crate::control::server::response_shape::types::{PlanKind, ShapedRows};
use crate::control::server::shared::authorization::{AuthorizationError, authorize_collection};
use crate::control::server::shared::clone_write::{
    CloneCheckedOutcome, InterceptAndAuthorizeParams, intercept_and_authorize, write_lease,
};
use crate::control::server::shared::ddl::result::{DdlError, DdlResult};
use crate::control::server::shared::ddl::sqlstate::error_code_to_sqlstate;
use crate::control::server::shared::session::{
    DmlTxnCtx, InTxnRoute, StagingGateError, TransactionState, route_in_tx_write,
};
use crate::control::state::SharedState;
use crate::types::TraceId;

use super::types::ddl_err;

/// Dispatch a write plan on the statement's transaction `txn_ctx`: inside a
/// transaction block it stages or buffers there and commits with the
/// statement, outside one it takes the durable route. Returns an error
/// response on failure. `None` means the write applied or joined the
/// transaction.
pub(in crate::control::server::shared::ddl::neutral::collection) async fn dispatch_plan(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: crate::types::DatabaseId,
    vshard_id: crate::types::VShardId,
    plan: PhysicalPlan,
    txn_ctx: &DmlTxnCtx<'_>,
) -> Option<Result<Vec<DdlResult>, DdlError>> {
    let task = nodedb_physical::physical_task::PhysicalTask {
        tenant_id: identity.tenant_id,
        database_id,
        vshard_id,
        plan,
        post_set_op: nodedb_physical::physical_task::PostSetOp::None,
        txn_id: None,
    };
    let sessions = txn_ctx.sessions;
    let session_id = txn_ctx.session_id;
    if sessions.transaction_state(session_id) == TransactionState::InBlock {
        let lease = match write_lease(state, task.tenant_id, database_id, &task.plan).await {
            Ok(lease) => Arc::new(lease),
            Err(error) => return Some(Err(error_to_ddl(&error))),
        };
        let buffer_start = sessions.buffered_task_count(session_id);
        let routed = route_in_tx_write(state, sessions, session_id, task, |staged| {
            dispatch_staged(state, identity, staged)
        })
        .await;
        if sessions.buffered_task_count(session_id) > buffer_start
            && !sessions.attach_tx_lease_scope_since(session_id, buffer_start, lease)
        {
            return Some(Err(DdlError::internal(
                "internal error: failed to retain descriptor leases for buffered transaction tasks",
            )));
        }
        return match routed {
            Ok(InTxnRoute::Buffered | InTxnRoute::Staged(_)) => None,
            // A write the transaction cannot hold will apply at once and
            // survive its rollback.
            Ok(InTxnRoute::Autocommit(_) | InTxnRoute::Read(_)) => {
                let (_, sqlstate, message) =
                    error_to_sqlstate(&crate::Error::CrossShardInExplicitTransaction);
                Some(Err(ddl_err(sqlstate, message)))
            }
            Err(error) => Some(Err(staging_error_to_ddl(error))),
        };
    }

    let emitter = ArcAuditEmitter(Arc::clone(&state.audit));
    let checked = match intercept_and_authorize(InterceptAndAuthorizeParams {
        state,
        task,
        identity,
        tenant_id: identity.tenant_id,
        permissions: &state.permissions,
        roles: &state.roles,
        emitter: &emitter,
    })
    .await
    {
        Ok(CloneCheckedOutcome::Handled(_)) => return None,
        Ok(CloneCheckedOutcome::Proceed(checked)) => checked,
        Err(error) => return Some(Err(error_to_ddl(&error))),
    };

    // The durable route: Raft in cluster mode, else the funnel's `AppendHere`.
    match crate::control::server::dispatch_utils::dispatch_authorized_durable_write(
        state,
        checked,
        TraceId::ZERO,
    )
    .await
    {
        Err(error) => Some(Err(error_to_ddl(&error))),
        // A refusal arrives as an error status inside an `Ok` response.
        Ok(response) if response.status == crate::bridge::envelope::Status::Error => {
            Some(Err(match response.error_code.as_deref() {
                Some(code) => {
                    let (_, sqlstate, message) = error_code_to_sqlstate(code);
                    ddl_err(sqlstate, message)
                }
                None => DdlError::internal("unknown data plane error"),
            }))
        }
        Ok(_) => None,
    }
}

/// Send one staging request of a transaction to the Data Plane: clone-checked
/// and authorized, as every dispatch is.
pub(super) async fn dispatch_staged(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    staged: nodedb_physical::physical_task::PhysicalTask,
) -> crate::Result<Response> {
    let emitter = ArcAuditEmitter(Arc::clone(&state.audit));
    match intercept_and_authorize(InterceptAndAuthorizeParams {
        state,
        task: staged,
        identity,
        tenant_id: identity.tenant_id,
        permissions: &state.permissions,
        roles: &state.roles,
        emitter: &emitter,
    })
    .await?
    {
        CloneCheckedOutcome::Handled(resp) => Ok(resp),
        CloneCheckedOutcome::Proceed(checked) => {
            crate::control::server::dispatch_utils::dispatch_authorized_to_data_plane(
                state,
                checked,
                TraceId::ZERO,
            )
            .await
        }
    }
}

/// Authorize a write target before triggers, sequences, or catalog reads run.
pub(in crate::control::server::shared::ddl::neutral::collection) fn authorize_write_target(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: crate::types::DatabaseId,
    collection: &str,
) -> Result<(), DdlError> {
    let emitter = ArcAuditEmitter(Arc::clone(&state.audit));
    authorize_collection(
        identity,
        database_id,
        collection,
        Permission::Write,
        &state.permissions,
        &state.roles,
        &emitter,
    )
    .map_err(authorization_error_to_ddl)
}

pub(super) fn authorization_error_to_ddl(error: AuthorizationError) -> DdlError {
    DdlError::new(
        nodedb_types::error::sqlstate::INSUFFICIENT_PRIVILEGE,
        error.resource().to_owned(),
    )
}

/// The DDL error for `error`.
pub(super) fn error_to_ddl(error: &crate::Error) -> DdlError {
    let (_, sqlstate, message) = error_to_sqlstate(error);
    ddl_err(sqlstate, message)
}

/// The DDL error for a write its transaction refused.
pub(super) fn staging_error_to_ddl(error: StagingGateError) -> DdlError {
    match error {
        StagingGateError::Dispatch(error) => error_to_ddl(&error),
        StagingGateError::Rejected { code: Some(code) } => {
            let (_, sqlstate, message) = error_code_to_sqlstate(&code);
            ddl_err(sqlstate, message)
        }
        StagingGateError::Rejected { code: None } => DdlError::internal("unknown data plane error"),
    }
}

/// Where a statement's `RETURNING` rows are shaped: the STORED rows a write
/// answered with, redacted for the caller through the same choke point the
/// pgwire dispatch loop uses, so a redaction policy masks identically on
/// every transport.
pub(super) struct ReturningShape<'a> {
    pub state: &'a SharedState,
    pub identity: &'a AuthenticatedIdentity,
    pub database_id: crate::types::DatabaseId,
    pub txn_ctx: &'a DmlTxnCtx<'a>,
    /// The statement's announced `RETURNING` columns, so this transport
    /// renders a returned cell exactly as pgwire does.
    pub output_schema: &'a OutputSchema,
}

impl ReturningShape<'_> {
    /// Shape `payload`, the `RETURNING` rows `plan`'s write answered with,
    /// and fold them into `rows`: a statement is ONE result set, however
    /// many tasks it planned to.
    pub(super) fn fold(
        &self,
        plan: &PhysicalPlan,
        payload: &[u8],
        rows: &mut Option<ShapedRows>,
    ) -> Result<(), DdlError> {
        let tenant_id = self.identity.tenant_id;
        let scope = crate::control::security::request_scope::RequestAuthScope::for_database(
            self.identity,
            self.state.auth_stores(),
            self.database_id,
        )?;
        let redaction = QueryRedaction::for_plan(tenant_id, self.database_id, scope.auth(), plan);
        // A RETURNING expression is evaluated here, per returned row, and a
        // sequence accessor in it resolves against this session's `currval`
        // map exactly as the pgwire loop's does.
        let sequences = SessionSequenceAccess::for_session(
            self.state,
            self.txn_ctx
                .sessions
                .sequence_values(self.txn_ctx.session_id),
            self.database_id,
            tenant_id,
        );
        let shaped = shape_returning_materialized(MaterializedShapeRequest {
            payload,
            plan,
            plan_kind: PlanKind::ReturningRows,
            projection: Some(self.output_schema),
            state: self.state,
            database_id: self.database_id,
            tenant_id,
            redaction: Some(redaction.ctx(&self.state.redaction)),
            sequences: Some(&sequences),
        })
        .map_err(|error| DdlError::from_error(&crate::Error::from(error)))?;
        match rows {
            Some(accumulated) => accumulated.append(shaped),
            None => *rows = Some(shaped),
        }
        Ok(())
    }
}
