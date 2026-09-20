// SPDX-License-Identifier: BUSL-1.1

//! Calvin multi-shard distributed dispatch.
//!
//! Handles the strict multi-shard path via the Calvin sequencer, including
//! the OLLP-dependent-predicate variant that runs an optimistic pre-execution
//! scan before submitting the transaction.

use pgwire::api::results::Response;
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::control::planner::calvin::{
    TxnDispatchPosition, dispatch_authorized_dependent_edge_recon,
    dispatch_authorized_tasks_to_calvin, is_dependent_predicate,
};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::security::request_scope::RequestAuthScope;
use crate::control::server::response_shape::calvin_fold::{
    CalvinFoldCtx, CalvinFoldError, fold_calvin_batch,
};
use crate::control::server::shared::metering::{PlanMeteringInfo, meter_dispatch};
use crate::control::server::shared::session::{SessionId, TransactionState};
use crate::types::{DatabaseId, TenantId};
use nodedb_physical::physical_task::PhysicalTask;

use super::super::super::command_tag::push_folded_tag;
use super::super::super::types::{dml_fold_error_to_pg, error_to_pg, error_to_sqlstate};
use super::super::core::NodeDbPgHandler;

/// Meter one Calvin task once the batch has committed — Calvin applies the
/// whole batch atomically, so by the time responses are being shaped every
/// task in `tasks` has already committed.
///
/// `rows: None` — a task yields either a tag contribution or its rows, which
/// the fold takes once for the statement's single result set; counting rows
/// here would mean reaching into that fold. `meter_dispatch` charges one unit
/// for `None`, correct for the write that just committed.
fn meter_calvin_task(
    state: &crate::control::state::SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    task: &PhysicalTask,
) {
    if !state.metering_config.enabled {
        return;
    }
    let info = PlanMeteringInfo::extract(&task.plan);
    let scope = RequestAuthScope::builder(identity, state.auth_stores())
        .with_session_database(Some(database_id))
        .build();
    meter_dispatch(state, &scope, &info, None);
}

/// Who issued the statement and how its rows must be encoded back.
///
/// Bundled because these are the connection's identity and result contract,
/// not parameters of the dispatch: they are looked up together at the call
/// site and travel unchanged through every branch below.
pub(super) struct CalvinDispatchSession<'a> {
    pub identity: &'a AuthenticatedIdentity,
    pub session_id: SessionId,
    pub result_formats: &'a [pgwire::api::results::FieldFormat],
    pub auth: &'a crate::control::security::auth_context::AuthContext,
    /// The statement's announced output columns, when it announced any.
    pub projection: Option<&'a crate::control::server::response_shape::schema::OutputSchema>,
}

impl NodeDbPgHandler {
    /// Drive Calvin strict multi-shard dispatch for the given task set.
    ///
    /// Returns the response vec on success: the statement's RETURNING rows
    /// (if any) and its one command tag. The caller returns this immediately
    /// — Calvin tasks do not go through the per-task dispatch loop.
    pub(super) async fn dispatch_calvin_multishard(
        &self,
        tasks: Vec<PhysicalTask>,
        tenant_id: TenantId,
        session: CalvinDispatchSession<'_>,
        reads: &[crate::control::server::shared::session::read_set::ReadSetEntry],
    ) -> PgWireResult<Vec<Response>> {
        let CalvinDispatchSession {
            identity,
            session_id,
            result_formats,
            auth,
            projection,
        } = session;
        let cross_shard_mode = self.sessions.cross_shard_txn_mode(session_id);
        let tx_state = self.sessions.transaction_state(session_id);
        let database_id = self
            .sessions
            .get_current_database(session_id)
            .unwrap_or(crate::types::DatabaseId::DEFAULT);

        // Presence guard preserved from the inlined implementation: BOTH the
        // static and OLLP paths require the completion registry to be wired, so
        // an absent registry rejects either path with `SequencerUnavailable`
        // here, before any classification or scan. The OLLP body re-fetches the
        // registry itself; this check keeps the static path's rejection
        // behaviour byte-identical.
        if self.state.calvin_completion_registry.get().is_none() {
            let (severity, code, message) = error_to_sqlstate(&crate::Error::SequencerUnavailable);
            return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                severity.to_owned(),
                code.to_owned(),
                message,
            ))));
        }

        let dependent_task = tasks.iter().find(|t| is_dependent_predicate(&t.plan));

        // Static (non-OLLP) Calvin path: build the TxClass and route the
        // submit-and-await to the SEQUENCER-GROUP leader via
        // `submit_calvin_routed`. Submitting to the LOCAL inbox here is the
        // silent-loss bug this fix addresses: only the sequencer leader's service
        // assigns and only its registry receives the replicated completion ack,
        // so a submit on a non-leader coordinator never completes. Routing fixes
        // that for cross-shard document writes from any coordinator.
        //
        // The OLLP (dependent-predicate) path below is COORDINATOR-OWNED: this
        // handler runs `run_dependent_with_retry`, which owns the
        // submit → await-assignment → await-completion loop and, on a post-exec
        // predicate-drift mismatch, runs a FRESH pre-execution reconnaissance
        // before resubmitting (the scheduler releases the aborted attempt's
        // locks and only signals the mismatch back — it no longer re-submits a
        // stale prediction). The submit step ROUTES to the sequencer-group leader
        // via `submit_calvin_routed_assign` (returning the leader-assigned
        // assignment) while the completion is awaited on this coordinator's local
        // registry, which receives the replicated completion ack on every
        // sequencer-group member. This makes the dependent path complete from a
        // non-leader coordinator, unifying single-node and cross-node into one
        // path, while still passing through this node's circuit-breaker / budget
        // gate.
        if dependent_task.is_none() {
            // Static (non-OLLP) path: delegate to the protocol-neutral
            // `dispatch_tasks_to_calvin` helper, supplying the session-derived
            // inputs (cross-shard mode, in-block state) it needs as parameters.
            // The helper classifies, rejects cross-shard writes inside an
            // explicit transaction block, builds the static TxClass, and routes
            // the SINGLE submit-and-await to the sequencer leader. On success
            // every task's outcome folds into the statement's one response.
            // A cross-shard span in a single statement executed mid-block cannot
            // be buffered atomically, so it rejects; an autocommit statement
            // proceeds. (The COMMIT flush of a buffered block routes through the
            // neutral commit orchestrator, not this per-statement path.)
            let position = if tx_state == TransactionState::InBlock {
                TxnDispatchPosition::MidBlockStatement
            } else {
                TxnDispatchPosition::Autocommit
            };
            let authorized = self.authorize_tasks(identity, &tasks)?;
            let apply_resp = dispatch_authorized_tasks_to_calvin(
                &self.state,
                authorized,
                tenant_id,
                cross_shard_mode,
                position,
                reads,
                None,
            )
            .await
            .map_err(|e| {
                let (severity, code, message) = error_to_sqlstate(&e);
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    severity.to_owned(),
                    code.to_owned(),
                    message,
                )))
            })?;

            return self.shape_calvin_batch(
                &tasks,
                apply_resp.as_ref(),
                CalvinBatchShaping {
                    identity,
                    result_formats,
                    auth,
                    projection,
                    tenant_id,
                    database_id,
                },
            );
        }

        // OLLP path: delegate the full reconnaissance + atomic-submit + drift-
        // retry orchestration to the protocol-neutral
        // `dispatch_dependent_edge_recon`. The dependent task is guaranteed
        // present (the static path returned early above); its `database_id` is
        // the recon scan's database. On `Ok` the batch shapes the SAME way as
        // the static path; on `Err` the typed `crate::Error` maps through the
        // pgwire error→SQLSTATE path.
        let database_id = dependent_task
            .ok_or_else(|| {
                // Unreachable: the static (non-dependent) path returns early
                // above. Surface a typed error rather than panicking if the
                // invariant is ever broken by a future refactor.
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "XX000".to_owned(),
                    "internal: static Calvin path reached the OLLP dispatch branch".to_owned(),
                )))
            })?
            .database_id;

        // Run the recon/OLLP dispatch, which drains this coordinator's sidecar
        // and returns the applied Data-Plane Response for the RETURNING doc
        // write (if any). `tasks` is cloned into the recon call so the original
        // list survives to shape the per-task responses afterwards: a RETURNING
        // task emits its rows, every other task its command tag.
        // Normal multi-shard OLLP dispatch (NOT the contended single-shard
        // route from `route_write_to_calvin`), so it stays on the strict
        // multi-vshard dependent `TxClass` builder (`allow_single_vshard:
        // false`).
        let authorized = self.authorize_tasks(identity, &tasks)?;
        let outcome = dispatch_authorized_dependent_edge_recon(
            &self.state,
            authorized,
            identity,
            tenant_id,
            database_id,
            false,
        )
        .await
        .map_err(|e| {
            let (severity, code, message) = error_to_sqlstate(&e);
            PgWireError::UserError(Box::new(ErrorInfo::new(
                severity.to_owned(),
                code.to_owned(),
                message,
            )))
        })?;

        self.shape_calvin_batch(
            &tasks,
            outcome.apply_result.as_ref(),
            CalvinBatchShaping {
                identity,
                result_formats,
                auth,
                projection,
                tenant_id,
                database_id,
            },
        )
    }

    /// Shape a completed Calvin batch into the statement's responses: its
    /// RETURNING rows, when a task carried them, then its one command tag.
    /// The fold itself is protocol-neutral (`response_shape::calvin_fold`), so
    /// native answers the same rows and count for the same batch.
    fn shape_calvin_batch(
        &self,
        tasks: &[PhysicalTask],
        apply_resp: Option<&crate::bridge::envelope::Response>,
        shaping: CalvinBatchShaping<'_>,
    ) -> PgWireResult<Vec<Response>> {
        let CalvinBatchShaping {
            identity,
            result_formats,
            auth,
            projection,
            tenant_id,
            database_id,
        } = shaping;
        let plans: Vec<&crate::bridge::envelope::PhysicalPlan> =
            tasks.iter().map(|task| &task.plan).collect();
        let fold = fold_calvin_batch(
            &plans,
            apply_resp,
            &CalvinFoldCtx {
                projection,
                state: &self.state,
                tenant_id,
                database_id,
                auth,
            },
        )
        .map_err(|e| match e {
            CalvinFoldError::Verb(e) => dml_fold_error_to_pg(&e),
            CalvinFoldError::Shape(e) => error_to_pg(&e),
        })?;
        for task in tasks {
            meter_calvin_task(&self.state, identity, database_id, task);
        }
        let mut responses: Vec<Response> = Vec::with_capacity(2);
        if let Some(shaped) = fold.rows {
            let (response, _notice) =
                super::super::shape_encode::shaped_query_response(shaped, result_formats);
            responses.push(response);
        }
        push_folded_tag(&mut responses, fold.tag);
        Ok(responses)
    }
}

/// How a completed Calvin batch is shaped back to the client.
struct CalvinBatchShaping<'a> {
    identity: &'a AuthenticatedIdentity,
    result_formats: &'a [pgwire::api::results::FieldFormat],
    auth: &'a crate::control::security::auth_context::AuthContext,
    projection: Option<&'a crate::control::server::response_shape::schema::OutputSchema>,
    tenant_id: TenantId,
    database_id: DatabaseId,
}
