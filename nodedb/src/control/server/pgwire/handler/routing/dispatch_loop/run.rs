// SPDX-License-Identifier: BUSL-1.1

//! The per-task dispatch loop for non-Calvin pgwire queries: tenant check,
//! in-transaction routing (`txn_route`: triggers, clone copy-on-write,
//! staging), streaming fast path, pre-dispatch hooks, dispatch, read tracking
//! (`tracking.rs`), auto-analyze, and metering. Shaping one task's response
//! lives in `task.rs`; the statement's tail (folded RETURNING rows, the
//! set-op merge, and the one folded command tag) lives in `finish.rs`.
//!
//! `execute.rs` holds the plan/authorize/admit entry points and hands the
//! admitted task list here.

use std::sync::Arc;

use pgwire::api::results::Response;
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::control::planner::calvin::write_class::{
    plan_counts_toward_statement_tag, plans_have_user_write,
};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::security::request_scope::RequestAuthScope;
use crate::control::server::response_shape::redaction::QueryRedaction;
use crate::control::server::response_shape::types::{ShapedRows, StatementTag};
use crate::control::server::shared::ddl::neutral::maintenance::auto_analyze;
use crate::control::server::shared::metering::{PlanMeteringInfo, meter_dispatch};
use crate::control::server::shared::quota_admission::admit_quota_for_dispatch;
use crate::control::server::shared::session::{DmlTxnCtx, SessionId};
use crate::control::server::shared::write_admission::plan_is_write;
use crate::types::TenantId;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use super::super::super::super::types::{
    dml_fold_error_to_pg, error_to_pg, error_to_sqlstate, response_status_to_sqlstate,
    sqlstate_error,
};
use super::super::super::core::NodeDbPgHandler;
use super::super::super::plan::{PlanKind, describe_plan};
use super::super::cluster_array::ClusterArrayResult;
use super::super::execute_dml_hooks::{self, PreDispatchHandled};
use super::super::result_shaping::ResultShaping;
use super::super::streaming::StreamSelectContext;
use super::finish::StatementTail;
use super::task::ShapeTaskParams;
use super::tracking::DispatchedTask;
use super::txn_task::StatementTxnOutcome;
use crate::control::server::shared::txn_route::{StatementEvents, TxnTaskContext};

pub(crate) struct DispatchTaskContext<'a> {
    pub(crate) plan_lease_scope: Arc<crate::control::lease::QueryLeaseScope>,
    pub(crate) tenant_id: TenantId,
    pub(crate) identity: &'a AuthenticatedIdentity,
    pub(crate) auth_ctx: &'a crate::control::security::auth_context::AuthContext,
    pub(crate) session_id: SessionId,
    pub(crate) shaping: ResultShaping<'a>,
    /// The row images the statement's cross-shard balances were settled
    /// from. A statement in a transaction adds them to its read set, so
    /// COMMIT's conflict check covers them.
    pub(crate) sum_target_reads:
        Vec<crate::control::server::shared::session::read_set::ReadSetEntry>,
}

impl NodeDbPgHandler {
    /// Execute the per-task dispatch loop for non-Calvin queries. `txn` is
    /// the statement's transaction: the client's block, or the implicit
    /// transaction a statement whose write fires a synchronous trigger body
    /// runs in (see `txn_task`). `None` for an autocommit statement.
    pub(super) async fn dispatch_task_loop_in(
        &self,
        tasks: Vec<PhysicalTask>,
        context: DispatchTaskContext<'_>,
        txn: Option<&DmlTxnCtx<'_>>,
    ) -> PgWireResult<Vec<Response>> {
        let DispatchTaskContext {
            plan_lease_scope,
            tenant_id,
            identity,
            auth_ctx,
            session_id,
            shaping,
            sum_target_reads,
        } = context;
        if let Some(txn) = txn
            && !sum_target_reads.is_empty()
        {
            txn.sessions
                .record_read_entries(txn.session_id, sum_target_reads);
        }
        let projection = shaping.projection;
        let result_formats = shaping.formats;
        let needs_set_op = tasks.iter().any(|t| t.post_set_op != PostSetOp::None);
        // A set-op merge blends rows from every branch into one result, so the
        // union of the branches' sources governs redaction. Resolved before the
        // loop consumes `tasks`.
        let set_op_redaction = needs_set_op
            .then(|| QueryRedaction::for_plans(tenant_id, auth_ctx, tasks.iter().map(|t| &t.plan)));
        let mut dedup_payloads: Vec<Vec<u8>> = Vec::new();
        let mut dedup_set_op = PostSetOp::None;
        // A statement's RETURNING rows are ONE result set, however many tasks
        // the statement planned to. A multi-row `INSERT ... RETURNING` plans one
        // task per row, so the rows are folded here and emitted once after the
        // loop rather than as a RowDescription/DataRow sequence per task, which
        // an extended-query client reads as several results for one statement.
        let mut returning_rows: Option<ShapedRows> = None;
        // The statement's ONE command tag, folded over its write tasks the
        // same way. `execute_sql` calls this loop once per `;`-separated
        // statement, so the fold never crosses a statement boundary.
        let mut statement_tag = StatementTag::default();
        let mut responses = Vec::with_capacity(tasks.len());
        // Session-scoped sequence access for the statement's Control-Plane
        // computed columns, resolved once: every task of one statement
        // targets the same database, so the first task's names it.
        let session_sequences = self.sessions.sequence_values(session_id);
        let statement_database_id = tasks.first().map(|t| t.database_id);
        // Checked once rather than per task — metering is disabled by
        // default, so this keeps the per-task extraction below (which clones
        // the collection name) a true no-op on the hot path for every
        // deployment that hasn't turned it on.
        let metering_enabled = self.state.metering_config.enabled;
        // A derived implicit-edge write beside the user's own never answers
        // the statement, exactly as Calvin's deposit rule has it.
        let has_user_write = plans_have_user_write(tasks.iter().map(|t| &t.plan));
        // A strong session reads linearizably, in and out of a transaction.
        let strong_reads = self.sessions.read_consistency(session_id).requires_leader();

        // A statement in a transaction routes each task through the shared
        // `txn_route`, and fires its SYNC AFTER STATEMENT bodies once after
        // its last task.
        let route_ctx = txn.map(|txn| TxnTaskContext {
            state: &self.state,
            identity,
            auth: auth_ctx,
            txn,
            lease_scope: &plan_lease_scope,
            fire_triggers: true,
        });
        let mut statement_events = StatementEvents::default();

        for mut task in tasks {
            if task.tenant_id != tenant_id {
                tracing::error!(
                    expected = %tenant_id,
                    actual = %task.tenant_id,
                    "SECURITY: task tenant_id mismatch — rejecting"
                );
                return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "42501".to_owned(),
                    "tenant isolation violation: task targets wrong tenant".to_owned(),
                ))));
            }

            // In a transaction: read, buffer for COMMIT, or stage now, with
            // the write's synchronous trigger bodies joining the same
            // transaction. A staged write that answers `RETURNING` folds its
            // rows into the statement's one result set.
            if let Some(route) = route_ctx.as_ref() {
                let plan = task.plan.clone();
                let task_database_id = task.database_id;
                let hooks = execute_dml_hooks::PreDispatchContext {
                    identity,
                    auth: auth_ctx,
                    tenant_id,
                    session_id,
                    plan_kind: describe_plan(&plan),
                    projection,
                    result_formats,
                };
                match self
                    .route_statement_txn_task(route, task, &mut statement_events)
                    .await?
                {
                    StatementTxnOutcome::Dispatch(routed_task) => task = *routed_task,
                    StatementTxnOutcome::Write(handled) => {
                        handled.fold_into(&mut statement_tag)?;
                        continue;
                    }
                    StatementTxnOutcome::Returning(returning) => {
                        for rows in returning {
                            self.shape_task_response(
                                ShapeTaskParams {
                                    response: &staged_rows_response(rows),
                                    plan: &plan,
                                    plan_kind: PlanKind::ReturningRows,
                                    counts_toward_tag: false,
                                    projection,
                                    result_formats,
                                    session_id,
                                    tenant_id,
                                    database_id: task_database_id,
                                    auth_ctx,
                                    session_sequences: session_sequences.clone(),
                                },
                                &mut responses,
                                &mut returning_rows,
                                &mut statement_tag,
                            )?;
                        }
                        continue;
                    }
                    StatementTxnOutcome::Clone(resp) => {
                        match self.clone_write_answer(hooks, &plan, &resp)? {
                            PreDispatchHandled::Rows(response) => responses.push(response),
                            PreDispatchHandled::Write(handled) => {
                                handled.fold_into(&mut statement_tag)?;
                            }
                        }
                        continue;
                    }
                }
            }

            // ClusterArray plans are handled entirely on the Control Plane by
            // the ArrayCoordinator — they must never reach the SPSC bridge or
            // trigger/DML machinery. Intercepted AFTER the routing gate, so an
            // in-transaction write is staged per shard and buffered (reshaped
            // into per-shard `ArrayOp` plans) instead of applying here and
            // surviving ROLLBACK, and an in-transaction read carries the
            // transaction id the gate stamped for read-your-own-writes.
            if matches!(
                task.plan,
                nodedb_physical::physical_plan::PhysicalPlan::ClusterArray(_)
            ) {
                let authorized = self
                    .authorize_tasks(identity, std::slice::from_ref(&task))?
                    .into_tasks()
                    .into_iter()
                    .next()
                    .ok_or_else(|| {
                        PgWireError::UserError(Box::new(ErrorInfo::new(
                            "ERROR".to_owned(),
                            nodedb_types::error::sqlstate::INTERNAL_ERROR.to_owned(),
                            "ClusterArray authorization returned no capability".to_owned(),
                        )))
                    })?;
                match self
                    .dispatch_cluster_array_task(
                        authorized,
                        projection,
                        result_formats,
                        session_id,
                        auth_ctx,
                    )
                    .await?
                {
                    ClusterArrayResult::Rows(response) => responses.push(response),
                    ClusterArrayResult::Dml(outcome) => statement_tag
                        .fold(outcome)
                        .map_err(|e| dml_fold_error_to_pg(&e))?,
                }
                continue;
            }

            let plan_kind = describe_plan(&task.plan);
            let counts_toward_tag = plan_counts_toward_statement_tag(&task.plan, has_user_write);
            let resp_post_set_op = task.post_set_op;
            let task_database_id = task.database_id;
            let task_vshard = task.vshard_id;
            let plan_for_response = task.plan.clone();
            // Extracted from the clone above, before dispatch — metering
            // needs the collection/engine shape after this task's dispatch
            // succeeds. Only covers the "normal dispatch" branch at the
            // bottom of this loop; the streaming fast path
            // (`maybe_stream_select`) and the `ClusterArray` short-circuit
            // dispatch through entirely separate code paths and are not
            // metered here.
            let plan_metering_info =
                metering_enabled.then(|| PlanMeteringInfo::extract(&plan_for_response));

            // A spent hard quota refuses the task BEFORE it runs. The
            // charging call at the bottom of this loop is on the success
            // path by design, so it can never be the place a cap blocks
            // anything — by the time it runs the work is already done.
            if let Some(info) = &plan_metering_info {
                let scope = RequestAuthScope::builder(identity, self.state.auth_stores())
                    .with_session_database(Some(task_database_id))
                    .build();
                admit_quota_for_dispatch(&self.state, &scope, info)
                    .map_err(|e| sqlstate_error("53400", &e.to_string()))?;
            }

            // Single-node pgwire streaming fast path (autocommit SELECT only).
            // In-transaction reads skip streaming so the transaction id rides on
            // the request and the data plane merges the transaction's own staged
            // writes into the scan (read-your-own-writes); the streaming path
            // builds per-core requests without the transaction id.
            if txn.is_none()
                && let Some(stream_response) = self
                    .maybe_stream_select(
                        &task,
                        StreamSelectContext {
                            identity,
                            auth: auth_ctx,
                            plan_kind,
                            session_id,
                            shaping: ResultShaping {
                                projection,
                                formats: result_formats,
                            },
                            lease_scope: Arc::clone(&plan_lease_scope),
                        },
                    )
                    .await?
            {
                responses.push(stream_response);
                continue;
            }

            // --- Pre-dispatch hooks: truncate restart-identity extraction and
            // clone write-path interception (`execute_dml_hooks.rs`).
            let (dml_info, truncate_restart_collection) = match self
                .run_pre_dispatch_hooks(
                    execute_dml_hooks::PreDispatchContext {
                        identity,
                        auth: auth_ctx,
                        tenant_id,
                        session_id,
                        plan_kind,
                        projection,
                        result_formats,
                    },
                    task,
                )
                .await?
            {
                execute_dml_hooks::PreDispatchOutcome::Handled(PreDispatchHandled::Rows(
                    response,
                )) => {
                    responses.push(response);
                    continue;
                }
                execute_dml_hooks::PreDispatchOutcome::Handled(PreDispatchHandled::Write(
                    handled,
                )) => {
                    handled.fold_into(&mut statement_tag)?;
                    continue;
                }
                execute_dml_hooks::PreDispatchOutcome::Proceed(proceed) => {
                    let execute_dml_hooks::PreDispatchProceed {
                        task: proceeding_task,
                        dml_info,
                        truncate_restart_collection,
                    } = *proceed;
                    task = proceeding_task;
                    (dml_info, truncate_restart_collection)
                }
            };

            // --- Normal dispatch ---
            let user_id: Option<std::sync::Arc<str>> =
                Some(std::sync::Arc::from(identity.username.as_str()));
            let linearizable = strong_reads && !plan_is_write(&task.plan);
            let (resp, shard_watermarks, distributed_reads) = self
                .dispatch_authorized_task_with_watermarks(task, user_id, identity, linearizable)
                .await
                .map_err(|e| {
                    let (severity, code, message) = error_to_sqlstate(&e);
                    PgWireError::UserError(Box::new(ErrorInfo::new(
                        severity.to_owned(),
                        code.to_owned(),
                        message,
                    )))
                })?;

            self.track_dispatched_task(
                DispatchedTask {
                    identity,
                    client_session: session_id,
                    txn,
                    plan: &plan_for_response,
                    vshard: task_vshard,
                    database_id: task_database_id,
                },
                &resp,
                shard_watermarks,
                &distributed_reads,
            )
            .await;

            if let Some((severity, code, message)) =
                response_status_to_sqlstate(resp.status, resp.error_code.as_deref())
            {
                return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    severity.to_owned(),
                    code.to_owned(),
                    message,
                ))));
            }

            // --- TRUNCATE RESTART IDENTITY ---
            if let Some(collection) = &truncate_restart_collection {
                self.state
                    .sequence_registry
                    .restart_sequences_for_collection(
                        task_database_id.as_u64(),
                        tenant_id.as_u64(),
                        collection,
                    );
            }

            // An autocommit write reaching here fires no synchronous trigger
            // body: one that does runs in an implicit transaction (`txn`).
            if let Some(ref info) = dml_info {
                auto_analyze::record_and_maybe_analyze(
                    &self.state,
                    identity,
                    task_database_id,
                    &info.collection,
                );
            }

            // This task's own row count, for metering below — `None` for the
            // set-op-deferred branch (its rows are only known after the
            // later cross-task merge) and for `Passthrough` (no row payload
            // to count); `meter_dispatch` charges one unit for `None`.
            let task_rows = if needs_set_op && resp_post_set_op != PostSetOp::None {
                dedup_payloads.push(resp.payload.to_vec());
                if dedup_set_op == PostSetOp::None {
                    dedup_set_op = resp_post_set_op;
                }
                None
            } else {
                self.shape_task_response(
                    ShapeTaskParams {
                        response: &resp,
                        plan: &plan_for_response,
                        plan_kind,
                        counts_toward_tag,
                        projection,
                        result_formats,
                        session_id,
                        tenant_id,
                        database_id: task_database_id,
                        auth_ctx,
                        session_sequences: session_sequences.clone(),
                    },
                    &mut responses,
                    &mut returning_rows,
                    &mut statement_tag,
                )?
            };

            // Metered here, once per successfully dispatched task — every
            // path reaching this point already passed the
            // `response_status_to_sqlstate` error check above.
            if let Some(info) = &plan_metering_info {
                let scope = RequestAuthScope::builder(identity, self.state.auth_stores())
                    .with_session_database(Some(task_database_id))
                    .build();
                meter_dispatch(&self.state, &scope, info, task_rows);
            }
        }

        if let Some(route) = route_ctx.as_ref() {
            statement_events
                .fire(route)
                .await
                .map_err(|error| error_to_pg(&error))?;
        }

        self.finish_statement(
            &mut responses,
            StatementTail {
                returning_rows,
                statement_tag,
                dedup_payloads,
                dedup_set_op,
                projection,
                result_formats,
                set_op_redaction,
                session_sequences,
                statement_database_id,
                tenant_id,
                session_id,
            },
        )?;

        Ok(responses)
    }
}

/// A staged write's `RETURNING` rows as the response the shaper reads.
fn staged_rows_response(rows: Vec<u8>) -> crate::bridge::envelope::Response {
    crate::bridge::envelope::Response {
        request_id: crate::types::RequestId::new(0),
        status: crate::bridge::envelope::Status::Ok,
        attempt: 0,
        partial: false,
        payload: crate::bridge::envelope::Payload::from_vec(rows),
        watermark_lsn: crate::types::Lsn::ZERO,
        error_code: None,
        stage_vote: None,
        read_version_lsn: crate::types::Lsn::ZERO,
        write_set: Vec::new(),
    }
}
