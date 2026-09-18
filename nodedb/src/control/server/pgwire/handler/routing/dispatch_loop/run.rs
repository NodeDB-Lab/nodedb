// SPDX-License-Identifier: BUSL-1.1

//! The per-task dispatch loop for non-Calvin pgwire queries: tenant check,
//! in-transaction routing, streaming fast path, pre-dispatch hooks, dispatch,
//! read tracking, AFTER triggers, and metering. Shaping one task's response
//! lives in `task.rs`; the statement's tail (folded RETURNING rows and the
//! set-op merge) lives in `finish.rs`.
//!
//! Split out of `execute.rs`, which keeps the plan/authorize/admit entry
//! points and hands the admitted task list here.

use std::sync::Arc;

use pgwire::api::results::Response;
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::security::request_scope::RequestAuthScope;
use crate::control::server::response_shape::redaction::QueryRedaction;
use crate::control::server::response_shape::types::ShapedRows;
use crate::control::server::shared::ddl::neutral::maintenance::auto_analyze;
use crate::control::server::shared::metering::{PlanMeteringInfo, meter_dispatch};
use crate::control::server::shared::quota_admission::admit_quota_for_dispatch;
use crate::control::server::shared::session::SessionId;
use crate::types::TenantId;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use super::super::super::super::types::{
    error_to_sqlstate, response_status_to_sqlstate, sqlstate_error,
};
use super::super::super::core::NodeDbPgHandler;
use super::super::super::plan::{PlanKind, describe_plan};
use super::super::execute_dml_hooks;
use super::super::result_shaping::ResultShaping;
use super::super::streaming::StreamSelectContext;
use super::finish::StatementTail;
use super::task::ShapeTaskParams;

pub(crate) struct DispatchTaskContext<'a> {
    pub(crate) plan_lease_scope: Arc<crate::control::lease::QueryLeaseScope>,
    pub(crate) tenant_id: TenantId,
    pub(crate) identity: &'a AuthenticatedIdentity,
    pub(crate) auth_ctx: &'a crate::control::security::auth_context::AuthContext,
    pub(crate) session_id: SessionId,
    pub(crate) shaping: ResultShaping<'a>,
}

impl NodeDbPgHandler {
    /// Execute the per-task dispatch loop for non-Calvin queries.
    pub(crate) async fn dispatch_task_loop(
        &self,
        tasks: Vec<PhysicalTask>,
        context: DispatchTaskContext<'_>,
    ) -> PgWireResult<Vec<Response>> {
        let DispatchTaskContext {
            plan_lease_scope,
            tenant_id,
            identity,
            auth_ctx,
            session_id,
            shaping,
        } = context;
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

            // Whether this task would answer with rows, read BEFORE the
            // routing gate consumes the task: a buffered or staged write
            // reports only a command tag, and a statement that asked for rows
            // must be told so rather than handed that tag.
            let returns_rows = matches!(describe_plan(&task.plan), PlanKind::ReturningRows);

            // In-transaction write-routing gate: protocol-neutral decision of
            // read / buffer-for-COMMIT / stage-now-and-buffer, shared with
            // every other dispatch loop (native, DSL/UPSERT). Moved to
            // `execute_dml_hooks.rs` to keep this file under the size limit;
            // behavior is unchanged.
            match self
                .route_task_in_txn(session_id, identity, task, Arc::clone(&plan_lease_scope))
                .await?
            {
                execute_dml_hooks::TxnRouteOutcome::Proceed(routed_task) => {
                    task = *routed_task;
                }
                execute_dml_hooks::TxnRouteOutcome::Handled(resp) => {
                    if returns_rows {
                        let (severity, code, message) = error_to_sqlstate(
                            &crate::control::server::shared::returning::
                                in_transaction_returning_unsupported(),
                        );
                        return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                            severity.to_owned(),
                            code.to_owned(),
                            message,
                        ))));
                    }
                    responses.push(resp);
                    continue;
                }
            }

            // ClusterArray plans are handled entirely on the Control Plane by
            // the ArrayCoordinator — they must never reach the SPSC bridge or
            // trigger/DML machinery. Intercepted AFTER the routing gate, so an
            // in-transaction write is buffered (reshaped into per-shard
            // `ArrayOp` plans) instead of applying here and surviving ROLLBACK.
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
                            "XX000".to_owned(),
                            "ClusterArray authorization returned no capability".to_owned(),
                        )))
                    })?;
                let response = self
                    .dispatch_cluster_array_task(
                        authorized,
                        projection,
                        result_formats,
                        session_id,
                        auth_ctx,
                    )
                    .await?;
                responses.push(response);
                continue;
            }

            let plan_kind = describe_plan(&task.plan);
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
            let in_transaction = self.sessions.transaction_state(session_id)
                == crate::control::server::shared::session::TransactionState::InBlock;
            if !in_transaction
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

            // --- Pre-dispatch hooks: trigger interception + clone write-path
            // interception (moved to execute_dml_hooks.rs to keep this file
            // under the size limit; behavior is unchanged).
            let (dml_info, old_row, truncate_restart_collection) = match self
                .run_pre_dispatch_hooks(
                    execute_dml_hooks::PreDispatchContext {
                        identity,
                        auth: auth_ctx,
                        tenant_id,
                        session_id,
                        plan_kind,
                        projection,
                    },
                    task,
                )
                .await?
            {
                execute_dml_hooks::PreDispatchOutcome::Handled(resp) => {
                    responses.push(resp);
                    continue;
                }
                execute_dml_hooks::PreDispatchOutcome::Proceed(proceed) => {
                    let execute_dml_hooks::PreDispatchProceed {
                        task: proceeding_task,
                        dml_info,
                        old_row,
                        truncate_restart_collection,
                    } = *proceed;
                    task = proceeding_task;
                    (dml_info, old_row, truncate_restart_collection)
                }
            };

            // --- Normal dispatch ---
            let user_id: Option<std::sync::Arc<str>> =
                Some(std::sync::Arc::from(identity.username.as_str()));
            let (resp, shard_watermarks, distributed_reads) = self
                .dispatch_authorized_task_with_watermarks(task, user_id, identity)
                .await
                .map_err(|e| {
                    let (severity, code, message) = error_to_sqlstate(&e);
                    PgWireError::UserError(Box::new(ErrorInfo::new(
                        severity.to_owned(),
                        code.to_owned(),
                        message,
                    )))
                })?;

            // Track reads for snapshot-isolation / cross-shard conflict detection
            // at the protocol-neutral layer. Recorded BEFORE the error
            // short-circuit so an absent-key point read (a `NotFound` from the
            // Data Plane) is still captured — a "not found" is a validatable
            // phantom observation, not a no-op. Only successful reads and
            // not-found reads record; a genuine dispatch failure does not.
            let records_read = resp.status == crate::bridge::envelope::Status::Ok
                || resp.error_code.as_deref()
                    == Some(&crate::bridge::envelope::ErrorCode::NotFound);
            if records_read
                && self.sessions.transaction_state(session_id)
                    == crate::control::server::shared::session::TransactionState::InBlock
            {
                let watermarks = if shard_watermarks.is_empty() {
                    vec![(task_vshard, resp.watermark_lsn)]
                } else {
                    shard_watermarks
                };
                crate::control::server::shared::session::record_reads_for_response(
                    &self.state,
                    &self.sessions,
                    session_id,
                    identity.tenant_id,
                    crate::control::server::shared::session::ResponseReads {
                        plan: &plan_for_response,
                        watermarks: &watermarks,
                        read_version_lsn: resp.read_version_lsn,
                        found: resp.status == crate::bridge::envelope::Status::Ok,
                        distributed_reads: &distributed_reads,
                        read_lsn_vshard: task_vshard,
                    },
                )
                .await;
            }

            // Record the session's OWN committed write-version so a later
            // transaction's read-set capture can be floored at it
            // (read-your-writes floor for cross-shard OCC). A prior autocommit
            // write must still floor a later transaction's read, so this records
            // regardless of transaction state — the version is the write's
            // committed per-collection `coll_write_lsn`, carried on
            // `read_version_lsn` by the replicated-write dispatch path. Only
            // successful writes with a non-zero version are recorded.
            if resp.status == crate::bridge::envelope::Status::Ok
                && resp.read_version_lsn > crate::types::Lsn::ZERO
                && matches!(
                    crate::control::security::identity::required_permission(&plan_for_response),
                    crate::control::security::identity::Permission::Write
                )
                && let Some(collection) =
                    crate::control::server::shared::plan_util::extract_collection(
                        &plan_for_response,
                    )
            {
                self.sessions.note_own_write(
                    session_id,
                    task_database_id,
                    identity.tenant_id,
                    collection,
                    resp.read_version_lsn,
                );
            }

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

            // --- AFTER triggers ---
            if let Some(ref info) = dml_info {
                crate::control::trigger::dml_hook_fire::fire_post_dispatch_triggers(
                    crate::control::trigger::dml_hook_fire::DispatchTriggerParams {
                        state: &self.state,
                        identity,
                        database_id: task_database_id,
                        tenant_id,
                        info,
                        old_row: &old_row,
                        cascade_depth: 0,
                    },
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

        self.finish_statement(
            &mut responses,
            StatementTail {
                returning_rows,
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
