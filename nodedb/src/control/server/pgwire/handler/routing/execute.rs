// SPDX-License-Identifier: BUSL-1.1

//! Plan-and-dispatch entry points for SQL queries on the simple-query and
//! extended-query (prepared-statement) paths.

use pgwire::api::results::{FieldFormat, Response, Tag};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::control::planner::calvin::{
    DispatchClass, classify_dispatch, plan_needs_implicit_edge_recon,
};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::compose::{self, ShapeOutcome};
use crate::types::TenantId;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use super::super::super::types::{error_to_sqlstate, response_status_to_sqlstate, sqlstate_error};
use super::super::core::NodeDbPgHandler;
use super::super::plan::{describe_plan, payload_to_response};
use super::super::shape_encode;
use super::planning::consistency_for_tasks;
use super::result_shaping::ResultShaping;
use super::set_ops;
use crate::control::server::response_shape::schema::OutputSchema;

impl NodeDbPgHandler {
    /// Plan and dispatch SQL after quota and DDL checks have passed.
    ///
    /// When in a transaction block (BEGIN..COMMIT), write operations are
    /// buffered instead of dispatched. Read operations execute immediately.
    /// The buffer is dispatched atomically on COMMIT.
    ///
    /// This is the simple-query entry point (no bound parameters). After
    /// dispatching, the SELECT projection list is parsed from `sql` and
    /// each query response is re-encoded with one pgwire field per projected
    /// column. The extended-query path (`execute_planned_sql_with_params`)
    /// skips this step because `execute_prepared` applies column projection
    /// using the richer schema from the Describe phase.
    pub(in crate::control::server::pgwire::handler) async fn execute_planned_sql(
        &self,
        identity: &AuthenticatedIdentity,
        sql: &str,
        tenant_id: TenantId,
        addr: &std::net::SocketAddr,
    ) -> PgWireResult<Vec<Response>> {
        // Planner output shapes every SELECT-read response through the neutral core.
        // Simple query has no Bind message, so no client-requested result
        // formats: everything renders in text.
        self.execute_planned_sql_inner(
            identity,
            sql,
            tenant_id,
            addr,
            &[],
            ResultShaping {
                projection: None,
                formats: &[],
            },
        )
        .await
    }

    /// Execute planned SQL with bound parameters (prepared statement path).
    pub(in crate::control::server::pgwire::handler) async fn execute_planned_sql_with_params(
        &self,
        identity: &AuthenticatedIdentity,
        sql: &str,
        tenant_id: TenantId,
        addr: &std::net::SocketAddr,
        params: &[nodedb_sql::ParamValue],
        shaping: ResultShaping<'_>,
    ) -> PgWireResult<Vec<Response>> {
        self.execute_planned_sql_inner(identity, sql, tenant_id, addr, params, shaping)
            .await
    }

    async fn execute_planned_sql_inner(
        &self,
        identity: &AuthenticatedIdentity,
        sql: &str,
        tenant_id: TenantId,
        addr: &std::net::SocketAddr,
        params: &[nodedb_sql::ParamValue],
        shaping: ResultShaping<'_>,
    ) -> PgWireResult<Vec<Response>> {
        let (mut tasks, output_schema, _plan_lease_scope) = self
            .plan_statement_to_tasks(identity, sql, tenant_id, addr, params)
            .await?;

        if tasks.is_empty() {
            return Ok(vec![Response::Execution(Tag::new("OK"))]);
        }

        // An externally-supplied prepared-statement schema (from the Describe
        // phase) wins; otherwise use the planner's fresh output schema for this
        // statement.
        let effective_schema = shaping.projection.or(Some(&output_schema));

        // Implicit graph-edge extraction: a schemaless document carrying
        // `_from`/`_to` is mirrored as a `GraphOp::EdgePut` task, homed and
        // surrogate-resolved per endpoint so it routes through the same
        // classify/Calvin/single-shard path as an explicit edge.
        let edge_database_id = self
            .sessions
            .get_current_database(addr)
            .unwrap_or(crate::types::DatabaseId::DEFAULT);
        crate::control::planner::implicit_edges::append_implicit_edge_tasks(
            &self.state,
            &mut tasks,
            tenant_id,
            edge_database_id,
            crate::types::TraceId::ZERO,
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

        // The final task set must be authorized before any clone interception,
        // orchestration, staging, or dispatch path can observe it.
        self.authorize_tasks(identity, &tasks)?;

        // Clone CoW read-path interception: for Shadowed/Materializing clones,
        // augment tasks with source-database reads and merge results.
        // Returns Some(responses) when clone dispatch is fully handled.
        // Returns None when this is not a cloned collection (fast path).
        if let Some(clone_responses) = self
            .maybe_dispatch_clone_reads(
                tasks.clone(),
                identity,
                tenant_id,
                addr,
                effective_schema,
                shaping.formats,
            )
            .await?
        {
            return Ok(clone_responses);
        }

        // Implicit-edge DELETE/UPDATE routing gate. A dependent predicate
        // (`BulkDelete`/`BulkUpdate`) on an edge-bearing collection must run
        // through the OLLP/Calvin coordinator path so the implicit edge-delete
        // tasks are derived (via the pre-exec recon read) and committed
        // atomically with the doc write. This MUST preempt gateway-forwarding:
        // a non-(data-shard)-leader coordinator would otherwise forward the raw
        // single-shard `BulkDelete` to the shard leader, bypassing edge cleanup
        // entirely. It also preempts the `classify_dispatch` match below, since a
        // single-collection delete classifies as `SingleShard`. Deliberately NOT
        // gated on `cross_shard_txn` mode — this is INTERNAL index/edge
        // maintenance that must run regardless of the user's cross-shard
        // preference (unlike the `MultiShard` arm, which gates USER cross-shard
        // writes on Strict). `dispatch_calvin_multishard` owns the OLLP retry
        // loop and routes the submit to the sequencer-group leader, so it runs
        // correctly on a coordinator that is not the data-shard leader.
        {
            let tx_state = self.sessions.transaction_state(addr);
            // The not-in-txn-block + registry-available guards are session-state
            // concerns and stay here (per protocol); the edge-bearing detection
            // is the protocol-neutral `plan_needs_implicit_edge_recon`. A genuine
            // catalog READ error propagates (misrouting a delete on a real I/O
            // fault would silently skip edge cleanup → dangling edges); an absent
            // catalog or collection row falls through as non-edge-bearing.
            if tx_state != crate::control::server::shared::session::TransactionState::InBlock
                && self.state.calvin_completion_registry.get().is_some()
                && plan_needs_implicit_edge_recon(&self.state, &tasks, tenant_id)
                    .map_err(|e| {
                        let (severity, code, message) = error_to_sqlstate(&e);
                        PgWireError::UserError(Box::new(ErrorInfo::new(
                            severity.to_owned(),
                            code.to_owned(),
                            message,
                        )))
                    })?
                    .is_some()
            {
                return self
                    .dispatch_calvin_multishard(tasks, tenant_id, identity, addr, shaping.formats)
                    .await;
            }
        }

        let consistency = consistency_for_tasks(&tasks);

        // When all tasks target a remote leader, route through the gateway.
        if self.should_forward_via_gateway(&tasks, consistency) {
            let database_id = self
                .sessions
                .get_current_database(addr)
                .unwrap_or(crate::types::DatabaseId::DEFAULT);
            return self
                .dispatch_tasks_via_gateway(
                    tasks,
                    tenant_id,
                    database_id,
                    effective_schema,
                    shaping.formats,
                )
                .await;
        }

        let tx_state = self.sessions.transaction_state(addr);
        // Autocommit statement routing: no session read-set to widen with.
        match classify_dispatch(&tasks, &std::collections::BTreeSet::new()) {
            DispatchClass::SingleShard { .. } => {
                // A single-shard dependent-predicate write (e.g. `DELETE ...
                // WHERE <non-pk>`) doesn't need OLLP/Calvin: one shard is one
                // Raft group, so the normal replicated-write dispatch path
                // applies it deterministically. Edge-bearing dependent
                // predicates are already preempted onto Calvin above; only
                // genuine multi-shard bulk writes need OLLP. Fall through.
            }
            DispatchClass::MultiShard { .. } => {
                if tx_state == crate::control::server::shared::session::TransactionState::InBlock {
                    let (severity, code, message) =
                        error_to_sqlstate(&crate::Error::CrossShardInExplicitTransaction);
                    return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                        severity.to_owned(),
                        code.to_owned(),
                        message,
                    ))));
                }

                let cross_shard_mode = self.sessions.cross_shard_txn_mode(addr);
                if cross_shard_mode
                    == crate::control::server::shared::session::cross_shard_mode::CrossShardTxnMode::Strict
                {
                    return self
                        .dispatch_calvin_multishard(
                            tasks,
                            tenant_id,
                            identity,
                            addr,
                            shaping.formats,
                        )
                        .await;
                }
            }
        }

        self.dispatch_task_loop(
            tasks,
            tenant_id,
            identity,
            addr,
            effective_schema,
            shaping.formats,
        )
        .await
    }

    /// Execute the per-task dispatch loop for non-Calvin queries.
    async fn dispatch_task_loop(
        &self,
        tasks: Vec<PhysicalTask>,
        tenant_id: TenantId,
        identity: &AuthenticatedIdentity,
        addr: &std::net::SocketAddr,
        projection: Option<&OutputSchema>,
        result_formats: &[FieldFormat],
    ) -> PgWireResult<Vec<Response>> {
        let needs_set_op = tasks.iter().any(|t| t.post_set_op != PostSetOp::None);
        let mut dedup_payloads: Vec<Vec<u8>> = Vec::new();
        let mut dedup_set_op = PostSetOp::None;
        let mut responses = Vec::with_capacity(tasks.len());

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

            // ClusterArray plans are handled entirely on the Control Plane by the
            // ArrayCoordinator — they must never reach the SPSC bridge or
            // trigger/DML machinery. Intercept them here and short-circuit.
            if let nodedb_physical::physical_plan::PhysicalPlan::ClusterArray(ref cluster_op) =
                task.plan
            {
                let response = self
                    .dispatch_cluster_array_task(cluster_op, projection, result_formats, addr)
                    .await?;
                responses.push(response);
                continue;
            }

            // In-transaction write-routing gate: protocol-neutral decision of
            // read / buffer-for-COMMIT / stage-now-and-buffer, shared with
            // every other dispatch loop (native, DSL/UPSERT). Moved to
            // `execute_dml_hooks.rs` to keep this file under the size limit;
            // behavior is unchanged.
            match self.route_task_in_txn(addr, identity, task).await? {
                super::execute_dml_hooks::TxnRouteOutcome::Proceed(routed_task) => {
                    task = *routed_task;
                }
                super::execute_dml_hooks::TxnRouteOutcome::Handled(resp) => {
                    responses.push(resp);
                    continue;
                }
            }

            let plan_kind = describe_plan(&task.plan);
            let resp_post_set_op = task.post_set_op;
            let task_database_id = task.database_id;
            let task_vshard = task.vshard_id;
            let plan_for_response = task.plan.clone();

            // Single-node pgwire streaming fast path (autocommit SELECT only).
            // In-transaction reads skip streaming so the transaction id rides on
            // the request and the data plane merges the transaction's own staged
            // writes into the scan (read-your-own-writes); the streaming path
            // builds per-core requests without the transaction id.
            let in_transaction = self.sessions.transaction_state(addr)
                == crate::control::server::shared::session::TransactionState::InBlock;
            if !in_transaction
                && let Some(stream_response) = self
                    .maybe_stream_select(
                        &task,
                        plan_kind,
                        resp_post_set_op,
                        addr,
                        projection,
                        result_formats,
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
                .run_pre_dispatch_hooks(identity, tenant_id, addr, plan_kind, task)
                .await?
            {
                super::execute_dml_hooks::PreDispatchOutcome::Handled(resp) => {
                    responses.push(resp);
                    continue;
                }
                super::execute_dml_hooks::PreDispatchOutcome::Proceed(proceed) => {
                    let super::execute_dml_hooks::PreDispatchProceed {
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
            let (resp, shard_watermarks) = self
                .dispatch_task_with_watermarks(task, user_id, Some(identity))
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
                || resp.error_code == Some(crate::bridge::envelope::ErrorCode::NotFound);
            if records_read
                && self.sessions.transaction_state(addr)
                    == crate::control::server::shared::session::TransactionState::InBlock
            {
                let watermarks = if shard_watermarks.is_empty() {
                    vec![(task_vshard, resp.watermark_lsn)]
                } else {
                    shard_watermarks
                };
                crate::control::server::shared::session::record_read_set(
                    &self.sessions,
                    addr,
                    identity.tenant_id,
                    &plan_for_response,
                    &watermarks,
                    resp.status == crate::bridge::envelope::Status::Ok,
                );
            }

            if let Some((severity, code, message)) =
                response_status_to_sqlstate(resp.status, &resp.error_code)
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
                    .restart_sequences_for_collection(tenant_id.as_u64(), collection);
            }

            // --- AFTER triggers ---
            if let Some(ref info) = dml_info {
                crate::control::trigger::dml_hook_fire::fire_post_dispatch_triggers(
                    crate::control::trigger::dml_hook_fire::DispatchTriggerParams {
                        state: &self.state,
                        identity,
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

                self.state
                    .dml_counter
                    .record_dml(tenant_id.as_u64(), &info.collection);
            }

            if needs_set_op && resp_post_set_op != PostSetOp::None {
                dedup_payloads.push(resp.payload.to_vec());
                if dedup_set_op == PostSetOp::None {
                    dedup_set_op = resp_post_set_op;
                }
            } else {
                match compose::shape_response_materialized(
                    &resp.payload,
                    &plan_for_response,
                    plan_kind,
                    projection,
                    &self.state,
                    task_database_id,
                    tenant_id,
                )
                .map_err(|e| sqlstate_error("XX000", e.message()))?
                {
                    ShapeOutcome::Rows(shaped) => {
                        let (response, notice) =
                            shape_encode::shaped_query_response(shaped, result_formats);
                        if let Some(n) = notice {
                            self.sessions.push_notice(addr, n);
                        }
                        responses.push(response);
                    }
                    ShapeOutcome::Passthrough => {
                        let shaped = payload_to_response(&resp.payload, plan_kind)?;
                        if let Some(notice) = shaped.notice {
                            self.sessions.push_notice(addr, notice);
                        }
                        responses.push(shaped.response);
                    }
                }
            }
        }

        // Set operations: merge sub-query payloads.
        if needs_set_op && !dedup_payloads.is_empty() {
            let (response, notice) =
                set_ops::apply_set_ops(&dedup_payloads, dedup_set_op, projection, result_formats);
            if let Some(n) = notice {
                self.sessions.push_notice(addr, n);
            }
            responses.push(response);
        }

        Ok(responses)
    }
}
