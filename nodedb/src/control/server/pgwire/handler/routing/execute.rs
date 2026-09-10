// SPDX-License-Identifier: BUSL-1.1

//! Plan-and-dispatch entry points for SQL queries on the simple-query and
//! extended-query (prepared-statement) paths.
//!
//! The per-task dispatch loop these entry points hand off to lives in
//! [`super::dispatch_loop`].

use std::sync::Arc;

use pgwire::api::results::{Response, Tag};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::control::planner::calvin::{DispatchClass, classify_dispatch};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::session::SessionId;
use crate::control::server::shared::write_admission::all_writes_bufferable;
use crate::types::TenantId;

use super::super::super::types::error_to_sqlstate;
use super::super::core::NodeDbPgHandler;
use super::dispatch_loop::DispatchTaskContext;
use super::result_shaping::ResultShaping;
use super::setup_error::StatementSetupError;
use crate::control::server::shared::retry::retry_on_schema_change;

impl NodeDbPgHandler {
    pub(super) async fn execute_planned_sql_inner(
        &self,
        identity: &AuthenticatedIdentity,
        sql: &str,
        tenant_id: TenantId,
        session_id: SessionId,
        params: &[nodedb_sql::ParamValue],
        shaping: ResultShaping<'_>,
    ) -> PgWireResult<Vec<Response>> {
        // Planning records the descriptor versions; the leases that pin them are
        // only acquired at the end of this block. A descriptor drain starting in
        // between fails the acquisition, so the whole setup — plan, authorize,
        // expand implicit edges, authorize again, admit — runs as ONE retried
        // unit under a single budget. Every step is safe to re-run: planning is
        // pure, the edge-bearing catalog flag is a read-then-conditional-write,
        // endpoint surrogates resolve get-or-create against a stable key, and a
        // failed admission rolls its own refcounts back.
        let edge_database_id = self
            .sessions
            .get_current_database(session_id)
            .unwrap_or(crate::types::DatabaseId::DEFAULT);
        let (tasks, output_schema, auth_ctx, plan_lease_scope, sum_target_reads) =
            retry_on_schema_change(move || async move {
                let (mut tasks, output_schema, versions, auth_ctx) = self
                    .plan_statement_to_tasks(identity, sql, tenant_id, session_id, params)
                    .await?;

                // Extraction marks catalog state and allocates surrogates.
                // Authorize the original planned tasks before it can perform
                // either side effect.
                let _preauthorized_tasks = self
                    .authorize_tasks(identity, &tasks)
                    .map_err(StatementSetupError::from)?;

                // Implicit graph-edge extraction: a schemaless document carrying
                // `_from`/`_to` is mirrored as a `GraphOp::EdgePut` task, homed
                // and surrogate-resolved per endpoint so it routes through the
                // same classify/Calvin/single-shard path as an explicit edge.
                crate::control::planner::implicit_edges::append_implicit_edge_tasks(
                    &self.state,
                    &mut tasks,
                    tenant_id,
                    edge_database_id,
                    crate::types::TraceId::ZERO,
                )
                .await
                .map_err(StatementSetupError::from)?;

                // Materialized-sum target rows are resolved here for the same
                // reason: the PK→surrogate map lives in the catalog redb, which
                // is Control-Plane state.
                // The entries cover the row images every cross-shard balance
                // this pass settled was folded from; they travel on the
                // dispatch read-set so Calvin's OCC check aborts rather than
                // committing a total folded from an image that has moved.
                let sum_target_reads =
                    crate::control::planner::materialized_sum::resolve_materialized_sum_targets(
                        &self.state,
                        &mut tasks,
                        tenant_id,
                        edge_database_id,
                        crate::types::TraceId::ZERO,
                    )
                    .await
                    .map_err(StatementSetupError::from)?;

                // A target that does not share the source's vShard cannot ride
                // the source write's transaction, so its balance is appended as
                // its own task, homed on the target — the classifier then
                // dual-homes the pair through Calvin.
                crate::control::planner::materialized_sum::append_cross_shard_balance_tasks(
                    &self.state,
                    &mut tasks,
                    tenant_id,
                    edge_database_id,
                )
                .map_err(StatementSetupError::from)?;

                // The final task set must be authorized before any clone
                // interception, orchestration, staging, or dispatch path can
                // observe it. Descriptor admission follows this check so an
                // implicit-edge denial consumes no descriptor lease.
                let _authorized_tasks = self
                    .authorize_tasks(identity, &tasks)
                    .map_err(StatementSetupError::from)?;
                let plan_lease_scope = self
                    .state
                    .acquire_plan_lease_scope(&versions)
                    .map_err(StatementSetupError::from)?;

                Ok::<_, StatementSetupError>((
                    tasks,
                    output_schema,
                    auth_ctx,
                    plan_lease_scope,
                    sum_target_reads,
                ))
            })
            .await
            .map_err(PgWireError::from)?;
        let plan_lease_scope = Arc::new(plan_lease_scope);

        if tasks.is_empty() {
            return Ok(vec![Response::Execution(Tag::new("OK"))]);
        }

        // An externally-supplied prepared-statement schema (from the Describe
        // phase) wins; otherwise use the planner's fresh output schema for this
        // statement.
        let effective_schema = shaping.projection.or(Some(&output_schema));

        // Implicit-edge dependent predicates must be preempted onto the
        // OLLP/Calvin path before gateway forwarding or ordinary dispatch.
        if let Some(responses) = self
            .maybe_dispatch_implicit_edge_recon(
                &tasks,
                tenant_id,
                identity,
                session_id,
                ResultShaping {
                    projection: effective_schema,
                    formats: shaping.formats,
                },
                &auth_ctx,
            )
            .await?
        {
            return Ok(responses);
        }

        if let Some(responses) = self
            .maybe_dispatch_tasks_via_gateway(
                &tasks,
                identity,
                tenant_id,
                session_id,
                ResultShaping {
                    projection: effective_schema,
                    formats: shaping.formats,
                },
                &auth_ctx,
            )
            .await?
        {
            return Ok(responses);
        }

        let tx_state = self.sessions.transaction_state(session_id);
        // Autocommit statement routing: the only reads to widen with are the
        // ones the materialized-sum settlement stamped on the source rows its
        // shipped balances were folded from.
        let sum_read_vshards = crate::control::planner::calvin::read_vshards_of(&sum_target_reads);
        match classify_dispatch(&tasks, &sum_read_vshards) {
            DispatchClass::SingleShard { .. } => {
                // A single-shard dependent-predicate write (e.g. `DELETE ...
                // WHERE <non-pk>`) doesn't need OLLP/Calvin: one shard is one
                // Raft group, so the normal replicated-write dispatch path
                // applies it deterministically. Edge-bearing dependent
                // predicates are already preempted onto Calvin above; only
                // genuine multi-shard bulk writes need OLLP. Fall through.
            }
            DispatchClass::MultiShard { .. } => {
                // Dispatching to Calvin here would apply the statement durably
                // at statement time, escaping the transaction buffer. Inside a
                // block, fall through to the per-task staging gate when the gate
                // can buffer every write — COMMIT then flushes the whole buffer
                // through Calvin. Anything else is refused, never applied.
                if tx_state == crate::control::server::shared::session::TransactionState::InBlock {
                    if !all_writes_bufferable(&tasks) {
                        let (severity, code, message) =
                            error_to_sqlstate(&crate::Error::CrossShardInExplicitTransaction);
                        return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                            severity.to_owned(),
                            code.to_owned(),
                            message,
                        ))));
                    }
                    // Bufferable: fall through to the staging gate below.
                } else {
                    let cross_shard_mode = self.sessions.cross_shard_txn_mode(session_id);
                    if cross_shard_mode
                        == crate::control::server::shared::session::cross_shard_mode::CrossShardTxnMode::Strict
                    {
                        return self
                            .dispatch_calvin_multishard(
                                tasks,
                                tenant_id,
                                super::calvin_dispatch::CalvinDispatchSession {
                                    identity,
                                    session_id,
                                    result_formats: shaping.formats,
                                    auth: &auth_ctx,
                                    projection: effective_schema,
                                },
                                &sum_target_reads,
                            )
                            .await;
                    }
                }
            }
        }

        self.dispatch_task_loop(
            tasks,
            DispatchTaskContext {
                plan_lease_scope: Arc::clone(&plan_lease_scope),
                tenant_id,
                identity,
                auth_ctx: &auth_ctx,
                session_id,
                shaping: ResultShaping {
                    projection: effective_schema,
                    formats: shaping.formats,
                },
            },
        )
        .await
    }
}
