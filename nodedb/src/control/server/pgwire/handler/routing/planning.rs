// SPDX-License-Identifier: BUSL-1.1

//! SQL planning: converts SQL text into physical task lists.

use std::sync::Arc;

use pgwire::api::results::Tag;
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::types::TenantId;
use nodedb_physical::physical_task::PhysicalTask;

use super::super::super::types::error_to_sqlstate;
use super::super::core::NodeDbPgHandler;
use super::catalog::current_descriptor_version;

impl NodeDbPgHandler {
    /// Plan a SQL statement to physical tasks, handling session auth, RETURNING
    /// strip, CHECK constraints, plan cache, and RETURNING injection.
    ///
    /// This is the single planning code path shared by both the simple-query
    /// (`execute_planned_sql_inner`) and any future callers that need typed
    /// physical plans without driving the dispatch loop. Returns the ready-to-
    /// dispatch task list and the plan-lease scope that must be kept alive until
    /// dispatch completes.
    pub(in crate::control::server::pgwire::handler) async fn plan_statement_to_tasks(
        &self,
        identity: &AuthenticatedIdentity,
        sql: &str,
        tenant_id: TenantId,
        addr: &std::net::SocketAddr,
        params: &[nodedb_sql::ParamValue],
    ) -> PgWireResult<(
        Vec<PhysicalTask>,
        crate::control::server::response_shape::schema::OutputSchema,
        crate::control::lease::QueryLeaseScope,
    )> {
        // Resolve opaque session handle if SET LOCAL nodedb.auth_session is set.
        let caller_fp = crate::control::security::session_handle::ClientFingerprint::from_peer(
            identity.tenant_id,
            addr,
        );
        let conn_key = addr.to_string();
        let mut auth_ctx =
            if let Some(handle) = self.sessions.get_parameter(addr, "nodedb.auth_session") {
                use crate::control::security::session_handle::ResolveOutcome;
                match self
                    .state
                    .session_handles
                    .resolve(&handle, &conn_key, &caller_fp)
                {
                    ResolveOutcome::Resolved(cached) => *cached,
                    ResolveOutcome::RateLimited => {
                        return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                            "FATAL".to_owned(),
                            "53300".to_owned(),
                            "session handle resolve rate limit exceeded on this \
                         connection — closing"
                                .to_owned(),
                        ))));
                    }
                    ResolveOutcome::Miss => {
                        crate::control::server::session_auth::build_auth_context_with_session(
                            identity,
                            &self.sessions,
                            addr,
                        )
                    }
                }
            } else {
                crate::control::server::session_auth::build_auth_context_with_session(
                    identity,
                    &self.sessions,
                    addr,
                )
            };

        // Extract per-query ON DENY override.
        let clean_sql =
            crate::control::server::session_auth::extract_and_apply_on_deny(sql, &mut auth_ctx);

        // Strip RETURNING clause before DataFusion planning.
        let (clean_sql, returning_spec) = super::super::returning::strip_returning(&clean_sql)
            .map_err(|e| {
                use super::super::super::types::error_to_sqlstate;
                let (severity, code, message) = error_to_sqlstate(&e);
                pgwire::error::PgWireError::UserError(Box::new(pgwire::error::ErrorInfo::new(
                    severity.to_owned(),
                    code.to_owned(),
                    message,
                )))
            })?;
        let has_returning = returning_spec.is_some();

        // Propagate the tenant's vector-dimension quota so ConvertContext can
        // reject oversized vectors without an extra lock inside the planner.
        {
            let tenants = match self.state.tenants.lock() {
                Ok(t) => t,
                Err(p) => p.into_inner(),
            };
            self.query_ctx
                .set_max_vector_dim(tenants.quota(tenant_id).max_vector_dim);
        }

        // Propagate the distributed shuffle-join override from the session
        // parameter bag (set via `SET nodedb.force_shuffle_join = on` and,
        // optionally, `SET nodedb.shuffle_num_parts = N`). The values were
        // validated at SET time, so a parse miss here defaults to "off" / 0.
        let force_shuffle_join = self
            .sessions
            .get_parameter(addr, "nodedb.force_shuffle_join")
            .as_deref()
            .and_then(super::super::session_cmds::parse_bool_session_value)
            .unwrap_or(false);
        let shuffle_num_parts = self
            .sessions
            .get_parameter(addr, "nodedb.shuffle_num_parts")
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(0);
        self.query_ctx
            .set_force_shuffle_join(force_shuffle_join, shuffle_num_parts);

        // Propagate the distributed shuffle-aggregate override from the session
        // parameter bag (set via `SET nodedb.force_shuffle_agg = on` and,
        // optionally, `SET nodedb.shuffle_agg_num_parts = N`). The values were
        // validated at SET time, so a parse miss here defaults to "off" / 0.
        let force_shuffle_agg = self
            .sessions
            .get_parameter(addr, "nodedb.force_shuffle_agg")
            .as_deref()
            .and_then(super::super::session_cmds::parse_bool_session_value)
            .unwrap_or(false);
        let shuffle_agg_num_parts = self
            .sessions
            .get_parameter(addr, "nodedb.shuffle_agg_num_parts")
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(0);
        self.query_ctx
            .set_force_shuffle_agg(force_shuffle_agg, shuffle_agg_num_parts);

        // Resolve the auto-shuffle cost threshold: the session override
        // `nodedb.broadcast_threshold_bytes` when set, otherwise the node's
        // configured `[tuning.cluster_transport] broadcast_threshold_bytes`.
        // Passing the resolved value (not just the override) makes a SET then
        // RESET correctly revert to the tuning default for this session.
        let tuning_threshold = self
            .state
            .tuning
            .cluster_transport
            .broadcast_threshold_bytes;
        let session_threshold = self
            .sessions
            .get_parameter(addr, "nodedb.broadcast_threshold_bytes")
            .and_then(|v| v.parse::<usize>().ok());
        let broadcast_threshold_bytes = session_threshold.unwrap_or(tuning_threshold);
        self.query_ctx
            .set_broadcast_threshold_bytes(broadcast_threshold_bytes);

        // Resolve the auto-shuffle-aggregate cost threshold (distinct-group
        // units): the session override `nodedb.shuffle_agg_threshold` when set,
        // otherwise the planner default. Passing the resolved value (not just the
        // override) makes a SET then RESET correctly revert to the default for
        // this session. Mirrors `broadcast_threshold_bytes` above.
        let session_agg_threshold = self
            .sessions
            .get_parameter(addr, "nodedb.shuffle_agg_threshold")
            .and_then(|v| v.parse::<usize>().ok());
        let shuffle_agg_threshold = session_agg_threshold
            .unwrap_or(crate::control::planner::context::query::DEFAULT_SHUFFLE_AGG_THRESHOLD);
        self.query_ctx
            .set_shuffle_agg_threshold(shuffle_agg_threshold);

        let database_id = self
            .sessions
            .get_current_database(addr)
            .unwrap_or(crate::types::DatabaseId::DEFAULT);

        // Enforce general CHECK constraints for INSERT/UPDATE before planning.
        self.enforce_check_constraints_if_needed(&clean_sql, tenant_id, database_id)
            .await?;

        // Validate enum-typed column values for INSERT/UPDATE before planning.
        self.enforce_enum_labels_if_needed(&clean_sql, tenant_id, database_id)
            .await?;

        // A session-level `nodedb.broadcast_threshold_bytes` override changes the
        // auto-shuffle decision the same way force-shuffle does, and the cache
        // key encodes neither knob. The node-wide tuning default is constant
        // across sessions, so plans cached under it stay consistent and need no
        // bypass; only a *session override* (different from the tuning default,
        // or any explicit SET) makes the cached plan's strategy assumption
        // unsafe to share. Treat a present session override exactly like
        // force-shuffle: bypass read AND put.
        let threshold_overridden = session_threshold.is_some_and(|t| t != tuning_threshold);

        // A session-level `nodedb.shuffle_agg_threshold` override changes the
        // auto-shuffle-aggregate decision the same way the broadcast threshold
        // does, and the cache key encodes neither knob. Any explicit session
        // override that differs from the default makes a cached plan's strategy
        // assumption unsafe to share, so treat it exactly like the broadcast
        // override: bypass read AND put.
        let agg_threshold_overridden = session_agg_threshold.is_some_and(|t| {
            t != crate::control::planner::context::query::DEFAULT_SHUFFLE_AGG_THRESHOLD
        });

        // Check plan cache before full planning. The cache key is
        // `(sql_hash, schema_version)` and does NOT vary by session knob, so it
        // is bypassed entirely while the force-shuffle override OR a non-default
        // broadcast-threshold override is engaged: a cached plan built under a
        // different join-strategy assumption would otherwise be served (and a
        // strategy-specific plan must not be cached for a later default query).
        // Skipping read AND put keeps the cache strategy-knob-free.
        let bypass_cache = force_shuffle_join
            || force_shuffle_agg
            || threshold_overridden
            || agg_threshold_overridden;
        let cached_tasks = if bypass_cache {
            None
        } else {
            let state = Arc::clone(&self.state);
            let tenant = tenant_id.as_u64();
            let db = database_id;
            self.sessions.get_cached_plan(addr, &clean_sql, move |id| {
                current_descriptor_version(&state, tenant, db, id)
            })
        };

        let (tasks, output_schema, lease_scope) = if !params.is_empty() {
            let perm_cache = self.state.permission_cache.read().await;
            let sec = crate::control::planner::context::PlanSecurityContext {
                identity,
                auth: &auth_ctx,
                rls_store: &self.state.rls,
                permissions: &self.state.permissions,
                roles: &self.state.roles,
                permission_cache: Some(&*perm_cache),
            };
            let (tasks, output_schema) = self
                .query_ctx
                .plan_sql_with_params_and_rls(&clean_sql, params, tenant_id, database_id, &sec)
                .await
                .map_err(|e| {
                    let (severity, code, message) = error_to_sqlstate(&e);
                    PgWireError::UserError(Box::new(ErrorInfo::new(
                        severity.to_owned(),
                        code.to_owned(),
                        message,
                    )))
                })?;
            (
                tasks,
                output_schema,
                crate::control::lease::QueryLeaseScope::empty(),
            )
        } else if let Some((tasks, versions, output_schema)) = cached_tasks {
            let scope = self.state.acquire_plan_lease_scope(&versions);
            (tasks, output_schema, scope)
        } else {
            let (planned, output_schema, versions) =
                super::super::retry::retry_on_schema_change(|| async {
                    let perm_cache = self.state.permission_cache.read().await;
                    let sec = crate::control::planner::context::PlanSecurityContext {
                        identity,
                        auth: &auth_ctx,
                        rls_store: &self.state.rls,
                        permissions: &self.state.permissions,
                        roles: &self.state.roles,
                        permission_cache: Some(&*perm_cache),
                    };
                    self.query_ctx
                        .plan_sql_with_rls_and_versions(
                            &clean_sql,
                            tenant_id,
                            database_id,
                            &sec,
                            has_returning,
                        )
                        .await
                })
                .await
                .map_err(|e| {
                    let (severity, code, message) = error_to_sqlstate(&e);
                    PgWireError::UserError(Box::new(ErrorInfo::new(
                        severity.to_owned(),
                        code.to_owned(),
                        message,
                    )))
                })?;

            let scope = self.state.acquire_plan_lease_scope(&versions);
            // Do not cache a plan built under a strategy-knob override (force
            // shuffle, or a non-default broadcast threshold) — the cache key
            // does not encode the session knob, so caching it would leak a
            // strategy-specific plan into later default queries on this session.
            if !bypass_cache {
                self.sessions.put_cached_plan(
                    addr,
                    &clean_sql,
                    planned.clone(),
                    versions,
                    output_schema.clone(),
                );
            }
            (planned, output_schema, scope)
        };

        // Inject RETURNING spec into DML plans.
        let tasks = if let Some(ref spec) = returning_spec {
            tasks
                .into_iter()
                .map(|mut task| {
                    inject_returning_spec(&mut task.plan, spec.clone());
                    task
                })
                .collect()
        } else {
            tasks
        };

        Ok((tasks, output_schema, lease_scope))
    }
}

/// Determine read consistency for a set of tasks.
pub(super) fn consistency_for_tasks(tasks: &[PhysicalTask]) -> crate::types::ReadConsistency {
    let has_writes = tasks.iter().any(|t| {
        crate::control::wal_replication::to_replicated_entry(
            t.tenant_id,
            t.database_id,
            t.vshard_id,
            &t.plan,
        )
        .is_some()
    });

    if has_writes {
        crate::types::ReadConsistency::Strong
    } else {
        crate::types::ReadConsistency::BoundedStaleness(std::time::Duration::from_secs(5))
    }
}

/// Inject a RETURNING spec into a DML physical plan variant.
///
/// Only `PointUpdate`, `BulkUpdate`, `PointDelete`, and `BulkDelete` are
/// affected. All other plan variants are left unchanged.
pub(super) fn inject_returning_spec(
    plan: &mut crate::bridge::envelope::PhysicalPlan,
    spec: nodedb_physical::physical_plan::ReturningSpec,
) {
    use crate::bridge::envelope::PhysicalPlan;
    use nodedb_physical::physical_plan::DocumentOp;

    match plan {
        PhysicalPlan::Document(DocumentOp::PointUpdate { returning, .. }) => {
            *returning = Some(spec);
        }
        PhysicalPlan::Document(DocumentOp::BulkUpdate { returning, .. }) => {
            *returning = Some(spec);
        }
        PhysicalPlan::Document(DocumentOp::PointDelete { returning, .. }) => {
            *returning = Some(spec);
        }
        PhysicalPlan::Document(DocumentOp::BulkDelete { returning, .. }) => {
            *returning = Some(spec);
        }
        PhysicalPlan::Document(DocumentOp::UpdateFromJoin { returning, .. }) => {
            *returning = Some(spec);
        }
        _ => {}
    }
}

/// Build the pgwire response for one task of a completed Calvin batch.
///
/// A task whose plan carries a RETURNING clause emits its deleted/updated rows
/// as a `Response::Query` decoded from `apply_resp`'s Data-Plane payload — the
/// site that previously dropped those rows, surfacing a bare command tag
/// instead. Every other task (and a RETURNING task with no carried payload)
/// keeps the synthesised `Response::Execution` command tag.
pub(super) fn calvin_execution_response(
    task: &PhysicalTask,
    apply_resp: Option<&crate::bridge::envelope::Response>,
    state: &crate::control::state::SharedState,
    tenant_id: TenantId,
    database_id: crate::types::DatabaseId,
    formats: &[pgwire::api::results::FieldFormat],
) -> pgwire::error::PgWireResult<pgwire::api::results::Response> {
    use super::super::plan::{calvin_tag_for_plan, is_calvin_foldable};
    use crate::control::server::response_shape::compose::{
        ShapeOutcome, shape_response_materialized,
    };
    use crate::control::server::response_shape::types::{PlanKind, describe_plan};

    // RETURNING path: shape the applied payload into DATA-ROWs, exactly as the
    // non-Calvin dispatch loop does for a RETURNING write.
    if let (PlanKind::ReturningRows, Some(resp)) = (describe_plan(&task.plan), apply_resp)
        && let Ok(ShapeOutcome::Rows(shaped)) = shape_response_materialized(
            resp.payload.as_bytes(),
            &task.plan,
            PlanKind::ReturningRows,
            None,
            state,
            database_id,
            tenant_id,
        )
    {
        let (response, _notice) =
            super::super::shape_encode::shaped_query_response(shaped, formats);
        return Ok(response);
    }

    // Plain (non-RETURNING) write with a deposited applied Response: surface its
    // ACTUAL affected count from the payload — exactly as the non-Calvin write
    // path does — rather than a fixed synthesized tag. `None` (multishard,
    // undeposited) falls through to the synthesized tag below.
    if let Some(resp) = apply_resp
        && let PlanKind::DmlResult(_) = describe_plan(&task.plan)
    {
        return Ok(super::super::plan::payload_to_response(
            resp.payload.as_bytes(),
            describe_plan(&task.plan),
        )?
        .response);
    }

    let tag = if is_calvin_foldable(&task.plan) {
        calvin_tag_for_plan(&task.plan)?
    } else {
        Tag::new("OK")
    };
    Ok(pgwire::api::results::Response::Execution(tag))
}
