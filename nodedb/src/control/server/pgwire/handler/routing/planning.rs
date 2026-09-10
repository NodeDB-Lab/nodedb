// SPDX-License-Identifier: BUSL-1.1

//! SQL planning: converts SQL text into physical task lists, and selects the
//! read consistency a planned task set requires.
//!
//! Calvin batch response shaping lives in `calvin_response.rs`.

use std::sync::Arc;

use pgwire::error::{ErrorInfo, PgWireError};

use crate::control::security::auth_context::AuthContext;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::security::request_scope::RequestAuthScope;
use crate::control::server::shared::returning;
use crate::control::server::shared::session::SessionId;
use crate::types::{DatabaseId, TenantId};
use nodedb_physical::physical_task::PhysicalTask;

use super::super::core::NodeDbPgHandler;
use super::catalog::current_descriptor_version;
use super::setup_error::StatementSetupError;

impl NodeDbPgHandler {
    /// Run the request-admission gate exactly once for a pgwire statement.
    /// Called before DDL dispatch or DataFusion planning — `plan_statement_to_tasks` must not admit again.
    pub(in crate::control::server::pgwire::handler) async fn admit_statement(
        &self,
        identity: &AuthenticatedIdentity,
        session_id: SessionId,
        database_id: DatabaseId,
    ) -> pgwire::error::PgWireResult<()> {
        let peer_addr = match session_id {
            SessionId::Connection(connection_id) => self
                .sessions
                .connection_metadata(connection_id)
                .map(|metadata| metadata.peer_addr)
                .ok_or_else(|| {
                    PgWireError::UserError(Box::new(ErrorInfo::new(
                        "FATAL".to_owned(),
                        "XX000".to_owned(),
                        "connection session metadata is unavailable".to_owned(),
                    )))
                })?,
            SessionId::LegacySocket(peer_addr) => peer_addr,
        };
        let peer_addr = peer_addr.to_string();
        let request = RequestAuthScope::builder(identity, self.state.auth_stores())
            .with_session_database(Some(database_id))
            .build_for_client(&peer_addr);
        crate::control::server::session_auth::check_request_admission(&self.state, &request, "sql")
            .map_err(|e| {
                let (severity, code, message) =
                    crate::control::server::pgwire::types::error_to_sqlstate(&e);
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    severity.to_owned(),
                    code.to_owned(),
                    message,
                )))
            })?;
        Ok(())
    }

    /// Plan a SQL statement to physical tasks: session auth, RETURNING strip,
    /// CHECK constraints, plan cache, RETURNING injection. Returns the task list
    /// and descriptor versions; errors stay typed to distinguish a descriptor-drain race from terminal failure.
    pub(in crate::control::server::pgwire::handler) async fn plan_statement_to_tasks(
        &self,
        identity: &AuthenticatedIdentity,
        sql: &str,
        tenant_id: TenantId,
        session_id: SessionId,
        params: &[nodedb_sql::ParamValue],
    ) -> Result<
        (
            Vec<PhysicalTask>,
            crate::control::server::response_shape::schema::OutputSchema,
            crate::control::planner::descriptor_set::DescriptorVersionSet,
            AuthContext,
        ),
        StatementSetupError,
    > {
        let peer_addr = match session_id {
            SessionId::Connection(connection_id) => self
                .sessions
                .connection_metadata(connection_id)
                .map(|metadata| metadata.peer_addr)
                .ok_or_else(|| {
                    StatementSetupError::protocol(
                        "FATAL",
                        "XX000",
                        "connection session metadata is unavailable",
                    )
                })?,
            SessionId::LegacySocket(peer_addr) => peer_addr,
        };
        let caller_fp = crate::control::security::session_handle::ClientFingerprint::from_peer(
            identity.tenant_id,
            &peer_addr,
        );
        let conn_key = format!("{session_id:?}");

        // Resolved once, up front, so every downstream consumer reads the same
        // value instead of re-querying session state and risking drift.
        let database_id = self
            .sessions
            .get_current_database(session_id)
            .unwrap_or(crate::types::DatabaseId::DEFAULT);

        // Resolve opaque session handle if SET LOCAL nodedb.auth_session is set.
        let adopted_auth_ctx = if let Some(handle) = self
            .sessions
            .get_parameter(session_id, "nodedb.auth_session")
        {
            use crate::control::security::session_handle::ResolveOutcome;
            match self
                .state
                .session_handles
                .resolve(&handle, &conn_key, &caller_fp)
            {
                ResolveOutcome::Resolved(cached) => Some(*cached),
                ResolveOutcome::RateLimited => {
                    return Err(StatementSetupError::protocol(
                        "FATAL",
                        "53300",
                        "session handle resolve rate limit exceeded on this \
                         connection — closing",
                    ));
                }
                ResolveOutcome::Miss => None,
            }
        } else {
            None
        };

        // Session-level `ON DENY` override lives only in session parameters.
        let session_on_deny = crate::control::server::session_auth::session_on_deny_override(
            &self.sessions,
            session_id,
        );

        // Adopt the pooled handle's cached context when present, else build fresh
        // from `identity`, running scope-grant enrichment either way.
        let mut scope_builder = RequestAuthScope::builder(identity, self.state.auth_stores())
            .with_session_database(Some(database_id))
            .with_on_deny(session_on_deny);
        if let Some(adopted) = adopted_auth_ctx {
            scope_builder = scope_builder.with_adopted_auth_context(adopted);
        }
        // Resolved against the same connection address as `admit_statement`'s scope,
        // else `$auth.risk_score` is unset and an IP-conditional grant withheld.
        let scope = scope_builder
            .build_for_client(&peer_addr.to_string())
            .into_resolved_scope();

        // Request-admission already ran once in `execute_single_sql` — must not admit again.

        // Per-query ON DENY always wins over the session-level override in `scope`.
        let (statement_sql, scope) =
            crate::control::server::session_auth::apply_per_query_on_deny(sql, scope);

        // Strip RETURNING clause before DataFusion planning.
        let (clean_sql, returning_spec) =
            returning::strip_returning(&statement_sql).map_err(StatementSetupError::from)?;

        // Forwards per-session planning GUCs into the shared query context, protocol-neutral
        // so pgwire and native honor them identically; flags drive the cache bypass below.
        let override_flags =
            crate::control::server::shared::planning_overrides::apply_planning_session_overrides(
                &self.query_ctx,
                &self.sessions,
                &self.state,
                session_id,
                tenant_id,
            );

        // Authoritative for both `PhysicalTask::database_id` and `$auth.database_id`.
        let database_id = scope.database_id();

        // Enforce general CHECK constraints for INSERT/UPDATE before planning.
        self.enforce_check_constraints_if_needed(
            &clean_sql,
            identity,
            tenant_id,
            database_id,
            scope.auth(),
        )
        .await
        .map_err(StatementSetupError::from)?;

        // Validate enum-typed column values for INSERT/UPDATE before planning.
        self.enforce_enum_labels_if_needed(&clean_sql, tenant_id, database_id)
            .await
            .map_err(StatementSetupError::from)?;

        // Cache key isn't session-knob-scoped, so bypass entirely under a strategy
        // override — else a plan built for one join strategy serves a differently-tuned query.
        let bypass_cache = override_flags.bypass_plan_cache();

        // A cached plan carries the RLS predicates and permission-tree filter
        // injected when it was built. Both stores keep a per-tenant version
        // bumped on every mutation; a cache hit re-validates the stamped
        // versions against these live values so a revoked grant or dropped
        // policy evicts the entry instead of replaying a frozen filter.
        let current_permission_tree_version = {
            let perm_cache = self.state.permission_cache.read().await;
            perm_cache.tenant_version(tenant_id.as_u64())
        };
        let current_rls_version = self.state.rls.tenant_version(tenant_id.as_u64());

        let cached_tasks = if bypass_cache {
            None
        } else {
            let state = Arc::clone(&self.state);
            let tenant = tenant_id.as_u64();
            let db = database_id;
            // Keyed on the statement INCLUDING its `RETURNING` clause: the
            // clause is stripped before planning, so two statements that
            // differ only in what they project share the stripped text — and
            // the cached entry carries the announced output columns.
            self.sessions.get_cached_plan(
                session_id,
                &statement_sql,
                move |id| current_descriptor_version(&state, tenant, db, id),
                current_permission_tree_version,
                current_rls_version,
            )
        };

        let (tasks, output_schema, versions) = if !params.is_empty() {
            let perm_cache = self.state.permission_cache.read().await;
            let sec = crate::control::planner::context::PlanSecurityContext {
                identity,
                auth: scope.auth(),
                rls_store: &self.state.rls,
                redaction_store: &self.state.redaction,
                permissions: &self.state.permissions,
                roles: &self.state.roles,
                permission_cache: Some(&*perm_cache),
            };
            let (tasks, output_schema, versions) = self
                .query_ctx
                .plan_sql_with_params_and_rls_and_versions(
                    &clean_sql,
                    params,
                    tenant_id,
                    database_id,
                    &sec,
                    returning_spec.as_ref(),
                )
                .await
                .map_err(StatementSetupError::from)?;
            (tasks, output_schema, versions)
        } else if let Some((tasks, versions, output_schema)) = cached_tasks {
            // Redaction refusal is a property of the current policy, not the compiled
            // plan; policy writes don't bump descriptor versions, so this re-runs every cache hit.
            crate::control::planner::redaction_refusal::refuse_unredactable_tasks(
                &tasks,
                scope.auth(),
                &self.state.redaction,
            )
            .map_err(StatementSetupError::from)?;
            (tasks, output_schema, versions)
        } else {
            let (planned, output_schema, versions, cache_eligibility) = {
                let perm_cache = self.state.permission_cache.read().await;
                let sec = crate::control::planner::context::PlanSecurityContext {
                    identity,
                    auth: scope.auth(),
                    rls_store: &self.state.rls,
                    redaction_store: &self.state.redaction,
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
                        returning_spec.as_ref(),
                    )
                    .await
                    .map_err(StatementSetupError::from)?
            };

            // Strategy overrides aren't in the cache key: caching a resolved PK→surrogate
            // binding would preserve stale row identity across later writes.
            if !bypass_cache && cache_eligibility.is_cacheable() {
                self.sessions.put_cached_plan(
                    session_id,
                    &statement_sql,
                    planned.clone(),
                    versions.clone(),
                    output_schema.clone(),
                );
            }
            (planned, output_schema, versions)
        };

        // Inject RETURNING spec into DML plans. An insert shape with no `returning`
        // slot is refused rather than silently dropped.
        let tasks = if let Some(ref spec) = returning_spec {
            let mut injected = Vec::with_capacity(tasks.len());
            for mut task in tasks {
                returning::refuse_unprojectable_insert_returning(&task.plan)
                    .map_err(StatementSetupError::from)?;
                returning::inject_returning_spec(&mut task.plan, spec.clone());
                injected.push(task);
            }
            injected
        } else {
            tasks
        };

        // Preauthorize before expansion allocates surrogates; descriptor admission
        // waits for the expanded task set's final authorization in the execute path.
        let _preauthorized_tasks = self
            .authorize_tasks(identity, &tasks)
            .map_err(StatementSetupError::from)?;

        // Caller only needs the resolved `AuthContext` by value from here on;
        // `scope` itself does not need to outlive this function.
        Ok((tasks, output_schema, versions, scope.auth().clone()))
    }
}

/// Whether any task in the set replicates through Raft.
/// A task whose encode refuses still counts — it belongs on the Raft path.
pub(super) fn has_replicated_writes(tasks: &[PhysicalTask]) -> bool {
    tasks.iter().any(|t| {
        match crate::control::wal_replication::ReplicableWrite::decide_for_replication(&t.plan) {
            // Refused: still a write that belongs on the Raft path.
            Err(_) => true,
            Ok(replicable) => !matches!(
                crate::control::wal_replication::to_replicated_entry(
                    t.tenant_id,
                    t.database_id,
                    t.vshard_id,
                    &replicable,
                ),
                Ok(None)
            ),
        }
    })
}

/// Determine read consistency for a set of tasks.
/// A write always goes to the leader; a read takes the session's
/// `default_read_consistency` (`Strong` until a client sets otherwise).
pub(super) fn consistency_for_tasks(
    sessions: &crate::control::server::shared::session::SessionStore,
    tasks: &[PhysicalTask],
    session_id: crate::control::server::shared::session::SessionId,
) -> crate::types::ReadConsistency {
    if has_replicated_writes(tasks) {
        return crate::types::ReadConsistency::Strong;
    }
    sessions.read_consistency(session_id)
}
