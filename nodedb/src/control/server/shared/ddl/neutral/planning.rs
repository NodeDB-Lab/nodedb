// SPDX-License-Identifier: BUSL-1.1

//! RLS-aware planning and authorization for neutral DDL readback queries.

use std::sync::Arc;

use nodedb_physical::physical_task::PhysicalTask;
use nodedb_types::DatabaseId;

use crate::control::lease::QueryLeaseScope;
use crate::control::planner::context::{PlanSecurityContext, QueryContext};
use crate::control::security::audit::ArcAuditEmitter;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::security::request_scope::RequestAuthScope;
use crate::control::server::response_shape::schema::OutputSchema;
use crate::control::server::shared::authorization::authorize_task_set;
use crate::control::server::shared::ddl::result::DdlError;
use crate::control::state::SharedState;

/// Plan a neutral DDL readback query and acquire the descriptor leases needed
/// through the Data Plane response.
///
/// Neutral DDL handlers reconstruct a small number of internal scans. They must
/// use the same RLS-aware planning and descriptor lease admission boundaries as
/// external query transports before dispatching those scans to the Data Plane.
/// The returned tasks are NOT pre-authorized: the caller clone-checks and
/// authorizes each one itself, immediately before dispatch, via
/// `shared::clone_write::intercept_and_authorize` (or
/// `intercept_authorize_and_dispatch` for the common read-only case) — never a
/// batch authorize followed by a dispatch loop.
pub async fn plan_authorized_sql(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
    database_id: DatabaseId,
) -> Result<(Vec<PhysicalTask>, OutputSchema, QueryLeaseScope), DdlError> {
    // Internal DDL scans still plan in the caller-selected database context.
    let scope = RequestAuthScope::for_database(identity, state.auth_stores(), database_id);
    let permission_cache = state.permission_cache.read().await;
    let sec = PlanSecurityContext {
        identity,
        auth: scope.auth(),
        rls_store: &state.rls,
        redaction_store: &state.redaction,
        permissions: &state.permissions,
        roles: &state.roles,
        permission_cache: Some(&*permission_cache),
    };
    let query_ctx = QueryContext::for_state(state);
    let (tasks, output_schema, versions, _) = query_ctx
        .plan_sql_with_rls_and_versions(sql, identity.tenant_id, database_id, &sec, None)
        .await
        .map_err(|error| DdlError::new("42601", format!("query planning failed: {error}")))?;

    let emitter = ArcAuditEmitter(Arc::clone(&state.audit));
    // Deliberate gate: proves the planned set is authorizable before a
    // descriptor lease is acquired. The caller re-derives the capability per
    // task through the clone-check gate, immediately before each dispatch.
    let _preauthorized_tasks =
        authorize_task_set(identity, &tasks, &state.permissions, &state.roles, &emitter).map_err(
            |error| {
                DdlError::new(
                    nodedb_types::error::sqlstate::INSUFFICIENT_PRIVILEGE.to_string(),
                    error.resource().to_string(),
                )
            },
        )?;

    // Admission follows authorization so denied requests never consume
    // descriptor leases. Callers retain this scope through dispatch and
    // response consumption.
    let lease_scope = state.acquire_plan_lease_scope(&versions).map_err(|error| {
        let (_, sqlstate, message) =
            crate::control::server::pgwire::types::error_to_sqlstate(&error);
        DdlError::new(sqlstate.to_string(), message)
    })?;

    Ok((tasks, output_schema, lease_scope))
}
