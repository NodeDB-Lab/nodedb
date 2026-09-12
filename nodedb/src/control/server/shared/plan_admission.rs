// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral statement setup: plan, authorize, extract implicit edges,
//! resolve materialized-sum targets, authorize again, and admit the descriptor
//! leases — as ONE retried unit.
//!
//! Planning reads the catalog and records a descriptor version; the lease that
//! pins that version is only acquired afterwards. A descriptor drain that starts
//! between those two steps fails the acquisition, so both must live inside the
//! same retry budget or the drain surfaces to the client as a hard error. The
//! acquisition fails closed before any lease is granted, so a retried attempt
//! never re-reads data it was not entitled to.
//!
//! Every step here is safe to re-run: planning is pure, the edge-bearing catalog
//! flag is a read-then-conditional-write, endpoint surrogates resolve
//! get-or-create against a stable key, and a failed admission rolls its own
//! refcounts back before returning.

use std::sync::Arc;

use nodedb_physical::physical_task::PhysicalTask;

use crate::control::planner::context::{PlanSecurityContext, QueryContext};
use crate::control::planner::descriptor_set::DescriptorVersionSet;
use crate::control::security::audit::ArcAuditEmitter;
use crate::control::security::request_scope::RequestAuthScope;
use crate::control::server::response_shape::schema::OutputSchema;
use crate::control::server::shared::authorization::authorize_task_set;
use crate::control::server::shared::retry::retry_on_schema_change;
use crate::control::state::SharedState;
use crate::types::TraceId;

/// Everything a statement needs before dispatch can begin.
pub struct PlanAdmission {
    /// The planned task list, including any appended implicit-edge tasks.
    /// The caller clone-checks and authorizes each task itself, immediately
    /// before dispatch, via `shared::clone_write::intercept_and_authorize` —
    /// this set is NOT pre-authorized, so a batch-authorize-then-loop cannot
    /// silently reintroduce the retarget-before-clone-check bug.
    pub tasks: Vec<PhysicalTask>,
    /// Output schema for the planned statement.
    pub output_schema: OutputSchema,
    /// Descriptor versions this statement was planned against.
    pub versions: DescriptorVersionSet,
    /// Descriptor lease holds; must stay alive for the whole execution.
    pub lease_scope: crate::control::lease::QueryLeaseScope,
    /// Read-set entries covering the row images every CROSS-SHARD
    /// materialized-sum balance in `tasks` was settled from.
    ///
    /// The caller must union these into the read-set it dispatches with. They
    /// are what makes the Calvin OCC check abort the statement — before any row
    /// moves — when the images a shipped balance was folded from have been
    /// written since. Empty for every statement that settled no cross-shard
    /// balance, which is every statement on a collection with no binding.
    pub sum_target_reads: Vec<crate::control::server::shared::session::read_set::ReadSetEntry>,
}

/// Inputs for [`plan_authorize_and_admit`].
pub struct PlanAdmissionRequest<'a> {
    pub state: &'a Arc<SharedState>,
    pub query_ctx: &'a QueryContext,
    /// The resolved, request-scoped auth contract: identity, enriched
    /// `AuthContext`, tenant, and database all bundled and guaranteed to
    /// agree with each other. See [`RequestAuthScope`].
    pub scope: &'a RequestAuthScope<'a>,
    /// SQL with any per-query `ON DENY` override already stripped.
    pub sql: &'a str,
    pub trace_id: TraceId,
}

/// Plan `sql`, authorize it, expand implicit graph edges, authorize the expanded
/// set, and acquire the descriptor leases — retrying the whole unit while a
/// descriptor drain is in flight.
pub async fn plan_authorize_and_admit(
    request: PlanAdmissionRequest<'_>,
) -> crate::Result<PlanAdmission> {
    let request = &request;
    retry_on_schema_change(move || plan_authorize_and_admit_once(request)).await
}

/// One attempt of the setup unit. Split out so the retry closure stays a plain
/// re-invocation with no partial state carried between attempts.
async fn plan_authorize_and_admit_once(
    request: &PlanAdmissionRequest<'_>,
) -> crate::Result<PlanAdmission> {
    let state = request.state;
    let query_ctx = request.query_ctx;
    let identity = request.scope.identity();
    let auth_ctx = request.scope.auth();
    let sql = request.sql;
    let tenant_id = request.scope.tenant_id();
    let database_id = request.scope.database_id();
    let trace_id = request.trace_id;

    // Re-read per attempt: a retry must plan against the catalog and permission
    // state as they are NOW, not as they were when the drained attempt started.
    let (mut tasks, output_schema, versions) = {
        let permission_cache = state.permission_cache.read().await;
        let security = PlanSecurityContext {
            identity,
            auth: auth_ctx,
            rls_store: &state.rls,
            redaction_store: &state.redaction,
            permissions: &state.permissions,
            roles: &state.roles,
            permission_cache: Some(&*permission_cache),
        };
        let (tasks, output_schema, versions, _cache_eligibility) = query_ctx
            .plan_sql_with_rls_and_versions(sql, tenant_id, database_id, &security, None)
            .await?;
        (tasks, output_schema, versions)
    };

    let emitter = ArcAuditEmitter(Arc::clone(&state.audit));

    // Implicit-edge extraction marks catalog state and allocates surrogates, so
    // the originally planned tasks must clear authorization before it runs.
    let _preauthorized_tasks =
        authorize_task_set(identity, &tasks, &state.permissions, &state.roles, &emitter)?;

    crate::control::planner::implicit_edges::append_implicit_edge_tasks(
        state,
        &mut tasks,
        tenant_id,
        database_id,
        trace_id,
    )
    .await?;

    let sum_target_reads =
        crate::control::planner::materialized_sum::resolve_materialized_sum_targets(
            state,
            &mut tasks,
            tenant_id,
            database_id,
            trace_id,
        )
        .await?;

    // Follows the resolution: it consumes the surrogates that pass bound, and
    // issues no lookup of its own.
    crate::control::planner::materialized_sum::append_cross_shard_balance_tasks(
        state,
        &mut tasks,
        tenant_id,
        database_id,
    )?;

    // Resolves each write's period-lock reference row into the same slot the
    // materialized-sum resolution above just populated — see
    // `period_lock::resolve_period_lock_targets`.
    crate::control::planner::period_lock::resolve_period_lock_targets(
        state,
        &mut tasks,
        tenant_id,
        database_id,
        trace_id,
    )
    .await?;

    // Deliberate gate: proves the final task set is authorizable before a
    // descriptor lease is acquired. The caller re-derives the capability per
    // task through the clone-check gate, immediately before each dispatch.
    let _authorized_tasks =
        authorize_task_set(identity, &tasks, &state.permissions, &state.roles, &emitter)?;

    // Admission follows authorization so a denied statement never consumes a
    // descriptor lease.
    let lease_scope = state.acquire_plan_lease_scope(&versions)?;

    Ok(PlanAdmission {
        tasks,
        output_schema,
        versions,
        lease_scope,
        sum_target_reads,
    })
}
