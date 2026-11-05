// SPDX-License-Identifier: BUSL-1.1

//! The dispatch capability gate: clone-write interception, clone-read
//! interception, and authorization merged into one step, so a caller can
//! only reach the Data-Plane dispatch boundary through
//! [`intercept_and_authorize`]. A caller holding a bare `AuthorizedTask` has
//! no way to produce a [`CloneCheckedTask`] itself, so an entry point that
//! forgets either clone hook fails to compile instead of silently bypassing it.

use nodedb_cluster::{DescriptorId, DescriptorKind};
use nodedb_physical::physical_task::PhysicalTask;
use nodedb_types::{DatabaseId, TenantId};

use crate::bridge::envelope::{PhysicalPlan, Response};
use crate::control::gateway::version_set::touched_collections;
use crate::control::lease::QueryLeaseScope;
use crate::control::planner::descriptor_set::DescriptorVersionSet;
use crate::control::security::audit::AuditEmitter;
use crate::control::security::identity::{AuthenticatedIdentity, Permission, required_permission};
use crate::control::security::permission::PermissionStore;
use crate::control::security::role::RoleStore;
use crate::control::server::shared::authorization::{AuthorizedTask, authorize_task_set};
use crate::control::server::shared::write_admission::check_mirror_read;
use crate::control::state::SharedState;
use crate::types::{TraceId, VShardId};

use super::entry::{CloneWriteOutcome, maybe_intercept_clone_write};

/// A physical task that has passed clone-write interception, then
/// authorization, in that order. Only [`intercept_and_authorize`] can produce
/// one — every dispatch boundary that reaches the Data Plane consumes this
/// type instead of a bare `AuthorizedTask`.
///
/// Boxed so this stays small relative to [`Response`]: `AuthorizedTask` wraps
/// a `PhysicalTask`, whose `PhysicalPlan` is the largest enum in the crate, so
/// an unboxed field here will force [`CloneCheckedOutcome`] to size itself to
/// the bigger of the two variants either way.
///
/// A write carries a descriptor lease on every collection it touches, from
/// authorization until the caller drops the lease after the write's outcome.
/// A descriptor drain waits for that lease on every node, so no write to a
/// drained collection is in flight once the drain returns.
pub struct CloneCheckedTask {
    task: Box<AuthorizedTask>,
    lease: QueryLeaseScope,
}

impl CloneCheckedTask {
    /// Unwrap into the authorization capability and the write's descriptor
    /// lease. The caller holds the lease until the dispatch returns its
    /// outcome; dropping it earlier lets a drain pass a write still in flight.
    pub fn into_parts(self) -> (AuthorizedTask, QueryLeaseScope) {
        (*self.task, self.lease)
    }

    pub fn tenant_id(&self) -> TenantId {
        self.task.tenant_id()
    }

    pub fn database_id(&self) -> DatabaseId {
        self.task.database_id()
    }

    pub fn vshard_id(&self) -> VShardId {
        self.task.vshard_id()
    }

    pub fn txn_id(&self) -> Option<crate::types::TxnId> {
        self.task.txn_id()
    }

    pub fn plan(&self) -> &PhysicalPlan {
        self.task.plan()
    }

    /// The roles of the identity the task was authorized for.
    pub fn user_roles(&self) -> Vec<String> {
        self.task.identity().role_names()
    }
}

/// Take a descriptor lease on every collection a write-class `task` touches,
/// at the version the planner leases.
///
/// Every write entry point takes this lease, SQL or not, so the descriptor
/// drain gates them all: a collection under drain refuses the write as
/// `RetryableSchemaChanged`, and the drain waits for writes already holding
/// the lease. A read, and a collection the catalog does not name, take none.
pub async fn write_lease(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    plan: &PhysicalPlan,
) -> crate::Result<QueryLeaseScope> {
    if required_permission(plan) == Permission::Read {
        return Ok(QueryLeaseScope::empty());
    }
    let mut versions = DescriptorVersionSet::new();
    record_collections(
        state,
        tenant_id,
        database_id,
        touched_collections(plan),
        &mut versions,
    )?;
    // Array DDL and cell writes lease the array's descriptor, so an array
    // drain gates them like a collection drain gates row writes.
    let array = match plan {
        PhysicalPlan::Array(op) => Some(op.primary_array()),
        PhysicalPlan::ClusterArray(op) => Some(op.array_id()),
        _ => None,
    };
    if let Some(array) = array {
        versions.record(array_descriptor(array), ARRAY_DESCRIPTOR_VERSION);
    }
    state.acquire_plan_lease_scope(&versions).await
}

/// Arrays carry no descriptor version. Every array lease and drain uses this
/// one, so a drain on an array covers every write to it.
pub const ARRAY_DESCRIPTOR_VERSION: u64 = 1;

/// The descriptor an array's lease and drain name.
pub fn array_descriptor(array: &nodedb_array::types::ArrayId) -> DescriptorId {
    DescriptorId::new(
        array.database_id.as_u64(),
        array.tenant_id.as_u64(),
        DescriptorKind::Array,
        array.name.clone(),
    )
}

/// Take a descriptor lease on each named collection (database-qualified or
/// bare), for a write whose collections are known without a plan.
pub async fn collections_write_lease(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    collections: impl IntoIterator<Item = String>,
) -> crate::Result<QueryLeaseScope> {
    let mut versions = DescriptorVersionSet::new();
    record_collections(state, tenant_id, database_id, collections, &mut versions)?;
    state.acquire_plan_lease_scope(&versions).await
}

/// Record the descriptor of every named collection the catalog holds.
fn record_collections(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    collections: impl IntoIterator<Item = String>,
    versions: &mut DescriptorVersionSet,
) -> crate::Result<()> {
    let catalog = state.credentials.catalog();
    for qualified in collections {
        let key = nodedb_types::CollectionKey::from_relation_name(database_id, &qualified)?;
        let Some(stored) = catalog.get_collection(database_id, tenant_id.as_u64(), key.name())?
        else {
            continue;
        };
        versions.record(
            DescriptorId::new(
                database_id.as_u64(),
                tenant_id.as_u64(),
                DescriptorKind::Collection,
                stored.name.clone(),
            ),
            stored.descriptor_version.max(1),
        );
    }
    Ok(())
}

/// Outcome of [`intercept_and_authorize`].
pub enum CloneCheckedOutcome {
    /// The clone-write hook fully handled the write; use this response.
    Handled(Response),
    /// Clear to dispatch (not clone-relevant, or the plan was retargeted).
    Proceed(CloneCheckedTask),
}

/// Inputs for [`intercept_and_authorize`] (and, with a `trace_id`, for
/// [`intercept_authorize_and_dispatch`]). Grouped into a struct because the
/// gate needs everything `maybe_intercept_clone_write` and `authorize_task_set`
/// each need — state, the task, the requester's identity and tenant, and the
/// authorization stores — which exceeds a readable positional argument count.
pub struct InterceptAndAuthorizeParams<'a> {
    pub state: &'a SharedState,
    pub task: PhysicalTask,
    pub identity: &'a AuthenticatedIdentity,
    pub tenant_id: TenantId,
    pub permissions: &'a PermissionStore,
    pub roles: &'a RoleStore,
    pub emitter: &'a dyn AuditEmitter,
}

/// Clone-check, then authorize, one physical task — the single function that
/// can produce a [`CloneCheckedTask`].
///
/// `classify()` inside [`maybe_intercept_clone_write`] is `O(1)` with no I/O
/// for a read plan (`ANALYZE`, `COPY TO`, cursor reads never reach a catalog
/// lookup here). A write shape none of `document`/`kv`/`kv_insert` claims
/// does one collection lookup to decide whether it must be refused as an
/// unsupported clone write. Every caller pays this gate uniformly rather
/// than special-casing itself out of it.
pub async fn intercept_and_authorize(
    params: InterceptAndAuthorizeParams<'_>,
) -> crate::Result<CloneCheckedOutcome> {
    let InterceptAndAuthorizeParams {
        state,
        mut task,
        identity,
        tenant_id,
        permissions,
        roles,
        emitter,
    } = params;
    // Every client read of every protocol passes here. A server-owned read
    // has no session consistency to judge, so it passes.
    if required_permission(&task.plan) == Permission::Read && !identity.is_internal_service() {
        check_mirror_read(state, task.database_id)?;
    }
    // Taken before the clone-write hook, which can complete the write itself.
    let lease = write_lease(state, task.tenant_id, task.database_id, &task.plan).await?;
    if let CloneWriteOutcome::Handled(resp) =
        maybe_intercept_clone_write(state, &mut task, identity, tenant_id).await?
    {
        return Ok(CloneCheckedOutcome::Handled(resp));
    }
    if required_permission(&task.plan) == Permission::Read
        && let super::super::clone_read::CloneReadOutcome::Handled(resp) =
            super::super::clone_read::maybe_intercept_clone_read(
                super::super::clone_read::CloneReadInterceptParams {
                    state,
                    task: &task,
                    identity,
                    tenant_id,
                    permissions,
                    roles,
                    emitter,
                },
            )
            .await?
    {
        return Ok(CloneCheckedOutcome::Handled(resp));
    }
    let authorized = authorize_task_set(
        identity,
        std::slice::from_ref(&task),
        permissions,
        roles,
        emitter,
    )
    .map_err(crate::Error::from)?
    .into_tasks()
    .into_iter()
    .next()
    .ok_or_else(|| crate::Error::Internal {
        detail: "authorization returned an empty capability set".into(),
    })?;
    Ok(CloneCheckedOutcome::Proceed(CloneCheckedTask {
        task: Box::new(authorized),
        lease,
    }))
}

/// Intercept, authorize, and dispatch one task to the Data Plane in one call —
/// the shape every read-only internal DDL scan needs (`ANALYZE`, `COPY TO`,
/// CHECK subquery evaluation, `VALIDATE TYPEGUARD`): no branching on the
/// capability besides "run it".
pub async fn intercept_authorize_and_dispatch(
    params: InterceptAndAuthorizeParams<'_>,
    trace_id: TraceId,
) -> crate::Result<Response> {
    let state = params.state;
    match intercept_and_authorize(params).await? {
        CloneCheckedOutcome::Handled(resp) => Ok(resp),
        CloneCheckedOutcome::Proceed(checked) => {
            crate::control::server::dispatch_utils::dispatch_authorized_to_data_plane(
                state, checked, trace_id,
            )
            .await
        }
    }
}
