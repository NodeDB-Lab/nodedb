// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `REFRESH CONTINUOUS AGGREGATE <name>` handler.
//!
//! The refresh is a cut: a replicated write on the data group of the
//! aggregate's root source. Every replica applies it in log order, flushes
//! the root's memtable, and folds exactly the partitions present at that
//! moment. So every replica, and every node after a failover, serves the
//! same refresh point.
//!
//! The handler proposes the cut, then waits for it to fold on the core
//! that owns the root's vShard. That core folds under the per-tick budget
//! while it holds the wait, so the refresh never blocks the core. The
//! statement deadline bounds the wait.
//!
//! Both steps run where the root's group lives. A node that replicates the
//! group proposes the cut, waits for its own replica to apply it, and waits
//! for the fold on its own core. A node that does not replicate the group
//! sends each step through the gateway to the group's leader.
//!
//! The caller needs read access to the root source, and ownership of the
//! aggregate (or a tenant admin role).

use nodedb_physical::physical_plan::{MetaOp, RefreshCut, TimeseriesOp};

use crate::bridge::envelope::PhysicalPlan;
use crate::control::gateway::core::QueryContext;
use crate::control::security::identity::{AuthenticatedIdentity, Role};
use crate::control::server::dispatch_utils::{
    AutocommitWrite, ReadPlacement, dispatch_durable_autocommit_write, owner_response,
    read_placement,
};
use crate::control::server::shared::ddl::user_dispatch::{
    DispatchRequest, RequestAdmission, dispatch_for_identity,
};
use crate::control::server::shared::response_payload::payload_or_typed_error;
use crate::control::server::shared::session::{StatementDeadline, connection_stream};
use crate::control::state::SharedState;
use crate::engine::timeseries::continuous_agg::ContinuousAggregateDef;
use crate::types::{DatabaseId, TenantId, TraceId};

use super::super::super::result::{DdlError, DdlResult};
use super::super::read_gate::CollectionReadGate;

fn err(sqlstate: &str, message: String) -> DdlError {
    DdlError::new(sqlstate, message)
}

/// The aggregate a refresh cuts: its root source and incarnation.
pub(crate) struct RefreshTarget {
    pub root: String,
    pub incarnation: nodedb_types::Hlc,
}

/// `REFRESH CONTINUOUS AGGREGATE <name>`. The aggregate's owner, a tenant
/// admin, or a superuser may refresh it.
pub async fn refresh_continuous_aggregate(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    name: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let target = authorized_target(state, identity, database_id, name)?;
    CollectionReadGate::for_request(state, identity, database_id)?.authorize(&target.root)?;
    let refresh_id = state.hlc_clock.now().wall_ns;
    let cut = RefreshCut {
        aggregate: name.to_string(),
        incarnation: target.incarnation,
        refresh_id,
        periodic: false,
    };
    propose_refresh_cut(state, identity.tenant_id, database_id, &target.root, cut)
        .await
        .map_err(|e| DdlError::from_error(&e))?;

    // Reached only through `shared::ddl::dispatch`, which every transport
    // calls after its own admission gate. The dispatch places the wait on a
    // replica of the root's group: here when this node replicates it, on the
    // group's leader otherwise (`dispatch_utils::owner_read`). A replica that
    // has not applied the cut yet holds the wait until it has, so the wait
    // needs no read confirm.
    let plan = PhysicalPlan::Meta(MetaOp::AwaitContinuousAggregateRefresh {
        collection: nodedb_types::QualifiedCollection::new(database_id, &target.root),
        name: name.to_string(),
        refresh_id,
    });
    dispatch_for_identity(DispatchRequest {
        state,
        identity,
        database_id,
        collection: &target.root,
        plan,
        deadline: StatementDeadline::of_statement(state),
        admission: RequestAdmission::AlreadyAdmitted,
        linearizable: false,
    })
    .await
    .map_err(|e| DdlError::from_error(&e))?;

    tracing::info!(name, root = %target.root, refresh_id, "continuous aggregate refreshed");
    Ok(vec![DdlResult::Status {
        command: "REFRESH CONTINUOUS AGGREGATE".to_string(),
        rows_affected: None,
    }])
}

/// Propose refresh cut `cut` on the data group of `root` and wait for its
/// apply. A REFRESH and the periodic catch-up both cut through here.
///
/// A node that replicates the group proposes the cut and answers once its
/// own replica applied it. A node that does not replicate the group can
/// neither propose to it nor apply the cut. It sends the cut through the
/// gateway to the group's leader, which proposes it and answers once its own
/// replica applied it.
pub(crate) async fn propose_refresh_cut(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    root: &str,
    cut: RefreshCut,
) -> crate::Result<()> {
    let plan = PhysicalPlan::Timeseries(TimeseriesOp::RefreshAggregate {
        collection: nodedb_types::QualifiedCollection::new(database_id, root),
        cut,
    });
    let vshard_id = nodedb_types::CollectionKey::from_bare(database_id, root).vshard();
    let response = match read_placement(state, vshard_id, None)? {
        ReadPlacement::Here(_) => {
            dispatch_durable_autocommit_write(
                state,
                AutocommitWrite {
                    tenant_id,
                    database_id,
                    vshard_id,
                    stream: connection_stream::current(),
                    plan,
                    trace_id: TraceId::ZERO,
                    event_source: crate::event::EventSource::User,
                    txn_id: None,
                    // A refresh is a system write.
                    user_roles: Vec::new(),
                },
            )
            .await?
        }
        ReadPlacement::Owner => {
            let ctx = QueryContext {
                tenant_id,
                trace_id: TraceId::ZERO,
                database_id,
                txn_id: None,
                linearizable: false,
            };
            // A trusted internal write: it carries no caller roles.
            let outcome = state
                .installed_gateway()?
                .execute_internal_outcome(&ctx, plan)
                .await;
            owner_response(outcome)?
        }
    };
    payload_or_typed_error(response).map(|_| ())
}

/// The root source and incarnation of `name` once `identity` may refresh
/// it. A chain that loops is refused, not followed.
fn authorized_target(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    name: &str,
) -> Result<RefreshTarget, DdlError> {
    let tenant = identity.tenant_id.as_u64();
    let catalog = state.credentials.catalog();
    let read = |agg: &str| {
        catalog
            .get_continuous_aggregate(database_id.as_u64(), tenant, agg)
            .map_err(|e| DdlError::from_error_in_context("catalog read", &e))
    };
    let stored = read(name)?.ok_or_else(|| {
        err(
            "42704",
            format!("continuous aggregate '{name}' does not exist"),
        )
    })?;
    let is_owner = stored.owner == identity.username;
    let is_admin = identity.is_superuser || identity.has_role(&Role::TenantAdmin);
    if !is_owner && !is_admin {
        return Err(err(
            "42501",
            format!("permission denied to refresh continuous aggregate '{name}'"),
        ));
    }
    let def: ContinuousAggregateDef = zerompk::from_msgpack(&stored.def_bytes).map_err(|e| {
        DdlError::internal(format!(
            "continuous aggregate '{name}': stored definition does not decode: {e}"
        ))
    })?;
    let root = resolve_root(name, stored.source, |agg| {
        catalog
            .get_continuous_aggregate(database_id.as_u64(), tenant, agg)
            .map(|s| s.map(|s| s.source))
    })
    .map_err(|e| DdlError::from_error(&e))?;
    Ok(RefreshTarget {
        root,
        incarnation: def.incarnation,
    })
}

/// Follow `source` up the chain of aggregates to the first source that is
/// not an aggregate. `upstream` reads the source of an aggregate, or `None`
/// when the name is not one.
pub(crate) fn resolve_root(
    name: &str,
    source: String,
    upstream: impl Fn(&str) -> crate::Result<Option<String>>,
) -> crate::Result<String> {
    let mut root = source;
    let mut seen = std::collections::HashSet::from([name.to_string()]);
    while let Some(next) = upstream(&root)? {
        if !seen.insert(root.clone()) {
            return Err(crate::Error::ObjectNotInPrerequisiteState {
                object: format!("continuous aggregate '{name}'"),
                detail: format!("its source chain loops at '{root}'"),
            });
        }
        root = next;
    }
    Ok(root)
}
