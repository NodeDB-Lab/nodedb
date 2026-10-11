// SPDX-License-Identifier: BUSL-1.1

//! Plans that run on the Control Plane instead of the gateway route:
//!
//! - array DDL, which proposes a replicated catalog entry;
//! - a cluster array op, which this node's array coordinator routes to the
//!   shards that own its cells;
//! - `INSERT ... SELECT`, an unresolved `MERGE` or `UPDATE ... FROM`, and a
//!   governed columnar predicate write, which issue their own writes.
//!
//! None of these is a clone-write shape, so each is authorized without the
//! clone-write check.

use std::sync::Arc;

use nodedb_physical::physical_plan::DocumentOp;
use nodedb_physical::physical_task::PhysicalTask;

use crate::bridge::envelope::{PhysicalPlan, Response, Status};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::cluster_array_dispatch::{is_cluster_array, run_cluster_array};
use crate::control::server::shared::statement_exec::authorize_one_task;
use crate::control::state::SharedState;

use super::super::super::super::auth::ApiError;
use super::encode::{gateway_error, response_error};

/// The payload an orchestrated plan answered with.
pub(super) struct Orchestrated {
    pub(super) payload: Vec<u8>,
    /// Whether the task is metered. Array DDL is not.
    pub(super) metered: bool,
}

/// Run `task` when it is a Control-Plane orchestrated plan. `None` means the
/// task takes the clone-write gate and the gateway route.
pub(super) async fn run_orchestrated(
    shared: &Arc<SharedState>,
    identity: &AuthenticatedIdentity,
    task: &PhysicalTask,
) -> Result<Option<Orchestrated>, ApiError> {
    let authorize = || authorize_one_task(shared, identity, task).map_err(gateway_error);

    if crate::control::array_catalog::ddl::is_array_ddl(&task.plan) {
        let response =
            crate::control::array_catalog::ddl::run_authorized_array_ddl(shared, authorize()?)
                .await
                .map_err(gateway_error)?;
        return answered(response, false);
    }

    if is_cluster_array(&task.plan) {
        let payload = run_cluster_array(shared, authorize()?)
            .await
            .map_err(gateway_error)?;
        return Ok(Some(Orchestrated {
            payload,
            metered: true,
        }));
    }

    let response = if let PhysicalPlan::Document(DocumentOp::InsertSelect { .. }) = &task.plan {
        crate::control::insert_select::run_authorized_insert_select(shared, authorize()?).await
    } else if let PhysicalPlan::Document(DocumentOp::Merge {
        target_collection: _,
        source_collection: _,
        source_alias: _,
        target_join_col: _,
        source_join_col: _,
        clauses: _,
        returning: _,
        resolved_inserts: None,
        resolved_insert_identities: _,
        source_rows: _,
        rls_filters: _,
        rls_write_check: _,
        resolved_sum_targets: _,
        declared_primary_key: _,
    }) = &task.plan
    {
        crate::control::merge_orchestrator::run_authorized_merge(shared, authorize()?).await
    } else if let PhysicalPlan::Document(DocumentOp::UpdateFromJoin {
        target_collection: _,
        source_collection: _,
        source_alias: _,
        target_join_col: _,
        source_join_col: _,
        updates: _,
        target_filters: _,
        returning: _,
        source_rows: None,
        rls_filters: _,
        rls_write_check: _,
        resolved_sum_targets: _,
        declared_primary_key: _,
    }) = &task.plan
    {
        crate::control::update_from_join_orchestrator::run_authorized_update_from_join(
            shared,
            authorize()?,
        )
        .await
    } else if let Some(resolver) = crate::control::write_resolve::resolver_for_plan(&task.plan) {
        crate::control::write_resolve::run_authorized_write_resolve(shared, authorize()?, resolver)
            .await
    } else {
        return Ok(None);
    };
    answered(response.map_err(gateway_error)?, true)
}

/// The payload of an orchestrated response. A refusal becomes the HTTP error
/// its code maps to.
fn answered(response: Response, metered: bool) -> Result<Option<Orchestrated>, ApiError> {
    if response.status != Status::Ok {
        return Err(response_error(&response));
    }
    Ok(Some(Orchestrated {
        payload: response.payload.to_vec(),
        metered,
    }))
}
