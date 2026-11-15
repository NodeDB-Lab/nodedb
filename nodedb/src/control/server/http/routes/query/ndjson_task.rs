// SPDX-License-Identifier: BUSL-1.1

//! One task of a materialized `/v1/query/stream` body: the Control-Plane
//! orchestrated plans, and the gateway route for every other task.

use std::sync::Arc;

use nodedb_physical::physical_task::PhysicalTask;

use crate::control::gateway::core::QueryContext;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::exchange::resolve::materialize_task_providers;
use crate::control::server::shared::cluster_array_dispatch::{is_cluster_array, run_cluster_array};
use crate::control::server::shared::session::read_scope;
use crate::control::server::shared::statement_exec::authorize_one_task;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId, TraceId};

/// Run one task and return its answer payloads.
pub(super) async fn dispatch_ndjson_task(
    shared: &Arc<SharedState>,
    identity: &AuthenticatedIdentity,
    task: PhysicalTask,
    tenant_id: TenantId,
    database_id: DatabaseId,
    trace_id: TraceId,
) -> crate::Result<Vec<Vec<u8>>> {
    // A provider relation's rows are filled here, where the identity is.
    let task = materialize_task_providers(shared, identity, task).await?;
    if crate::control::array_catalog::ddl::is_array_ddl(&task.plan) {
        match authorize_one_task(shared, identity, &task) {
            Ok(authorized_task) => crate::control::array_catalog::ddl::run_authorized_array_ddl(
                shared,
                authorized_task,
            )
            .await
            .map(|response| vec![response.payload.to_vec()]),
            Err(e) => Err(e),
        }
    } else if is_cluster_array(&task.plan) {
        // This node's array coordinator routes the op to the
        // shards that own its cells.
        match authorize_one_task(shared, identity, &task) {
            Ok(authorized_task) => run_cluster_array(shared, authorized_task)
                .await
                .map(|payload| vec![payload]),
            Err(e) => Err(e),
        }
    } else if matches!(
        &task.plan,
        crate::bridge::envelope::PhysicalPlan::Document(
            nodedb_physical::physical_plan::DocumentOp::InsertSelect { .. }
        )
    ) {
        match authorize_one_task(shared, identity, &task) {
            Ok(authorized_task) => {
                crate::control::insert_select::run_authorized_insert_select(shared, authorized_task)
                    .await
                    .map(|response| vec![response.payload.to_vec()])
            }
            Err(e) => Err(e),
        }
    } else if matches!(
        &task.plan,
        crate::bridge::envelope::PhysicalPlan::Document(
            nodedb_physical::physical_plan::DocumentOp::Merge {
                resolved_inserts: None,
                ..
            }
        )
    ) {
        match authorize_one_task(shared, identity, &task) {
            Ok(authorized_task) => {
                crate::control::merge_orchestrator::run_authorized_merge(shared, authorized_task)
                    .await
                    .map(|response| vec![response.payload.to_vec()])
            }
            Err(e) => Err(e),
        }
    } else if matches!(
        &task.plan,
        crate::bridge::envelope::PhysicalPlan::Document(
            nodedb_physical::physical_plan::DocumentOp::UpdateFromJoin {
                source_rows: None,
                ..
            }
        )
    ) {
        match authorize_one_task(shared, identity, &task) {
            Ok(authorized_task) => {
                crate::control::update_from_join_orchestrator::run_authorized_update_from_join(
                    shared,
                    authorized_task,
                )
                .await
                .map(|response| vec![response.payload.to_vec()])
            }
            Err(e) => Err(e),
        }
    } else if let Some(resolver) = crate::control::write_resolve::resolver_for_plan(&task.plan) {
        // A governed columnar predicate UPDATE/DELETE resolves to a concrete row set
        // before proposing.
        match authorize_one_task(shared, identity, &task) {
            Ok(authorized_task) => crate::control::write_resolve::run_authorized_write_resolve(
                shared,
                authorized_task,
                resolver,
            )
            .await
            .map(|response| vec![response.payload.to_vec()]),
            Err(e) => Err(e),
        }
    } else {
        // Clone CoW write-path interception, then authorization, run once
        // per task before dispatch — same protocol-neutral gate every
        // transport runs.
        let emitter = crate::control::security::audit::ArcAuditEmitter(Arc::clone(&shared.audit));
        match crate::control::server::shared::clone_write::intercept_and_authorize(
            crate::control::server::shared::clone_write::InterceptAndAuthorizeParams {
                state: shared,
                task,
                identity,
                tenant_id,
                permissions: &shared.permissions,
                roles: &shared.roles,
                emitter: &emitter,
            },
        )
        .await
        {
            Ok(crate::control::server::shared::clone_write::CloneCheckedOutcome::Handled(resp)) => {
                Ok(vec![resp.payload.to_vec()])
            }
            Ok(crate::control::server::shared::clone_write::CloneCheckedOutcome::Proceed(
                checked,
            )) => match shared.installed_gateway() {
                Ok(gateway) => {
                    let gw_ctx = QueryContext {
                        tenant_id: checked.tenant_id(),
                        trace_id,
                        database_id,
                        txn_id: None,
                        linearizable: read_scope::linearizable(),
                    };
                    gateway.execute(&gw_ctx, checked).await
                }
                Err(error) => Err(error),
            },
            Err(e) => Err(e),
        }
    }
}
