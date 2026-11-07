// SPDX-License-Identifier: BUSL-1.1

//! Per-task routing: the mirror read check, orchestrated DML, exchange
//! resolution, and the replicated-vs-local dispatch choice.

use std::sync::Arc;

use crate::bridge::envelope::Response;
use crate::control::cluster::linearizable_read::{StatementDeadline, confirm_linearizable_read};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::exchange::ReadScope;
use crate::control::server::exchange::resolve::{
    DistributedReadCapture, Resolved, resolve_and_materialize,
};
use crate::control::server::shared::write_admission::{check_mirror_read, plan_is_write};
use crate::types::TraceId;
use nodedb_physical::physical_task::PhysicalTask;

use super::super::core::NodeDbPgHandler;
use super::authorize::reject_unadmitted_crdt_apply;
use super::replicated::ReplicatedWrite;

impl NodeDbPgHandler {
    pub(super) async fn dispatch_task_inner(
        &self,
        mut task: PhysicalTask,
        user_id: Option<Arc<str>>,
        identity: &AuthenticatedIdentity,
        linearizable: bool,
        distributed_reads: &mut Vec<DistributedReadCapture>,
    ) -> crate::Result<Response> {
        use crate::control::security::identity::{Permission, required_permission};
        let perm = required_permission(&task.plan);

        // This path resolves Exchange reads before the dispatch capability gate
        // runs its mirror read check, so a read checks here first. Writes pass
        // the shared write gates below this method.
        if matches!(perm, Permission::Read) {
            check_mirror_read(&self.state, task.database_id)?;
        }

        if matches!(
            &task.plan,
            crate::bridge::envelope::PhysicalPlan::Document(
                nodedb_physical::physical_plan::DocumentOp::InsertSelect { .. }
            )
        ) {
            let authorized = self.authorize_for_dispatch(identity, &task)?;
            return crate::control::insert_select::run_authorized_insert_select(
                &self.state,
                authorized,
            )
            .await;
        }

        // Autocommit `MERGE` orchestrates on the Control Plane (`control::merge_orchestrator`).
        // In-transaction MERGE buffers for COMMIT replay and never reaches this method.
        if matches!(
            &task.plan,
            crate::bridge::envelope::PhysicalPlan::Document(
                nodedb_physical::physical_plan::DocumentOp::Merge {
                    resolved_inserts: None,
                    ..
                }
            )
        ) {
            let authorized = self.authorize_for_dispatch(identity, &task)?;
            return crate::control::merge_orchestrator::run_authorized_merge(
                &self.state,
                authorized,
            )
            .await;
        }

        // Scans the source on its own core and ships raw rows into the plan (source's
        // vShard can differ). In-transaction buffers for COMMIT replay instead.
        if matches!(
            &task.plan,
            crate::bridge::envelope::PhysicalPlan::Document(
                nodedb_physical::physical_plan::DocumentOp::UpdateFromJoin {
                    source_rows: None,
                    ..
                }
            )
        ) {
            let authorized = self.authorize_for_dispatch(identity, &task)?;
            return crate::control::update_from_join_orchestrator::run_authorized_update_from_join(
                &self.state,
                authorized,
            )
            .await;
        }

        // Can't replicate bare over Raft — a follower has no writing identity to decide
        // `$auth.*` against. `write_resolve` resolves it while the identity is live.
        if let Some(resolver) = crate::control::write_resolve::resolver_for_plan(&task.plan) {
            let authorized = self.authorize_for_dispatch(identity, &task)?;
            return crate::control::write_resolve::run_authorized_write_resolve(
                &self.state,
                authorized,
                resolver,
            )
            .await;
        }

        // Array DDL proposes a replicated catalog entry; every node's
        // post-apply opens or drops the array on its cores.
        if crate::control::array_catalog::ddl::is_array_ddl(&task.plan) {
            let authorized = self.authorize_for_dispatch(identity, &task)?;
            return crate::control::array_catalog::ddl::run_authorized_array_ddl(
                &self.state,
                authorized,
            )
            .await;
        }

        // Clone-read must run first: resolving derived Exchange plans below
        // dispatches straight to the Data Plane, bypassing the clone check.
        if let Some(resp) = self
            .maybe_intercept_clone_read_early(&task, identity, perm)
            .await?
        {
            return Ok(resp);
        }

        // Resolve derived Exchange plans before authorizing the dispatched task.
        let scope = ReadScope {
            database_id: task.database_id,
            tenant_id: task.tenant_id,
            trace_id: TraceId::ZERO,
            txn_id: task.txn_id,
            linearizable,
        };
        match resolve_and_materialize(&self.state, identity, task.plan, scope).await? {
            Resolved::Gathered(resp, _watermarks, caps) => {
                *distributed_reads = caps;
                return Ok(resp);
            }
            Resolved::Plan(resolved_plan) => {
                let resolved_plan = *resolved_plan;
                task.plan = resolved_plan;
            }
            Resolved::Stream(stream) => {
                return crate::control::server::exchange::gather::stream_to_response(stream).await;
            }
        }

        reject_unadmitted_crdt_apply(&task.plan)?;
        let checked = self
            .intercept_and_authorize_for_dispatch(identity, task)
            .await?;
        let checked = match checked {
            crate::control::server::shared::clone_write::CloneCheckedOutcome::Handled(resp) => {
                return Ok(resp);
            }
            crate::control::server::shared::clone_write::CloneCheckedOutcome::Proceed(t) => t,
        };
        // The entry carries resolved rows: a timeseries ingest resolves here,
        // on the proposer, before the entry exists.
        let resolved = crate::control::write_resolve::resolve_for_log(
            &self.state,
            crate::control::write_resolve::WriteResolveContext {
                tenant_id: checked.tenant_id(),
                database_id: checked.database_id(),
            },
            checked.vshard_id(),
            checked.plan(),
        )
        .await?;
        if let Some(entry) = crate::control::wal_replication::to_replicated_entry(
            checked.tenant_id(),
            checked.database_id(),
            checked.vshard_id(),
            &crate::control::wal_replication::ReplicableWrite::decide_for_replication(
                resolved.as_ref().unwrap_or(checked.plan()),
            )?,
        )? {
            let async_proposer = self.state.async_raft_proposer()?;
            let (authorized, _lease) = checked.into_parts();
            // Every replica judges a role-guarded transition against the
            // writer's roles the entry carries.
            let entry = entry.with_user_roles(authorized.identity().role_names());
            return self
                .dispatch_replicated_write(ReplicatedWrite {
                    entry,
                    proposer: async_proposer,
                })
                .await;
        }
        // A read runs here when this node replicates its group (and leads it,
        // for a transaction's read: the staging overlay lives on the leader),
        // confirmed first. Otherwise it runs on the group's leader.
        if !plan_is_write(checked.plan()) {
            use crate::control::server::dispatch_utils::{
                ReadPlacement, owner_response, read_placement,
            };
            match read_placement(&self.state, checked.vshard_id(), checked.txn_id())? {
                ReadPlacement::Here(groups) => {
                    if linearizable {
                        let deadline = StatementDeadline::of_statement(&self.state);
                        confirm_linearizable_read(&self.state, &groups, deadline).await?;
                    }
                }
                ReadPlacement::Owner => {
                    let gateway = self.state.installed_gateway()?;
                    let ctx = crate::control::gateway::core::QueryContext {
                        tenant_id: checked.tenant_id(),
                        trace_id: TraceId::ZERO,
                        database_id: checked.database_id(),
                        txn_id: checked.txn_id(),
                        linearizable,
                    };
                    return owner_response(gateway.execute_outcome(&ctx, checked).await);
                }
            }
        }
        self.dispatch_local(checked, user_id).await
    }
}
