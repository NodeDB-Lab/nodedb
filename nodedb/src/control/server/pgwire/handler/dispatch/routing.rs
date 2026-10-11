// SPDX-License-Identifier: BUSL-1.1

//! Per-task routing: mirror checks, orchestrated DML, exchange
//! resolution, and the replicated-vs-local dispatch choice.

use std::sync::Arc;

use crate::bridge::envelope::Response;
use crate::control::cluster::linearizable_read::{
    confirm_linearizable_read, statement_read_deadline,
};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::exchange::ReadScope;
use crate::control::server::exchange::resolve::{
    DistributedReadCapture, Resolved, resolve_and_materialize,
};
use crate::control::server::shared::write_admission::plan_is_write;
use crate::types::{ReadConsistency, TraceId};
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

        // Mirror enforcement: writes reject on non-promoted mirrors; reads gate by
        // ReadConsistency. Catalog lookup skipped for db id=0 to stay allocation-free.
        let catalog = self.state.credentials.catalog();
        if task.database_id.as_u64() != 0
            && let Ok(Some(descriptor)) = catalog.get_database(task.database_id)
            && let Some(origin) = descriptor.mirror_origin.as_ref()
            && !matches!(origin.status, nodedb_types::MirrorStatus::Promoted)
        {
            if matches!(perm, Permission::Write | Permission::Admin) {
                return Err(crate::Error::MirrorReadOnly {
                    database: descriptor.name.clone(),
                });
            }

            use crate::control::server::pgwire::ddl::database::{
                MirrorReadOutcome, check_mirror_read_consistency,
            };
            // Defaults to Strong: mirrors aren't the source leader, so reads reject
            // unless the session opted into BoundedStaleness or Eventual.
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or(std::time::Duration::ZERO)
                .as_millis() as u64;
            let outcome = check_mirror_read_consistency(
                catalog,
                task.database_id,
                origin,
                ReadConsistency::Strong,
                now_ms,
            );
            if let MirrorReadOutcome::Reject { message, .. } = outcome {
                return Err(crate::Error::StaleReadNotLeader {
                    database: descriptor.name.clone(),
                    source_cluster: origin.source_cluster.clone(),
                    detail: message,
                });
            }
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
            let (_authorized, _lease) = checked.into_parts();
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
                        let deadline = statement_read_deadline(&self.state);
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
