// SPDX-License-Identifier: BUSL-1.1

//! Per-task routing: freeze/mirror checks, orchestrated DML, exchange
//! resolution, and the replicated-vs-local dispatch choice.

use std::sync::Arc;

use crate::bridge::envelope::Response;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::exchange::resolve::{
    DistributedReadCapture, Resolved, resolve_and_materialize,
};
use crate::types::{Lsn, ReadConsistency, TraceId, VShardId};
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
        shard_watermarks: &mut Vec<(VShardId, Lsn)>,
        distributed_reads: &mut Vec<DistributedReadCapture>,
    ) -> crate::Result<Response> {
        // Reject user writes against a database frozen by a clone materializer sweep.
        // Reads/DDL pass through.
        use crate::control::security::identity::{Permission, required_permission};
        let perm = required_permission(&task.plan);
        if matches!(perm, Permission::Write | Permission::Admin)
            && self.state.materialize_freeze.is_frozen(task.database_id)
        {
            return Err(crate::Error::SourceFrozen {
                database_id: task.database_id,
            });
        }

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
        if let Some(resolver) = crate::control::write_resolve::resolver_for_plan(&task.plan)
            && self.state.async_raft_proposer().is_some()
        {
            let authorized = self.authorize_for_dispatch(identity, &task)?;
            return crate::control::write_resolve::run_authorized_write_resolve(
                &self.state,
                authorized,
                resolver,
            )
            .await;
        }

        // `DROP ARRAY` reaches every core so each releases its store and segment dir —
        // otherwise a follow-up `CREATE ARRAY` carries stale state.
        if matches!(
            task.plan,
            crate::bridge::envelope::PhysicalPlan::Array(
                nodedb_physical::physical_plan::ArrayOp::DropArray { .. }
            )
        ) {
            // Broadcast bypasses the write funnel, so a denied DROP must not
            // delete catalog rows or surrogate bindings.
            let authorized = self.authorize_for_dispatch(identity, &task)?;
            let task = authorized.into_physical_task();
            return crate::control::array_catalog::ddl::run_authorized_drop(
                &self.state,
                task.tenant_id,
                task.database_id,
                task.plan,
                TraceId::ZERO,
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
        match resolve_and_materialize(
            &self.state,
            identity,
            task.database_id,
            task.tenant_id,
            task.plan,
            TraceId::ZERO,
            task.txn_id,
        )
        .await?
        {
            Resolved::Gathered(resp, wms, caps) => {
                *shard_watermarks = wms;
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
        if let Some(async_proposer) = self.state.async_raft_proposer()
            && let Some(entry) = crate::control::wal_replication::to_replicated_entry(
                checked.tenant_id(),
                checked.database_id(),
                checked.vshard_id(),
                &crate::control::wal_replication::ReplicableWrite::decide_for_replication(
                    checked.plan(),
                )?,
            )?
        {
            return self
                .dispatch_replicated_write(ReplicatedWrite {
                    entry,
                    proposer: async_proposer,
                    authorized: checked.into_authorized(),
                })
                .await;
        }
        self.dispatch_local(checked, user_id).await
    }
}
