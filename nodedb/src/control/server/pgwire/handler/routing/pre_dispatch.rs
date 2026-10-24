// SPDX-License-Identifier: BUSL-1.1

//! Pre-dispatch routing gates for pgwire planned task sets.

use pgwire::api::results::Response;
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use nodedb_physical::physical_task::PhysicalTask;

use crate::control::planner::calvin::plan_needs_implicit_edge_recon;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::session::{SessionId, TransactionState};
use crate::types::TenantId;

use super::placement::TaskPlacement;
use super::planning::{consistency_for_tasks, has_replicated_writes};
use super::result_shaping::ResultShaping;

use super::super::super::types::error_to_sqlstate;
use super::super::core::NodeDbPgHandler;

/// This node has missed a topology transition and must not coordinate work
/// until it catches up.
fn superseded_topology_view(behind: u64) -> PgWireError {
    stale_read(format!(
        "this node is {behind} cluster generation(s) behind and is not \
         coordinating queries until it catches up; retry"
    ))
}

/// A retryable refusal of a read that needs a leader.
fn stale_read(message: String) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        nodedb_types::error::sqlstate::STALE_READ_NOT_LEADER.to_owned(),
        message,
    )))
}

/// No leader is known for a group whose read requires one.
fn no_serving_leader() -> PgWireError {
    stale_read("no leader is currently serving this range; retry".to_owned())
}

/// A linearizable read that no node can serve with proof.
fn unconfirmable_read() -> PgWireError {
    stale_read(
        "this read spans ranges led by different nodes, and this node holds no \
         replica of some of them; retry on a node that replicates every range"
            .to_owned(),
    )
}

impl NodeDbPgHandler {
    /// Refuse to coordinate while this node's cluster epoch is behind. Its
    /// routing table is then a stale view of who leads what.
    fn refuse_if_topology_view_superseded(&self) -> PgWireResult<()> {
        let Some(epoch) = self.state.cluster_epoch.get() else {
            return Ok(());
        };
        if epoch.is_behind() {
            return Err(superseded_topology_view(epoch.generations_behind()));
        }
        Ok(())
    }

    /// Route an implicit-edge dependent predicate through OLLP/Calvin when its
    /// catalog and session prerequisites require atomic edge maintenance.
    pub(super) async fn maybe_dispatch_implicit_edge_recon(
        &self,
        tasks: &[PhysicalTask],
        tenant_id: TenantId,
        session: super::calvin_dispatch::CalvinDispatchSession<'_>,
        sum_target_reads: &[crate::control::server::shared::session::read_set::ReadSetEntry],
    ) -> PgWireResult<Option<Vec<Response>>> {
        let tx_state = self.sessions.transaction_state(session.session_id);
        if tx_state == TransactionState::InBlock {
            return Ok(None);
        }

        let needs_recon =
            plan_needs_implicit_edge_recon(&self.state, tasks, tenant_id).map_err(|error| {
                let (severity, code, message) = error_to_sqlstate(&error);
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    severity.to_owned(),
                    code.to_owned(),
                    message,
                )))
            })?;
        if needs_recon.is_none() {
            return Ok(None);
        }

        self.dispatch_calvin_multishard(
            tasks.to_vec(),
            tenant_id,
            session,
            // The tasks are already settled: the reads stamped on the source images
            // their cross-shard balances were folded from travel with them.
            sum_target_reads,
        )
        .await
        .map(Some)
    }

    /// Forward an ordinary remote-leader task set through the gateway.
    ///
    /// Unresolved multi-step DML stays local so its orchestrator can resolve
    /// final plans before authorization. A `ClusterArray` plan stays local
    /// because this node's array coordinator routes it to the owning shards.
    /// Array DDL stays local because the dispatch loop proposes it as a
    /// replicated catalog entry. Its task's vShard names no owner.
    ///
    /// The caller skips this for an in-block statement. A write forwarded
    /// here applies durably at once, outside the transaction; the dispatch
    /// loop's staging gate stages it on the owner under this transaction
    /// instead, through `leader_forward`. A read forwarded here loses the
    /// transaction id its gather resolves with and records no read for
    /// commit-time validation; the loop's gather reaches the owner carrying
    /// the transaction id, so the owner resolves this transaction's overlay.
    pub(super) async fn maybe_dispatch_tasks_via_gateway(
        &self,
        tasks: &[PhysicalTask],
        identity: &AuthenticatedIdentity,
        tenant_id: TenantId,
        session_id: SessionId,
        shaping: ResultShaping<'_>,
        auth: &crate::control::security::auth_context::AuthContext,
    ) -> PgWireResult<Option<Vec<Response>>> {
        let ResultShaping {
            projection,
            formats: result_formats,
        } = shaping;
        if has_orchestrated_dml(tasks) || has_cluster_array_op(tasks) || has_array_ddl(tasks) {
            return Ok(None);
        }
        self.refuse_if_topology_view_superseded()?;
        let consistency = consistency_for_tasks(&self.sessions, tasks, session_id);
        let needs_confirmed_leader = consistency.requires_leader() && !has_replicated_writes(tasks);
        match self.placement_for_tasks(tasks, consistency, needs_confirmed_leader) {
            // Each read in the set confirms its group where it is served: the
            // local dispatch for a replica read here, the serving node for a
            // leg the gateway sends elsewhere.
            TaskPlacement::Local | TaskPlacement::LocalConfirmed => return Ok(None),
            TaskPlacement::NoLeader => return Err(no_serving_leader()),
            TaskPlacement::Unconfirmable => return Err(unconfirmable_read()),
            TaskPlacement::Gateway => {}
        }

        let database_id = self
            .sessions
            .get_current_database(session_id)
            .unwrap_or(crate::types::DatabaseId::DEFAULT);
        // Clone-check and authorization now run per task, inside
        // `dispatch_tasks_via_gateway`, immediately before each task forwards.
        self.dispatch_tasks_via_gateway(
            tasks.to_vec(),
            super::gateway_dispatch::GatewayDispatchParams {
                identity,
                tenant_id,
                database_id,
                session_id,
                projection,
                result_formats,
                auth,
            },
        )
        .await
        .map(Some)
    }
}

/// Whether any task is a `ClusterArray` plan.
///
/// The `ArrayCoordinator` on this node fans such a plan out to the shards
/// that own its cells. The plan's own vShard names no owner, so forwarding
/// it to that vShard's leader is wrong, and the plan has no wire encoding.
/// The dispatch loop runs it here.
fn has_cluster_array_op(tasks: &[PhysicalTask]) -> bool {
    tasks.iter().any(|task| {
        matches!(
            &task.plan,
            crate::bridge::envelope::PhysicalPlan::ClusterArray(_)
        )
    })
}

/// Whether any task is array DDL.
///
/// The dispatch loop proposes it as a `PutArray` or `DeleteArray` catalog
/// entry, which every node applies. Forwarded to its vShard's leader, it
/// opens the array on that one node and writes no catalog entry.
fn has_array_ddl(tasks: &[PhysicalTask]) -> bool {
    tasks
        .iter()
        .any(|task| crate::control::array_catalog::ddl::is_array_ddl(&task.plan))
}

fn has_orchestrated_dml(tasks: &[PhysicalTask]) -> bool {
    tasks.iter().any(|task| {
        matches!(
            &task.plan,
            crate::bridge::envelope::PhysicalPlan::Document(
                nodedb_physical::physical_plan::DocumentOp::InsertSelect { .. }
                    | nodedb_physical::physical_plan::DocumentOp::Merge {
                        resolved_inserts: None,
                        ..
                    }
                    | nodedb_physical::physical_plan::DocumentOp::UpdateFromJoin {
                        source_rows: None,
                        ..
                    }
            )
        )
    })
}
