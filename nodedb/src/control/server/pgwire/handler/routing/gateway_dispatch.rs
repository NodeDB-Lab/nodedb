//! Gateway-based dispatch: routes tasks through `Gateway::execute` instead of
//! the old SQL-string `ForwardRequest` forwarding path.
//!
//! `should_forward_via_gateway` mirrors the old `remote_leader_for_tasks`
//! detection logic but returns a bool rather than the leader node id, because
//! the gateway handles the node selection internally.
//!
//! `dispatch_tasks_via_gateway` replaces `forward_sql`: each task is dispatched
//! via `gateway.execute(ctx, plan)` which ships pre-planned `PhysicalPlan` bytes
//! over QUIC via `ExecuteRequest`, rather than raw SQL text.

use pgwire::api::results::{Response, Tag};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::control::gateway::GatewayErrorMap;
use crate::control::planner::physical::PhysicalTask;
use crate::types::{ReadConsistency, TenantId};

use super::super::core::NodeDbPgHandler;
use super::super::plan::{PlanKind, payload_to_response};

impl NodeDbPgHandler {
    /// Returns `true` when every task targets a single remote leader and the
    /// gateway is available to forward them. This replaces the old
    /// `remote_leader_for_tasks` helper which returned the leader node id.
    pub(super) fn should_forward_via_gateway(
        &self,
        tasks: &[PhysicalTask],
        consistency: ReadConsistency,
    ) -> bool {
        if self.state.gateway.is_none() {
            return false;
        }
        let routing = match self.state.cluster_routing.as_ref() {
            Some(r) => r,
            None => return false,
        };
        let routing = routing.read().unwrap_or_else(|p| p.into_inner());
        let my_node = self.state.node_id;

        let mut remote_leader: Option<u64> = None;
        for task in tasks {
            let vshard_id = task.vshard_id.as_u16();
            let group_id = match routing.group_for_vshard(vshard_id) {
                Ok(g) => g,
                Err(_) => return false,
            };
            let info = match routing.group_info(group_id) {
                Some(i) => i,
                None => return false,
            };
            let leader = info.leader;

            // Task is local — don't forward.
            if leader == my_node {
                return false;
            }
            // Local replica acceptable for non-strong reads — don't forward.
            if !consistency.requires_leader() && info.members.contains(&my_node) {
                return false;
            }
            // No known leader — can't forward.
            if leader == 0 {
                return false;
            }

            match remote_leader {
                None => remote_leader = Some(leader),
                // Tasks fan out across multiple leaders — don't use gateway forward.
                Some(prev) if prev != leader => return false,
                _ => {}
            }
        }

        remote_leader.is_some()
    }

    /// Execute all tasks via the gateway. Each task's plan is dispatched
    /// through `gateway.execute()` which ships the pre-planned physical
    /// plan to the target node via `ExecuteRequest`.
    pub(super) async fn dispatch_tasks_via_gateway(
        &self,
        tasks: Vec<PhysicalTask>,
        tenant_id: TenantId,
    ) -> PgWireResult<Vec<Response>> {
        let gateway = self.state.gateway.as_ref().ok_or_else(|| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "55000".to_owned(),
                "gateway not available".to_owned(),
            )))
        })?;

        let gw_ctx = crate::control::gateway::core::QueryContext {
            tenant_id,
            trace_id: 0,
        };

        let mut responses: Vec<Response> = Vec::with_capacity(tasks.len());
        for task in tasks {
            let payloads = gateway.execute(&gw_ctx, task.plan).await.map_err(|e| {
                let (code, msg) = GatewayErrorMap::to_pgwire(&e);
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    code.to_owned(),
                    msg,
                )))
            })?;

            if payloads.is_empty() {
                responses.push(Response::Execution(Tag::new("OK")));
            } else {
                for payload in &payloads {
                    responses.push(payload_to_response(payload, PlanKind::MultiRow));
                }
            }
        }

        if responses.is_empty() {
            responses.push(Response::Execution(Tag::new("OK")));
        }

        Ok(responses)
    }
}
