//! Cross-node SQL forwarding: leader detection + RPC dispatch.
//!
//! Split out of `routing/mod.rs` to keep that file under the
//! 500-line soft limit and to give the forwarding path its own
//! home as typed leader-forwarding retry logic grows.
//!
//! The forwarding path is taken when:
//!
//! - Every planned task targets a single vShard whose leader is
//!   a remote node, AND
//! - The caller's read consistency requires leader execution
//!   (Strong) or the local node is not a replica of that vShard.
//!
//! When taken, we send the original SQL text to the remote leader
//! via the existing `ForwardRequest` RPC. The leader's
//! `LocalForwarder` re-plans and executes locally, then ships
//! back the serialized row payloads. This is the pre-gateway
//! pattern (shipping SQL strings instead of physical plans); the
//! gateway rewrite replaces it with `ExecuteRequest` carrying
//! the pre-planned physical task bytes.

use pgwire::api::results::{Response, Tag};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::control::planner::physical::PhysicalTask;
use crate::types::{ReadConsistency, TenantId};

use super::super::core::NodeDbPgHandler;
use super::super::plan::{PlanKind, payload_to_response};
use super::super::retry::retry_on_not_leader;

impl NodeDbPgHandler {
    /// Check if every task targets a single remote leader we
    /// should forward to. Returns `None` if any task should run
    /// locally, if the tasks fan out across leaders, or if the
    /// metadata routing table has no opinion yet.
    pub(super) fn remote_leader_for_tasks(
        &self,
        tasks: &[PhysicalTask],
        consistency: ReadConsistency,
    ) -> Option<u64> {
        let routing = self.state.cluster_routing.as_ref()?;
        let routing = routing.read().unwrap_or_else(|p| p.into_inner());
        let my_node = self.state.node_id;

        let mut remote_leader: Option<u64> = None;

        for task in tasks {
            let vshard_id = task.vshard_id.as_u16();
            let group_id = routing.group_for_vshard(vshard_id).ok()?;
            let info = routing.group_info(group_id)?;
            let leader = info.leader;

            if leader == my_node {
                return None;
            }
            if !consistency.requires_leader() && info.members.contains(&my_node) {
                return None;
            }
            if leader == 0 {
                return None;
            }

            match remote_leader {
                None => remote_leader = Some(leader),
                Some(prev) if prev != leader => return None,
                _ => {}
            }
        }

        remote_leader
    }

    /// Forward a SQL query to a remote leader node via QUIC.
    ///
    /// Wraps the RPC dispatch in `retry_on_not_leader` so a
    /// transient leader election between the routing decision
    /// and the forwarded RPC auto-retries up to 3 times with
    /// 50ms / 100ms / 200ms backoff. After the retry budget the
    /// error surfaces as `Error::NotLeader` which
    /// `error_to_sqlstate` maps to a typed Postgres error code.
    pub(super) async fn forward_sql(
        &self,
        sql: &str,
        tenant_id: TenantId,
        leader: u64,
    ) -> PgWireResult<Vec<Response>> {
        let transport = match &self.state.cluster_transport {
            Some(t) => t,
            None => {
                return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "55000".to_owned(),
                    "cluster transport not available".to_owned(),
                ))));
            }
        };

        let leader_addr = self
            .state
            .cluster_topology
            .as_ref()
            .and_then(|t| {
                let topo = t.read().unwrap_or_else(|p| p.into_inner());
                topo.get_node(leader).map(|n| n.addr.clone())
            })
            .unwrap_or_else(|| format!("node-{leader}"));
        let leader_addr_for_err = leader_addr.clone();

        let deadline_ms =
            std::time::Duration::from_secs(self.state.tuning.network.default_deadline_secs)
                .as_millis() as u64;

        let responses: Vec<Response> = retry_on_not_leader(|| async {
            let req = nodedb_cluster::rpc_codec::RaftRpc::ForwardRequest(
                nodedb_cluster::rpc_codec::ForwardRequest {
                    sql: sql.to_owned(),
                    tenant_id: tenant_id.as_u32(),
                    deadline_remaining_ms: deadline_ms,
                    trace_id: 0,
                },
            );

            let resp =
                transport
                    .send_rpc(leader, req)
                    .await
                    .map_err(|e| crate::Error::NotLeader {
                        vshard_id: crate::types::VShardId::new(0),
                        leader_node: leader,
                        leader_addr: format!("{leader_addr} (rpc error: {e})"),
                    })?;

            match resp {
                nodedb_cluster::rpc_codec::RaftRpc::ForwardResponse(fwd) => {
                    if !fwd.success {
                        // A "not leader" failure surfaced from the
                        // remote leader means our topology view is
                        // stale — bubble it up as a typed NotLeader
                        // so the retry helper can take another pass.
                        if fwd.error_message.contains("not leader")
                            || fwd.error_message.contains("NotLeader")
                        {
                            return Err(crate::Error::NotLeader {
                                vshard_id: crate::types::VShardId::new(0),
                                leader_node: leader,
                                leader_addr: leader_addr.clone(),
                            });
                        }
                        return Err(crate::Error::PlanError {
                            detail: format!("remote execution failed: {}", fwd.error_message),
                        });
                    }

                    let mut responses = Vec::with_capacity(fwd.payloads.len());
                    for payload in &fwd.payloads {
                        responses.push(payload_to_response(payload, PlanKind::MultiRow));
                    }
                    if responses.is_empty() {
                        responses.push(Response::Execution(Tag::new("OK")));
                    }
                    Ok::<Vec<Response>, crate::Error>(responses)
                }
                other => Err(crate::Error::PlanError {
                    detail: format!("unexpected response from leader: {other:?}"),
                }),
            }
        })
        .await
        .map_err(|e| {
            let (severity, code, message) =
                crate::control::server::pgwire::types::error_to_sqlstate(&e);
            PgWireError::UserError(Box::new(ErrorInfo::new(
                severity.to_owned(),
                code.to_owned(),
                format!("{message} (forward target: {leader_addr_for_err})"),
            )))
        })?;

        Ok(responses)
    }
}
