//! Task dispatch and query forwarding.

use std::sync::Arc;
use std::time::Instant;

use pgwire::api::results::{Response, Tag};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::bridge::envelope::{Priority, Request};
use crate::control::planner::physical::PhysicalTask;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::types::{Lsn, ReadConsistency, TenantId};

use super::core::NodeDbPgHandler;
use super::plan::{PlanKind, describe_plan, payload_to_response};

use super::super::types::{error_to_sqlstate, response_status_to_sqlstate};

/// Default request deadline: 30 seconds.
const DEFAULT_DEADLINE: std::time::Duration = std::time::Duration::from_secs(30);

impl NodeDbPgHandler {
    /// Dispatch a single physical task and wait for the response.
    ///
    /// In cluster mode, write operations are proposed to Raft first and only
    /// executed on the Data Plane after quorum commit. Reads bypass Raft.
    pub(super) async fn dispatch_task(
        &self,
        task: PhysicalTask,
    ) -> crate::Result<crate::bridge::envelope::Response> {
        if let (Some(proposer), Some(tracker)) =
            (&self.state.raft_proposer, &self.state.propose_tracker)
        {
            if let Some(entry) = crate::control::wal_replication::to_replicated_entry(
                task.tenant_id,
                task.vshard_id,
                &task.plan,
            ) {
                return self
                    .dispatch_replicated_write(entry, proposer, tracker)
                    .await;
            }
        }

        self.dispatch_local(task).await
    }

    /// Dispatch a write through Raft: propose → await commit → return result.
    async fn dispatch_replicated_write(
        &self,
        entry: crate::control::wal_replication::ReplicatedEntry,
        proposer: &Arc<crate::control::wal_replication::RaftProposer>,
        tracker: &Arc<crate::control::wal_replication::ProposeTracker>,
    ) -> crate::Result<crate::bridge::envelope::Response> {
        let data = entry.to_bytes();
        let vshard_id = entry.vshard_id;

        let request_id = self.next_request_id();

        let (group_id, log_index) =
            proposer(vshard_id, data).map_err(|e| crate::Error::Dispatch {
                detail: format!("raft propose failed: {e}"),
            })?;

        let rx = tracker.register(group_id, log_index);

        let result = tokio::time::timeout(DEFAULT_DEADLINE, rx)
            .await
            .map_err(|_| crate::Error::Dispatch {
                detail: format!("raft commit timeout for group {group_id} index {log_index}"),
            })?
            .map_err(|_| crate::Error::Dispatch {
                detail: "propose waiter channel closed".into(),
            })?;

        match result {
            Ok(payload) => Ok(crate::bridge::envelope::Response {
                request_id,
                status: crate::bridge::envelope::Status::Ok,
                attempt: 1,
                partial: false,
                payload: payload.into(),
                watermark_lsn: Lsn::new(log_index),
                error_code: None,
            }),
            Err(err_msg) => Ok(crate::bridge::envelope::Response {
                request_id,
                status: crate::bridge::envelope::Status::Error,
                attempt: 1,
                partial: false,
                payload: Arc::from(err_msg.as_bytes()),
                watermark_lsn: Lsn::new(0),
                error_code: Some(crate::bridge::envelope::ErrorCode::Internal { detail: err_msg }),
            }),
        }
    }

    /// Dispatch a task directly to the local Data Plane (single-node or reads).
    ///
    /// For write operations, the WAL is appended **before** dispatching to the
    /// Data Plane. This ensures durability: if the process crashes after WAL
    /// append but before Data Plane execution, the write is replayed on recovery.
    /// Reads bypass the WAL entirely.
    async fn dispatch_local(
        &self,
        task: PhysicalTask,
    ) -> crate::Result<crate::bridge::envelope::Response> {
        // Append writes to WAL for durability (single-node mode).
        // In cluster mode, Raft handles durability — this path is reads-only.
        self.wal_append_if_write(task.tenant_id, task.vshard_id, &task.plan)?;

        let request_id = self.next_request_id();
        let request = Request {
            request_id,
            tenant_id: task.tenant_id,
            vshard_id: task.vshard_id,
            plan: task.plan,
            deadline: Instant::now() + DEFAULT_DEADLINE,
            priority: Priority::Normal,
            trace_id: 0,
            consistency: ReadConsistency::Strong,
        };

        let rx = self.state.tracker.register(request_id);

        match self.state.dispatcher.lock() {
            Ok(mut d) => d.dispatch(request)?,
            Err(poisoned) => poisoned.into_inner().dispatch(request)?,
        };

        tokio::time::timeout(DEFAULT_DEADLINE, rx)
            .await
            .map_err(|_| crate::Error::DeadlineExceeded { request_id })?
            .map_err(|_| crate::Error::Dispatch {
                detail: "response channel closed".into(),
            })
    }

    /// Append a write operation to the WAL for single-node durability.
    ///
    /// Serializes the write as MessagePack and appends to the appropriate
    /// WAL record type. Read operations are no-ops (return Ok immediately).
    fn wal_append_if_write(
        &self,
        tenant_id: TenantId,
        vshard_id: crate::types::VShardId,
        plan: &crate::bridge::envelope::PhysicalPlan,
    ) -> crate::Result<()> {
        use crate::bridge::envelope::PhysicalPlan;

        // Only write operations need WAL durability. Reads return immediately.
        match plan {
            PhysicalPlan::PointPut {
                collection,
                document_id,
                value,
            } => {
                let entry = rmp_serde::to_vec(&(collection, document_id, value)).map_err(|e| {
                    crate::Error::Serialization {
                        format: "msgpack".into(),
                        detail: format!("wal point put: {e}"),
                    }
                })?;
                self.state.wal.append_put(tenant_id, vshard_id, &entry)?;
                return Ok(());
            }
            PhysicalPlan::PointDelete {
                collection,
                document_id,
            } => {
                let entry = rmp_serde::to_vec(&(collection, document_id)).map_err(|e| {
                    crate::Error::Serialization {
                        format: "msgpack".into(),
                        detail: format!("wal point delete: {e}"),
                    }
                })?;
                self.state.wal.append_delete(tenant_id, vshard_id, &entry)?;
                return Ok(());
            }
            PhysicalPlan::VectorInsert {
                collection,
                vector,
                dim,
            } => {
                let entry = rmp_serde::to_vec(&(collection, vector, dim)).map_err(|e| {
                    crate::Error::Serialization {
                        format: "msgpack".into(),
                        detail: format!("wal vector insert: {e}"),
                    }
                })?;
                self.state
                    .wal
                    .append_vector_put(tenant_id, vshard_id, &entry)?;
                return Ok(());
            }
            PhysicalPlan::VectorBatchInsert {
                collection,
                vectors,
                dim,
            } => {
                // Batch: single WAL record for the entire batch (group commit).
                let entry = rmp_serde::to_vec(&(collection, vectors, dim)).map_err(|e| {
                    crate::Error::Serialization {
                        format: "msgpack".into(),
                        detail: format!("wal vector batch insert: {e}"),
                    }
                })?;
                self.state
                    .wal
                    .append_vector_put(tenant_id, vshard_id, &entry)?;
                return Ok(());
            }
            PhysicalPlan::VectorDelete {
                collection,
                vector_id,
            } => {
                let entry = rmp_serde::to_vec(&(collection, vector_id)).map_err(|e| {
                    crate::Error::Serialization {
                        format: "msgpack".into(),
                        detail: format!("wal vector delete: {e}"),
                    }
                })?;
                self.state
                    .wal
                    .append_vector_delete(tenant_id, vshard_id, &entry)?;
                return Ok(());
            }
            PhysicalPlan::CrdtApply { delta, .. } => {
                self.state
                    .wal
                    .append_crdt_delta(tenant_id, vshard_id, delta)?;
                return Ok(());
            }
            PhysicalPlan::EdgePut {
                src_id,
                label,
                dst_id,
                properties,
            } => {
                let entry =
                    rmp_serde::to_vec(&(src_id, label, dst_id, properties)).map_err(|e| {
                        crate::Error::Serialization {
                            format: "msgpack".into(),
                            detail: format!("wal edge put: {e}"),
                        }
                    })?;
                self.state.wal.append_put(tenant_id, vshard_id, &entry)?;
                return Ok(());
            }
            PhysicalPlan::EdgeDelete {
                src_id,
                label,
                dst_id,
            } => {
                let entry = rmp_serde::to_vec(&(src_id, label, dst_id)).map_err(|e| {
                    crate::Error::Serialization {
                        format: "msgpack".into(),
                        detail: format!("wal edge delete: {e}"),
                    }
                })?;
                self.state.wal.append_delete(tenant_id, vshard_id, &entry)?;
                return Ok(());
            }
            PhysicalPlan::SetVectorParams {
                collection,
                m,
                ef_construction,
                metric,
            } => {
                let entry =
                    rmp_serde::to_vec(&(collection, m, ef_construction, metric)).map_err(|e| {
                        crate::Error::Serialization {
                            format: "msgpack".into(),
                            detail: format!("wal set vector params: {e}"),
                        }
                    })?;
                self.state
                    .wal
                    .append_vector_params(tenant_id, vshard_id, &entry)?;
                return Ok(());
            }
            // Read operations and control commands: no WAL needed.
            _ => {}
        }
        Ok(())
    }

    /// Plan and dispatch SQL after quota and DDL checks have passed.
    pub(super) async fn execute_planned_sql(
        &self,
        identity: &AuthenticatedIdentity,
        sql: &str,
        tenant_id: TenantId,
    ) -> PgWireResult<Vec<Response>> {
        let tasks = self.query_ctx.plan_sql(sql, tenant_id).await.map_err(|e| {
            let (severity, code, message) = error_to_sqlstate(&e);
            PgWireError::UserError(Box::new(ErrorInfo::new(
                severity.to_owned(),
                code.to_owned(),
                message,
            )))
        })?;

        if tasks.is_empty() {
            return Ok(vec![Response::Execution(Tag::new("OK"))]);
        }

        // Determine read consistency and check if forwarding is needed.
        let consistency = self.consistency_for_tasks(&tasks);

        // Check if ALL tasks go to a single remote leader (common case).
        if let Some(leader) = self.remote_leader_for_tasks(&tasks, consistency) {
            return self.forward_sql(sql, tenant_id, leader).await;
        }

        // If tasks target multiple remote leaders, we can't forward the SQL as-is.
        // Fall through to local dispatch — tasks targeting remote vShards will fail
        // with a routing error. True scatter-gather across multiple leaders requires
        // per-task forwarding with result merging (deferred to scatter-gather phase).
        // For single-collection queries (the common case), all tasks share one leader.

        let mut responses = Vec::with_capacity(tasks.len());
        for task in tasks {
            self.check_permission(identity, &task.plan)?;

            let plan_kind = describe_plan(&task.plan);
            let resp = self.dispatch_task(task).await.map_err(|e| {
                let (severity, code, message) = error_to_sqlstate(&e);
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    severity.to_owned(),
                    code.to_owned(),
                    message,
                )))
            })?;

            if let Some((severity, code, message)) =
                response_status_to_sqlstate(resp.status, &resp.error_code)
            {
                return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    severity.to_owned(),
                    code.to_owned(),
                    message,
                ))));
            }

            responses.push(payload_to_response(&resp.payload, plan_kind));
        }

        Ok(responses)
    }

    /// Determine read consistency for a set of tasks.
    fn consistency_for_tasks(&self, tasks: &[PhysicalTask]) -> ReadConsistency {
        let has_writes = tasks.iter().any(|t| {
            crate::control::wal_replication::to_replicated_entry(t.tenant_id, t.vshard_id, &t.plan)
                .is_some()
        });

        if has_writes {
            ReadConsistency::Strong
        } else {
            ReadConsistency::BoundedStaleness(std::time::Duration::from_secs(5))
        }
    }

    /// Check if all tasks target a single remote leader.
    fn remote_leader_for_tasks(
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
    async fn forward_sql(
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

        let req = nodedb_cluster::rpc_codec::RaftRpc::ForwardRequest(
            nodedb_cluster::rpc_codec::ForwardRequest {
                sql: sql.to_owned(),
                tenant_id: tenant_id.as_u32(),
                deadline_remaining_ms: DEFAULT_DEADLINE.as_millis() as u64,
                trace_id: 0,
            },
        );

        // Look up leader's address for the redirect hint.
        let leader_addr = self
            .state
            .cluster_topology
            .as_ref()
            .and_then(|t| {
                let topo = t.read().unwrap_or_else(|p| p.into_inner());
                topo.get_node(leader).map(|n| n.addr.clone())
            })
            .unwrap_or_else(|| format!("node-{leader}"));

        let resp = transport.send_rpc(leader, req).await.map_err(|e| {
            // Return a redirect hint so the client can reconnect directly.
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "01R01".to_owned(),
                format!("not leader; redirect to {leader_addr} (forward failed: {e})"),
            )))
        })?;

        match resp {
            nodedb_cluster::rpc_codec::RaftRpc::ForwardResponse(fwd) => {
                if !fwd.success {
                    return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                        "ERROR".to_owned(),
                        "XX000".to_owned(),
                        format!("remote execution failed: {}", fwd.error_message),
                    ))));
                }

                let mut responses = Vec::with_capacity(fwd.payloads.len());
                for payload in &fwd.payloads {
                    responses.push(payload_to_response(payload, PlanKind::MultiRow));
                }
                if responses.is_empty() {
                    responses.push(Response::Execution(Tag::new("OK")));
                }
                Ok(responses)
            }
            other => Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "XX000".to_owned(),
                format!("unexpected response from leader: {other:?}"),
            )))),
        }
    }
}
