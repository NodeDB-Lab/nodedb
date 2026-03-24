//! Core dispatch mechanics: single-task dispatch, Raft replication, and local Data Plane submission.

use std::sync::Arc;
use std::time::Instant;

use crate::bridge::envelope::{Payload, Priority, Request, Response};
use crate::control::planner::physical::PhysicalTask;
use crate::types::{Lsn, ReadConsistency};

use super::core::NodeDbPgHandler;

/// Default request deadline: 30 seconds.
const DEFAULT_DEADLINE: std::time::Duration = std::time::Duration::from_secs(30);

impl NodeDbPgHandler {
    /// Dispatch a single physical task and wait for the response.
    ///
    /// In cluster mode, write operations are proposed to Raft first and only
    /// executed on the Data Plane after quorum commit. Reads bypass Raft.
    pub(super) async fn dispatch_task(&self, task: PhysicalTask) -> crate::Result<Response> {
        // Broadcast scans to all cores — data is distributed across cores.
        if matches!(
            task.plan,
            crate::bridge::envelope::PhysicalPlan::DocumentScan { .. }
                | crate::bridge::envelope::PhysicalPlan::Aggregate { .. }
                | crate::bridge::envelope::PhysicalPlan::PartialAggregate { .. }
                | crate::bridge::envelope::PhysicalPlan::GraphHop { .. }
                | crate::bridge::envelope::PhysicalPlan::GraphNeighbors { .. }
                | crate::bridge::envelope::PhysicalPlan::GraphPath { .. }
                | crate::bridge::envelope::PhysicalPlan::GraphSubgraph { .. }
                | crate::bridge::envelope::PhysicalPlan::VectorSearch { .. }
                | crate::bridge::envelope::PhysicalPlan::TextSearch { .. }
                | crate::bridge::envelope::PhysicalPlan::HybridSearch { .. }
                | crate::bridge::envelope::PhysicalPlan::GraphRagFusion { .. }
        ) {
            return crate::control::server::dispatch_utils::broadcast_to_all_cores(
                &self.state,
                task.tenant_id,
                task.plan,
                0,
            )
            .await;
        }

        if let (Some(proposer), Some(tracker)) =
            (&self.state.raft_proposer, &self.state.propose_tracker)
            && let Some(entry) = crate::control::wal_replication::to_replicated_entry(
                task.tenant_id,
                task.vshard_id,
                &task.plan,
            )
        {
            return self
                .dispatch_replicated_write(entry, proposer, tracker)
                .await;
        }

        self.dispatch_local(task).await
    }

    /// Dispatch a write through Raft: propose → await commit → return result.
    async fn dispatch_replicated_write(
        &self,
        entry: crate::control::wal_replication::ReplicatedEntry,
        proposer: &Arc<crate::control::wal_replication::RaftProposer>,
        tracker: &Arc<crate::control::wal_replication::ProposeTracker>,
    ) -> crate::Result<Response> {
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
            Ok(payload) => Ok(Response {
                request_id,
                status: crate::bridge::envelope::Status::Ok,
                attempt: 1,
                partial: false,
                payload: payload.into(),
                watermark_lsn: Lsn::new(log_index),
                error_code: None,
            }),
            Err(err_msg) => {
                let err_str = err_msg.to_string();
                Ok(Response {
                    request_id,
                    status: crate::bridge::envelope::Status::Error,
                    attempt: 1,
                    partial: false,
                    payload: Payload::from_arc(Arc::from(err_str.as_bytes())),
                    watermark_lsn: Lsn::new(0),
                    error_code: Some(crate::bridge::envelope::ErrorCode::Internal {
                        detail: err_str,
                    }),
                })
            }
        }
    }

    /// Dispatch a task directly to the local Data Plane (single-node or reads).
    ///
    /// For write operations, the WAL is appended **before** dispatching to the
    /// Data Plane. This ensures durability: if the process crashes after WAL
    /// append but before Data Plane execution, the write is replayed on recovery.
    /// Reads bypass the WAL entirely.
    async fn dispatch_local(&self, task: PhysicalTask) -> crate::Result<Response> {
        self.wal_append_if_write(task.tenant_id, task.vshard_id, &task.plan)?;
        self.submit_to_data_plane(task.tenant_id, task.vshard_id, task.plan)
            .await
    }

    /// Dispatch a task to the Data Plane WITHOUT individual WAL append.
    ///
    /// Used by COMMIT to dispatch buffered transaction tasks after the
    /// entire transaction has been written as a single `RecordType::Transaction`
    /// WAL record. Skipping per-task WAL avoids double-writing.
    pub(super) async fn dispatch_task_no_wal(&self, task: PhysicalTask) -> crate::Result<Response> {
        self.submit_to_data_plane(task.tenant_id, task.vshard_id, task.plan)
            .await
    }

    /// Build a `Request`, register with the tracker, dispatch to the Data Plane,
    /// and await the response. Shared by `dispatch_local` and `dispatch_task_no_wal`.
    async fn submit_to_data_plane(
        &self,
        tenant_id: crate::types::TenantId,
        vshard_id: crate::types::VShardId,
        plan: crate::bridge::envelope::PhysicalPlan,
    ) -> crate::Result<Response> {
        let request_id = self.next_request_id();
        let request = Request {
            request_id,
            tenant_id,
            vshard_id,
            plan,
            deadline: Instant::now() + DEFAULT_DEADLINE,
            priority: Priority::Normal,
            trace_id: 0,
            consistency: ReadConsistency::Strong,
            idempotency_key: None,
        };

        let rx = self.state.tracker.register_oneshot(request_id);

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
}
