//! ClusterForwarder: executes forwarded SQL queries on the local Data Plane.
//!
//! When a client connects to a non-leader node, the pgwire handler detects
//! the vShard is owned by another node and forwards the SQL over QUIC via
//! `NexarTransport::send_rpc`. The leader node receives a `ForwardRequest`,
//! and the `ClusterForwarder` executes it locally using the same planning
//! and dispatch path as a direct pgwire query.
//!
//! ## Trust model
//!
//! Node-to-node forwarding is trusted — the originating node has already
//! authenticated the client. The `tenant_id` in the `ForwardRequest` is
//! accepted without re-authentication. mTLS between nodes ensures only
//! legitimate cluster members can forward.

use std::sync::Arc;

use tracing::{debug, warn};

use crate::control::planner::context::QueryContext;
use crate::control::state::SharedState;
use crate::types::TenantId;

/// Forwarder that executes SQL queries on the local Data Plane.
///
/// Implements `nodedb_cluster::RequestForwarder` for use in the Raft loop's
/// RPC handler. Lives on the Control Plane (Send + Sync).
pub struct ClusterForwarder {
    shared: Arc<SharedState>,
    query_ctx: Arc<QueryContext>,
}

impl ClusterForwarder {
    pub fn new(shared: Arc<SharedState>, query_ctx: Arc<QueryContext>) -> Self {
        Self { shared, query_ctx }
    }
}

impl nodedb_cluster::RequestForwarder for ClusterForwarder {
    async fn execute_forwarded(
        &self,
        req: nodedb_cluster::rpc_codec::ForwardRequest,
    ) -> nodedb_cluster::rpc_codec::ForwardResponse {
        let tenant_id = TenantId::new(req.tenant_id);
        let sql = &req.sql;

        debug!(
            tenant_id = req.tenant_id,
            sql = %sql,
            trace_id = req.trace_id,
            "executing forwarded query"
        );

        // 1. Plan SQL via DataFusion.
        let tasks = match self.query_ctx.plan_sql(sql, tenant_id).await {
            Ok(tasks) => tasks,
            Err(e) => {
                return nodedb_cluster::rpc_codec::ForwardResponse {
                    success: false,
                    payloads: vec![],
                    error_message: format!("SQL planning failed: {e}"),
                };
            }
        };

        if tasks.is_empty() {
            return nodedb_cluster::rpc_codec::ForwardResponse {
                success: true,
                payloads: vec![],
                error_message: String::new(),
            };
        }

        // 2. Execute each task via the SPSC bridge.
        let mut payloads = Vec::with_capacity(tasks.len());

        for task in tasks {
            // WAL append for write operations.
            if let Err(e) = crate::control::server::dispatch_utils::wal_append_if_write(
                &self.shared.wal,
                task.tenant_id,
                task.vshard_id,
                &task.plan,
            ) {
                return nodedb_cluster::rpc_codec::ForwardResponse {
                    success: false,
                    payloads,
                    error_message: format!("WAL append failed: {e}"),
                };
            }

            // Dispatch to Data Plane.
            match crate::control::server::dispatch_utils::dispatch_to_data_plane(
                &self.shared,
                task.tenant_id,
                task.vshard_id,
                task.plan,
                req.trace_id,
            )
            .await
            {
                Ok(response) => {
                    if response.status != crate::bridge::envelope::Status::Ok {
                        let detail = response
                            .error_code
                            .as_ref()
                            .map(|c| format!("{c:?}"))
                            .unwrap_or_else(|| "execution error".into());
                        return nodedb_cluster::rpc_codec::ForwardResponse {
                            success: false,
                            payloads,
                            error_message: detail,
                        };
                    }
                    payloads.push(response.payload.as_ref().to_vec());
                }
                Err(e) => {
                    warn!(error = %e, "forwarded query dispatch failed");
                    return nodedb_cluster::rpc_codec::ForwardResponse {
                        success: false,
                        payloads,
                        error_message: format!("dispatch failed: {e}"),
                    };
                }
            }
        }

        nodedb_cluster::rpc_codec::ForwardResponse {
            success: true,
            payloads,
            error_message: String::new(),
        }
    }
}
