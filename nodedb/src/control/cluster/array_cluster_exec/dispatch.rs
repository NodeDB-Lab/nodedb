// SPDX-License-Identifier: BUSL-1.1

//! `NexarArrayDispatch` bridges `ShardRpcDispatch` ↔ `NexarTransport`,
//! tunnelling `VShardEnvelope` bytes through `RaftRpc::VShardEnvelope`.
//!
//! When the target shard is owned by the local node, `NexarArrayDispatch`
//! short-circuits the QUIC transport and dispatches directly to the local
//! Data Plane via `handle_array_shard_rpc`. This avoids the `NodeUnreachable`
//! error that arises when a node tries to `send_rpc` to itself (the transport
//! peer table does not contain an entry for the local node's own ID).

use std::sync::Arc;

use async_trait::async_trait;
use nodedb_cluster::distributed_array::ArrayLocalExecutor;
use nodedb_cluster::distributed_array::handler::handle_array_shard_rpc;
use nodedb_cluster::distributed_array::rpc::ShardRpcDispatch;
use nodedb_cluster::error::Result as ClusterResult;
use nodedb_cluster::rpc_codec::RaftRpc;
use nodedb_cluster::wire::VShardEnvelope;
use nodedb_cluster::{NexarTransport, RoutingTable};

use crate::control::cluster::array_cluster_helpers::array_resp_msg_type;
use crate::control::cluster::array_executor::DataPlaneArrayExecutor;
use crate::control::state::SharedState;

/// Implements `ShardRpcDispatch` by either short-circuiting to the local Data
/// Plane (when the target shard leader is this node) or tunnelling through
/// `NexarTransport::send_rpc` via `RaftRpc::VShardEnvelope`.
///
/// The short-circuit path avoids the `NodeUnreachable` error that arises when a
/// coordinator tries to `send_rpc` to itself — the QUIC transport peer table
/// never contains an entry for the local node ID.
pub struct NexarArrayDispatch {
    transport: Arc<NexarTransport>,
    routing: Arc<std::sync::RwLock<RoutingTable>>,
    /// This node's own ID, used to detect local-shard requests.
    own_node_id: u64,
    /// Local Data Plane executor — used when the target shard is owned by
    /// this node, bypassing the QUIC transport entirely.
    local_executor: Arc<dyn ArrayLocalExecutor>,
}

impl NexarArrayDispatch {
    pub fn new(
        transport: Arc<NexarTransport>,
        routing: Arc<std::sync::RwLock<RoutingTable>>,
        own_node_id: u64,
        state: Arc<SharedState>,
    ) -> Self {
        let local_executor =
            Arc::new(DataPlaneArrayExecutor::new(state)) as Arc<dyn ArrayLocalExecutor>;
        Self {
            transport,
            routing,
            own_node_id,
            local_executor,
        }
    }
}

#[async_trait]
impl ShardRpcDispatch for NexarArrayDispatch {
    async fn call(&self, req: VShardEnvelope, timeout_ms: u64) -> ClusterResult<VShardEnvelope> {
        match self.call_once(&req, timeout_ms).await {
            Ok(resp) => Ok(resp),
            Err(first_err) if is_transport_err(&first_err) => {
                // Routing table may have pointed at a stale or fake leader.
                // The metadata applier heals it asynchronously; wait briefly,
                // re-read routing, and retry once.
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                self.call_once(&req, timeout_ms).await
            }
            Err(e) => Err(e),
        }
    }
}

fn is_transport_err(e: &nodedb_cluster::error::ClusterError) -> bool {
    matches!(e, nodedb_cluster::error::ClusterError::Transport { .. })
}

impl NexarArrayDispatch {
    async fn call_once(
        &self,
        req: &VShardEnvelope,
        timeout_ms: u64,
    ) -> ClusterResult<VShardEnvelope> {
        // Resolve the shard's leader node from the routing table.
        let node_id = {
            let rt = self.routing.read().map_err(|_| {
                nodedb_cluster::error::ClusterError::Transport {
                    detail: "routing table lock poisoned".into(),
                }
            })?;
            rt.leader_for_vshard(req.vshard_id)?
        };

        // Short-circuit: if the target shard is owned by this node, dispatch
        // directly to the local Data Plane instead of looping through QUIC.
        if node_id == self.own_node_id {
            let req_opcode = req.msg_type as u32;
            let resp_opcode = req_opcode + 1;
            let resp_msg_type = array_resp_msg_type(resp_opcode).ok_or_else(|| {
                nodedb_cluster::error::ClusterError::Codec {
                    detail: format!("local dispatch: unknown response opcode {resp_opcode}"),
                }
            })?;

            let resp_payload = handle_array_shard_rpc(
                req_opcode,
                req.vshard_id,
                &req.payload,
                &self.local_executor,
            )
            .await?;

            return Ok(VShardEnvelope::new(
                resp_msg_type,
                self.own_node_id,
                req.source_node,
                req.vshard_id,
                resp_payload,
            ));
        }

        // Remote path: encode the VShardEnvelope to bytes for the RaftRpc tunnel.
        let envelope_bytes = req.to_bytes();

        // Wrap in RaftRpc and send. The remote node's handler decodes the
        // envelope bytes and dispatches to the appropriate shard handler.
        let rpc = RaftRpc::VShardEnvelope(envelope_bytes);
        let resp_rpc = tokio::time::timeout(
            std::time::Duration::from_millis(timeout_ms),
            self.transport.send_rpc(node_id, rpc),
        )
        .await
        // The request can have reached the shard, so a timeout is never
        // resent: `call` retries only a `Transport` error.
        .map_err(|_| nodedb_cluster::error::ClusterError::ShardTimeout {
            vshard_id: req.vshard_id,
            elapsed_ms: timeout_ms,
        })??;

        match resp_rpc {
            RaftRpc::VShardEnvelope(bytes) => VShardEnvelope::from_bytes(&bytes).ok_or_else(|| {
                nodedb_cluster::error::ClusterError::Transport {
                    detail: "array shard response: failed to decode VShardEnvelope".into(),
                }
            }),
            // The shard's handler failed with a typed error. It is rebuilt as
            // the error the local short-circuit returns, so `WrongOwner`
            // reaches the fan-out's reroute retry.
            RaftRpc::VShardRefusal(refusal) => {
                Err(nodedb_cluster::error::ClusterError::from(refusal.error))
            }
            other => Err(nodedb_cluster::error::ClusterError::Transport {
                detail: format!(
                    "array shard RPC: unexpected response type {:?}",
                    std::mem::discriminant(&other)
                ),
            }),
        }
    }
}
