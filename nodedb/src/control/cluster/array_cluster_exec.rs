//! Control-Plane executor for `PhysicalPlan::ClusterArray` variants.
//!
//! `NexarArrayDispatch` bridges `ShardRpcDispatch` ↔ `NexarTransport`,
//! tunnelling `VShardEnvelope` bytes through `RaftRpc::VShardEnvelope`.
//!
//! When the target shard is owned by the local node, `NexarArrayDispatch`
//! short-circuits the QUIC transport and dispatches directly to the local
//! Data Plane via `handle_array_shard_rpc`. This avoids the `NodeUnreachable`
//! error that arises when a node tries to `send_rpc` to itself (the transport
//! peer table does not contain an entry for the local node's own ID).
//!
//! `ClusterArrayExecutor` wraps the dispatch and routing table, constructs
//! the appropriate `ArrayCoordinator`, and converts the coordinator's
//! results into raw response bytes that the routing loop can return.

use std::sync::Arc;

use async_trait::async_trait;
use nodedb_cluster::circuit_breaker::{CircuitBreaker, CircuitBreakerConfig};
use nodedb_cluster::distributed_array::ArrayLocalExecutor;
use nodedb_cluster::distributed_array::coordinator::{
    ArrayCoordinator, ArrayWriteCoordParams, coord_delete, coord_put,
};
use nodedb_cluster::distributed_array::handler::handle_array_shard_rpc;
use nodedb_cluster::distributed_array::merge::ArrayAggPartial;
use nodedb_cluster::distributed_array::rpc::ShardRpcDispatch;
use nodedb_cluster::distributed_array::wire::{ArrayShardAggReq, ArrayShardSliceReq};
use nodedb_cluster::error::Result as ClusterResult;
use nodedb_cluster::routing::VSHARD_COUNT;
use nodedb_cluster::rpc_codec::RaftRpc;
use nodedb_cluster::wire::VShardEnvelope;
use nodedb_cluster::{NexarTransport, RoutingTable};

use crate::bridge::physical_plan::ClusterArrayOp;
use crate::control::cluster::array_cluster_helpers::{
    array_resp_msg_type, cluster_err, encode_err, finalize_agg_partials,
};
use crate::control::cluster::array_executor::DataPlaneArrayExecutor;
use crate::control::state::SharedState;

/// Default RPC timeout for array cluster operations in milliseconds.
const ARRAY_RPC_TIMEOUT_MS: u64 = 30_000;

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
            let req_opcode = req.msg_type as u16;
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
        .map_err(|_| nodedb_cluster::error::ClusterError::Transport {
            detail: format!("array shard RPC timeout ({timeout_ms}ms) to node {node_id}"),
        })??;

        match resp_rpc {
            RaftRpc::VShardEnvelope(bytes) => VShardEnvelope::from_bytes(&bytes).ok_or_else(|| {
                nodedb_cluster::error::ClusterError::Transport {
                    detail: "array shard response: failed to decode VShardEnvelope".into(),
                }
            }),
            other => Err(nodedb_cluster::error::ClusterError::Transport {
                detail: format!(
                    "array shard RPC: unexpected response type {:?}",
                    std::mem::discriminant(&other)
                ),
            }),
        }
    }
}

/// Control-Plane executor for distributed array operations.
///
/// Constructed per-request from `SharedState` by the routing interceptor.
/// Holds references to the transport and routing table; is not cached.
pub struct ClusterArrayExecutor {
    dispatch: Arc<dyn ShardRpcDispatch>,
    circuit_breaker: Arc<CircuitBreaker>,
    source_node: u64,
    total_shards: u16,
}

impl ClusterArrayExecutor {
    /// Construct from cluster state.
    ///
    /// `state` is used to construct the local-dispatch fast path in
    /// `NexarArrayDispatch` — when a target shard is owned by this node,
    /// the dispatcher bypasses QUIC and calls the local Data Plane directly.
    pub fn new(
        transport: Arc<NexarTransport>,
        routing: Arc<std::sync::RwLock<RoutingTable>>,
        source_node: u64,
        state: Arc<SharedState>,
    ) -> Self {
        let dispatch = Arc::new(NexarArrayDispatch::new(
            transport,
            routing,
            source_node,
            state,
        ));
        let circuit_breaker = Arc::new(CircuitBreaker::new(CircuitBreakerConfig::default()));
        Self {
            dispatch,
            circuit_breaker,
            source_node,
            total_shards: VSHARD_COUNT,
        }
    }

    /// Execute a `ClusterArrayOp` and return raw response bytes ready to be
    /// returned to the client. The response format mirrors local `ArrayOp`
    /// responses so downstream decode logic is unchanged.
    pub async fn execute(&self, op: &ClusterArrayOp) -> crate::Result<Vec<u8>> {
        match op {
            ClusterArrayOp::Slice {
                array_id,
                slice_msgpack,
                attr_projection,
                limit,
                slice_hilbert_ranges,
                prefix_bits,
            } => {
                self.execute_slice(
                    array_id,
                    slice_msgpack,
                    attr_projection,
                    *limit,
                    slice_hilbert_ranges,
                    *prefix_bits,
                )
                .await
            }

            ClusterArrayOp::Agg {
                array_id,
                attr_idx,
                reducer_msgpack,
                group_by_dim,
                slice_hilbert_ranges,
                prefix_bits,
            } => {
                self.execute_agg(
                    array_id,
                    *attr_idx,
                    reducer_msgpack,
                    *group_by_dim,
                    slice_hilbert_ranges,
                    *prefix_bits,
                )
                .await
            }

            ClusterArrayOp::Put {
                array_id_msgpack,
                cells,
                wal_lsn,
                prefix_bits,
                ..
            } => {
                self.execute_put(array_id_msgpack, cells, *wal_lsn, *prefix_bits)
                    .await
            }

            ClusterArrayOp::Delete {
                array_id_msgpack,
                coords,
                wal_lsn,
                prefix_bits,
                ..
            } => {
                self.execute_delete(array_id_msgpack, coords, *wal_lsn, *prefix_bits)
                    .await
            }
        }
    }

    async fn execute_slice(
        &self,
        array_id: &nodedb_array::types::ArrayId,
        slice_msgpack: &[u8],
        attr_projection: &[u32],
        limit: u32,
        slice_hilbert_ranges: &[(u64, u64)],
        prefix_bits: u8,
    ) -> crate::Result<Vec<u8>> {
        let coordinator = ArrayCoordinator::for_slice(
            self.source_node,
            ARRAY_RPC_TIMEOUT_MS,
            slice_hilbert_ranges,
            prefix_bits,
            self.total_shards,
            Arc::clone(&self.dispatch),
            Arc::clone(&self.circuit_breaker),
        )
        .map_err(cluster_err)?;

        let array_id_msgpack = zerompk::to_msgpack_vec(array_id).map_err(encode_err)?;
        let req = ArrayShardSliceReq {
            array_id_msgpack,
            slice_msgpack: slice_msgpack.to_vec(),
            attr_projection: attr_projection.to_vec(),
            limit,
            cell_filter_msgpack: vec![],
            // prefix_bits, slice_hilbert_ranges, and shard_hilbert_range are
            // stamped per-shard by coord_slice from ArrayCoordParams, which was
            // populated by for_slice above. Initialise to defaults here;
            // coord_slice overwrites them before serialising the request envelope.
            prefix_bits: 0,
            slice_hilbert_ranges: vec![],
            shard_hilbert_range: None,
        };
        let rows = coordinator
            .coord_slice(req, limit)
            .await
            .map_err(cluster_err)?;

        // Re-encode as a flat msgpack array — one element per row — to match
        // the local `ArrayOp::Slice` response shape that pgwire decodes via
        // `split_msgpack_array_rows`. A nested `Vec<Vec<u8>>` would prepend
        // an extra length header to each row.
        let mut out = Vec::with_capacity(5 + rows.iter().map(|r| r.len()).sum::<usize>());
        let n = rows.len();
        if n <= 15 {
            out.push(0x90 | (n as u8));
        } else if n <= u16::MAX as usize {
            out.push(0xdc);
            out.extend_from_slice(&(n as u16).to_be_bytes());
        } else {
            out.push(0xdd);
            out.extend_from_slice(&(n as u32).to_be_bytes());
        }
        for r in &rows {
            out.extend_from_slice(r);
        }
        Ok(out)
    }

    async fn execute_agg(
        &self,
        array_id: &nodedb_array::types::ArrayId,
        attr_idx: u32,
        reducer_msgpack: &[u8],
        group_by_dim: i32,
        slice_hilbert_ranges: &[(u64, u64)],
        prefix_bits: u8,
    ) -> crate::Result<Vec<u8>> {
        let coordinator = ArrayCoordinator::for_slice(
            self.source_node,
            ARRAY_RPC_TIMEOUT_MS,
            slice_hilbert_ranges,
            prefix_bits,
            self.total_shards,
            Arc::clone(&self.dispatch),
            Arc::clone(&self.circuit_breaker),
        )
        .map_err(cluster_err)?;

        let array_id_msgpack = zerompk::to_msgpack_vec(array_id).map_err(encode_err)?;
        let req = ArrayShardAggReq {
            array_id_msgpack,
            attr_idx,
            reducer_msgpack: reducer_msgpack.to_vec(),
            group_by_dim,
            cell_filter_msgpack: vec![],
            shard_hilbert_range: None,
        };
        let partials: Vec<ArrayAggPartial> =
            coordinator.coord_agg(req).await.map_err(cluster_err)?;

        // Decode the reducer so we know which field to finalize.
        let reducer: crate::bridge::physical_plan::ArrayReducer =
            zerompk::from_msgpack(reducer_msgpack).map_err(|e| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("agg reducer decode: {e}"),
            })?;

        // Finalize each partial into the same {"group", "result"} / {"result"}
        // shape the local ArrayOp::Aggregate path produces. This ensures
        // payload_to_response → decode_payload_to_json emits the same JSON
        // structure for both single-node and cluster agg queries.
        let rows = finalize_agg_partials(&partials, &reducer, group_by_dim);
        zerompk::to_msgpack_vec(&rows).map_err(encode_err)
    }

    async fn execute_put(
        &self,
        array_id_msgpack: &[u8],
        cells: &[(u64, Vec<u8>)],
        wal_lsn: u64,
        prefix_bits: u8,
    ) -> crate::Result<Vec<u8>> {
        let params = ArrayWriteCoordParams {
            source_node: self.source_node,
            timeout_ms: ARRAY_RPC_TIMEOUT_MS,
        };
        coord_put(
            &params,
            array_id_msgpack.to_vec(),
            prefix_bits,
            wal_lsn,
            cells,
            &self.dispatch,
            &self.circuit_breaker,
        )
        .await
        .map_err(cluster_err)?;

        // Return a simple `{"affected": N}` JSON payload — same shape as the
        // local ArrayOp::Put response so downstream decode is unchanged.
        let affected = cells.len() as u64;
        zerompk::to_msgpack_vec(&affected).map_err(encode_err)
    }

    async fn execute_delete(
        &self,
        array_id_msgpack: &[u8],
        coords: &[(u64, Vec<u8>)],
        wal_lsn: u64,
        prefix_bits: u8,
    ) -> crate::Result<Vec<u8>> {
        let params = ArrayWriteCoordParams {
            source_node: self.source_node,
            timeout_ms: ARRAY_RPC_TIMEOUT_MS,
        };
        coord_delete(
            &params,
            array_id_msgpack.to_vec(),
            prefix_bits,
            wal_lsn,
            coords,
            &self.dispatch,
            &self.circuit_breaker,
        )
        .await
        .map_err(cluster_err)?;

        let deleted = coords.len() as u64;
        zerompk::to_msgpack_vec(&deleted).map_err(encode_err)
    }
}
