// SPDX-License-Identifier: BUSL-1.1

//! `ClusterArrayExecutor` wraps the `NexarArrayDispatch` and routing table,
//! constructs the appropriate `ArrayCoordinator`, and converts the
//! coordinator's results into raw response bytes that the routing loop can
//! return.

use std::sync::Arc;

use nodedb_cluster::circuit_breaker::{CircuitBreaker, CircuitBreakerConfig};
use nodedb_cluster::distributed_array::coordinator::{
    ArrayCoordinator, ArrayWriteCoordParams, coord_delete, coord_put,
};
use nodedb_cluster::distributed_array::rpc::ShardRpcDispatch;
use nodedb_cluster::distributed_array::wire::{ArrayShardAggReq, ArrayShardSliceReq};
use nodedb_cluster::routing::VSHARD_COUNT;
use nodedb_cluster::{NexarTransport, RoutingTable};

use crate::control::cluster::array_cluster_helpers::{
    cluster_err, encode_err, finalize_agg_partials,
};
use crate::control::state::SharedState;
use crate::data::executor::response_codec;
use crate::types::TxnId;
use nodedb_physical::physical_plan::ClusterArrayOp;
use zerompk;

use super::dispatch::NexarArrayDispatch;

/// Default RPC timeout for array cluster operations in milliseconds.
const ARRAY_RPC_TIMEOUT_MS: u64 = 30_000;

/// Control-Plane executor for distributed array operations.
///
/// Constructed per-request from `SharedState` by the routing interceptor.
/// Holds references to the transport and routing table; is not cached.
pub struct ClusterArrayExecutor {
    dispatch: Arc<dyn ShardRpcDispatch>,
    circuit_breaker: Arc<CircuitBreaker>,
    source_node: u64,
    total_shards: u32,
}

struct SliceArgs<'a> {
    array_id: &'a nodedb_array::types::ArrayId,
    slice_msgpack: &'a [u8],
    attr_projection: &'a [u32],
    limit: u32,
    slice_hilbert_ranges: &'a [(u64, u64)],
    prefix_bits: u8,
    system_time: nodedb_types::SystemTimeScope,
    valid_at_ms: Option<i64>,
    txn_id: Option<TxnId>,
}

struct AggArgs<'a> {
    array_id: &'a nodedb_array::types::ArrayId,
    attr_idx: u32,
    reducer_msgpack: &'a [u8],
    group_by_dim: i32,
    slice_hilbert_ranges: &'a [(u64, u64)],
    prefix_bits: u8,
    system_as_of: Option<i64>,
    valid_at_ms: Option<i64>,
    txn_id: Option<TxnId>,
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
    ///
    /// `txn_id` is the reading transaction's id. A `Slice`/`Agg` inside a
    /// transaction block carries it to every shard so each shard folds its
    /// own staged cells into the result. A `Put`/`Delete` never runs here
    /// inside a transaction block: the staging gate fans it out per shard
    /// (`session::array_fanout_stage`) before it reaches this executor.
    pub(crate) async fn execute(
        &self,
        op: &ClusterArrayOp,
        txn_id: Option<TxnId>,
    ) -> crate::Result<Vec<u8>> {
        match op {
            ClusterArrayOp::Slice {
                array_id,
                slice_msgpack,
                attr_projection,
                limit,
                slice_hilbert_ranges,
                prefix_bits,
                system_time,
                valid_at_ms,
            } => {
                self.execute_slice(SliceArgs {
                    array_id,
                    slice_msgpack,
                    attr_projection,
                    limit: *limit,
                    slice_hilbert_ranges,
                    prefix_bits: *prefix_bits,
                    system_time: *system_time,
                    valid_at_ms: *valid_at_ms,
                    txn_id,
                })
                .await
            }

            ClusterArrayOp::Agg {
                array_id,
                attr_idx,
                reducer_msgpack,
                group_by_dim,
                slice_hilbert_ranges,
                prefix_bits,
                system_as_of,
                valid_at_ms,
            } => {
                self.execute_agg(AggArgs {
                    array_id,
                    attr_idx: *attr_idx,
                    reducer_msgpack,
                    group_by_dim: *group_by_dim,
                    slice_hilbert_ranges,
                    prefix_bits: *prefix_bits,
                    system_as_of: *system_as_of,
                    valid_at_ms: *valid_at_ms,
                    txn_id,
                })
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

    async fn execute_slice(&self, args: SliceArgs<'_>) -> crate::Result<Vec<u8>> {
        let SliceArgs {
            array_id,
            slice_msgpack,
            attr_projection,
            limit,
            slice_hilbert_ranges,
            prefix_bits,
            system_time,
            valid_at_ms,
            txn_id,
        } = args;
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
            system_time,
            valid_at_ms,
            txn_id: txn_id.map(TxnId::as_u64),
        };
        let result = coordinator
            .coord_slice(req, limit, system_time)
            .await
            .map_err(cluster_err)?;

        // Encode rows into the structured `ArraySliceResponse` — identical to
        // the shape that `dispatch_array_slice` emits for single-node requests.
        // This makes local and cluster payloads byte-identical so the pgwire
        // handler can use a single decoder regardless of topology. The
        // `truncated_before_horizon` flag is OR-reduced across shards by the
        // coordinator so the upstream NOTICE is emitted whenever any shard
        // dropped data below its system-time horizon.
        let mut rows_msgpack =
            Vec::with_capacity(5 + result.rows.iter().map(|r| r.len()).sum::<usize>());
        let n = result.rows.len();
        if n <= 15 {
            rows_msgpack.push(0x90 | (n as u8));
        } else if n <= u16::MAX as usize {
            rows_msgpack.push(0xdc);
            rows_msgpack.extend_from_slice(&(n as u16).to_be_bytes());
        } else {
            rows_msgpack.push(0xdd);
            rows_msgpack.extend_from_slice(&(n as u32).to_be_bytes());
        }
        for r in &result.rows {
            rows_msgpack.extend_from_slice(r);
        }
        let resp = crate::data::executor::response_codec::ArraySliceResponse {
            rows_msgpack,
            truncated_before_horizon: result.truncated_before_horizon,
        };
        zerompk::to_msgpack_vec(&resp).map_err(|e| crate::Error::Codec {
            detail: format!("cluster slice response encode: {e}"),
        })
    }

    async fn execute_agg(&self, args: AggArgs<'_>) -> crate::Result<Vec<u8>> {
        let AggArgs {
            array_id,
            attr_idx,
            reducer_msgpack,
            group_by_dim,
            slice_hilbert_ranges,
            prefix_bits,
            system_as_of,
            valid_at_ms,
            txn_id,
        } = args;
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
            system_as_of,
            valid_at_ms,
            txn_id: txn_id.map(TxnId::as_u64),
        };
        let agg = coordinator.coord_agg(req).await.map_err(cluster_err)?;

        // Decode the reducer so we know which field to finalize.
        let reducer: nodedb_physical::physical_plan::ArrayReducer =
            zerompk::from_msgpack(reducer_msgpack).map_err(|e| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("agg reducer decode: {e}"),
            })?;

        // Finalize each partial into the same {"group", "result"} / {"result"}
        // shape the local ArrayOp::Aggregate path produces, plus the trailing
        // {"truncated_before_horizon": bool} summary row for temporal queries
        // only. This ensures payload_to_response → decode_payload_to_json emits
        // the same JSON structure for both single-node and cluster agg queries —
        // including the below-horizon signal, which must not be silently dropped.
        let emit_horizon = system_as_of.is_some() || valid_at_ms.is_some();
        let rows = finalize_agg_partials(
            &agg.partials,
            &reducer,
            group_by_dim,
            agg.truncated_before_horizon,
            emit_horizon,
        );
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
        let resps = coord_put(
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

        // Sum each shard's real `affected` count — never `cells.len()`, which
        // counts cells named in the request, not cells the Data Plane
        // actually wrote. `{"inserted": n}` matches what the local
        // `ArrayOp::Put` handler emits, so `extract_affected_count` reads both.
        let affected: u64 = resps.iter().map(|r| r.affected).sum();
        response_codec::encode_count("inserted", affected as usize)
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
        let resps = coord_delete(
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

        // Sum each shard's real `affected` count — never `coords.len()`,
        // which counts coordinates named, not coordinates that existed and
        // were removed. A DELETE of an absent coordinate must answer 0, and
        // `{"deleted": n}` matches the local `ArrayOp::Delete` handler.
        let affected: u64 = resps.iter().map(|r| r.affected).sum();
        response_codec::encode_count("deleted", affected as usize)
    }
}
