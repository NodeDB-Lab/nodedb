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
use nodedb_cluster::distributed_array::wire::{
    ArrayShardAggReq, ArrayShardMaintOp, ArrayShardSliceReq,
};
use nodedb_cluster::routing::VSHARD_COUNT;
use nodedb_cluster::{NexarTransport, RoutingTable};

use crate::control::cluster::array_cluster_helpers::{
    cluster_err, coord_err, encode_err, encode_row_array, finalize_agg_partials,
};
use crate::control::server::shared::session::pending_shard_reads::{ShardObservation, ShardReads};
use crate::control::server::shared::session::read_set::EngineTag;
use crate::control::state::SharedState;
use crate::data::executor::response_codec;
use crate::types::{TxnId, VShardId};
use nodedb_physical::physical_plan::ClusterArrayOp;
use zerompk;

use super::dispatch::NexarArrayDispatch;

/// Default RPC timeout for array cluster operations in milliseconds.
pub(super) const ARRAY_RPC_TIMEOUT_MS: u64 = 30_000;

/// Control-Plane executor for distributed array operations.
///
/// Constructed per-request from `SharedState` by the routing interceptor.
/// Holds references to the transport and routing table; is not cached.
pub struct ClusterArrayExecutor {
    pub(super) dispatch: Arc<dyn ShardRpcDispatch>,
    pub(super) circuit_breaker: Arc<CircuitBreaker>,
    pub(super) source_node: u64,
    pub(super) total_shards: u32,
}

struct SliceArgs<'a> {
    array_id: &'a nodedb_array::types::ArrayId,
    slice_msgpack: &'a [u8],
    attr_projection: &'a [u32],
    limit: u32,
    slice_tile_keys: &'a [u64],
    prefix_bits: u8,
    system_time: nodedb_types::SystemTimeScope,
    valid_at_ms: Option<i64>,
    zone_msgpack: &'a [u8],
    physical: bool,
    txn_id: Option<TxnId>,
}

struct AggArgs<'a> {
    array_id: &'a nodedb_array::types::ArrayId,
    attr_idx: u32,
    reducer_msgpack: &'a [u8],
    group_by_dim: i32,
    slice_tile_keys: &'a [u64],
    prefix_bits: u8,
    system_time: nodedb_types::SystemTimeScope,
    valid_at_ms: Option<i64>,
    region_msgpack: &'a [u8],
    physical: bool,
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
                slice_tile_keys,
                prefix_bits,
                system_time,
                valid_at_ms,
                zone_msgpack,
                physical,
            } => {
                self.execute_slice(SliceArgs {
                    array_id,
                    slice_msgpack,
                    attr_projection,
                    limit: *limit,
                    slice_tile_keys,
                    prefix_bits: *prefix_bits,
                    system_time: *system_time,
                    valid_at_ms: *valid_at_ms,
                    zone_msgpack,
                    physical: *physical,
                    txn_id,
                })
                .await
            }

            ClusterArrayOp::Agg {
                array_id,
                attr_idx,
                reducer_msgpack,
                group_by_dim,
                slice_tile_keys,
                prefix_bits,
                system_time,
                valid_at_ms,
                region_msgpack,
                physical,
            } => {
                self.execute_agg(AggArgs {
                    array_id,
                    attr_idx: *attr_idx,
                    reducer_msgpack,
                    group_by_dim: *group_by_dim,
                    slice_tile_keys,
                    prefix_bits: *prefix_bits,
                    system_time: *system_time,
                    valid_at_ms: *valid_at_ms,
                    region_msgpack,
                    physical: *physical,
                    txn_id,
                })
                .await
            }

            ClusterArrayOp::Elementwise {
                left,
                right,
                op,
                attr_idx,
                prefix_bits,
                system_time,
                valid_at_ms,
            } => {
                self.execute_elementwise(super::elementwise::ElementwiseArgs {
                    left,
                    right,
                    op: *op,
                    attr_idx: *attr_idx,
                    prefix_bits: *prefix_bits,
                    system_time: *system_time,
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

            ClusterArrayOp::Flush {
                array_id,
                prefix_bits,
            } => {
                self.execute_maint(array_id, ArrayShardMaintOp::Flush, *prefix_bits)
                    .await
            }

            ClusterArrayOp::Compact {
                array_id,
                audit_retain_ms,
                prefix_bits,
            } => {
                let op = ArrayShardMaintOp::Compact {
                    audit_retain_ms: *audit_retain_ms,
                };
                self.execute_maint(array_id, op, *prefix_bits).await
            }
        }
    }

    async fn execute_slice(&self, args: SliceArgs<'_>) -> crate::Result<Vec<u8>> {
        let SliceArgs {
            array_id,
            slice_msgpack,
            attr_projection,
            limit,
            slice_tile_keys,
            prefix_bits,
            system_time,
            valid_at_ms,
            zone_msgpack,
            physical,
            txn_id,
        } = args;
        let coordinator = ArrayCoordinator::for_slice(
            self.source_node,
            ARRAY_RPC_TIMEOUT_MS,
            slice_tile_keys,
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
            // coord_slice stamps prefix_bits, slice_tile_keys and shard_bucket
            // per shard from the ArrayCoordParams that for_slice built.
            prefix_bits: 0,
            slice_tile_keys: vec![],
            shard_bucket: None,
            system_time,
            valid_at_ms,
            txn_id: txn_id.map(TxnId::as_u64),
            zone_msgpack: zone_msgpack.to_vec(),
            physical,
        };
        let result = coordinator
            .coord_slice(req, limit, system_time)
            .await
            .map_err(coord_err)?;
        note_array_reads(array_id, &result.read_versions);

        // Encode rows into the structured `ArraySliceResponse` — identical to
        // the shape that `dispatch_array_slice` emits for single-node requests.
        // This makes local and cluster payloads byte-identical so the pgwire
        // handler can use a single decoder regardless of topology. The
        // `truncated_before_horizon` flag is OR-reduced across shards by the
        // coordinator so the upstream NOTICE is emitted whenever any shard
        // dropped data below its system-time horizon.
        let resp = crate::data::executor::response_codec::ArraySliceResponse {
            rows_msgpack: encode_row_array(&result.rows),
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
            slice_tile_keys,
            prefix_bits,
            system_time,
            valid_at_ms,
            region_msgpack,
            physical,
            txn_id,
        } = args;
        let coordinator = ArrayCoordinator::for_slice(
            self.source_node,
            ARRAY_RPC_TIMEOUT_MS,
            slice_tile_keys,
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
            shard_bucket: None,
            system_time,
            valid_at_ms,
            txn_id: txn_id.map(TxnId::as_u64),
            region_msgpack: region_msgpack.to_vec(),
            physical,
        };
        let agg = coordinator.coord_agg(req).await.map_err(coord_err)?;
        note_array_reads(array_id, &agg.read_versions);

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
        let emit_horizon = system_time.is_temporal() || valid_at_ms.is_some();
        let rows = finalize_agg_partials(
            &agg.partials,
            &reducer,
            group_by_dim,
            agg.truncated_before_horizon,
            emit_horizon,
        )?;
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
        .map_err(coord_err)?;

        // Sum each shard's real `affected` count — never `cells.len()`, which
        // counts cells named in the request, not cells the Data Plane
        // actually wrote. `{"inserted": n}` matches what the local
        // `ArrayOp::Put` handler emits, so `extract_affected_count` reads both.
        let affected: u64 = resps.iter().map(|r| r.affected).sum();
        // Every cell of a put writes one cell version, a repeated coordinate
        // included. Fewer means a shard dropped cells it acknowledged.
        if affected != cells.len() as u64 {
            return Err(crate::Error::Internal {
                detail: format!(
                    "array put: the shards wrote {affected} of {} cells",
                    cells.len()
                ),
            });
        }
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
        .map_err(coord_err)?;

        // Sum each shard's real `affected` count — never `coords.len()`,
        // which counts coordinates named, not coordinates that existed and
        // were removed. A DELETE of an absent coordinate must answer 0, and
        // `{"deleted": n}` matches the local `ArrayOp::Delete` handler.
        let affected: u64 = resps.iter().map(|r| r.affected).sum();
        response_codec::encode_count("deleted", affected as usize)
    }
}

/// Note every vShard a cluster array read covered, at the version its shard
/// leg reported, for the running request's transaction read-set. A leg that
/// matched no row reports its vShard too. The request's protocol records the
/// note when the request ends (`session::pending_shard_reads::record_pending`), so
/// commit validation checks the read on each covered vShard: inside the
/// Calvin barrier, or against each vShard's leader on a local commit.
pub(super) fn note_array_reads(
    array_id: &nodedb_array::types::ArrayId,
    read_versions: &[nodedb_types::ShardVersion],
) {
    crate::control::server::shared::session::pending_shard_reads::note(ShardReads {
        engine: EngineTag::ClusterArray,
        tenant_id: array_id.tenant_id,
        database_id: array_id.database_id,
        collection: Some(array_id.name.clone()),
        shards: read_versions
            .iter()
            .map(|shard| ShardObservation {
                vshard: VShardId::new(shard.vshard),
                version: shard.version,
            })
            .collect(),
    });
}
