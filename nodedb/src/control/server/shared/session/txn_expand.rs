// SPDX-License-Identifier: BUSL-1.1

//! Reshape a fan-out write into the per-shard plans the transaction buffer
//! replays at COMMIT.
//!
//! `ClusterArrayOp::{Put, Delete}` is a Control-Plane routing wrapper with no
//! Data-Plane handler, so the buffer cannot replay it as-is. Partitioning its
//! cells by owning vShard yields ordinary `ArrayOp::{Put, Delete}` tasks,
//! which the existing buffer, WAL encoder, and COMMIT replay already handle.

use nodedb_array::types::ArrayId;
use nodedb_cluster::distributed_array::partition::{partition_delete_coords, partition_put_cells};
use nodedb_physical::physical_plan::{ArrayOp, ClusterArrayOp, PhysicalPlan};
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use crate::control::cluster::array_executor::flatten_blob_vec;
use crate::control::server::shared::sql::staging_predicates::StagedTagKind;
use crate::engine::array::wal::{ArrayDeleteCell, ArrayPutCell};
use crate::types::VShardId;

/// The per-vShard `ArrayOp::{Put, Delete}` tasks a `ClusterArrayOp::{Put,
/// Delete}` fans out to, with the command tag the statement answers with.
/// `None` for every other plan.
///
/// The statement-time fan-out stage ([`super::array_fanout_stage`]) stages
/// and buffers exactly these tasks, so the overlay each shard holds and the
/// plan COMMIT replays come from one partition.
pub fn expand_cluster_array_write(
    task: &PhysicalTask,
) -> crate::Result<Option<(StagedTagKind, Vec<PhysicalTask>)>> {
    if let PhysicalPlan::ClusterArray(ClusterArrayOp::Put {
        array_id,
        cells,
        prefix_bits,
        ..
    }) = &task.plan
    {
        let tasks = expand_put(task, array_id, cells, *prefix_bits)?;
        return Ok(Some((StagedTagKind::Insert, tasks)));
    }
    if let PhysicalPlan::ClusterArray(ClusterArrayOp::Delete {
        array_id,
        coords,
        prefix_bits,
        ..
    }) = &task.plan
    {
        let tasks = expand_delete(task, array_id, coords, *prefix_bits)?;
        return Ok(Some((StagedTagKind::Delete, tasks)));
    }
    Ok(None)
}

/// The tasks to buffer for `task`. A plan with no fan-out shape buffers as
/// itself, so this is the identity for every engine but distributed Array.
pub fn expand_for_buffering(task: PhysicalTask) -> crate::Result<Vec<PhysicalTask>> {
    match &task.plan {
        PhysicalPlan::ClusterArray(ClusterArrayOp::Put {
            array_id,
            cells,
            prefix_bits,
            ..
        }) => expand_put(&task, array_id, cells, *prefix_bits),
        PhysicalPlan::ClusterArray(ClusterArrayOp::Delete {
            array_id,
            coords,
            prefix_bits,
            ..
        }) => expand_delete(&task, array_id, coords, *prefix_bits),

        // Every other plan buffers verbatim. Enumerated rather than wildcarded
        // so a new fan-out variant forces an explicit decision here.
        PhysicalPlan::ClusterArray(ClusterArrayOp::Slice { .. } | ClusterArrayOp::Agg { .. })
        | PhysicalPlan::Vector(_)
        | PhysicalPlan::Graph(_)
        | PhysicalPlan::Document(_)
        | PhysicalPlan::Kv(_)
        | PhysicalPlan::Text(_)
        | PhysicalPlan::Columnar(_)
        | PhysicalPlan::Timeseries(_)
        | PhysicalPlan::Spatial(_)
        | PhysicalPlan::Crdt(_)
        | PhysicalPlan::Query(_)
        | PhysicalPlan::Meta(_)
        | PhysicalPlan::Array(_)
        | PhysicalPlan::ClusterEvent(_) => Ok(vec![task]),
    }
}

/// One `ArrayOp::Put` per owning vShard, over the cells that land on it.
fn expand_put(
    origin: &PhysicalTask,
    array_id: &ArrayId,
    cells: &[(u64, Vec<u8>)],
    prefix_bits: u8,
) -> crate::Result<Vec<PhysicalTask>> {
    partition_put_cells(cells, prefix_bits)
        .map_err(partition_error)?
        .into_iter()
        .map(|bucket| {
            let cells_msgpack =
                flatten_blob_vec::<ArrayPutCell>(&bucket.cells_msgpack, "buffered array put")?;
            Ok(shard_task(
                origin,
                VShardId::new(bucket.vshard_id),
                PhysicalPlan::Array(ArrayOp::Put {
                    array_id: array_id.clone(),
                    cells_msgpack,
                    // Stamped by the write funnel at COMMIT replay, exactly as
                    // on the single-node planner path.
                    wal_lsn: 0,
                    provenance: None,
                }),
            ))
        })
        .collect()
}

/// One `ArrayOp::Delete` per owning vShard, over the coords that land on it.
fn expand_delete(
    origin: &PhysicalTask,
    array_id: &ArrayId,
    coords: &[(u64, Vec<u8>)],
    prefix_bits: u8,
) -> crate::Result<Vec<PhysicalTask>> {
    partition_delete_coords(coords, prefix_bits)
        .map_err(partition_error)?
        .into_iter()
        .map(|bucket| {
            let coords_msgpack = flatten_blob_vec::<ArrayDeleteCell>(
                &bucket.coords_msgpack,
                "buffered array delete",
            )?;
            Ok(shard_task(
                origin,
                VShardId::new(bucket.vshard_id),
                PhysicalPlan::Array(ArrayOp::Delete {
                    array_id: array_id.clone(),
                    coords_msgpack,
                    wal_lsn: 0,
                    provenance: None,
                }),
            ))
        })
        .collect()
}

/// One per-shard task, inheriting the originating statement's tenant and
/// database. `txn_id` stays unset — `buffer_write` stamps the session's.
fn shard_task(origin: &PhysicalTask, vshard_id: VShardId, plan: PhysicalPlan) -> PhysicalTask {
    PhysicalTask {
        tenant_id: origin.tenant_id,
        vshard_id,
        database_id: origin.database_id,
        plan,
        post_set_op: PostSetOp::None,
        txn_id: None,
    }
}

/// Partitioning refuses only an out-of-range routing granularity, which comes
/// from the array catalog entry rather than the statement.
fn partition_error(e: nodedb_cluster::error::ClusterError) -> crate::Error {
    crate::Error::Internal {
        detail: format!("array write partition for transaction buffer: {e}"),
    }
}
