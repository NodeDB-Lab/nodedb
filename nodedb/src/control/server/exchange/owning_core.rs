// SPDX-License-Identifier: BUSL-1.1

//! Route a single-vShard-homed plan to its ONE owning Data-Plane core.
//!
//! `gather_single_owning_core` is the single-node sibling of
//! [`super::gather::gather_all_cores`] for plans whose whole collection lives on
//! exactly one vShard (document / kv / columnar / timeseries / spatial / vector
//! / text). Instead of broadcasting the plan to every core — which seeds an
//! identity scalar-aggregate row on each empty non-owning core and returns one
//! row per core for a no-`GROUP BY` aggregate — it resolves the collection's
//! owning vShard and dispatches the bare plan to it alone, exactly mirroring
//! what the cluster branch of [`super::gather::gather_all_vshards`] does via the
//! gateway. The owning core already holds every row of a single-vShard-homed
//! collection, so this returns the identical row set a broadcast would have,
//! minus the empty cores' spurious contributions.

use crate::bridge::envelope::PhysicalPlan;
use crate::control::server::dispatch_utils::{
    dispatch_to_data_plane_with_txn, reject_data_plane_error,
};
use crate::control::server::payload_merge::{encode_msgpack_array, extract_msgpack_elements};
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId, TraceId, TxnId, VShardId};

use super::gather::{GatherOutcome, gather_all_cores};

/// Single-node routing decision for a resolved `Exchange{Gather}` child plan.
///
/// Mirrors the cluster branch of [`super::gather::gather_all_vshards`]:
///
/// - A cluster-partitioned leaf (graph traversal / array) spreads its rows
///   across cores by node-id / tile-id, so it fans to every local core via
///   [`gather_all_cores`].
/// - A single-vShard-homed collection (document / kv / columnar / timeseries /
///   spatial / vector / text) lives wholly on ONE core; the bare plan routes to
///   that owning core via [`gather_single_owning_core`]. Broadcasting would seed
///   a scalar-aggregate identity row on every empty non-owning core — so a
///   no-`GROUP BY` aggregate returns one row PER core instead of one merged row
///   — and would duplicate a plain scan's rows across cores.
/// - A plan with no resolvable collection (e.g. `ProviderScan` carrying embedded
///   rows) keeps the broadcast fallback unchanged.
pub async fn gather_single_node(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    plan: PhysicalPlan,
    trace_id: TraceId,
    txn_id: Option<TxnId>,
) -> crate::Result<GatherOutcome> {
    if nodedb_physical::physical_plan::plan_contains_cluster_partitioned_leaf(&plan) {
        return gather_all_cores(state, tenant_id, database_id, plan, trace_id, txn_id).await;
    }
    if let Some(collection) = plan.collection() {
        let vshard_id = VShardId::from_collection_in_database(database_id, collection);
        return gather_single_owning_core(
            state,
            tenant_id,
            database_id,
            plan,
            vshard_id,
            trace_id,
            txn_id,
        )
        .await;
    }
    gather_all_cores(state, tenant_id, database_id, plan, trace_id, txn_id).await
}

/// Dispatch `plan` to the single Data-Plane core that owns `vshard_id` and
/// gather the one bounded response into a [`GatherOutcome`].
///
/// `vshard_id` is the collection's owning vShard
/// (`VShardId::from_collection_in_database(database_id, collection)`); the
/// dispatcher's `VShardRouter` resolves it to the one core holding the
/// collection's rows.
///
/// The returned outcome carries that core's own `watermark_lsn` /
/// `read_version_lsn` and exactly one `shard_watermarks` entry keyed to the
/// collection's vShard — matching the cluster `dispatch_local` path so an
/// in-transaction read records the same OCC read-set entry the write-set uses
/// (writes home to the same `from_collection_in_database` vShard). Aggregate
/// finalization (`finalize_aggregate`) is a passthrough over the merged array,
/// so one complete aggregate row in yields one row out.
pub async fn gather_single_owning_core(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    plan: PhysicalPlan,
    vshard_id: VShardId,
    trace_id: TraceId,
    txn_id: Option<TxnId>,
) -> crate::Result<GatherOutcome> {
    let resp = dispatch_single_owning_core(
        state,
        tenant_id,
        database_id,
        plan,
        vshard_id,
        trace_id,
        txn_id,
    )
    .await?;
    let payload_bytes: &[u8] = resp.payload.as_ref();
    let all_elements = extract_msgpack_elements(payload_bytes);
    let merged_array = encode_msgpack_array(&all_elements);

    Ok(GatherOutcome {
        raw: payload_bytes.to_vec(),
        merged_array,
        watermark_lsn: resp.watermark_lsn,
        read_version_lsn: resp.read_version_lsn,
        shard_watermarks: vec![(vshard_id, resp.watermark_lsn)],
    })
}

/// Dispatch `plan` to the single Data-Plane core that owns `vshard_id` and
/// return that core's response, its payload in the shape the core produced.
pub async fn dispatch_single_owning_core(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    plan: PhysicalPlan,
    vshard_id: VShardId,
    trace_id: TraceId,
    txn_id: Option<TxnId>,
) -> crate::Result<crate::bridge::envelope::Response> {
    // `Box::pin` breaks an async-fn recursion cycle: `dispatch_to_data_plane_*`
    // re-enters `resolve_exchange_in_plan`. The plan handed here is the bare,
    // Exchange-free child of the resolved Gather, so the re-entrant resolve is a
    // no-op — but the future must be heap-indirected so its size stays finite.
    let resp = Box::pin(dispatch_to_data_plane_with_txn(
        state,
        tenant_id,
        database_id,
        vshard_id,
        plan,
        trace_id,
        txn_id,
    ))
    .await?;

    // Shared boundary rule: `NotFound` reads back as an empty (still
    // validatable) observation; any other error status surfaces with its typed
    // code rather than being swallowed as an empty success.
    reject_data_plane_error(&resp)?;
    Ok(resp)
}
