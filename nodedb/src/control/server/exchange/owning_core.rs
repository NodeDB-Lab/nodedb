// SPDX-License-Identifier: BUSL-1.1

//! Route a single-vShard-homed plan to its ONE owning Data-Plane core.
//!
//! `gather_single_owning_core` is the one-core sibling of
//! [`super::gather::gather_all_cores`]. It dispatches the bare plan to the one
//! core that owns a vShard, the way [`super::gather::gather_all_vshards`]
//! routes a single-vShard-homed plan through the gateway. Broadcasting instead
//! will seed an identity scalar-aggregate row on each empty non-owning core,
//! and a no-`GROUP BY` aggregate will return one row per core.

use crate::bridge::envelope::PhysicalPlan;
use crate::control::local_dispatch::reject_data_plane_error;
use crate::control::server::dispatch_utils::dispatch_routed_read_to_data_plane;
use crate::control::server::payload_merge::{encode_msgpack_array, extract_msgpack_elements};
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId, TraceId, TxnId, VShardId};

use super::gather::GatherOutcome;

/// Dispatch `plan` to the single Data-Plane core that owns `vshard_id` and
/// gather the one bounded response into a [`GatherOutcome`].
///
/// `vshard_id` is the collection's owning vShard (the vShard of its
/// canonical `CollectionKey`); the
/// dispatcher's `VShardRouter` resolves it to the one core holding the
/// collection's rows.
///
/// The returned outcome carries that core's own `watermark_lsn` /
/// `read_versions` and exactly one `shard_watermarks` entry keyed to the
/// collection's vShard — matching the cluster `dispatch_local` path so an
/// in-transaction read records the same OCC read-set entry the write-set uses
/// (writes home to the same `CollectionKey` vShard). Aggregate
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
        read_versions: resp.read_versions.clone(),
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
    // Every caller already routed the read to this node and confirmed it: a
    // single-node gather, or a leg another node sent here.
    let resp = Box::pin(dispatch_routed_read_to_data_plane(
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
