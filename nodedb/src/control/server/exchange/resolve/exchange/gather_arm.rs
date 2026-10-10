// SPDX-License-Identifier: BUSL-1.1

//! Root-level `Gather` and `Broadcast` exchange resolution: fan a child plan
//! to every vShard and merge, with a streaming fast path for `Gather`.

use nodedb_physical::physical_plan::PhysicalPlan;

use crate::control::server::exchange::full_scan::{ScanSide, full_scan_plan_for_collection};
use crate::control::server::exchange::gather::{
    GatherOutcome, finalize_aggregate, gather_all_vshards, outcome_to_response,
};
use crate::control::server::exchange::resolve::capture::DistributedReadCapture;
use crate::control::state::SharedState;

use crate::control::server::exchange::read_scope::ReadScope;

use super::dispatch::resolve_exchange;
use super::entry::Resolved;

/// Resolve a root-level `Exchange{Gather}` node.
pub(super) async fn resolve_gather(
    state: &SharedState,
    ctx: ReadScope,
    child: PhysicalPlan,
    as_aggregate: bool,
    captures: &mut Vec<DistributedReadCapture>,
) -> crate::Result<Resolved> {
    let ReadScope {
        database_id,
        tenant_id,
        trace_id,
        txn_id,
        linearizable,
    } = ctx;
    let child = match Box::pin(resolve_exchange(state, ctx, child, captures)).await? {
        Resolved::Plan(p) => *p,
        Resolved::Gathered(resp, wms, caps) => {
            return Ok(Resolved::Gathered(resp, wms, caps));
        }
        // A nested Exchange that itself resolved to a stream cannot be
        // re-wrapped by an outer Gather without materializing first;
        // surface it as the stream (the outer Gather is redundant —
        // nested root-level Gathers do not occur in practice, but if one
        // did, the inner stream is already the correct result).
        Resolved::Stream(s) => return Ok(Resolved::Stream(s)),
    };

    // Streaming fast path: a non-aggregate, unordered scan can stream
    // straight to the client without coordinator-side materialization.
    //
    // `gateway.execute_stream_internal` routes the scan to its owning
    // vShard — local cores when this node owns it, or the remote owner over
    // QUIC (L4 streaming transport) — and merges the per-route streams with
    // `select_all`.
    //
    // Aggregate gathers keep the materialize-then-merge behaviour.
    //
    // An in-transaction read (`txn_id.is_some()`) also keeps the
    // materialize path: streaming collapses per-core watermarks into one
    // value, but a transaction must record each participating shard's own
    // read version for optimistic-concurrency validation, so it takes the
    // `gather_all_vshards` branch below whose `GatherOutcome` preserves
    // `shard_watermarks`.
    if !as_aggregate && txn_id.is_none() && child.is_streamable_unordered_scan() {
        let gateway = state.installed_gateway()?;
        let ctx = crate::control::gateway::core::QueryContext {
            tenant_id,
            trace_id,
            database_id,
            txn_id: None,
            linearizable,
        };
        let stream = gateway.execute_stream_internal(&ctx, child).await?;
        return Ok(Resolved::Stream(stream));
    }

    // Determine the single base collection this gather observes for the
    // transaction read-set BEFORE the child plan is moved into the
    // gather. For a gathered `HashJoin` it is the probe (left) collection
    // scanned locally on the routed vShard; the build (right) collection
    // is captured separately at its own gather point in `join_input`. For
    // any other single-collection gather it is the child's own
    // collection. Only in-transaction reads need captures (the read-set
    // is only recorded inside a transaction block), so autocommit skips
    // the catalog lookup entirely.
    let probe_collection: Option<String> = if txn_id.is_some() {
        match &child {
            PhysicalPlan::Query(nodedb_physical::physical_plan::QueryOp::HashJoin {
                left_collection,
                ..
            }) => Some(left_collection.to_string()),
            other => other.collection().map(str::to_owned),
        }
    } else {
        None
    };

    let outcome: GatherOutcome = gather_all_vshards(state, child, ctx).await?;

    // Record the probe/single-collection read at its OWN observed read
    // versions (the gathered collection's version per vShard), scoped to
    // a bare single-collection scan so the commit-time OCC validator
    // re-homes and revalidates exactly that collection's vshard. A
    // `HashJoin` plan will otherwise collapse to the left collection
    // alone via `extract_collection` and miss the build side (captured
    // separately in `join_input`).
    if let Some(coll) = probe_collection
        && let Some(scan_plan) = full_scan_plan_for_collection(
            state,
            database_id,
            tenant_id,
            ScanSide::read_set_only(&coll),
        )?
    {
        captures.push(DistributedReadCapture {
            scan_plan,
            read_versions: outcome.read_versions.clone(),
        });
    }

    let payload = if as_aggregate {
        finalize_aggregate(&outcome.merged_array)
    } else {
        outcome.merged_array
    };
    Ok(Resolved::Gathered(
        outcome_to_response(payload, outcome.watermark_lsn, outcome.read_versions),
        outcome.shard_watermarks,
        std::mem::take(captures),
    ))
}

/// Resolve a root-level `Exchange{Broadcast}` node: unusual but treated as
/// Gather without merge.
pub(super) async fn resolve_broadcast(
    state: &SharedState,
    ctx: ReadScope,
    child: PhysicalPlan,
    captures: &mut Vec<DistributedReadCapture>,
) -> crate::Result<Resolved> {
    let outcome = gather_all_vshards(state, child, ctx).await?;
    Ok(Resolved::Gathered(
        outcome_to_response(
            outcome.merged_array,
            outcome.watermark_lsn,
            outcome.read_versions,
        ),
        outcome.shard_watermarks,
        std::mem::take(captures),
    ))
}
