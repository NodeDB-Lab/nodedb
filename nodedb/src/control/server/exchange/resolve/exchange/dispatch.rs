// SPDX-License-Identifier: BUSL-1.1

//! Match dispatcher for pass 2: routes each `Exchange` / join / post-process
//! plan shape to its concern-specific resolver.

use nodedb_physical::physical_plan::{ExchangeMode, ExchangeOp, PhysicalPlan, QueryOp};

use crate::control::server::exchange::resolve::capture::DistributedReadCapture;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId, TraceId, TxnId};

use super::entry::Resolved;
use super::hash_join_arm::HashJoinFields;
use super::post_process_arm::PostProcessFields;
use super::{gather_arm, hash_join_arm, post_process_arm, shuffle_arm};

/// Request-scoped identifiers threaded through every arm resolver, bundled
/// to keep each resolver's argument list within the clippy default arity.
#[derive(Clone, Copy)]
pub(super) struct ResolveCtx {
    pub database_id: DatabaseId,
    pub tenant_id: TenantId,
    pub trace_id: TraceId,
    pub txn_id: Option<TxnId>,
}

/// Resolve any `Exchange` nodes in `plan`.
///
/// - Root-level `Gather` → gather all vShards, return `Resolved::Gathered`.
/// - `Broadcast` nested inside a `HashJoin` input → gather the child, embed
///   the `merged_array` as `ProviderScan{None, rows}`, return `Resolved::Plan`.
/// - Root-level `Shuffle` wrapping a `HashJoin` → orchestrate a cross-node
///   grace hash join, return `Resolved::Gathered`. `Shuffle` as a join input is
///   a typed error.
/// - Anything else → `Resolved::Plan` unchanged.
///
/// `captures` accumulates one [`DistributedReadCapture`] per base collection an
/// in-transaction distributed read observes: build/right sides push at their
/// gather points in [`crate::control::server::exchange::resolve::join_input::gather_join_build_side`]
/// / [`crate::control::server::exchange::resolve::join_input::resolve_join_input`],
/// the probe/single side pushes in the root Gather arm here. Only the outermost
/// root arm returning `Resolved::Gathered` `mem::take`s the accumulator, so
/// every base collection is captured exactly once and taken exactly once at the
/// true root; a nested `Exchange` that itself resolves to `Gathered` returns its
/// already-taken captures up unchanged.
pub(super) async fn resolve_exchange(
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: TenantId,
    plan: PhysicalPlan,
    trace_id: TraceId,
    txn_id: Option<TxnId>,
    captures: &mut Vec<DistributedReadCapture>,
) -> crate::Result<Resolved> {
    let ctx = ResolveCtx {
        database_id,
        tenant_id,
        trace_id,
        txn_id,
    };
    match plan {
        // Root-level Gather: fan child to all vShards and merge. First resolve any
        // Exchange{Broadcast} nodes nested inside the child (e.g. a HashJoin's
        // build side) so the plan fanned to cores is self-contained — no
        // Exchange node may reach a Data-Plane core.
        PhysicalPlan::Query(QueryOp::Exchange(ExchangeOp {
            child,
            mode: ExchangeMode::Gather { as_aggregate },
        })) => gather_arm::resolve_gather(state, ctx, *child, as_aggregate, captures).await,

        // Root-level Broadcast: unusual but treat as Gather without merge.
        PhysicalPlan::Query(QueryOp::Exchange(ExchangeOp {
            child,
            mode: ExchangeMode::Broadcast,
        })) => gather_arm::resolve_broadcast(state, ctx, *child, captures).await,

        // Root-level Shuffle: orchestrate a real cross-node grace hash join.
        // The child must be a `QueryOp::HashJoin` (shuffle wraps a complete hash
        // join); `resolve::shuffle` validates that, fans producers + consumers,
        // and returns the merged join rows as `Resolved::Gathered`.
        PhysicalPlan::Query(QueryOp::Exchange(ExchangeOp {
            child,
            mode: ExchangeMode::Shuffle { keys, num_parts },
        })) => shuffle_arm::resolve_shuffle(state, ctx, *child, keys, num_parts).await,

        // Root-level ShuffleAggregate: orchestrate a real cross-node distributed
        // GROUP BY shuffle. The child must be a `QueryOp::Aggregate` (shuffle
        // wraps a complete aggregate); `resolve::shuffle_aggregate` validates
        // that, fans the partial-state producers + per-part consumers, and
        // returns the merged finalized rows as `Resolved::Gathered`.
        PhysicalPlan::Query(QueryOp::Exchange(ExchangeOp {
            child,
            mode: ExchangeMode::ShuffleAggregate { keys, num_parts },
        })) => shuffle_arm::resolve_shuffle_aggregate(state, ctx, *child, keys, num_parts).await,

        // HashJoin: resolve Broadcast children embedded in left_input / right_input.
        PhysicalPlan::Query(QueryOp::HashJoin {
            left_collection,
            right_collection,
            left_alias,
            right_alias,
            on,
            join_type,
            limit,
            post_group_by,
            post_aggregates,
            projection,
            computed_projection,
            join_filters,
            post_filters,
            left_input,
            right_input,
            left_bitmap,
            right_bitmap,
            left_rls_filters,
            right_rls_filters,
            left_scan_filters,
            right_scan_filters,
        }) => {
            hash_join_arm::resolve_hash_join(
                state,
                ctx,
                captures,
                HashJoinFields {
                    left_collection: left_collection.to_string(),
                    right_collection: right_collection.to_string(),
                    left_alias,
                    right_alias,
                    on,
                    join_type,
                    limit,
                    post_group_by,
                    post_aggregates,
                    projection,
                    computed_projection,
                    join_filters,
                    post_filters,
                    left_input,
                    right_input,
                    left_bitmap,
                    right_bitmap,
                    left_rls_filters,
                    right_rls_filters,
                    left_scan_filters,
                    right_scan_filters,
                },
            )
            .await
        }

        // PostProcess: materialize the child's rows on the coordinator, then
        // lower to a `ProviderScan` that applies filter → offset → sort →
        // distinct → project → limit on a single core (its existing tail). This
        // keeps "run exactly once over the full union" correct: the child is
        // gathered here, so the relational tail never runs per-shard.
        PhysicalPlan::Query(QueryOp::PostProcess {
            input,
            filters,
            projection,
            computed_columns,
            sort_keys,
            limit,
            offset,
            distinct,
        }) => {
            post_process_arm::resolve_post_process(
                state,
                ctx,
                captures,
                PostProcessFields {
                    input,
                    filters,
                    projection,
                    computed_columns,
                    sort_keys,
                    limit,
                    offset,
                    distinct,
                },
            )
            .await
        }

        // All other plan variants: pass through unchanged.
        other => Ok(Resolved::Plan(Box::new(other))),
    }
}
