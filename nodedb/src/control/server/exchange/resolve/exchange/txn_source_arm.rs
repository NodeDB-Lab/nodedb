// SPDX-License-Identifier: BUSL-1.1

//! Read-your-writes and system-time scopes for per-shard reads.
//!
//! A per-shard aggregate and a join's by-name side read their collection by
//! name in the Data Plane, which sees committed rows only. The engine's
//! own full scan folds the transaction's staged writes and reads at any
//! system-time scope. Inside a transaction, or at a scope other than
//! `Current`, such a read is rewritten here to run over that scan, gathered
//! on the coordinator under the same predicates, read policy, and scope.

use nodedb_physical::physical_plan::{PhysicalPlan, QueryOp};

use crate::control::server::exchange::full_scan::{ScanSide, full_scan_plan_for_collection};
use crate::control::server::exchange::read_scope::ReadScope;
use crate::control::server::exchange::resolve::capture::DistributedReadCapture;
use crate::control::server::exchange::resolve::join_input::resolve_join_input;
use crate::control::state::SharedState;

use super::aggregate_input_arm::{AggregateFields, resolve_aggregate_input};
use super::entry::Resolved;

/// The outcome of [`txn_aggregate`]: the rewritten aggregate, or the plan
/// unchanged when no rewrite applies.
pub(super) enum TxnRewrite {
    Resolved(Resolved),
    Unchanged(Box<PhysicalPlan>),
}

/// Rewrite an in-transaction per-shard aggregate, or one at a system-time
/// scope other than `Current`, to aggregate over the collection's full
/// engine scan at the aggregate's scope.
///
/// The aggregate's `filters` already hold the WHERE predicates and the read
/// policy; they move onto the scan, which applies them per row before the
/// staged rows are folded. A current read outside a transaction, or any
/// other plan, comes back unchanged.
pub(super) async fn txn_aggregate(
    state: &SharedState,
    ctx: ReadScope,
    captures: &mut Vec<DistributedReadCapture>,
    plan: PhysicalPlan,
) -> crate::Result<TxnRewrite> {
    let PhysicalPlan::Query(QueryOp::Aggregate {
        collection,
        input: None,
        group_by,
        aggregates,
        filters,
        system_time,
        having,
        limit,
        sub_group_by,
        sub_aggregates,
        grouping_sets,
        sort_keys,
        group_key_aliases,
    }) = plan
    else {
        return Ok(TxnRewrite::Unchanged(Box::new(plan)));
    };
    if ctx.txn_id.is_none() && system_time.is_current() {
        return Ok(TxnRewrite::Unchanged(Box::new(PhysicalPlan::Query(
            QueryOp::Aggregate {
                collection,
                input: None,
                group_by,
                aggregates,
                filters,
                system_time,
                having,
                limit,
                sub_group_by,
                sub_aggregates,
                grouping_sets,
                sort_keys,
                group_key_aliases,
            },
        ))));
    }
    let scan = full_scan_plan_for_collection(
        state,
        ctx.database_id,
        ctx.tenant_id,
        ScanSide::join_side(collection.as_str(), &filters, &[]).at(system_time),
    )?;
    let Some(scan) = scan else {
        // No catalog record on this node: the by-name aggregate stands.
        return Ok(TxnRewrite::Unchanged(Box::new(PhysicalPlan::Query(
            QueryOp::Aggregate {
                collection,
                input: None,
                group_by,
                aggregates,
                filters,
                system_time,
                having,
                limit,
                sub_group_by,
                sub_aggregates,
                grouping_sets,
                sort_keys,
                group_key_aliases,
            },
        ))));
    };
    let resolved = resolve_aggregate_input(
        state,
        ctx,
        captures,
        AggregateFields {
            collection,
            input: Box::new(scan),
            group_by,
            aggregates,
            // The scan applies them.
            filters: Vec::new(),
            system_time,
            having,
            limit,
            sub_group_by,
            sub_aggregates,
            grouping_sets,
            sort_keys,
            group_key_aliases,
        },
    )
    .await?;
    Ok(TxnRewrite::Resolved(resolved))
}

/// Gather a join's by-name side through the collection's full engine scan
/// inside a transaction, so the side's rows include the staged writes, or
/// at a scope other than `Current`, so the side reads the scope's versions.
/// Returns the embedded input, or `None` when the side keeps its by-name
/// scan.
pub(super) async fn txn_join_side(
    state: &SharedState,
    ctx: ReadScope,
    captures: &mut Vec<DistributedReadCapture>,
    side: ScanSide<'_>,
) -> crate::Result<Option<Box<PhysicalPlan>>> {
    if (ctx.txn_id.is_none() && side.system_time().is_current()) || side.collection().is_empty() {
        return Ok(None);
    }
    let Some(scan) = full_scan_plan_for_collection(state, ctx.database_id, ctx.tenant_id, side)?
    else {
        return Ok(None);
    };
    let gathered = PhysicalPlan::Query(QueryOp::Exchange(
        nodedb_physical::physical_plan::ExchangeOp {
            child: Box::new(scan),
            mode: nodedb_physical::physical_plan::ExchangeMode::Broadcast,
        },
    ));
    resolve_join_input(state, ctx, Some(Box::new(gathered)), captures).await
}
