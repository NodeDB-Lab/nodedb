// SPDX-License-Identifier: BUSL-1.1

//! Resolution of nested-loop and sort-merge joins.
//!
//! Both read each side by collection name in the Data Plane, which sees
//! current committed rows only. Inside a transaction, or at a scope other
//! than `Current`, each by-name side is gathered through the collection's
//! full engine scan at the join's scope instead, so the join sees the
//! transaction's snapshot and staged writes on either side. The scan applies the side's
//! read policy, so an embedded side carries none of its own.

use nodedb_physical::physical_plan::{PhysicalPlan, QueryOp};
use nodedb_types::SystemTimeScope;

use crate::control::server::exchange::full_scan::ScanSide;
use crate::control::server::exchange::read_scope::ReadScope;
use crate::control::server::exchange::resolve::capture::DistributedReadCapture;
use crate::control::server::exchange::resolve::join_input::resolve_join_input;
use crate::control::state::SharedState;

use super::entry::Resolved;
use super::txn_source_arm::txn_join_side;

/// One by-name join side during resolution.
struct Side<'a> {
    collection: &'a str,
    rls_filters: &'a mut Vec<u8>,
    input: Option<Box<PhysicalPlan>>,
    /// The scope the join reads its by-name sides at.
    system_time: SystemTimeScope,
}

/// Resolve one side: an input already set has its Exchange children
/// resolved. A by-name side inside a transaction, or at a scope other than
/// `Current`, becomes its gathered engine scan at the join's scope, and its
/// read policy moves onto that scan.
async fn resolve_side(
    state: &SharedState,
    ctx: ReadScope,
    captures: &mut Vec<DistributedReadCapture>,
    side: Side<'_>,
) -> crate::Result<Option<Box<PhysicalPlan>>> {
    if side.input.is_some() {
        return resolve_join_input(state, ctx, side.input, captures).await;
    }
    let embedded = txn_join_side(
        state,
        ctx,
        captures,
        ScanSide::join_side(side.collection, side.rls_filters, &[]).at(side.system_time),
    )
    .await?;
    if embedded.is_some() {
        side.rls_filters.clear();
    }
    Ok(embedded)
}

/// Resolve a `QueryOp::NestedLoopJoin` or `QueryOp::SortMergeJoin`. Any
/// other plan comes back unchanged.
pub(super) async fn resolve_by_name_join(
    state: &SharedState,
    ctx: ReadScope,
    captures: &mut Vec<DistributedReadCapture>,
    plan: PhysicalPlan,
) -> crate::Result<Resolved> {
    let resolved = match plan {
        PhysicalPlan::Query(QueryOp::NestedLoopJoin {
            left_collection,
            right_collection,
            condition,
            join_type,
            limit,
            mut left_rls_filters,
            mut right_rls_filters,
            left_input,
            right_input,
            system_time,
        }) => {
            let left_input = resolve_side(
                state,
                ctx,
                captures,
                Side {
                    collection: left_collection.as_str(),
                    rls_filters: &mut left_rls_filters,
                    input: left_input,
                    system_time,
                },
            )
            .await?;
            let right_input = resolve_side(
                state,
                ctx,
                captures,
                Side {
                    collection: right_collection.as_str(),
                    rls_filters: &mut right_rls_filters,
                    input: right_input,
                    system_time,
                },
            )
            .await?;
            PhysicalPlan::Query(QueryOp::NestedLoopJoin {
                left_collection,
                right_collection,
                condition,
                join_type,
                limit,
                left_rls_filters,
                right_rls_filters,
                left_input,
                right_input,
                system_time,
            })
        }
        PhysicalPlan::Query(QueryOp::SortMergeJoin {
            left_collection,
            right_collection,
            on,
            join_type,
            limit,
            pre_sorted,
            mut left_rls_filters,
            mut right_rls_filters,
            left_input,
            right_input,
            system_time,
        }) => {
            let left_input = resolve_side(
                state,
                ctx,
                captures,
                Side {
                    collection: left_collection.as_str(),
                    rls_filters: &mut left_rls_filters,
                    input: left_input,
                    system_time,
                },
            )
            .await?;
            let right_input = resolve_side(
                state,
                ctx,
                captures,
                Side {
                    collection: right_collection.as_str(),
                    rls_filters: &mut right_rls_filters,
                    input: right_input,
                    system_time,
                },
            )
            .await?;
            PhysicalPlan::Query(QueryOp::SortMergeJoin {
                left_collection,
                right_collection,
                on,
                join_type,
                limit,
                pre_sorted,
                left_rls_filters,
                right_rls_filters,
                left_input,
                right_input,
                system_time,
            })
        }
        other => other,
    };
    Ok(Resolved::Plan(Box::new(resolved)))
}
