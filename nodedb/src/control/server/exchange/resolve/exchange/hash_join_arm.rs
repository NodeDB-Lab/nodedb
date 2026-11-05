// SPDX-License-Identifier: BUSL-1.1

//! `HashJoin` exchange resolution: resolve `Broadcast` children embedded in
//! `left_input` / `right_input`, and cross-node build-side gather.

use nodedb_physical::physical_plan::{PhysicalPlan, QueryOp};

use crate::control::server::exchange::full_scan::ScanSide;
use crate::control::server::exchange::resolve::capture::DistributedReadCapture;
use crate::control::server::exchange::resolve::join_input::{
    gather_join_build_side, resolve_join_input,
};
use crate::control::state::SharedState;

use super::entry::Resolved;
use crate::control::server::exchange::read_scope::ReadScope;

/// Fields of a `QueryOp::HashJoin` plan node, carried through resolution as
/// one value instead of as individually threaded arguments.
pub(super) struct HashJoinFields {
    pub left_collection: String,
    pub right_collection: String,
    pub left_alias: Option<String>,
    pub right_alias: Option<String>,
    pub on: Vec<(String, String)>,
    pub join_type: String,
    pub limit: usize,
    pub projection: Vec<nodedb_physical::physical_plan::JoinProjection>,
    pub computed_projection: Vec<u8>,
    pub join_filters: Vec<u8>,
    pub post_filters: Vec<u8>,
    pub left_input: Option<Box<PhysicalPlan>>,
    pub right_input: Option<Box<PhysicalPlan>>,
    pub left_bitmap: Option<Box<PhysicalPlan>>,
    pub right_bitmap: Option<Box<PhysicalPlan>>,
    pub left_rls_filters: Vec<u8>,
    pub right_rls_filters: Vec<u8>,
    pub left_scan_filters: Vec<u8>,
    pub right_scan_filters: Vec<u8>,
}

/// Resolve a `QueryOp::HashJoin` node: resolve `Broadcast` children embedded
/// in `left_input` / `right_input`, then cross-node gather the build side.
pub(super) async fn resolve_hash_join(
    state: &SharedState,
    ctx: ReadScope,
    captures: &mut Vec<DistributedReadCapture>,
    fields: HashJoinFields,
) -> crate::Result<Resolved> {
    let HashJoinFields {
        left_collection,
        right_collection,
        left_alias,
        right_alias,
        on,
        join_type,
        limit,
        projection,
        computed_projection,
        join_filters,
        post_filters,
        left_input,
        mut right_input,
        left_bitmap,
        right_bitmap,
        mut left_rls_filters,
        right_rls_filters,
        mut left_scan_filters,
        right_scan_filters,
    } = fields;

    let mut left_input = resolve_join_input(state, ctx, left_input, captures).await?;
    right_input = resolve_join_input(state, ctx, right_input, captures).await?;

    // Inside a transaction the by-name probe side reads its own staged
    // writes through the collection's engine scan. The scan applies the
    // side's policy and predicates, so the embedded side carries neither.
    if left_input.is_none() {
        left_input = super::txn_source_arm::txn_join_side(
            state,
            ctx,
            captures,
            ScanSide::join_side(&left_collection, &left_rls_filters, &left_scan_filters),
        )
        .await?;
        if left_input.is_some() {
            left_rls_filters.clear();
            left_scan_filters.clear();
        }
    }

    // Cross-node build-side gather.
    //
    // The HashJoin task routes to the LEFT (probe) collection's owning
    // vShard, where the LEFT side is scanned locally. The RIGHT (build)
    // collection is otherwise scanned BY NAME from that same node — but
    // a single-vShard-homed build collection can live on a DIFFERENT
    // node, so the by-name scan returns nothing and the join drops rows.
    //
    // When the build side has not already been materialized by
    // `resolve_join_input` (`right_input` still `None`), and
    // `right_collection` names a real user collection (catalog sides
    // carry an empty name and are already embedded as a
    // ProviderScan), gather the build collection
    // across all vShards on the coordinator and inline it as a
    // `ProviderScan`. The HashJoin shipped to the probe node is then
    // self-contained. Only the RIGHT/build side is gathered; the
    // LEFT/probe side stays local to the routed vShard.
    if right_input.is_none() && !right_collection.is_empty() {
        right_input = gather_join_build_side(
            state,
            ctx,
            // The side's own collection and its own injected policy,
            // taken as one value: a planner that swaps build and probe
            // swaps both together, never one without the other.
            ScanSide::join_side(&right_collection, &right_rls_filters, &right_scan_filters),
            captures,
        )
        .await?;
    }

    Ok(Resolved::Plan(Box::new(PhysicalPlan::Query(
        QueryOp::HashJoin {
            left_collection: nodedb_types::QualifiedCollection::from_stored(
                ctx.database_id,
                left_collection,
            )?,
            right_collection: nodedb_types::QualifiedCollection::from_stored(
                ctx.database_id,
                right_collection,
            )?,
            left_alias,
            right_alias,
            on,
            join_type,
            limit,
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
    ))))
}
