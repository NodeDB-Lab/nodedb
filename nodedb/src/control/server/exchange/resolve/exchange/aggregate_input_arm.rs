// SPDX-License-Identifier: BUSL-1.1

//! Input-sourced `Aggregate` resolution: materialize the aggregate's child on
//! the coordinator, then hand the Data Plane an aggregate over a
//! `ProviderScan` of those rows.

use nodedb_physical::physical_plan::{
    AggregateSpec, GroupKeySpec, PhysicalPlan, QueryOp, SortKeySpec,
};
use nodedb_types::QualifiedCollection;

use crate::control::server::exchange::resolve::capture::DistributedReadCapture;
use crate::control::state::SharedState;

use super::entry::Resolved;
use super::post_process_arm::{ChildRows, materialize_child_rows, provider_scan_of_rows};
use crate::control::server::exchange::read_scope::ReadScope;

/// Fields of a `QueryOp::Aggregate { input: Some(_) }` plan node, carried
/// through resolution as one value.
pub(super) struct AggregateFields {
    pub collection: QualifiedCollection,
    pub input: Box<PhysicalPlan>,
    pub group_by: Vec<GroupKeySpec>,
    pub aggregates: Vec<AggregateSpec>,
    pub filters: Vec<u8>,
    pub system_time: nodedb_types::SystemTimeScope,
    pub having: Vec<u8>,
    pub limit: usize,
    pub sub_group_by: Vec<String>,
    pub sub_aggregates: Vec<AggregateSpec>,
    pub grouping_sets: Vec<Vec<u32>>,
    pub sort_keys: Vec<SortKeySpec>,
    pub group_key_aliases: Vec<Option<String>>,
}

/// Resolve an input-sourced `QueryOp::Aggregate`.
///
/// A child that is already a materialized `ProviderScan` (a catalog table or
/// continuous aggregate filled by pass 1, or constant rows) passes through
/// unchanged. Any other
/// child — an `Exchange{Gather}` over a sharded body, a `PostProcess`, a
/// constant result — is materialized on the coordinator and replaced by a
/// `ProviderScan` over its rows, so the aggregate runs exactly once over the
/// full relation and no Exchange reaches a Data-Plane core.
pub(super) async fn resolve_aggregate_input(
    state: &SharedState,
    ctx: ReadScope,
    captures: &mut Vec<DistributedReadCapture>,
    fields: AggregateFields,
) -> crate::Result<Resolved> {
    let AggregateFields {
        collection,
        input,
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
    } = fields;

    let rebuild = |input: Box<PhysicalPlan>| {
        Resolved::Plan(Box::new(PhysicalPlan::Query(QueryOp::Aggregate {
            collection,
            input: Some(input),
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
        })))
    };

    // Fast path: the child is already materialized rows. Pass 1 filled every
    // named provider and kept its name, so every `ProviderScan` here holds
    // its rows.
    if matches!(*input, PhysicalPlan::Query(QueryOp::ProviderScan { .. })) {
        return Ok(rebuild(input));
    }

    let (rows, sources) = match materialize_child_rows(state, ctx, captures, *input).await? {
        ChildRows::Rows { rows, sources } => (rows, sources),
        ChildRows::Passthrough(resolved) => return Ok(resolved),
    };
    Ok(rebuild(Box::new(provider_scan_of_rows(rows, sources))))
}
