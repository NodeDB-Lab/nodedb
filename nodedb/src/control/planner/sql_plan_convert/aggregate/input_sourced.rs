// SPDX-License-Identifier: BUSL-1.1

//! Aggregate over an input-sourced body: a derived table, constant result, or
//! union that carries no routing collection. The body lowers to one physical
//! plan, a sharded body is gathered, and one coordinator-local `Aggregate`
//! task runs over the materialized rows.

use nodedb_sql::types::{AggregateExpr, Filter, SqlExpr, SqlPlan};

use crate::types::TenantId;
use nodedb_physical::physical_plan::*;
use nodedb_physical::physical_task::PhysicalTask;

use super::super::body::convert_body_to_single_plan;
use super::super::convert::ConvertContext;
use super::super::filter::serialize_filters;
use super::spec::{InputSourcedTaskParams, build_input_sourced_aggregate_task};

pub(super) struct InputSourcedAggregateParams<'a> {
    pub input: &'a SqlPlan,
    pub group_by: &'a [SqlExpr],
    pub aggregates: &'a [AggregateExpr],
    pub having: &'a [Filter],
    pub limit: usize,
    pub grouping_sets: Option<&'a [Vec<usize>]>,
    /// Post-aggregate sort keys, already lowered to bridge specs.
    pub sort_keys: Vec<SortKeySpec>,
    pub tenant_id: TenantId,
    pub ctx: &'a ConvertContext,
}

/// Lower an aggregate whose input is a materialized relation.
///
/// The body lowers to ONE relation through `convert_body_to_single_plan`: a
/// set-operation body becomes a coordinator-resolved `SetOp`, and a sharded
/// body is wrapped in `Exchange{Gather}` so the coordinator resolves it to a
/// `ProviderScan` before the aggregate runs. The emitted task is
/// coordinator-local: an empty collection keeps it on the coordinator vshard
/// and `is_sharded_source` reports the `Some(input)` aggregate as
/// non-sharded, so it runs once and is never broadcast.
pub(super) fn convert_input_sourced_aggregate(
    p: InputSourcedAggregateParams<'_>,
) -> crate::Result<Vec<PhysicalTask>> {
    let InputSourcedAggregateParams {
        input,
        group_by,
        aggregates,
        having,
        limit,
        grouping_sets,
        sort_keys,
        tenant_id,
        ctx,
    } = p;

    // The input-sourced aggregate executor does not expand ROLLUP / CUBE /
    // GROUPING SETS. A typed error beats a silent base-grouping-only answer.
    if grouping_sets.is_some_and(|sets| !sets.is_empty()) {
        return Err(crate::Error::PlanError {
            detail: "ROLLUP / CUBE / GROUPING SETS over a derived-table body is not supported"
                .to_string(),
        });
    }

    // The body is ONE relation, already gathered when sharded, so the
    // aggregate observes the full union exactly once.
    let child = convert_body_to_single_plan(input, tenant_id, ctx)?;

    let having_bytes = serialize_filters(having)?;

    Ok(vec![build_input_sourced_aggregate_task(
        InputSourcedTaskParams {
            tenant_id,
            ctx,
            raw_collection: String::new(),
            child,
            group_by,
            aggregates,
            having_bytes,
            limit,
            sort_keys,
        },
    )])
}
