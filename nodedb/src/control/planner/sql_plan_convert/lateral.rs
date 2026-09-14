// SPDX-License-Identifier: BUSL-1.1

//! Bridge lowering for `SqlPlan::LateralTopK` and `SqlPlan::LateralLoop`.
//!
//! Both variants embed the outer sub-plan as an `outer_plan: Box<PhysicalPlan>`
//! inside the `QueryOp` so the Data Plane executor can materialise outer rows
//! in-process before iterating over them.

use nodedb_sql::types::{Filter, Projection, SortKey, SqlPlan};

use crate::bridge::envelope::PhysicalPlan;
use crate::types::TenantId;
use nodedb_physical::physical_plan::SortKeySpec;
use nodedb_physical::physical_plan::{JoinProjection, QueryOp};

use super::convert::ConvertContext;
use super::filter::serialize_filters;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

/// Bundled arguments for [`convert_lateral_top_k`].
pub(super) struct ConvertLateralTopKParams<'a> {
    pub outer: &'a SqlPlan,
    pub outer_alias: Option<&'a str>,
    pub inner_collection: &'a str,
    pub inner_filters: &'a [Filter],
    pub inner_order_by: &'a [SortKey],
    pub inner_limit: usize,
    pub correlation_keys: &'a [(String, String)],
    pub lateral_alias: &'a str,
    pub projection: &'a [Projection],
    pub left_join: bool,
    pub tenant_id: TenantId,
    pub ctx: &'a ConvertContext,
}

/// Lower `SqlPlan::LateralTopK` to a `QueryOp::LateralTopK` physical task.
pub(super) fn convert_lateral_top_k(
    params: ConvertLateralTopKParams<'_>,
) -> crate::Result<Vec<PhysicalTask>> {
    let ConvertLateralTopKParams {
        outer,
        outer_alias,
        inner_collection,
        inner_filters,
        inner_order_by,
        inner_limit,
        correlation_keys,
        lateral_alias,
        projection,
        left_join,
        tenant_id,
        ctx,
    } = params;
    let outer_tasks = super::convert::convert_one(outer, tenant_id, ctx)?;
    let outer_task = outer_tasks
        .into_iter()
        .next()
        .ok_or_else(|| crate::Error::PlanError {
            detail: "LateralTopK: outer plan produced no physical tasks".into(),
        })?;
    let outer_vshard = outer_task.vshard_id;
    let outer_collection_name = collection_name_from_plan(outer).unwrap_or_default();
    let outer_alias_str = outer_alias.unwrap_or(&outer_collection_name).to_string();

    let inner_filter_bytes = serialize_filters(inner_filters)?;
    let order_by_spec = sort_keys_to_spec(inner_order_by);
    let join_projection = projection_to_join_projections(projection);
    let inner_coll_qualified =
        nodedb_types::QualifiedCollection::new(ctx.database_id, inner_collection);

    Ok(vec![PhysicalTask {
        tenant_id,
        vshard_id: outer_vshard,
        database_id: ctx.database_id,
        plan: PhysicalPlan::Query(QueryOp::LateralTopK {
            outer_plan: Box::new(outer_task.plan),
            outer_alias: outer_alias_str,
            inner_collection: inner_coll_qualified,
            inner_filters: inner_filter_bytes,
            inner_order_by: order_by_spec,
            inner_limit,
            correlation_keys: correlation_keys.to_vec(),
            lateral_alias: lateral_alias.to_string(),
            projection: join_projection,
            left_join,
        }),
        post_set_op: PostSetOp::None,
        txn_id: None,
    }])
}

/// Bundled arguments for [`convert_lateral_loop`].
pub(super) struct ConvertLateralLoopParams<'a> {
    pub outer: &'a SqlPlan,
    pub outer_alias: Option<&'a str>,
    pub inner: &'a SqlPlan,
    pub correlation_predicates: &'a [(String, String)],
    pub lateral_alias: &'a str,
    pub projection: &'a [Projection],
    pub outer_row_cap: usize,
    pub left_join: bool,
    pub tenant_id: TenantId,
    pub ctx: &'a ConvertContext,
}

/// Lower `SqlPlan::LateralLoop` to a `QueryOp::LateralLoop` physical task.
pub(super) fn convert_lateral_loop(
    params: ConvertLateralLoopParams<'_>,
) -> crate::Result<Vec<PhysicalTask>> {
    let ConvertLateralLoopParams {
        outer,
        outer_alias,
        inner,
        correlation_predicates,
        lateral_alias,
        projection,
        outer_row_cap,
        left_join,
        tenant_id,
        ctx,
    } = params;
    let outer_tasks = super::convert::convert_one(outer, tenant_id, ctx)?;
    let outer_task = outer_tasks
        .into_iter()
        .next()
        .ok_or_else(|| crate::Error::PlanError {
            detail: "LateralLoop: outer plan produced no physical tasks".into(),
        })?;
    let outer_vshard = outer_task.vshard_id;
    let outer_collection_name = collection_name_from_plan(outer).unwrap_or_default();
    let outer_alias_str = outer_alias.unwrap_or(&outer_collection_name).to_string();

    let inner_collection = collection_name_from_plan(inner).unwrap_or_default();
    let inner_qualified_collection =
        nodedb_types::QualifiedCollection::new(ctx.database_id, &inner_collection);
    let inner_filter_bytes = inner_filters_from_plan(inner)?;
    let join_projection = projection_to_join_projections(projection);

    Ok(vec![PhysicalTask {
        tenant_id,
        vshard_id: outer_vshard,
        database_id: ctx.database_id,
        plan: PhysicalPlan::Query(QueryOp::LateralLoop {
            outer_plan: Box::new(outer_task.plan),
            outer_alias: outer_alias_str,
            inner_collection: inner_qualified_collection,
            inner_filters: inner_filter_bytes,
            correlation_predicates: correlation_predicates.to_vec(),
            lateral_alias: lateral_alias.to_string(),
            projection: join_projection,
            left_join,
            outer_row_cap,
        }),
        post_set_op: PostSetOp::None,
        txn_id: None,
    }])
}

/// Extract the collection name from a scan-like SqlPlan.
pub(super) fn collection_name_from_plan(plan: &SqlPlan) -> Option<String> {
    match plan {
        SqlPlan::Scan { collection, .. }
        | SqlPlan::DocumentIndexLookup { collection, .. }
        | SqlPlan::PointGet { collection, .. } => Some(collection.clone()),
        _ => None,
    }
}

/// Extract base filters from a scan-like SqlPlan.
fn inner_filters_from_plan(plan: &SqlPlan) -> crate::Result<Vec<u8>> {
    match plan {
        SqlPlan::Scan { filters, .. } | SqlPlan::DocumentIndexLookup { filters, .. } => {
            serialize_filters(filters)
        }
        _ => Ok(Vec::new()),
    }
}

/// Convert `SortKey` list to its physical form.
///
/// Every key is carried, including computed ones: the scan evaluates the
/// expression per row, so dropping a non-column key here would silently
/// return the lateral side unordered.
fn sort_keys_to_spec(keys: &[SortKey]) -> Vec<SortKeySpec> {
    super::expr::convert_sort_keys(keys)
}

/// Convert `Projection` list to `JoinProjection` list.
fn projection_to_join_projections(projection: &[Projection]) -> Vec<JoinProjection> {
    projection
        .iter()
        .filter_map(|p| match p {
            Projection::Column(name) => Some(JoinProjection {
                source: name.clone(),
                output: name.clone(),
            }),
            Projection::Computed { alias, .. } => Some(JoinProjection {
                source: alias.clone(),
                output: alias.clone(),
            }),
            // A sequence stamp reads no column from the joined rows: the
            // control plane fills the cell after shaping. Nothing to fetch.
            Projection::Sequence { .. } => None,
            Projection::Star | Projection::QualifiedStar(_) => None,
        })
        .collect()
}
