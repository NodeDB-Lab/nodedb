// SPDX-License-Identifier: BUSL-1.1

//! Bridge lowering for `SqlPlan::LateralTopK` and `SqlPlan::LateralLoop`.
//!
//! Both variants embed the outer sub-plan as an `outer_plan: Box<PhysicalPlan>`
//! inside the `QueryOp` so the Data Plane executor can materialise outer rows
//! in-process before iterating over them.

use nodedb_sql::types::{DocumentIndexLookupPlan, Filter, Projection, SortKey, SqlPlan};

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
    pub temporal: nodedb_sql::temporal::TemporalScope,
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
        temporal,
        tenant_id,
        ctx,
    } = params;
    let outer_task = single_outer_task(outer, tenant_id, ctx)?;
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
            inner_system_time: temporal.system_time,
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
    let outer_task = single_outer_task(outer, tenant_id, ctx)?;
    let outer_vshard = outer_task.vshard_id;
    let outer_collection_name = collection_name_from_plan(outer).unwrap_or_default();
    let outer_alias_str = outer_alias.unwrap_or(&outer_collection_name).to_string();

    let inner_collection =
        collection_name_from_plan(inner).ok_or_else(|| crate::Error::PlanError {
            detail: format!(
                "LATERAL: the inner side lowers to {}, and the join scans its inner side by \
                 collection name; name a stored collection inside LATERAL",
                inner.variant_name()
            ),
        })?;
    let inner_qualified_collection =
        nodedb_types::QualifiedCollection::new(ctx.database_id, &inner_collection);
    let inner_filter_bytes = inner_filters_from_plan(inner)?;
    let inner_system_time = inner_system_time_from_plan(inner)?;
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
            inner_system_time,
        }),
        post_set_op: PostSetOp::None,
        txn_id: None,
    }])
}

/// The one task a LATERAL join's outer side lowers to.
///
/// The Data Plane runs the outer plan in process on one core. A body of
/// several tasks, such as a set operation, has no one plan to run. Taking its
/// first task drops the rows of every other branch.
fn single_outer_task(
    outer: &SqlPlan,
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<PhysicalTask> {
    let mut tasks = super::convert::convert_one(outer, tenant_id, ctx)?;
    match (tasks.len(), tasks.pop()) {
        (1, Some(task)) => Ok(task),
        (count, _) => Err(crate::Error::PlanError {
            detail: format!(
                "LATERAL: the outer side lowers to {count} physical tasks, and the join runs \
                 exactly one; move the set operation out of the outer side"
            ),
        }),
    }
}

/// Extract the collection name from a scan-like SqlPlan.
///
/// Used for the inner side's name, so a plan with no such name is an error
/// there and an empty fallback name is never scanned.
pub(super) fn collection_name_from_plan(plan: &SqlPlan) -> Option<String> {
    match plan {
        SqlPlan::Scan { collection, .. }
        | SqlPlan::DocumentIndexLookup(DocumentIndexLookupPlan { collection, .. })
        | SqlPlan::PointGet { collection, .. } => Some(collection.clone()),
        _ => None,
    }
}

/// Extract base filters from a scan-like SqlPlan.
fn inner_filters_from_plan(plan: &SqlPlan) -> crate::Result<Vec<u8>> {
    match plan {
        SqlPlan::Scan { filters, .. }
        | SqlPlan::DocumentIndexLookup(DocumentIndexLookupPlan { filters, .. }) => {
            serialize_filters(filters)
        }
        _ => Ok(Vec::new()),
    }
}

/// The system time a loop's inner scan reads at. A plan that carries no
/// temporal scope cannot read the statement's cut, so it is refused.
fn inner_system_time_from_plan(plan: &SqlPlan) -> crate::Result<nodedb_types::SystemTimeScope> {
    match plan {
        SqlPlan::Scan { temporal, .. }
        | SqlPlan::DocumentIndexLookup(DocumentIndexLookupPlan { temporal, .. }) => {
            Ok(temporal.system_time)
        }
        other => Err(crate::Error::PlanError {
            detail: format!(
                "LATERAL: the inner side lowers to {}, which carries no system time; name a \
                 stored collection inside LATERAL",
                other.variant_name()
            ),
        }),
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
    let pass_through = |name: &str| JoinProjection {
        source: name.to_string(),
        output: name.to_string(),
    };
    let mut specs = Vec::with_capacity(projection.len());
    for p in projection {
        match p {
            Projection::Column(name) => specs.push(pass_through(name)),
            Projection::Computed { alias, .. } => specs.push(pass_through(alias)),
            // The Control Plane evaluates the entry over the lateral row, so
            // every base column it reads passes through under its own name.
            Projection::CpComputed { expr, .. } => {
                for column in nodedb_sql::types::plan::referenced_columns(expr) {
                    if !specs
                        .iter()
                        .any(|spec: &JoinProjection| spec.source == column)
                    {
                        specs.push(pass_through(&column));
                    }
                }
            }
            Projection::Star | Projection::QualifiedStar(_) => {}
        }
    }
    specs
}

#[cfg(test)]
mod tests {
    use nodedb_sql::temporal::TemporalScope;
    use nodedb_sql::types::{EngineType, SqlValue};
    use nodedb_types::SystemTimeScope;

    use super::*;

    /// A loop's inner scan reads the statement's system time.
    #[test]
    fn a_loop_inner_scan_gives_its_system_time() {
        let scope = SystemTimeScope::AsOf(5);
        let inner = SqlPlan::Scan {
            collection: "inner".to_string(),
            alias: None,
            engine: EngineType::DocumentSchemaless,
            filters: Vec::new(),
            projection: Vec::new(),
            sort_keys: Vec::new(),
            limit: None,
            offset: 0,
            distinct: false,
            window_functions: Vec::new(),
            temporal: TemporalScope {
                system_time: scope,
                ..TemporalScope::default()
            },
        };
        assert_eq!(inner_system_time_from_plan(&inner).ok(), Some(scope));
    }

    /// A plan with no temporal scope cannot read the cut, so it is refused
    /// and never reads the current state.
    #[test]
    fn a_plan_with_no_system_time_is_refused() {
        let inner = SqlPlan::ConstantResult {
            columns: vec!["x".to_string()],
            values: vec![SqlValue::Int(1)],
            volatile: false,
        };
        assert!(matches!(
            inner_system_time_from_plan(&inner),
            Err(crate::Error::PlanError { .. })
        ));
    }
}
