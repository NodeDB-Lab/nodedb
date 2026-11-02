// SPDX-License-Identifier: BUSL-1.1

//! Query engine plan builders (recursive CTE).

use nodedb_types::QualifiedCollection;
use nodedb_types::protocol::TextFields;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::server::native::dispatch::DispatchCtx;
use nodedb_physical::physical_plan::QueryOp;

pub(crate) async fn build_recursive_scan(
    ctx: &DispatchCtx<'_>,
    fields: &TextFields,
    collection: &str,
) -> crate::Result<PhysicalPlan> {
    let base_filters = fields.filters.clone().unwrap_or_default();
    let limit = super::requested_limit(fields)?;

    Ok(PhysicalPlan::Query(QueryOp::RecursiveScan {
        collection: QualifiedCollection::new(ctx.database_id(), collection),
        base_filters,
        recursive_filters: Vec::new(),
        join_link: None,
        max_iterations: 100,
        distinct: true,
        limit,
        system_time: nodedb_types::SystemTimeScope::Current,
    }))
}
