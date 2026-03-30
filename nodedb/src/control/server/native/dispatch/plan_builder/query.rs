//! Query engine plan builders (recursive CTE).

use nodedb_types::protocol::TextFields;

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::QueryOp;

pub(crate) fn build_recursive_scan(
    fields: &TextFields,
    collection: &str,
) -> crate::Result<PhysicalPlan> {
    let base_filters = fields.filters.clone().unwrap_or_default();
    let limit = fields.limit.unwrap_or(10_000) as usize;

    Ok(PhysicalPlan::Query(QueryOp::RecursiveScan {
        collection: collection.to_string(),
        base_filters,
        recursive_filters: Vec::new(),
        max_iterations: 100,
        distinct: true,
        limit,
    }))
}
