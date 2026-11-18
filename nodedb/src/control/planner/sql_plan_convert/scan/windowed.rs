// SPDX-License-Identifier: BUSL-1.1

//! A windowed scan on an engine whose scan op has no window slot.

use nodedb_sql::types::EngineType;

use crate::bridge::envelope::PhysicalPlan;
use nodedb_physical::physical_plan::QueryOp;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use super::super::aggregate::{extract_computed_columns, serialize_window_functions};
use super::super::expr::convert_sort_keys;
use super::super::scan_params::ScanParams;
use super::core::convert_scan;

/// Whether the engine's physical scan op carries window specs. Only the
/// document scan has the slot, so every other engine runs its windows in
/// the coordinator tail.
pub(super) fn scan_op_evaluates_windows(engine: &EngineType) -> bool {
    matches!(
        engine,
        EngineType::DocumentSchemaless | EngineType::DocumentStrict
    )
}

/// Lower a windowed scan on an engine whose scan op has no window slot.
///
/// The body scan keeps the WHERE filters and reads every column of every
/// row that passes them. The `PostProcess` tail runs the windows over the
/// full row set, then ORDER BY, DISTINCT, OFFSET and LIMIT, so a LIMIT
/// never cuts rows before the windows see them. The tail evaluates windows
/// with the same evaluator as the document scan.
pub(super) fn convert_windowed_scan(p: ScanParams<'_>) -> crate::Result<Vec<PhysicalTask>> {
    let ScanParams {
        collection,
        engine,
        filters,
        projection,
        sort_keys,
        limit,
        offset,
        distinct,
        window_functions,
        tenant_id,
        temporal,
        database_id,
    } = p;
    let mut body_tasks = convert_scan(ScanParams {
        collection,
        engine,
        filters,
        projection: &[],
        sort_keys: &[],
        limit: &None,
        offset: &0,
        distinct: &false,
        window_functions: &[],
        tenant_id,
        temporal,
        database_id,
    })?;
    let body = match (body_tasks.len(), body_tasks.pop()) {
        (1, Some(task)) => task.plan,
        (n, _) => {
            return Err(crate::Error::PlanError {
                detail: format!(
                    "windowed scan on '{collection}' lowers to {n} physical tasks; \
                     the window tail needs a single relation"
                ),
            });
        }
    };
    Ok(vec![PhysicalTask {
        tenant_id,
        // Coordinator-local, like a derived-table tail: the gathered rows
        // run through the window pass once.
        vshard_id: nodedb_types::CollectionKey::from_bare(database_id, "").vshard(),
        database_id,
        plan: PhysicalPlan::Query(QueryOp::PostProcess {
            input: Box::new(super::super::body::gather_if_sharded(body)),
            filters: Vec::new(),
            projection: super::super::set_ops::lower_subquery_projection(
                projection,
                window_functions,
            )?,
            computed_columns: extract_computed_columns(projection, window_functions, false)?,
            window_functions: serialize_window_functions(window_functions, false)?,
            sort_keys: convert_sort_keys(sort_keys),
            limit: *limit,
            offset: *offset,
            distinct: *distinct,
        }),
        post_set_op: PostSetOp::None,
        txn_id: None,
    }])
}
