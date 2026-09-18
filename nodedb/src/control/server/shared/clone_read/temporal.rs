// SPDX-License-Identifier: BUSL-1.1

//! Temporal `AS OF` extractors for the clone CoW read-path.

/// Extract the `system_as_of_ms` value from a physical plan, if present.
/// Derives the clone predation query LSN for `FOR SYSTEM_TIME AS OF <ms>`.
pub(super) fn extract_system_as_of_ms(
    plan: Option<&nodedb_physical::physical_plan::PhysicalPlan>,
) -> Option<i64> {
    use nodedb_physical::physical_plan::{PhysicalPlan, QueryOp};
    // Exhaustive: a new engine must decide here how `AS OF` is plumbed, or
    // that it's unsupported — no catch-all `_ =>`.
    match plan? {
        PhysicalPlan::Document(op) => extract_doc_as_of(op),
        PhysicalPlan::Columnar(op) => extract_columnar_as_of(op),
        PhysicalPlan::Timeseries(op) => extract_timeseries_as_of(op),
        // Structural wrappers: qualifier sits on the child, not the wrapper.
        PhysicalPlan::Query(QueryOp::Exchange(op)) => extract_system_as_of_ms(Some(&*op.child)),
        PhysicalPlan::Query(QueryOp::PostProcess { input, .. }) => {
            extract_system_as_of_ms(Some(&**input))
        }
        PhysicalPlan::Query(QueryOp::SetOp { inputs, .. }) => inputs
            .iter()
            .find_map(|input| extract_system_as_of_ms(Some(input))),
        // Index-only/overlay engines carry no qualifier; compose with a data-bearing collection.
        PhysicalPlan::Vector(_)
        | PhysicalPlan::Graph(_)
        | PhysicalPlan::Kv(_)
        | PhysicalPlan::Text(_)
        | PhysicalPlan::Spatial(_)
        | PhysicalPlan::Crdt(_)
        | PhysicalPlan::Query(_)
        | PhysicalPlan::Meta(_)
        | PhysicalPlan::Array(_)
        | PhysicalPlan::ClusterArray(_)
        | PhysicalPlan::ClusterEvent(_) => None,
    }
}

fn extract_doc_as_of(op: &nodedb_physical::physical_plan::DocumentOp) -> Option<i64> {
    use nodedb_physical::physical_plan::DocumentOp;
    match op {
        DocumentOp::Scan { system_time, .. } => system_time.as_of_ms(),
        _ => None,
    }
}

fn extract_columnar_as_of(op: &nodedb_physical::physical_plan::ColumnarOp) -> Option<i64> {
    use nodedb_physical::physical_plan::ColumnarOp;
    match op {
        ColumnarOp::Scan { system_time, .. } => system_time.as_of_ms(),
        _ => None,
    }
}

fn extract_timeseries_as_of(op: &nodedb_physical::physical_plan::TimeseriesOp) -> Option<i64> {
    use nodedb_physical::physical_plan::TimeseriesOp;
    match op {
        TimeseriesOp::Scan { system_time, .. } => system_time.as_of_ms(),
        _ => None,
    }
}
