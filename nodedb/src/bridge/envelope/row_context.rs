// SPDX-License-Identifier: BUSL-1.1

//! `describe_row_context`: the row shape a plan's payload carries.
//!
//! The Data Plane wraps a stored-row read in a `{id, data}` scan envelope and
//! answers every other row-producing op with flat rows. This decides which
//! from the plan, so no decoder reads it off a row's column names. Both
//! planes read it: the Control Plane to decode a response, the Data Plane to
//! read the payload of a plan it runs inline. Each engine whose ops emit
//! envelopes is matched exhaustively, so a new op is a compile error until
//! it decides.

use nodedb_physical::physical_plan::{DocumentOp, PhysicalPlan, QueryOp, TextOp};

use crate::data::executor::response_codec::RowContext;

/// The row shape of the payload `plan` produces.
pub fn describe_row_context(plan: &PhysicalPlan) -> RowContext {
    match plan {
        PhysicalPlan::Document(op) => document_row_context(op),
        PhysicalPlan::Text(op) => text_row_context(op),
        PhysicalPlan::Query(op) => query_row_context(op),

        // KV rows are `{key, value}` maps, vector hits are flattened by the
        // response translator, and columnar, timeseries, spatial, graph,
        // CRDT and array ops encode flat value rows. No op of these engines
        // emits a scan envelope.
        PhysicalPlan::Kv(_)
        | PhysicalPlan::Vector(_)
        | PhysicalPlan::Columnar(_)
        | PhysicalPlan::Timeseries(_)
        | PhysicalPlan::Spatial(_)
        | PhysicalPlan::Graph(_)
        | PhysicalPlan::Crdt(_)
        | PhysicalPlan::Array(_)
        | PhysicalPlan::ClusterArray(_)
        // Catalog, session, cluster and event ops answer no rows.
        | PhysicalPlan::Meta(_)
        | PhysicalPlan::ClusterEvent(_) => RowContext::Flat,
    }
}

fn document_row_context(op: &DocumentOp) -> RowContext {
    match op {
        // Scan, range scan and index fetch answer through
        // `encode_raw_document_rows`.
        DocumentOp::Scan { .. } | DocumentOp::RangeScan { .. } | DocumentOp::IndexedFetch { .. } => {
            RowContext::ScanEnvelope
        }

        // A point get answers the row body itself.
        DocumentOp::PointGet { .. }
        // Writes answer a count or a `RowsPayload`.
        | DocumentOp::PointPut { .. }
        | DocumentOp::PointInsert { .. }
        | DocumentOp::BatchInsert { .. }
        | DocumentOp::InsertSelect { .. }
        | DocumentOp::PointUpdate { .. }
        | DocumentOp::BulkUpdate { .. }
        | DocumentOp::UpdateFromJoin { .. }
        | DocumentOp::PointDelete { .. }
        | DocumentOp::BulkDelete { .. }
        | DocumentOp::Merge { .. }
        | DocumentOp::Truncate { .. }
        | DocumentOp::Upsert { .. }
        | DocumentOp::BatchUpsert { .. }
        // Internal stages answer their own caller.
        | DocumentOp::Register { .. }
        | DocumentOp::IndexLookup { .. }
        | DocumentOp::DropIndex { .. }
        | DocumentOp::BackfillIndex { .. }
        | DocumentOp::EstimateCount { .. }
        | DocumentOp::MaterializeScan { .. }
        | DocumentOp::ResolveWrite(_)
        | DocumentOp::ApplyBalanceDelta { .. }
        | DocumentOp::ResolvedWrite { .. } => RowContext::Flat,
    }
}

fn text_row_context(op: &TextOp) -> RowContext {
    match op {
        // Each hit is a `DocumentRow`: `{id, data}`.
        TextOp::Search { .. } | TextOp::PhraseSearch { .. } | TextOp::BM25ScoreScan { .. } => {
            RowContext::ScanEnvelope
        }
        // Hybrid hits are flat `{doc_id, score, ...}` maps.
        TextOp::HybridSearch { .. }
        | TextOp::HybridSearchTriple { .. }
        | TextOp::FtsIndexDoc { .. }
        | TextOp::FtsDeleteDoc { .. }
        | TextOp::SetTextConfig { .. }
        | TextOp::RebuildIndex { .. } => RowContext::Flat,
    }
}

fn query_row_context(op: &QueryOp) -> RowContext {
    match op {
        // A gather answers its child's rows unchanged.
        QueryOp::Exchange(exchange) => describe_row_context(&exchange.child),

        // A post-processing tail, a provider scan, a set op, a join or an
        // aggregate answers relational rows: the gather flattens envelopes
        // before any of them runs.
        QueryOp::PostProcess { .. }
        | QueryOp::ProviderScan { .. }
        | QueryOp::SetOp { .. }
        | QueryOp::Aggregate { .. }
        | QueryOp::FacetCounts { .. }
        | QueryOp::HashJoin { .. }
        | QueryOp::NestedLoopJoin { .. }
        | QueryOp::SortMergeJoin { .. }
        | QueryOp::RecursiveScan { .. }
        | QueryOp::RecursiveValue { .. }
        | QueryOp::LateralTopK { .. }
        | QueryOp::LateralLoop { .. }
        | QueryOp::PartialAggregate { .. }
        | QueryOp::PartialAggregateState { .. }
        | QueryOp::ShuffleJoinConsume { .. }
        | QueryOp::ShuffleAggregateConsume { .. } => RowContext::Flat,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_physical::physical_plan::{ExchangeMode, ExchangeOp};
    use nodedb_types::{DatabaseId, QualifiedCollection};

    fn scan() -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::Scan {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "docs"),
            filters: Vec::new(),
            limit: 10,
            offset: 0,
            sort_keys: Vec::new(),
            distinct: false,
            projection: Vec::new(),
            computed_columns: Vec::new(),
            window_functions: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
            prefilter: None,
        })
    }

    #[test]
    fn a_scan_and_its_gather_carry_envelopes() {
        assert_eq!(describe_row_context(&scan()), RowContext::ScanEnvelope);
        let gather = PhysicalPlan::Query(QueryOp::Exchange(ExchangeOp {
            child: Box::new(scan()),
            mode: ExchangeMode::Gather {
                as_aggregate: false,
            },
        }));
        assert_eq!(describe_row_context(&gather), RowContext::ScanEnvelope);
    }

    #[test]
    fn a_set_op_carries_flat_rows() {
        let plan = PhysicalPlan::Query(QueryOp::SetOp {
            inputs: vec![scan(), scan()],
            op: nodedb_physical::physical_plan::SetOpKind::UnionDistinct,
            columns: vec![Vec::new(), Vec::new()],
        });
        assert_eq!(describe_row_context(&plan), RowContext::Flat);
    }
}
