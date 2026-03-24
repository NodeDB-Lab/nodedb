//! Plan classification and response formatting.

use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response, Tag};

use crate::bridge::envelope::PhysicalPlan;

use super::super::types::text_field;

#[derive(Debug, Clone, Copy)]
pub(super) enum PlanKind {
    SingleDocument,
    MultiRow,
    Execution,
}

/// Extract the collection name from a physical plan (if applicable).
pub(super) fn extract_collection(plan: &PhysicalPlan) -> Option<&str> {
    match plan {
        PhysicalPlan::PointGet { collection, .. }
        | PhysicalPlan::VectorSearch { collection, .. }
        | PhysicalPlan::RangeScan { collection, .. }
        | PhysicalPlan::CrdtRead { collection, .. }
        | PhysicalPlan::CrdtApply { collection, .. }
        | PhysicalPlan::VectorInsert { collection, .. }
        | PhysicalPlan::VectorBatchInsert { collection, .. }
        | PhysicalPlan::VectorMultiSearch { collection, .. }
        | PhysicalPlan::VectorDelete { collection, .. }
        | PhysicalPlan::DocumentBatchInsert { collection, .. }
        | PhysicalPlan::PointPut { collection, .. }
        | PhysicalPlan::PointDelete { collection, .. }
        | PhysicalPlan::PointUpdate { collection, .. }
        | PhysicalPlan::DocumentScan { collection, .. }
        | PhysicalPlan::Aggregate { collection, .. }
        | PhysicalPlan::HashJoin {
            left_collection: collection,
            ..
        }
        | PhysicalPlan::NestedLoopJoin {
            left_collection: collection,
            ..
        }
        | PhysicalPlan::GraphRagFusion { collection, .. }
        | PhysicalPlan::SetCollectionPolicy { collection, .. }
        | PhysicalPlan::SetVectorParams { collection, .. }
        | PhysicalPlan::TextSearch { collection, .. }
        | PhysicalPlan::HybridSearch { collection, .. }
        | PhysicalPlan::PartialAggregate { collection, .. }
        | PhysicalPlan::BroadcastJoin {
            large_collection: collection,
            ..
        }
        | PhysicalPlan::ShuffleJoin {
            left_collection: collection,
            ..
        }
        | PhysicalPlan::BulkUpdate { collection, .. }
        | PhysicalPlan::BulkDelete { collection, .. }
        | PhysicalPlan::Upsert { collection, .. }
        | PhysicalPlan::InsertSelect {
            target_collection: collection,
            ..
        }
        | PhysicalPlan::Truncate { collection }
        | PhysicalPlan::EstimateCount { collection, .. } => Some(collection.as_str()),
        PhysicalPlan::EdgePut { .. }
        | PhysicalPlan::EdgeDelete { .. }
        | PhysicalPlan::GraphHop { .. }
        | PhysicalPlan::GraphNeighbors { .. }
        | PhysicalPlan::GraphPath { .. }
        | PhysicalPlan::GraphSubgraph { .. }
        | PhysicalPlan::WalAppend { .. }
        | PhysicalPlan::Cancel { .. }
        | PhysicalPlan::TransactionBatch { .. }
        | PhysicalPlan::CreateSnapshot
        | PhysicalPlan::Compact
        | PhysicalPlan::Checkpoint => None,
    }
}

pub(super) fn describe_plan(plan: &PhysicalPlan) -> PlanKind {
    match plan {
        PhysicalPlan::PointGet { .. } | PhysicalPlan::CrdtRead { .. } => PlanKind::SingleDocument,
        PhysicalPlan::VectorSearch { .. }
        | PhysicalPlan::RangeScan { .. }
        | PhysicalPlan::GraphHop { .. }
        | PhysicalPlan::GraphNeighbors { .. }
        | PhysicalPlan::GraphPath { .. }
        | PhysicalPlan::GraphSubgraph { .. }
        | PhysicalPlan::GraphRagFusion { .. }
        | PhysicalPlan::DocumentScan { .. }
        | PhysicalPlan::Aggregate { .. }
        | PhysicalPlan::HashJoin { .. } => PlanKind::MultiRow,
        _ => PlanKind::Execution,
    }
}

pub(super) fn payload_to_response(payload: &[u8], kind: PlanKind) -> Response {
    match kind {
        PlanKind::Execution => Response::Execution(Tag::new("OK")),
        PlanKind::SingleDocument | PlanKind::MultiRow => {
            let col_name = if matches!(kind, PlanKind::SingleDocument) {
                "document"
            } else {
                "result"
            };
            let schema = Arc::new(vec![text_field(col_name)]);
            if payload.is_empty() {
                Response::Query(QueryResponse::new(schema, stream::empty()))
            } else {
                let text = crate::data::executor::response_codec::decode_payload_to_json(payload);
                let mut encoder = DataRowEncoder::new(schema.clone());
                if let Err(e) = encoder.encode_field(&text) {
                    tracing::error!(error = %e, "failed to encode field");
                    return Response::Execution(Tag::new("ERROR"));
                }
                let row = encoder.take_row();
                Response::Query(QueryResponse::new(schema, stream::iter(vec![Ok(row)])))
            }
        }
    }
}
