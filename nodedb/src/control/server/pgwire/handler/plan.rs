//! Plan classification and response formatting.

use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response, Tag};
use sonic_rs;

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::{
    ColumnarOp, CrdtOp, DocumentOp, GraphOp, KvOp, MetaOp, QueryOp, SpatialOp, TextOp,
    TimeseriesOp, VectorOp,
};
use crate::data::executor::response_codec::{ArraySliceResponse, decode_payload_to_json};
use zerompk;

use super::super::types::text_field;

#[derive(Debug, Clone, Copy)]
pub(super) enum PlanKind {
    SingleDocument,
    MultiRow,
    /// Array slice result — decoded via `ArraySliceResponse` to surface the
    /// `truncated_before_horizon` flag as a pgwire NOTICE when set.
    ArraySlice,
    Execution,
    /// DML operation that returns affected row count.
    /// The tag name is used in the pgwire `CommandComplete` message (e.g., "UPDATE", "DELETE").
    DmlResult(&'static str),
}

/// Extract the collection name from a physical plan (if applicable).
pub(super) fn extract_collection(plan: &PhysicalPlan) -> Option<&str> {
    match plan {
        PhysicalPlan::Document(DocumentOp::PointGet { collection, .. })
        | PhysicalPlan::Vector(VectorOp::Search { collection, .. })
        | PhysicalPlan::Document(DocumentOp::RangeScan { collection, .. })
        | PhysicalPlan::Crdt(CrdtOp::Read { collection, .. })
        | PhysicalPlan::Crdt(CrdtOp::Apply { collection, .. })
        | PhysicalPlan::Vector(VectorOp::Insert { collection, .. })
        | PhysicalPlan::Vector(VectorOp::BatchInsert { collection, .. })
        | PhysicalPlan::Vector(VectorOp::MultiSearch { collection, .. })
        | PhysicalPlan::Vector(VectorOp::Delete { collection, .. })
        | PhysicalPlan::Document(DocumentOp::BatchInsert { collection, .. })
        | PhysicalPlan::Document(DocumentOp::PointPut { collection, .. })
        | PhysicalPlan::Document(DocumentOp::PointDelete { collection, .. })
        | PhysicalPlan::Document(DocumentOp::PointUpdate { collection, .. })
        | PhysicalPlan::Document(DocumentOp::Scan { collection, .. })
        | PhysicalPlan::Query(QueryOp::Aggregate { collection, .. })
        | PhysicalPlan::Query(QueryOp::HashJoin {
            left_collection: collection,
            ..
        })
        | PhysicalPlan::Query(QueryOp::NestedLoopJoin {
            left_collection: collection,
            ..
        })
        | PhysicalPlan::Graph(GraphOp::RagFusion { collection, .. })
        | PhysicalPlan::Crdt(CrdtOp::SetPolicy { collection, .. })
        | PhysicalPlan::Vector(VectorOp::SetParams { collection, .. })
        | PhysicalPlan::Text(TextOp::Search { collection, .. })
        | PhysicalPlan::Text(TextOp::HybridSearch { collection, .. })
        | PhysicalPlan::Query(QueryOp::PartialAggregate { collection, .. })
        | PhysicalPlan::Query(QueryOp::FacetCounts { collection, .. })
        | PhysicalPlan::Query(QueryOp::BroadcastJoin {
            large_collection: collection,
            ..
        })
        | PhysicalPlan::Query(QueryOp::ShuffleJoin {
            left_collection: collection,
            ..
        })
        | PhysicalPlan::Document(DocumentOp::BulkUpdate { collection, .. })
        | PhysicalPlan::Document(DocumentOp::BulkDelete { collection, .. })
        | PhysicalPlan::Document(DocumentOp::Upsert { collection, .. })
        | PhysicalPlan::Document(DocumentOp::InsertSelect {
            target_collection: collection,
            ..
        })
        | PhysicalPlan::Document(DocumentOp::Truncate { collection, .. })
        | PhysicalPlan::Document(DocumentOp::EstimateCount { collection, .. })
        | PhysicalPlan::Columnar(ColumnarOp::Scan { collection, .. })
        | PhysicalPlan::Columnar(ColumnarOp::Insert { collection, .. })
        | PhysicalPlan::Timeseries(TimeseriesOp::Scan { collection, .. })
        | PhysicalPlan::Timeseries(TimeseriesOp::Ingest { collection, .. })
        | PhysicalPlan::Spatial(SpatialOp::Scan { collection, .. })
        | PhysicalPlan::Document(DocumentOp::Register { collection, .. })
        | PhysicalPlan::Document(DocumentOp::IndexLookup { collection, .. })
        | PhysicalPlan::Document(DocumentOp::IndexedFetch { collection, .. })
        | PhysicalPlan::Document(DocumentOp::DropIndex { collection, .. }) => {
            Some(collection.as_str())
        }
        PhysicalPlan::Graph(GraphOp::EdgePut { .. })
        | PhysicalPlan::Graph(GraphOp::EdgeDelete { .. })
        | PhysicalPlan::Graph(GraphOp::Hop { .. })
        | PhysicalPlan::Graph(GraphOp::Neighbors { .. })
        | PhysicalPlan::Graph(GraphOp::Path { .. })
        | PhysicalPlan::Graph(GraphOp::Subgraph { .. })
        | PhysicalPlan::Meta(MetaOp::WalAppend { .. })
        | PhysicalPlan::Meta(MetaOp::Cancel { .. })
        | PhysicalPlan::Meta(MetaOp::TransactionBatch { .. })
        | PhysicalPlan::Meta(MetaOp::CreateSnapshot)
        | PhysicalPlan::Meta(MetaOp::Compact)
        | PhysicalPlan::Meta(MetaOp::Checkpoint)
        | PhysicalPlan::Graph(GraphOp::Algo { .. })
        | PhysicalPlan::Graph(GraphOp::Match { .. }) => None,
        _ => None,
    }
}

pub(super) fn describe_plan(plan: &PhysicalPlan) -> PlanKind {
    match plan {
        PhysicalPlan::Document(DocumentOp::PointGet { .. })
        | PhysicalPlan::Crdt(CrdtOp::Read { .. }) => PlanKind::SingleDocument,

        PhysicalPlan::Vector(VectorOp::Search { .. })
        | PhysicalPlan::Document(DocumentOp::RangeScan { .. })
        | PhysicalPlan::Graph(GraphOp::Hop { .. })
        | PhysicalPlan::Graph(GraphOp::Neighbors { .. })
        | PhysicalPlan::Graph(GraphOp::Path { .. })
        | PhysicalPlan::Graph(GraphOp::Subgraph { .. })
        | PhysicalPlan::Graph(GraphOp::RagFusion { .. })
        | PhysicalPlan::Document(DocumentOp::Scan { .. })
        | PhysicalPlan::Document(DocumentOp::IndexedFetch { .. })
        | PhysicalPlan::Columnar(ColumnarOp::Scan { .. })
        | PhysicalPlan::Timeseries(TimeseriesOp::Scan { .. })
        | PhysicalPlan::Spatial(SpatialOp::Scan { .. })
        | PhysicalPlan::Kv(KvOp::Scan { .. })
        | PhysicalPlan::Kv(KvOp::BatchGet { .. })
        | PhysicalPlan::Query(QueryOp::Aggregate { .. })
        | PhysicalPlan::Query(QueryOp::FacetCounts { .. })
        | PhysicalPlan::Query(QueryOp::HashJoin { .. })
        | PhysicalPlan::Query(QueryOp::InlineHashJoin { .. })
        | PhysicalPlan::Query(QueryOp::RecursiveScan { .. })
        | PhysicalPlan::Graph(GraphOp::Algo { .. })
        | PhysicalPlan::Graph(GraphOp::Match { .. }) => PlanKind::MultiRow,

        PhysicalPlan::Kv(KvOp::Get { .. }) | PhysicalPlan::Kv(KvOp::FieldGet { .. }) => {
            PlanKind::SingleDocument
        }

        // Constant-result expressions (SELECT 1, SELECT 'hello', etc.)
        // are compiled to RawResponse with a msgpack-encoded row. Treat
        // as a multi-row scan so the payload is decoded and streamed back.
        PhysicalPlan::Meta(MetaOp::RawResponse { .. }) => PlanKind::MultiRow,

        // DML operations that return affected row count.
        PhysicalPlan::Document(DocumentOp::PointPut { .. })
        | PhysicalPlan::Document(DocumentOp::BatchInsert { .. })
        | PhysicalPlan::Columnar(ColumnarOp::Insert { .. }) => DmlResult("INSERT"),

        PhysicalPlan::Document(DocumentOp::PointUpdate {
            returning: true, ..
        })
        | PhysicalPlan::Document(DocumentOp::BulkUpdate {
            returning: true, ..
        }) => PlanKind::MultiRow,
        PhysicalPlan::Document(DocumentOp::PointUpdate { .. })
        | PhysicalPlan::Document(DocumentOp::BulkUpdate { .. }) => DmlResult("UPDATE"),

        PhysicalPlan::Document(DocumentOp::PointDelete { .. })
        | PhysicalPlan::Document(DocumentOp::BulkDelete { .. }) => DmlResult("DELETE"),

        PhysicalPlan::Document(DocumentOp::Truncate { .. }) => DmlResult("TRUNCATE"),

        PhysicalPlan::Document(DocumentOp::InsertSelect { .. }) => DmlResult("INSERT"),

        PhysicalPlan::Document(DocumentOp::Upsert { .. }) => DmlResult("UPSERT"),

        // Array engine read & maintenance ops produce a JSON-array
        // payload of rows; route to the multi-row decoder so each row
        // streams as its own pgwire `result` field. Aggregate's payload
        // is plain msgpack (decode_payload_to_json transcodes); Slice /
        // Project payloads use the tagged Value codec which transcodes
        // to a JSON array of arrays — clients receive JSON text per row.
        PhysicalPlan::Array(crate::bridge::physical_plan::ArrayOp::Slice { .. }) => {
            PlanKind::ArraySlice
        }
        PhysicalPlan::Array(crate::bridge::physical_plan::ArrayOp::Project { .. })
        | PhysicalPlan::Array(crate::bridge::physical_plan::ArrayOp::Aggregate { .. })
        | PhysicalPlan::Array(crate::bridge::physical_plan::ArrayOp::Elementwise { .. }) => {
            PlanKind::MultiRow
        }
        // Flush / Compact return `{flushed: 1}` / `{compacted: N}` —
        // route as SingleDocument so the row's `document` column
        // carries the status JSON.
        PhysicalPlan::Array(crate::bridge::physical_plan::ArrayOp::Flush { .. })
        | PhysicalPlan::Array(crate::bridge::physical_plan::ArrayOp::Compact { .. }) => {
            PlanKind::SingleDocument
        }

        _ => PlanKind::Execution,
    }
}

// Bring the variant into scope for brevity in match arms above.
use PlanKind::DmlResult;

/// Extract affected row count from a JSON or MessagePack payload.
///
/// Looks for `"affected"`, `"truncated"`, `"inserted"`, or `"accepted"` fields.
fn extract_affected_count(payload: &[u8]) -> Option<u64> {
    if payload.is_empty() {
        return None;
    }
    let v: serde_json::Value = nodedb_types::json_from_msgpack(payload)
        .ok()
        .or_else(|| sonic_rs::from_slice(payload).ok())?;
    v.get("affected")
        .or_else(|| v.get("truncated"))
        .or_else(|| v.get("inserted"))
        .or_else(|| v.get("accepted"))
        .and_then(|n| n.as_u64())
}

/// Outcome of shaping a Data Plane payload into a pgwire `Response`.
///
/// `notice` is set when the response shaper detected a condition the client
/// should know about (e.g. `truncated_before_horizon` on an array slice).
/// Callers forward it to the per-connection notice queue.
pub(super) struct ShapedResponse {
    pub response: Response,
    pub notice: Option<String>,
}

impl From<Response> for ShapedResponse {
    fn from(response: Response) -> Self {
        Self {
            response,
            notice: None,
        }
    }
}

/// NOTICE message emitted when a slice request's `system_as_of` cutoff fell
/// below the oldest tile version on at least one shard.
const TRUNCATED_BEFORE_HORIZON_NOTICE: &str = "AS OF SYSTEM TIME cutoff is older than the oldest retained tile version; \
     results may be incomplete";

pub(super) fn payload_to_response(payload: &[u8], kind: PlanKind) -> ShapedResponse {
    match kind {
        PlanKind::Execution => Response::Execution(Tag::new("OK")).into(),
        PlanKind::DmlResult(tag) => {
            let count = if payload.is_empty() {
                // Point operations with empty payload succeeded on exactly 1 row.
                1
            } else {
                extract_affected_count(payload).unwrap_or(1) as usize
            };
            Response::Execution(Tag::new(tag).with_rows(count)).into()
        }
        PlanKind::ArraySlice => {
            // Decode `ArraySliceResponse` envelope: extract rows and flag.
            // Fall back to treating the payload as a plain array when the
            // decode fails (e.g., empty payload or legacy shape).
            let schema = Arc::new(vec![text_field("result")]);
            if payload.is_empty() {
                return Response::Query(QueryResponse::new(schema, stream::empty())).into();
            }
            let (rows_json, truncated) =
                if let Ok(resp) = zerompk::from_msgpack::<ArraySliceResponse>(payload) {
                    let text = decode_payload_to_json(&resp.rows_msgpack);
                    (text, resp.truncated_before_horizon)
                } else {
                    // Fallback for any legacy or plain-array payloads.
                    (decode_payload_to_json(payload), false)
                };
            let notice = if truncated {
                Some(TRUNCATED_BEFORE_HORIZON_NOTICE.to_string())
            } else {
                None
            };
            let response = if let Ok(serde_json::Value::Array(items)) =
                sonic_rs::from_str::<serde_json::Value>(&rows_json)
            {
                let row_schema = schema.clone();
                let rows: Vec<_> = items
                    .iter()
                    .map(|item| {
                        let mut encoder = DataRowEncoder::new(row_schema.clone());
                        let _ = encoder.encode_field(&item.to_string());
                        Ok(encoder.take_row())
                    })
                    .collect();
                Response::Query(QueryResponse::new(schema, stream::iter(rows)))
            } else {
                let mut encoder = DataRowEncoder::new(schema.clone());
                if let Err(e) = encoder.encode_field(&rows_json) {
                    tracing::error!(error = %e, "failed to encode array slice field");
                    return Response::Execution(Tag::new("ERROR")).into();
                }
                let row = encoder.take_row();
                Response::Query(QueryResponse::new(schema, stream::iter(vec![Ok(row)])))
            };
            ShapedResponse { response, notice }
        }
        PlanKind::SingleDocument | PlanKind::MultiRow => {
            let col_name = if matches!(kind, PlanKind::SingleDocument) {
                "document"
            } else {
                "result"
            };
            let schema = Arc::new(vec![text_field(col_name)]);
            if payload.is_empty() {
                return Response::Query(QueryResponse::new(schema, stream::empty())).into();
            }
            let text = decode_payload_to_json(payload);

            // For multi-row results, parse the JSON array and stream each
            // element as a separate pgwire row. This avoids materializing
            // a single giant row for large result sets.
            if matches!(kind, PlanKind::MultiRow)
                && let Ok(serde_json::Value::Array(items)) =
                    sonic_rs::from_str::<serde_json::Value>(&text)
            {
                let row_schema = schema.clone();
                let rows: Vec<_> = items
                    .iter()
                    .map(|item| {
                        let mut encoder = DataRowEncoder::new(row_schema.clone());
                        let _ = encoder.encode_field(&item.to_string());
                        Ok(encoder.take_row())
                    })
                    .collect();
                return Response::Query(QueryResponse::new(schema, stream::iter(rows))).into();
            }

            // Single document or non-array: send as one row.
            let mut encoder = DataRowEncoder::new(schema.clone());
            if let Err(e) = encoder.encode_field(&text) {
                tracing::error!(error = %e, "failed to encode field");
                return Response::Execution(Tag::new("ERROR")).into();
            }
            let row = encoder.take_row();
            Response::Query(QueryResponse::new(schema, stream::iter(vec![Ok(row)]))).into()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::extract_affected_count;

    #[test]
    fn extract_affected_count_reads_msgpack_payload() {
        let payload = nodedb_types::json_to_msgpack(&serde_json::json!({ "inserted": 3 })).unwrap();
        assert_eq!(extract_affected_count(&payload), Some(3));
    }
}
