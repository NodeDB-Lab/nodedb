// SPDX-License-Identifier: BUSL-1.1

//! Plan classification and response formatting.

use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response, Tag};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use sonic_rs;

use crate::bridge::envelope::PhysicalPlan;
use crate::data::executor::response_codec::decode_payload_to_json;
use nodedb_physical::physical_plan::DocumentOp;

use super::super::types::text_field;

pub(super) use crate::control::server::response_shape::types::{
    DmlOutcome, PlanKind, describe_plan,
};

/// Returns `true` when a plan can produce a deterministic pgwire tag without
/// a round-trip to the Data Plane.
///
/// Folding is only sound for a write that CANNOT be a no-op — one that either
/// applies exactly one row or fails the statement. Any write whose row count
/// depends on state the plan has not read must get its count from the
/// mutation's own response (`calvin_execution_response` surfaces it from the
/// deposited applied `Response`), because a synthesised count is a claim about
/// rows nobody looked at.
///
/// **Foldable** — writes that unconditionally apply one row:
///   - `PointPut` (Document) → INSERT 0 1 (upsert: always writes)
///   - `KvOp::Put` → UPSERT 1 (upsert: always writes; tagged like
///     `DocumentOp::Upsert`, the SQL `UPSERT` statement both lower from)
///
/// **Not foldable**:
///   - `PointDelete`, `PointUpdate`, `KvOp::Delete` — no-op when the target row
///     is absent, which a resolved primary key does NOT rule out: a surrogate
///     outlives the row it was assigned to, so a delete of an already-deleted
///     key reaches the Data Plane looking exactly like a delete of a live row
///   - `PointInsert`, `KvOp::Insert`, `KvOp::InsertIfAbsent` — an
///     `ON CONFLICT DO NOTHING` insert onto an existing key applies 0 rows
///   - `KvOp::InsertOnConflictUpdate` — outcome (insert vs update) is decided
///     by the handler, not the plan
///   - Any plan with `RETURNING` (response stream carries rows, not a tag)
///   - `InsertSelect` (row count from source query; unknown at plan time)
///   - `BatchInsert`, `BatchPut` (N rows; count in payload)
///   - `BulkUpdate`, `BulkDelete` (predicate-based; count in payload)
///   - `TimeseriesOp::Ingest` (separate path)
///   - `ColumnarOp::Insert` (batch path; count in payload)
///   - Any `Array`, `Spatial`, `Vector`, `Graph`, or `Text` write
///   - Any `SELECT` / `Query` plan (mixing read responses with a write tag
///     corrupts the response stream)
///   - Any other plan not explicitly listed above
pub(super) fn is_calvin_foldable(plan: &PhysicalPlan) -> bool {
    use nodedb_physical::physical_plan::KvOp;

    match plan {
        // Upserts: the row is written whether or not it existed before, so the
        // count is 1 without consulting state.
        PhysicalPlan::Document(DocumentOp::PointPut { .. })
        | PhysicalPlan::Kv(KvOp::Put { .. }) => true,

        // Everything else: not foldable. The foldable arms above take
        // precedence; these inner wildcards catch every remaining op of each
        // engine. Exhaustive so a new PhysicalPlan variant forces a decision.
        PhysicalPlan::Document(_)
        | PhysicalPlan::Kv(_)
        | PhysicalPlan::Vector(_)
        | PhysicalPlan::Graph(_)
        | PhysicalPlan::Text(_)
        | PhysicalPlan::Columnar(_)
        | PhysicalPlan::Timeseries(_)
        | PhysicalPlan::Spatial(_)
        | PhysicalPlan::Crdt(_)
        | PhysicalPlan::Query(_)
        | PhysicalPlan::Meta(_)
        | PhysicalPlan::Array(_)
        | PhysicalPlan::ClusterArray(_)
        | PhysicalPlan::ClusterEvent(_) => false,
    }
}

/// Synthesise the count-bearing outcome for a Calvin-foldable plan.
///
/// Caller invariant: `plan` must already have passed `is_calvin_foldable`.
/// The match arms here are kept in lockstep with that predicate so a desync
/// between the two is loud rather than silent.
pub(super) fn calvin_tag_for_plan(plan: &PhysicalPlan) -> PgWireResult<DmlOutcome> {
    use nodedb_physical::physical_plan::KvOp;

    match plan {
        PhysicalPlan::Document(DocumentOp::PointPut { .. }) => Ok(DmlOutcome {
            verb: "INSERT",
            affected: 1,
        }),
        // The SQL `UPSERT` statement: same tag as its `DocumentOp::Upsert`
        // sibling and as `describe_plan`'s `DmlResult("UPSERT")` arm.
        PhysicalPlan::Kv(KvOp::Put { .. }) => Ok(DmlOutcome {
            verb: "UPSERT",
            affected: 1,
        }),

        other => Err(invalid_plan_shape(format!(
            "calvin_tag_for_plan called on non-foldable plan: {other:?}"
        ))),
    }
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

pub(super) fn multirow_payload_to_response(payload: &[u8]) -> ShapedResponse {
    let schema = Arc::new(vec![text_field("result")]);
    if payload.is_empty() {
        return Response::Query(QueryResponse::new(schema, stream::empty())).into();
    }
    let text = decode_payload_to_json(payload);

    // For multi-row results, parse the JSON array and stream each
    // element as a separate pgwire row. This avoids materializing
    // a single giant row for large result sets.
    if let Ok(serde_json::Value::Array(items)) = sonic_rs::from_str::<serde_json::Value>(&text) {
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
    if let Err(error) = encoder.encode_field(&text) {
        tracing::error!(%error, "failed to encode field");
        return Response::Execution(Tag::new("ERROR")).into();
    }
    let row = encoder.take_row();
    Response::Query(QueryResponse::new(schema, stream::iter(vec![Ok(row)]))).into()
}

fn invalid_plan_shape(message: String) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        "XX000".to_owned(),
        message,
    )))
}

#[cfg(test)]
mod tests {
    use super::super::super::command_tag::render;
    use super::*;
    use crate::control::server::response_shape::types::{
        payload_to_dml_outcome, staged_dml_outcome,
    };
    use crate::control::server::shared::sql::staging_predicates::StagedTagKind;
    use nodedb_physical::physical_plan::KvOp;
    use nodedb_types::{DatabaseId, QualifiedCollection};

    #[test]
    fn calvin_tag_rejects_non_foldable_plan() {
        let plan = PhysicalPlan::Kv(KvOp::Get {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "items"),
            key: Vec::new(),
            rls_filters: Vec::new(),
            surrogate_ceiling: None,
        });
        assert!(calvin_tag_for_plan(&plan).is_err());
    }

    #[test]
    fn multirow_helper_remains_infallible() {
        let shaped = multirow_payload_to_response(&[]);
        assert!(matches!(shaped.response, Response::Query(_)));
    }

    #[test]
    fn foldable_tag_still_matches_operation() {
        // An upsert applies one row unconditionally, so its tag needs no
        // round-trip.
        let plan = PhysicalPlan::Kv(KvOp::Put {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "items"),
            key: Vec::new(),
            value: Vec::new(),
            ttl_ms: 0,
            surrogate: nodedb_types::Surrogate::ZERO,
            returning: None,
            rls_filters: Vec::new(),
        });
        assert!(is_calvin_foldable(&plan));
        let outcome = calvin_tag_for_plan(&plan).expect("foldable plan renders a tag");
        assert_eq!(
            outcome,
            DmlOutcome {
                verb: "UPSERT",
                affected: 1
            }
        );
        let tag: pgwire::messages::response::CommandComplete = render(outcome).into();
        assert_eq!(tag.tag, "UPSERT 1");
    }

    /// A write that can legitimately touch nothing must NOT be folded: its count
    /// is only knowable from the mutation's own response. Folding a delete let a
    /// re-delete of an already-deleted key report a removed row.
    #[test]
    fn no_op_capable_writes_are_never_folded() {
        let delete = PhysicalPlan::Kv(KvOp::Delete {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "items"),
            keys: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            returning: None,
            rls_filters: Vec::new(),
        });
        assert!(!is_calvin_foldable(&delete));
        assert!(calvin_tag_for_plan(&delete).is_err());

        let point_delete = PhysicalPlan::Document(DocumentOp::PointDelete {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "items"),
            document_id: "a".into(),
            surrogate: nodedb_types::Surrogate::ZERO,
            pk_bytes: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            resolved_sum_targets: Vec::new(),
        });
        assert!(!is_calvin_foldable(&point_delete));
        assert!(calvin_tag_for_plan(&point_delete).is_err());
    }

    /// A staged `TRUNCATE` renders the same bare tag autocommit does.
    #[test]
    fn staged_truncate_renders_a_bare_tag() {
        let outcome = staged_dml_outcome(StagedTagKind::Truncate, 0);
        let tag: pgwire::messages::response::CommandComplete =
            crate::control::server::pgwire::command_tag::render(outcome).into();
        assert_eq!(tag.tag, "TRUNCATE");
    }

    /// `DmlResultByOp` renders the verb the handler reported.
    #[test]
    fn dml_result_by_op_renders_the_reported_verb() {
        let update = nodedb_types::json_to_msgpack(&serde_json::json!({
            "affected": 1,
            "op": "update"
        }))
        .expect("encode payload");
        let outcome = payload_to_dml_outcome(&update, PlanKind::DmlResultByOp)
            .expect("update tag")
            .expect("count-bearing");
        let tag: pgwire::messages::response::CommandComplete = render(outcome).into();
        assert_eq!(tag.tag, "UPDATE 1");
    }
}
