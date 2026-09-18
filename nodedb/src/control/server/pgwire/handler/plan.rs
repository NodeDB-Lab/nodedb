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

use crate::control::server::shared::sql::staging_predicates::{
    StagedTagKind, extract_kv_conflict_op, require_affected_count,
};

use super::super::command_tag::dml_tag;
use super::super::types::text_field;

pub(super) use crate::control::server::response_shape::types::{PlanKind, describe_plan};

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

/// Render a neutral [`StagedTagKind`] (decided by the protocol-neutral
/// staging gate) as the pgwire `CommandComplete` tag, preserving the exact
/// tag strings the pre-refactor `point_write_tag` / `kv_write_tag` produced:
/// `INSERT 0 n` / `UPDATE n` / `DELETE n`, and for a KV
/// `InsertOnConflictUpdate` outcome, `UPDATE n` when the stage handler
/// resolved to an update or `INSERT 0 n` when it resolved to an insert.
pub(super) fn tag_from_staged(kind: StagedTagKind, affected: usize) -> Tag {
    match kind {
        StagedTagKind::Insert => dml_tag("INSERT", affected),
        StagedTagKind::Update => dml_tag("UPDATE", affected),
        StagedTagKind::Delete => dml_tag("DELETE", affected),
        StagedTagKind::KvUpsert { updated: true } => dml_tag("UPDATE", affected),
        StagedTagKind::KvUpsert { updated: false } => dml_tag("INSERT", affected),
        // Matches the autocommit `DocumentOp::Upsert` / `KvOp::Put` tag
        // exactly: always the literal `UPSERT` command, regardless of
        // insert-vs-update outcome (see `response_shape::types::describe_plan`'s
        // `DmlResult("UPSERT")` arms and `payload_to_response`'s
        // `PlanKind::DmlResult` rendering).
        StagedTagKind::Upsert => dml_tag("UPSERT", affected),
        // Statement-time in-transaction MERGE: the Postgres command tag for a
        // MERGE is `MERGE <total-rows-affected>` across all arms.
        StagedTagKind::Merge => dml_tag("MERGE", affected),
        // Statement-time in-transaction `UPDATE ... FROM`: an UPDATE reports the
        // Postgres `UPDATE <n>` command tag over the matched target rows.
        StagedTagKind::UpdateFromJoin => dml_tag("UPDATE", affected),
        // KV `Incr` / `IncrFloat` / `Cas` / `GetSet` never reach pgwire's
        // generic tag-rendering path today: their sole SQL surface (`SELECT
        // KV_INCR(..)` and friends, in `ddl/neutral/kv_atomic/`) reads
        // `StagedWriteOutcome::payload` directly and never calls
        // `tag_from_staged`. This arm exists only so the match stays
        // exhaustive against a new `PhysicalPlan::Kv` caller; it renders the
        // same tag pgwire uses for a function-call `SELECT`.
        StagedTagKind::RawPayload => dml_tag("SELECT", affected),
    }
}

/// Synthesise the pgwire `CommandComplete` tag for a Calvin-foldable plan.
///
/// Caller invariant: `plan` must already have passed `is_calvin_foldable`.
/// The match arms here are kept in lockstep with that predicate so a desync
/// between the two is loud rather than silent.
pub(super) fn calvin_tag_for_plan(plan: &PhysicalPlan) -> PgWireResult<Tag> {
    use nodedb_physical::physical_plan::KvOp;

    match plan {
        PhysicalPlan::Document(DocumentOp::PointPut { .. }) => Ok(dml_tag("INSERT", 1)),
        // The SQL `UPSERT` statement: same tag as its `DocumentOp::Upsert`
        // sibling and as `describe_plan`'s `DmlResult("UPSERT")` arm.
        PhysicalPlan::Kv(KvOp::Put { .. }) => Ok(dml_tag("UPSERT", 1)),

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

pub(super) fn payload_to_response(payload: &[u8], kind: PlanKind) -> PgWireResult<ShapedResponse> {
    match kind {
        PlanKind::Execution => Ok(Response::Execution(Tag::new("OK")).into()),
        PlanKind::DmlResult(tag) => {
            // The count comes from the write, always. There is no "point
            // operations affected exactly 1 row" shortcut: a point delete or a
            // conflicting `ON CONFLICT DO NOTHING` insert is the same plan
            // whether it touched a row or not, so assuming 1 here reported rows
            // that were never there.
            let count = require_affected_count(payload).map_err(|e| {
                invalid_plan_shape(format!("{tag} response is missing its affected count: {e}"))
            })? as usize;
            Ok(Response::Execution(dml_tag(tag, count)).into())
        }
        PlanKind::DmlResultByOp => {
            let count = require_affected_count(payload).map_err(|e| {
                invalid_plan_shape(format!(
                    "DmlResultByOp response is missing its affected count: {e}"
                ))
            })? as usize;
            // The handler decides insert-vs-update at apply time and reports
            // it as `op`. A missing or unknown verb is a handler bug, never a
            // default tag.
            let tag = match extract_kv_conflict_op(payload).as_deref() {
                Some("insert") => "INSERT",
                Some("update") => "UPDATE",
                other => {
                    return Err(invalid_plan_shape(format!(
                        "DmlResultByOp response carries no usable `op` verb \
                         (got {other:?}); the handler must report `insert` or `update`"
                    )));
                }
            };
            Ok(Response::Execution(dml_tag(tag, count)).into())
        }
        PlanKind::ArraySlice | PlanKind::ReturningRows | PlanKind::SingleDocument => {
            Err(invalid_plan_shape(format!(
                "payload_to_response cannot handle plan kind {kind:?}"
            )))
        }
        PlanKind::MultiRow => Ok(multirow_payload_to_response(payload)),
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
    use super::*;
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
    fn passthrough_rejects_precomposed_shapes() {
        assert!(payload_to_response(&[], PlanKind::ArraySlice).is_err());
        assert!(payload_to_response(&[], PlanKind::ReturningRows).is_err());
        assert!(payload_to_response(&[], PlanKind::SingleDocument).is_err());
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
        let tag: pgwire::messages::response::CommandComplete = calvin_tag_for_plan(&plan)
            .expect("foldable plan renders a tag")
            .into();
        assert_eq!(tag.tag, "UPSERT 1");
    }

    /// `KvOp::InsertOnConflictUpdate` reports the verb it resolved to; the tag
    /// follows it, and a payload with no verb is refused rather than defaulted.
    #[test]
    fn dml_result_by_op_follows_the_reported_verb() {
        let update = nodedb_types::json_to_msgpack(&serde_json::json!({
            "affected": 1,
            "op": "update"
        }))
        .expect("encode payload");
        let shaped = payload_to_response(&update, PlanKind::DmlResultByOp).expect("update tag");
        let Response::Execution(tag) = shaped.response else {
            panic!("expected an execution tag");
        };
        let tag: pgwire::messages::response::CommandComplete = tag.into();
        assert_eq!(tag.tag, "UPDATE 1");

        let insert = nodedb_types::json_to_msgpack(&serde_json::json!({
            "affected": 1,
            "op": "insert"
        }))
        .expect("encode payload");
        let shaped = payload_to_response(&insert, PlanKind::DmlResultByOp).expect("insert tag");
        let Response::Execution(tag) = shaped.response else {
            panic!("expected an execution tag");
        };
        let tag: pgwire::messages::response::CommandComplete = tag.into();
        assert_eq!(tag.tag, "INSERT 0 1");

        let no_verb = nodedb_types::json_to_msgpack(&serde_json::json!({ "affected": 1 }))
            .expect("encode payload");
        assert!(payload_to_response(&no_verb, PlanKind::DmlResultByOp).is_err());
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

    /// A count-bearing response with no count is a handler bug, not a `1`.
    #[test]
    fn dml_tag_requires_a_reported_count() {
        assert!(payload_to_response(&[], PlanKind::DmlResult("DELETE")).is_err());
        let payload = nodedb_types::json_to_msgpack(&serde_json::json!({ "affected": 0 }))
            .expect("encode count payload");
        assert!(payload_to_response(&payload, PlanKind::DmlResult("DELETE")).is_ok());
    }
}
