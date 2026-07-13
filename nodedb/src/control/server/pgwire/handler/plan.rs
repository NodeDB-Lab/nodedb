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
    StagedTagKind, extract_affected_count,
};

use super::super::types::text_field;

pub(super) use crate::control::server::response_shape::types::{PlanKind, describe_plan};

/// Returns `true` when a plan can produce a deterministic pgwire tag without
/// a round-trip to the Data Plane.
///
/// The Calvin multi-shard batch completes as a unit; the Data Plane does not
/// stream individual row counts back per task. For foldable plans we synthesise
/// a tag at plan time (INSERT 0 1, UPDATE 1, DELETE 1). Conservative list:
///
/// **Foldable** — plain point writes where the affected row count is always 1:
///   - `PointPut`, `PointInsert` (Document) → INSERT 0 1
///   - `PointUpdate` without RETURNING (Document) → UPDATE 1
///   - `PointDelete` without RETURNING (Document) → DELETE 1
///   - `KvOp::Put`, `KvOp::Insert`, `KvOp::InsertIfAbsent` → INSERT 0 1
///   - `KvOp::Delete` → DELETE 1
///
/// **Not foldable** (conservative defaults — do NOT expand without care):
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
        // Plain point document writes — always affects 1 row, no RETURNING.
        PhysicalPlan::Document(DocumentOp::PointPut { .. })
        | PhysicalPlan::Document(DocumentOp::PointInsert { .. }) => true,

        // PointUpdate / PointDelete: foldable only when no RETURNING clause.
        PhysicalPlan::Document(DocumentOp::PointUpdate {
            returning: None, ..
        })
        | PhysicalPlan::Document(DocumentOp::PointDelete {
            returning: None, ..
        }) => true,

        // Plain KV point writes.
        PhysicalPlan::Kv(KvOp::Put { .. })
        | PhysicalPlan::Kv(KvOp::Insert { .. })
        | PhysicalPlan::Kv(KvOp::InsertIfAbsent { .. })
        | PhysicalPlan::Kv(KvOp::Delete { .. }) => true,

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
        | PhysicalPlan::ClusterArray(_) => false,
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
        StagedTagKind::Insert => Tag::new("INSERT").with_rows(affected),
        StagedTagKind::Update => Tag::new("UPDATE").with_rows(affected),
        StagedTagKind::Delete => Tag::new("DELETE").with_rows(affected),
        StagedTagKind::KvUpsert { updated: true } => Tag::new("UPDATE").with_rows(affected),
        StagedTagKind::KvUpsert { updated: false } => Tag::new("INSERT").with_rows(affected),
        // Matches the autocommit `DocumentOp::Upsert` tag exactly: always the
        // literal `UPSERT` command, regardless of insert-vs-update outcome
        // (see `response_shape::types::describe_plan`'s `DmlResult("UPSERT")`
        // arm and `payload_to_response`'s `PlanKind::DmlResult` rendering).
        StagedTagKind::DocUpsert => Tag::new("UPSERT").with_rows(affected),
        // Statement-time in-transaction MERGE: the Postgres command tag for a
        // MERGE is `MERGE <total-rows-affected>` across all arms.
        StagedTagKind::Merge => Tag::new("MERGE").with_rows(affected),
        // Statement-time in-transaction `UPDATE ... FROM`: an UPDATE reports the
        // Postgres `UPDATE <n>` command tag over the matched target rows.
        StagedTagKind::UpdateFromJoin => Tag::new("UPDATE").with_rows(affected),
        // KV `Incr` / `IncrFloat` / `Cas` / `GetSet` never reach pgwire's
        // generic tag-rendering path today: their sole SQL surface (`SELECT
        // KV_INCR(..)` and friends, in `ddl/neutral/kv_atomic.rs`) reads
        // `StagedWriteOutcome::payload` directly and never calls
        // `tag_from_staged`. This arm exists only so the match stays
        // exhaustive against a new `PhysicalPlan::Kv` caller; it renders the
        // same tag pgwire uses for a function-call `SELECT`.
        StagedTagKind::RawPayload => Tag::new("SELECT").with_rows(affected),
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
        PhysicalPlan::Document(DocumentOp::PointPut { .. })
        | PhysicalPlan::Document(DocumentOp::PointInsert { .. })
        | PhysicalPlan::Kv(KvOp::Put { .. })
        | PhysicalPlan::Kv(KvOp::Insert { .. })
        | PhysicalPlan::Kv(KvOp::InsertIfAbsent { .. }) => Ok(Tag::new("INSERT").with_rows(1)),

        PhysicalPlan::Document(DocumentOp::PointUpdate {
            returning: None, ..
        }) => Ok(Tag::new("UPDATE").with_rows(1)),

        PhysicalPlan::Document(DocumentOp::PointDelete {
            returning: None, ..
        })
        | PhysicalPlan::Kv(KvOp::Delete { .. }) => Ok(Tag::new("DELETE").with_rows(1)),

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
            let count = if payload.is_empty() {
                // Point operations with empty payload succeeded on exactly 1 row.
                1
            } else {
                extract_affected_count(payload).unwrap_or(1) as usize
            };
            Ok(Response::Execution(Tag::new(tag).with_rows(count)).into())
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

    #[test]
    fn calvin_tag_rejects_non_foldable_plan() {
        let plan = PhysicalPlan::Kv(KvOp::Get {
            collection: "items".into(),
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
        let plan = PhysicalPlan::Kv(KvOp::Delete {
            collection: "items".into(),
            keys: Vec::new(),
        });
        assert!(calvin_tag_for_plan(&plan).is_ok());
    }
}
