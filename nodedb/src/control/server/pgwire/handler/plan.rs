// SPDX-License-Identifier: BUSL-1.1

//! Plan classification and response formatting.

use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response, Tag};
use sonic_rs;

use crate::data::executor::response_codec::decode_payload_to_json;

use super::super::types::text_field;

pub(super) use crate::control::server::response_shape::types::{PlanKind, describe_plan};

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

#[cfg(test)]
mod tests {
    use super::super::super::command_tag::render;
    use super::*;
    use crate::bridge::envelope::PhysicalPlan;
    use crate::control::server::response_shape::types::{
        payload_to_dml_outcome, staged_dml_outcome,
    };
    use crate::control::server::shared::sql::staging_predicates::StagedTagKind;
    use nodedb_physical::physical_plan::KvOp;
    use nodedb_types::{DatabaseId, QualifiedCollection};

    #[test]
    fn multirow_helper_remains_infallible() {
        let shaped = multirow_payload_to_response(&[]);
        assert!(matches!(shaped.response, Response::Query(_)));
    }

    /// The folded Calvin upsert tag renders as the SQL `UPSERT` command.
    #[test]
    fn foldable_tag_still_matches_operation() {
        use crate::control::server::response_shape::calvin_fold::calvin_tag_for_plan;

        let plan = PhysicalPlan::Kv(KvOp::Put {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "items"),
            key: Vec::new(),
            value: Vec::new(),
            ttl_ms: 0,
            surrogate: nodedb_types::Surrogate::ZERO,
            returning: None,
            rls_filters: Vec::new(),
            provenance: None,
        });
        let outcome = calvin_tag_for_plan(&plan).expect("an upsert folds without a round-trip");
        let tag: pgwire::messages::response::CommandComplete = render(outcome).into();
        assert_eq!(tag.tag, "UPSERT 1");
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
