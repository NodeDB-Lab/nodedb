//! Pgwire response construction for graph DSL handlers.

use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response};
use pgwire::error::PgWireResult;

use crate::control::server::pgwire::types::{sqlstate_error, text_field};
use crate::data::executor::response_codec;

/// Render a Data-Plane JSON payload as a single-column `result` row.
///
/// An empty payload yields an empty result set with the schema still
/// attached so pgwire clients can decode column metadata.
pub(super) fn payload_to_query_response(
    payload: &crate::bridge::envelope::Payload,
) -> PgWireResult<Vec<Response>> {
    let schema = Arc::new(vec![text_field("result")]);
    if payload.is_empty() {
        return Ok(vec![Response::Query(QueryResponse::new(
            schema,
            stream::empty(),
        ))]);
    }

    let json_text = response_codec::decode_payload_to_json(payload);
    let mut encoder = DataRowEncoder::new(schema.clone());
    encoder
        .encode_field(&json_text)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
    let row = encoder.take_row();
    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(vec![Ok(row)]),
    ))])
}
