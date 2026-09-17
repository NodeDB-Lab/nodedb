// SPDX-License-Identifier: BUSL-1.1

//! Shared result construction for graph read handlers.

use serde_json::{Map, Value as JsonValue};

use crate::control::server::response_shape::types::ShapedRows;
use crate::data::executor::response_codec;

use super::super::super::result::DdlResult;

/// Render a Data-Plane JSON payload as a single-column `result` row set.
///
/// An empty payload yields an empty result set with the schema still attached
/// so the entrypoint can decode column metadata — byte-identical to the pgwire
/// handler's empty `QueryResponse`.
pub(super) fn payload_to_rows(payload: &crate::bridge::envelope::Payload) -> Vec<DdlResult> {
    let columns = vec!["result".to_string()];

    if payload.is_empty() {
        return vec![DdlResult::Rows(ShapedRows::text_rows(columns, Vec::new()))];
    }

    let json_text = response_codec::decode_payload_to_json(payload);
    let mut row = Map::new();
    row.insert("result".to_string(), JsonValue::String(json_text));

    vec![DdlResult::Rows(ShapedRows::text_rows(columns, vec![row]))]
}
