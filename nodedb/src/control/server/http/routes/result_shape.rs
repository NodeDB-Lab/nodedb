// SPDX-License-Identifier: BUSL-1.1

//! Shared HTTP query-result shaping helper.
//!
//! Routes every HTTP query result path (materialized JSON, materialized
//! NDJSON) through the protocol-neutral [`shape_response_materialized`]
//! core — the same shaping pgwire and native use — so HTTP SELECT rows are
//! KV-wrapped, vector-translated, decoded, scan-envelope-unwrapped, and
//! projected identically to the other two protocols. Non-row plan kinds
//! (`Execution`, `DmlResult`) come back as [`HttpShaped::Passthrough`]; the
//! caller keeps its existing raw decode/base64 fallback for those.

use crate::control::server::response_shape::cell::row_to_wire_json;
use crate::control::server::response_shape::compose::{ShapeOutcome, shape_response_materialized};
use crate::control::server::response_shape::request::MaterializedShapeRequest;
use nodedb_types::NodeDbError;

/// Outcome of shaping one Data-Plane payload for an HTTP response.
pub(super) enum HttpShaped {
    /// Row-producing plan kinds, already decoded, envelope-unwrapped, and
    /// (when a projection applies) column-selected.
    Rows(Vec<serde_json::Value>),
    /// Tag/execution plan kinds — the caller keeps its existing raw
    /// decode/base64 behavior for these.
    Passthrough,
}

/// Shape one Data-Plane payload for HTTP, mirroring the pgwire/native
/// materialized-shaping call site.
pub(super) fn shape_http_payload(
    request: MaterializedShapeRequest<'_>,
) -> Result<HttpShaped, NodeDbError> {
    match shape_response_materialized(request)? {
        // Each row map is already keyed by `ShapedRows::cell_keys`, so it
        // serializes to JSON key for key, each typed cell converted at this
        // edge. When two output columns share a name the later one carries a
        // `_<n>` suffix (`SELECT w.id, b.id` → `{"id": …, "id_1": …}`) — a
        // JSON object cannot repeat a key, and dropping the duplicate would
        // silently lose a projected column.
        ShapeOutcome::Rows(shaped) => Ok(HttpShaped::Rows(
            shaped
                .rows
                .iter()
                .map(|row| serde_json::Value::Object(row_to_wire_json(row)))
                .collect(),
        )),
        ShapeOutcome::Passthrough => Ok(HttpShaped::Passthrough),
    }
}

/// Decode a Data-Plane payload to JSON for a `Passthrough` result (writes /
/// tags): MessagePack first, then JSON passthrough, then base64 as a last
/// resort for undecodable binary payloads. Matches the pre-shaping-core raw
/// behavior of the materialized JSON handler exactly.
pub(super) fn passthrough_json_row(payload: &[u8]) -> serde_json::Value {
    if let Ok(val) = nodedb_types::json_from_msgpack(payload) {
        return val;
    }
    if let Ok(val) = sonic_rs::from_slice::<serde_json::Value>(payload) {
        return val;
    }
    use base64::Engine;
    let encoded = base64::engine::general_purpose::STANDARD.encode(payload);
    serde_json::json!({ "data": encoded })
}

/// Append a `Passthrough` payload (writes / tags) to an NDJSON buffer as one
/// line per top-level JSON array element, or one line for a non-array
/// payload. Matches the pre-shaping-core raw behavior of the materialized
/// NDJSON handler exactly.
pub(super) fn passthrough_to_ndjson(payload: &[u8], ndjson: &mut String) {
    let json_str = crate::data::executor::response_codec::decode_payload_to_json(payload);
    if json_str.trim_start().starts_with('[') {
        for lv in sonic_rs::to_array_iter(json_str.as_str()).flatten() {
            ndjson.push_str(lv.as_raw_str());
            ndjson.push('\n');
        }
    } else {
        ndjson.push_str(&json_str);
        ndjson.push('\n');
    }
}

/// Render protocol-neutral DDL results to JSON rows.
///
/// Status and empty results keep their prior JSON shapes (`{type, tag}` /
/// `{type: "empty"}`). Row-returning results (SHOW / EXPLAIN / introspection)
/// emit one JSON object per row instead of a stub note — HTTP can return
/// DDL/SHOW rows directly.
pub(super) fn ddl_results_to_json(
    results: Vec<crate::control::server::shared::ddl::DdlResult>,
) -> Vec<serde_json::Value> {
    use crate::control::server::shared::ddl::DdlResult;

    let mut rows = Vec::new();
    for result in results {
        match result {
            DdlResult::Status { command, .. } => {
                rows.push(serde_json::json!({
                    "type": "execution",
                    "tag": command,
                }));
            }
            // Keyed by `ShapedRows::cell_keys`, same JSON contract as
            // `shape_http_payload` above (duplicate names take a `_<n>` key).
            DdlResult::Rows(shaped) => {
                for row in &shaped.rows {
                    rows.push(serde_json::Value::Object(row_to_wire_json(row)));
                }
            }
            DdlResult::Empty => {
                rows.push(serde_json::json!({ "type": "empty" }));
            }
        }
    }
    rows
}
