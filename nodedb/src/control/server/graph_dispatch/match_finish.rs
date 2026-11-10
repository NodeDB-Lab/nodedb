// SPDX-License-Identifier: BUSL-1.1

//! The coordinator's tail of a MATCH: the merged rows of every core and
//! shard take DISTINCT, ORDER BY, OFFSET and LIMIT here, once
//! (`pattern::executor::finish_match_rows`), before a client sees them.

use crate::bridge::envelope::Payload;
use crate::engine::graph::pattern::ast::MatchQuery;
use crate::engine::graph::pattern::executor::{BindingRow, finish_match_rows, rows_to_msgpack};

/// Decode a bare msgpack rows array (the shape `rows_to_msgpack` produces and
/// `unwrap_match_envelope` returns) into typed binding rows. Each row is a
/// native tagged object. An empty payload is an empty row set.
pub(crate) fn decode_match_rows(payload: &[u8]) -> crate::Result<Vec<BindingRow>> {
    if payload.is_empty() {
        return Ok(Vec::new());
    }
    let rows = zerompk::from_msgpack::<Vec<nodedb_types::Value>>(payload).map_err(|e| {
        crate::Error::Codec {
            detail: format!("MATCH: invalid rows array: {e}"),
        }
    })?;
    rows.into_iter()
        .enumerate()
        .map(|(index, row)| match row {
            nodedb_types::Value::Object(fields) => Ok(fields),
            other => Err(crate::Error::Codec {
                detail: format!(
                    "MATCH: row {index} is a {} value, not an object",
                    other.type_name()
                ),
            }),
        })
        .collect()
}

/// The merged rows payload with the query's window applied, re-encoded for
/// the native row transport. `query_bytes` is the serialized `MatchQuery`
/// the plan carries.
pub(crate) fn finish_match_payload(
    query_bytes: &[u8],
    payload: &Payload,
) -> crate::Result<Payload> {
    let query: MatchQuery =
        zerompk::from_msgpack(query_bytes).map_err(|e| crate::Error::Codec {
            detail: format!("MATCH: invalid query: {e}"),
        })?;
    let rows = finish_match_rows(&query, decode_match_rows(payload.as_ref())?)?;
    Ok(Payload::from_vec(rows_to_msgpack(&rows)?))
}
