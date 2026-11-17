// SPDX-License-Identifier: BUSL-1.1

//! A key-value collection's snapshot rows, in the shape a document snapshot
//! carries.
//!
//! A KV scan answers a flat msgpack array of row maps. Each map is the
//! `{key, ...}` row every KV read returns, and a KV row push carries the
//! same map as its payload. A snapshot carries `{id, data}` envelopes, so
//! each KV row becomes an envelope with its key as `id` and the row map as
//! `data`. The client then lands a snapshot row exactly as it lands the
//! row's push.

use nodedb_query::msgpack_scan::{array_header, extract_field, read_str, skip_value};
use nodedb_types::sync::wire::ShapeSnapshotRefusal;

use super::snapshot::SnapshotFailure;

/// The `{id, data}` envelope array of the KV scan `payload`.
pub(super) fn kv_rows_as_envelopes(payload: &[u8]) -> Result<Vec<u8>, SnapshotFailure> {
    let malformed = |offset: usize, what: &str| {
        SnapshotFailure::new(
            ShapeSnapshotRefusal::MalformedRows,
            format!(
                "the {}-byte KV scan payload holds {what} at offset {offset}",
                payload.len()
            ),
        )
    };
    let mut rows: Vec<(String, Vec<u8>)> = Vec::new();
    if !payload.is_empty() {
        let (count, mut at) =
            array_header(payload, 0).ok_or_else(|| malformed(0, "no row array"))?;
        for _ in 0..count {
            let end = skip_value(payload, at).ok_or_else(|| malformed(at, "a truncated row"))?;
            let row = payload
                .get(at..end)
                .ok_or_else(|| malformed(at, "a truncated row"))?;
            let key = extract_field(row, 0, "key")
                .and_then(|(start, _)| read_str(row, start))
                .ok_or_else(|| malformed(at, "a row with no text key"))?;
            rows.push((key.to_owned(), row.to_vec()));
            at = end;
        }
        if at != payload.len() {
            return Err(malformed(at, "bytes past the row array"));
        }
    }
    crate::data::executor::response_codec::encode_raw_document_rows(&rows).map_err(|error| {
        SnapshotFailure::new(
            ShapeSnapshotRefusal::MalformedRows,
            format!("the KV rows do not encode as snapshot rows: {error}"),
        )
    })
}

#[cfg(test)]
mod tests {
    use crate::data::executor::response_codec::{RowContext, scan_rows_in_context};

    use super::*;

    #[test]
    fn kv_rows_become_envelopes_keyed_by_their_key() {
        let rows = serde_json::json!([
            {"key": "k1", "v": 1},
            {"key": "k2", "v": 2},
        ]);
        let payload = nodedb_types::json_to_msgpack(&rows).expect("encode rows");

        let envelopes = kv_rows_as_envelopes(&payload).expect("reshape");
        let docs = scan_rows_in_context(&envelopes, RowContext::ScanEnvelope).expect("split");
        let ids: Vec<&str> = docs.iter().map(|(id, _)| id.as_str()).collect();
        assert_eq!(ids, vec!["k1", "k2"]);
        let first = nodedb_types::json_from_msgpack(&docs[0].1).expect("decode row");
        assert_eq!(first["key"], "k1");
        assert_eq!(first["v"], 1);
    }

    #[test]
    fn an_empty_scan_is_an_empty_snapshot() {
        let envelopes = kv_rows_as_envelopes(&[]).expect("reshape");
        let docs = scan_rows_in_context(&envelopes, RowContext::ScanEnvelope).expect("split");
        assert!(docs.is_empty());
    }

    #[test]
    fn a_row_with_no_key_is_refused() {
        let payload =
            nodedb_types::json_to_msgpack(&serde_json::json!([{"v": 1}])).expect("encode rows");
        let failure = kv_rows_as_envelopes(&payload).expect_err("no key");
        assert_eq!(failure.refusal, ShapeSnapshotRefusal::MalformedRows);
    }
}
