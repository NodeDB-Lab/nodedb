// SPDX-License-Identifier: BUSL-1.1

//! Data-Plane response payload decoding for the RESP surface.
//!
//! Data-Plane responses are MessagePack (`response_codec`), not JSON. Decoding
//! them with a JSON parser fails on every non-empty payload, and a decoder that
//! swallows that failure reports "no keys" / "zero" instead of the real answer.
//! Every RESP handler that needs to look inside a payload goes through here.

use crate::control::security::redaction::RedactionStore;
use crate::control::server::response_shape::redaction::{QueryRedaction, redact_envelope_row};
use crate::data::executor::response_codec::decode_payload_to_json;

/// Decode a Data-Plane payload into a JSON value.
///
/// Returns `Value::Null` for an empty or undecodable payload; callers that
/// cannot treat a decode failure as an empty result should check for `Null`
/// explicitly rather than defaulting.
pub(super) fn payload_json(payload: &[u8]) -> serde_json::Value {
    if payload.is_empty() {
        return serde_json::Value::Null;
    }
    sonic_rs::from_str(&decode_payload_to_json(payload)).unwrap_or(serde_json::Value::Null)
}

/// Extract an integer field from a Data-Plane payload.
pub(super) fn payload_field_i64(payload: &[u8], field: &str) -> Option<i64> {
    payload_json(payload).get(field)?.as_i64()
}

/// Decode a KV scan payload into its entry rows.
///
/// Shared by `KEYS`, `SCAN` and `DBSIZE`, which all read the same scan result
/// shape: a msgpack array of entry maps carrying a `key` field alongside the
/// row's stored columns.
fn scan_entries(payload: &[u8]) -> Option<Vec<serde_json::Value>> {
    match payload_json(payload) {
        serde_json::Value::Array(entries) => Some(entries),
        serde_json::Value::Null if payload.is_empty() => Some(Vec::new()),
        _ => None,
    }
}

/// Read the `key` field out of already-decoded scan entries.
///
/// The KV scan handler injects that field as a plain string (see the
/// `inject_str_field` calls in the Data-Plane scan handler), so the bytes are
/// taken as-is — base64-decoding them, as this path once did, discards every
/// key whose name is not incidentally valid base64.
fn entry_keys(entries: &[serde_json::Value]) -> Vec<Vec<u8>> {
    entries
        .iter()
        .filter_map(|e| e.get("key")?.as_str().map(|k| k.as_bytes().to_vec()))
        .collect()
}

/// Decode a KV scan payload into its keys.
///
/// For `DBSIZE`, which reports only how many keys the collection holds. The
/// answer is a count, not a stored value, so no rule can rewrite it.
pub(super) fn scan_keys(payload: &[u8]) -> Option<Vec<Vec<u8>>> {
    Some(entry_keys(&scan_entries(payload)?))
}

/// Decode a KV scan payload into its keys, with this command's column
/// redaction applied to each entry row first.
///
/// `KEYS` and `SCAN` hand the client the rows' primary keys, which a KV
/// collection carries as the ordinary column `key` — a redaction rule naming
/// it therefore covers this path exactly as it covers `SELECT key FROM ...`.
/// An entry whose key a rule nulls out has nothing left to list and is
/// dropped.
pub(super) fn redacted_scan_keys(
    payload: &[u8],
    redaction: Option<&QueryRedaction>,
    store: &RedactionStore,
) -> Option<Vec<Vec<u8>>> {
    let mut entries = scan_entries(payload)?;
    for entry in &mut entries {
        // KV entries are flat field maps.
        redact_envelope_row(
            redaction,
            store,
            crate::data::executor::response_codec::RowContext::Flat,
            entry,
        );
    }
    Some(entry_keys(&entries))
}
