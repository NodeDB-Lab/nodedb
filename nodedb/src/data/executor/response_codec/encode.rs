// SPDX-License-Identifier: BUSL-1.1

//! Generic encoders for Data Plane response payloads, plus the
//! `decode_payload` / `decode_payload_to_json` / `decode_payload_value`
//! counterparts used at the Control Plane boundary.
//!
//! # Every encoder here emits MessagePack
//!
//! Including the ones whose names mention JSON: `encode_json_as_msgpack` and
//! `encode_json_vec_as_msgpack` are named for the `serde_json::Value` they
//! TAKE, never for what they produce. A Control-Plane caller that hands these
//! bytes to a JSON parser gets a parse failure on the first byte, and a caller
//! that then defaults the failure away (`unwrap_or_default`, `if let Ok`)
//! reports a successful empty result for every query — which is silent data
//! loss, not a degraded mode. Read a payload back with [`decode_payload`] (or
//! [`decode_payload_to_json`] for the text form); nothing else is a correct
//! counterpart.

use serde::de::DeserializeOwned;

/// Serialize a response payload as MessagePack bytes.
///
/// Drop-in replacement for `serde_json::to_vec(&value)` in handler code.
/// Returns MessagePack bytes that are 30-50% smaller and 2-3x faster to
/// produce than JSON. Read back with [`decode_payload`].
pub(in crate::data::executor) fn encode<T: zerompk::ToMessagePack>(
    value: &T,
) -> crate::Result<Vec<u8>> {
    zerompk::to_msgpack_vec(value).map_err(|e| crate::Error::Codec {
        detail: format!("response serialization: {e}"),
    })
}

/// Encode a `serde_json::Value` payload as MessagePack bytes.
///
/// Named for its INPUT: the output is MessagePack, like every encoder in this
/// module. Read back with [`decode_payload`] / [`decode_payload_to_json`].
pub(in crate::data::executor) fn encode_json_as_msgpack(
    value: &serde_json::Value,
) -> crate::Result<Vec<u8>> {
    nodedb_types::json_to_msgpack(value).map_err(|e| crate::Error::Codec {
        detail: format!("response serialization: {e}"),
    })
}

/// Encode any `Serialize` type as MessagePack bytes.
///
/// Serializes via serde to an intermediate `serde_json::Value`, then converts
/// to MessagePack. Use `encode()` for types that implement `ToMessagePack`
/// directly (faster, no intermediate). Read back with [`decode_payload`].
pub(in crate::data::executor) fn encode_serde<T: serde::Serialize>(
    value: &T,
) -> crate::Result<Vec<u8>> {
    let json_value = serde_json::to_value(value).map_err(|e| crate::Error::Codec {
        detail: format!("serde serialization: {e}"),
    })?;
    encode_json_as_msgpack(&json_value)
}

/// Encode a slice of `serde_json::Value` rows as MessagePack bytes.
///
/// The name carries `as_msgpack` because the old one (`encode_json_vec`) read
/// as "encode to JSON" and was taken that way by four separate Control-Plane
/// decoders, each of which parsed these bytes as JSON, failed, and defaulted
/// the failure into an empty row set. Read back with [`decode_payload`].
pub(in crate::data::executor) fn encode_json_vec_as_msgpack(
    values: &[serde_json::Value],
) -> crate::Result<Vec<u8>> {
    let wrapped: Vec<nodedb_types::JsonValue> = values
        .iter()
        .map(|v| nodedb_types::JsonValue(v.clone()))
        .collect();
    zerompk::to_msgpack_vec(&wrapped).map_err(|e| crate::Error::Codec {
        detail: format!("response serialization: {e}"),
    })
}

/// Encode a slice of `nodedb_types::Value` as a msgpack array.
///
/// No JSON intermediary — values are serialized directly to standard msgpack.
pub(in crate::data::executor) fn encode_value_vec(
    values: &[nodedb_types::Value],
) -> crate::Result<Vec<u8>> {
    let mut buf = Vec::with_capacity(values.len() * 64);
    let n = values.len();
    if n <= 15 {
        buf.push(0x90 | n as u8);
    } else if n <= 0xFFFF {
        buf.push(0xDC);
        buf.extend_from_slice(&(n as u16).to_be_bytes());
    } else {
        buf.push(0xDD);
        buf.extend_from_slice(&(n as u32).to_be_bytes());
    }
    for val in values {
        let encoded = nodedb_types::value_to_msgpack(val).map_err(|e| crate::Error::Codec {
            detail: format!("value serialization: {e}"),
        })?;
        buf.extend_from_slice(&encoded);
    }
    Ok(buf)
}

/// Encode a simple `{"key": count}` response (for insert confirmations).
///
/// Crate-visible: the Control-Plane cluster array coordinator emits the same
/// count map a local Data-Plane array write does, so one decoder serves both.
pub(crate) fn encode_count(key: &str, count: usize) -> crate::Result<Vec<u8>> {
    let mut map = std::collections::BTreeMap::new();
    map.insert(key, count);
    zerompk::to_msgpack_vec(&map).map_err(|e| crate::Error::Codec {
        detail: format!("count response serialization: {e}"),
    })
}

/// Encode `{"affected": n, "op": op}` — the count of a write whose verb the
/// handler decides at apply time (`"insert"` or `"update"`). The Control Plane
/// reads `op` via `extract_kv_conflict_op` to render `INSERT 0 n` / `UPDATE n`.
pub(crate) fn encode_affected_with_op(affected: u64, op: &str) -> Vec<u8> {
    let mut payload = Vec::with_capacity(32);
    nodedb_query::msgpack_scan::write_map_header(&mut payload, 2);
    nodedb_query::msgpack_scan::write_kv_i64(&mut payload, "affected", affected as i64);
    nodedb_query::msgpack_scan::write_kv_str(&mut payload, "op", op);
    payload
}

/// Deserialize a Data-Plane response payload into `T`.
///
/// THE counterpart to the encoders above, and the only correct way for a
/// Control-Plane caller to read one of their payloads back. Every encoder in
/// this module emits MessagePack — including `encode_json_as_msgpack` and
/// `encode_json_vec_as_msgpack`, which are named for the `serde_json::Value`
/// they take — so a bare `sonic_rs::from_slice` / `serde_json::from_slice` on
/// these bytes fails on the first byte, every time.
///
/// An empty payload yields `T::default()`: a handler with nothing to report
/// sends no bytes, and for the row-shaped `T`s these callers use that is an
/// empty result, not a failure. A NON-empty payload that will not deserialize
/// is an error, never a default — that is the distinction whose absence turned
/// four decode bugs into silently empty answers instead of loud ones.
pub fn decode_payload<T: DeserializeOwned + Default>(payload: &[u8]) -> crate::Result<T> {
    if payload.is_empty() {
        return Ok(T::default());
    }
    let text = decode_payload_to_json(payload);
    sonic_rs::from_str(&text).map_err(|e| crate::Error::Codec {
        detail: format!("response payload could not be decoded: {e}"),
    })
}

/// Decode a MessagePack or JSON payload to a JSON string for pgwire/HTTP output.
///
/// Auto-detects format: if first byte indicates MessagePack, transcodes directly
/// to JSON text via streaming transcoder (no intermediate `serde_json::Value`).
/// If already JSON (starts with `[` or `{`), returns as-is.
pub fn decode_payload_to_json(payload: &[u8]) -> String {
    let Some(&first) = payload.first() else {
        return String::new();
    };

    if looks_like_json(first) {
        return String::from_utf8_lossy(payload).into_owned();
    }

    nodedb_types::msgpack_to_json_string(payload)
        .unwrap_or_else(|_| String::from_utf8_lossy(payload).into_owned())
}

/// Decode a MessagePack or JSON payload to a typed [`nodedb_types::Value`].
///
/// The counterpart of [`decode_payload_to_json`] for the shaping kernel,
/// with the same format sniff: JSON text parses through `serde_json::Value`
/// (so a JSON string is a `Value::String`, never parsed further), anything
/// else reads as msgpack (a `bin` is `Value::Bytes`, an instant ext is a
/// `Value::DateTime` / `Value::NaiveDateTime`). An empty payload is
/// `Value::Null`. A non-empty payload that decodes as neither is an error.
pub fn decode_payload_value(payload: &[u8]) -> crate::Result<nodedb_types::Value> {
    let Some(&first) = payload.first() else {
        return Ok(nodedb_types::Value::Null);
    };

    if looks_like_json(first) {
        return sonic_rs::from_slice::<serde_json::Value>(payload)
            .map(nodedb_types::Value::from)
            .map_err(|e| crate::Error::Codec {
                detail: format!("response payload is not JSON text: {e}"),
            });
    }

    nodedb_types::value_from_msgpack(payload).map_err(|e| crate::Error::Codec {
        detail: format!("response payload is not msgpack: {e}"),
    })
}

/// True when `first` can open JSON text: an array, object, string, number or
/// one of the `true` / `false` / `null` literals. No msgpack marker for a map
/// or array shares these bytes.
fn looks_like_json(first: u8) -> bool {
    first == b'['
        || first == b'{'
        || first == b'"'
        || first.is_ascii_digit()
        || first == b't'
        || first == b'f'
        || first == b'n'
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_count_msg() {
        let bytes = encode_count("inserted", 42).unwrap();
        let json = decode_payload_to_json(&bytes);
        assert!(json.contains("\"inserted\""));
        assert!(json.contains("42"));
    }

    #[test]
    fn json_passthrough() {
        let json_str = r#"[{"id":1}]"#;
        let result = decode_payload_to_json(json_str.as_bytes());
        assert_eq!(result, json_str);
    }

    #[test]
    fn msgpack_to_json_roundtrip() {
        let value = serde_json::json!({"key": "value", "num": 42});
        let msgpack = nodedb_types::json_to_msgpack(&value).unwrap();
        let json = decode_payload_to_json(&msgpack);
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["key"], "value");
        assert_eq!(parsed["num"], 42);
    }

    // ── decode_payload: the counterpart every encoder here needs ────────────
    //
    // Each case below pins one Control-Plane read whose decoder must not be a bare
    // JSON parser that defaults the failure away. The shared assertion is the same
    // one in every case: the bytes an encoder produced must NOT parse as JSON (that
    // is the trap), and must decode through `decode_payload` to exactly what went
    // in.

    /// A JSON parser must fail on these bytes. If it ever stops failing, the
    /// encoders changed format and the `decode_payload` contract needs rechecking —
    /// but while it does fail, any decoder that defaults the failure away is
    /// silently reporting an empty result.
    fn assert_not_json(payload: &[u8]) {
        assert!(
            sonic_rs::from_slice::<serde_json::Value>(payload).is_err(),
            "encoder output parsed as JSON; the trap this guards no longer exists"
        );
    }

    /// `decode_payload_value` sniffs the same way `decode_payload_to_json`
    /// does: JSON text and msgpack both land as the typed value, a msgpack
    /// `bin` as bytes, and an empty payload as NULL.
    #[test]
    fn decode_payload_value_reads_json_text_and_msgpack_alike() {
        use nodedb_types::Value;

        let from_json = decode_payload_value(br#"[{"id":1,"s":"x"}]"#).unwrap();
        let mut row = std::collections::HashMap::new();
        row.insert("id".to_string(), Value::Integer(1));
        row.insert("s".to_string(), Value::String("x".into()));
        assert_eq!(from_json, Value::Array(vec![Value::Object(row.clone())]));

        let msgpack =
            nodedb_types::value_to_msgpack(&Value::Array(vec![Value::Object(row)])).unwrap();
        assert_eq!(decode_payload_value(&msgpack).unwrap(), from_json);

        let bin = nodedb_types::value_to_msgpack(&Value::Bytes(vec![0, 255])).unwrap();
        assert_eq!(
            decode_payload_value(&bin).unwrap(),
            Value::Bytes(vec![0, 255])
        );

        assert_eq!(decode_payload_value(&[]).unwrap(), Value::Null);
        assert!(decode_payload_value(&[0xC1]).is_err());
        assert!(decode_payload_value(b"true-ish").is_err());
    }

    /// `TOPK` / `RANGE` rows — `encode_json_vec_as_msgpack`.
    #[test]
    fn decode_payload_reads_back_json_vec_rows() {
        let rows = vec![
            serde_json::json!({ "rank": 1, "key": "p2" }),
            serde_json::json!({ "rank": 2, "key": "p1" }),
        ];
        let payload = encode_json_vec_as_msgpack(&rows).unwrap();
        assert_not_json(&payload);

        let decoded: Vec<serde_json::Value> = decode_payload(&payload).unwrap();
        assert_eq!(decoded, rows, "the rows encoded must be the rows decoded");
    }

    /// `LAST_VALUES` — `encode` over a tuple list.
    #[test]
    fn decode_payload_reads_back_last_values() {
        let entries: Vec<(u64, i64, f64)> =
            vec![(7, 1_700_000_000_000, 21.5), (9, 1_700_000_001_000, 4.0)];
        let payload = encode(&entries).unwrap();
        assert_not_json(&payload);

        let decoded: Vec<(u64, i64, f64)> = decode_payload(&payload).unwrap();
        assert_eq!(decoded, entries);
    }

    /// `LAST_VALUE` — `encode` over an `Option`. A present series and an absent one
    /// are different facts, and both must survive the round trip.
    #[test]
    fn decode_payload_distinguishes_present_and_absent_last_value() {
        let present = encode(&Some((1_700_000_000_000i64, 21.5f64))).unwrap();
        assert_not_json(&present);
        let decoded: Option<(i64, f64)> = decode_payload(&present).unwrap();
        assert_eq!(decoded, Some((1_700_000_000_000, 21.5)));

        let absent = encode(&Option::<(i64, f64)>::None).unwrap();
        let decoded: Option<(i64, f64)> = decode_payload(&absent).unwrap();
        assert_eq!(
            decoded, None,
            "an absent series decodes to None, not an error"
        );
    }

    /// Remote graph traverse node ids — `encode` over a string list. Dropping one
    /// of these payloads makes a cross-shard traversal report the local shard's
    /// nodes as the whole answer.
    #[test]
    fn decode_payload_reads_back_traverse_node_ids() {
        let nodes: Vec<String> = vec!["n1".into(), "n2".into(), "n3".into()];
        let payload = encode(&nodes).unwrap();
        assert_not_json(&payload);

        let decoded: Vec<String> = decode_payload(&payload).unwrap();
        assert_eq!(decoded, nodes);
    }

    /// `SHOW CONTINUOUS AGGREGATES` runtime stats — `encode_serde` over a
    /// `Serialize` type.
    #[test]
    fn decode_payload_reads_back_serde_encoded_stats() {
        #[derive(serde::Serialize, serde::Deserialize, Default, PartialEq, Debug)]
        struct Stats {
            name: String,
            watermark_ts: i64,
            stale: bool,
        }
        let stats = vec![Stats {
            name: "hourly".into(),
            watermark_ts: 1_700_000_000_000,
            stale: false,
        }];
        let payload = encode_serde(&stats).unwrap();
        assert_not_json(&payload);

        let decoded: Vec<Stats> = decode_payload(&payload).unwrap();
        assert_eq!(decoded, stats);
    }

    /// An empty payload is an empty result: a handler with nothing to report sends
    /// no bytes, and that is a fact, not a failure.
    #[test]
    fn decode_payload_treats_an_empty_payload_as_an_empty_result() {
        let decoded: Vec<serde_json::Value> = decode_payload(&[]).unwrap();
        assert!(decoded.is_empty());
    }

    /// A NON-empty payload that will not deserialize must be an error. This is the
    /// distinction whose absence turned every one of the decode bugs above into a
    /// successful empty answer instead of a loud one.
    #[test]
    fn decode_payload_errors_on_an_undecodable_payload() {
        // Valid msgpack, wrong shape for the target type.
        let payload = encode_json_as_msgpack(&serde_json::json!("not a row list")).unwrap();
        let decoded: crate::Result<Vec<serde_json::Value>> = decode_payload(&payload);
        assert!(
            decoded.is_err(),
            "an unreadable payload must surface as an error, never as zero rows"
        );

        // Bytes that are neither msgpack nor JSON.
        let garbage: Vec<u8> = vec![0xC1, 0xC1, 0xC1];
        let decoded: crate::Result<Vec<String>> = decode_payload(&garbage);
        assert!(decoded.is_err());
    }
}
