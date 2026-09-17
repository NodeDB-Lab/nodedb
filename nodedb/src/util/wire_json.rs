// SPDX-License-Identifier: BUSL-1.1

//! Edge conversion of a typed cell to the JSON a text protocol emits.
//!
//! HTTP renders JSON text, pgwire renders a composite cell's JSON text
//! (`response_shape::cell::cell_text`), and `control::security` redacts a
//! typed cell before rendering it into a redaction preview; each converts
//! through [`value_to_wire_json`], and nowhere else: the one place a byte
//! cell picks its text form is here. This lives outside
//! `control::server::response_shape` so `control::security` — which
//! `response_shape` itself depends on for redaction — never depends back
//! on `control::server`.
//!
//! A `Value::Bytes` cell renders as unpadded standard base64, the text the
//! msgpack → JSON transcoder (`nodedb_types::msgpack_to_json_string`) emits
//! for a msgpack `bin` — not the hex `serde_json::Value::from(Value)`
//! produces. Every other variant renders exactly as that `From` does.

use base64::Engine;
use nodedb_types::Value;

/// Render one typed cell as the JSON value a text protocol emits.
pub fn value_to_wire_json(cell: &Value) -> serde_json::Value {
    match cell {
        Value::Bytes(bytes) => serde_json::Value::String(
            base64::engine::general_purpose::STANDARD_NO_PAD.encode(bytes),
        ),
        Value::Array(items) | Value::Set(items) => {
            serde_json::Value::Array(items.iter().map(value_to_wire_json).collect())
        }
        Value::Object(map) => serde_json::Value::Object(
            map.iter()
                .map(|(k, v)| (k.clone(), value_to_wire_json(v)))
                .collect(),
        ),
        Value::Null
        | Value::Bool(_)
        | Value::Integer(_)
        | Value::Float(_)
        | Value::String(_)
        | Value::Uuid(_)
        | Value::Ulid(_)
        | Value::DateTime(_)
        | Value::NaiveDateTime(_)
        | Value::Duration(_)
        | Value::Decimal(_)
        | Value::Geometry(_)
        | Value::Regex(_)
        | Value::Range { .. }
        | Value::Record { .. }
        | Value::ArrayCell(_)
        | Value::Vector(_) => serde_json::Value::from(cell.clone()),
        // `Value` is `#[non_exhaustive]`: a variant this crate cannot name
        // renders through the shared `From`, like the scalar arm above.
        _ => serde_json::Value::from(cell.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::NdbDateTime;

    /// A byte cell renders as the same unpadded base64 the msgpack → JSON
    /// transcoder emits for a `bin`, nested or not.
    #[test]
    fn bytes_render_as_unpadded_base64_like_the_transcoder() {
        let bytes = vec![0u8, 255, 7, 1];
        let transcoded = nodedb_types::msgpack_to_json_string(
            &nodedb_types::value_to_msgpack(&Value::Bytes(bytes.clone())).expect("encode"),
        )
        .expect("transcode");
        let expected: serde_json::Value = serde_json::from_str(&transcoded).expect("json");

        assert_eq!(value_to_wire_json(&Value::Bytes(bytes.clone())), expected);
        assert_eq!(
            value_to_wire_json(&Value::Array(vec![Value::Bytes(bytes)])),
            serde_json::Value::Array(vec![expected])
        );
    }

    /// An instant renders as ISO-8601 text, a number as itself, NULL as null.
    #[test]
    fn scalars_render_through_the_shared_from() {
        let at = NdbDateTime::from_micros(1_583_402_400_000_000);
        assert_eq!(
            value_to_wire_json(&Value::NaiveDateTime(at)),
            serde_json::Value::String("2020-03-05T10:00:00.000000Z".into())
        );
        assert_eq!(
            value_to_wire_json(&Value::Integer(7)),
            serde_json::Value::from(7i64)
        );
        assert_eq!(value_to_wire_json(&Value::Null), serde_json::Value::Null);
    }
}
