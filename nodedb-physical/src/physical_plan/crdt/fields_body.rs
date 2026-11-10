// SPDX-License-Identifier: Apache-2.0

//! The fields body a CRDT write carries: [`super::CrdtOp::ListInsert`]'s
//! block fields and [`super::CrdtOp::DocUpsert`]'s row fields.
//!
//! The body is a native object `[7, {field: tagged value}]`, so every field
//! keeps its exact type from the client or the SQL statement to the Loro
//! document. The Data Plane enters each field into Loro through
//! `nodedb_crdt::loro_value`.

use std::collections::HashMap;

use nodedb_types::Value;
use nodedb_types::object_msgpack::{ObjectFormat, ObjectView};

/// Encode `fields` as a CRDT fields body.
pub fn encode_fields_body(fields: &HashMap<String, Value>) -> Result<Vec<u8>, String> {
    zerompk::to_msgpack_vec(&Value::Object(fields.clone()))
        .map_err(|e| format!("CRDT fields do not encode: {e}"))
}

/// Decode a CRDT fields body. A body that is not a native object is refused,
/// naming why.
pub fn decode_fields_body(body: &[u8]) -> Result<HashMap<String, Value>, String> {
    let view = ObjectView::new(body)
        .map_err(|e| format!("CRDT fields are not a MessagePack object: {e}"))?;
    if view.format() != ObjectFormat::Native {
        return Err("CRDT fields are a plain MessagePack map; \
                    a fields body is a native object of tagged values"
            .into());
    }
    match view
        .decode()
        .map_err(|e| format!("CRDT fields do not decode: {e}"))?
    {
        Value::Object(fields) => Ok(fields),
        other => Err(format!(
            "CRDT fields decode as {}, expected an object",
            other.type_name()
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_body_keeps_every_type() {
        let fields = HashMap::from([
            (
                "price".to_string(),
                Value::Decimal("12.50".parse().expect("decimal literal")),
            ),
            (
                "uid".to_string(),
                Value::Uuid("550e8400-e29b-41d4-a716-446655440000".into()),
            ),
            ("blob".to_string(), Value::Bytes(vec![0x01, 0xff])),
        ]);
        let body = encode_fields_body(&fields).expect("encode");
        assert_eq!(decode_fields_body(&body), Ok(fields));
    }

    #[test]
    fn a_plain_map_or_garbage_is_refused() {
        let plain =
            zerompk::to_msgpack_vec(&HashMap::from([("a".to_string(), 1_i64)])).expect("plain map");
        assert!(decode_fields_body(&plain).is_err());
        assert!(decode_fields_body(&[]).is_err());
        assert!(decode_fields_body(&[0xc1]).is_err());
    }
}
