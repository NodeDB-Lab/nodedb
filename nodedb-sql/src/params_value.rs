// SPDX-License-Identifier: Apache-2.0

//! The parameter a NodeDB [`Value`] binds as, and the tagged parameter
//! form that carries one over pgwire.
//!
//! JSON holds an object's integers, fractional decimals, finite floats,
//! strings, booleans and `NULL` exactly. It holds a whole decimal, a UUID,
//! an instant, bytes, a duration and a non-finite float only as another
//! type. An object or a mixed array with such a leaf binds under the
//! parameter type [`TAGGED_PARAM_OID`] (`any`), whose binary form is the
//! NodeDB tagged MessagePack of the value. Every leaf keeps its type. An
//! object or array without one binds as `jsonb`, so genuine JSON keeps
//! its own type and is never read as a tagged value.

use nodedb_types::Value;

use crate::params::ParamValue;

/// The parameter type OID of a tagged NodeDB value, see
/// [`nodedb_types::pg_compat::TAGGED_VALUE_PARAM_OID`].
pub const TAGGED_PARAM_OID: u32 = nodedb_types::pg_compat::TAGGED_VALUE_PARAM_OID;

/// The parameter the tagged MessagePack `bytes` spell.
///
/// The bytes are untrusted. They are first walked by the depth-bounded
/// `msgpack_span::skip_value`, so the recursive decoder never sees a
/// value nested past that bound, a truncated value, or bytes past the
/// value. `Err` names why the bytes spell no parameter.
pub fn param_from_tagged_msgpack(bytes: &[u8]) -> Result<ParamValue, String> {
    if nodedb_types::msgpack_span::skip_value(bytes, 0) != Some(bytes.len()) {
        return Err(
            "the tagged MessagePack value is truncated, nested too deeply, or followed by \
             other bytes"
                .to_owned(),
        );
    }
    let value: Value = zerompk::from_msgpack(bytes)
        .map_err(|e| format!("invalid tagged MessagePack value: {e}"))?;
    param_from_value(&value)
}

/// The parameter `value` binds as. Each leaf keeps its exact type. An
/// object's fields are in key order. A ULID is its text, a vector an array
/// of floats, and a geometry its GeoJSON object.
///
/// A set, a regex, a range, a record and an array cell have no parameter
/// form: `Err` names the kind.
pub fn param_from_value(value: &Value) -> Result<ParamValue, String> {
    Ok(match value {
        Value::Null => ParamValue::Null,
        Value::Bool(b) => ParamValue::Bool(*b),
        Value::Integer(i) => ParamValue::Int64(*i),
        Value::Float(f) => ParamValue::Float64(*f),
        Value::Decimal(d) => ParamValue::Decimal(*d),
        Value::String(s) | Value::Ulid(s) => ParamValue::Text(s.clone()),
        Value::Uuid(u) => ParamValue::Uuid(
            nodedb_types::id_gen::canonical_uuid(u).ok_or_else(|| format!("invalid UUID {u:?}"))?,
        ),
        Value::DateTime(at) => ParamValue::Timestamptz(*at),
        Value::NaiveDateTime(at) => ParamValue::Timestamp(*at),
        Value::Bytes(b) => ParamValue::Bytes(b.clone()),
        Value::Duration(d) => ParamValue::Duration(*d),
        Value::Vector(v) => ParamValue::Array(
            v.iter()
                .map(|f| ParamValue::Float64(f64::from(*f)))
                .collect(),
        ),
        Value::Array(items) => ParamValue::Array(
            items
                .iter()
                .map(param_from_value)
                .collect::<Result<_, _>>()?,
        ),
        Value::Object(fields) => {
            let mut fields: Vec<_> = fields.iter().collect();
            fields.sort_by(|a, b| a.0.cmp(b.0));
            ParamValue::Object(
                fields
                    .into_iter()
                    .map(|(key, item)| Ok((key.clone(), param_from_value(item)?)))
                    .collect::<Result<_, String>>()?,
            )
        }
        Value::Geometry(g) => crate::params_composite::param_from_json_text(
            &nodedb_types::geometry::geojson_tree(g).to_string(),
        )?,
        Value::Set(_)
        | Value::Regex(_)
        | Value::Range { .. }
        | Value::Record { .. }
        | Value::ArrayCell(_)
        // `Value` is non-exhaustive: a later kind has no parameter form yet.
        | _ => {
            return Err(format!(
                "a {} value has no parameter form",
                value.type_name()
            ));
        }
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn tagged(value: &Value) -> Vec<u8> {
        zerompk::to_msgpack_vec(value).expect("encode")
    }

    #[test]
    fn every_leaf_keeps_its_type() {
        let uuid = "550e8400-e29b-41d4-a716-446655440000";
        let at = nodedb_types::NdbDateTime::from_micros(1_700_000_000_000_000);
        let object = Value::Object(HashMap::from([
            ("d".to_string(), Value::Decimal("5".parse().unwrap())),
            ("u".to_string(), Value::Uuid(uuid.into())),
            ("t".to_string(), Value::DateTime(at)),
            ("b".to_string(), Value::Bytes(vec![1, 2])),
            (
                "l".to_string(),
                Value::Array(vec![Value::Float(f64::NAN), Value::Integer(1)]),
            ),
        ]));
        let ParamValue::Object(fields) =
            param_from_tagged_msgpack(&tagged(&object)).expect("tagged object")
        else {
            panic!("an object");
        };
        let keys: Vec<&str> = fields.iter().map(|(key, _)| key.as_str()).collect();
        assert_eq!(keys, ["b", "d", "l", "t", "u"]);
        assert!(matches!(&fields[0].1, ParamValue::Bytes(b) if b == &[1, 2]));
        assert!(
            matches!(&fields[1].1, ParamValue::Decimal(d) if d.scale() == 0 && d.to_string() == "5")
        );
        assert!(matches!(
            &fields[2].1,
            ParamValue::Array(items)
                if matches!(items[..], [ParamValue::Float64(f), ParamValue::Int64(1)] if f.is_nan())
        ));
        assert!(matches!(&fields[3].1, ParamValue::Timestamptz(t) if *t == at));
        assert!(matches!(&fields[4].1, ParamValue::Uuid(u) if u == uuid));
    }

    #[test]
    fn a_whole_decimal_renders_as_a_typed_numeric() {
        let object = Value::Object(HashMap::from([(
            "d".to_string(),
            Value::Decimal("5".parse().unwrap()),
        )]));
        let param = param_from_tagged_msgpack(&tagged(&object)).expect("tagged object");
        assert_eq!(
            crate::params_composite::param_sql_text(&param),
            "jsonb_build_object('d', NUMERIC '5')"
        );
    }

    #[test]
    fn malformed_or_trailing_bytes_are_refused() {
        assert!(param_from_tagged_msgpack(&[0xc1]).is_err());
        assert!(param_from_tagged_msgpack(&[]).is_err());
        let mut bytes = tagged(&Value::Integer(1));
        bytes.push(0);
        assert!(param_from_tagged_msgpack(&bytes).is_err());
    }

    #[test]
    fn a_kind_without_a_parameter_form_is_refused() {
        let error = param_from_value(&Value::Regex("a.*".into())).expect_err("a regex");
        assert!(error.contains("regex"), "{error}");
        assert!(param_from_value(&Value::Uuid("nope".into())).is_err());
    }

    #[test]
    fn nesting_past_the_decoder_depth_is_refused() {
        // `[6, [ ... ]]` nested far past the decoder's depth bound.
        let mut bytes = Vec::new();
        for _ in 0..10_000 {
            bytes.extend_from_slice(&[0x92, 0x06, 0x91]);
        }
        bytes.extend_from_slice(&[0x91, 0x00]);
        assert!(param_from_tagged_msgpack(&bytes).is_err());
    }
}
