// SPDX-License-Identifier: Apache-2.0

//! Msgpack ext encoding for a UUID and a decimal in plain msgpack.
//!
//! Plain msgpack has no UUID or decimal kind. A writer that must keep either
//! typed, such as an `UPDATE` literal, writes it as an ext value:
//! - Ext type `4`: a UUID, its 16 bytes (`fixext16`).
//! - Ext type `5`: a decimal, the 16 bytes of `rust_decimal::Decimal::serialize`
//!   (`fixext16`).
//!
//! [`crate::value_from_msgpack`] reads each back as `Value::Uuid` /
//! `Value::Decimal`. JSON renderings write the UUID's canonical text and the
//! decimal's exact digits as a string.

use crate::value::Value;

/// Ext type byte of a UUID.
pub const EXT_UUID: i8 = 4;
/// Ext type byte of a decimal.
pub const EXT_DECIMAL: i8 = 5;

/// The ext type and payload of a UUID or decimal `value`. `None` for any
/// other value, and for a UUID whose text does not parse.
pub fn scalar_ext(value: &Value) -> Option<(i8, [u8; 16])> {
    match value {
        Value::Uuid(text) => crate::id_gen::uuid_to_bytes(text).map(|bytes| (EXT_UUID, bytes)),
        Value::Decimal(d) => Some((EXT_DECIMAL, d.serialize())),
        _ => None,
    }
}

/// The value a UUID or decimal ext carries. `None` for any other ext type
/// or a payload of the wrong length.
pub fn scalar_from_ext(ext_type: i8, payload: &[u8]) -> Option<Value> {
    match ext_type {
        EXT_UUID => crate::id_gen::uuid_from_bytes(payload).map(Value::Uuid),
        EXT_DECIMAL => {
            let bytes: [u8; 16] = payload.try_into().ok()?;
            Some(Value::Decimal(rust_decimal::Decimal::deserialize(bytes)))
        }
        _ => None,
    }
}

/// The JSON rendering of a UUID or decimal ext: its text as a string.
pub fn scalar_ext_json_text(ext_type: i8, payload: &[u8]) -> Option<String> {
    match scalar_from_ext(ext_type, payload)? {
        Value::Uuid(text) => Some(text),
        Value::Decimal(d) => Some(d.to_string()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_uuid_and_a_decimal_round_trip() {
        let uuid = Value::Uuid("550e8400-e29b-41d4-a716-446655440000".into());
        let decimal = Value::Decimal("12.50".parse().unwrap());
        for value in [uuid, decimal] {
            let (ext_type, payload) = scalar_ext(&value).expect("typed scalar");
            assert_eq!(scalar_from_ext(ext_type, &payload), Some(value));
        }
        assert_eq!(
            scalar_ext_json_text(
                EXT_DECIMAL,
                &scalar_ext(&Value::Decimal("0.10".parse().unwrap()))
                    .unwrap()
                    .1
            ),
            Some("0.10".to_string())
        );
        assert_eq!(scalar_from_ext(EXT_UUID, &[0; 15]), None);
        assert!(scalar_ext(&Value::Integer(1)).is_none());
    }
}
