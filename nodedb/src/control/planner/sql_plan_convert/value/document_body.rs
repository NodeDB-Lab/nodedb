// SPDX-License-Identifier: BUSL-1.1

//! The body one document-route `INSERT` or `UPSERT` row is written as.
//!
//! A plain MessagePack map holds a UUID, a fractional decimal and an instant
//! only as text, the form the document row decoders read for a declared
//! column. A row that holds such a value is written as a native object
//! instead, so a schemaless field keeps the value's exact type, as a native
//! `document_put` keeps it. Every other row stays the plain map.
//!
//! The identity column is written as the key text either way. The document
//! id is that text, and the body field under the identity column must equal
//! it.

use nodedb_sql::types::SqlValue;

use super::convert::{sql_value_to_nodedb_value, sql_value_to_string};
use super::msgpack_write::row_to_msgpack;

/// The body of `row`, whose identity column is `identity`.
pub(crate) fn document_row_body(
    row: &[(String, SqlValue)],
    identity: &str,
) -> crate::Result<Vec<u8>> {
    let typed = row
        .iter()
        .any(|(name, value)| !name.eq_ignore_ascii_case(identity) && needs_native(value));
    if !typed {
        return row_to_msgpack(row);
    }
    let fields = row
        .iter()
        .map(|(name, value)| {
            let value = match value {
                SqlValue::Null => nodedb_types::Value::Null,
                _ if name.eq_ignore_ascii_case(identity) => {
                    nodedb_types::Value::String(sql_value_to_string(value))
                }
                _ => sql_value_to_nodedb_value(value),
            };
            (name.clone(), value)
        })
        .collect();
    zerompk::to_msgpack_vec(&nodedb_types::Value::Object(fields)).map_err(|e| {
        crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("document row body: {e}"),
        }
    })
}

/// Whether the plain map would hold `value` as text: a UUID, an instant, a
/// duration, or a decimal that is not a `uint64`, at any depth.
fn needs_native(value: &SqlValue) -> bool {
    match value {
        SqlValue::Uuid(_)
        | SqlValue::Timestamp(_)
        | SqlValue::Timestamptz(_)
        | SqlValue::Duration(_) => true,
        SqlValue::Decimal(d) => nodedb_types::Value::decimal_as_wide_u64(d).is_none(),
        SqlValue::Array(items) => items.iter().any(needs_native),
        SqlValue::Object(fields) => fields.iter().any(|(_, value)| needs_native(value)),
        SqlValue::Int(_)
        | SqlValue::Float(_)
        | SqlValue::String(_)
        | SqlValue::Bool(_)
        | SqlValue::Null
        | SqlValue::Bytes(_)
        | SqlValue::Geometry(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const UUID: &str = "550e8400-e29b-41d4-a716-446655440000";

    fn decoded(body: &[u8]) -> std::collections::HashMap<String, nodedb_types::Value> {
        match nodedb_types::object_msgpack::decode_object(body).expect("native body decodes") {
            nodedb_types::Value::Object(fields) => fields,
            other => panic!("an object, got {other:?}"),
        }
    }

    #[test]
    fn a_plain_row_stays_a_plain_map() {
        let row = vec![
            ("id".to_string(), SqlValue::String("d1".into())),
            ("n".to_string(), SqlValue::Int(3)),
            ("blob".to_string(), SqlValue::Bytes(vec![1])),
        ];
        assert_eq!(
            document_row_body(&row, "id").expect("encodes"),
            row_to_msgpack(&row).expect("encodes")
        );
    }

    #[test]
    fn a_typed_row_is_a_native_object_keeping_every_type() {
        let at = nodedb_types::NdbDateTime::from_micros(1_704_164_645_000_000);
        let price: rust_decimal::Decimal = "12.50".parse().expect("decimal");
        let row = vec![
            ("id".to_string(), SqlValue::Uuid(UUID.into())),
            ("uid".to_string(), SqlValue::Uuid(UUID.into())),
            ("at".to_string(), SqlValue::Timestamptz(at)),
            ("naive".to_string(), SqlValue::Timestamp(at)),
            ("price".to_string(), SqlValue::Decimal(price)),
            ("blob".to_string(), SqlValue::Bytes(vec![1, 2, 0xff])),
            ("gone".to_string(), SqlValue::Null),
        ];
        let fields = decoded(&document_row_body(&row, "id").expect("encodes"));
        assert_eq!(fields["id"], nodedb_types::Value::String(UUID.into()));
        assert_eq!(fields["uid"], nodedb_types::Value::Uuid(UUID.into()));
        assert_eq!(fields["at"], nodedb_types::Value::DateTime(at));
        assert_eq!(fields["naive"], nodedb_types::Value::NaiveDateTime(at));
        assert_eq!(fields["price"], nodedb_types::Value::Decimal(price));
        assert_eq!(fields["blob"], nodedb_types::Value::Bytes(vec![1, 2, 0xff]));
        assert_eq!(fields["gone"], nodedb_types::Value::Null);
    }

    #[test]
    fn a_typed_identity_alone_keeps_the_plain_map() {
        let row = vec![("id".to_string(), SqlValue::Uuid(UUID.into()))];
        assert_eq!(
            document_row_body(&row, "id").expect("encodes"),
            row_to_msgpack(&row).expect("encodes")
        );
    }
}
