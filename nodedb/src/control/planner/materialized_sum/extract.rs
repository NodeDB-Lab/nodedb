// SPDX-License-Identifier: BUSL-1.1

//! Join-key extraction from a planned document body.
//!
//! The body a write op carries is MessagePack at plan time (the strict
//! Binary-Tuple encoding happens in the Data Plane): a plain map, or a native
//! object `[7, {field: <tagged value>}]`. A native body reads as the plain
//! body of the same values, so one decode reaches every binding's
//! `join_column` regardless of the collection's storage mode.

use memchr::memmem;

/// Read `join_column` out of a document `value` and render it as the target
/// collection's primary-key STRING — the rendering
/// [`nodedb_types::extract_pk_value`] gives an inserted row's key, so the
/// bytes hash to the binding the target row was created under.
///
/// Returns `None` when the field is absent or null, or the body does not
/// decode: the row simply does not participate in this binding. A
/// present-but-empty string IS a join value and is returned as one — the
/// emptiness is the caller's to judge. A non-scalar value is not a key.
///
/// A cheap byte pre-filter skips the decode when the column name does not occur
/// in the body at all.
pub fn join_value_from_body(value: &[u8], join_column: &str) -> Option<String> {
    memmem::find(value, join_column.as_bytes())?;
    nodedb_types::extract_pk_value(value, join_column)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn doc(fields: &[(&str, rmpv::Value)]) -> Vec<u8> {
        let map = rmpv::Value::Map(
            fields
                .iter()
                .map(|(k, v)| (rmpv::Value::String((*k).into()), v.clone()))
                .collect(),
        );
        let mut buf = Vec::new();
        rmpv::encode::write_value(&mut buf, &map).expect("encode test doc");
        buf
    }

    #[test]
    fn string_join_value_is_returned_verbatim() {
        let v = doc(&[
            ("account_id", rmpv::Value::String("acc-1".into())),
            ("amount", rmpv::Value::Integer(10.into())),
        ]);
        assert_eq!(
            join_value_from_body(&v, "account_id"),
            Some("acc-1".to_string())
        );
    }

    #[test]
    fn integer_join_value_renders_as_decimal() {
        let v = doc(&[("account_id", rmpv::Value::Integer(42.into()))]);
        assert_eq!(
            join_value_from_body(&v, "account_id"),
            Some("42".to_string())
        );
    }

    #[test]
    fn absent_column_yields_none() {
        let v = doc(&[("amount", rmpv::Value::Integer(10.into()))]);
        assert!(join_value_from_body(&v, "account_id").is_none());
    }

    #[test]
    fn null_join_value_yields_none() {
        let v = doc(&[("account_id", rmpv::Value::Nil)]);
        assert!(join_value_from_body(&v, "account_id").is_none());
    }

    /// A native object body yields the join value its plain body yields.
    #[test]
    fn native_join_value_reads_as_its_plain_body() {
        use nodedb_types::Value;
        let object = Value::Object(
            [
                ("account_id".to_string(), Value::String("acc-1".into())),
                ("amount".to_string(), Value::Integer(10)),
            ]
            .into_iter()
            .collect(),
        );
        let native = zerompk::to_msgpack_vec(&object).expect("native body");
        let plain = nodedb_types::value_to_msgpack(&object).expect("plain body");
        assert_eq!(
            join_value_from_body(&native, "account_id"),
            Some("acc-1".to_string())
        );
        assert_eq!(
            join_value_from_body(&native, "account_id"),
            join_value_from_body(&plain, "account_id")
        );
    }

    #[test]
    fn non_scalar_join_value_is_not_a_key() {
        let v = doc(&[(
            "account_id",
            rmpv::Value::Array(vec![rmpv::Value::Integer(1.into())]),
        )]);
        assert!(join_value_from_body(&v, "account_id").is_none());
    }
}
