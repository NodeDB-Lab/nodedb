// SPDX-License-Identifier: BUSL-1.1

//! Join-key extraction from a planned document body.
//!
//! The body a write op carries is standard MessagePack at plan time (the strict
//! Binary-Tuple encoding happens in the Data Plane), so one decode reaches every
//! binding's `join_column` regardless of the collection's storage mode.

use memchr::memmem;

/// Read `join_column` out of a standard-msgpack document `value` and render it
/// as the target collection's primary-key STRING — the same rendering
/// `extract_doc_id` applies to an inserted row's key, so the bytes hash to the
/// binding the target row was created under.
///
/// Returns `None` when the field is absent or null: the row simply does not
/// participate in this binding. A present-but-empty string IS a join value and
/// is returned as one — the emptiness is the caller's to judge.
///
/// A repeated key resolves to its last entry.
///
/// A cheap byte pre-filter skips the decode when the column name does not occur
/// in the body at all.
pub fn join_value_from_body(value: &[u8], join_column: &str) -> Option<String> {
    memmem::find(value, join_column.as_bytes())?;

    let decoded = crate::util::bounded_msgpack::read_value(value).ok()?;
    let rmpv::Value::Map(entries) = decoded else {
        return None;
    };

    // A repeated key resolves to its last entry, the same rule the Data Plane
    // decoder applies.
    let last = entries.iter().rev().find_map(|(k, v)| match k {
        rmpv::Value::String(s) if s.as_str() == Some(join_column) => Some(v),
        _ => None,
    })?;
    render_key(last)
}

/// Render a msgpack scalar the way the document engine renders a primary key.
/// Non-scalar values (maps, arrays, binary) are not primary keys and yield
/// `None` rather than a lossy debug string that could collide with a real key.
fn render_key(v: &rmpv::Value) -> Option<String> {
    match v {
        rmpv::Value::String(s) => s.as_str().map(str::to_string),
        rmpv::Value::Integer(i) => Some(i.to_string()),
        rmpv::Value::F32(f) => Some(f.to_string()),
        rmpv::Value::F64(f) => Some(f.to_string()),
        rmpv::Value::Boolean(b) => Some(b.to_string()),
        rmpv::Value::Nil
        | rmpv::Value::Binary(_)
        | rmpv::Value::Array(_)
        | rmpv::Value::Map(_)
        | rmpv::Value::Ext(_, _) => None,
    }
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

    #[test]
    fn non_scalar_join_value_is_not_a_key() {
        let v = doc(&[(
            "account_id",
            rmpv::Value::Array(vec![rmpv::Value::Integer(1.into())]),
        )]);
        assert!(join_value_from_body(&v, "account_id").is_none());
    }

    #[test]
    fn repeated_join_key_takes_the_last_value() {
        let v = doc(&[
            ("account_id", rmpv::Value::Nil),
            ("account_id", rmpv::Value::String("acc".into())),
        ]);
        assert_eq!(
            join_value_from_body(&v, "account_id"),
            Some("acc".to_string())
        );
    }

    #[test]
    fn repeated_join_key_ending_in_null_yields_none() {
        let v = doc(&[
            ("account_id", rmpv::Value::String("acc".into())),
            ("account_id", rmpv::Value::Nil),
        ]);
        assert!(join_value_from_body(&v, "account_id").is_none());
    }
}
