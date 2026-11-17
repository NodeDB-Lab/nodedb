// SPDX-License-Identifier: Apache-2.0

//! Join and partition key values read from plain or native object rows.
//!
//! A row is a plain MessagePack map or a native tagged `Value::Object`.
//! Every reader here decodes through [`ObjectView`], so a native row's
//! tagged values never read as plain arrays. Key identity is the GROUP BY
//! key encoding over canonical values: plain and native values, and numbers
//! equal by exact value, share it.

use nodedb_types::{ObjectView, Value};

use crate::expr::EvalError;

use super::aggregate_helpers::decode_error;
use super::canonical_key::canonical_key_value;
use super::group_key::encode_group_values;

/// The value of join key `key` in `view`: the field itself, else the trailing
/// segment of a qualified name (`alias.field` reads `field`). `None` when
/// neither field is present.
fn join_key_value(view: ObjectView<'_>, key: &str) -> Result<Option<Value>, EvalError> {
    if let Some(value) = view.get(key).map_err(decode_error)? {
        return Ok(Some(value));
    }
    match key.rsplit_once('.') {
        Some((_, field)) => view.get(field).map_err(decode_error),
        None => Ok(None),
    }
}

/// The typed values of `keys` in the object row `doc`, in `keys` order. A
/// missing field reads as NULL. A row that is not an object is an error.
pub fn join_key_values<S: AsRef<str>>(doc: &[u8], keys: &[S]) -> Result<Vec<Value>, EvalError> {
    let view = ObjectView::new(doc).map_err(decode_error)?;
    keys.iter()
        .map(|key| Ok(join_key_value(view, key.as_ref())?.unwrap_or(Value::Null)))
        .collect()
}

/// The canonical identity of `keys` in `doc`. Two rows share it exactly when
/// their key values are equal under the GROUP BY key encoding, whether each
/// row is plain or native.
pub fn join_key_identity<S: AsRef<str>>(doc: &[u8], keys: &[S]) -> Result<String, EvalError> {
    encode_group_values(&join_key_values(doc, keys)?)
}

/// The canonical identity an equi-join matches `doc` on, without the key
/// values. `None` when any key component is NULL or missing. The hash join
/// probes on this alone.
pub fn join_match_identity<S: AsRef<str>>(
    doc: &[u8],
    keys: &[S],
) -> Result<Option<String>, EvalError> {
    let values = join_key_values(doc, keys)?;
    if values.iter().any(|value| matches!(value, Value::Null)) {
        return Ok(None);
    }
    encode_group_values(&values).map(Some)
}

/// The canonical identity an equi-join matches `doc` on, with its
/// canonical key values. `None` when any key component is NULL or missing:
/// `NULL = x` is never true, so such a row matches no row. Two rows match
/// exactly when both are `Some` with equal identity.
pub fn join_match_key<S: AsRef<str>>(
    doc: &[u8],
    keys: &[S],
) -> Result<Option<(Vec<Value>, String)>, EvalError> {
    let values = join_key_values(doc, keys)?;
    if values.iter().any(|value| matches!(value, Value::Null)) {
        return Ok(None);
    }
    let canonical: Vec<Value> = values.into_iter().map(canonical_key_value).collect();
    let identity = encode_group_values(&canonical)?;
    Ok(Some((canonical, identity)))
}

/// The qualified field name in `doc` that the bare key `key` names: the
/// first field ending in `.<key>`. `None` when `doc` has `key` itself or no
/// such field.
pub fn resolve_qualified_key(doc: &[u8], key: &str) -> Result<Option<String>, EvalError> {
    let view = ObjectView::new(doc).map_err(decode_error)?;
    let suffix = format!(".{key}");
    let mut qualified = None;
    for field in view.fields() {
        let (name, _) = field.map_err(decode_error)?;
        if name == key {
            return Ok(None);
        }
        if qualified.is_none() && name.ends_with(&suffix) {
            qualified = Some(name.to_owned());
        }
    }
    Ok(qualified)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn row(fields: &[(&str, Value)]) -> Value {
        Value::Object(
            fields
                .iter()
                .map(|(key, value)| ((*key).to_owned(), value.clone()))
                .collect::<HashMap<_, _>>(),
        )
    }

    fn native(fields: &[(&str, Value)]) -> Vec<u8> {
        zerompk::to_msgpack_vec(&row(fields)).expect("native row")
    }

    fn plain(fields: &[(&str, Value)]) -> Vec<u8> {
        nodedb_types::value_to_msgpack(&row(fields)).expect("plain row")
    }

    #[test]
    fn plain_and_native_rows_share_key_identity() {
        for value in [
            Value::Integer(9_007_199_254_740_993),
            Value::Float(2.5),
            Value::String("a\"b".into()),
            Value::Bool(true),
            Value::Null,
            Value::Array(vec![Value::Integer(4), Value::String("text".into())]),
            Value::Object(HashMap::from([
                ("b".to_owned(), Value::Integer(2)),
                ("a".to_owned(), Value::Integer(1)),
            ])),
        ] {
            let fields = [("k", value.clone()), ("other", Value::Integer(7))];
            let native_id = join_key_identity(&native(&fields), &["k"]).expect("native");
            let plain_id = join_key_identity(&plain(&fields), &["k"]).expect("plain");
            assert_eq!(native_id, plain_id, "{value:?}");
            assert_eq!(
                join_key_values(&native(&fields), &["k"]).expect("native values"),
                vec![value]
            );
        }
    }

    #[test]
    fn native_arrays_read_as_values_not_tagged_arrays() {
        let value = Value::Array(vec![Value::Integer(1), Value::Integer(2)]);
        let fields = [("k", value.clone())];
        assert_eq!(
            join_key_values(&native(&fields), &["k"]).expect("values"),
            vec![value]
        );
    }

    #[test]
    fn distinct_values_have_distinct_identity() {
        let one = join_key_identity(&native(&[("k", Value::Integer(1))]), &["k"]).expect("one");
        let text =
            join_key_identity(&native(&[("k", Value::String("1".into()))]), &["k"]).expect("text");
        let decimal = join_key_identity(
            &native(&[("k", Value::Decimal("1.5".parse().expect("decimal")))]),
            &["k"],
        )
        .expect("decimal");
        assert_ne!(one, text);
        assert_ne!(one, decimal);
    }

    #[test]
    fn null_or_missing_components_never_match() {
        let doc = native(&[("a", Value::Integer(1)), ("b", Value::Null)]);
        assert_eq!(join_match_key(&doc, &["a", "b"]).expect("null"), None);
        assert_eq!(join_match_key(&doc, &["a", "c"]).expect("missing"), None);
        assert!(join_match_key(&doc, &["a"]).expect("present").is_some());
    }

    #[test]
    fn match_identity_equals_match_key_identity() {
        for value in [
            Value::Integer(5),
            Value::Decimal("1.50".parse().expect("decimal")),
            Value::Null,
        ] {
            let doc = native(&[("k", value)]);
            assert_eq!(
                join_match_identity(&doc, &["k"]).expect("identity"),
                join_match_key(&doc, &["k"])
                    .expect("key")
                    .map(|(_, identity)| identity)
            );
        }
    }

    #[test]
    fn match_keys_agree_across_numeric_types() {
        let id = |value: Value| {
            join_match_key(&native(&[("k", value)]), &["k"])
                .expect("key")
                .map(|(_, identity)| identity)
        };
        let five = id(Value::Integer(5));
        assert_eq!(id(Value::Float(5.0)), five);
        assert_eq!(id(Value::Decimal("5.00".parse().expect("decimal"))), five);
        assert_eq!(id(Value::Float(-0.0)), id(Value::Integer(0)));
        assert_ne!(
            id(Value::Float(0.1)),
            id(Value::Decimal("0.1".parse().expect("decimal")))
        );
    }

    #[test]
    fn qualified_keys_fall_back_to_the_trailing_segment() {
        let doc = native(&[("id", Value::Integer(5))]);
        assert_eq!(
            join_key_values(&doc, &["t.id", "missing"]).expect("values"),
            vec![Value::Integer(5), Value::Null]
        );
    }

    #[test]
    fn bare_keys_resolve_to_qualified_fields() {
        for doc in [
            native(&[("orders.amount", Value::Integer(1))]),
            plain(&[("orders.amount", Value::Integer(1))]),
        ] {
            assert_eq!(
                resolve_qualified_key(&doc, "amount").expect("resolve"),
                Some("orders.amount".to_owned())
            );
        }
        let bare = native(&[("amount", Value::Integer(1))]);
        assert_eq!(resolve_qualified_key(&bare, "amount").expect("bare"), None);
    }

    #[test]
    fn malformed_rows_return_errors() {
        let truncated = [0x92, 7, 0x81, 0xa1, b'k'];
        assert!(join_key_values(&truncated, &["k"]).is_err());
        assert!(join_key_identity(&truncated, &["k"]).is_err());
        assert!(resolve_qualified_key(&truncated, "k").is_err());
    }
}
