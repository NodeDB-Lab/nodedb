// SPDX-License-Identifier: Apache-2.0

//! Canonical GROUP BY keys with lossless native value decoding.

use std::borrow::Cow;

use nodedb_crs::CrsContext;
use nodedb_types::{ObjectView, Value};

use crate::expr::{EvalError, GroupKeySpec};
use crate::msgpack_scan::index::FieldIndex;

use super::aggregate_helpers::decode_error;
use super::canonical_key::canonical_key_value;

/// Build a storage-independent group key, retaining native types.
pub fn build_group_key(
    doc: &[u8],
    group_keys: &[GroupKeySpec],
    crs: &CrsContext<'_>,
) -> Result<String, EvalError> {
    build_group_key_values(doc, group_keys, crs).map(|(key, _)| key)
}

/// Build a group key using a field index when its ranges address a plain map.
pub fn build_group_key_indexed(
    doc: &[u8],
    group_keys: &[GroupKeySpec],
    idx: &FieldIndex,
    crs: &CrsContext<'_>,
) -> Result<String, EvalError> {
    build_group_key_values_indexed(doc, group_keys, idx, crs).map(|(key, _)| key)
}

/// [`build_group_key`] with the row's typed key values, one per slot. The
/// key is the canonical identity. The values are the row's own, for a
/// group's representative output.
pub fn build_group_key_values(
    doc: &[u8],
    group_keys: &[GroupKeySpec],
    crs: &CrsContext<'_>,
) -> Result<(String, Vec<Value>), EvalError> {
    build_key(
        ObjectView::new(doc).map_err(decode_error)?,
        group_keys,
        None,
        crs,
    )
}

/// [`build_group_key_indexed`] with the row's typed key values.
pub fn build_group_key_values_indexed(
    doc: &[u8],
    group_keys: &[GroupKeySpec],
    idx: &FieldIndex,
    crs: &CrsContext<'_>,
) -> Result<(String, Vec<Value>), EvalError> {
    let view = ObjectView::new(doc).map_err(decode_error)?;
    let index = (view.format() == nodedb_types::ObjectFormat::Plain).then_some(idx);
    build_key(view, group_keys, index, crs)
}

fn build_key(
    view: ObjectView<'_>,
    group_keys: &[GroupKeySpec],
    index: Option<&FieldIndex>,
    crs: &CrsContext<'_>,
) -> Result<(String, Vec<Value>), EvalError> {
    if group_keys.is_empty() {
        return Ok(("__all__".to_owned(), Vec::new()));
    }
    let document = if group_keys
        .iter()
        .any(|spec| spec.field.is_none() && spec.expr.is_some())
    {
        Some(view.decode().map_err(decode_error)?)
    } else {
        None
    };
    let mut values = Vec::with_capacity(group_keys.len());
    for spec in group_keys {
        if let Some(field) = spec.field.as_deref() {
            let value = match index {
                Some(index) => index
                    .get(field)
                    .map(|(start, end)| {
                        let bytes = view
                            .map_bytes()
                            .get(start..end)
                            .ok_or_else(|| decode_error("field index range exceeds object body"))?;
                        view.decode_value(bytes).map_err(decode_error)
                    })
                    .transpose()?,
                None => view.get(field).map_err(decode_error)?,
            };
            values.push(value.unwrap_or(Value::Null));
        } else if let (Some(expr), Some(document)) = (spec.expr.as_ref(), document.as_ref()) {
            values.push(expr.eval(document, crs)?);
        }
    }
    let key = encode_group_values(&values)?;
    Ok((key, values))
}

/// A value's canonical form, borrowed when it is already canonical.
fn canonical_ref(value: &Value) -> Cow<'_, Value> {
    match value {
        Value::Float(_)
        | Value::Decimal(_)
        | Value::Array(_)
        | Value::Set(_)
        | Value::Object(_) => Cow::Owned(canonical_key_value(value.clone())),
        other => Cow::Borrowed(other),
    }
}

/// The canonical identity of a list of key values. Values equal by exact
/// value give equal text, whatever their numeric type and whether they came
/// from a plain or a native row: each value first reads as its
/// [`canonical_key_value`]. A list of JSON-safe scalars encodes as a JSON
/// array, and any other list as tagged `native:` hex. [`decode_group_key`]
/// reads either form back.
pub fn encode_group_values(values: &[Value]) -> Result<String, EvalError> {
    let values: Vec<Cow<'_, Value>> = values.iter().map(canonical_ref).collect();
    let mut plain = String::from("[");
    for (position, value) in values.iter().enumerate() {
        if position != 0 {
            plain.push(',');
        }
        match value.as_ref() {
            Value::Null => plain.push_str("null"),
            Value::Bool(value) => plain.push_str(if *value { "true" } else { "false" }),
            Value::Integer(value) => plain.push_str(&value.to_string()),
            // A canonical finite float is non-integral, so its shortest
            // text carries a `.` and never reads back as an integer.
            Value::Float(value) if value.is_finite() => plain.push_str(&value.to_string()),
            Value::String(value) => {
                plain.push_str(&serde_json::to_string(value).map_err(decode_error)?)
            }
            // A canonical integer past `i64` that fits a `u64` stays JSON.
            Value::Decimal(value)
                if value.scale() == 0 && u64::try_from(value.mantissa()).is_ok() =>
            {
                plain.push_str(&value.mantissa().to_string())
            }
            _ => return encode_native_values(&values),
        }
    }
    plain.push(']');
    Ok(plain)
}

fn encode_native_values(values: &[Cow<'_, Value>]) -> Result<String, EvalError> {
    let array = Value::Array(values.iter().map(|value| value.as_ref().clone()).collect());
    let bytes = zerompk::to_msgpack_vec(&array).map_err(decode_error)?;
    Ok(format!("native:{}", hex::encode(bytes)))
}

/// Decode a group key while retaining native value types.
pub fn decode_group_key(key: &str) -> Result<Vec<Value>, EvalError> {
    if key == "__all__" {
        return Ok(Vec::new());
    }
    let value = if let Some(encoded) = key.strip_prefix("native:") {
        let bytes = hex::decode(encoded).map_err(decode_error)?;
        zerompk::from_msgpack::<Value>(&bytes).map_err(decode_error)?
    } else {
        // sonic-rs parses float text exactly, so a decoded key re-encodes
        // to the same text.
        let value: serde_json::Value = sonic_rs::from_str(key).map_err(decode_error)?;
        Value::from(value)
    };
    match value {
        Value::Array(values) => Ok(values),
        _ => Err(decode_error("group key is not an array")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::SqlExpr;

    fn crs() -> CrsContext<'static> {
        CrsContext::builtin_only()
    }
    use serde_json::json;

    fn encode(v: &serde_json::Value) -> Vec<u8> {
        nodedb_types::json_msgpack::json_to_msgpack(v).expect("encode")
    }

    fn keys(fields: &[&str]) -> Vec<GroupKeySpec> {
        fields.iter().map(|f| GroupKeySpec::column(*f)).collect()
    }

    #[test]
    fn single_string_field() {
        let doc = encode(&json!({"name": "alice", "age": 30}));
        let key = build_group_key(&doc, &keys(&["name"]), &crs()).unwrap();
        assert_eq!(key, r#"["alice"]"#);
    }

    #[test]
    fn single_int_field() {
        let doc = encode(&json!({"status": 200}));
        let key = build_group_key(&doc, &keys(&["status"]), &crs()).unwrap();
        assert_eq!(key, "[200]");
    }

    /// A `uint64` above `i64::MAX` keys by its exact digits, and integers one
    /// apart above 2^53 key apart.
    #[test]
    fn large_integers_key_exactly() {
        let doc = encode(&json!({"v": u64::MAX}));
        let key = build_group_key(&doc, &keys(&["v"]), &crs()).unwrap();
        assert_eq!(key, "[18446744073709551615]");
        let a = encode(&json!({"v": 9_007_199_254_740_993_i64}));
        let b = encode(&json!({"v": 9_007_199_254_740_992_i64}));
        assert_ne!(
            build_group_key(&a, &keys(&["v"]), &crs()).unwrap(),
            build_group_key(&b, &keys(&["v"]), &crs()).unwrap()
        );
    }

    #[test]
    fn multiple_fields() {
        let doc = encode(&json!({"city": "ny", "year": 2024}));
        let key = build_group_key(&doc, &keys(&["city", "year"]), &crs()).unwrap();
        assert_eq!(key, r#"["ny",2024]"#);
    }

    #[test]
    fn missing_field_is_null() {
        let doc = encode(&json!({"x": 1}));
        let key = build_group_key(&doc, &keys(&["missing"]), &crs()).unwrap();
        assert_eq!(key, "[null]");
    }

    #[test]
    fn empty_group_fields() {
        let doc = encode(&json!({"x": 1}));
        let key = build_group_key(&doc, &[], &crs()).unwrap();
        assert_eq!(key, "__all__");
    }

    #[test]
    fn null_field_value() {
        let doc = encode(&json!({"v": null}));
        let key = build_group_key(&doc, &keys(&["v"]), &crs()).unwrap();
        assert_eq!(key, "[null]");
    }

    #[test]
    fn float_field() {
        let doc = encode(&json!({"temp": 36.6}));
        let key = build_group_key(&doc, &keys(&["temp"]), &crs()).unwrap();
        assert_eq!(key, "[36.6]");
    }

    /// Instant group values retain their kind across indexed and direct keys.
    #[test]
    fn instant_field_keys_retain_kind() {
        use nodedb_types::InstantKind;
        let mut doc = Vec::new();
        crate::msgpack_scan::write_map_header(&mut doc, 2);
        crate::msgpack_scan::write_kv_instant(
            &mut doc,
            "ts",
            InstantKind::Naive,
            1_583_402_400_000_000,
        );
        crate::msgpack_scan::write_kv_i64(&mut doc, "n", 1);

        let key = build_group_key(&doc, &keys(&["ts", "n"]), &crs()).unwrap();
        assert_eq!(
            decode_group_key(&key).unwrap(),
            vec![
                Value::NaiveDateTime(nodedb_types::NdbDateTime::from_micros(
                    1_583_402_400_000_000
                )),
                Value::Integer(1),
            ]
        );
        let idx = FieldIndex::build(&doc, 0).unwrap_or_else(FieldIndex::empty);
        assert_eq!(
            build_group_key_indexed(&doc, &keys(&["ts", "n"]), &idx, &crs()).unwrap(),
            key
        );
    }

    /// A computed group key evaluates its expression and folds the result into
    /// the key slot; `alpha` and `ALPHA` collapse under `UPPER(label)`.
    fn upper_label_spec() -> GroupKeySpec {
        GroupKeySpec {
            output_name: "u".to_string(),
            field: None,
            expr: Some(SqlExpr::Function {
                name: "upper".to_string(),
                args: vec![SqlExpr::Column("label".to_string())],
            }),
        }
    }

    #[test]
    fn computed_key_folds_expression_value() {
        let lower = encode(&json!({"label": "alpha", "score": 7}));
        let upper = encode(&json!({"label": "ALPHA", "score": 3}));
        assert_eq!(
            build_group_key(&lower, &[upper_label_spec()], &crs()).unwrap(),
            r#"["ALPHA"]"#
        );
        assert_eq!(
            build_group_key(&upper, &[upper_label_spec()], &crs()).unwrap(),
            r#"["ALPHA"]"#
        );
    }

    /// A computed GROUP BY key that divides by zero fails the statement with a
    /// division-by-zero error rather than grouping the row under a `null`
    /// key.
    #[test]
    fn computed_key_division_by_zero_errors() {
        let doc = encode(&json!({"n": 10, "d": 0}));
        let spec = GroupKeySpec {
            output_name: "q".to_string(),
            field: None,
            expr: Some(SqlExpr::BinaryOp {
                left: Box::new(SqlExpr::Column("n".to_string())),
                op: crate::expr::BinaryOp::Div,
                right: Box::new(SqlExpr::Column("d".to_string())),
            }),
        };
        assert_eq!(
            build_group_key(&doc, std::slice::from_ref(&spec), &crs()),
            Err(EvalError::DivisionByZero)
        );
        let idx = FieldIndex::build(&doc, 0).unwrap_or_else(FieldIndex::empty);
        assert_eq!(
            build_group_key_indexed(&doc, std::slice::from_ref(&spec), &idx, &crs()),
            Err(EvalError::DivisionByZero)
        );
    }

    #[test]
    fn computed_key_indexed_matches_unindexed() {
        let doc = encode(&json!({"label": "beta", "score": 5}));
        let specs = [upper_label_spec()];
        let idx = FieldIndex::build(&doc, 0).unwrap_or_else(FieldIndex::empty);
        assert_eq!(
            build_group_key(&doc, &specs, &crs()).unwrap(),
            build_group_key_indexed(&doc, &specs, &idx, &crs()).unwrap(),
        );
    }

    #[test]
    fn mixed_column_and_computed_keys() {
        let doc = encode(&json!({"region": "us", "label": "west"}));
        let specs = [GroupKeySpec::column("region"), upper_label_spec()];
        assert_eq!(
            build_group_key(&doc, &specs, &crs()).unwrap(),
            r#"["us","WEST"]"#
        );
    }
}

#[cfg(test)]
#[path = "group_native_tests.rs"]
mod native_tests;
