// SPDX-License-Identifier: Apache-2.0

//! Evaluation of `MetadataFilter` (nodedb-types) against a document.
//!
//! One evaluator serves every document shape: a field lookup closure
//! resolves top-level fields. Adapters cover JSON documents (shape sync,
//! vector post-filter), field maps (Lite edge properties) and plain-msgpack
//! maps (Origin edge properties).
//!
//! Semantics:
//! - `Eq` on a missing field matches only a `Null` value. `Ne` negates `Eq`.
//! - `Gt`/`Gte`/`Lt`/`Lte` are false when the field is missing or null, the
//!   value is null, or the pair has no order.
//! - `In` on a missing field is false. `NotIn` on a missing field is true.
//! - `And([])` is true. `Or([])` is false.
//!
//! Coercion, one rule for equality and order:
//! - Numeric: numbers, decimals, numeric strings and bools (`true` = 1)
//!   compare as numbers. Integer pairs compare exactly.
//! - Instant: a typed instant compares with an instant or an ISO-8601 string
//!   by epoch microseconds.
//! - Text: strings, UUIDs and ULIDs order lexicographically.
//! - Any other pair of different kinds is unequal and has no order. So
//!   `{"score": "n/a"}` fails `score > 5`, and an array fails every order.

use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::convert::Infallible;

use nodedb_types::Value;
use nodedb_types::filter::MetadataFilter;

use crate::msgpack_scan::FieldIndex;
use crate::msgpack_scan::reader::read_value;
use crate::value_ops::{coerced_eq, involves_instant, numeric_order};

/// A plain-msgpack property map the evaluator cannot read.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PropertyMapError {
    /// The bytes are not one well-formed MessagePack map.
    #[error("property bytes are not a well-formed MessagePack map")]
    NotAMap,
    /// The map holds `field`, but its value does not decode.
    #[error("property field '{field}' does not decode")]
    Field { field: String },
}

/// Evaluate `filter` against a document whose top-level fields `lookup` resolves.
pub fn matches_metadata_filter_with<'a, F>(lookup: &F, filter: &MetadataFilter) -> bool
where
    F: Fn(&str) -> Option<Cow<'a, Value>>,
{
    let infallible = |name: &str| Ok::<_, Infallible>(lookup(name));
    match try_matches_metadata_filter_with(&infallible, filter) {
        Ok(admitted) => admitted,
        Err(never) => match never {},
    }
}

/// Evaluate `filter` against a document whose top-level fields `lookup`
/// resolves. A lookup error stops evaluation and returns that error.
pub fn try_matches_metadata_filter_with<'a, F, E>(
    lookup: &F,
    filter: &MetadataFilter,
) -> Result<bool, E>
where
    F: Fn(&str) -> Result<Option<Cow<'a, Value>>, E>,
{
    Ok(match filter {
        MetadataFilter::Eq { field, value } => equals(lookup(field)?.as_deref(), value),
        MetadataFilter::Ne { field, value } => !equals(lookup(field)?.as_deref(), value),
        MetadataFilter::Gt { field, value } => {
            ordered(lookup(field)?.as_deref(), value, |o| o == Ordering::Greater)
        }
        MetadataFilter::Gte { field, value } => {
            ordered(lookup(field)?.as_deref(), value, |o| o != Ordering::Less)
        }
        MetadataFilter::Lt { field, value } => {
            ordered(lookup(field)?.as_deref(), value, |o| o == Ordering::Less)
        }
        MetadataFilter::Lte { field, value } => {
            ordered(lookup(field)?.as_deref(), value, |o| o != Ordering::Greater)
        }
        MetadataFilter::In { field, values } => {
            lookup(field)?.is_some_and(|found| values.iter().any(|v| filter_eq(&found, v)))
        }
        MetadataFilter::NotIn { field, values } => {
            lookup(field)?.is_none_or(|found| !values.iter().any(|v| filter_eq(&found, v)))
        }
        MetadataFilter::And(children) => {
            for child in children {
                if !try_matches_metadata_filter_with(lookup, child)? {
                    return Ok(false);
                }
            }
            true
        }
        MetadataFilter::Or(children) => {
            for child in children {
                if try_matches_metadata_filter_with(lookup, child)? {
                    return Ok(true);
                }
            }
            false
        }
        MetadataFilter::Not(inner) => !try_matches_metadata_filter_with(lookup, inner)?,
        // `MetadataFilter` is `#[non_exhaustive]`: a variant this evaluator
        // does not know admits nothing.
        _ => false,
    })
}

fn equals(found: Option<&Value>, value: &Value) -> bool {
    match found {
        Some(found) => filter_eq(found, value),
        None => value.is_null(),
    }
}

/// Equality under the module's coercion rule: [`coerced_eq`], plus text
/// kinds equal by their text.
fn filter_eq(a: &Value, b: &Value) -> bool {
    coerced_eq(a, b) || matches!((text(a), text(b)), (Some(x), Some(y)) if x == y)
}

fn ordered(found: Option<&Value>, value: &Value, accept: impl Fn(Ordering) -> bool) -> bool {
    match found {
        Some(found) if !found.is_null() && !value.is_null() => {
            filter_order(found, value).is_some_and(accept)
        }
        _ => false,
    }
}

/// Order under the module's coercion rule. `None` for a pair of different
/// kinds outside the coercions, and for a NaN.
fn filter_order(a: &Value, b: &Value) -> Option<Ordering> {
    if involves_instant(a, b) {
        return a.partial_cmp_coerced(b);
    }
    if let Some(order) = numeric_order(a, b) {
        return order;
    }
    match (text(a), text(b)) {
        (Some(x), Some(y)) => Some(x.cmp(y)),
        _ => None,
    }
}

/// The text of a string-kind value: a string, UUID or ULID.
fn text(v: &Value) -> Option<&str> {
    match v {
        Value::String(s) | Value::Uuid(s) | Value::Ulid(s) => Some(s),
        _ => None,
    }
}

/// `filter` against a field map (Lite edge properties).
pub fn matches_metadata_fields(fields: &HashMap<String, Value>, filter: &MetadataFilter) -> bool {
    matches_metadata_filter_with(&|name: &str| fields.get(name).map(Cow::Borrowed), filter)
}

/// Every filter of `filters` against a field map. An empty list admits.
pub fn matches_all_fields(fields: &HashMap<String, Value>, filters: &[MetadataFilter]) -> bool {
    filters.iter().all(|f| matches_metadata_fields(fields, f))
}

/// Every filter of `filters` against a plain-msgpack map. Decodes only the
/// fields a filter names. Empty bytes evaluate as `{}`. An empty filter list
/// admits without reading `doc`.
///
/// Bytes that are not one well-formed map, and a named field whose value
/// does not decode, are an error: a property map the predicate cannot read
/// is not a map the predicate admitted.
pub fn matches_all_msgpack(
    doc: &[u8],
    filters: &[MetadataFilter],
) -> Result<bool, PropertyMapError> {
    if filters.is_empty() {
        return Ok(true);
    }
    let index = if doc.is_empty() {
        FieldIndex::empty()
    } else {
        FieldIndex::build(doc, 0).ok_or(PropertyMapError::NotAMap)?
    };
    let lookup = |name: &str| -> Result<Option<Cow<'static, Value>>, PropertyMapError> {
        let Some((start, end)) = index.get(name) else {
            return Ok(None);
        };
        let decoded = read_value(doc, start).or_else(|| {
            doc.get(start..end)
                .and_then(|bytes| nodedb_types::json_msgpack::value_from_msgpack(bytes).ok())
        });
        match decoded {
            Some(value) => Ok(Some(Cow::Owned(value))),
            None => Err(PropertyMapError::Field {
                field: name.to_owned(),
            }),
        }
    };
    for filter in filters {
        if !try_matches_metadata_filter_with(&lookup, filter)? {
            return Ok(false);
        }
    }
    Ok(true)
}

/// `filter` against a JSON document (shape-sync predicates, vector post-filter).
pub fn matches_metadata_filter(doc: &serde_json::Value, filter: &MetadataFilter) -> bool {
    let lookup = |name: &str| doc.get(name).map(|v| Cow::Owned(Value::from(v.clone())));
    matches_metadata_filter_with(&lookup, filter)
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::filter::MetadataFilter;
    use nodedb_types::value::Value;
    use serde_json::json;

    fn gt(field: &str, value: Value) -> MetadataFilter {
        MetadataFilter::Gt {
            field: field.into(),
            value,
        }
    }

    fn lt(field: &str, value: Value) -> MetadataFilter {
        MetadataFilter::Lt {
            field: field.into(),
            value,
        }
    }

    fn ne(field: &str, value: Value) -> MetadataFilter {
        MetadataFilter::Ne {
            field: field.into(),
            value,
        }
    }

    #[test]
    fn eq_match() {
        let doc = json!({"status": "active", "age": 25});
        let filter = MetadataFilter::eq("status", "active");
        assert!(matches_metadata_filter(&doc, &filter));
    }

    #[test]
    fn eq_no_match() {
        let doc = json!({"status": "inactive"});
        let filter = MetadataFilter::eq("status", "active");
        assert!(!matches_metadata_filter(&doc, &filter));
    }

    #[test]
    fn gt_numeric() {
        let doc = json!({"age": 30});
        assert!(matches_metadata_filter(
            &doc,
            &gt("age", Value::Integer(25))
        ));
    }

    #[test]
    fn and_filter() {
        let doc = json!({"status": "active", "age": 30});
        let filter = MetadataFilter::and(vec![
            MetadataFilter::eq("status", "active"),
            gt("age", Value::Integer(25)),
        ]);
        assert!(matches_metadata_filter(&doc, &filter));
    }

    #[test]
    fn or_filter() {
        let doc = json!({"status": "inactive", "age": 30});
        let filter = MetadataFilter::or(vec![
            MetadataFilter::eq("status", "active"),
            gt("age", Value::Integer(25)),
        ]);
        assert!(matches_metadata_filter(&doc, &filter));
    }

    #[test]
    fn not_filter() {
        let doc = json!({"status": "active"});
        let filter = MetadataFilter::Not(Box::new(MetadataFilter::eq("status", "inactive")));
        assert!(matches_metadata_filter(&doc, &filter));
    }

    #[test]
    fn in_filter() {
        let doc = json!({"role": "admin"});
        let filter = MetadataFilter::In {
            field: "role".into(),
            values: vec![Value::from("admin"), Value::from("superadmin")],
        };
        assert!(matches_metadata_filter(&doc, &filter));
    }

    #[test]
    fn missing_field() {
        let doc = json!({"name": "Alice"});
        let filter = MetadataFilter::eq("status", "active");
        assert!(!matches_metadata_filter(&doc, &filter));
    }

    #[test]
    fn eq_and_ne_on_missing_field_and_null() {
        let doc = json!({"present": null});
        assert!(matches_metadata_filter(
            &doc,
            &MetadataFilter::eq("absent", Value::Null)
        ));
        assert!(matches_metadata_filter(
            &doc,
            &MetadataFilter::eq("present", Value::Null)
        ));
        assert!(!matches_metadata_filter(&doc, &ne("absent", Value::Null)));
        assert!(matches_metadata_filter(
            &doc,
            &ne("absent", Value::from("x"))
        ));
        assert!(!matches_metadata_filter(
            &doc,
            &MetadataFilter::eq("absent", "x")
        ));
    }

    #[test]
    fn eq_coerces_numeric_strings_and_bools() {
        let doc = json!({"n": "5", "flag": true});
        assert!(matches_metadata_filter(
            &doc,
            &MetadataFilter::eq("n", Value::Integer(5))
        ));
        assert!(matches_metadata_filter(
            &doc,
            &MetadataFilter::eq("flag", Value::Integer(1))
        ));
    }

    #[test]
    fn ordered_comparisons_reject_missing_and_null() {
        let doc = json!({"score": null});
        for filter in [
            gt("missing", Value::Integer(0)),
            lt("missing", Value::Integer(100)),
            MetadataFilter::Lte {
                field: "missing".into(),
                value: Value::Integer(100),
            },
            MetadataFilter::Gte {
                field: "missing".into(),
                value: Value::Integer(-100),
            },
            lt("score", Value::Integer(100)),
            gt("score", Value::Integer(-100)),
        ] {
            assert!(!matches_metadata_filter(&doc, &filter), "{filter:?}");
        }
        let doc = json!({"score": 3});
        assert!(!matches_metadata_filter(&doc, &lt("score", Value::Null)));
    }

    #[test]
    fn ordered_comparisons_reject_nan() {
        let fields = HashMap::from([("score".to_owned(), Value::Float(f64::NAN))]);
        for filter in [
            gt("score", Value::Integer(0)),
            lt("score", Value::Integer(0)),
            MetadataFilter::Gte {
                field: "score".into(),
                value: Value::Float(f64::NAN),
            },
            MetadataFilter::Lte {
                field: "score".into(),
                value: Value::Integer(0),
            },
        ] {
            assert!(!matches_metadata_fields(&fields, &filter), "{filter:?}");
        }
    }

    #[test]
    fn in_and_not_in_on_missing_field() {
        let doc = json!({});
        let values = vec![Value::from("a")];
        assert!(!matches_metadata_filter(
            &doc,
            &MetadataFilter::In {
                field: "k".into(),
                values: values.clone(),
            }
        ));
        assert!(matches_metadata_filter(
            &doc,
            &MetadataFilter::NotIn {
                field: "k".into(),
                values,
            }
        ));
    }

    #[test]
    fn empty_and_admits_empty_or_rejects() {
        let doc = json!({});
        assert!(matches_metadata_filter(
            &doc,
            &MetadataFilter::And(Vec::new())
        ));
        assert!(!matches_metadata_filter(
            &doc,
            &MetadataFilter::Or(Vec::new())
        ));
    }

    #[test]
    fn nested_not() {
        let doc = json!({"score": 9});
        let filter = MetadataFilter::Not(Box::new(MetadataFilter::Not(Box::new(gt(
            "score",
            Value::Integer(5),
        )))));
        assert!(matches_metadata_filter(&doc, &filter));
        let filter = MetadataFilter::Not(Box::new(MetadataFilter::Or(vec![
            lt("score", Value::Integer(5)),
            MetadataFilter::eq("score", Value::Integer(9)),
        ])));
        assert!(!matches_metadata_filter(&doc, &filter));
    }

    #[test]
    fn msgpack_matches_field_map() {
        let fields = HashMap::from([
            ("score".to_owned(), Value::Integer(9)),
            ("kind".to_owned(), Value::from("road")),
            ("closed".to_owned(), Value::Bool(false)),
        ]);
        let bytes = nodedb_types::json_msgpack::value_to_msgpack(&Value::Object(fields.clone()))
            .expect("encode");
        let cases = [
            vec![gt("score", Value::Integer(5))],
            vec![lt("score", Value::Integer(5))],
            vec![
                MetadataFilter::In {
                    field: "kind".into(),
                    values: vec![Value::from("road"), Value::from("rail")],
                },
                MetadataFilter::Not(Box::new(MetadataFilter::eq("closed", true))),
            ],
            vec![MetadataFilter::eq("missing", Value::Null)],
            vec![lt("missing", Value::Integer(5))],
            Vec::new(),
        ];
        for filters in cases {
            assert_eq!(
                matches_all_msgpack(&bytes, &filters),
                Ok(matches_all_fields(&fields, &filters)),
                "{filters:?}"
            );
        }
    }

    #[test]
    fn empty_msgpack_evaluates_as_empty_object() {
        assert_eq!(
            matches_all_msgpack(&[], &[MetadataFilter::eq("missing", Value::Null)]),
            Ok(true)
        );
        assert_eq!(
            matches_all_msgpack(&[], &[lt("score", Value::Integer(5))]),
            Ok(false)
        );
        assert_eq!(matches_all_msgpack(&[], &[]), Ok(true));
    }

    /// `{"score": 9}` with the value's tag replaced by the reserved `0xc1`.
    fn corrupt_score_map() -> Vec<u8> {
        let mut bytes =
            nodedb_types::json_msgpack::json_to_msgpack(&json!({"score": 9})).expect("encode");
        let last = bytes.len() - 1;
        bytes[last] = 0xc1;
        bytes
    }

    #[test]
    fn a_named_field_that_does_not_decode_is_an_error() {
        let bytes = corrupt_score_map();
        let result = matches_all_msgpack(&bytes, &[gt("score", Value::Integer(5))]);
        assert!(result.is_err(), "{result:?}");
        assert_eq!(
            matches_all_msgpack(&bytes, &[]),
            Ok(true),
            "no filter reads nothing"
        );
    }

    #[test]
    fn bytes_that_are_not_a_map_are_an_error() {
        let list = nodedb_types::json_msgpack::json_to_msgpack(&json!([1, 2])).expect("encode");
        assert_eq!(
            matches_all_msgpack(&list, &[MetadataFilter::eq("k", Value::Null)]),
            Err(PropertyMapError::NotAMap)
        );
        // A map header that claims more entries than the bytes hold.
        assert_eq!(
            matches_all_msgpack(&[0x82, 0xa1, b'k', 0x01], &[gt("k", Value::Integer(0))]),
            Err(PropertyMapError::NotAMap)
        );
    }

    #[test]
    fn integers_past_two_pow_53_filter_exactly() {
        let fields = HashMap::from([("ts".to_owned(), Value::Integer(9_007_199_254_740_993))]);
        let at = Value::Integer(9_007_199_254_740_992);
        assert!(!matches_metadata_fields(
            &fields,
            &MetadataFilter::eq("ts", at.clone())
        ));
        assert!(matches_metadata_fields(&fields, &gt("ts", at.clone())));
        assert!(!matches_metadata_fields(&fields, &lt("ts", at)));
    }

    /// Each pair of different kinds outside the coercions has no order.
    #[test]
    fn ordered_comparisons_across_kinds_are_unordered() {
        let fields = HashMap::from([
            ("text".to_owned(), Value::from("n/a")),
            ("list".to_owned(), Value::Array(vec![Value::Integer(9)])),
            ("num".to_owned(), Value::Integer(9)),
            (
                "obj".to_owned(),
                Value::Object(HashMap::from([("a".to_owned(), Value::Integer(1))])),
            ),
            ("flag".to_owned(), Value::Bool(true)),
        ]);
        let unordered = [
            ("text", Value::Integer(5)),
            ("list", Value::Integer(5)),
            ("num", Value::from("abc")),
            ("num", Value::Array(vec![Value::Integer(1)])),
            ("obj", Value::Integer(0)),
            ("obj", Value::from("a")),
            ("list", Value::from("a")),
            ("flag", Value::from("yes")),
        ];
        for (field, value) in unordered {
            for filter in [
                gt(field, value.clone()),
                lt(field, value.clone()),
                MetadataFilter::Gte {
                    field: field.into(),
                    value: value.clone(),
                },
                MetadataFilter::Lte {
                    field: field.into(),
                    value: value.clone(),
                },
            ] {
                assert!(!matches_metadata_fields(&fields, &filter), "{filter:?}");
            }
        }
    }

    /// The coercions order the same pairs that compare equal.
    #[test]
    fn coerced_kinds_order_and_compare_alike() {
        let fields = HashMap::from([
            ("n".to_owned(), Value::from("10")),
            ("flag".to_owned(), Value::Bool(true)),
            ("name".to_owned(), Value::from("bob")),
            (
                "id".to_owned(),
                Value::Uuid("550e8400-e29b-41d4-a716-446655440000".into()),
            ),
        ]);
        assert!(matches_metadata_fields(
            &fields,
            &gt("n", Value::Integer(9))
        ));
        assert!(matches_metadata_fields(
            &fields,
            &MetadataFilter::eq("n", Value::Integer(10))
        ));
        assert!(matches_metadata_fields(
            &fields,
            &gt("flag", Value::Integer(0))
        ));
        assert!(matches_metadata_fields(
            &fields,
            &MetadataFilter::eq("flag", Value::Integer(1))
        ));
        assert!(matches_metadata_fields(
            &fields,
            &gt("name", Value::from("alice"))
        ));
        assert!(matches_metadata_fields(
            &fields,
            &lt("name", Value::from("carol"))
        ));
        let id = Value::from("550e8400-e29b-41d4-a716-446655440000");
        assert!(matches_metadata_fields(
            &fields,
            &MetadataFilter::eq("id", id.clone())
        ));
        assert!(matches_metadata_fields(
            &fields,
            &MetadataFilter::Gte {
                field: "id".into(),
                value: id,
            }
        ));
    }

    #[test]
    fn a_non_numeric_string_fails_a_numeric_order_over_msgpack() {
        let bytes =
            nodedb_types::json_msgpack::json_to_msgpack(&json!({"score": "n/a"})).expect("encode");
        assert_eq!(
            matches_all_msgpack(&bytes, &[gt("score", Value::Integer(5))]),
            Ok(false)
        );
        assert_eq!(
            matches_all_msgpack(&bytes, &[lt("score", Value::Integer(5))]),
            Ok(false)
        );
    }
}
