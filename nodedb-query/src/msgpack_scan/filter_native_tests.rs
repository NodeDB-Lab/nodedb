// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::expr::SqlExpr;
use nodedb_types::Value;

fn predicate(field: &str, op: FilterOp, value: Value) -> ScanFilter {
    ScanFilter {
        field: field.to_owned(),
        op,
        value,
        clauses: vec![],
        expr: None,
    }
}

#[test]
fn native_filters_share_typed_value_semantics() {
    let decimal = Value::Decimal("9007199254740993.125".parse().unwrap());
    let row = Value::Object(std::collections::HashMap::from([
        ("v".to_owned(), decimal.clone()),
        ("other".to_owned(), decimal.clone()),
        (
            "array".to_owned(),
            Value::Array(vec![Value::Integer(2), Value::Integer(42)]),
        ),
        ("yes".to_owned(), Value::Bool(true)),
        ("null".to_owned(), Value::Null),
    ]));
    let bytes = zerompk::to_msgpack_vec(&row).unwrap();
    let crs = CrsContext::builtin_only();
    let mut expr = predicate("", FilterOp::Expr, Value::Null);
    expr.expr = Some(SqlExpr::Column("yes".to_owned()));
    for filter in [
        predicate("v", FilterOp::Eq, decimal),
        predicate("v", FilterOp::EqColumn, Value::String("other".to_owned())),
        predicate("array", FilterOp::ArrayContains, Value::Integer(42)),
        predicate("null", FilterOp::IsNull, Value::Null),
        predicate("missing", FilterOp::IsNull, Value::Null),
        expr,
    ] {
        let expected = filter.matches_value(&row, &crs).unwrap();
        assert!(expected);
        assert_eq!(filter.matches_binary(&bytes, &crs).unwrap(), expected);
        assert_eq!(
            filter
                .matches_binary_indexed(&bytes, &FieldIndex::empty(), &crs)
                .unwrap(),
            expected
        );
    }
}

#[test]
fn plain_tag_shaped_arrays_remain_arrays_in_filters() {
    let bytes = nodedb_types::json_to_msgpack(&serde_json::json!({"v": [2, 42]})).unwrap();
    let filter = predicate("v", FilterOp::ArrayContains, Value::Integer(42));
    assert!(
        filter
            .matches_binary(&bytes, &CrsContext::builtin_only())
            .unwrap()
    );
}

#[test]
fn malformed_native_filter_bodies_return_errors() {
    let bytes = [0x92, 7, 0x81, 0xa1, b'v', 0x91, 2];
    let filter = predicate("v", FilterOp::IsNull, Value::Null);
    let crs = CrsContext::builtin_only();
    assert!(filter.matches_binary(&bytes, &crs).is_err());
    assert!(
        filter
            .matches_binary_indexed(&bytes, &FieldIndex::empty(), &crs)
            .is_err()
    );
}
