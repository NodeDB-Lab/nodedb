// SPDX-License-Identifier: Apache-2.0

//! A Decimal literal in a `WHERE` on an undeclared field compares exactly
//! against a stored Decimal, Float and Integer, in a native object body and
//! in a plain MessagePack map body.

use std::collections::HashMap;

use nodedb_crs::CrsContext;
use nodedb_query::scan_filter::{FilterOp, ScanFilter};
use nodedb_types::Value;

fn decimal(text: &str) -> Value {
    Value::Decimal(text.parse().expect("decimal literal"))
}

fn filter(op: FilterOp, value: Value) -> ScanFilter {
    ScanFilter {
        field: "price".into(),
        op,
        value,
        clauses: Vec::new(),
        expr: None,
    }
}

/// A native object body `{price: value}`.
fn native(value: Value) -> Vec<u8> {
    let fields = HashMap::from([("price".to_string(), value)]);
    zerompk::to_msgpack_vec(&Value::Object(fields)).expect("native body")
}

/// A plain MessagePack map body `{price: value}`.
fn plain(value: &serde_json::Value) -> Vec<u8> {
    nodedb_types::json_msgpack::json_to_msgpack(&serde_json::json!({ "price": value }))
        .expect("plain body")
}

fn matches(filter: &ScanFilter, body: &[u8]) -> bool {
    filter
        .matches_binary(body, &CrsContext::builtin_only())
        .expect("filter evaluates")
}

#[test]
fn a_decimal_literal_equals_every_numeric_kind_by_value() {
    let eq = filter(FilterOp::Eq, decimal("12.50"));
    assert!(matches(&eq, &native(decimal("12.50"))), "Decimal 12.50");
    assert!(matches(&eq, &native(decimal("12.5"))), "Decimal 12.5");
    assert!(matches(&eq, &native(Value::Float(12.5))), "Float 12.5");
    assert!(!matches(&eq, &native(Value::Integer(12))), "Integer 12");
    assert!(!matches(&eq, &native(decimal("3.00"))), "Decimal 3.00");
    assert!(
        matches(&eq, &plain(&serde_json::json!(12.5))),
        "plain float"
    );
    assert!(
        !matches(&eq, &plain(&serde_json::json!(12))),
        "plain integer"
    );

    let whole = filter(FilterOp::Eq, decimal("12.00"));
    assert!(matches(&whole, &native(Value::Integer(12))), "Integer 12");
    assert!(matches(&whole, &plain(&serde_json::json!(12))), "plain 12");
}

#[test]
fn a_decimal_literal_orders_every_numeric_kind_exactly() {
    let gt = filter(FilterOp::Gt, decimal("12.49"));
    assert!(matches(&gt, &native(decimal("12.50"))));
    assert!(matches(&gt, &native(Value::Float(12.5))));
    assert!(!matches(&gt, &native(Value::Integer(12))));
    assert!(matches(&gt, &native(Value::Integer(13))));
    assert!(matches(&gt, &plain(&serde_json::json!(12.5))));
}
