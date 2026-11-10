// SPDX-License-Identifier: BUSL-1.1

//! The counter value an `INCR_FLOAT` stores once a declared column rule has
//! fitted the image it computed.

use nodedb_query::msgpack_scan::{KvBodyShape, kv_body_shape, kv_body_to_row};
use nodedb_types::Value;

/// The counter value the image `fitted` stores.
///
/// `value` and `computed` are what `INCR_FLOAT` computed. `fitted` is the
/// image a declared column rule made of `computed`, such as a `DECIMAL(p,s)`
/// column rounding to its scale. A raw body stores the counter as its text.
/// A typed row stores it in the column whose computed cell is `value`, and
/// that column's fitted cell is read as a number. When no cell reads as a
/// number, `value` is the answer: the rule kept it.
pub fn fitted_counter_f64(value: f64, computed: &[u8], fitted: &[u8]) -> f64 {
    if kv_body_shape(fitted) == KvBodyShape::Raw {
        return std::str::from_utf8(fitted)
            .ok()
            .and_then(|text| text.trim().parse::<f64>().ok())
            .unwrap_or(value);
    }
    // Both images decode in their own shape: a plain map or a native row.
    let (Ok((Value::Object(before), _)), Ok((Value::Object(after), _))) =
        (kv_body_to_row(computed), kv_body_to_row(fitted))
    else {
        return value;
    };
    let mut moved: Vec<&String> = before
        .iter()
        .filter(|(_, cell)| matches!(cell, Value::Float(f) if *f == value))
        .map(|(name, _)| name)
        .collect();
    moved.sort();
    moved
        .into_iter()
        .filter_map(|name| after.get(name))
        .find_map(cell_f64)
        .unwrap_or(value)
}

/// A stored numeric cell as `f64`. A decimal column stores its value as text.
fn cell_f64(cell: &Value) -> Option<f64> {
    match cell {
        Value::Float(f) => Some(*f),
        Value::Integer(i) => Some(*i as f64),
        Value::String(text) => text.trim().parse().ok(),
        Value::Decimal(d) => d.to_string().parse().ok(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn row(fields: &[(&str, Value)]) -> Vec<u8> {
        let map: HashMap<String, Value> = fields
            .iter()
            .map(|(name, value)| ((*name).to_string(), value.clone()))
            .collect();
        nodedb_types::value_to_msgpack(&Value::Object(map)).expect("encode row")
    }

    #[test]
    fn raw_body_reads_the_fitted_text() {
        assert_eq!(fitted_counter_f64(1.005, b"1.005", b"1.01"), 1.01);
    }

    #[test]
    fn typed_row_reads_the_fitted_counter_cell() {
        let computed = row(&[
            ("label", Value::String("gold".into())),
            ("price", Value::Float(1.005)),
        ]);
        let fitted = row(&[
            ("label", Value::String("gold".into())),
            ("price", Value::String("1.01".into())),
        ]);
        assert_eq!(fitted_counter_f64(1.005, &computed, &fitted), 1.01);
    }

    #[test]
    fn an_unchanged_counter_keeps_its_value() {
        let image = row(&[("n", Value::Float(2.5))]);
        assert_eq!(fitted_counter_f64(2.5, &image, &image), 2.5);
    }
}
