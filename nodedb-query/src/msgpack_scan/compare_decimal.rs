// SPDX-License-Identifier: Apache-2.0

//! Numeric comparison for a `DECIMAL` column's MessagePack cells.
//!
//! MessagePack has no decimal type. A `DECIMAL` cell is the string of its
//! canonical text, except an integral one above `i64::MAX`, which is a
//! `uint64`. One column therefore holds strings and integers side by side.
//! [`compare_field_bytes`] ranks numbers before strings and orders strings
//! by their bytes, so `5` sorts after `9223372036854775808` and `10` before
//! `9`. A caller that knows the column is `DECIMAL` compares through here.

use std::cmp::Ordering;
use std::str::FromStr;

use rust_decimal::Decimal;

use super::compare::compare_field_bytes;
use super::reader::{read_f64, read_integer, read_str};

/// The decimal a MessagePack cell at `offset` stands for: a decimal string,
/// an integer, or a finite float. `None` for any other cell.
pub fn decimal_reading(buf: &[u8], offset: usize) -> Option<Decimal> {
    if let Some(text) = read_str(buf, offset) {
        return Decimal::from_str(text.trim())
            .or_else(|_| Decimal::from_scientific(text.trim()))
            .ok();
    }
    if let Some(integer) = read_integer(buf, offset) {
        return Decimal::try_from_i128_with_scale(integer, 0).ok();
    }
    read_f64(buf, offset).and_then(|float| Decimal::try_from(float).ok())
}

/// Order two `DECIMAL` cells by the numbers they stand for.
///
/// A pair where either cell has no decimal reading falls back to
/// [`compare_field_bytes`], so a non-numeric cell keeps its usual rank.
pub fn compare_decimal_field_bytes(
    a_buf: &[u8],
    a_range: (usize, usize),
    b_buf: &[u8],
    b_range: (usize, usize),
) -> Ordering {
    match (
        decimal_reading(a_buf, a_range.0),
        decimal_reading(b_buf, b_range.0),
    ) {
        (Some(a), Some(b)) => a.cmp(&b),
        _ => compare_field_bytes(a_buf, a_range, b_buf, b_range),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cell(value: &serde_json::Value) -> Vec<u8> {
        nodedb_types::json_to_msgpack(value).expect("encode cell")
    }

    fn cmp(a: &serde_json::Value, b: &serde_json::Value) -> Ordering {
        let (a, b) = (cell(a), cell(b));
        compare_decimal_field_bytes(&a, (0, a.len()), &b, (0, b.len()))
    }

    /// A small decimal stored as text orders below a `uint64` one.
    #[test]
    fn text_and_uint64_cells_order_numerically() {
        let small = serde_json::json!("5");
        let mid = serde_json::json!(9_223_372_036_854_775_808_u64);
        let max = serde_json::json!(u64::MAX);
        assert_eq!(cmp(&small, &mid), Ordering::Less);
        assert_eq!(cmp(&mid, &max), Ordering::Less);
        assert_eq!(cmp(&max, &small), Ordering::Greater);
    }

    /// Two text cells order by value, not by their bytes.
    #[test]
    fn text_cells_order_by_value() {
        assert_eq!(
            cmp(&serde_json::json!("10"), &serde_json::json!("9.5")),
            Ordering::Greater
        );
        assert_eq!(
            cmp(&serde_json::json!("-2.50"), &serde_json::json!("-2.5")),
            Ordering::Equal
        );
    }

    /// A cell with no decimal reading keeps the generic rank.
    #[test]
    fn non_numeric_cell_falls_back() {
        assert_eq!(
            cmp(&serde_json::json!("abc"), &serde_json::json!("5")),
            Ordering::Greater
        );
    }
}
