// SPDX-License-Identifier: BUSL-1.1

//! Native sort-key evaluation and comparison.

use std::str::FromStr;

use nodedb_physical::physical_plan::SortKeySpec;
use nodedb_types::Value;
use nodedb_types::columnar::{ColumnType, StrictSchema};
use rust_decimal::Decimal;

/// One row's evaluated sort keys, positionally aligned with the ORDER BY list.
pub(in crate::data::executor) type SortValues = Vec<Value>;

/// Per ORDER BY key, whether it is a bare column `schema` declares `DECIMAL`.
///
/// A `DECIMAL` cell is text, or a `uint64` past `i64::MAX`, so the generic
/// comparators order such a column by type rank and by bytes. A flagged key
/// compares by value instead. A collection with no strict schema flags none.
pub(in crate::data::executor) fn decimal_sort_keys(
    sort_keys: &[SortKeySpec],
    schema: Option<&StrictSchema>,
) -> Vec<bool> {
    let Some(schema) = schema else {
        return Vec::new();
    };
    sort_keys
        .iter()
        .map(|key| {
            key.as_column().is_some_and(|field| {
                schema.columns.iter().any(|column| {
                    column.name == field && matches!(column.column_type, ColumnType::Decimal { .. })
                })
            })
        })
        .collect()
}

/// Whether key `idx` is flagged `DECIMAL` in `decimal_keys`.
pub(in crate::data::executor) fn is_decimal_key(decimal_keys: &[bool], idx: usize) -> bool {
    decimal_keys.get(idx).copied().unwrap_or(false)
}

/// The decimal an evaluated sort value stands for: a decimal string or a
/// number. `None` for any other value.
pub(in crate::data::executor) fn native_decimal(value: &Value) -> Option<Decimal> {
    match value {
        Value::Decimal(value) => Some(*value),
        Value::Integer(value) => Some(Decimal::from(*value)),
        Value::Float(value) => Decimal::try_from(*value).ok(),
        Value::String(text) => Decimal::from_str(text.trim())
            .or_else(|_| Decimal::from_scientific(text.trim()))
            .ok(),
        _ => None,
    }
}

fn type_rank(value: &Value) -> u8 {
    match value {
        Value::Null => 0,
        Value::Bool(_) => 1,
        Value::Integer(_) | Value::Float(_) | Value::Decimal(_) => 2,
        Value::DateTime(_) => 3,
        Value::NaiveDateTime(_) => 4,
        Value::String(_) | Value::Uuid(_) | Value::Ulid(_) | Value::Regex(_) => 5,
        Value::Bytes(_) => 6,
        Value::Array(_) | Value::Set(_) => 7,
        Value::Object(_) => 8,
        _ => 9,
    }
}

/// Preserve JSON expression ordering for mixed primitive and composite values.
fn expression_rank(value: &Value) -> u8 {
    match value {
        Value::Null => 0,
        Value::String(_) => 1,
        Value::Integer(_) | Value::Float(_) | Value::Decimal(_) => 2,
        Value::Array(_) => 3,
        Value::Bool(false) => 4,
        Value::Bool(true) => 5,
        Value::Object(_) => 6,
        _ => 7,
    }
}

/// Preserve stored-field type ranks and compare numeric values exactly.
fn compare_key_values(a: &Value, b: &Value, decimal: bool, computed: bool) -> std::cmp::Ordering {
    if decimal && let (Some(a), Some(b)) = (native_decimal(a), native_decimal(b)) {
        return a.cmp(&b);
    }
    let rank = if computed { expression_rank } else { type_rank };
    rank(a)
        .cmp(&rank(b))
        .then_with(|| nodedb_query::value_ops::compare_sort_values(a, b))
}

/// Order two stored column values: by type rank, then numbers by their exact
/// reading across integer, float and decimal. Join keys order by this, as
/// `ORDER BY` over a stored column does.
pub(in crate::data::executor) fn compare_column_values(a: &Value, b: &Value) -> std::cmp::Ordering {
    compare_key_values(a, b, false, false)
}

/// Compare provider expressions with the existing expression sort order.
pub(in crate::data::executor) fn compare_expression_values(
    a: &Value,
    b: &Value,
) -> std::cmp::Ordering {
    compare_key_values(a, b, false, true)
}

/// Whether every key names a stored column.
pub(in crate::data::executor) fn all_column_keys(sort_keys: &[SortKeySpec]) -> bool {
    sort_keys.iter().all(|k| k.as_column().is_some())
}

/// Evaluate every sort key against one raw msgpack document.
///
/// A document that does not decode is an error, as is an expression
/// evaluation error. Sorting the document under NULL keys would place it by
/// values it does not hold.
pub(in crate::data::executor) fn eval_sort_values(
    doc_bytes: &[u8],
    sort_keys: &[SortKeySpec],
    crs: &nodedb_crs::CrsContext<'_>,
) -> crate::Result<SortValues> {
    let doc = crate::data::executor::doc_format::decode_document_value(doc_bytes).map_err(|e| {
        crate::Error::Codec {
            detail: format!("ORDER BY document decode: {e}"),
        }
    })?;

    sort_keys
        .iter()
        .map(|k| k.expr.eval(&doc, crs).map_err(crate::Error::from))
        .collect()
}

/// Compare two rows by their pre-evaluated sort values. `decimal_keys` flags
/// the keys [`decimal_sort_keys`] found `DECIMAL`.
pub(in crate::data::executor) fn compare_sort_values(
    a: &[Value],
    b: &[Value],
    sort_keys: &[SortKeySpec],
    decimal_keys: &[bool],
) -> std::cmp::Ordering {
    for (idx, key) in sort_keys.iter().enumerate() {
        let (Some(va), Some(vb)) = (a.get(idx), b.get(idx)) else {
            continue;
        };
        // NULL placement is decided by the key's NULLS FIRST/LAST setting and
        // is never flipped by the sort direction.
        let ord = match key.order_nulls(va.is_null(), vb.is_null()) {
            Some(ord) => ord,
            None => key.direct(compare_key_values(
                va,
                vb,
                is_decimal_key(decimal_keys, idx),
                !all_column_keys(sort_keys),
            )),
        };
        if ord != std::cmp::Ordering::Equal {
            return ord;
        }
    }
    std::cmp::Ordering::Equal
}

/// Encode evaluated sort values for a spill run record.
pub(in crate::data::executor) fn encode_sort_values(values: &SortValues) -> crate::Result<Vec<u8>> {
    zerompk::to_msgpack_vec(values).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("sort key values: {e}"),
    })
}

/// Decode evaluated sort values read back from a spill run record.
pub(in crate::data::executor) fn decode_sort_values(bytes: &[u8]) -> crate::Result<SortValues> {
    zerompk::from_msgpack(bytes).map_err(|e| crate::Error::Storage {
        engine: "sort".into(),
        detail: format!("sort run corrupt: key values undecodable: {e}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn evaluated_and_spilled_keys_retain_native_values() {
        let values = vec![
            Value::Decimal("123456789.123456789".parse().expect("decimal")),
            Value::Uuid("550e8400-e29b-41d4-a716-446655440000".into()),
            Value::Bytes(vec![0, 255]),
            Value::Array(vec![Value::Decimal(Decimal::MAX)]),
        ];
        let fields = values
            .iter()
            .enumerate()
            .map(|(index, value)| (format!("key{index}"), value.clone()))
            .collect();
        let doc = zerompk::to_msgpack_vec(&Value::Object(fields)).expect("native document");
        let keys = (0..values.len())
            .map(|index| SortKeySpec::column(format!("key{index}"), true))
            .collect::<Vec<_>>();
        let evaluated = eval_sort_values(&doc, &keys, &nodedb_crs::CrsContext::builtin_only())
            .expect("native keys");
        assert_eq!(evaluated, values);
        let encoded = encode_sort_values(&evaluated).expect("encode keys");
        assert_eq!(decode_sort_values(&encoded).expect("decode keys"), values);
    }

    #[test]
    fn malformed_spill_keys_return_storage_error() {
        assert!(decode_sort_values(&[]).is_err());
        assert!(decode_sort_values(&[0x91]).is_err());
    }
}
