// SPDX-License-Identifier: BUSL-1.1

//! ORDER BY for columnar rows: sort-key evaluation and comparison.

use nodedb_physical::physical_plan::SortKeySpec;
use nodedb_types::value::Value;

/// Evaluate every sort key for one row.
///
/// A key that names a stored column reads straight out of the row. A computed
/// key (`ORDER BY 100 / weight`) is evaluated against the row, so the sort
/// orders by the value the client asked for instead of dropping the key and
/// answering in scan order. A zero divisor there fails the statement with
/// SQLSTATE `22012`, exactly as it does in the projection.
pub(in crate::data::executor) fn eval_row_sort_values(
    row: &[Value],
    schema: &nodedb_types::columnar::ColumnarSchema,
    sort_keys: &[SortKeySpec],
) -> crate::Result<Vec<Value>> {
    let mut out = Vec::with_capacity(sort_keys.len());
    let mut row_object: Option<Value> = None;

    for key in sort_keys {
        if let Some(field) = key.as_column() {
            let value = schema
                .columns
                .iter()
                .position(|c| c.name == field)
                .and_then(|idx| row.get(idx).cloned())
                .unwrap_or(Value::Null);
            out.push(value);
            continue;
        }

        // Build the row object once, and only when a computed key needs it.
        let object = match row_object {
            Some(ref o) => o,
            None => row_object.insert(row_to_object(row, schema)),
        };
        out.push(key.expr.eval(object).map_err(crate::Error::from)?);
    }
    Ok(out)
}

fn row_to_object(row: &[Value], schema: &nodedb_types::columnar::ColumnarSchema) -> Value {
    let mut map = std::collections::HashMap::with_capacity(schema.columns.len());
    for (idx, column) in schema.columns.iter().enumerate() {
        map.insert(
            column.name.clone(),
            row.get(idx).cloned().unwrap_or(Value::Null),
        );
    }
    Value::Object(map)
}

/// Compare two rows by their pre-evaluated sort values.
pub(in crate::data::executor) fn compare_sort_values(
    a: &[Value],
    b: &[Value],
    sort_keys: &[SortKeySpec],
) -> std::cmp::Ordering {
    for (idx, key) in sort_keys.iter().enumerate() {
        let av = a.get(idx).unwrap_or(&Value::Null);
        let bv = b.get(idx).unwrap_or(&Value::Null);
        // NULL placement comes from the key's NULLS FIRST/LAST setting and is
        // never reversed by the sort direction.
        let ord = match key.order_nulls(matches!(av, Value::Null), matches!(bv, Value::Null)) {
            Some(ord) => ord,
            None => key.direct(compare_values(av, bv)),
        };
        if ord != std::cmp::Ordering::Equal {
            return ord;
        }
    }
    std::cmp::Ordering::Equal
}

/// Compare two non-NULL values. NULL handling lives in the caller, which
/// applies the key's NULLS FIRST/LAST setting before reaching here.
fn compare_values(a: &Value, b: &Value) -> std::cmp::Ordering {
    match (a, b) {
        (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
        (Value::Null, _) => std::cmp::Ordering::Greater,
        (_, Value::Null) => std::cmp::Ordering::Less,
        (Value::Integer(x), Value::Integer(y)) => x.cmp(y),
        (Value::Float(x), Value::Float(y)) => x.partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal),
        // Exact: an integer or integral decimal never rounds through f64.
        (
            Value::Integer(_) | Value::Float(_) | Value::Decimal(_),
            Value::Integer(_) | Value::Float(_) | Value::Decimal(_),
        ) => nodedb_query::value_ops::numeric_order(a, b)
            .flatten()
            .unwrap_or(std::cmp::Ordering::Equal),
        (Value::String(x), Value::String(y)) => x.cmp(y),
        (Value::Bool(x), Value::Bool(y)) => x.cmp(y),
        (Value::DateTime(x), Value::DateTime(y))
        | (Value::NaiveDateTime(x), Value::NaiveDateTime(y))
        | (Value::DateTime(x), Value::NaiveDateTime(y))
        | (Value::NaiveDateTime(x), Value::DateTime(y)) => x.unix_millis().cmp(&y.unix_millis()),
        // Fallback: compare debug-formatted forms so the sort is
        // deterministic for exotic types that happen to coincide in a
        // sort key column.
        _ => format!("{a:?}").cmp(&format!("{b:?}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cmp::Ordering;

    #[test]
    fn numbers_compare_exactly_across_types() {
        let above = Value::Integer(9_007_199_254_740_993);
        let float = Value::Float(9_007_199_254_740_992.0);
        assert_eq!(compare_values(&above, &float), Ordering::Greater);
        assert_eq!(compare_values(&float, &above), Ordering::Less);
        let u_max = Value::from_u64(u64::MAX);
        assert_eq!(
            compare_values(&u_max, &Value::Integer(i64::MAX)),
            Ordering::Greater
        );
        assert_eq!(
            compare_values(&Value::from_u64(u64::MAX - 1), &u_max),
            Ordering::Less
        );
        assert_eq!(
            compare_values(&Value::Integer(2), &Value::Float(2.5)),
            Ordering::Less
        );
    }
}
