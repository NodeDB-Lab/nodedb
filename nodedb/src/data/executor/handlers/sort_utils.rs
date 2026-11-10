// SPDX-License-Identifier: BUSL-1.1

//! Shared msgpack-row sorting utilities used by scan handlers.

use nodedb_physical::physical_plan::SortKeySpec;
use nodedb_query::EvalError;
use nodedb_types::Value;

/// Sort msgpack-map rows by the ORDER BY terms. Decodes each row as an object,
/// evaluates each sort expression against it, and reorders the original
/// msgpack bytes.
///
/// A sort key is an expression, so evaluating it can fail — `ORDER BY 1/qty`
/// over a row with `qty = 0` returns `Err(EvalError::DivisionByZero)` and the
/// statement fails with SQLSTATE `22012`, the same as it would in a
/// projection. Returning the rows unsorted instead would answer a different
/// query than the client asked for.
///
/// A row that does not decode is an error. Sorting it as `null` would place
/// it by a value it does not hold.
pub(in crate::data::executor) fn sort_msgpack_rows(
    rows: &mut [Vec<u8>],
    sort_keys: &[SortKeySpec],
    crs: &nodedb_crs::CrsContext<'_>,
) -> crate::Result<()> {
    if sort_keys.is_empty() {
        return Ok(());
    }

    let decoded: Vec<Value> = rows
        .iter()
        .map(|r| {
            nodedb_types::object_msgpack::decode_object(r).map_err(|e| crate::Error::Codec {
                detail: format!("ORDER BY row decode: {e}"),
            })
        })
        .collect::<crate::Result<_>>()?;

    let keyed = sort_key_values(&decoded, sort_keys, crs)?;

    let mut indices: Vec<usize> = (0..rows.len()).collect();
    indices.sort_by(|&a, &b| compare_key_rows(&keyed[a], &keyed[b], sort_keys));

    let original: Vec<Vec<u8>> = rows.to_vec();
    for (dst, src) in indices.iter().enumerate() {
        rows[dst] = original[*src].clone();
    }
    Ok(())
}

/// Evaluate every sort expression against every row, up front.
///
/// Sorting compares a pair at a time and `sort_by` cannot report an error, so
/// the fallible work happens here where it can propagate.
pub(in crate::data::executor) fn sort_key_values(
    rows: &[Value],
    sort_keys: &[SortKeySpec],
    crs: &nodedb_crs::CrsContext<'_>,
) -> Result<Vec<Vec<Value>>, EvalError> {
    rows.iter()
        .map(|row| {
            sort_keys
                .iter()
                .map(|k| k.expr.eval(row, crs))
                .collect::<Result<Vec<_>, _>>()
        })
        .collect()
}

/// Compare two rows' pre-evaluated sort values.
pub(in crate::data::executor) fn compare_key_rows(
    a: &[Value],
    b: &[Value],
    sort_keys: &[SortKeySpec],
) -> std::cmp::Ordering {
    for (idx, key) in sort_keys.iter().enumerate() {
        let (Some(va), Some(vb)) = (a.get(idx), b.get(idx)) else {
            continue;
        };
        // NULL placement follows the key's NULLS FIRST/LAST setting, which the
        // sort direction never flips.
        let ord = match key.order_nulls(va.is_null(), vb.is_null()) {
            Some(ord) => ord,
            None => key.direct(
                crate::data::executor::handlers::document::sort::compare::compare_expression_values(
                    va, vb,
                ),
            ),
        };
        if ord != std::cmp::Ordering::Equal {
            return ord;
        }
    }
    std::cmp::Ordering::Equal
}

#[cfg(test)]
mod native_tests {
    use super::*;

    fn row(value: Value) -> Vec<u8> {
        zerompk::to_msgpack_vec(&Value::Object(
            [("key".into(), value)].into_iter().collect(),
        ))
        .expect("native row")
    }

    #[test]
    fn provider_sort_keeps_exact_native_keys_and_null_direction() {
        let low = Value::Decimal("123456789.123456789".parse().expect("decimal"));
        let high = Value::Decimal("123456789.123456790".parse().expect("decimal"));
        let keys = [SortKeySpec::column("key", true)];
        let mut rows = vec![row(high.clone()), row(low.clone())];
        sort_msgpack_rows(&mut rows, &keys, &nodedb_crs::CrsContext::builtin_only()).expect("sort");
        assert_eq!(
            nodedb_types::object_msgpack::decode_object(&rows[0])
                .expect("object")
                .get("key"),
            Some(&low)
        );
        assert_eq!(
            nodedb_types::object_msgpack::decode_object(&rows[1])
                .expect("object")
                .get("key"),
            Some(&high)
        );
        for ascending in [true, false] {
            let key = SortKeySpec::column("key", ascending);
            let expected = key.order_nulls(true, false).expect("null order");
            assert_eq!(
                compare_key_rows(&[Value::Null], std::slice::from_ref(&low), &[key]),
                expected
            );
        }
    }

    #[test]
    fn primitive_provider_comparison_keeps_expression_ordering() {
        let values = [
            Value::String("text".into()),
            Value::Integer(3),
            Value::Array(vec![]),
            Value::Bool(false),
            Value::Bool(true),
            Value::Object(Default::default()),
        ];
        let key = [SortKeySpec::column("key", true)];
        for pair in values.windows(2) {
            assert_eq!(
                compare_key_rows(&pair[..1], &pair[1..], &key),
                std::cmp::Ordering::Less
            );
        }
    }
}
