//! ORDER BY comparator: sort a materialized row buffer by a typed sort-key list.

/// Compare two columnar rows by the planner's sort-key list. Missing
/// columns compare as `Null`; unknown column names fall through as
/// `Equal` so a mis-typed ORDER BY at least keeps scan order stable.
pub(in crate::data::executor) fn sort_rows_by_keys(
    a: &[nodedb_types::value::Value],
    b: &[nodedb_types::value::Value],
    schema: &nodedb_types::columnar::ColumnarSchema,
    sort_keys: &[(String, bool)],
) -> std::cmp::Ordering {
    use nodedb_types::value::Value;
    for (field, ascending) in sort_keys {
        let idx = match schema.columns.iter().position(|c| &c.name == field) {
            Some(i) => i,
            None => continue,
        };
        let av = a.get(idx).unwrap_or(&Value::Null);
        let bv = b.get(idx).unwrap_or(&Value::Null);
        let ord = compare_values(av, bv);
        if ord != std::cmp::Ordering::Equal {
            return if *ascending { ord } else { ord.reverse() };
        }
    }
    std::cmp::Ordering::Equal
}

fn compare_values(
    a: &nodedb_types::value::Value,
    b: &nodedb_types::value::Value,
) -> std::cmp::Ordering {
    use nodedb_types::value::Value;
    // Null sorts last in ascending order (Postgres default).
    match (a, b) {
        (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
        (Value::Null, _) => std::cmp::Ordering::Greater,
        (_, Value::Null) => std::cmp::Ordering::Less,
        (Value::Integer(x), Value::Integer(y)) => x.cmp(y),
        (Value::Float(x), Value::Float(y)) => x.partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal),
        (Value::Integer(x), Value::Float(y)) => (*x as f64)
            .partial_cmp(y)
            .unwrap_or(std::cmp::Ordering::Equal),
        (Value::Float(x), Value::Integer(y)) => x
            .partial_cmp(&(*y as f64))
            .unwrap_or(std::cmp::Ordering::Equal),
        (Value::String(x), Value::String(y)) => x.cmp(y),
        (Value::Bool(x), Value::Bool(y)) => x.cmp(y),
        (Value::DateTime(x), Value::DateTime(y)) => x.unix_millis().cmp(&y.unix_millis()),
        // Fallback: compare debug-formatted forms so the sort is
        // deterministic for exotic types that happen to coincide in a
        // sort key column.
        _ => format!("{a:?}").cmp(&format!("{b:?}")),
    }
}
