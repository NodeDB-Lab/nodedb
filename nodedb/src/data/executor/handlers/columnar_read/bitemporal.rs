//! Bitemporal row-visibility predicate for system-time / valid-time queries.

/// Bitemporal row-level visibility predicate.
///
/// - `system_as_of_ms`: when `Some`, any row whose `_ts_system` value
///   exceeds the cutoff is hidden (write happened after the query's
///   system-time horizon).
/// - `valid_at_ms`: when `Some`, the row's
///   `[_ts_valid_from, _ts_valid_until)` interval must contain this
///   point.
///
/// Rows from non-bitemporal collections have no `_ts_system` column and
/// all three indices are `None`; the function returns `true` unconditionally.
pub(super) fn bitemporal_row_visible(
    row: &[nodedb_types::value::Value],
    ts_system_idx: Option<usize>,
    ts_valid_from_idx: Option<usize>,
    ts_valid_until_idx: Option<usize>,
    system_as_of_ms: Option<i64>,
    valid_at_ms: Option<i64>,
) -> bool {
    use nodedb_types::value::Value;
    if let Some(cutoff) = system_as_of_ms
        && let Some(idx) = ts_system_idx
        && let Some(v) = row.get(idx)
    {
        let ts = match v {
            Value::Integer(i) => *i,
            Value::DateTime(dt) => dt.micros / 1000,
            _ => return false,
        };
        if ts > cutoff {
            return false;
        }
    }
    if let Some(point) = valid_at_ms {
        let vf = ts_valid_from_idx
            .and_then(|i| row.get(i))
            .and_then(|v| match v {
                Value::Integer(i) => Some(*i),
                Value::DateTime(dt) => Some(dt.micros / 1000),
                _ => None,
            })
            .unwrap_or(i64::MIN);
        let vu = ts_valid_until_idx
            .and_then(|i| row.get(i))
            .and_then(|v| match v {
                Value::Integer(i) => Some(*i),
                Value::DateTime(dt) => Some(dt.micros / 1000),
                _ => None,
            })
            .unwrap_or(i64::MAX);
        if point < vf || point >= vu {
            return false;
        }
    }
    true
}
