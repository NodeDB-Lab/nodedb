// SPDX-License-Identifier: BUSL-1.1

//! Bitemporal row-visibility predicate for system-time / valid-time queries.

/// Extract the system-time ordinal (`_ts_system`) of a columnar row for
/// audit-log ordering. Rows missing the column (non-bitemporal, or the
/// column index is `None`) sort first via `i64::MIN`.
pub(super) fn row_system_time(
    row: &[nodedb_types::value::Value],
    ts_system_idx: Option<usize>,
) -> i64 {
    match ts_system_idx.and_then(|i| row.get(i)) {
        Some(nodedb_types::value::Value::Integer(i)) => *i,
        _ => i64::MIN,
    }
}

/// Bitemporal row-level visibility predicate.
///
/// - `system_as_of`: when `Some`, the system-time ordinal cutoff
///   (`SystemTimeScope::as_of_ordinal`). A row whose `_ts_system` ordinal
///   exceeds it is hidden (written after the query's system-time cut).
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
    system_as_of: Option<i64>,
    valid_at_ms: Option<i64>,
) -> bool {
    use nodedb_types::value::Value;
    if let Some(cutoff) = system_as_of
        && let Some(idx) = ts_system_idx
        && let Some(v) = row.get(idx)
    {
        let Value::Integer(ordinal) = v else {
            return false;
        };
        if *ordinal > cutoff {
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

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::value::Value;

    fn row(ts_system: i64, value: i64) -> Vec<Value> {
        // [_ts_system, _ts_valid_from, _ts_valid_until, value]
        vec![
            Value::Integer(ts_system),
            Value::Integer(0),
            Value::Integer(i64::MAX),
            Value::Integer(value),
        ]
    }

    #[test]
    fn row_system_time_reads_column() {
        assert_eq!(row_system_time(&row(123, 9), Some(0)), 123);
        // No bitemporal column resolved → sorts first.
        assert_eq!(row_system_time(&row(123, 9), None), i64::MIN);
    }

    #[test]
    fn all_versions_sort_orders_ascending_by_system_time() {
        // A single logical key updated three times: three distinct versions
        // distinguished by `_ts_system`. Under AS OF SYSTEM TIME NULL every
        // version is emitted ordered ascending by system time.
        let mut rows = [row(300, 3), row(100, 1), row(200, 2)];
        let ts_idx = Some(0);
        rows.sort_by_key(|r| row_system_time(r, ts_idx));
        let times: Vec<i64> = rows.iter().map(|r| row_system_time(r, ts_idx)).collect();
        assert_eq!(times, vec![100, 200, 300]);
        // The payload column tracks the version it belongs to.
        let values: Vec<i64> = rows
            .iter()
            .map(|r| match r.get(3) {
                Some(Value::Integer(v)) => *v,
                _ => -1,
            })
            .collect();
        assert_eq!(values, vec![1, 2, 3]);
    }

    /// Two versions written in one millisecond, one on each side of a
    /// pinned cut inside that millisecond: the cut sees the first and hides
    /// the second, which a millisecond stamp cannot tell apart.
    #[test]
    fn a_pinned_cut_splits_two_versions_of_one_millisecond() {
        let ms = 1_700_000_000_000;
        let first = ms * nodedb_types::NANOS_PER_MS + 10;
        let second = ms * nodedb_types::NANOS_PER_MS + 20;
        let cut = nodedb_types::SystemTimeScope::Pinned {
            ordinal: ms * nodedb_types::NANOS_PER_MS + 15,
            cut_marker: 0,
        }
        .as_of_ordinal();
        let visible = |ordinal| {
            bitemporal_row_visible(&row(ordinal, 1), Some(0), Some(1), Some(2), cut, None)
        };
        assert!(visible(first), "the version before the cut is visible");
        assert!(!visible(second), "the version after the cut is hidden");
        let whole_ms = nodedb_types::SystemTimeScope::AsOf(ms).as_of_ordinal();
        let visible_at_ms = |ordinal| {
            bitemporal_row_visible(&row(ordinal, 1), Some(0), Some(1), Some(2), whole_ms, None)
        };
        assert!(
            visible_at_ms(first) && visible_at_ms(second),
            "AS OF the millisecond sees both"
        );
    }

    #[test]
    fn all_versions_does_not_apply_system_cutoff() {
        // With no system-time cutoff (Current/AllVersions both pass None to the
        // predicate) every row is visible regardless of `_ts_system`.
        assert!(bitemporal_row_visible(
            &row(999, 1),
            Some(0),
            Some(1),
            Some(2),
            None,
            None
        ));
    }
}
