// SPDX-License-Identifier: BUSL-1.1

//! Time-range narrowing for timeseries scans.
//!
//! Partition pruning and the memtable's timestamp pre-filter both work off a
//! `(min, max)` envelope. That envelope is derived here, in the Data Plane,
//! from the query's own predicates on the collection's **declared** time
//! column — the one place that knows which column that is.
//!
//! Deriving it in the Control Plane would mean guessing the time column from
//! its name, which is wrong in both directions: it prunes on a column the
//! collection does not key on, and it silently gives up on any time key whose
//! name is not one of a handful of conventional spellings.
//!
//! The envelope is inclusive and deliberately permissive: strict and
//! non-strict comparisons narrow it identically, because the exact predicate
//! is still evaluated per row afterwards. A too-wide envelope costs I/O; a
//! too-narrow one loses rows.

use crate::bridge::scan_filter::{FilterOp, ScanFilter};
use crate::engine::timeseries::columnar_memtable::TimeKind;

/// Narrow `plan_range` with every bound the query places on the time key
/// named by `time_key`, lowering each literal to stored milliseconds by the
/// key's kind.
///
/// Returns `plan_range` unchanged when the collection has no declared time
/// key, or when no predicate references it. A literal the kind cannot lower
/// narrows nothing: the exact predicate still decides each row afterwards.
pub(in crate::data::executor) fn narrow_time_range(
    plan_range: (i64, i64),
    filters: &[ScanFilter],
    time_key: Option<(&str, TimeKind)>,
) -> (i64, i64) {
    let Some((time_key, kind)) = time_key else {
        return plan_range;
    };
    let (mut min_ts, mut max_ts) = plan_range;
    for filter in filters {
        if !filter.field.eq_ignore_ascii_case(time_key) {
            continue;
        }
        let Some(ms) = kind.literal_ms(&filter.value) else {
            continue;
        };
        match filter.op {
            FilterOp::Gt | FilterOp::Gte => {
                if ms > min_ts {
                    min_ts = ms;
                }
            }
            FilterOp::Lt | FilterOp::Lte => {
                if ms < max_ts {
                    max_ts = ms;
                }
            }
            FilterOp::Eq => {
                if ms > min_ts {
                    min_ts = ms;
                }
                if ms < max_ts {
                    max_ts = ms;
                }
            }
            _ => {}
        }
    }
    (min_ts, max_ts)
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::{InstantKind, Value};

    const UNBOUNDED: (i64, i64) = (i64::MIN, i64::MAX);
    const MILLIS: TimeKind = TimeKind::Millis;
    const NAIVE: TimeKind = TimeKind::Instant(InstantKind::Naive);

    fn filter(field: &str, op: FilterOp, value: Value) -> ScanFilter {
        ScanFilter {
            field: field.into(),
            op,
            value,
            clauses: Vec::new(),
            expr: None,
        }
    }

    #[test]
    fn no_declared_time_key_leaves_the_range_alone() {
        let filters = vec![filter("ts", FilterOp::Gt, Value::Integer(100))];
        assert_eq!(narrow_time_range(UNBOUNDED, &filters, None), UNBOUNDED);
    }

    #[test]
    fn bounds_on_the_declared_key_narrow_both_ends() {
        let filters = vec![
            filter("captured_at", FilterOp::Gte, Value::Integer(100)),
            filter("captured_at", FilterOp::Lt, Value::Integer(900)),
        ];
        assert_eq!(
            narrow_time_range(UNBOUNDED, &filters, Some(("captured_at", MILLIS))),
            (100, 900)
        );
    }

    /// A declared instant key lowers a typed instant, and text that spells
    /// one, to stored milliseconds.
    #[test]
    fn an_instant_key_lowers_instant_literals() {
        let at = nodedb_types::NdbDateTime::from_micros(1_583_402_400_000_000);
        let filters = vec![
            filter("captured_at", FilterOp::Gte, Value::NaiveDateTime(at)),
            filter(
                "captured_at",
                FilterOp::Lt,
                Value::String("2020-03-05 11:00:00".into()),
            ),
        ];
        assert_eq!(
            narrow_time_range(UNBOUNDED, &filters, Some(("captured_at", NAIVE))),
            (1_583_402_400_000, 1_583_406_000_000)
        );
    }

    /// A bare integer against an instant key carries no unit, so it narrows
    /// nothing; the per-row predicate still decides.
    #[test]
    fn an_integer_against_an_instant_key_narrows_nothing() {
        let filters = vec![filter("captured_at", FilterOp::Gt, Value::Integer(100))];
        assert_eq!(
            narrow_time_range(UNBOUNDED, &filters, Some(("captured_at", NAIVE))),
            UNBOUNDED
        );
    }

    #[test]
    fn predicates_on_other_columns_are_ignored() {
        // A column literally named `timestamp` that is not the time key must
        // not prune anything — its values are unrelated to partition time.
        let filters = vec![
            filter("timestamp", FilterOp::Gt, Value::Integer(500)),
            filter("value", FilterOp::Lt, Value::Float(1.0)),
        ];
        assert_eq!(
            narrow_time_range(UNBOUNDED, &filters, Some(("captured_at", MILLIS))),
            UNBOUNDED
        );
    }

    #[test]
    fn equality_pins_both_ends() {
        let filters = vec![filter("ts", FilterOp::Eq, Value::Integer(4_200))];
        assert_eq!(
            narrow_time_range(UNBOUNDED, &filters, Some(("ts", MILLIS))),
            (4_200, 4_200)
        );
    }

    #[test]
    fn a_plan_supplied_range_is_never_widened() {
        let filters = vec![
            filter("ts", FilterOp::Gte, Value::Integer(0)),
            filter("ts", FilterOp::Lte, Value::Integer(10_000)),
        ];
        assert_eq!(
            narrow_time_range((100, 900), &filters, Some(("ts", MILLIS))),
            (100, 900)
        );
    }

    #[test]
    fn case_differences_in_the_predicate_still_match_the_key() {
        let filters = vec![filter("TS", FilterOp::Gt, Value::Integer(7))];
        assert_eq!(
            narrow_time_range(UNBOUNDED, &filters, Some(("ts", MILLIS))),
            (7, i64::MAX)
        );
    }
}
