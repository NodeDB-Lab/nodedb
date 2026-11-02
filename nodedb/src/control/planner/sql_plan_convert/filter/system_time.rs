// SPDX-License-Identifier: BUSL-1.1

//! `_ts_system` predicates of a timeseries scan, in stored ordinals.
//!
//! A timeseries row stores the system-time ordinal of its batch in
//! `_ts_system`, and the scan compares predicates against the stored cell.
//! A user writes and reads the column in milliseconds. So each comparison
//! with a constant millisecond becomes the equivalent ordinal bound:
//!
//! - `<= ms` and `> ms` compare with `ms_to_ordinal_upper(ms)`.
//! - `< ms` and `>= ms` compare with `ms_to_ordinal_upper(ms - 1)`, as
//!   `<=` and `>` of it.
//! - `= ms` is the range of `ms`, `!= ms` its complement.
//! - `IN` is a disjunction of ranges, `NOT IN` a conjunction of complements.
//!
//! A predicate the rewrite cannot express in ordinals is refused: the
//! rewrite compares an ordinal with a millisecond.

use nodedb_query::scan_filter::{FilterOp, ScanFilter};
use nodedb_types::Value;
use nodedb_types::columnar::schema::TS_SYSTEM;

/// Rewrite every `_ts_system` predicate of `filters` from milliseconds to
/// stored ordinals. Every other predicate passes through unchanged.
pub(crate) fn system_time_filters_to_ordinals(
    filters: Vec<ScanFilter>,
) -> crate::Result<Vec<ScanFilter>> {
    let mut out = Vec::with_capacity(filters.len());
    for filter in filters {
        out.extend(rewrite(filter)?);
    }
    Ok(out)
}

/// The AND-group that replaces `filter`.
fn rewrite(filter: ScanFilter) -> crate::Result<Vec<ScanFilter>> {
    if filter.op == FilterOp::Or {
        let clauses = filter
            .clauses
            .into_iter()
            .map(system_time_filters_to_ordinals)
            .collect::<crate::Result<Vec<_>>>()?;
        return Ok(vec![or(clauses)]);
    }
    if let Some(expr) = &filter.expr
        && expr_reads_system_time(expr)
    {
        return Err(refusal("An expression"));
    }
    if !names_system_time(&filter.field) && !column_operand_is_system_time(&filter) {
        return Ok(vec![filter]);
    }
    let field = filter.field.clone();
    match filter.op {
        FilterOp::Lte => Ok(vec![bound(
            &field,
            FilterOp::Lte,
            upper(ms(&filter.value)?),
        )]),
        FilterOp::Gt => Ok(vec![bound(&field, FilterOp::Gt, upper(ms(&filter.value)?))]),
        FilterOp::Lt => Ok(vec![bound(
            &field,
            FilterOp::Lte,
            below(ms(&filter.value)?),
        )]),
        FilterOp::Gte => Ok(vec![bound(&field, FilterOp::Gt, below(ms(&filter.value)?))]),
        FilterOp::Eq => Ok(within(&field, ms(&filter.value)?)),
        FilterOp::Ne => Ok(vec![outside(&field, ms(&filter.value)?)]),
        FilterOp::In => {
            let clauses = list(&filter.value)?
                .into_iter()
                .map(|m| within(&field, m))
                .collect();
            Ok(vec![or(clauses)])
        }
        FilterOp::NotIn => Ok(list(&filter.value)?
            .into_iter()
            .map(|m| outside(&field, m))
            .collect()),
        FilterOp::IsNull | FilterOp::IsNotNull | FilterOp::Exists | FilterOp::NotExists => {
            Ok(vec![filter])
        }
        FilterOp::MatchAll | FilterOp::Or | FilterOp::Expr => Ok(vec![filter]),
        FilterOp::Contains
        | FilterOp::Like
        | FilterOp::NotLike
        | FilterOp::Ilike
        | FilterOp::NotIlike
        | FilterOp::ArrayContains
        | FilterOp::ArrayContainsAll
        | FilterOp::ArrayOverlap => Err(refusal("A text or array operator")),
        FilterOp::GtColumn
        | FilterOp::GteColumn
        | FilterOp::LtColumn
        | FilterOp::LteColumn
        | FilterOp::EqColumn
        | FilterOp::NeColumn => Err(refusal("A column comparison")),
    }
}

/// True when `field` names `_ts_system`, bare or qualified.
fn names_system_time(field: &str) -> bool {
    field == TS_SYSTEM
        || field
            .rsplit_once('.')
            .is_some_and(|(_, column)| column == TS_SYSTEM)
}

/// True for a column-vs-column filter whose other column is `_ts_system`.
fn column_operand_is_system_time(filter: &ScanFilter) -> bool {
    matches!(
        filter.op,
        FilterOp::GtColumn
            | FilterOp::GteColumn
            | FilterOp::LtColumn
            | FilterOp::LteColumn
            | FilterOp::EqColumn
            | FilterOp::NeColumn
    ) && matches!(&filter.value, Value::String(other) if names_system_time(other))
}

/// True when `expr` reads `_ts_system`.
fn expr_reads_system_time(expr: &nodedb_query::expr::SqlExpr) -> bool {
    let mut columns = Vec::new();
    crate::control::planner::redaction_refusal::collect_columns(expr, &mut columns);
    columns.into_iter().any(names_system_time)
}

/// The millisecond constant a `_ts_system` comparison carries.
fn ms(value: &Value) -> crate::Result<i64> {
    match value {
        Value::Integer(ms) => Ok(*ms),
        other => Err(crate::Error::PlanError {
            detail: format!(
                "a {TS_SYSTEM} predicate compares with an integer millisecond, got {other:?}"
            ),
        }),
    }
}

/// The milliseconds of an `IN` / `NOT IN` list.
fn list(value: &Value) -> crate::Result<Vec<i64>> {
    match value {
        Value::Array(items) => items.iter().map(ms).collect(),
        other => Err(crate::Error::PlanError {
            detail: format!("a {TS_SYSTEM} IN list holds integer milliseconds, got {other:?}"),
        }),
    }
}

/// The last ordinal of millisecond `ms`.
fn upper(ms: i64) -> i64 {
    nodedb_types::ms_to_ordinal_upper(ms)
}

/// The last ordinal before millisecond `ms`.
fn below(ms: i64) -> i64 {
    nodedb_types::ms_to_ordinal_upper(ms.saturating_sub(1))
}

/// The AND-group that admits exactly the ordinals of millisecond `ms`.
fn within(field: &str, ms: i64) -> Vec<ScanFilter> {
    vec![
        bound(field, FilterOp::Gt, below(ms)),
        bound(field, FilterOp::Lte, upper(ms)),
    ]
}

/// The filter that admits every ordinal outside millisecond `ms`.
fn outside(field: &str, ms: i64) -> ScanFilter {
    or(vec![
        vec![bound(field, FilterOp::Lte, below(ms))],
        vec![bound(field, FilterOp::Gt, upper(ms))],
    ])
}

fn bound(field: &str, op: FilterOp, ordinal: i64) -> ScanFilter {
    ScanFilter {
        field: field.to_string(),
        op,
        value: Value::Integer(ordinal),
        clauses: Vec::new(),
        expr: None,
    }
}

fn or(clauses: Vec<Vec<ScanFilter>>) -> ScanFilter {
    ScanFilter {
        field: String::new(),
        op: FilterOp::Or,
        value: Value::Null,
        clauses,
        expr: None,
    }
}

fn refusal(shape: &str) -> crate::Error {
    crate::Error::PlanError {
        detail: format!(
            "{TS_SYSTEM} on a timeseries collection takes comparisons with integer \
             milliseconds, IN lists, and NULL checks. {shape} over it is not supported"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cmp(op: FilterOp, ms: i64) -> ScanFilter {
        ScanFilter {
            field: TS_SYSTEM.to_string(),
            op,
            value: Value::Integer(ms),
            clauses: Vec::new(),
            expr: None,
        }
    }

    /// The comparable parts of a filter list: field, operator, value.
    fn shape(filters: &[ScanFilter]) -> Vec<(String, FilterOp, Value)> {
        filters
            .iter()
            .map(|f| (f.field.clone(), f.op, f.value.clone()))
            .collect()
    }

    fn rewrite_one(filter: ScanFilter) -> Vec<ScanFilter> {
        system_time_filters_to_ordinals(vec![filter]).expect("rewrite")
    }

    /// True when `ordinal` passes the rewritten AND-group.
    fn admits(group: &[ScanFilter], ordinal: i64) -> bool {
        group.iter().all(|f| match f.op {
            FilterOp::Or => f.clauses.iter().any(|c| admits(c, ordinal)),
            FilterOp::Lte => matches!(f.value, Value::Integer(b) if ordinal <= b),
            FilterOp::Gt => matches!(f.value, Value::Integer(b) if ordinal > b),
            other => panic!("unexpected op {other:?}"),
        })
    }

    const MS: i64 = 1_000;
    const FIRST: i64 = MS * nodedb_types::NANOS_PER_MS;
    const LAST: i64 = FIRST + nodedb_types::NANOS_PER_MS - 1;

    #[test]
    fn each_operator_admits_the_ordinals_of_its_milliseconds() {
        let cases: [(FilterOp, [bool; 4]); 6] = [
            // ordinals: last of ms-1, first of ms, last of ms, first of ms+1
            (FilterOp::Lte, [true, true, true, false]),
            (FilterOp::Lt, [true, false, false, false]),
            (FilterOp::Gt, [false, false, false, true]),
            (FilterOp::Gte, [false, true, true, true]),
            (FilterOp::Eq, [false, true, true, false]),
            (FilterOp::Ne, [true, false, false, true]),
        ];
        let probes = [FIRST - 1, FIRST, LAST, LAST + 1];
        for (op, expected) in cases {
            let group = rewrite_one(cmp(op, MS));
            for (probe, want) in probes.iter().zip(expected) {
                assert_eq!(admits(&group, *probe), want, "{op:?} at {probe}");
            }
        }
    }

    #[test]
    fn in_and_not_in_rewrite_to_ranges() {
        let list = |op| ScanFilter {
            field: TS_SYSTEM.to_string(),
            op,
            value: Value::Array(vec![Value::Integer(MS), Value::Integer(MS + 5)]),
            clauses: Vec::new(),
            expr: None,
        };
        let inside = rewrite_one(list(FilterOp::In));
        let excluded = rewrite_one(list(FilterOp::NotIn));
        for (probe, member) in [
            (FIRST, true),
            (LAST + 1, false),
            (LAST + 5 * 1_000_000, true),
        ] {
            assert_eq!(admits(&inside, probe), member, "IN at {probe}");
            assert_eq!(admits(&excluded, probe), !member, "NOT IN at {probe}");
        }
    }

    #[test]
    fn predicates_inside_an_or_rewrite_and_other_fields_pass_through() {
        let other = ScanFilter {
            field: "v".to_string(),
            op: FilterOp::Gt,
            value: Value::Integer(MS),
            clauses: Vec::new(),
            expr: None,
        };
        let group = rewrite_one(or(vec![vec![cmp(FilterOp::Eq, MS)], vec![other.clone()]]));
        let [ScanFilter { clauses, .. }] = group.as_slice() else {
            panic!("one OR filter, got {group:?}");
        };
        assert!(admits(&clauses[0], FIRST));
        assert!(!admits(&clauses[0], LAST + 1));
        assert_eq!(shape(&clauses[1]), shape(std::slice::from_ref(&other)));
        assert_eq!(
            shape(&rewrite_one(other.clone())),
            shape(std::slice::from_ref(&other))
        );
    }

    #[test]
    fn a_qualified_field_rewrites() {
        let mut filter = cmp(FilterOp::Lte, MS);
        filter.field = format!("t.{TS_SYSTEM}");
        let group = rewrite_one(filter);
        assert_eq!(group[0].value, Value::Integer(LAST));
    }

    #[test]
    fn shapes_without_an_ordinal_form_are_refused() {
        let mut like = cmp(FilterOp::Like, MS);
        like.value = Value::String("1%".into());
        assert!(system_time_filters_to_ordinals(vec![like]).is_err());
        let mut float = cmp(FilterOp::Gt, MS);
        float.value = Value::Float(1.5);
        assert!(system_time_filters_to_ordinals(vec![float]).is_err());
        let expr = ScanFilter {
            field: String::new(),
            op: FilterOp::Expr,
            value: Value::Null,
            clauses: Vec::new(),
            expr: Some(nodedb_query::expr::SqlExpr::Column(TS_SYSTEM.to_string())),
        };
        assert!(system_time_filters_to_ordinals(vec![expr]).is_err());
        let null_check = cmp(FilterOp::IsNotNull, 0);
        assert_eq!(
            shape(&rewrite_one(null_check.clone())),
            shape(std::slice::from_ref(&null_check))
        );
    }
}
