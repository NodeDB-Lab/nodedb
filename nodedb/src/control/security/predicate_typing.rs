// SPDX-License-Identifier: BUSL-1.1

//! Type the literals of a compiled RLS predicate against the collection's
//! declared columns.
//!
//! A policy literal reaches the parser as whatever the author typed:
//! `captured_at > 1583402400000` or `captured_at > '2020-03-05'`. The Data
//! Plane compares an instant column against a typed instant, and an untyped
//! integer has no order against one, so a policy left untyped matches
//! nothing on such a column. This pass resolves every literal compared
//! against a declared `TIMESTAMP` / `TIMESTAMPTZ` column through
//! [`nodedb_sql::planner::predicate_coerce::coerce_read_literal`], the same
//! rule the planner applies to a query's `WHERE` literal, so a policy and a
//! query on the same column agree on the instant.
//!
//! A literal the declared type cannot read (`captured_at >= true`) is an
//! error naming the column and the literal. The caller refuses the policy:
//! a policy that cannot be enforced as written is never stored.

use nodedb_sql::planner::predicate_coerce::coerce_read_literal;
use nodedb_sql::types::{ColumnInfo, SqlValue};
use nodedb_types::json_msgpack::InstantKind;

use super::predicate::{PredicateValue, RlsPredicate};

/// Type every literal in `predicate` that a `Compare` node pairs with a
/// declared column of `columns`.
///
/// Literals on `CONTAINS` / `INTERSECTS` nodes and on plan-time-only
/// comparisons (`$auth.x = 'lit'`, which name no document field) are left
/// as written: neither compares a literal against a declared scalar column.
pub fn type_predicate_literals(
    predicate: RlsPredicate,
    columns: &[ColumnInfo],
) -> crate::Result<RlsPredicate> {
    Ok(match predicate {
        RlsPredicate::Compare { field, op, value } => {
            let value = type_compare_literal(&field, value, columns)?;
            RlsPredicate::Compare { field, op, value }
        }
        RlsPredicate::And(children) => RlsPredicate::And(type_children(children, columns)?),
        RlsPredicate::Or(children) => RlsPredicate::Or(type_children(children, columns)?),
        RlsPredicate::Not(inner) => {
            RlsPredicate::Not(Box::new(type_predicate_literals(*inner, columns)?))
        }
        RlsPredicate::Contains { .. }
        | RlsPredicate::Intersects { .. }
        | RlsPredicate::AlwaysTrue
        | RlsPredicate::AlwaysFalse => predicate,
    })
}

fn type_children(
    children: Vec<RlsPredicate>,
    columns: &[ColumnInfo],
) -> crate::Result<Vec<RlsPredicate>> {
    children
        .into_iter()
        .map(|child| type_predicate_literals(child, columns))
        .collect()
}

/// Type the right-hand side of `field <op> value` when it is a literal and
/// `field` names a declared column.
fn type_compare_literal(
    field: &str,
    value: PredicateValue,
    columns: &[ColumnInfo],
) -> crate::Result<PredicateValue> {
    let PredicateValue::Literal(literal) = value else {
        return Ok(value);
    };
    let Some(column) = columns
        .iter()
        .find(|column| column.name.eq_ignore_ascii_case(field))
    else {
        return Ok(PredicateValue::Literal(literal));
    };
    let Some(sql_literal) = json_to_sql_literal(&literal) else {
        return Ok(PredicateValue::Literal(literal));
    };
    let coerced =
        coerce_read_literal(column, sql_literal).map_err(|error| crate::Error::BadRequest {
            detail: format!("RLS predicate: {error}"),
        })?;
    Ok(sql_literal_to_predicate_value(coerced, literal))
}

/// The `SqlValue` a policy literal denotes, for the literal kinds the policy
/// parser produces. An array or object literal has no scalar form and stays
/// as written.
fn json_to_sql_literal(literal: &serde_json::Value) -> Option<SqlValue> {
    match literal {
        serde_json::Value::Null => Some(SqlValue::Null),
        serde_json::Value::Bool(b) => Some(SqlValue::Bool(*b)),
        serde_json::Value::Number(n) => n
            .as_i64()
            .map(SqlValue::Int)
            .or_else(|| n.as_f64().map(SqlValue::Float)),
        serde_json::Value::String(s) => Some(SqlValue::String(s.clone())),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => None,
    }
}

/// The predicate value a coerced literal becomes: a typed instant, or the
/// original literal when the declared type imposed no representation.
fn sql_literal_to_predicate_value(
    coerced: SqlValue,
    original: serde_json::Value,
) -> PredicateValue {
    match coerced {
        SqlValue::Timestamp(at) => PredicateValue::Instant {
            at,
            kind: InstantKind::Naive,
        },
        SqlValue::Timestamptz(at) => PredicateValue::Instant {
            at,
            kind: InstantKind::Utc,
        },
        SqlValue::Null
        | SqlValue::Bool(_)
        | SqlValue::Int(_)
        | SqlValue::Float(_)
        | SqlValue::Decimal(_)
        | SqlValue::String(_)
        | SqlValue::Bytes(_)
        | SqlValue::Array(_) => PredicateValue::Literal(original),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::predicate::CompareOp;
    use crate::control::security::predicate_parser::parse_predicate;
    use nodedb_sql::types::SqlDataType;
    use nodedb_types::datetime::NdbDateTime;

    /// `2020-03-05T10:00:00Z` as epoch milliseconds.
    const EARLY_MS: i64 = 1_583_402_400_000;

    fn early() -> NdbDateTime {
        NdbDateTime::from_micros(EARLY_MS * 1_000)
    }

    fn column(name: &str, data_type: SqlDataType, is_primary_key: bool) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            data_type,
            nullable: true,
            is_primary_key,
            default: None,
            raw_type: None,
            int_width: None,
            float_width: None,
        }
    }

    fn columns() -> Vec<ColumnInfo> {
        vec![
            column("id", SqlDataType::String, true),
            column("captured_at", SqlDataType::Timestamp, false),
            column("seen_at", SqlDataType::Timestamptz, false),
            column("n", SqlDataType::Int64, false),
        ]
    }

    fn typed(text: &str) -> crate::Result<RlsPredicate> {
        let parsed = parse_predicate(text).expect("policy text parses");
        type_predicate_literals(parsed, &columns())
    }

    fn compare_value(predicate: &RlsPredicate) -> &PredicateValue {
        match predicate {
            RlsPredicate::Compare { value, .. } => value,
            other => panic!("expected a comparison, got {other:?}"),
        }
    }

    /// A numeric literal against a `TIMESTAMP` column is epoch milliseconds
    /// and becomes a naive instant.
    #[test]
    fn numeric_literal_on_an_instant_column_becomes_a_naive_instant() {
        let predicate = typed(&format!("captured_at >= {EARLY_MS}")).expect("types");
        assert!(matches!(
            compare_value(&predicate),
            PredicateValue::Instant { at, kind: InstantKind::Naive } if *at == early()
        ));
    }

    /// A text literal against a `TIMESTAMPTZ` column is parsed as ISO-8601
    /// and becomes a zoned instant.
    #[test]
    fn text_literal_on_a_zoned_column_becomes_a_utc_instant() {
        let predicate = typed("seen_at > '2020-03-05 10:00:00'").expect("types");
        assert!(matches!(
            compare_value(&predicate),
            PredicateValue::Instant { at, kind: InstantKind::Utc } if *at == early()
        ));
    }

    /// Typing reaches every comparison under `AND` / `OR` / `NOT`.
    #[test]
    fn typing_descends_through_composite_nodes() {
        let predicate = typed(&format!(
            "(captured_at >= {EARLY_MS} OR owner = $auth.username) AND NOT seen_at < '2020-03-05'"
        ))
        .expect("types");
        let RlsPredicate::And(children) = &predicate else {
            panic!("expected AND, got {predicate:?}");
        };
        let RlsPredicate::Or(or_children) = &children[0] else {
            panic!("expected OR, got {:?}", children[0]);
        };
        assert!(matches!(
            compare_value(&or_children[0]),
            PredicateValue::Instant { .. }
        ));
        let RlsPredicate::Not(inner) = &children[1] else {
            panic!("expected NOT, got {:?}", children[1]);
        };
        assert!(matches!(
            compare_value(inner),
            PredicateValue::Instant { .. }
        ));
    }

    /// A literal no instant can be read from is refused, naming the column.
    #[test]
    fn non_instant_literal_on_an_instant_column_is_refused_naming_the_column() {
        let error = typed("captured_at >= true").expect_err("a boolean is refused");
        let detail = error.to_string();
        assert!(
            detail.contains("captured_at") && detail.contains("boolean"),
            "error must name the column and the literal kind: {detail}"
        );
        let error = typed("captured_at >= 'not a date'").expect_err("non-datetime text is refused");
        let detail = error.to_string();
        assert!(
            detail.contains("captured_at") && detail.contains("not a date"),
            "error must name the column and the literal: {detail}"
        );
    }

    /// A column that is not an instant, an undeclared field, the primary
    /// key, and an `$auth.*` reference are all left as written.
    #[test]
    fn non_instant_targets_are_left_as_written() {
        for text in [
            "n > 5",
            "undeclared > 5",
            "id = '2020-03-05'",
            "captured_at = $auth.id",
        ] {
            let predicate = typed(text).unwrap_or_else(|e| panic!("{text}: {e}"));
            assert!(
                !matches!(compare_value(&predicate), PredicateValue::Instant { .. }),
                "{text} must not be typed as an instant: {predicate:?}"
            );
        }
        let predicate = typed("n > 5").expect("types");
        assert!(matches!(
            predicate,
            RlsPredicate::Compare {
                op: CompareOp::Gt,
                value: PredicateValue::Literal(_),
                ..
            }
        ));
    }
}
