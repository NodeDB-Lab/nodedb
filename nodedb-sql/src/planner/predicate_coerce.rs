// SPDX-License-Identifier: Apache-2.0

//! Coerce the literals a predicate compares against a declared `TIMESTAMP` /
//! `TIMESTAMPTZ` column into typed instants, once, at plan time.
//!
//! # Why this exists
//!
//! A time literal in a `WHERE`, `ON`, or `WHEN` clause reaches the planner as
//! whatever the user typed: `at > 1583402400000`, `at > '2020-03-05
//! 10:00:00'`, `at > TIMESTAMP '...'`. The write side already resolves the
//! same three spellings to one typed instant through
//! [`super::declared_type_coerce::coerce_value`], so every engine stores an
//! instant. Left as typed, a numeric read literal is an `Integer` the Data
//! Plane compares against an instant: the pair has no defined equality or
//! order, so `=` never matches and `>=` either matches nothing or, on a path
//! that guessed a unit, matches by accident.
//!
//! Resolving the literal here, against the declared column type the scope
//! already carries, means the Data Plane only ever compares instants with
//! instants. It also puts the type error where PostgreSQL puts it: `WHERE at
//! = true` fails the statement naming the column, instead of scanning every
//! row to match none.
//!
//! # Scope
//!
//! Only `TIMESTAMP` / `TIMESTAMPTZ` columns are coerced. Every other declared
//! type is left as written: the Data Plane's coerced comparison already
//! reads `"5"` against `5` and `1.5` against `1`, so no read literal for
//! those types is lost the way a bare integer against an instant is.
//!
//! The primary-key column is exempt, mirroring the write side: the engines
//! derive a row's identity from the literal's own rendering on both the
//! write and the read side, and re-typing one side would make the row
//! unfindable by the other.
//!
//! A column the scope does not declare (a schemaless field, a computed
//! alias, an aggregate output) is left untouched: there is no declared type
//! to coerce to.

use crate::error::Result;
use crate::resolver::columns::TableScope;
use crate::types::{BinaryOp, ColumnInfo, SqlDataType, SqlExpr, SqlValue, UnaryOp};

use super::declared_type_coerce::coerce_value;

/// Coerce one literal compared against `column` by the read-side rule.
///
/// The one rule for a literal a predicate compares against a declared
/// column: an instant column (`TIMESTAMP` / `TIMESTAMPTZ`) types the literal
/// through [`coerce_value`], the primary-key column and every other declared
/// type keep the literal as written. `WHERE` clauses reach it through
/// [`coerce_predicate_literals`]; row-level-security policy compilation calls
/// it directly, so a policy literal and a query literal on the same column
/// resolve to the same typed instant.
///
/// Errors name the column and the literal when the literal carries no
/// instant.
pub fn coerce_read_literal(column: &ColumnInfo, value: SqlValue) -> Result<SqlValue> {
    if column.is_primary_key || !is_instant(&column.data_type) {
        return Ok(value);
    }
    coerce_value(&column.name, value, &column.data_type)
}

/// Coerce, in place, every literal `expr` compares against a declared
/// instant column.
///
/// Walks `AND` / `OR` / `NOT` down to each predicate and handles the shapes
/// that pair one column with literals: `column <op> literal` (either
/// orientation), `column BETWEEN literal AND literal`, and `column IN
/// (literal, …)`. A literal of a kind that carries no instant (a boolean,
/// bytes, an array) is a typed error naming the column.
pub(crate) fn coerce_predicate_literals(expr: &mut SqlExpr, scope: &TableScope) -> Result<()> {
    match expr {
        SqlExpr::BinaryOp { left, op, right } => match op {
            BinaryOp::And | BinaryOp::Or => {
                coerce_predicate_literals(left, scope)?;
                coerce_predicate_literals(right, scope)
            }
            BinaryOp::Eq
            | BinaryOp::Ne
            | BinaryOp::Gt
            | BinaryOp::Ge
            | BinaryOp::Lt
            | BinaryOp::Le => match (left.as_mut(), right.as_mut()) {
                (SqlExpr::Column { table, name }, SqlExpr::Literal(value))
                | (SqlExpr::Literal(value), SqlExpr::Column { table, name }) => {
                    coerce_literal(scope, table.as_deref(), name, value)
                }
                // Column-vs-column, computed operands, subqueries: no literal
                // to coerce.
                _ => Ok(()),
            },
            BinaryOp::Add
            | BinaryOp::Sub
            | BinaryOp::Mul
            | BinaryOp::Div
            | BinaryOp::Mod
            | BinaryOp::Concat => Ok(()),
        },
        SqlExpr::UnaryOp {
            op: UnaryOp::Not,
            expr,
        } => coerce_predicate_literals(expr, scope),
        SqlExpr::Between {
            expr, low, high, ..
        } => {
            let SqlExpr::Column { table, name } = expr.as_ref() else {
                return Ok(());
            };
            for bound in [low.as_mut(), high.as_mut()] {
                if let SqlExpr::Literal(value) = bound {
                    coerce_literal(scope, table.as_deref(), name, value)?;
                }
            }
            Ok(())
        }
        SqlExpr::InList { expr, list, .. } => {
            let SqlExpr::Column { table, name } = expr.as_ref() else {
                return Ok(());
            };
            for element in list.iter_mut() {
                if let SqlExpr::Literal(value) = element {
                    coerce_literal(scope, table.as_deref(), name, value)?;
                }
            }
            Ok(())
        }
        SqlExpr::UnaryOp {
            op: UnaryOp::Neg, ..
        }
        | SqlExpr::Column { .. }
        | SqlExpr::Literal(_)
        | SqlExpr::Function { .. }
        | SqlExpr::Case { .. }
        | SqlExpr::Cast { .. }
        | SqlExpr::Subquery(_)
        | SqlExpr::Wildcard
        | SqlExpr::IsNull { .. }
        | SqlExpr::Like { .. }
        | SqlExpr::ArrayLiteral(_) => Ok(()),
    }
}

/// Coerce one literal compared against the column `table.name` names, when
/// the scope declares that column as an instant.
fn coerce_literal(
    scope: &TableScope,
    table: Option<&str>,
    name: &str,
    value: &mut SqlValue,
) -> Result<()> {
    let Some(column) = scope.declared_column(table, name) else {
        return Ok(());
    };
    let taken = std::mem::replace(value, SqlValue::Null);
    *value = coerce_read_literal(column, taken)?;
    Ok(())
}

/// Whether a declared type stores a typed instant.
fn is_instant(declared: &SqlDataType) -> bool {
    match declared {
        SqlDataType::Timestamp | SqlDataType::Timestamptz => true,
        SqlDataType::Int64
        | SqlDataType::Float64
        | SqlDataType::String
        | SqlDataType::Bool
        | SqlDataType::Bytes
        | SqlDataType::Decimal
        | SqlDataType::Uuid
        | SqlDataType::Vector(_)
        | SqlDataType::Geometry
        | SqlDataType::Unknown => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resolver::columns::ResolvedTable;
    use crate::types::{CollectionInfo, ColumnInfo, EngineType};
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

    /// `t (id TEXT PRIMARY KEY, at TIMESTAMP, at_tz TIMESTAMPTZ, n BIGINT,
    /// pk_at TIMESTAMP PRIMARY KEY)` under alias `a`.
    fn scope() -> TableScope {
        let info = CollectionInfo {
            name: "t".into(),
            engine: EngineType::DocumentStrict,
            columns: vec![
                column("id", SqlDataType::String, true),
                column("at", SqlDataType::Timestamp, false),
                column("at_tz", SqlDataType::Timestamptz, false),
                column("n", SqlDataType::Int64, false),
                column("pk_at", SqlDataType::Timestamp, true),
            ],
            primary_key: Some("id".into()),
            has_auto_tier: false,
            indexes: Vec::new(),
            bitemporal: false,
            primary: nodedb_types::PrimaryEngine::Document,
            vector_primary: None,
            partition_strategy: nodedb_types::PartitionStrategy::CollectionHomed,
            open_schema: CollectionInfo::open_schema_for(EngineType::DocumentStrict),
        };
        TableScope::single(ResolvedTable {
            name: info.name.clone(),
            alias: Some("a".into()),
            info,
        })
        .expect("single-relation scope")
    }

    fn col(table: Option<&str>, name: &str) -> SqlExpr {
        SqlExpr::Column {
            table: table.map(str::to_string),
            name: name.into(),
        }
    }

    fn lit(value: SqlValue) -> SqlExpr {
        SqlExpr::Literal(value)
    }

    fn cmp(left: SqlExpr, op: BinaryOp, right: SqlExpr) -> SqlExpr {
        SqlExpr::BinaryOp {
            left: Box::new(left),
            op,
            right: Box::new(right),
        }
    }

    fn literal_of(expr: &SqlExpr) -> &SqlValue {
        match expr {
            SqlExpr::Literal(v) => v,
            other => panic!("expected a literal, got {other:?}"),
        }
    }

    fn coerced(mut expr: SqlExpr) -> SqlExpr {
        coerce_predicate_literals(&mut expr, &scope()).expect("predicate coerces");
        expr
    }

    #[test]
    fn column_op_literal_coerces_a_numeric_literal_to_the_declared_instant() {
        let expr = coerced(cmp(
            col(None, "at"),
            BinaryOp::Gt,
            lit(SqlValue::Int(EARLY_MS)),
        ));
        let SqlExpr::BinaryOp { right, .. } = &expr else {
            panic!("shape preserved");
        };
        assert_eq!(literal_of(right), &SqlValue::Timestamp(early()));
    }

    #[test]
    fn literal_op_column_is_coerced_in_the_mirrored_orientation() {
        let expr = coerced(cmp(
            lit(SqlValue::String("2020-03-05 10:00:00".into())),
            BinaryOp::Lt,
            col(Some("a"), "at_tz"),
        ));
        let SqlExpr::BinaryOp { left, .. } = &expr else {
            panic!("shape preserved");
        };
        assert_eq!(literal_of(left), &SqlValue::Timestamptz(early()));
    }

    #[test]
    fn between_coerces_both_bounds() {
        let expr = coerced(SqlExpr::Between {
            expr: Box::new(col(None, "at")),
            low: Box::new(lit(SqlValue::Int(EARLY_MS))),
            high: Box::new(lit(SqlValue::String("2020-03-05T11:00:00Z".into()))),
            negated: false,
        });
        let SqlExpr::Between { low, high, .. } = &expr else {
            panic!("shape preserved");
        };
        assert_eq!(literal_of(low), &SqlValue::Timestamp(early()));
        assert_eq!(
            literal_of(high),
            &SqlValue::Timestamp(NdbDateTime::from_micros((EARLY_MS + 3_600_000) * 1_000))
        );
    }

    #[test]
    fn in_list_coerces_every_literal_element() {
        let expr = coerced(SqlExpr::InList {
            expr: Box::new(col(None, "at")),
            list: vec![
                lit(SqlValue::Int(EARLY_MS)),
                lit(SqlValue::String("2020-03-05 10:00:00".into())),
            ],
            negated: true,
        });
        let SqlExpr::InList { list, .. } = &expr else {
            panic!("shape preserved");
        };
        for element in list {
            assert_eq!(literal_of(element), &SqlValue::Timestamp(early()));
        }
    }

    #[test]
    fn nested_and_or_not_reach_every_predicate() {
        let expr = coerced(SqlExpr::UnaryOp {
            op: UnaryOp::Not,
            expr: Box::new(cmp(
                cmp(col(None, "at"), BinaryOp::Ge, lit(SqlValue::Int(EARLY_MS))),
                BinaryOp::Or,
                cmp(
                    cmp(col(None, "n"), BinaryOp::Eq, lit(SqlValue::Int(1))),
                    BinaryOp::And,
                    cmp(
                        col(None, "at_tz"),
                        BinaryOp::Le,
                        lit(SqlValue::Int(EARLY_MS)),
                    ),
                ),
            )),
        });
        let SqlExpr::UnaryOp { expr, .. } = &expr else {
            panic!("shape preserved");
        };
        let SqlExpr::BinaryOp { left, right, .. } = expr.as_ref() else {
            panic!("shape preserved");
        };
        let SqlExpr::BinaryOp { right: at_lit, .. } = left.as_ref() else {
            panic!("shape preserved");
        };
        assert_eq!(literal_of(at_lit), &SqlValue::Timestamp(early()));
        let SqlExpr::BinaryOp {
            left: n_pred,
            right: tz_pred,
            ..
        } = right.as_ref()
        else {
            panic!("shape preserved");
        };
        let SqlExpr::BinaryOp { right: n_lit, .. } = n_pred.as_ref() else {
            panic!("shape preserved");
        };
        assert_eq!(literal_of(n_lit), &SqlValue::Int(1), "BIGINT is untouched");
        let SqlExpr::BinaryOp { right: tz_lit, .. } = tz_pred.as_ref() else {
            panic!("shape preserved");
        };
        assert_eq!(literal_of(tz_lit), &SqlValue::Timestamptz(early()));
    }

    #[test]
    fn a_column_the_scope_does_not_declare_is_left_untouched() {
        let expr = coerced(cmp(
            col(None, "created"),
            BinaryOp::Eq,
            lit(SqlValue::Int(EARLY_MS)),
        ));
        let SqlExpr::BinaryOp { right, .. } = &expr else {
            panic!("shape preserved");
        };
        assert_eq!(literal_of(right), &SqlValue::Int(EARLY_MS));
    }

    #[test]
    fn a_qualifier_naming_another_relation_is_left_untouched() {
        let expr = coerced(cmp(
            col(Some("other"), "at"),
            BinaryOp::Eq,
            lit(SqlValue::Int(EARLY_MS)),
        ));
        let SqlExpr::BinaryOp { right, .. } = &expr else {
            panic!("shape preserved");
        };
        assert_eq!(literal_of(right), &SqlValue::Int(EARLY_MS));
    }

    #[test]
    fn the_primary_key_column_is_exempt() {
        let expr = coerced(cmp(
            col(None, "pk_at"),
            BinaryOp::Eq,
            lit(SqlValue::Int(EARLY_MS)),
        ));
        let SqlExpr::BinaryOp { right, .. } = &expr else {
            panic!("shape preserved");
        };
        assert_eq!(literal_of(right), &SqlValue::Int(EARLY_MS));
    }

    #[test]
    fn column_vs_column_and_null_are_left_alone() {
        let expr = coerced(cmp(
            cmp(col(None, "at"), BinaryOp::Lt, col(None, "at_tz")),
            BinaryOp::And,
            cmp(col(None, "at"), BinaryOp::Eq, lit(SqlValue::Null)),
        ));
        let SqlExpr::BinaryOp { right, .. } = &expr else {
            panic!("shape preserved");
        };
        let SqlExpr::BinaryOp {
            right: null_lit, ..
        } = right.as_ref()
        else {
            panic!("shape preserved");
        };
        assert_eq!(literal_of(null_lit), &SqlValue::Null);
    }

    #[test]
    fn a_boolean_literal_is_a_typed_error_naming_the_column() {
        let mut expr = cmp(col(None, "at"), BinaryOp::Eq, lit(SqlValue::Bool(true)));
        let err = coerce_predicate_literals(&mut expr, &scope())
            .expect_err("a boolean carries no instant");
        let detail = err.to_string();
        assert!(
            detail.contains("at") && detail.contains("a boolean"),
            "error must name the column and the kind: {detail}"
        );
    }

    #[test]
    fn unparseable_text_is_a_typed_error_naming_the_column_and_literal() {
        let mut expr = SqlExpr::InList {
            expr: Box::new(col(None, "at")),
            list: vec![lit(SqlValue::String("not a date".into()))],
            negated: false,
        };
        let err = coerce_predicate_literals(&mut expr, &scope())
            .expect_err("text that spells no instant is refused");
        let detail = err.to_string();
        assert!(
            detail.contains("at") && detail.contains("not a date"),
            "error must name the column and the literal: {detail}"
        );
    }
}
