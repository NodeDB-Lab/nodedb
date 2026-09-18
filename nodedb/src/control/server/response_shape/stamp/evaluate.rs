// SPDX-License-Identifier: BUSL-1.1

//! Evaluates the Control-Plane computed columns of a result set, row by row,
//! and writes each value into the flat row under its alias.
//!
//! Rows run in slice order and columns in SELECT-list order. The row document
//! an expression sees is built once per row and updated with each stamped
//! column's alias, so a later column reads an earlier alias exactly as SQL
//! left-to-right evaluation does.

use std::collections::HashMap;

use nodedb_types::Value;

use crate::control::sequence::SequenceAccess;
use crate::control::server::response_shape::schema::CpComputedColumn;
use crate::control::server::response_shape::types::ShapedRow;

use super::substitute::resolve_accessors;

/// Stamp every computed column onto every row.
///
/// A sequence accessor error keeps its own class (`42704` for an unknown
/// sequence, `55000` for a prerequisite-state refusal), and an evaluation
/// error such as division by zero keeps `22012`.
pub fn stamp_rows(
    rows: &mut [ShapedRow],
    columns: &[CpComputedColumn],
    access: &dyn SequenceAccess,
) -> crate::Result<()> {
    if columns.is_empty() {
        return Ok(());
    }
    for row in rows.iter_mut() {
        let mut doc = row_document(row);
        for column in columns {
            let resolved = resolve_accessors(&column.expr, &doc, access)?;
            let value = resolved.eval(&doc)?;
            if let Some(map) = doc.as_object_mut() {
                map.insert(column.alias.clone(), value.clone());
            }
            row.insert(column.alias.clone(), value);
        }
    }
    Ok(())
}

/// The flat row as the document an expression evaluates against.
fn row_document(row: &ShapedRow) -> Value {
    Value::Object(
        row.iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<HashMap<String, Value>>(),
    )
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use super::*;
    use crate::bridge::expr_eval::{BinaryOp, SqlExpr};

    /// Hands out 1, 2, 3, ... and remembers the last value per sequence.
    struct FakeAccess {
        next: RefCell<i64>,
        last: RefCell<HashMap<String, i64>>,
    }

    impl FakeAccess {
        fn new() -> Self {
            Self {
                next: RefCell::new(0),
                last: RefCell::new(HashMap::new()),
            }
        }
    }

    impl SequenceAccess for FakeAccess {
        fn nextval(&self, name: &str) -> crate::Result<i64> {
            let mut next = self.next.borrow_mut();
            *next += 1;
            self.last.borrow_mut().insert(name.to_string(), *next);
            Ok(*next)
        }
        fn currval(&self, name: &str) -> crate::Result<i64> {
            self.last.borrow().get(name).copied().ok_or_else(|| {
                crate::Error::ObjectNotInPrerequisiteState {
                    object: name.to_string(),
                    detail: format!("currval of sequence \"{name}\" is not yet defined"),
                }
            })
        }
        fn setval(&self, name: &str, value: i64) -> crate::Result<i64> {
            *self.next.borrow_mut() = value;
            self.last.borrow_mut().insert(name.to_string(), value);
            Ok(value)
        }
    }

    fn accessor(name: &str) -> SqlExpr {
        SqlExpr::Function {
            name: name.to_string(),
            args: vec![SqlExpr::Literal(Value::String("s".to_string()))],
        }
    }

    fn column(alias: &str, expr: SqlExpr) -> CpComputedColumn {
        CpComputedColumn {
            alias: alias.to_string(),
            expr,
        }
    }

    fn rows(n: usize) -> Vec<ShapedRow> {
        (0..n)
            .map(|i| {
                let mut row = ShapedRow::new();
                row.insert("id".to_string(), Value::Integer(i as i64));
                row
            })
            .collect()
    }

    #[test]
    fn each_row_gets_its_own_nextval_in_row_order() {
        let access = FakeAccess::new();
        let mut rows = rows(3);
        stamp_rows(&mut rows, &[column("n", accessor("nextval"))], &access).expect("stamp");
        let stamped: Vec<&Value> = rows.iter().map(|r| &r["n"]).collect();
        assert_eq!(
            stamped,
            [&Value::Integer(1), &Value::Integer(2), &Value::Integer(3)]
        );
    }

    #[test]
    fn currval_after_nextval_in_the_same_row_reads_that_rows_value() {
        let access = FakeAccess::new();
        let mut rows = rows(2);
        stamp_rows(
            &mut rows,
            &[
                column("n", accessor("nextval")),
                column("c", accessor("currval")),
            ],
            &access,
        )
        .expect("stamp");
        assert_eq!(rows[0]["n"], rows[0]["c"]);
        assert_eq!(rows[1]["n"], rows[1]["c"]);
        assert_eq!(rows[1]["n"], Value::Integer(2));
    }

    #[test]
    fn a_later_column_can_reference_an_earlier_alias() {
        let access = FakeAccess::new();
        let mut rows = rows(1);
        stamp_rows(
            &mut rows,
            &[
                column("n", accessor("nextval")),
                column(
                    "doubled",
                    SqlExpr::BinaryOp {
                        left: Box::new(SqlExpr::Column("n".to_string())),
                        op: BinaryOp::Mul,
                        right: Box::new(SqlExpr::Literal(Value::Integer(2))),
                    },
                ),
            ],
            &access,
        )
        .expect("stamp");
        assert_eq!(rows[0]["doubled"], Value::Integer(2));
    }

    #[test]
    fn an_expression_reads_the_rows_own_columns() {
        let access = FakeAccess::new();
        let mut rows = rows(2);
        stamp_rows(
            &mut rows,
            &[column(
                "tagged",
                SqlExpr::BinaryOp {
                    left: Box::new(accessor("nextval")),
                    op: BinaryOp::Add,
                    right: Box::new(SqlExpr::Column("id".to_string())),
                },
            )],
            &access,
        )
        .expect("stamp");
        assert_eq!(rows[0]["tagged"], Value::Integer(1));
        assert_eq!(rows[1]["tagged"], Value::Integer(3));
    }

    #[test]
    fn currval_before_nextval_fails_with_its_own_class() {
        let access = FakeAccess::new();
        let mut rows = rows(1);
        let err = stamp_rows(&mut rows, &[column("c", accessor("currval"))], &access)
            .expect_err("must fail");
        assert!(matches!(
            err,
            crate::Error::ObjectNotInPrerequisiteState { .. }
        ));
    }

    #[test]
    fn division_by_zero_keeps_its_class() {
        let access = FakeAccess::new();
        let mut rows = rows(1);
        let err = stamp_rows(
            &mut rows,
            &[column(
                "bad",
                SqlExpr::BinaryOp {
                    left: Box::new(accessor("nextval")),
                    op: BinaryOp::Div,
                    right: Box::new(SqlExpr::Literal(Value::Integer(0))),
                },
            )],
            &access,
        )
        .expect_err("must fail");
        assert!(matches!(err, crate::Error::DivisionByZero));
    }

    #[test]
    fn no_columns_leaves_rows_untouched() {
        let access = FakeAccess::new();
        let mut rows = rows(2);
        stamp_rows(&mut rows, &[], &access).expect("stamp");
        assert!(rows.iter().all(|r| r.len() == 1));
    }
}
