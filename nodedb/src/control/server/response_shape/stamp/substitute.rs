// SPDX-License-Identifier: BUSL-1.1

//! Replaces every sequence accessor call in an expression with the value the
//! session's registry hands back for the current row.
//!
//! The walk is exhaustive over [`SqlExpr`] and resolves arguments before the
//! call that holds them, so an accessor nested inside another accessor's
//! argument runs first. Sibling calls run left to right, which is the order
//! `nextval` then `currval` in one SELECT list relies on.

use nodedb_types::Value;

use crate::bridge::expr_eval::SqlExpr;
use crate::control::sequence::SequenceAccess;

/// Resolve every sequence accessor call in `expr` against `row`, returning
/// an expression with each call replaced by its integer result.
pub(super) fn resolve_accessors(
    expr: &SqlExpr,
    row: &Value,
    access: &dyn SequenceAccess,
) -> crate::Result<SqlExpr> {
    match expr {
        SqlExpr::Column(_)
        | SqlExpr::Literal(_)
        | SqlExpr::OldColumn(_)
        | SqlExpr::ExcludedColumn(_) => Ok(expr.clone()),
        SqlExpr::BinaryOp { left, op, right } => Ok(SqlExpr::BinaryOp {
            left: Box::new(resolve_accessors(left, row, access)?),
            op: *op,
            right: Box::new(resolve_accessors(right, row, access)?),
        }),
        SqlExpr::Negate(inner) => Ok(SqlExpr::Negate(Box::new(resolve_accessors(
            inner, row, access,
        )?))),
        SqlExpr::Cast { expr, to_type } => Ok(SqlExpr::Cast {
            expr: Box::new(resolve_accessors(expr, row, access)?),
            to_type: to_type.clone(),
        }),
        SqlExpr::Case {
            operand,
            when_thens,
            else_expr,
        } => Ok(SqlExpr::Case {
            operand: resolve_boxed(operand.as_deref(), row, access)?,
            when_thens: when_thens
                .iter()
                .map(|(when, then)| {
                    Ok((
                        resolve_accessors(when, row, access)?,
                        resolve_accessors(then, row, access)?,
                    ))
                })
                .collect::<crate::Result<Vec<_>>>()?,
            else_expr: resolve_boxed(else_expr.as_deref(), row, access)?,
        }),
        SqlExpr::Coalesce(items) => Ok(SqlExpr::Coalesce(resolve_list(items, row, access)?)),
        SqlExpr::NullIf(left, right) => Ok(SqlExpr::NullIf(
            Box::new(resolve_accessors(left, row, access)?),
            Box::new(resolve_accessors(right, row, access)?),
        )),
        SqlExpr::IsNull { expr, negated } => Ok(SqlExpr::IsNull {
            expr: Box::new(resolve_accessors(expr, row, access)?),
            negated: *negated,
        }),
        SqlExpr::Function { name, args } => {
            let args = resolve_list(args, row, access)?;
            let lowered = name.to_ascii_lowercase();
            match lowered.as_str() {
                "nextval" => {
                    let sequence = sequence_name(&lowered, &args, row)?;
                    Ok(SqlExpr::Literal(Value::Integer(access.nextval(&sequence)?)))
                }
                "currval" => {
                    let sequence = sequence_name(&lowered, &args, row)?;
                    Ok(SqlExpr::Literal(Value::Integer(access.currval(&sequence)?)))
                }
                "setval" => {
                    let (sequence, value) = setval_args(&args, row)?;
                    Ok(SqlExpr::Literal(Value::Integer(
                        access.setval(&sequence, value)?,
                    )))
                }
                _ => Ok(SqlExpr::Function {
                    name: name.clone(),
                    args,
                }),
            }
        }
    }
}

fn resolve_boxed(
    expr: Option<&SqlExpr>,
    row: &Value,
    access: &dyn SequenceAccess,
) -> crate::Result<Option<Box<SqlExpr>>> {
    expr.map(|e| resolve_accessors(e, row, access).map(Box::new))
        .transpose()
}

fn resolve_list(
    items: &[SqlExpr],
    row: &Value,
    access: &dyn SequenceAccess,
) -> crate::Result<Vec<SqlExpr>> {
    items
        .iter()
        .map(|item| resolve_accessors(item, row, access))
        .collect()
}

/// The single sequence-name argument of `nextval` / `currval`, evaluated
/// against the row.
fn sequence_name(function: &str, args: &[SqlExpr], row: &Value) -> crate::Result<String> {
    let [arg] = args else {
        return Err(crate::Error::PlanError {
            detail: format!(
                "{function}() takes exactly one argument, the sequence name; got {}",
                args.len()
            ),
        });
    };
    match arg.eval(row)? {
        Value::String(name) => Ok(name),
        other => Err(crate::Error::PlanError {
            detail: format!(
                "{function}() argument must evaluate to a sequence name (text); got {other:?}"
            ),
        }),
    }
}

/// The `(name, value)` arguments of `setval`, both evaluated against the row.
fn setval_args(args: &[SqlExpr], row: &Value) -> crate::Result<(String, i64)> {
    let [name_arg, value_arg] = args else {
        return Err(crate::Error::PlanError {
            detail: format!(
                "setval() takes exactly two arguments, the sequence name and the value; got {}",
                args.len()
            ),
        });
    };
    let name = match name_arg.eval(row)? {
        Value::String(name) => name,
        other => {
            return Err(crate::Error::PlanError {
                detail: format!(
                    "setval() first argument must evaluate to a sequence name (text); got {other:?}"
                ),
            });
        }
    };
    let value = coerce_i64(&value_arg.eval(row)?).ok_or_else(|| crate::Error::PlanError {
        detail: "setval() second argument must evaluate to a bigint".to_string(),
    })?;
    Ok((name, value))
}

/// An integer from an evaluated cell: an integer as is, a float with no
/// fraction inside `i64`, or text that parses as one.
fn coerce_i64(value: &Value) -> Option<i64> {
    match value {
        Value::Integer(i) => Some(*i),
        Value::Float(f) if f.fract() == 0.0 && f.abs() < i64::MAX as f64 => Some(*f as i64),
        Value::String(s) => s.trim().parse::<i64>().ok(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::collections::HashMap;

    use super::*;
    use crate::bridge::expr_eval::BinaryOp;

    /// Records every call in order and answers from a counter.
    struct FakeAccess {
        calls: RefCell<Vec<String>>,
        next: RefCell<i64>,
        last: RefCell<Option<i64>>,
    }

    impl FakeAccess {
        fn new() -> Self {
            Self {
                calls: RefCell::new(Vec::new()),
                next: RefCell::new(0),
                last: RefCell::new(None),
            }
        }
    }

    impl SequenceAccess for FakeAccess {
        fn nextval(&self, name: &str) -> crate::Result<i64> {
            self.calls.borrow_mut().push(format!("nextval:{name}"));
            let mut next = self.next.borrow_mut();
            *next += 1;
            *self.last.borrow_mut() = Some(*next);
            Ok(*next)
        }
        fn currval(&self, name: &str) -> crate::Result<i64> {
            self.calls.borrow_mut().push(format!("currval:{name}"));
            self.last
                .borrow()
                .ok_or_else(|| crate::Error::ObjectNotInPrerequisiteState {
                    object: name.to_string(),
                    detail: "not yet called".into(),
                })
        }
        fn setval(&self, name: &str, value: i64) -> crate::Result<i64> {
            self.calls
                .borrow_mut()
                .push(format!("setval:{name}={value}"));
            *self.next.borrow_mut() = value;
            Ok(value)
        }
    }

    fn call(name: &str, args: Vec<SqlExpr>) -> SqlExpr {
        SqlExpr::Function {
            name: name.to_string(),
            args,
        }
    }

    fn text(s: &str) -> SqlExpr {
        SqlExpr::Literal(Value::String(s.to_string()))
    }

    fn row() -> Value {
        Value::Object(HashMap::new())
    }

    #[test]
    fn nextval_becomes_an_integer_literal() {
        let access = FakeAccess::new();
        let out =
            resolve_accessors(&call("NEXTVAL", vec![text("s")]), &row(), &access).expect("resolve");
        assert_eq!(out, SqlExpr::Literal(Value::Integer(1)));
        assert_eq!(access.calls.borrow().as_slice(), ["nextval:s"]);
    }

    #[test]
    fn siblings_resolve_left_to_right_and_nested_inner_first() {
        let access = FakeAccess::new();
        // nextval('s') + setval('s', nextval('s') + 10)
        let expr = SqlExpr::BinaryOp {
            left: Box::new(call("nextval", vec![text("s")])),
            op: BinaryOp::Add,
            right: Box::new(call(
                "setval",
                vec![
                    text("s"),
                    SqlExpr::BinaryOp {
                        left: Box::new(call("nextval", vec![text("s")])),
                        op: BinaryOp::Add,
                        right: Box::new(SqlExpr::Literal(Value::Integer(10))),
                    },
                ],
            )),
        };
        let out = resolve_accessors(&expr, &row(), &access).expect("resolve");
        assert_eq!(
            access.calls.borrow().as_slice(),
            ["nextval:s", "nextval:s", "setval:s=12"]
        );
        assert_eq!(out.eval(&row()).expect("eval"), Value::Integer(13));
    }

    #[test]
    fn sequence_name_can_come_from_the_row() {
        let access = FakeAccess::new();
        let mut fields = HashMap::new();
        fields.insert("seq".to_string(), Value::String("orders".to_string()));
        let row = Value::Object(fields);
        resolve_accessors(
            &call("nextval", vec![SqlExpr::Column("seq".to_string())]),
            &row,
            &access,
        )
        .expect("resolve");
        assert_eq!(access.calls.borrow().as_slice(), ["nextval:orders"]);
    }

    #[test]
    fn non_text_sequence_name_is_a_plan_error_naming_the_function() {
        let access = FakeAccess::new();
        let err = resolve_accessors(
            &call("currval", vec![SqlExpr::Literal(Value::Integer(3))]),
            &row(),
            &access,
        )
        .expect_err("must fail");
        assert!(
            matches!(err, crate::Error::PlanError { ref detail } if detail.contains("currval()"))
        );
        assert!(access.calls.borrow().is_empty());
    }

    #[test]
    fn wrong_arity_is_a_plan_error() {
        let access = FakeAccess::new();
        assert!(matches!(
            resolve_accessors(&call("nextval", vec![]), &row(), &access),
            Err(crate::Error::PlanError { .. })
        ));
        assert!(matches!(
            resolve_accessors(&call("setval", vec![text("s")]), &row(), &access),
            Err(crate::Error::PlanError { .. })
        ));
    }

    #[test]
    fn setval_value_coerces_from_text_and_integral_float() {
        let access = FakeAccess::new();
        resolve_accessors(
            &call("setval", vec![text("s"), text(" 7 ")]),
            &row(),
            &access,
        )
        .expect("text");
        resolve_accessors(
            &call(
                "setval",
                vec![text("s"), SqlExpr::Literal(Value::Float(9.0))],
            ),
            &row(),
            &access,
        )
        .expect("float");
        assert!(matches!(
            resolve_accessors(
                &call(
                    "setval",
                    vec![text("s"), SqlExpr::Literal(Value::Float(9.5))]
                ),
                &row(),
                &access,
            ),
            Err(crate::Error::PlanError { .. })
        ));
        assert_eq!(
            access.calls.borrow().as_slice(),
            ["setval:s=7", "setval:s=9"]
        );
    }

    #[test]
    fn other_functions_and_leaves_pass_through_with_resolved_arguments() {
        let access = FakeAccess::new();
        let expr = SqlExpr::Coalesce(vec![
            SqlExpr::Column("x".to_string()),
            call("abs", vec![call("nextval", vec![text("s")])]),
        ]);
        let out = resolve_accessors(&expr, &row(), &access).expect("resolve");
        assert_eq!(
            out,
            SqlExpr::Coalesce(vec![
                SqlExpr::Column("x".to_string()),
                call("abs", vec![SqlExpr::Literal(Value::Integer(1))]),
            ])
        );
    }
}
