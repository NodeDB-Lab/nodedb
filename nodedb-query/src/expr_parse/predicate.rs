// SPDX-License-Identifier: Apache-2.0

//! Postfix predicates on an operand: `IS [NOT] NULL`, `[NOT] LIKE`,
//! `[NOT] ILIKE`, `[NOT] IN (...)` and `[NOT] BETWEEN ... AND ...`.
//!
//! Each lowers onto a form the evaluator already has, the same way the SQL
//! planner lowers it. LIKE and ILIKE become the `like` and `ilike` functions.
//! IN becomes an OR of equalities. BETWEEN becomes two comparisons.

use super::error::ExprParseError;
use super::parser::{expect_keyword, parse_additive, parse_arg_list, peek_keyword};
use super::tokenizer::Token;
use crate::expr::{BinaryOp, SqlExpr};
use nodedb_types::Value;

/// Parse the predicate that follows `operand`, if one does. Returns
/// `operand` unchanged when the next token starts no predicate.
pub(super) fn parse_postfix_predicate(
    operand: SqlExpr,
    tokens: &[Token],
    pos: &mut usize,
    depth: &mut usize,
) -> Result<SqlExpr, ExprParseError> {
    if peek_keyword(tokens, *pos, "IS") {
        *pos += 1;
        let negated = peek_keyword(tokens, *pos, "NOT");
        if negated {
            *pos += 1;
        }
        expect_keyword(tokens, pos, "NULL")?;
        return Ok(SqlExpr::IsNull {
            expr: Box::new(operand),
            negated,
        });
    }

    // `NOT` after an operand only ever opens a negated predicate.
    let negated = peek_keyword(tokens, *pos, "NOT")
        && ["LIKE", "ILIKE", "IN", "BETWEEN"]
            .iter()
            .any(|keyword| peek_keyword(tokens, *pos + 1, keyword));
    let start = if negated { *pos + 1 } else { *pos };

    let predicate = if peek_keyword(tokens, start, "LIKE") || peek_keyword(tokens, start, "ILIKE") {
        let name = if peek_keyword(tokens, start, "ILIKE") {
            "ilike"
        } else {
            "like"
        };
        *pos = start + 1;
        let pattern = parse_additive(tokens, pos, depth)?;
        let mut args = vec![operand, pattern];
        if peek_keyword(tokens, *pos, "ESCAPE") {
            *pos += 1;
            args.push(parse_additive(tokens, pos, depth)?);
        }
        SqlExpr::Function {
            name: name.to_string(),
            args,
        }
    } else if peek_keyword(tokens, start, "IN") {
        *pos = start + 1;
        let list = parse_arg_list(tokens, pos, depth)?;
        // `IN` is an OR of equalities, so a negated list is built from
        // inequalities joined by AND rather than wrapped below.
        return Ok(in_list(operand, list, negated));
    } else if peek_keyword(tokens, start, "BETWEEN") {
        *pos = start + 1;
        let low = parse_additive(tokens, pos, depth)?;
        expect_keyword(tokens, pos, "AND")?;
        let high = parse_additive(tokens, pos, depth)?;
        // `NOT BETWEEN` is the outside of the range, built directly.
        return Ok(between(operand, low, high, negated));
    } else {
        return Ok(operand);
    };

    Ok(if negated {
        SqlExpr::Negate(Box::new(predicate))
    } else {
        predicate
    })
}

fn binary(left: SqlExpr, op: BinaryOp, right: SqlExpr) -> SqlExpr {
    SqlExpr::BinaryOp {
        left: Box::new(left),
        op,
        right: Box::new(right),
    }
}

/// `target IN (list)` as `target = a OR target = b ...`. `NOT IN` is
/// `target <> a AND target <> b ...`. An empty list is false, or true when
/// negated.
fn in_list(target: SqlExpr, list: Vec<SqlExpr>, negated: bool) -> SqlExpr {
    let (compare, combine) = if negated {
        (BinaryOp::NotEq, BinaryOp::And)
    } else {
        (BinaryOp::Eq, BinaryOp::Or)
    };
    list.into_iter()
        .map(|item| binary(target.clone(), compare, item))
        .reduce(|acc, next| binary(acc, combine, next))
        .unwrap_or(SqlExpr::Literal(Value::Bool(negated)))
}

/// `target BETWEEN low AND high` as `target >= low AND target <= high`.
/// `NOT BETWEEN` is `target < low OR target > high`.
fn between(target: SqlExpr, low: SqlExpr, high: SqlExpr, negated: bool) -> SqlExpr {
    if negated {
        binary(
            binary(target.clone(), BinaryOp::Lt, low),
            BinaryOp::Or,
            binary(target, BinaryOp::Gt, high),
        )
    } else {
        binary(
            binary(target.clone(), BinaryOp::GtEq, low),
            BinaryOp::And,
            binary(target, BinaryOp::LtEq, high),
        )
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::Value;

    use super::super::parser::parse_generated_expr;

    fn eval(text: &str, doc: serde_json::Value) -> Value {
        let (expr, _) = parse_generated_expr(text).expect("parses");
        expr.eval(&Value::from(doc), &nodedb_crs::CrsContext::builtin_only())
            .expect("evaluates")
    }

    #[test]
    fn like_and_not_like() {
        let doc = serde_json::json!({"email": "a@b.c"});
        assert_eq!(eval("email LIKE '%@%'", doc.clone()), Value::Bool(true));
        assert_eq!(eval("email NOT LIKE '%@%'", doc), Value::Bool(false));
        let plain = serde_json::json!({"email": "nobody"});
        assert_eq!(eval("email LIKE '%@%'", plain), Value::Bool(false));
    }

    #[test]
    fn ilike_ignores_case() {
        let doc = serde_json::json!({"name": "Alice"});
        assert_eq!(eval("name ILIKE 'ali%'", doc.clone()), Value::Bool(true));
        assert_eq!(eval("name LIKE 'ali%'", doc), Value::Bool(false));
    }

    #[test]
    fn like_with_escape() {
        let doc = serde_json::json!({"code": "50%"});
        assert_eq!(eval("code LIKE '50!%' ESCAPE '!'", doc), Value::Bool(true));
        let other = serde_json::json!({"code": "500"});
        assert_eq!(
            eval("code LIKE '50!%' ESCAPE '!'", other),
            Value::Bool(false)
        );
    }

    #[test]
    fn like_on_null_is_null() {
        let doc = serde_json::json!({"email": null});
        assert_eq!(eval("email LIKE '%@%'", doc), Value::Null);
    }

    #[test]
    fn in_and_not_in() {
        let doc = serde_json::json!({"role": "admin"});
        assert_eq!(
            eval("role IN ('admin', 'user')", doc.clone()),
            Value::Bool(true)
        );
        assert_eq!(
            eval("role NOT IN ('admin', 'user')", doc.clone()),
            Value::Bool(false)
        );
        assert_eq!(eval("role IN ('guest')", doc), Value::Bool(false));
    }

    #[test]
    fn between_and_not_between() {
        let doc = serde_json::json!({"age": 30});
        assert_eq!(
            eval("age BETWEEN 18 AND 65", doc.clone()),
            Value::Bool(true)
        );
        assert_eq!(
            eval("age NOT BETWEEN 18 AND 65", doc.clone()),
            Value::Bool(false)
        );
        assert_eq!(eval("age BETWEEN 40 AND 65", doc), Value::Bool(false));
    }

    #[test]
    fn between_binds_inside_and() {
        let doc = serde_json::json!({"age": 30, "active": true});
        assert_eq!(
            eval("age BETWEEN 18 AND 65 AND active = true", doc),
            Value::Bool(true)
        );
    }

    #[test]
    fn is_null_and_is_not_null() {
        let doc = serde_json::json!({"a": null, "b": 1});
        assert_eq!(eval("a IS NULL", doc.clone()), Value::Bool(true));
        assert_eq!(eval("b IS NULL", doc.clone()), Value::Bool(false));
        assert_eq!(eval("b IS NOT NULL", doc), Value::Bool(true));
    }

    #[test]
    fn not_applies_to_the_whole_comparison() {
        let doc = serde_json::json!({"x": 1});
        assert_eq!(eval("NOT x = 2", doc.clone()), Value::Bool(true));
        assert_eq!(eval("NOT x = 1 OR x = 1", doc), Value::Bool(true));
    }

    #[test]
    fn columns_inside_predicates_are_dependencies() {
        let (_, deps) =
            parse_generated_expr("a LIKE b AND c IN (d, 1) AND e BETWEEN f AND g").expect("parses");
        assert_eq!(deps, vec!["a", "b", "c", "d", "e", "f", "g"]);
    }
}
