// SPDX-License-Identifier: Apache-2.0

//! ON-clause / USING-clause extraction: equi-join keys + non-equi residual.

use sqlparser::ast;

use crate::error::{Result, SqlError};
use crate::parser::normalize::normalize_ident;
use crate::planner::predicate_coerce::coerce_predicate_literals;
use crate::resolver::ColumnScope;
use crate::resolver::columns::TableScope;
use crate::resolver::expr::convert_expr;
use crate::types::*;

/// (join_type, equi_keys, non-equi condition)
pub(super) type JoinSpec = (JoinType, Vec<(String, String)>, Option<SqlExpr>);

/// Extract join type, equi-join keys, and non-equi condition.
pub(super) fn extract_join_spec(op: &ast::JoinOperator, scope: &TableScope) -> Result<JoinSpec> {
    match op {
        ast::JoinOperator::Inner(constraint) | ast::JoinOperator::Join(constraint) => {
            let (keys, cond) = extract_join_constraint(constraint, scope)?;
            Ok((JoinType::Inner, keys, cond))
        }
        ast::JoinOperator::Left(constraint) | ast::JoinOperator::LeftOuter(constraint) => {
            let (keys, cond) = extract_join_constraint(constraint, scope)?;
            Ok((JoinType::Left, keys, cond))
        }
        ast::JoinOperator::Right(constraint) | ast::JoinOperator::RightOuter(constraint) => {
            let (keys, cond) = extract_join_constraint(constraint, scope)?;
            Ok((JoinType::Right, keys, cond))
        }
        ast::JoinOperator::FullOuter(constraint) => {
            let (keys, cond) = extract_join_constraint(constraint, scope)?;
            Ok((JoinType::Full, keys, cond))
        }
        ast::JoinOperator::CrossJoin(constraint) => {
            let (keys, cond) = extract_join_constraint(constraint, scope)?;
            Ok((JoinType::Cross, keys, cond))
        }
        ast::JoinOperator::Semi(constraint) | ast::JoinOperator::LeftSemi(constraint) => {
            let (keys, cond) = extract_join_constraint(constraint, scope)?;
            Ok((JoinType::Semi, keys, cond))
        }
        ast::JoinOperator::Anti(constraint) | ast::JoinOperator::LeftAnti(constraint) => {
            let (keys, cond) = extract_join_constraint(constraint, scope)?;
            Ok((JoinType::Anti, keys, cond))
        }
        _ => Err(SqlError::Unsupported {
            detail: format!("join type: {op:?}"),
        }),
    }
}

/// (equi_keys, non-equi condition)
type JoinConstraintResult = (Vec<(String, String)>, Option<SqlExpr>);

fn extract_join_constraint(
    constraint: &ast::JoinConstraint,
    scope: &TableScope,
) -> Result<JoinConstraintResult> {
    match constraint {
        ast::JoinConstraint::On(expr) => {
            let mut keys = Vec::new();
            let mut non_equi = Vec::new();
            extract_equi_keys(expr, &mut keys, &mut non_equi, scope)?;
            let cond = if non_equi.is_empty() {
                None
            } else {
                let mut combined = convert_expr(&non_equi[0], &ColumnScope::Relations(scope))?;
                for pred in &non_equi[1..] {
                    combined = SqlExpr::BinaryOp {
                        left: Box::new(combined),
                        op: crate::types::BinaryOp::And,
                        right: Box::new(convert_expr(pred, &ColumnScope::Relations(scope))?),
                    };
                }
                // A non-equi ON predicate is evaluated per candidate pair by
                // the same comparison the WHERE path uses, so its literals
                // follow the same declared-instant rule.
                coerce_predicate_literals(&mut combined, scope)?;
                Some(combined)
            };
            Ok((keys, cond))
        }
        ast::JoinConstraint::Using(columns) => {
            let keys = columns
                .iter()
                .map(|c| {
                    let name = crate::parser::normalize::normalize_object_name_checked(c)?;
                    scope.check_name(None, &name)?;
                    Ok((name.clone(), name))
                })
                .collect::<Result<Vec<_>>>()?;
            Ok((keys, None))
        }
        ast::JoinConstraint::Natural => Err(SqlError::Unsupported {
            detail: "NATURAL JOIN is not supported; use explicit ON or USING clause".into(),
        }),
        ast::JoinConstraint::None => Err(SqlError::Unsupported {
            detail: "implicit cross join (no ON/USING clause) is not supported".into(),
        }),
    }
}

fn extract_equi_keys(
    expr: &ast::Expr,
    keys: &mut Vec<(String, String)>,
    non_equi: &mut Vec<ast::Expr>,
    scope: &TableScope,
) -> Result<()> {
    match expr {
        ast::Expr::BinaryOp {
            left,
            op: ast::BinaryOperator::And,
            right,
        } => {
            extract_equi_keys(left, keys, non_equi, scope)?;
            extract_equi_keys(right, keys, non_equi, scope)?;
        }
        ast::Expr::BinaryOp {
            left,
            op: ast::BinaryOperator::Eq,
            right,
        } => {
            if let (Some(l), Some(r)) = (extract_col_ref(left), extract_col_ref(right)) {
                check_join_key(scope, &l)?;
                check_join_key(scope, &r)?;
                keys.push((l, r));
            } else {
                non_equi.push(expr.clone());
            }
        }
        _ => {
            non_equi.push(expr.clone());
        }
    }
    Ok(())
}

/// Orient equi-join keys to the FROM order: `(left_side_key, right_side_key)`.
///
/// `extract_equi_keys` records each `a = b` pair in the textual order it
/// appears in the ON clause (`(a, b)`), which is independent of which side of
/// the FROM clause each operand belongs to. Downstream physical-plan
/// conversion assumes `on.0` references the left input and `on.1` the right
/// input (it builds the hash index on the right key and probes with the left).
/// A query that writes `ON right.k = left.k` would otherwise build the index
/// on the wrong side and match zero rows.
///
/// For each key pair, if the first operand is qualified by an alias/name
/// belonging to the right side (and the second is not), swap so the right-side
/// operand is always `on.1`. `right_ids` is the set of normalized
/// alias/table-name identifiers for the right relation.
pub(super) fn orient_keys_to_sides(keys: &mut [(String, String)], right_ids: &[String]) {
    let qualifier = |k: &str| k.rsplit_once('.').map(|(q, _)| q.to_string());
    let on_right = |k: &str| {
        qualifier(k)
            .map(|q| right_ids.iter().any(|id| id == &q))
            .unwrap_or(false)
    };
    for (l, r) in keys.iter_mut() {
        // Swap only when the left operand clearly belongs to the right side and
        // the right operand does not — i.e. the pair is written reversed.
        if on_right(l) && !on_right(r) {
            std::mem::swap(l, r);
        }
    }
}

/// Reject a join key, qualified or bare, that names nothing in `scope`.
fn check_join_key(scope: &TableScope, key: &str) -> Result<()> {
    match key.rsplit_once('.') {
        Some((table, column)) => scope.check_name(Some(table), column),
        None => scope.check_name(None, key),
    }
}

fn extract_col_ref(expr: &ast::Expr) -> Option<String> {
    match expr {
        ast::Expr::Identifier(ident) => Some(normalize_ident(ident)),
        // Two-part: table.column — preserve the qualified form as the join key.
        ast::Expr::CompoundIdentifier(parts) if parts.len() == 2 => Some(format!(
            "{}.{}",
            normalize_ident(&parts[0]),
            normalize_ident(&parts[1])
        )),
        // Three or more parts: schema.table.column — reject by returning None;
        // the caller will push the expression into non_equi which convert_expr
        // then rejects with SqlError::Unsupported.
        ast::Expr::CompoundIdentifier(_) => None,
        _ => None,
    }
}
