//! ON-clause / USING-clause extraction: equi-join keys + non-equi residual.

use sqlparser::ast;

use crate::error::{Result, SqlError};
use crate::parser::normalize::normalize_ident;
use crate::resolver::expr::convert_expr;
use crate::types::*;

/// (join_type, equi_keys, non-equi condition)
pub(super) type JoinSpec = (JoinType, Vec<(String, String)>, Option<SqlExpr>);

/// Extract join type, equi-join keys, and non-equi condition.
pub(super) fn extract_join_spec(op: &ast::JoinOperator) -> Result<JoinSpec> {
    match op {
        ast::JoinOperator::Inner(constraint) | ast::JoinOperator::Join(constraint) => {
            let (keys, cond) = extract_join_constraint(constraint)?;
            Ok((JoinType::Inner, keys, cond))
        }
        ast::JoinOperator::Left(constraint) | ast::JoinOperator::LeftOuter(constraint) => {
            let (keys, cond) = extract_join_constraint(constraint)?;
            Ok((JoinType::Left, keys, cond))
        }
        ast::JoinOperator::Right(constraint) | ast::JoinOperator::RightOuter(constraint) => {
            let (keys, cond) = extract_join_constraint(constraint)?;
            Ok((JoinType::Right, keys, cond))
        }
        ast::JoinOperator::FullOuter(constraint) => {
            let (keys, cond) = extract_join_constraint(constraint)?;
            Ok((JoinType::Full, keys, cond))
        }
        ast::JoinOperator::CrossJoin(constraint) => {
            let (keys, cond) = extract_join_constraint(constraint)?;
            Ok((JoinType::Cross, keys, cond))
        }
        ast::JoinOperator::Semi(constraint) | ast::JoinOperator::LeftSemi(constraint) => {
            let (keys, cond) = extract_join_constraint(constraint)?;
            Ok((JoinType::Semi, keys, cond))
        }
        ast::JoinOperator::Anti(constraint) | ast::JoinOperator::LeftAnti(constraint) => {
            let (keys, cond) = extract_join_constraint(constraint)?;
            Ok((JoinType::Anti, keys, cond))
        }
        _ => Err(SqlError::Unsupported {
            detail: format!("join type: {op:?}"),
        }),
    }
}

/// (equi_keys, non-equi condition)
type JoinConstraintResult = (Vec<(String, String)>, Option<SqlExpr>);

fn extract_join_constraint(constraint: &ast::JoinConstraint) -> Result<JoinConstraintResult> {
    match constraint {
        ast::JoinConstraint::On(expr) => {
            let mut keys = Vec::new();
            let mut non_equi = Vec::new();
            extract_equi_keys(expr, &mut keys, &mut non_equi)?;
            let cond = if non_equi.is_empty() {
                None
            } else {
                let mut combined = convert_expr(&non_equi[0])?;
                for pred in &non_equi[1..] {
                    combined = SqlExpr::BinaryOp {
                        left: Box::new(combined),
                        op: crate::types::BinaryOp::And,
                        right: Box::new(convert_expr(pred)?),
                    };
                }
                Some(combined)
            };
            Ok((keys, cond))
        }
        ast::JoinConstraint::Using(columns) => {
            let keys = columns
                .iter()
                .map(|c| {
                    let name = crate::parser::normalize::normalize_object_name(c);
                    (name.clone(), name)
                })
                .collect();
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
) -> Result<()> {
    match expr {
        ast::Expr::BinaryOp {
            left,
            op: ast::BinaryOperator::And,
            right,
        } => {
            extract_equi_keys(left, keys, non_equi)?;
            extract_equi_keys(right, keys, non_equi)?;
        }
        ast::Expr::BinaryOp {
            left,
            op: ast::BinaryOperator::Eq,
            right,
        } => {
            if let (Some(l), Some(r)) = (extract_col_ref(left), extract_col_ref(right)) {
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

fn extract_col_ref(expr: &ast::Expr) -> Option<String> {
    match expr {
        ast::Expr::Identifier(ident) => Some(normalize_ident(ident)),
        ast::Expr::CompoundIdentifier(parts) if !parts.is_empty() => Some(
            parts
                .iter()
                .map(normalize_ident)
                .collect::<Vec<_>>()
                .join("."),
        ),
        _ => None,
    }
}
