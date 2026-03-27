//! Index-backed range scan extraction from WHERE predicates.

use datafusion::logical_expr::{BinaryExpr, Operator};
use datafusion::prelude::*;

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::DocumentOp;
use crate::control::planner::physical::PhysicalTask;
use crate::types::{TenantId, VShardId};

/// Try to convert a predicate on a non-id field into an index-backed RangeScan.
///
/// Supports:
/// - Equality: `WHERE field = value` → exact range `[value, value\0)`
/// - Range: `WHERE field > lower AND field < upper` → range `(lower, upper)`
/// - Single-bound: `WHERE field >= value` → range `[value, ...)`
pub(in crate::control::planner) fn try_range_scan_from_predicate(
    collection: &str,
    predicate: &Expr,
    tenant_id: TenantId,
    vshard: VShardId,
) -> Option<PhysicalTask> {
    match predicate {
        // Simple equality: field = value
        Expr::BinaryExpr(binary) if binary.op == Operator::Eq => {
            let (col_name, value) = extract_col_literal(binary)?;
            if col_name == "id" || col_name == "document_id" {
                return None;
            }
            let value_clean = value.trim_matches('\'').trim_matches('"').to_string();
            Some(PhysicalTask {
                tenant_id,
                vshard_id: vshard,
                plan: PhysicalPlan::Document(DocumentOp::RangeScan {
                    collection: collection.to_string(),
                    field: col_name.to_string(),
                    lower: Some(value_clean.as_bytes().to_vec()),
                    upper: Some(format!("{value_clean}\x00").as_bytes().to_vec()),
                    limit: 1000,
                }),
            })
        }

        // Range predicates on single field: GT, GTE, LT, LTE
        Expr::BinaryExpr(binary)
            if matches!(
                binary.op,
                Operator::Gt | Operator::GtEq | Operator::Lt | Operator::LtEq
            ) =>
        {
            let (col_name, value) = extract_col_literal(binary)?;
            if col_name == "id" || col_name == "document_id" {
                return None;
            }
            let value_clean = value.trim_matches('\'').trim_matches('"').to_string();
            let value_bytes = value_clean.as_bytes().to_vec();

            let (lower, upper) = match binary.op {
                Operator::Gt => (Some(format!("{value_clean}\x00").as_bytes().to_vec()), None),
                Operator::GtEq => (Some(value_bytes), None),
                Operator::Lt => (None, Some(value_bytes)),
                Operator::LtEq => (None, Some(format!("{value_clean}\x00").as_bytes().to_vec())),
                _ => return None,
            };

            Some(PhysicalTask {
                tenant_id,
                vshard_id: vshard,
                plan: PhysicalPlan::Document(DocumentOp::RangeScan {
                    collection: collection.to_string(),
                    field: col_name.to_string(),
                    lower,
                    upper,
                    limit: 1000,
                }),
            })
        }

        // AND of two range predicates on the same field
        Expr::BinaryExpr(binary) if binary.op == Operator::And => {
            let left_scan =
                try_range_scan_from_predicate(collection, &binary.left, tenant_id, vshard)?;
            let right_scan =
                try_range_scan_from_predicate(collection, &binary.right, tenant_id, vshard)?;

            if let (
                PhysicalPlan::Document(DocumentOp::RangeScan {
                    field: f1,
                    lower: l1,
                    upper: u1,
                    ..
                }),
                PhysicalPlan::Document(DocumentOp::RangeScan {
                    field: f2,
                    lower: l2,
                    upper: u2,
                    ..
                }),
            ) = (&left_scan.plan, &right_scan.plan)
                && f1 == f2
            {
                let merged_lower = l1.clone().or_else(|| l2.clone());
                let merged_upper = u1.clone().or_else(|| u2.clone());
                return Some(PhysicalTask {
                    tenant_id,
                    vshard_id: vshard,
                    plan: PhysicalPlan::Document(DocumentOp::RangeScan {
                        collection: collection.to_string(),
                        field: f1.clone(),
                        lower: merged_lower,
                        upper: merged_upper,
                        limit: 1000,
                    }),
                });
            }
            None
        }

        _ => None,
    }
}

/// Extract (column_name, literal_value_string) from a binary expression
/// where one side is a Column and the other is a Literal.
pub(in crate::control::planner) fn extract_col_literal(
    binary: &BinaryExpr,
) -> Option<(String, String)> {
    match (&*binary.left, &*binary.right) {
        (Expr::Column(col), Expr::Literal(lit, _)) => Some((col.name.clone(), lit.to_string())),
        (Expr::Literal(lit, _), Expr::Column(col)) => Some((col.name.clone(), lit.to_string())),
        _ => None,
    }
}
