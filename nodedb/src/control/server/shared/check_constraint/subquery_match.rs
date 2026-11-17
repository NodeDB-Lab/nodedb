// SPDX-License-Identifier: BUSL-1.1

//! Builds the restricted `COUNT(*)` match query of a subquery CHECK.

use sqlparser::ast::{Expr, Query, Select, SelectItem, SetExpr};

use crate::control::security::catalog::types::CheckConstraintDef;
use crate::control::server::shared::ddl::result::DdlError;

use super::subquery::evaluation_error;

pub(super) fn build_match_query(
    lhs: &Expr,
    subquery: &Query,
    negated: bool,
    constraint: &CheckConstraintDef,
) -> Result<String, DdlError> {
    if subquery.with.is_some()
        || subquery.order_by.is_some()
        || subquery.limit_clause.is_some()
        || subquery.fetch.is_some()
        || !subquery.locks.is_empty()
    {
        return Err(evaluation_error(
            constraint,
            "IN subquery does not support WITH, ORDER BY, LIMIT, FETCH, or locks",
        ));
    }
    let SetExpr::Select(select) = subquery.body.as_ref() else {
        return Err(evaluation_error(
            constraint,
            "IN subquery must have a SELECT body",
        ));
    };
    validate_subquery_shape(select, constraint)?;
    let projection = match select.projection.as_slice() {
        [SelectItem::UnnamedExpr(expr)] => expr,
        [SelectItem::ExprWithAlias { expr, .. }] => expr,
        _ => {
            return Err(evaluation_error(
                constraint,
                "IN subquery must select exactly one expression",
            ));
        }
    };

    let projection_sql = canonical_check_expr_sql(projection);
    let lhs_sql = canonical_check_expr_sql(lhs);
    let from_sql = canonical_check_from_sql(&select.from[0]);
    // IN is accepted by CHECK when a row matches or when a NULL row makes the
    // result UNKNOWN. NOT IN is violated only by an actual equal row; a NULL
    // without a match also yields UNKNOWN and therefore passes CHECK.
    match (negated, select.selection.as_ref()) {
        (true, Some(where_expr)) => Ok(format!(
            "SELECT COUNT(*) AS cnt FROM {from_sql} WHERE ({projection_sql}) = ({lhs_sql}) AND ({})",
            canonical_check_expr_sql(where_expr)
        )),
        (true, None) => Ok(format!(
            "SELECT COUNT(*) AS cnt FROM {from_sql} WHERE ({projection_sql}) = ({lhs_sql})"
        )),
        (false, Some(where_expr)) => Ok(format!(
            "SELECT COUNT(*) AS cnt FROM {from_sql} WHERE (({projection_sql}) = ({lhs_sql}) OR ({projection_sql}) IS NULL) AND ({})",
            canonical_check_expr_sql(where_expr)
        )),
        (false, None) => Ok(format!(
            "SELECT COUNT(*) AS cnt FROM {from_sql} WHERE (({projection_sql}) = ({lhs_sql}) OR ({projection_sql}) IS NULL)"
        )),
    }
}

fn canonical_check_expr_sql(expr: &Expr) -> String {
    expr.to_string()
}

fn canonical_check_from_sql(from: &sqlparser::ast::TableWithJoins) -> String {
    from.to_string()
}

fn validate_subquery_shape(
    select: &Select,
    constraint: &CheckConstraintDef,
) -> Result<(), DdlError> {
    if select.from.len() != 1 || !select.from[0].joins.is_empty() {
        return Err(evaluation_error(
            constraint,
            "IN subquery must have exactly one ordinary FROM relation without joins",
        ));
    }
    if !matches!(
        select.from[0].relation,
        sqlparser::ast::TableFactor::Table { args: None, .. }
    ) {
        return Err(evaluation_error(
            constraint,
            "IN subquery FROM must be an ordinary table relation",
        ));
    }
    let has_grouping = match &select.group_by {
        sqlparser::ast::GroupByExpr::All(_) => true,
        sqlparser::ast::GroupByExpr::Expressions(expressions, _) => !expressions.is_empty(),
    };
    if select.having.is_some() || has_grouping || select.distinct.is_some() {
        return Err(evaluation_error(
            constraint,
            "IN subquery does not support grouping, HAVING, or DISTINCT",
        ));
    }
    Ok(())
}
