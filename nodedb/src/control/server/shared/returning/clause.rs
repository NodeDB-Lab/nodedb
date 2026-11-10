// SPDX-License-Identifier: BUSL-1.1

//! Resolving a stripped `RETURNING` item list against the planned DML target.
//!
//! Once the plans exist, the item list resolves against the target
//! collection through `nodedb_sql`: a bare column and a star ride to the Data
//! Plane as a name projection, and every other item is a Control-Plane
//! computed column the response stage evaluates per returned row. The
//! Data-Plane spec derived from that projection is what the inject step
//! attaches to the plan.

use nodedb_physical::physical_plan::{ReturningColumns, ReturningItem, ReturningSpec};
use nodedb_sql::types::SqlPlan;
use nodedb_sql::types::plan::referenced_columns;
use nodedb_sql::types::query::Projection;

pub use nodedb_sql::types::ResolvedReturning as ReturningClause;

/// The stored columns the Data Plane returns for a resolved projection:
/// `Star` when any item is a star, otherwise every named column plus every
/// base column a computed item reads, deduplicated in first-seen order.
///
/// A computed item is never evaluated on the Data Plane. The response shaper
/// evaluates it after column redaction and with the session's sequence
/// access, so a masked column never reaches an expression and a sequence
/// accessor advances once per returned row. Names are bare: a stored row
/// keys its fields by column name, so a `target.col` qualifier is dropped.
pub fn stored_columns_spec(projection: &[Projection]) -> ReturningSpec {
    let mut names: Vec<String> = Vec::with_capacity(projection.len());
    let mut push = |qualified: &str| {
        let bare = qualified.rsplit('.').next().unwrap_or(qualified);
        if !names.iter().any(|name| name == bare) {
            names.push(bare.to_owned());
        }
    };
    for entry in projection {
        match entry {
            Projection::Column(name) => push(name),
            Projection::Computed { expr, .. } | Projection::CpComputed { expr, .. } => {
                for name in referenced_columns(expr) {
                    push(&name);
                }
            }
            Projection::Star | Projection::QualifiedStar(_) => {
                return ReturningSpec {
                    columns: ReturningColumns::Star,
                };
            }
        }
    }
    ReturningSpec {
        columns: ReturningColumns::Named(
            names
                .into_iter()
                .map(|name| ReturningItem::Column { name, alias: None })
                .collect(),
        ),
    }
}

/// Read the outer statement's response clause from its resolved plan.
pub fn resolve_returning_for_plans(plans: &[SqlPlan]) -> Option<ReturningClause> {
    fn clause(plan: &SqlPlan) -> Option<&ReturningClause> {
        match plan {
            SqlPlan::Cte(cte) => clause(&cte.outer),
            _ => plan.returning(),
        }
    }
    plans.first().and_then(clause).cloned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_sql::types_expr::{BinaryOp, SqlExpr, SqlValue};

    fn column(name: &str) -> SqlExpr {
        SqlExpr::Column {
            table: None,
            name: name.into(),
        }
    }

    /// A computed item ships the base columns it reads, never the expression,
    /// so the shaper evaluates it after redaction with sequence access.
    #[test]
    fn stored_spec_ships_the_columns_an_expression_reads() {
        let projection = vec![
            Projection::Column("id".into()),
            Projection::CpComputed {
                expr: SqlExpr::BinaryOp {
                    left: Box::new(column("score")),
                    op: BinaryOp::Add,
                    right: Box::new(column("id")),
                },
                alias: "total".into(),
            },
        ];
        let spec = stored_columns_spec(&projection);
        assert_eq!(spec.source_names(), Some(vec!["id".into(), "score".into()]));
        let starred = stored_columns_spec(&[Projection::Column("id".into()), Projection::Star]);
        assert_eq!(starred.columns, ReturningColumns::Star);
    }

    /// A lone star asks the Data Plane for every stored column.
    #[test]
    fn spec_for_a_star_is_star() {
        let spec = nodedb_sql::planner::returning::returning_spec(&[Projection::Star])
            .expect("projection");
        assert_eq!(spec.columns, ReturningColumns::Star);
    }

    #[test]
    fn computed_items_keep_expression_order_and_names() {
        let projection = vec![
            Projection::Column("id".into()),
            Projection::CpComputed {
                expr: SqlExpr::BinaryOp {
                    left: Box::new(column("score")),
                    op: BinaryOp::Add,
                    right: Box::new(SqlExpr::Literal(SqlValue::Int(2))),
                },
                alias: "total".into(),
            },
        ];
        let spec = nodedb_sql::planner::returning::returning_spec(&projection).expect("projection");
        assert_eq!(spec.output_names(), Some(vec!["id".into(), "total".into()]));
        let ReturningColumns::Named(items) = spec.columns else {
            panic!("named projection")
        };
        let ReturningItem::Computed { expression, name } = &items[1] else {
            panic!("computed item")
        };
        assert_eq!(name, "total");
        let expr: nodedb_query::expr::SqlExpr =
            zerompk::from_msgpack(expression).expect("expression");
        let row = nodedb_types::Value::Object(std::collections::HashMap::from([(
            "score".into(),
            nodedb_types::Value::Integer(3),
        )]));
        assert_eq!(
            expr.eval(&row, &nodedb_crs::CrsContext::builtin_only())
                .expect("evaluate"),
            nodedb_types::Value::Integer(5)
        );
    }

    #[test]
    fn output_names_star_returns_none() {
        let spec = ReturningSpec {
            columns: ReturningColumns::Star,
        };
        assert!(spec.output_names().is_none());
    }

    #[test]
    fn output_names_named_uses_aliases() {
        let spec = ReturningSpec {
            columns: ReturningColumns::Named(vec![
                ReturningItem::Column {
                    name: "id".into(),
                    alias: None,
                },
                ReturningItem::Column {
                    name: "x".into(),
                    alias: Some("val".into()),
                },
            ]),
        };
        assert_eq!(
            spec.output_names(),
            Some(vec!["id".to_string(), "val".to_string()])
        );
    }
}
