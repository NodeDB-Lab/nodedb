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

use crate::Error;
use crate::control::planner::plan_error_map::map_plan_error;
use nodedb_sql::catalog::SqlCatalog;
use nodedb_sql::types::SqlPlan;
use nodedb_sql::types::plan::referenced_columns;
use nodedb_sql::types::query::Projection;

/// A resolved RETURNING clause: the Control-Plane projection and the
/// Data-Plane spec derived from it.
#[derive(Debug, Clone)]
pub struct ReturningClause {
    /// The announced output columns, in clause order. A `CpComputed` entry is
    /// evaluated by the response shaper per returned row.
    pub projection: Vec<Projection>,
    /// The base columns the Data Plane returns by name. Display names and
    /// drops are the shaper's job.
    pub spec: ReturningSpec,
}

/// Resolve the item text after `RETURNING` against `target`.
///
/// `tenant_id` scopes the planner error mapping so an unknown target names
/// the tenant it was looked up under.
pub fn resolve_returning_clause(
    items_sql: &str,
    target: &str,
    catalog: &dyn SqlCatalog,
    tenant_id: crate::types::TenantId,
) -> crate::Result<ReturningClause> {
    let projection = nodedb_sql::resolve_returning_items(items_sql, target, catalog)
        .map_err(|error| map_plan_error(error, tenant_id))?;
    let spec = spec_from_projection(&projection);
    Ok(ReturningClause { projection, spec })
}

/// The Data-Plane spec a resolved projection needs: `Star` for a lone star,
/// otherwise every named column plus every base column a computed entry
/// reads, deduplicated in first-seen order.
///
/// Names are bare: a stored row keys its fields by column name, and the
/// Control-Plane evaluator reads a column by its bare name too, so a
/// `target.col` qualifier is dropped here.
fn spec_from_projection(projection: &[Projection]) -> ReturningSpec {
    if matches!(projection, [Projection::Star]) {
        return ReturningSpec {
            columns: ReturningColumns::Star,
        };
    }
    let mut names: Vec<String> = Vec::with_capacity(projection.len());
    let mut push = |qualified: &str| {
        let bare = qualified.rsplit('.').next().unwrap_or(qualified);
        if !names.iter().any(|n| n == bare) {
            names.push(bare.to_string());
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
            // A star among named items still needs every stored column; the
            // shaper projects the announced list onto whatever came back.
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
                .map(|name| ReturningItem { name, alias: None })
                .collect(),
        ),
    }
}

/// The collection a DML plan's RETURNING clause projects, `None` for a plan
/// that has no such clause. Exhaustive so a new plan variant decides here.
pub fn returning_target_collection(plans: &[SqlPlan]) -> Option<String> {
    let plan = plans.first()?;
    match plan {
        SqlPlan::Insert { collection, .. }
        | SqlPlan::KvInsert { collection, .. }
        | SqlPlan::Upsert { collection, .. }
        | SqlPlan::Update { collection, .. }
        | SqlPlan::UpdateFrom { collection, .. }
        | SqlPlan::Delete { collection, .. }
        | SqlPlan::TimeseriesIngest { collection, .. }
        | SqlPlan::VectorPrimaryInsert { collection, .. } => Some(collection.clone()),
        SqlPlan::Merge { target, .. } | SqlPlan::InsertSelect { target, .. } => {
            Some(target.clone())
        }
        SqlPlan::ConstantResult { .. }
        | SqlPlan::Scan { .. }
        | SqlPlan::PointGet { .. }
        | SqlPlan::DocumentIndexLookup { .. }
        | SqlPlan::RangeScan { .. }
        | SqlPlan::Truncate { .. }
        | SqlPlan::Join { .. }
        | SqlPlan::Aggregate { .. }
        | SqlPlan::TimeseriesScan { .. }
        | SqlPlan::VectorSearch { .. }
        | SqlPlan::MultiVectorSearch { .. }
        | SqlPlan::SparseSearch { .. }
        | SqlPlan::TextSearch { .. }
        | SqlPlan::HybridSearch { .. }
        | SqlPlan::HybridSearchTriple { .. }
        | SqlPlan::SpatialScan { .. }
        | SqlPlan::Union { .. }
        | SqlPlan::Intersect { .. }
        | SqlPlan::Except { .. }
        | SqlPlan::RecursiveScan { .. }
        | SqlPlan::RecursiveValue { .. }
        | SqlPlan::Cte { .. }
        | SqlPlan::Subquery { .. }
        | SqlPlan::CreateArray { .. }
        | SqlPlan::DropArray { .. }
        | SqlPlan::AlterArray { .. }
        | SqlPlan::InsertArray { .. }
        | SqlPlan::DeleteArray { .. }
        | SqlPlan::ArraySlice { .. }
        | SqlPlan::ArrayProject { .. }
        | SqlPlan::ArrayAgg { .. }
        | SqlPlan::ArrayElementwise { .. }
        | SqlPlan::ArrayFlush { .. }
        | SqlPlan::ArrayCompact { .. }
        | SqlPlan::LateralTopK { .. }
        | SqlPlan::LateralLoop { .. }
        | SqlPlan::CreateIndex { .. }
        | SqlPlan::DropIndex { .. } => None,
    }
}

/// Resolve `returning_items` against the target the planned statement names.
///
/// `None` when the statement carries no clause. A clause on a plan with no
/// RETURNING target is refused by name rather than dropped.
pub fn resolve_returning_for_plans(
    plans: &[SqlPlan],
    returning_items: Option<&str>,
    catalog: &dyn SqlCatalog,
    tenant_id: crate::types::TenantId,
) -> crate::Result<Option<ReturningClause>> {
    let Some(items) = returning_items else {
        return Ok(None);
    };
    let Some(target) = returning_target_collection(plans) else {
        let shape = plans
            .first()
            .map_or("an empty statement", SqlPlan::variant_name);
        return Err(Error::BadRequest {
            detail: format!(
                "RETURNING is not supported on {shape}; it is supported on INSERT, UPSERT, \
                 UPDATE, DELETE, and MERGE against a collection"
            ),
        });
    };
    resolve_returning_clause(items, &target, catalog, tenant_id).map(Some)
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

    /// A lone star asks the Data Plane for every stored column.
    #[test]
    fn spec_for_a_star_is_star() {
        let spec = spec_from_projection(&[Projection::Star]);
        assert_eq!(spec.columns, ReturningColumns::Star);
    }

    /// Named columns and the base columns computed entries read reach the
    /// Data Plane as bare names, deduplicated in first-seen order, with no
    /// alias: display names are the shaper's job.
    #[test]
    fn spec_lists_named_and_referenced_base_columns_once() {
        let projection = vec![
            Projection::Column("id".into()),
            Projection::CpComputed {
                expr: SqlExpr::BinaryOp {
                    left: Box::new(column("score")),
                    op: BinaryOp::Add,
                    right: Box::new(column("id")),
                },
                alias: "d".into(),
            },
            Projection::Computed {
                expr: column("score"),
                alias: "s".into(),
            },
        ];
        let spec = spec_from_projection(&projection);
        assert_eq!(
            spec.columns,
            ReturningColumns::Named(vec![
                ReturningItem {
                    name: "id".into(),
                    alias: None,
                },
                ReturningItem {
                    name: "score".into(),
                    alias: None,
                },
            ])
        );
    }

    /// A computed entry that reads no column (a bare accessor) adds nothing
    /// to the Data Plane list.
    #[test]
    fn spec_for_a_bare_accessor_reads_only_the_named_columns() {
        let projection = vec![
            Projection::Column("id".into()),
            Projection::CpComputed {
                expr: SqlExpr::Function {
                    name: "nextval".into(),
                    args: vec![SqlExpr::Literal(SqlValue::String("s".into()))],
                    distinct: false,
                },
                alias: "n".into(),
            },
        ];
        let spec = spec_from_projection(&projection);
        assert_eq!(
            spec.columns,
            ReturningColumns::Named(vec![ReturningItem {
                name: "id".into(),
                alias: None,
            }])
        );
    }

    #[test]
    fn a_read_plan_has_no_returning_target() {
        let plan = SqlPlan::ConstantResult {
            columns: Vec::new(),
            values: Vec::new(),
            volatile: false,
        };
        assert!(returning_target_collection(&[plan]).is_none());
        assert!(returning_target_collection(&[]).is_none());
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
                ReturningItem {
                    name: "id".into(),
                    alias: None,
                },
                ReturningItem {
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
