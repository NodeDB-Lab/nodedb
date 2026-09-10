// SPDX-License-Identifier: Apache-2.0

//! Relational tail for plans that cannot absorb ORDER BY / LIMIT / OFFSET.
//!
//! Most plan variants carry their own sort keys and row bound. The ones that
//! do not — search plans ranked by relevance, joins, lateral loops — used to
//! have the clause silently dropped, answering a bounded, ordered query with
//! unbounded rows in whatever order the engine produced. Wrapping them in
//! [`SqlPlan::Subquery`] routes the clause through the same
//! materialize-then-post-process node derived tables already use.
//!
//! A tail can only sort by a column the body still emits. A body that narrows
//! its rows to the SELECT list (`SELECT a.name … ORDER BY a.rank`) would drop
//! the sort column before the tail ever sees it, so the body is widened to
//! keep it and the tail carries the original projection to strip it again —
//! the tail projects *after* it sorts, so the narrowing costs nothing.

use crate::error::{Result, SqlError};
use crate::planner::qualified_name;
use crate::types::{Projection, SortKey, SqlExpr, SqlPlan};

/// Wrap `input` in a post-processing node that applies `sort_keys`, `offset`
/// and `limit` over its materialized rows, in that order.
///
/// Filters are left empty so the body's own predicates pass through untouched.
/// The projection is empty unless the body had to be widened to keep a sort
/// column, in which case it is the body's original projection.
pub(in crate::planner::select) fn post_process(
    input: SqlPlan,
    sort_keys: Vec<SortKey>,
    limit: Option<usize>,
    offset: usize,
) -> Result<SqlPlan> {
    let mut input = input;
    let projection = retain_sort_columns(&mut input, &sort_keys)?;
    Ok(SqlPlan::Subquery {
        input: Box::new(input),
        filters: Vec::new(),
        projection,
        window_functions: Vec::new(),
        sort_keys,
        offset,
        distinct: false,
        limit,
    })
}

/// Make every sort-key column survive `input`'s own projection.
///
/// Returns the projection the tail must apply: empty when the body already
/// emits every sort column (nothing was added, so nothing needs stripping),
/// otherwise the body's projection as it stood before widening.
fn retain_sort_columns(input: &mut SqlPlan, sort_keys: &[SortKey]) -> Result<Vec<Projection>> {
    if sort_keys.is_empty() {
        return Ok(Vec::new());
    }
    let variant = input.variant_name();

    // An array body narrows to named attributes rather than a SELECT list, so
    // a sort column it does not already emit cannot be added back.
    if let SqlPlan::ArraySlice {
        attr_projection, ..
    }
    | SqlPlan::ArrayProject {
        attr_projection, ..
    } = input
        && !attr_projection.is_empty()
        && let Some(missing) = first_missing_attr(attr_projection, sort_keys)
    {
        return Err(SqlError::Unsupported {
            detail: format!(
                "ORDER BY '{missing}' over a {variant} plan that projects only \
                 [{}]; add the column to the attribute list",
                attr_projection.join(", ")
            ),
        });
    }

    let Some(projection) = body_projection_mut(input) else {
        // No SELECT-list projection to narrow rows with — every column the
        // body produces reaches the tail.
        return Ok(Vec::new());
    };
    // An empty or star projection keeps every column.
    if projection.is_empty()
        || projection
            .iter()
            .any(|p| matches!(p, Projection::Star | Projection::QualifiedStar(_)))
    {
        return Ok(Vec::new());
    }

    let original = projection.clone();
    let mut widened = false;
    for key in sort_keys {
        match &key.expr {
            SqlExpr::Column { table, name } => {
                if !projects_column(&original, table.as_deref(), name) {
                    projection.push(Projection::Column(qualified_name(table.as_deref(), name)));
                    widened = true;
                }
            }
            // A computed sort expression's column references cannot be
            // enumerated here, so there is no way to prove they survive a
            // narrowing body. Reject rather than sort by a value that
            // resolves to NULL on every row and leaves the rows in whatever
            // order the body produced.
            _ => {
                return Err(SqlError::Unsupported {
                    detail: format!(
                        "ORDER BY over a computed expression on a {variant} plan whose \
                         projection narrows the row; project the sort expression's columns \
                         in the SELECT list"
                    ),
                });
            }
        }
    }

    Ok(if widened { original } else { Vec::new() })
}

/// Whether `projection` already emits the column `table.name`, under either its
/// own name or a computed alias.
fn projects_column(projection: &[Projection], table: Option<&str>, name: &str) -> bool {
    let qualified = qualified_name(table, name);
    projection.iter().any(|p| match p {
        Projection::Column(projected) => {
            projected.eq_ignore_ascii_case(&qualified) || bare(projected).eq_ignore_ascii_case(name)
        }
        Projection::Computed { alias, .. } => alias.eq_ignore_ascii_case(name),
        Projection::Star | Projection::QualifiedStar(_) => true,
    })
}

/// The column name without its table qualifier.
fn bare(name: &str) -> &str {
    name.rsplit('.').next().unwrap_or(name)
}

/// The first sort-key name an array body's attribute list does not emit.
fn first_missing_attr(attrs: &[String], sort_keys: &[SortKey]) -> Option<String> {
    sort_keys.iter().find_map(|key| match &key.expr {
        SqlExpr::Column { name, .. } => {
            if attrs.iter().any(|a| a.eq_ignore_ascii_case(name)) {
                None
            } else {
                Some(name.clone())
            }
        }
        _ => None,
    })
}

/// The SELECT-list projection a plan narrows its rows with, when it has one.
///
/// `None` means the variant emits whatever columns its source produced, so a
/// sort key cannot be projected away.
fn body_projection_mut(plan: &mut SqlPlan) -> Option<&mut Vec<Projection>> {
    match plan {
        SqlPlan::Scan { projection, .. }
        | SqlPlan::Join { projection, .. }
        | SqlPlan::PointGet { projection, .. }
        | SqlPlan::RangeScan { projection, .. }
        | SqlPlan::DocumentIndexLookup { projection, .. }
        | SqlPlan::TimeseriesScan { projection, .. }
        | SqlPlan::VectorSearch { projection, .. }
        | SqlPlan::MultiVectorSearch { projection, .. }
        | SqlPlan::SparseSearch { projection, .. }
        | SqlPlan::TextSearch { projection, .. }
        | SqlPlan::HybridSearch { projection, .. }
        | SqlPlan::HybridSearchTriple { projection, .. }
        | SqlPlan::SpatialScan { projection, .. }
        | SqlPlan::RecursiveScan { projection, .. }
        | SqlPlan::LateralTopK { projection, .. }
        | SqlPlan::LateralLoop { projection, .. }
        | SqlPlan::Subquery { projection, .. } => Some(projection),
        _ => None,
    }
}
