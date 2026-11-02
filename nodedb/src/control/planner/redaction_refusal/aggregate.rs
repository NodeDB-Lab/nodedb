// SPDX-License-Identifier: BUSL-1.1

//! Refusal of aggregates computed over a redacted column.
//!
//! An aggregate is evaluated in the Data Plane over the STORED values, which
//! are never redacted — redaction is a Control-Plane rewrite of the result
//! rows. Masking the scalar an aggregate produces therefore protects nothing:
//! `MIN(ssn)` has already disclosed a stored `ssn`, and `SUM(salary)` a fact
//! about every stored `salary`. There is no correct masking for these, so the
//! query is refused instead.

use nodedb_physical::physical_plan::AggregateSpec;
use nodedb_query::expr::SqlExpr;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::server::response_shape::redaction::plan_source_collections;

use super::lookup::RefusalCtx;

/// Refuse when any spec in `specs` aggregates a redacted column of `collection`.
pub(super) fn refuse_aggregates(
    ctx: &RefusalCtx<'_>,
    collection: &nodedb_types::QualifiedCollection,
    specs: &[AggregateSpec],
) -> crate::Result<()> {
    refuse_specs(ctx, collection.as_str(), specs)
}

/// [`refuse_aggregates`] over a collection named by its stored string.
fn refuse_specs(
    ctx: &RefusalCtx<'_>,
    collection: &str,
    specs: &[AggregateSpec],
) -> crate::Result<()> {
    if collection.is_empty() {
        return Ok(());
    }
    for spec in specs {
        for field in read_fields(spec) {
            if ctx.field_is_redacted(collection, field) {
                return Err(refusal(ctx, collection, field));
            }
        }
    }
    Ok(())
}

/// Refuse when an aggregate over an input plan reads a redacted column of
/// any relation that input reads.
///
/// The aggregate names no collection of its own. Its input's relations are
/// the ones the result-path masking hook keys on
/// ([`plan_source_collections`]), each with the qualifier its columns carry:
/// a join's sides, a post-processed or set-operation body, a named provider
/// relation, and the sources of rows the coordinator gathered.
pub(super) fn refuse_input_aggregates(
    ctx: &RefusalCtx<'_>,
    input: &PhysicalPlan,
    specs: &[AggregateSpec],
) -> crate::Result<()> {
    let sources = plan_source_collections(input, ctx.database_id);
    for spec in specs {
        for field in read_fields(spec) {
            refuse_source_field(ctx, &sources, field)?;
        }
    }
    Ok(())
}

/// Refuse when `field` is a redacted column of one of `sources`, per
/// [`RefusalCtx::redacted_source`].
fn refuse_source_field(
    ctx: &RefusalCtx<'_>,
    sources: &[(String, String)],
    field: &str,
) -> crate::Result<()> {
    match ctx.redacted_source(sources, field) {
        Some(collection) => Err(refusal(ctx, collection, field)),
        None => Ok(()),
    }
}

/// Refuse when a grouped timeseries scan aggregates a redacted column.
pub(super) fn refuse_timeseries_aggregates(
    ctx: &RefusalCtx<'_>,
    collection: &nodedb_types::QualifiedCollection,
    aggregates: &[(String, String)],
) -> crate::Result<()> {
    let collection = collection.as_str();
    if collection.is_empty() {
        return Ok(());
    }
    for (_function, field) in aggregates {
        if !is_count_star(field) && ctx.field_is_redacted(collection, field) {
            return Err(refusal(ctx, collection, field));
        }
    }
    Ok(())
}

/// Refuse when a facet request counts a redacted column.
///
/// Facet counts return `[{value, count}]` per field — the stored values
/// themselves, in a nested array the row-level masking hook does not reach.
pub(super) fn refuse_facet_fields(
    ctx: &RefusalCtx<'_>,
    collection: &nodedb_types::QualifiedCollection,
    fields: &[String],
) -> crate::Result<()> {
    let collection = collection.as_str();
    if collection.is_empty() {
        return Ok(());
    }
    for field in fields {
        if ctx.field_is_redacted(collection, field) {
            return Err(ctx.refusal(format!(
                "column '{field}' on '{collection}' is redacted for this role: faceting a \
                 redacted column is not permitted — facet counts enumerate the stored values \
                 the policy protects"
            )));
        }
    }
    Ok(())
}

fn refusal(ctx: &RefusalCtx<'_>, collection: &str, field: &str) -> crate::Error {
    ctx.refusal(format!(
        "column '{field}' on '{collection}' is redacted for this role: aggregating a redacted \
         column is not permitted — the aggregate is computed over the stored values, so \
         masking its result would not protect them"
    ))
}

/// Every stored column one aggregate spec reads.
///
/// `field` is the simple field-based argument; `expr` is the optional
/// per-document expression evaluated before aggregating (`MIN(LENGTH(ssn))`),
/// whose column references are read exactly the same way.
fn read_fields(spec: &AggregateSpec) -> Vec<&str> {
    let mut fields = Vec::new();
    if !is_count_star(&spec.field) {
        fields.push(spec.field.as_str());
    }
    if let Some(expr) = &spec.expr {
        collect_columns(expr, &mut fields);
    }
    fields
}

/// True for an aggregate argument that reads no column value.
///
/// `COUNT(*)` reaches the plan as the sentinel field `"*"` (see
/// `AggregateSpec::field`), and an argument-less aggregate as the empty
/// string. Neither discloses anything about a redacted column, so neither is
/// refused.
fn is_count_star(field: &str) -> bool {
    field == "*" || field.is_empty()
}

/// Collect every column reference in `expr`.
pub(crate) fn collect_columns<'e>(expr: &'e SqlExpr, out: &mut Vec<&'e str>) {
    match expr {
        SqlExpr::Column(name) | SqlExpr::OldColumn(name) | SqlExpr::ExcludedColumn(name) => {
            out.push(name.as_str());
        }
        SqlExpr::Literal(_) => {}
        SqlExpr::BinaryOp { left, right, .. } => {
            collect_columns(left, out);
            collect_columns(right, out);
        }
        SqlExpr::Negate(inner) => collect_columns(inner, out),
        SqlExpr::Function { args, .. } => {
            for arg in args {
                collect_columns(arg, out);
            }
        }
        SqlExpr::Cast { expr, .. } => collect_columns(expr, out),
        SqlExpr::Case {
            operand,
            when_thens,
            else_expr,
        } => {
            if let Some(operand) = operand {
                collect_columns(operand, out);
            }
            for (when, then) in when_thens {
                collect_columns(when, out);
                collect_columns(then, out);
            }
            if let Some(else_expr) = else_expr {
                collect_columns(else_expr, out);
            }
        }
        SqlExpr::Coalesce(args) => {
            for arg in args {
                collect_columns(arg, out);
            }
        }
        SqlExpr::NullIf(left, right) => {
            collect_columns(left, out);
            collect_columns(right, out);
        }
        SqlExpr::IsNull { expr, .. } => collect_columns(expr, out),
    }
}
