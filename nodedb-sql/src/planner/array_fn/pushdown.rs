// SPDX-License-Identifier: Apache-2.0

//! Pushdown of an outer query's constraints into the slice it reads.
//!
//! The outer query over an `ARRAY_SLICE` relation keeps every clause in its
//! own tail, so results never depend on what this module folds. Folding only
//! narrows what the Data Plane reads:
//!
//! - An outer `LIMIT n` caps the slice when nothing else in the outer query
//!   reads past the cut. The cap becomes the smaller nonzero of the two.
//! - A top-level `AND` conjunct that bounds a dim narrows that dim's range.
//!   This runs only when the slice has no row cap of its own: under a cap, a
//!   narrower range changes which cells the cap keeps.

use crate::types::{
    ArrayCatalogView, ArraySlicePlan, BinaryOp, CompareOp, Filter, FilterExpr, JoinType, SqlExpr,
    SqlPlan, SqlValue,
};
use crate::types_array::{ArrayCoordLiteral, ArrayDimType, ArrayDomainBound, NamedDimRange};

/// Narrow `slice` by the constraints `outer` places on `relation`, the name
/// the outer query reads the slice's rows by.
pub fn push_down_into_slice(
    slice: &mut ArraySlicePlan,
    view: &ArrayCatalogView,
    outer: &SqlPlan,
    relation: &str,
) {
    if slice.limit == 0
        && let Some(filters) = relation_scan_filters(outer, relation)
    {
        for filter in filters {
            narrow_by_filter(slice, view, &filter.expr);
        }
    }
    if let Some(limit) = foldable_limit(outer, relation) {
        slice.limit = folded_cap(slice.limit, limit);
    }
}

/// The filters of the scan that reads `relation` as the outer query's row
/// source. The scan sits at the root, under an aggregate, or as the probe
/// side of a join a WHERE subquery adds. Each filter there is a conjunct
/// every output row satisfies.
fn relation_scan_filters<'a>(plan: &'a SqlPlan, relation: &str) -> Option<&'a [Filter]> {
    if let SqlPlan::Scan {
        collection,
        filters,
        ..
    } = plan
        && collection == relation
    {
        return Some(filters);
    }
    if let SqlPlan::Aggregate { input, .. } = plan {
        return relation_scan_filters(input, relation);
    }
    if let SqlPlan::Join {
        left, join_type, ..
    } = plan
        && matches!(join_type, JoinType::Semi | JoinType::Anti | JoinType::Cross)
    {
        return relation_scan_filters(left, relation);
    }
    None
}

/// The outer `LIMIT`, when the outer query is a plain read of `relation`
/// that no filter, sort, offset, DISTINCT or window reads past.
fn foldable_limit(plan: &SqlPlan, relation: &str) -> Option<usize> {
    let SqlPlan::Scan {
        collection,
        filters,
        sort_keys,
        limit: Some(limit),
        offset: 0,
        distinct: false,
        window_functions,
        ..
    } = plan
    else {
        return None;
    };
    (collection == relation
        && filters.is_empty()
        && sort_keys.is_empty()
        && window_functions.is_empty()
        && *limit > 0)
        .then_some(*limit)
}

/// The smaller nonzero of the slice cap and the outer limit. A limit past
/// the `u32` cap range leaves an uncapped slice uncapped.
fn folded_cap(cap: u32, limit: usize) -> u32 {
    match u32::try_from(limit) {
        Ok(limit) if cap == 0 || limit < cap => limit,
        Ok(_) | Err(_) => cap,
    }
}

/// Apply every top-level conjunct of `expr` that bounds a dim.
fn narrow_by_filter(slice: &mut ArraySlicePlan, view: &ArrayCatalogView, expr: &FilterExpr) {
    match expr {
        FilterExpr::Expr(sql) => narrow_by_expr(slice, view, sql),
        FilterExpr::And(filters) => {
            for filter in filters {
                narrow_by_filter(slice, view, &filter.expr);
            }
        }
        FilterExpr::Comparison { field, op, value } => {
            let op = match op {
                CompareOp::Eq => Some(BinaryOp::Eq),
                CompareOp::Gt => Some(BinaryOp::Gt),
                CompareOp::Ge => Some(BinaryOp::Ge),
                CompareOp::Lt => Some(BinaryOp::Lt),
                CompareOp::Le => Some(BinaryOp::Le),
                CompareOp::Ne => None,
            };
            if let Some(op) = op {
                narrow_dim(slice, view, field, op, value);
            }
        }
        FilterExpr::Between { field, low, high } => {
            narrow_dim(slice, view, field, BinaryOp::Ge, low);
            narrow_dim(slice, view, field, BinaryOp::Le, high);
        }
        FilterExpr::InList { .. }
        | FilterExpr::IsNull { .. }
        | FilterExpr::IsNotNull { .. }
        | FilterExpr::Or(_)
        | FilterExpr::Not(_) => {}
    }
}

fn narrow_by_expr(slice: &mut ArraySlicePlan, view: &ArrayCatalogView, expr: &SqlExpr) {
    match expr {
        SqlExpr::BinaryOp {
            left,
            op: BinaryOp::And,
            right,
        } => {
            narrow_by_expr(slice, view, left);
            narrow_by_expr(slice, view, right);
        }
        SqlExpr::BinaryOp { left, op, right } => {
            if let (SqlExpr::Column { name, .. }, SqlExpr::Literal(value)) =
                (left.as_ref(), right.as_ref())
            {
                narrow_dim(slice, view, name, *op, value);
            }
        }
        SqlExpr::Between {
            expr,
            low,
            high,
            negated: false,
        } => {
            if let (SqlExpr::Column { name, .. }, SqlExpr::Literal(lo), SqlExpr::Literal(hi)) =
                (expr.as_ref(), low.as_ref(), high.as_ref())
            {
                narrow_dim(slice, view, name, BinaryOp::Ge, lo);
                narrow_dim(slice, view, name, BinaryOp::Le, hi);
            }
        }
        _ => {}
    }
}

/// A dim bound on one numeric axis. Integer and timestamp dims compare as
/// `i64`, float dims as `f64`.
#[derive(Clone, Copy, PartialEq, PartialOrd)]
enum Num {
    Int(i64),
    Float(f64),
}

/// Tighten the range of dim `name` by `name <op> value`.
///
/// The new range holds every coordinate the predicate admits, and may hold
/// more: a strict bound on a float dim stays inclusive. A predicate that
/// would empty the range leaves it unchanged; the outer filter answers it.
fn narrow_dim(
    slice: &mut ArraySlicePlan,
    view: &ArrayCatalogView,
    name: &str,
    op: BinaryOp,
    value: &SqlValue,
) {
    let Some(dim) = view.dims.iter().find(|d| d.name == name) else {
        return;
    };
    let Some((lo_bound, hi_bound)) = predicate_bounds(dim.dtype, op, value) else {
        return;
    };
    let existing = slice.slice.dim_ranges.iter().position(|r| r.dim == name);
    let (lo, hi) = match existing {
        Some(i) => {
            let range = &slice.slice.dim_ranges[i];
            (
                literal_num(dim.dtype, &range.lo),
                literal_num(dim.dtype, &range.hi),
            )
        }
        None => (
            domain_num(dim.dtype, &dim.lo),
            domain_num(dim.dtype, &dim.hi),
        ),
    };
    let (Some(lo), Some(hi)) = (lo, hi) else {
        return;
    };
    let lo = match lo_bound {
        Some(bound) if bound > lo => bound,
        Some(_) | None => lo,
    };
    let hi = match hi_bound {
        Some(bound) if bound < hi => bound,
        Some(_) | None => hi,
    };
    if lo > hi {
        return;
    }
    let range = NamedDimRange {
        dim: name.to_string(),
        lo: num_literal(lo),
        hi: num_literal(hi),
    };
    match existing {
        Some(i) => slice.slice.dim_ranges[i] = range,
        None => slice.slice.dim_ranges.push(range),
    }
}

/// The `(lower, upper)` bounds `dim <op> value` places on a dim of type
/// `dtype`, each `None` when the predicate leaves that side open. `None`
/// when the predicate bounds nothing this module can express.
fn predicate_bounds(
    dtype: ArrayDimType,
    op: BinaryOp,
    value: &SqlValue,
) -> Option<(Option<Num>, Option<Num>)> {
    let (floor, ceil, strict_step) = match (dtype, value) {
        (ArrayDimType::Int64, SqlValue::Int(v)) => (Num::Int(*v), Num::Int(*v), true),
        (ArrayDimType::Int64, SqlValue::Float(f)) if f.is_finite() => {
            (Num::Int(f.floor() as i64), Num::Int(f.ceil() as i64), false)
        }
        (ArrayDimType::Float64, SqlValue::Int(v)) => {
            (Num::Float(*v as f64), Num::Float(*v as f64), false)
        }
        (ArrayDimType::Float64, SqlValue::Float(f)) if f.is_finite() => {
            (Num::Float(*f), Num::Float(*f), false)
        }
        (ArrayDimType::TimestampMs, SqlValue::Timestamp(at) | SqlValue::Timestamptz(at)) => (
            Num::Int(at.micros.div_euclid(1000)),
            Num::Int(-(-at.micros).div_euclid(1000)),
            false,
        ),
        _ => return None,
    };
    // A strict bound on an integer literal moves one step inward. Every
    // other strict bound stays inclusive, which only widens the range.
    let above = |n: Num| match (n, strict_step) {
        (Num::Int(v), true) => v.checked_add(1).map(Num::Int),
        (other, _) => Some(other),
    };
    let below = |n: Num| match (n, strict_step) {
        (Num::Int(v), true) => v.checked_sub(1).map(Num::Int),
        (other, _) => Some(other),
    };
    match op {
        BinaryOp::Eq => Some((Some(floor), Some(ceil))),
        BinaryOp::Ge => Some((Some(floor), None)),
        BinaryOp::Gt => Some((Some(above(floor)?), None)),
        BinaryOp::Le => Some((None, Some(ceil))),
        BinaryOp::Lt => Some((None, Some(below(ceil)?))),
        BinaryOp::Add
        | BinaryOp::Sub
        | BinaryOp::Mul
        | BinaryOp::Div
        | BinaryOp::Mod
        | BinaryOp::Ne
        | BinaryOp::And
        | BinaryOp::Or
        | BinaryOp::Concat => None,
    }
}

fn literal_num(dtype: ArrayDimType, lit: &ArrayCoordLiteral) -> Option<Num> {
    match (dtype, lit) {
        (ArrayDimType::Int64 | ArrayDimType::TimestampMs, ArrayCoordLiteral::Int64(v)) => {
            Some(Num::Int(*v))
        }
        (ArrayDimType::Float64, ArrayCoordLiteral::Int64(v)) => Some(Num::Float(*v as f64)),
        (ArrayDimType::Float64, ArrayCoordLiteral::Float64(v)) => Some(Num::Float(*v)),
        _ => None,
    }
}

fn domain_num(dtype: ArrayDimType, bound: &ArrayDomainBound) -> Option<Num> {
    match (dtype, bound) {
        (ArrayDimType::Int64, ArrayDomainBound::Int64(v))
        | (ArrayDimType::TimestampMs, ArrayDomainBound::TimestampMs(v)) => Some(Num::Int(*v)),
        (ArrayDimType::Float64, ArrayDomainBound::Float64(v)) => Some(Num::Float(*v)),
        _ => None,
    }
}

fn num_literal(n: Num) -> ArrayCoordLiteral {
    match n {
        Num::Int(v) => ArrayCoordLiteral::Int64(v),
        Num::Float(v) => ArrayCoordLiteral::Float64(v),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::temporal::TemporalScope;
    use crate::types::EngineType;
    use crate::types_array::{ArrayAttrAst, ArrayAttrType, ArrayDimAst, ArraySliceAst};

    fn view() -> ArrayCatalogView {
        ArrayCatalogView {
            name: "img".into(),
            dims: vec![
                ArrayDimAst {
                    name: "x".into(),
                    dtype: ArrayDimType::Int64,
                    lo: ArrayDomainBound::Int64(0),
                    hi: ArrayDomainBound::Int64(3),
                },
                ArrayDimAst {
                    name: "y".into(),
                    dtype: ArrayDimType::Int64,
                    lo: ArrayDomainBound::Int64(0),
                    hi: ArrayDomainBound::Int64(3),
                },
            ],
            attrs: vec![ArrayAttrAst {
                name: "nir".into(),
                dtype: ArrayAttrType::Float64,
                nullable: true,
                meta: Default::default(),
            }],
            tile_extents: vec![2, 2],
            georef: None,
        }
    }

    fn slice(limit: u32) -> ArraySlicePlan {
        ArraySlicePlan {
            name: "img".into(),
            slice: ArraySliceAst::default(),
            attr_projection: Vec::new(),
            limit,
            zone: None,
            temporal: TemporalScope::default(),
            physical: false,
        }
    }

    fn outer(filters: Vec<Filter>, limit: Option<usize>) -> SqlPlan {
        SqlPlan::Scan {
            collection: "rel".into(),
            alias: None,
            engine: EngineType::DocumentSchemaless,
            filters,
            projection: Vec::new(),
            sort_keys: Vec::new(),
            limit,
            offset: 0,
            distinct: false,
            window_functions: Vec::new(),
            temporal: TemporalScope::default(),
        }
    }

    fn cmp(column: &str, op: BinaryOp, value: i64) -> SqlExpr {
        SqlExpr::BinaryOp {
            left: Box::new(SqlExpr::Column {
                table: None,
                name: column.into(),
            }),
            op,
            right: Box::new(SqlExpr::Literal(SqlValue::Int(value))),
        }
    }

    fn and(left: SqlExpr, right: SqlExpr) -> Filter {
        Filter {
            expr: FilterExpr::Expr(SqlExpr::BinaryOp {
                left: Box::new(left),
                op: BinaryOp::And,
                right: Box::new(right),
            }),
        }
    }

    fn range(s: &ArraySlicePlan, dim: &str) -> Option<(ArrayCoordLiteral, ArrayCoordLiteral)> {
        s.slice
            .dim_ranges
            .iter()
            .find(|r| r.dim == dim)
            .map(|r| (r.lo.clone(), r.hi.clone()))
    }

    #[test]
    fn dim_conjuncts_narrow_an_uncapped_slice() {
        let mut s = slice(0);
        let filters = vec![and(cmp("x", BinaryOp::Gt, 0), cmp("y", BinaryOp::Le, 1))];
        push_down_into_slice(&mut s, &view(), &outer(filters, None), "rel");
        assert_eq!(
            range(&s, "x"),
            Some((ArrayCoordLiteral::Int64(1), ArrayCoordLiteral::Int64(3)))
        );
        assert_eq!(
            range(&s, "y"),
            Some((ArrayCoordLiteral::Int64(0), ArrayCoordLiteral::Int64(1)))
        );
    }

    #[test]
    fn a_capped_slice_keeps_its_ranges() {
        let mut s = slice(2);
        let filters = vec![Filter {
            expr: FilterExpr::Expr(cmp("x", BinaryOp::Eq, 3)),
        }];
        push_down_into_slice(&mut s, &view(), &outer(filters, None), "rel");
        assert!(s.slice.dim_ranges.is_empty());
        assert_eq!(s.limit, 2);
    }

    #[test]
    fn an_attr_conjunct_and_an_or_leave_the_ranges() {
        let mut s = slice(0);
        let or = Filter {
            expr: FilterExpr::Expr(SqlExpr::BinaryOp {
                left: Box::new(cmp("x", BinaryOp::Eq, 1)),
                op: BinaryOp::Or,
                right: Box::new(cmp("x", BinaryOp::Eq, 2)),
            }),
        };
        let attr = Filter {
            expr: FilterExpr::Expr(cmp("nir", BinaryOp::Gt, 0)),
        };
        push_down_into_slice(&mut s, &view(), &outer(vec![or, attr], None), "rel");
        assert!(s.slice.dim_ranges.is_empty());
    }

    #[test]
    fn an_empty_intersection_leaves_the_range() {
        let mut s = slice(0);
        let filters = vec![and(cmp("x", BinaryOp::Ge, 2), cmp("x", BinaryOp::Lt, 1))];
        push_down_into_slice(&mut s, &view(), &outer(filters, None), "rel");
        assert_eq!(
            range(&s, "x"),
            Some((ArrayCoordLiteral::Int64(2), ArrayCoordLiteral::Int64(3)))
        );
    }

    #[test]
    fn a_plain_limit_folds_to_the_smaller_cap() {
        let mut s = slice(0);
        push_down_into_slice(&mut s, &view(), &outer(Vec::new(), Some(5)), "rel");
        assert_eq!(s.limit, 5);

        let mut s = slice(2);
        push_down_into_slice(&mut s, &view(), &outer(Vec::new(), Some(100)), "rel");
        assert_eq!(s.limit, 2);
    }

    #[test]
    fn a_limit_under_a_filter_does_not_fold() {
        let mut s = slice(0);
        let filters = vec![Filter {
            expr: FilterExpr::Expr(cmp("nir", BinaryOp::Gt, 0)),
        }];
        push_down_into_slice(&mut s, &view(), &outer(filters, Some(5)), "rel");
        assert_eq!(s.limit, 0);
    }

    #[test]
    fn limit_zero_does_not_fold() {
        let mut s = slice(0);
        push_down_into_slice(&mut s, &view(), &outer(Vec::new(), Some(0)), "rel");
        assert_eq!(s.limit, 0);
    }
}
