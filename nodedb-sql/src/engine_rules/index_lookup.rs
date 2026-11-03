// SPDX-License-Identifier: Apache-2.0

//! Shared equality-to-index rewrite for the document engines.

use crate::planner::computed_tail;
use crate::types::*;

use super::params::ScanParams;

/// Attempt to rewrite `ScanParams` into a [`SqlPlan::DocumentIndexLookup`]
/// when exactly one of the filters is an equality predicate on a `Ready`
/// indexed field. Returns `None` to fall through to a generic `Scan`.
///
/// Shared by the schemaless and strict document engines so the
/// index-rewrite rule has one source of truth. Normalizes strict column
/// names to `$.column` before matching against index fields because the
/// catalog stores every document index in JSON-path canonical form.
pub(crate) fn try_document_index_lookup(
    params: &ScanParams,
    engine: EngineType,
) -> Option<SqlPlan> {
    // Window functions still force a full scan because the IndexedFetch
    // handler does not evaluate window aggregates. The handler orders the
    // matched rows itself. DISTINCT is safe on the index path: the handler
    // emits each matched doc exactly once and any further dedup is a
    // cheap Control-Plane pass on the scan-shaped response.
    if !params.window_functions.is_empty() {
        return None;
    }
    // A computed SELECT item runs in a tail over the fetched rows, so a sort
    // key can name a value only the tail holds. The handler orders by stored
    // fields, so that shape scans.
    if !params.sort_keys.is_empty() && computed_tail::needs_computed_tail(&params.projection) {
        return None;
    }
    // The versioned index answers the current state and a system-time
    // cutoff. It holds no valid time and lists no history, so a valid-time
    // or all-versions read scans the versions instead.
    if !matches!(params.temporal.valid_time, crate::temporal::ValidTime::Any)
        || matches!(
            params.temporal.system_time,
            nodedb_types::SystemTimeScope::AllVersions
        )
    {
        return None;
    }

    // Take the first equality that lines up with a Ready index. The
    // remaining filters stay as post-filters.
    let IndexedEquality {
        filter_index: i,
        index: idx,
        value,
        residual,
        others,
    } = find_indexed_equality(&params.filters, &params.indexes)?;
    let mut remaining = params.filters.clone();
    match residual {
        Some(expr) => {
            remaining[i] = Filter {
                expr: FilterExpr::Expr(expr),
            };
        }
        None => {
            remaining.remove(i);
        }
    }

    // The indexed fetch evaluates no expression. A computed SELECT item
    // runs in a tail over the fetched rows, and the tail takes the
    // DISTINCT and row bound, which apply to the computed output.
    let tail = computed_tail::needs_computed_tail(&params.projection);
    let bounds = computed_tail::TailBounds {
        distinct: params.distinct,
        limit: params.limit,
        offset: params.offset,
    };
    let lookup = SqlPlan::DocumentIndexLookup(DocumentIndexLookupPlan {
        collection: params.collection.clone(),
        alias: params.alias.clone(),
        engine,
        field: idx.field.clone(),
        value,
        also: others,
        filters: remaining,
        projection: computed_tail::body_projection(&params.projection),
        sort_keys: params.sort_keys.clone(),
        limit: if tail { None } else { params.limit },
        offset: if tail { 0 } else { params.offset },
        distinct: !tail && params.distinct,
        window_functions: params.window_functions.clone(),
        case_insensitive: idx.case_insensitive,
        temporal: params.temporal,
    });
    Some(computed_tail::with_computed_tail(
        lookup,
        &params.projection,
        bounds,
    ))
}

/// An equality conjunct that a `Ready` secondary index answers.
pub(crate) struct IndexedEquality<'a> {
    /// Position of the filter holding the equality.
    pub filter_index: usize,
    pub index: &'a IndexSpec,
    /// The literal to look up, lowercased for a case-insensitive index.
    pub value: SqlValue,
    /// The filter's other conjuncts. `None` when the filter was a bare
    /// equality.
    pub residual: Option<SqlExpr>,
    /// Equalities on the other `Ready` indexes. Each stays in the filters
    /// as well, so the executor can narrow by them and the filters still
    /// decide a row.
    pub others: Vec<IndexProbe>,
}

/// The first equality in `filters` that lines up with a `Ready` index whose
/// partial-index predicate the query entails, plus the equalities on the
/// other such indexes. No statistics say which index is narrowest, so the
/// executor reads the ids of every one and keeps the shared ones.
///
/// Predicates appear in three shapes:
///   - `FilterExpr::Comparison { field, Eq, value }` — resolver path.
///   - `FilterExpr::Expr(BinaryOp { Column, Eq, Literal })` — generic.
///   - `FilterExpr::Expr(BinaryOp { left AND right })` — the top-level AND
///     of a compound WHERE. An equality nested in a conjunction still
///     picks up the index.
///
/// Every equality conjunct is a candidate, so a leading equality on an
/// unindexed column does not hide an indexed one. Every conjunct of a
/// partial index's predicate must appear as a conjunct of the query WHERE,
/// otherwise the index omits rows the query expects.
pub(crate) fn find_indexed_equality<'a>(
    filters: &[Filter],
    indexes: &'a [IndexSpec],
) -> Option<IndexedEquality<'a>> {
    let query_conjuncts: Vec<SqlExpr> = filters
        .iter()
        .flat_map(|f| match &f.expr {
            FilterExpr::Expr(e) => {
                let mut v = Vec::new();
                flatten_and(e, &mut v);
                v
            }
            _ => Vec::new(),
        })
        .collect();

    let candidates = filters.iter().enumerate().flat_map(|(i, f)| {
        equality_candidates(&f.expr)
            .into_iter()
            .map(move |(field, value, residual)| (i, field, value, residual))
    });
    let mut matched: Vec<(usize, &IndexSpec, SqlValue, Option<SqlExpr>)> = Vec::new();
    for (filter_index, field, value, residual) in candidates {
        let canonical = canonical_index_field(&field);
        let Some(index) = indexes.iter().find(|i| {
            i.state == IndexState::Ready
                && i.field == canonical
                && partial_index_entailed(i.predicate.as_deref(), &query_conjuncts)
        }) else {
            continue;
        };
        let value = if index.case_insensitive {
            lowercase_string_value(&value)
        } else {
            value
        };
        matched.push((filter_index, index, value, residual));
    }
    let mut matched = matched.into_iter();
    let (filter_index, index, value, residual) = matched.next()?;
    let mut others: Vec<IndexProbe> = Vec::new();
    for (_, other, other_value, _) in matched {
        if other.field != index.field && others.iter().all(|p| p.field != other.field) {
            others.push(IndexProbe {
                field: other.field.clone(),
                value: other_value,
            });
        }
    }
    Some(IndexedEquality {
        filter_index,
        index,
        value,
        residual,
        others,
    })
}

/// Every `(column_name, equality_value, residual_expr)` a filter offers as an
/// index lookup: one per column-equals-literal conjunct. The residual is the
/// rest of the conjunction with that equality removed, in the original order
/// — `None` when the filter was a bare equality. Empty when the filter holds
/// no usable equality.
fn equality_candidates(expr: &FilterExpr) -> Vec<(String, SqlValue, Option<SqlExpr>)> {
    match expr {
        FilterExpr::Comparison {
            field,
            op: CompareOp::Eq,
            value,
        } => vec![(field.clone(), value.clone(), None)],
        FilterExpr::Expr(sql_expr) => equalities_from_expr(sql_expr),
        _ => Vec::new(),
    }
}

/// The equalities of an `AND` conjunction tree, each with the residual of its
/// sibling conjuncts. Non-equality conjuncts offer none.
fn equalities_from_expr(expr: &SqlExpr) -> Vec<(String, SqlValue, Option<SqlExpr>)> {
    let mut conjuncts: Vec<SqlExpr> = Vec::new();
    flatten_and(expr, &mut conjuncts);

    (0..conjuncts.len())
        .filter(|&i| is_column_eq_literal(&conjuncts[i]))
        .filter_map(|i| {
            let (col, lit) = match &conjuncts[i] {
                SqlExpr::BinaryOp { left, op, right } => {
                    match (left.as_ref(), op, right.as_ref()) {
                        (SqlExpr::Column { name, .. }, BinaryOp::Eq, SqlExpr::Literal(v))
                        | (SqlExpr::Literal(v), BinaryOp::Eq, SqlExpr::Column { name, .. }) => {
                            (name.clone(), v.clone())
                        }
                        _ => return None,
                    }
                }
                _ => return None,
            };
            let siblings = conjuncts
                .iter()
                .enumerate()
                .filter(|&(j, _)| j != i)
                .map(|(_, c)| c.clone())
                .collect();
            Some((col, lit, rebuild_and(siblings)))
        })
        .collect()
}

/// Append every leaf of a right-leaning `AND` tree to `out`. Non-AND
/// expressions are a single leaf.
fn flatten_and(expr: &SqlExpr, out: &mut Vec<SqlExpr>) {
    match expr {
        SqlExpr::BinaryOp {
            left,
            op: BinaryOp::And,
            right,
        } => {
            flatten_and(left, out);
            flatten_and(right, out);
        }
        other => out.push(other.clone()),
    }
}

/// Reassemble a list of conjuncts into a right-leaning AND tree.
/// Empty input returns `None`; single input returns itself.
fn rebuild_and(mut conjuncts: Vec<SqlExpr>) -> Option<SqlExpr> {
    let last = conjuncts.pop()?;
    Some(
        conjuncts
            .into_iter()
            .rfold(last, |acc, next| SqlExpr::BinaryOp {
                left: Box::new(next),
                op: BinaryOp::And,
                right: Box::new(acc),
            }),
    )
}

/// Decide whether the query's WHERE conjuncts entail the partial-index
/// predicate. `None` predicate means a full index — trivially entailed.
/// Initial version uses conjunct-level structural equality: every
/// conjunct of the index predicate must appear (by `PartialEq`) as a
/// conjunct of the query. This is conservative and deliberately so —
/// a false positive here would silently omit rows from query results.
fn partial_index_entailed(predicate: Option<&str>, query_conjuncts: &[SqlExpr]) -> bool {
    let Some(text) = predicate else {
        return true;
    };
    let Ok(parsed) = crate::parse_expr_string(text) else {
        // A catalog predicate we can't parse is not entailed — refuse
        // to use the index rather than assume anything about its
        // contents. The DDL path validates at CREATE INDEX time, so
        // reaching this branch indicates drift.
        return false;
    };
    let mut index_conjuncts: Vec<SqlExpr> = Vec::new();
    flatten_and(&parsed, &mut index_conjuncts);
    // Structural equality via the stable `Debug` representation:
    // `SqlExpr` doesn't derive `PartialEq` (several nested variants
    // carry types that can't derive it cheaply), but the Debug form is
    // canonical for equivalent trees produced by the same parser. This
    // is conservative — it matches only identical AST shapes and not,
    // e.g., `a = 1` vs `1 = a`. Callers should write index predicates
    // in the same normal form they use in query WHERE clauses, which
    // is the convention everywhere else in the codebase.
    index_conjuncts.iter().all(|ic| {
        let ic_dbg = format!("{ic:?}");
        query_conjuncts.iter().any(|qc| format!("{qc:?}") == ic_dbg)
    })
}

fn is_column_eq_literal(expr: &SqlExpr) -> bool {
    matches!(
        expr,
        SqlExpr::BinaryOp { left, op: BinaryOp::Eq, right }
            if matches!(
                (left.as_ref(), right.as_ref()),
                (SqlExpr::Column { .. }, SqlExpr::Literal(_))
                | (SqlExpr::Literal(_), SqlExpr::Column { .. }),
            )
    )
}

fn canonical_index_field(field: &str) -> String {
    if field.starts_with("$.") || field.starts_with('$') {
        field.to_string()
    } else {
        format!("$.{field}")
    }
}

fn lowercase_string_value(v: &SqlValue) -> SqlValue {
    if let SqlValue::String(s) = v {
        SqlValue::String(s.to_lowercase())
    } else {
        v.clone()
    }
}
