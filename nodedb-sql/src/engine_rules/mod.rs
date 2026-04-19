pub mod columnar;
pub mod document_schemaless;
pub mod document_strict;
pub mod kv;
pub mod spatial;
pub mod timeseries;

use crate::error::Result;
use crate::types::*;

/// Parameters for planning an INSERT operation.
pub struct InsertParams {
    pub collection: String,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<(String, SqlValue)>>,
    pub column_defaults: Vec<(String, String)>,
    /// `ON CONFLICT DO NOTHING` semantics: duplicate-PK rows are skipped
    /// silently. `false` for plain `INSERT` (raises `unique_violation`).
    pub if_absent: bool,
}

/// Parameters for planning a SCAN operation.
pub struct ScanParams {
    pub collection: String,
    pub alias: Option<String>,
    pub filters: Vec<Filter>,
    pub projection: Vec<Projection>,
    pub sort_keys: Vec<SortKey>,
    pub limit: Option<usize>,
    pub offset: usize,
    pub distinct: bool,
    pub window_functions: Vec<WindowSpec>,
    /// Secondary indexes available on the scan's collection. Document
    /// engines consult this to rewrite equality-on-indexed-field into
    /// [`SqlPlan::DocumentIndexLookup`]. Other engines ignore it today.
    pub indexes: Vec<IndexSpec>,
}

/// Parameters for planning a POINT GET operation.
pub struct PointGetParams {
    pub collection: String,
    pub alias: Option<String>,
    pub key_column: String,
    pub key_value: SqlValue,
}

/// Parameters for planning an UPDATE operation.
pub struct UpdateParams {
    pub collection: String,
    pub assignments: Vec<(String, SqlExpr)>,
    pub filters: Vec<Filter>,
    pub target_keys: Vec<SqlValue>,
    pub returning: bool,
}

/// Parameters for planning a DELETE operation.
pub struct DeleteParams {
    pub collection: String,
    pub filters: Vec<Filter>,
    pub target_keys: Vec<SqlValue>,
}

/// Parameters for planning an UPSERT operation.
pub struct UpsertParams {
    pub collection: String,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<(String, SqlValue)>>,
    pub column_defaults: Vec<(String, String)>,
    /// `ON CONFLICT (...) DO UPDATE SET` assignments. Empty for plain
    /// `UPSERT INTO ...`; populated when the caller is
    /// `INSERT ... ON CONFLICT ... DO UPDATE SET`.
    pub on_conflict_updates: Vec<(String, SqlExpr)>,
}

/// Parameters for planning an AGGREGATE operation.
pub struct AggregateParams {
    pub collection: String,
    pub alias: Option<String>,
    pub filters: Vec<Filter>,
    pub group_by: Vec<SqlExpr>,
    pub aggregates: Vec<AggregateExpr>,
    pub having: Vec<Filter>,
    pub limit: usize,
    /// Timeseries-specific: bucket interval from time_bucket() call.
    pub bucket_interval_ms: Option<i64>,
    /// Timeseries-specific: non-time GROUP BY columns.
    pub group_columns: Vec<String>,
    /// Whether the collection has auto-tiering enabled.
    pub has_auto_tier: bool,
}

/// Engine-specific planning rules.
///
/// Each engine type implements this trait to produce the correct `SqlPlan`
/// variant for each operation, or return an error if the operation is not
/// supported. This is the single source of truth for operation routing —
/// no downstream code should ever check engine type to decide routing.
pub trait EngineRules {
    /// Plan an INSERT. Returns `Err` if the engine does not support inserts
    /// (e.g. timeseries routes to `TimeseriesIngest` instead).
    fn plan_insert(&self, params: InsertParams) -> Result<Vec<SqlPlan>>;
    /// Plan an UPSERT (insert-or-merge). Returns `Err` for append-only or
    /// columnar engines that don't support merge semantics.
    fn plan_upsert(&self, params: UpsertParams) -> Result<Vec<SqlPlan>>;
    /// Plan a table scan (SELECT without point-get optimization).
    fn plan_scan(&self, params: ScanParams) -> Result<SqlPlan>;
    /// Plan a point lookup by primary key. Returns `Err` for engines
    /// that don't support O(1) key lookups (e.g. timeseries).
    fn plan_point_get(&self, params: PointGetParams) -> Result<SqlPlan>;
    /// Plan an UPDATE. Returns `Err` for append-only engines.
    fn plan_update(&self, params: UpdateParams) -> Result<Vec<SqlPlan>>;
    /// Plan a DELETE (point or bulk).
    fn plan_delete(&self, params: DeleteParams) -> Result<Vec<SqlPlan>>;
    /// Plan a GROUP BY / aggregate query.
    fn plan_aggregate(&self, params: AggregateParams) -> Result<SqlPlan>;
}

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
    // Sort / window functions still force a full scan because the
    // IndexedFetch handler does not yet order rows or evaluate window
    // aggregates. DISTINCT is safe on the index path: the handler
    // emits each matched doc exactly once and any further dedup is a
    // cheap Control-Plane pass on the scan-shaped response.
    if !params.sort_keys.is_empty() || !params.window_functions.is_empty() {
        return None;
    }

    // Iterate filters to find the first equality candidate that lines up
    // with a Ready index. Keep the remaining filters as post-filters.
    // Predicates appear in three shapes:
    //   - `FilterExpr::Comparison { field, Eq, value }` — resolver path.
    //   - `FilterExpr::Expr(BinaryOp { Column, Eq, Literal })` — generic.
    //   - `FilterExpr::Expr(BinaryOp { left AND right })` — the
    //     top-level AND the `convert_where_to_filters` path produces
    //     for compound WHERE clauses like `a = 1 AND b > 2`. Descend
    //     into AND conjuncts so an equality nested in a conjunction
    //     still picks up the index, and the non-equality siblings
    //     survive as a residual conjunction carried on the
    //     IndexedFetch node (not as a wrapping Filter plan node —
    //     that wrapping is the Control-Plane post-filter anti-pattern
    //     the handler's `filters` payload exists to eliminate).
    // Pre-collect the query's top-level conjuncts once. Used for
    // partial-index entailment: every conjunct of the index predicate
    // must appear as a conjunct of the query WHERE for the rewrite to
    // be sound, otherwise the index would silently omit rows the
    // query expects to see.
    let query_conjuncts: Vec<SqlExpr> = params
        .filters
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

    for (i, f) in params.filters.iter().enumerate() {
        let Some((field, value, residual)) = extract_equality_with_residual(&f.expr) else {
            continue;
        };
        let canonical = canonical_index_field(&field);
        let Some(idx) = params.indexes.iter().find(|i| {
            i.state == IndexState::Ready
                && i.field == canonical
                && partial_index_entailed(i.predicate.as_deref(), &query_conjuncts)
        }) else {
            continue;
        };

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

        let lookup_value = if idx.case_insensitive {
            lowercase_string_value(&value)
        } else {
            value
        };

        return Some(SqlPlan::DocumentIndexLookup {
            collection: params.collection.clone(),
            alias: params.alias.clone(),
            engine,
            field: idx.field.clone(),
            value: lookup_value,
            filters: remaining,
            projection: params.projection.clone(),
            sort_keys: params.sort_keys.clone(),
            limit: params.limit,
            offset: params.offset,
            distinct: params.distinct,
            window_functions: params.window_functions.clone(),
            case_insensitive: idx.case_insensitive,
        });
    }
    None
}

/// Pull `(column_name, equality_value, residual_expr)` out of a filter
/// expression if it contains a column-equals-literal predicate that can
/// drive an index lookup. Returns `None` if no usable equality is
/// present. The returned residual is the rest of the conjunction with
/// the equality removed — `None` when the filter was a bare equality.
fn extract_equality_with_residual(
    expr: &FilterExpr,
) -> Option<(String, SqlValue, Option<SqlExpr>)> {
    match expr {
        FilterExpr::Comparison {
            field,
            op: CompareOp::Eq,
            value,
        } => Some((field.clone(), value.clone(), None)),
        FilterExpr::Expr(sql_expr) => {
            let (col, lit, residual) = split_equality_from_expr(sql_expr)?;
            Some((col, lit, residual))
        }
        _ => None,
    }
}

/// Return `(column, literal, residual_expr)` for an equality found
/// anywhere in a right-leaning AND conjunction tree, or `None` if no
/// bare column-equals-literal predicate exists. The residual preserves
/// every sibling conjunct in their original order; `None` means the
/// expression was a bare equality with nothing left behind.
fn split_equality_from_expr(expr: &SqlExpr) -> Option<(String, SqlValue, Option<SqlExpr>)> {
    // Gather the conjuncts of a top-level AND chain left-to-right.
    let mut conjuncts: Vec<SqlExpr> = Vec::new();
    flatten_and(expr, &mut conjuncts);

    // Find the first conjunct that is a bare column-equals-literal.
    let eq_idx = conjuncts.iter().position(is_column_eq_literal)?;
    let eq = conjuncts.remove(eq_idx);
    let (col, lit) = match eq {
        SqlExpr::BinaryOp { left, op, right } => match (*left, op, *right) {
            (SqlExpr::Column { name, .. }, BinaryOp::Eq, SqlExpr::Literal(v)) => (name, v),
            (SqlExpr::Literal(v), BinaryOp::Eq, SqlExpr::Column { name, .. }) => (name, v),
            _ => return None,
        },
        _ => return None,
    };

    let residual = rebuild_and(conjuncts);
    Some((col, lit, residual))
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

/// Resolve the engine rules for a given engine type.
///
/// No catch-all — compiler enforces exhaustiveness.
pub fn resolve_engine_rules(engine: EngineType) -> &'static dyn EngineRules {
    match engine {
        EngineType::DocumentSchemaless => &document_schemaless::SchemalessRules,
        EngineType::DocumentStrict => &document_strict::StrictRules,
        EngineType::KeyValue => &kv::KvRules,
        EngineType::Columnar => &columnar::ColumnarRules,
        EngineType::Timeseries => &timeseries::TimeseriesRules,
        EngineType::Spatial => &spatial::SpatialRules,
    }
}
