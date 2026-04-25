//! Built-in function registry for SQL planning.
//!
//! Tracks known functions, their categories, and whether they trigger
//! special engine routing (e.g., vector_distance → VectorSearch).

/// Function category.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FunctionCategory {
    Scalar,
    Aggregate,
    Window,
}

/// Whether a function triggers special engine routing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchTrigger {
    None,
    VectorSearch,
    MultiVectorSearch,
    TextSearch,
    HybridSearch,
    TextMatch,
    SpatialDWithin,
    SpatialContains,
    SpatialIntersects,
    SpatialWithin,
    TimeBucket,
    /// Array engine table-valued read: `NDARRAY_SLICE(name, slice_obj, attrs?, limit?)`.
    NdArraySlice,
    /// Array engine table-valued read: `NDARRAY_PROJECT(name, attrs)`.
    NdArrayProject,
    /// Array engine table-valued aggregate:
    /// `NDARRAY_AGG(name, attr, reducer, group_by_dim?)`.
    NdArrayAgg,
    /// Array engine table-valued elementwise:
    /// `NDARRAY_ELEMENTWISE(left, right, op, attr)`.
    NdArrayElementwise,
    /// Array engine maintenance scalar (returns BOOL): `NDARRAY_FLUSH(name)`.
    NdArrayFlush,
    /// Array engine maintenance scalar (returns BOOL): `NDARRAY_COMPACT(name)`.
    NdArrayCompact,
}

/// Metadata about a known function.
#[derive(Debug, Clone)]
pub struct FunctionMeta {
    pub name: &'static str,
    pub category: FunctionCategory,
    pub min_args: usize,
    pub max_args: usize,
    pub search_trigger: SearchTrigger,
}

/// The function registry.
pub struct FunctionRegistry {
    functions: Vec<FunctionMeta>,
}

impl FunctionRegistry {
    /// Create the default registry with all built-in functions.
    pub fn new() -> Self {
        Self {
            functions: builtin_functions(),
        }
    }

    /// Look up a function by name (case-insensitive).
    pub fn lookup(&self, name: &str) -> Option<&FunctionMeta> {
        let lower = name.to_lowercase();
        self.functions.iter().find(|f| f.name == lower)
    }

    /// Check if a function triggers special search routing.
    pub fn search_trigger(&self, name: &str) -> SearchTrigger {
        self.lookup(name)
            .map(|f| f.search_trigger)
            .unwrap_or(SearchTrigger::None)
    }

    /// Check if a function is an aggregate.
    pub fn is_aggregate(&self, name: &str) -> bool {
        self.lookup(name)
            .is_some_and(|f| f.category == FunctionCategory::Aggregate)
    }

    /// Check if a function is a window function.
    pub fn is_window(&self, name: &str) -> bool {
        self.lookup(name)
            .is_some_and(|f| f.category == FunctionCategory::Window)
    }
}

impl Default for FunctionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

fn s(
    name: &'static str,
    cat: FunctionCategory,
    min: usize,
    max: usize,
    trigger: SearchTrigger,
) -> FunctionMeta {
    FunctionMeta {
        name,
        category: cat,
        min_args: min,
        max_args: max,
        search_trigger: trigger,
    }
}

fn builtin_functions() -> Vec<FunctionMeta> {
    use FunctionCategory::*;
    use SearchTrigger::*;

    vec![
        // ── Standard aggregates ──
        s("count", Aggregate, 0, 1, None),
        s("sum", Aggregate, 1, 1, None),
        s("avg", Aggregate, 1, 1, None),
        s("min", Aggregate, 1, 1, None),
        s("max", Aggregate, 1, 1, None),
        // ── Standard window ──
        s("row_number", Window, 0, 0, None),
        s("rank", Window, 0, 0, None),
        s("dense_rank", Window, 0, 0, None),
        s("lag", Window, 1, 3, None),
        s("lead", Window, 1, 3, None),
        s("first_value", Window, 1, 1, None),
        s("last_value", Window, 1, 1, None),
        s("nth_value", Window, 2, 2, None),
        // ── Vector search ──
        s("vector_distance", Scalar, 2, 3, VectorSearch),
        s("multi_vector_search", Scalar, 1, 2, MultiVectorSearch),
        s("multi_vector_score", Scalar, 3, 3, None),
        s("sparse_score", Scalar, 3, 3, None),
        // ── Text search ──
        s("bm25_score", Scalar, 2, 2, TextSearch),
        s("search_score", Scalar, 2, 2, TextSearch),
        s("text_match", Scalar, 2, 3, TextMatch),
        // ── Hybrid search ──
        s("rrf_score", Scalar, 2, 4, HybridSearch),
        // ── Spatial ──
        s("st_dwithin", Scalar, 3, 3, SpatialDWithin),
        s("st_contains", Scalar, 2, 2, SpatialContains),
        s("st_intersects", Scalar, 2, 2, SpatialIntersects),
        s("st_within", Scalar, 2, 2, SpatialWithin),
        s("st_distance", Scalar, 2, 2, None),
        s("st_point", Scalar, 2, 2, None),
        // ── Timeseries ──
        s("time_bucket", Scalar, 2, 2, TimeBucket),
        // ── Timeseries aggregates ──
        s("ts_percentile", Aggregate, 2, 2, None),
        s("ts_stddev", Aggregate, 1, 1, None),
        s("ts_correlate", Aggregate, 2, 2, None),
        // ── Timeseries window ──
        s("ts_rate", Window, 1, 1, None),
        s("ts_derivative", Window, 1, 1, None),
        s("ts_moving_avg", Window, 2, 2, None),
        s("ts_ema", Window, 2, 2, None),
        s("ts_delta", Window, 1, 1, None),
        s("ts_interpolate", Window, 1, 1, None),
        s("ts_lag", Window, 1, 3, None),
        s("ts_lead", Window, 1, 3, None),
        s("ts_rank", Window, 0, 0, None),
        // ── Approximate aggregates ──
        s("approx_count_distinct", Aggregate, 1, 1, None),
        s("approx_percentile", Aggregate, 2, 2, None),
        s("approx_topk", Aggregate, 2, 2, None),
        s("approx_count", Aggregate, 1, 1, None),
        // ── Document helpers ──
        s("doc_get", Scalar, 2, 3, None),
        s("doc_exists", Scalar, 2, 2, None),
        s("doc_array_contains", Scalar, 3, 3, None),
        s("nav", Scalar, 2, 2, None),
        // ── Utility ──
        s("chunk_text", Scalar, 2, 3, None),
        s("currency", Scalar, 1, 2, None),
        s("distribute", Scalar, 2, 3, None),
        s("allocate", Scalar, 2, 3, None),
        s("resolve_permission", Scalar, 2, 3, None),
        // ── Standard scalar ──
        s("coalesce", Scalar, 1, 255, None),
        s("nullif", Scalar, 2, 2, None),
        s("abs", Scalar, 1, 1, None),
        s("ceil", Scalar, 1, 1, None),
        s("floor", Scalar, 1, 1, None),
        s("round", Scalar, 1, 2, None),
        s("lower", Scalar, 1, 1, None),
        s("upper", Scalar, 1, 1, None),
        s("length", Scalar, 1, 1, None),
        s("trim", Scalar, 1, 1, None),
        s("substring", Scalar, 2, 3, None),
        s("concat", Scalar, 1, 255, None),
        s("replace", Scalar, 3, 3, None),
        s("now", Scalar, 0, 0, None),
        s("current_timestamp", Scalar, 0, 0, None),
        s("make_array", Scalar, 0, 255, None),
        // ── Array engine ──
        s("ndarray_slice", Scalar, 2, 4, NdArraySlice),
        s("ndarray_project", Scalar, 2, 2, NdArrayProject),
        s("ndarray_agg", Scalar, 3, 4, NdArrayAgg),
        s("ndarray_elementwise", Scalar, 4, 4, NdArrayElementwise),
        s("ndarray_flush", Scalar, 1, 1, NdArrayFlush),
        s("ndarray_compact", Scalar, 1, 1, NdArrayCompact),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lookup_builtin() {
        let reg = FunctionRegistry::new();
        assert!(reg.is_aggregate("COUNT"));
        assert!(reg.is_aggregate("sum"));
        assert!(!reg.is_aggregate("vector_distance"));
        assert!(reg.is_window("row_number"));
        assert_eq!(
            reg.search_trigger("vector_distance"),
            SearchTrigger::VectorSearch
        );
        assert_eq!(
            reg.search_trigger("st_dwithin"),
            SearchTrigger::SpatialDWithin
        );
        assert_eq!(reg.search_trigger("time_bucket"), SearchTrigger::TimeBucket);
    }
}
