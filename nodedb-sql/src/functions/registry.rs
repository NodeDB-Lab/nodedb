// SPDX-License-Identifier: Apache-2.0

//! Built-in function registry for SQL planning.
//!
//! Tracks known functions, their categories, arg specs, return types,
//! and whether they trigger special engine routing (e.g., vector_distance
//! → VectorSearch).

use nodedb_types::Volatility;
use nodedb_types::columnar::ColumnType;

use super::builtins::builtin_functions;

/// Semantic version for tracking when a function was introduced.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Version {
    pub major: u8,
    pub minor: u8,
    pub patch: u8,
}

impl Version {
    pub const fn new(major: u8, minor: u8, patch: u8) -> Self {
        Self {
            major,
            minor,
            patch,
        }
    }
}

/// Specification for a single function argument position.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArgTypeSpec {
    /// Argument name, for documentation and error messages.
    pub name: &'static str,
    /// Accepted column types. Empty slice means any type is accepted (wildcard).
    pub accepted: &'static [ColumnType],
    /// If true on the last argument, this argument may repeat zero or more times.
    pub variadic: bool,
}

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
    /// Sparse-vector inverted-index search: `sparse_score(field, '{dim: weight, ...}')`
    /// in ORDER BY routes to `SqlPlan::SparseSearch` and lowers to
    /// `VectorOp::SparseSearch` (dot-product top-k over the sparse index).
    SparseSearch,
    TextSearch,
    HybridSearch,
    TextMatch,
    SpatialDWithin,
    SpatialContains,
    SpatialIntersects,
    SpatialWithin,
    TimeBucket,
    /// Array engine table-valued read: `ARRAY_SLICE(name, slice_obj, attrs?, limit?)`.
    ArraySlice,
    /// Array engine table-valued read: `ARRAY_PROJECT(name, attrs)`.
    ArrayProject,
    /// Array engine table-valued aggregate:
    /// `ARRAY_AGG(name, attr, reducer, group_by_dim?)`.
    ArrayAgg,
    /// Array engine table-valued elementwise:
    /// `ARRAY_ELEMENTWISE(left, right, op, attr)`.
    ArrayElementwise,
    /// Array engine maintenance scalar (returns BOOL): `ARRAY_FLUSH(name)`.
    ArrayFlush,
    /// Array engine maintenance scalar (returns BOOL): `ARRAY_COMPACT(name)`.
    ArrayCompact,
    /// Planner-intercepted graph-distance marker for three-source RRF fusion.
    /// `graph_score(node_id_col, seed_id, depth => N, label => 'edge_label')`
    /// is never evaluated per-row; the hybrid planner extracts its arguments
    /// and builds a graph BFS spec in the physical plan.
    GraphSearch,
}

/// Metadata about a known function.
#[derive(Debug, Clone)]
pub struct FunctionMeta {
    pub name: &'static str,
    pub category: FunctionCategory,
    pub min_args: usize,
    pub max_args: usize,
    pub search_trigger: SearchTrigger,
    /// Static return type, when known at plan time. `None` means the type
    /// is context-dependent or unknown (resolved at runtime).
    pub return_type: Option<ColumnType>,
    /// Per-position argument type specifications.
    pub arg_types: &'static [ArgTypeSpec],
    /// Version in which this function was introduced.
    pub since: Version,
    /// How far a call's result can be reused. Orthogonal to `category`: a
    /// function is `Scalar` and `Volatile` at once. A `Volatile` function is
    /// never folded to a literal and never admitted to a plan cache.
    pub volatility: Volatility,
}

impl FunctionMeta {
    /// Mark this registration `Volatile`.
    pub fn volatile(mut self) -> Self {
        self.volatility = Volatility::Volatile;
        self
    }
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

    /// Whether a function must re-evaluate on every execution.
    /// An unknown name is not volatile — the existence gate rejects it first.
    pub fn is_volatile(&self, name: &str) -> bool {
        self.lookup(name)
            .is_some_and(|f| f.volatility.is_volatile())
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

    #[test]
    fn chunk_text_renamed_to_ndb_chunk_text() {
        let reg = FunctionRegistry::new();
        assert!(
            reg.lookup("ndb_chunk_text").is_some(),
            "ndb_chunk_text must be registered"
        );
        assert!(
            reg.lookup("chunk_text").is_none(),
            "old name chunk_text must not exist"
        );
    }

    #[test]
    fn unimplemented_functions_removed() {
        let reg = FunctionRegistry::new();
        assert!(reg.lookup("currency").is_none());
        assert!(reg.lookup("distribute").is_none());
        assert!(reg.lookup("allocate").is_none());
        assert!(reg.lookup("resolve_permission").is_none());
        assert!(reg.lookup("convert_currency").is_none());
    }

    #[test]
    fn all_builtins_have_since_set() {
        let reg = FunctionRegistry::new();
        let v0_1_0 = Version::new(0, 1, 0);
        for f in &reg.functions {
            assert_eq!(
                f.since, v0_1_0,
                "function '{}' must have since = Version::new(0, 1, 0)",
                f.name
            );
        }
    }

    #[test]
    fn all_builtins_arg_counts_consistent() {
        let reg = FunctionRegistry::new();
        for f in &reg.functions {
            let n = f.arg_types.len();
            let last_variadic = f.arg_types.last().is_some_and(|a| a.variadic);
            if last_variadic {
                // min_args must be <= arg_types.len()
                assert!(
                    f.min_args <= n,
                    "function '{}': min_args ({}) > arg_types.len() ({}) with variadic last arg",
                    f.name,
                    f.min_args,
                    n
                );
            } else {
                // min_args <= n <= max_args
                assert!(
                    f.min_args <= n,
                    "function '{}': min_args ({}) > arg_types.len() ({})",
                    f.name,
                    f.min_args,
                    n
                );
                assert!(
                    n <= f.max_args,
                    "function '{}': arg_types.len() ({}) > max_args ({})",
                    f.name,
                    n,
                    f.max_args
                );
            }
        }
    }

    #[test]
    fn return_type_spot_checks() {
        let reg = FunctionRegistry::new();
        assert_eq!(
            reg.lookup("now").and_then(|f| f.return_type),
            Some(ColumnType::Timestamptz),
            "now() must return Timestamptz"
        );
        assert_eq!(
            reg.lookup("count").and_then(|f| f.return_type),
            Some(ColumnType::Int64),
            "count must return Int64"
        );
        assert_eq!(
            reg.lookup("doc_exists").and_then(|f| f.return_type),
            Some(ColumnType::Bool),
            "doc_exists must return Bool"
        );
        assert_eq!(
            reg.lookup("st_contains").and_then(|f| f.return_type),
            Some(ColumnType::Bool),
            "st_contains must return Bool"
        );
        assert_eq!(
            reg.lookup("pg_fts_match").and_then(|f| f.return_type),
            Some(ColumnType::Bool),
            "pg_fts_match must return Bool"
        );
        assert_eq!(
            reg.lookup("lower").and_then(|f| f.return_type),
            Some(ColumnType::String),
            "lower must return String"
        );
    }

    /// INVARIANT: every name the runtime evaluator
    /// (`nodedb_query::functions::*::try_eval`) actually dispatches on MUST
    /// resolve through `FunctionRegistry::lookup`, or `resolver::expr::
    /// functions::convert_function_depth`'s plan-time existence gate rejects
    /// a call the evaluator would otherwise happily run — the exact bug
    /// class this whole registry audit exists to close.
    ///
    /// Each list below is a literal transcription of one
    /// `nodedb-query/src/functions/<module>.rs` `try_eval` match arm's
    /// name set (including every `|`-joined alias), NOT derived from this
    /// registry — a name added to a `try_eval` match arm without a matching
    /// registry entry must fail this test, not silently pass because both
    /// sides drifted together. When either side changes, update the other.
    #[test]
    fn runtime_match_arms_are_all_registered() {
        let reg = FunctionRegistry::new();
        let assert_all = |module: &str, names: &[&str]| {
            for name in names {
                assert!(
                    reg.lookup(name).is_some(),
                    "nodedb_query::functions::{module}::try_eval handles '{name}' \
                     but FunctionRegistry has no entry for it — the \
                     plan-time gate would reject a call the runtime evaluator supports"
                );
            }
        };

        // `nodedb_query::functions::math::try_eval`.
        assert_all(
            "math",
            &[
                "abs", "round", "ceil", "ceiling", "floor", "power", "pow", "sqrt", "mod", "sign",
                "log", "ln", "log10", "log2", "exp",
            ],
        );

        // `nodedb_query::functions::string::try_eval`.
        assert_all(
            "string",
            &[
                "upper",
                "lower",
                "trim",
                "ltrim",
                "rtrim",
                "length",
                "char_length",
                "character_length",
                "substr",
                "substring",
                "concat",
                "replace",
                "reverse",
            ],
        );

        // `nodedb_query::functions::conditional::try_eval`.
        assert_all("conditional", &["coalesce", "nullif", "greatest", "least"]);

        // `nodedb_query::functions::id::try_eval`.
        assert_all(
            "id",
            &[
                "uuid",
                "uuid_v4",
                "gen_random_uuid",
                "uuid_v7",
                "ulid",
                "cuid2",
                "nanoid",
                "is_uuid",
                "is_ulid",
                "is_cuid2",
                "is_nanoid",
                "id_type",
                "uuid_version",
                "ulid_timestamp",
            ],
        );

        // `nodedb_query::functions::datetime::try_eval`. Individual date
        // parts (`year`, `month`, `day`, `hour`, `minute`, `second`,
        // `dow`/`dayofweek`, `epoch`) are string literals consumed by
        // `extract`/`date_part`/`date_trunc`, not standalone function
        // names, so they are deliberately absent from this list.
        assert_all(
            "datetime",
            &[
                "now",
                "current_timestamp",
                "datetime",
                "to_datetime",
                "unix_secs",
                "epoch_secs",
                "unix_millis",
                "epoch_millis",
                "extract",
                "date_part",
                "date_trunc",
                "datetrunc",
                "date_add",
                "datetime_add",
                "date_sub",
                "datetime_sub",
                "date_diff",
                "datediff",
                "duration",
                "to_duration",
                "decimal",
                "to_decimal",
                "time_bucket",
            ],
        );

        // `nodedb_query::functions::json::{mod,legacy}::try_eval`.
        assert_all(
            "json",
            &[
                "pg_json_get",
                "pg_json_get_text",
                "pg_json_path_get",
                "pg_json_path_get_text",
                "pg_json_contains",
                "pg_json_contained_by",
                "pg_json_has_key",
                "pg_json_has_all_keys",
                "pg_json_has_any_key",
                "json_value",
                "json_query",
                "json_exists",
                "json_extract",
                "json_get",
                "json_set",
                "json_remove",
                "json_keys",
                "json_values",
                "json_length",
                "json_len",
                "json_type",
                "json_array",
                "json_object",
                "json_contains",
                "json_merge",
                "json_patch",
            ],
        );

        // `nodedb_query::functions::types::try_eval`.
        assert_all("types", &["typeof", "type_of"]);

        // `nodedb_query::functions::array::try_eval`. Distinct from the
        // "Array engine" table-valued functions (`array_slice`/
        // `array_project`/`array_agg`/`array_elementwise`/`array_flush`/
        // `array_compact`), which are covered by
        // `all_builtins_arg_counts_consistent` via their own registrations.
        assert_all(
            "array",
            &[
                "array_length",
                "cardinality",
                "array_append",
                "array_prepend",
                "array_remove",
                "array_concat",
                "array_cat",
                "array_distinct",
                "array_contains",
                "array_position",
                "array_reverse",
            ],
        );

        // `nodedb_query::functions::fts::pg_fts_exec::try_eval_fts`.
        assert_all(
            "fts",
            &[
                "pg_fts_match",
                "pg_to_tsquery",
                "pg_plainto_tsquery",
                "pg_websearch_to_tsquery",
                "pg_phraseto_tsquery",
                "pg_to_tsvector",
                "pg_ts_rank",
                "pg_ts_headline",
            ],
        );

        // `nodedb_query::functions::system::try_eval`.
        assert_all(
            "system",
            &["version", "format_type", "pg_get_expr", "col_description"],
        );

        // `nodedb_query::geo_functions::eval_geo_function`. Distinct from
        // the other modules above: this one lives in `nodedb-query`'s
        // top-level `geo_functions` module (not `functions::<name>`), and
        // is dispatched into from `functions::eval_function` for any
        // geo-prefixed/ST_-prefixed name — every `|`-joined alias on its
        // match arms is flattened into this one list, same as `math`'s
        // `ceil`/`ceiling` pairing above.
        assert_all(
            "geo_functions",
            &[
                "geo_distance",
                "haversine_distance",
                "geo_bearing",
                "haversine_bearing",
                "geo_point",
                "st_point",
                "geo_geohash",
                "geo_geohash_decode",
                "geo_geohash_neighbors",
                "st_contains",
                "st_intersects",
                "st_within",
                "st_disjoint",
                "st_dwithin",
                "st_distance",
                "st_buffer",
                "st_envelope",
                "st_union",
                "geo_length",
                "geo_perimeter",
                "geo_line",
                "geo_polygon",
                "geo_circle",
                "geo_bbox",
                "geo_as_geojson",
                "geo_from_geojson",
                "geo_as_wkt",
                "geo_from_wkt",
                "geo_x",
                "geo_y",
                "geo_num_points",
                "geo_type",
                "geo_is_valid",
                "geo_h3",
                "geo_h3_to_boundary",
                "geo_h3_resolution",
                "st_geohash",
                "st_geohashdecode",
                "h3_latlngtocell",
                "h3_celltolatlng",
                "st_intersection",
            ],
        );
    }
}
