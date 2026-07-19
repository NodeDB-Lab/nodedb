// SPDX-License-Identifier: Apache-2.0

//! Static argument type specifications for all built-in functions.
//!
//! Each constant defines the accepted argument types per position.
//! An empty `accepted` slice means any type is accepted (wildcard).
//! `variadic: true` on the last entry means the argument repeats.

use nodedb_types::columnar::ColumnType;

use super::registry::ArgTypeSpec;

// ── Shorthands ──────────────────────────────────────────────────────────────

const ANY: &[ColumnType] = &[];

const NUMERIC: &[ColumnType] = &[
    ColumnType::Int64,
    ColumnType::Float64,
    ColumnType::Decimal {
        precision: 38,
        scale: 10,
    },
];

const TEXT: &[ColumnType] = &[ColumnType::String];

const FLOAT64_ONLY: &[ColumnType] = &[ColumnType::Float64];

const INT64_ONLY: &[ColumnType] = &[ColumnType::Int64];

const VECTOR_ONLY: &[ColumnType] = &[ColumnType::Vector(0)];

const GEOMETRY_ONLY: &[ColumnType] = &[ColumnType::Geometry];

const TIMESTAMP_TYPES: &[ColumnType] = &[ColumnType::Timestamp, ColumnType::Timestamptz];

// ── Helper constructors ──────────────────────────────────────────────────────

pub(super) const fn any(name: &'static str) -> ArgTypeSpec {
    ArgTypeSpec {
        name,
        accepted: ANY,
        variadic: false,
    }
}

pub(super) const fn any_variadic(name: &'static str) -> ArgTypeSpec {
    ArgTypeSpec {
        name,
        accepted: ANY,
        variadic: true,
    }
}

pub(super) const fn typed(name: &'static str, accepted: &'static [ColumnType]) -> ArgTypeSpec {
    ArgTypeSpec {
        name,
        accepted,
        variadic: false,
    }
}

pub(super) const fn typed_variadic(
    name: &'static str,
    accepted: &'static [ColumnType],
) -> ArgTypeSpec {
    ArgTypeSpec {
        name,
        accepted,
        variadic: true,
    }
}

// ── Standard aggregates ──────────────────────────────────────────────────────

/// count(*) / count(expr)
pub static COUNT_ARGS: &[ArgTypeSpec] = &[any_variadic("expr")];

pub static SUM_ARGS: &[ArgTypeSpec] = &[typed("expr", NUMERIC)];

pub static AVG_ARGS: &[ArgTypeSpec] = &[typed("expr", NUMERIC)];

pub static MIN_ARGS: &[ArgTypeSpec] = &[any("expr")];

pub static MAX_ARGS: &[ArgTypeSpec] = &[any("expr")];

// ── Standard window ──────────────────────────────────────────────────────────

pub static NO_ARGS: &[ArgTypeSpec] = &[];

pub static LAG_LEAD_ARGS: &[ArgTypeSpec] =
    &[any("expr"), typed("offset", INT64_ONLY), any("default")];

pub static FIRST_LAST_VALUE_ARGS: &[ArgTypeSpec] = &[any("expr")];

pub static NTH_VALUE_ARGS: &[ArgTypeSpec] = &[any("expr"), typed("n", INT64_ONLY)];

pub static NTILE_ARGS: &[ArgTypeSpec] = &[typed("buckets", INT64_ONLY)];

// ── Vector search ────────────────────────────────────────────────────────────

pub static VECTOR_DISTANCE_ARGS: &[ArgTypeSpec] = &[
    typed("column", VECTOR_ONLY),
    typed("query", VECTOR_ONLY),
    any("metric"),
];

pub static MULTI_VECTOR_SEARCH_ARGS: &[ArgTypeSpec] = &[any("query"), any("options")];

pub static MULTI_VECTOR_SCORE_ARGS: &[ArgTypeSpec] =
    &[any("col1"), any("col2"), typed("query", VECTOR_ONLY)];

pub static SPARSE_SCORE_ARGS: &[ArgTypeSpec] =
    &[any("col"), any("query"), typed("boost", FLOAT64_ONLY)];

// ── Text search ───────────────────────────────────────────────────────────────

pub static BM25_SCORE_ARGS: &[ArgTypeSpec] = &[any("column"), typed("query", TEXT)];

pub static SEARCH_SCORE_ARGS: &[ArgTypeSpec] = &[any("column"), typed("query", TEXT)];

pub static TEXT_MATCH_ARGS: &[ArgTypeSpec] = &[any("column"), typed("query", TEXT), any("options")];

// ── Hybrid search ─────────────────────────────────────────────────────────────

/// Two-source `rrf_score(rank1, rank2, k1?, k2?)` — vector + text.
/// Accepts 2–4 arguments; the planner validates arity at plan time.
pub static RRF_SCORE_ARGS: &[ArgTypeSpec] = &[any("rank1"), any("rank2"), any("k1"), any("k2")];

/// Three-source `rrf_score(rank1, rank2, rank3, k1?, k2?, k3?)` — vector + text + graph.
/// Shares the same function name; arity dispatch happens in the planner.
pub static RRF_SCORE_TRIPLE_ARGS: &[ArgTypeSpec] = &[
    any("rank1"),
    any("rank2"),
    any("rank3"),
    any("k1"),
    any("k2"),
    any("k3"),
];

/// `graph_score(node_id_col, seed_id, depth => N, label => 'edge_label')`.
///
/// This is a planner-intercepted marker function — it is never evaluated
/// per-row. The hybrid planner recognises it as the graph-distance source
/// in a three-source `rrf_score(...)` call and lowers it to a graph BFS
/// spec attached to the physical plan. Arguments beyond the first two are
/// named (`depth =>`, `label =>`).
pub static GRAPH_SCORE_ARGS: &[ArgTypeSpec] =
    &[any("node_id_col"), any("seed_id"), any_variadic("options")];

// ── Spatial ───────────────────────────────────────────────────────────────────

pub static SPATIAL_3_ARGS: &[ArgTypeSpec] = &[
    typed("geom1", GEOMETRY_ONLY),
    typed("geom2", GEOMETRY_ONLY),
    typed("distance", FLOAT64_ONLY),
];

pub static SPATIAL_2_ARGS: &[ArgTypeSpec] =
    &[typed("geom1", GEOMETRY_ONLY), typed("geom2", GEOMETRY_ONLY)];

pub static ST_BUFFER_ARGS: &[ArgTypeSpec] = &[
    typed("geom", GEOMETRY_ONLY),
    typed("distance", FLOAT64_ONLY),
    typed("segments", INT64_ONLY),
];

pub static ST_ENVELOPE_ARGS: &[ArgTypeSpec] = &[typed("geom", GEOMETRY_ONLY)];

pub static ST_POINT_ARGS: &[ArgTypeSpec] = &[typed("x", FLOAT64_ONLY), typed("y", FLOAT64_ONLY)];

pub static ST_GEOHASH_ARGS: &[ArgTypeSpec] = &[
    typed("lng", FLOAT64_ONLY),
    typed("lat", FLOAT64_ONLY),
    typed("precision", INT64_ONLY),
];

pub static ST_GEOHASHDECODE_ARGS: &[ArgTypeSpec] = &[typed("geohash", TEXT)];

pub static H3_LATLNGTOCELL_ARGS: &[ArgTypeSpec] = &[
    typed("lat", FLOAT64_ONLY),
    typed("lng", FLOAT64_ONLY),
    typed("resolution", INT64_ONLY),
];

pub static H3_CELLTOLATLNG_ARGS: &[ArgTypeSpec] = &[typed("h3_index", TEXT)];

// ── Timeseries ────────────────────────────────────────────────────────────────

pub static TIME_BUCKET_ARGS: &[ArgTypeSpec] = &[any("interval"), typed("ts", TIMESTAMP_TYPES)];

pub static TS_PERCENTILE_ARGS: &[ArgTypeSpec] = &[any("expr"), typed("percentile", FLOAT64_ONLY)];

pub static TS_STDDEV_ARGS: &[ArgTypeSpec] = &[any("expr")];

pub static TS_CORRELATE_ARGS: &[ArgTypeSpec] = &[any("col1"), any("col2")];

pub static TS_WINDOW_1_ARGS: &[ArgTypeSpec] = &[any("expr")];

pub static TS_MOVING_AVG_ARGS: &[ArgTypeSpec] = &[any("expr"), typed("window", INT64_ONLY)];

pub static TS_EMA_ARGS: &[ArgTypeSpec] = &[any("expr"), typed("alpha", FLOAT64_ONLY)];

pub static TS_LAG_LEAD_ARGS: &[ArgTypeSpec] =
    &[any("expr"), typed("offset", INT64_ONLY), any("default")];

// ── Approximate aggregates ────────────────────────────────────────────────────

pub static APPROX_COUNT_DISTINCT_ARGS: &[ArgTypeSpec] = &[any("expr")];

pub static APPROX_PERCENTILE_ARGS: &[ArgTypeSpec] =
    &[any("expr"), typed("percentile", FLOAT64_ONLY)];

pub static APPROX_TOPK_ARGS: &[ArgTypeSpec] = &[any("expr"), typed("k", INT64_ONLY)];

pub static APPROX_COUNT_ARGS: &[ArgTypeSpec] = &[any("expr")];

// ── Grouping set helpers ──────────────────────────────────────────────────────

/// `GROUPING(col [, col2, ...])` — accepts 1 or more column references.
pub static GROUPING_ARGS: &[ArgTypeSpec] = &[any_variadic("col")];

// ── Document helpers ──────────────────────────────────────────────────────────

pub static DOC_GET_ARGS: &[ArgTypeSpec] = &[any("doc"), typed("path", TEXT), any("default")];

pub static DOC_EXISTS_ARGS: &[ArgTypeSpec] = &[any("doc"), typed("path", TEXT)];

pub static DOC_ARRAY_CONTAINS_ARGS: &[ArgTypeSpec] =
    &[any("doc"), typed("path", TEXT), any("value")];

pub static NAV_ARGS: &[ArgTypeSpec] = &[any("doc"), typed("path", TEXT)];

// ── Utility ───────────────────────────────────────────────────────────────────

pub static NDB_CHUNK_TEXT_ARGS: &[ArgTypeSpec] = &[
    typed("text", TEXT),
    typed("chunk_size", INT64_ONLY),
    typed("overlap", INT64_ONLY),
];

// ── Standard scalar ───────────────────────────────────────────────────────────

pub static COALESCE_ARGS: &[ArgTypeSpec] = &[any_variadic("expr")];

pub static NULLIF_ARGS: &[ArgTypeSpec] = &[any("expr1"), any("expr2")];

pub static MATH_1_ARGS: &[ArgTypeSpec] = &[typed("expr", NUMERIC)];

pub static ROUND_ARGS: &[ArgTypeSpec] = &[typed("expr", NUMERIC), typed("scale", INT64_ONLY)];

pub static STRING_1_ARGS: &[ArgTypeSpec] = &[typed("expr", TEXT)];

pub static LENGTH_ARGS: &[ArgTypeSpec] = &[typed("expr", TEXT)];

pub static SUBSTRING_ARGS: &[ArgTypeSpec] = &[
    typed("expr", TEXT),
    typed("start", INT64_ONLY),
    typed("length", INT64_ONLY),
];

pub static CONCAT_ARGS: &[ArgTypeSpec] = &[typed_variadic("expr", TEXT)];

pub static REPLACE_ARGS: &[ArgTypeSpec] =
    &[typed("expr", TEXT), typed("from", TEXT), typed("to", TEXT)];

pub static MAKE_ARRAY_ARGS: &[ArgTypeSpec] = &[any_variadic("expr")];

pub static PG_CATALOG_2_ARGS: &[ArgTypeSpec] = &[any("value"), any("context")];

// ── PostgreSQL JSON operators ─────────────────────────────────────────────────

pub static PG_JSON_2_ARGS: &[ArgTypeSpec] = &[any("json_col"), any("key")];

pub static PG_JSON_BOOL_2_ARGS: &[ArgTypeSpec] = &[any("json_col"), any("operand")];

// ── SQL/JSON standard functions ───────────────────────────────────────────────

pub static JSON_VALUE_ARGS: &[ArgTypeSpec] = &[any("json_col"), typed("path", TEXT)];

pub static JSON_QUERY_ARGS: &[ArgTypeSpec] = &[any("json_col"), typed("path", TEXT)];

pub static JSON_EXISTS_ARGS: &[ArgTypeSpec] = &[any("json_col"), typed("path", TEXT)];

// ── PostgreSQL FTS operators ──────────────────────────────────────────────────

pub static PG_FTS_MATCH_ARGS: &[ArgTypeSpec] = &[any("tsvector"), any("tsquery")];

pub static PG_TO_TSVECTOR_ARGS: &[ArgTypeSpec] = &[typed("config", TEXT), typed("document", TEXT)];

pub static PG_TO_TSQUERY_ARGS: &[ArgTypeSpec] = &[typed("config", TEXT), typed("query", TEXT)];

pub static PG_TS_RANK_ARGS: &[ArgTypeSpec] = &[
    any("tsvector"),
    any("tsquery"),
    typed("weights", FLOAT64_ONLY),
    typed("normalization", INT64_ONLY),
];

pub static PG_TS_HEADLINE_ARGS: &[ArgTypeSpec] = &[
    typed("config", TEXT),
    any("document"),
    any("tsquery"),
    typed("options", TEXT),
];

// ── Array engine ──────────────────────────────────────────────────────────────

pub static ARRAY_SLICE_ARGS: &[ArgTypeSpec] = &[
    typed("name", TEXT),
    any("slice_obj"),
    any("attrs"),
    typed("limit", INT64_ONLY),
];

pub static ARRAY_PROJECT_ARGS: &[ArgTypeSpec] = &[typed("name", TEXT), any("attrs")];

pub static ARRAY_AGG_ARGS: &[ArgTypeSpec] = &[
    typed("name", TEXT),
    typed("attr", TEXT),
    typed("reducer", TEXT),
    typed("group_by_dim", INT64_ONLY),
];

pub static ARRAY_ELEMENTWISE_ARGS: &[ArgTypeSpec] = &[
    typed("left", TEXT),
    typed("right", TEXT),
    typed("op", TEXT),
    typed("attr", TEXT),
];

pub static ARRAY_MAINT_ARGS: &[ArgTypeSpec] = &[typed("name", TEXT)];
