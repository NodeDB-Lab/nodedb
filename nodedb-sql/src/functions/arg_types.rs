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

const TIMESTAMP_TYPES: &[ColumnType] = &[ColumnType::Timestamp, ColumnType::Timestamptz];

const ARRAY_ONLY: &[ColumnType] = &[ColumnType::Array];

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

/// `mod(a, b)` / `power(base, exp)` / `pow(base, exp)` — two required
/// numeric arguments.
pub static MATH_2_ARGS: &[ArgTypeSpec] = &[typed("left", NUMERIC), typed("right", NUMERIC)];

pub static ROUND_ARGS: &[ArgTypeSpec] = &[typed("expr", NUMERIC), typed("scale", INT64_ONLY)];

/// `greatest(expr, ...)` / `least(expr, ...)` — one or more arguments of any
/// (comparable) type.
pub static GREATEST_LEAST_ARGS: &[ArgTypeSpec] = &[any_variadic("expr")];

/// `typeof(expr)` / `type_of(expr)` — accepts any value, including NULL.
pub static TYPEOF_ARGS: &[ArgTypeSpec] = &[any("expr")];

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

// ── ID generation / detection (nodedb-query's `functions/id.rs`) ──────────────

/// `is_uuid(value)` / `is_ulid(value)` / `is_cuid2(value)` / `is_nanoid(value)`
/// / `id_type(value)` / `uuid_version(value)` / `ulid_timestamp(value)` — a
/// single text value to classify. Shared shape across all detector functions.
pub static ID_CHECK_ARGS: &[ArgTypeSpec] = &[typed("value", TEXT)];

/// `nanoid(length?)` — optional custom length, defaults to the standard
/// 21-character id when omitted.
pub static NANOID_ARGS: &[ArgTypeSpec] = &[typed("length", INT64_ONLY)];

// ── Array element functions (nodedb-query's `functions/array.rs`) ─────────────
//
// Distinct from the "Array engine" table-valued functions above
// (`array_slice`/`array_project`/...): these operate on an in-row `Array`
// value, mirroring the match arms in `nodedb_query::functions::array::try_eval`.

/// `array_length(arr)` / `cardinality(arr)` / `array_distinct(arr)` /
/// `array_reverse(arr)` — a single array argument.
pub static ARRAY_1_ARGS: &[ArgTypeSpec] = &[typed("arr", ARRAY_ONLY)];

/// `array_append(arr, value)` / `array_remove(arr, value)` /
/// `array_contains(arr, value)` / `array_position(arr, value)` — an array
/// plus an element of any type.
pub static ARRAY_ELEM_ARGS: &[ArgTypeSpec] = &[typed("arr", ARRAY_ONLY), any("value")];

/// `array_prepend(value, arr)` — value-first argument order, matching
/// `nodedb_query::functions::array::try_eval`'s `"array_prepend"` arm.
pub static ARRAY_PREPEND_ARGS: &[ArgTypeSpec] = &[any("value"), typed("arr", ARRAY_ONLY)];

/// `array_concat(arr1, arr2)` / `array_cat(arr1, arr2)`.
pub static ARRAY_CAT_ARGS: &[ArgTypeSpec] = &[typed("arr1", ARRAY_ONLY), typed("arr2", ARRAY_ONLY)];

// ── DateTime / duration (nodedb-query's `functions/datetime.rs`) ──────────────

/// `datetime(value)` / `to_datetime(value)` / `unix_secs(value)` /
/// `epoch_secs(value)` / `unix_millis(value)` / `epoch_millis(value)` /
/// `duration(value)` / `to_duration(value)` / `decimal(value)` /
/// `to_decimal(value)` — a single value whose accepted shape (string,
/// integer micros, or an existing timestamp/duration) varies by function, so
/// the position is left untyped (`any`) rather than over-constrained.
pub static DATETIME_1_ARGS: &[ArgTypeSpec] = &[any("value")];

/// `extract(part, ts)` / `date_part(part, ts)` / `date_trunc(part, ts)` /
/// `datetrunc(part, ts)` — a text field-name selector plus the
/// timestamp-like value to read it from.
pub static DATE_PART_ARGS: &[ArgTypeSpec] = &[typed("part", TEXT), any("ts")];

/// `date_add(ts, duration)` / `datetime_add(ts, duration)` /
/// `date_sub(ts, duration)` / `datetime_sub(ts, duration)`.
pub static DATE_ARITH_ARGS: &[ArgTypeSpec] = &[any("ts"), typed("duration", TEXT)];

/// `date_diff(ts1, ts2)` / `datediff(ts1, ts2)`.
pub static DATE_DIFF_ARGS: &[ArgTypeSpec] = &[any("ts1"), any("ts2")];

// ── Legacy JSON manipulation (nodedb-query's `functions/json/legacy.rs`) ──────
//
// Distinct from the PostgreSQL JSON operators and SQL/JSON standard
// functions above: these are the pre-existing `json_*` document-helper
// functions.

/// `json_extract(json, path)` / `json_get(json, path)`.
pub static JSON_EXTRACT_ARGS: &[ArgTypeSpec] = &[any("json"), typed("path", TEXT)];

/// `json_set(json, key, value)`.
pub static JSON_SET_ARGS: &[ArgTypeSpec] = &[any("json"), typed("key", TEXT), any("value")];

/// `json_remove(json, key)`.
pub static JSON_REMOVE_ARGS: &[ArgTypeSpec] = &[any("json"), typed("key", TEXT)];

/// `json_keys(json)` / `json_values(json)` / `json_length(json)` /
/// `json_len(json)` / `json_type(json)` — a single JSON-shaped value.
pub static JSON_1_ARGS: &[ArgTypeSpec] = &[any("json")];

/// `json_array(value, ...)` — zero or more values of any type.
pub static JSON_ARRAY_ARGS: &[ArgTypeSpec] = &[any_variadic("value")];

/// `json_object(key, value, ...)` — a flat, variadic key/value list.
pub static JSON_OBJECT_ARGS: &[ArgTypeSpec] = &[any_variadic("kv")];

/// `json_contains(container, needle)`.
pub static JSON_CONTAINS_ARGS: &[ArgTypeSpec] = &[any("container"), any("needle")];

/// `json_merge(base, overlay)` / `json_patch(base, overlay)`.
pub static JSON_MERGE_ARGS: &[ArgTypeSpec] = &[any("base"), any("overlay")];

// ── Sequence accessors ───────────────────────────────────────────────────────

/// `nextval(name)` / `currval(name)` — one sequence name.
pub static SEQUENCE_NAME_ARGS: &[ArgTypeSpec] = &[typed("sequence", TEXT)];

/// `setval(name, value)` — a sequence name and the value it must take.
pub static SETVAL_ARGS: &[ArgTypeSpec] = &[typed("sequence", TEXT), typed("value", INT64_ONLY)];
