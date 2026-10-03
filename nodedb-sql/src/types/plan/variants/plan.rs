// SPDX-License-Identifier: Apache-2.0

//! The `SqlPlan` enum — top-level plan produced by the SQL planner. Larger
//! payloads live in per-family structs beside it.

use crate::temporal::TemporalScope;
use crate::types_expr::{SqlExpr, SqlPayloadAtom, SqlValue};
pub use nodedb_types::vector_distance::DistanceMetric;

use crate::types::filter::Filter;
use crate::types::query::{
    AggOutputSlot, AggregateExpr, EngineType, JoinType, Projection, SortKey, SpatialPredicate,
    WindowSpec,
};

use super::super::vector_opts::{ArrayPrefilter, VectorAnnOptions};

use super::array::{
    AlterArrayPlan, ArrayAggPlan, ArrayElementwisePlan, ArrayProjectPlan, ArraySlicePlan,
    CreateArrayPlan, DeleteArrayPlan, InsertArrayPlan,
};
use super::cte::CtePlan;
use super::hybrid::{HybridSearchPlan, HybridSearchTriplePlan};
use super::index_ddl::{CreateIndexPlan, DropIndexPlan};
use super::index_reads::{DocumentIndexLookupPlan, RangeScanPlan};
use super::lateral::{LateralLoopPlan, LateralTopKPlan};
use super::merge::MergePlan;
use super::recursive::{RecursiveScanPlan, RecursiveValuePlan};
use super::text::TextSearchPlan;
use super::timeseries::{TimeseriesIngestPlan, TimeseriesScanPlan};
use super::vector_primary::{
    VectorPrimaryDeletePlan, VectorPrimaryInsertPlan, VectorPrimaryTruncatePlan,
    VectorPrimaryUpdatePlan,
};
use super::writes::{InsertPlan, KvInsertPlan, UpsertPlan};

/// The top-level plan produced by the SQL planner.
#[derive(Debug, Clone)]
pub enum SqlPlan {
    // ── Constant ──
    /// Query with no FROM clause: SELECT 1, SELECT 'hello' AS name, etc.
    /// Produces a single row with evaluated constant expressions.
    ConstantResult {
        columns: Vec<String>,
        values: Vec<SqlValue>,
        /// Whether any projected expression called a `Volatile` function.
        /// The values were evaluated while this plan was built, so a cached
        /// plan would replay them; a volatile plan is never cached.
        volatile: bool,
    },

    // ── Reads ──
    Scan {
        collection: String,
        alias: Option<String>,
        engine: EngineType,
        filters: Vec<Filter>,
        projection: Vec<Projection>,
        sort_keys: Vec<SortKey>,
        limit: Option<usize>,
        offset: usize,
        distinct: bool,
        window_functions: Vec<WindowSpec>,
        /// Bitemporal qualifier extracted from `FOR SYSTEM_TIME` /
        /// `FOR VALID_TIME`. Default when the scan is current-state.
        temporal: TemporalScope,
    },
    PointGet {
        collection: String,
        alias: Option<String>,
        engine: EngineType,
        key_column: String,
        key_value: SqlValue,
        /// Resolved SELECT target list, for output-schema derivation.
        projection: Vec<Projection>,
    },
    /// Document fetch via a secondary index: equality predicate on an
    /// indexed field. The executor performs an index lookup to resolve
    /// matching document IDs, reads each document, and applies any
    /// remaining filters, projection, sort, and limit.
    ///
    /// Emitted by `document_schemaless::plan_scan` /
    /// `document_strict::plan_scan` when the WHERE clause contains a
    /// single equality predicate on a `Ready` indexed field. Any
    /// additional predicates fall through as post-filters.
    DocumentIndexLookup(DocumentIndexLookupPlan),
    RangeScan(RangeScanPlan),

    // ── Writes ──
    Insert(InsertPlan),
    /// KV INSERT: key and value are fundamentally separate.
    /// Each entry is `(key, value_columns)`.
    KvInsert(KvInsertPlan),
    /// UPSERT: insert or merge if document exists.
    Upsert(UpsertPlan),
    InsertSelect {
        target: String,
        source: Box<SqlPlan>,
        limit: usize,
        /// `(target_column, source_expression)`, in target-column order.
        ///
        /// Empty means passthrough: `INSERT INTO t SELECT * FROM s` copies
        /// each source row unchanged. Non-empty means every target column is
        /// materialized from the paired expression over the source row.
        column_map: Vec<(String, SqlExpr)>,
    },
    Update {
        collection: String,
        engine: EngineType,
        assignments: Vec<(String, SqlExpr)>,
        filters: Vec<Filter>,
        target_keys: Vec<SqlValue>,
        returning: bool,
    },
    /// `UPDATE target SET col = src.col2 FROM src WHERE target.id = src.id`
    ///
    /// Two-phase execution: scan `source` with `source_filters`, then for
    /// each matched source row that satisfies the join predicates against a
    /// target row, apply `assignments` (which may reference source columns
    /// via qualified names `src.col`).
    ///
    /// `join_predicates` are equality pairs `(target_col, source_col)` extracted
    /// from the WHERE clause linking the two tables. `target_filters` are
    /// remaining WHERE predicates that reference only `target`.
    UpdateFrom {
        collection: String,
        engine: EngineType,
        /// The FROM source: a `Scan`, `Join`, or other read plan.
        source: Box<SqlPlan>,
        /// Column name used as the target's join key (e.g. `"id"`).
        target_join_col: String,
        /// Column name used as the source's join key (e.g. `"id"`).
        source_join_col: String,
        /// SET assignments — RHS may be `SqlExpr::Column { table: Some("src"), .. }`.
        assignments: Vec<(String, SqlExpr)>,
        /// Filters that apply only to the target collection.
        target_filters: Vec<Filter>,
        returning: bool,
    },
    Delete {
        collection: String,
        engine: EngineType,
        filters: Vec<Filter>,
        target_keys: Vec<SqlValue>,
    },
    Truncate {
        collection: String,
        engine: EngineType,
        restart_identity: bool,
    },

    // ── Joins ──
    Join {
        left: Box<SqlPlan>,
        right: Box<SqlPlan>,
        on: Vec<(String, String)>,
        join_type: JoinType,
        condition: Option<SqlExpr>,
        /// `None` = no SQL `LIMIT` clause (output bounded downstream by the
        /// memory byte budget, never silently truncated); `Some(n)` = explicit
        /// `LIMIT n` (output capped at exactly `n`).
        limit: Option<usize>,
        /// Post-join projection: column names to keep (empty = all columns).
        projection: Vec<Projection>,
        /// Post-join filters (from WHERE clause).
        filters: Vec<Filter>,
    },

    // ── Aggregation ──
    Aggregate {
        input: Box<SqlPlan>,
        group_by: Vec<SqlExpr>,
        /// SELECT-list output alias for each GROUP BY key, parallel to
        /// `group_by`. `Some(alias)` when the projection aliased the key
        /// (`SELECT k AS label ... GROUP BY k` → `Some("label")`); `None`
        /// when the projection has no explicit alias for that key, so the
        /// output column name falls back to the raw grouped column name.
        /// Empty when the plan was built without a projection in scope
        /// (treated the same as all-`None`).
        group_by_aliases: Vec<Option<String>>,
        /// SELECT-list interleaving of `group_by` keys and `aggregates`, in
        /// output order. Empty when built without a projection in scope (see
        /// `AggOutputSlot`) — output_schema falls back to group-keys-first.
        output_order: Vec<AggOutputSlot>,
        aggregates: Vec<AggregateExpr>,
        having: Vec<Filter>,
        limit: usize,
        /// When the GROUP BY contains ROLLUP/CUBE/GROUPING SETS, this field holds
        /// the expansion. Each inner `Vec<usize>` is one grouping set — the indices
        /// into `group_by` (the canonical key list) that are *present* (non-NULL)
        /// for rows in that set.  `None` = plain single-set GROUP BY.
        grouping_sets: Option<Vec<Vec<usize>>>,
        /// ORDER BY applied to the aggregated rows. Empty = no sort
        /// (executor returns groups in hash-map iteration order).
        /// Populated by `apply_order_by` when an outer ORDER BY
        /// targets a GROUP BY result; the Aggregate executor sorts the
        /// finalized group rows before returning.
        sort_keys: Vec<SortKey>,
    },

    // ── Timeseries ──
    TimeseriesScan(TimeseriesScanPlan),
    TimeseriesIngest(TimeseriesIngestPlan),

    // ── Search (first-class) ──
    VectorSearch {
        collection: String,
        field: String,
        query_vector: Vec<f32>,
        top_k: usize,
        ef_search: usize,
        /// Distance metric requested by the query operator (`<->`, `<=>`, `<#>`).
        /// Overrides the collection-default metric at search time.
        metric: DistanceMetric,
        filters: Vec<Filter>,
        /// Optional cross-engine prefilter: when set, the ND-array slice
        /// runs first and its output cells' surrogates form a bitmap that
        /// gates the HNSW candidate set. Set by the planner when an
        /// `ORDER BY vector_distance(...) LIMIT k` query is JOINed against
        /// `ARRAY_SLICE(...)`. The convert layer lowers this to
        /// `VectorOp::Search { inline_prefilter_plan: Some(ArrayOp::SurrogateBitmapScan) }`.
        array_prefilter: Option<ArrayPrefilter>,
        /// ANN knobs parsed from the optional third JSON-string argument
        /// to `vector_distance(field, query, '{...}')`.
        ann_options: VectorAnnOptions,
        /// When `true`, the projection contains only the surrogate/PK column
        /// and/or `vector_distance(...)` — no payload fields. The Data Plane
        /// can skip the document-body fetch entirely for vector-primary
        /// collections. Always `false` for non-vector-primary collections
        /// (document body is the primary result).
        skip_payload_fetch: bool,
        /// Predicates against payload-indexed columns on a vector-primary
        /// collection. Each atom is `Eq(field, value)`, `In(field, values)`,
        /// or `Range(field, ...)`. The convert layer translates SqlValue →
        /// nodedb_types::Value and emits them as
        /// `VectorOp::Search::payload_filters`. The Data Plane intersects
        /// the resulting bitmap with the HNSW candidate set via the
        /// per-collection `PayloadIndexSet::pre_filter`.
        payload_filters: Vec<SqlPayloadAtom>,
        /// Primary keys a top-level `WHERE pk = v` / `pk IN (...)` conjunct
        /// names. The search ranks only those rows: the convert layer lowers
        /// the keys to the candidate bitmap the index search honors. `None`:
        /// no key restriction. `Some(empty)`: no row is a candidate.
        pk_prefilter: Option<Vec<SqlValue>>,
        /// Resolved SELECT target list, for output-schema derivation.
        projection: Vec<Projection>,
    },
    MultiVectorSearch {
        collection: String,
        query_vector: Vec<f32>,
        top_k: usize,
        ef_search: usize,
        /// Resolved SELECT target list, for output-schema derivation.
        projection: Vec<Projection>,
    },
    /// Sparse-vector inverted-index search.
    ///
    /// Produced by the planner when the leading ORDER BY expression is
    /// `sparse_score(field, '{dim: weight, ...}')`. The query literal is
    /// parsed into `(dimension, weight)` entries at plan time. The convert
    /// layer lowers this to `VectorOp::SparseSearch`, which returns the
    /// `top_k` documents with the highest dot-product score — matching the
    /// `DESC` (similarity) ordering the SQL author wrote.
    SparseSearch {
        collection: String,
        field: String,
        query_entries: Vec<(u32, f32)>,
        top_k: usize,
        /// Resolved SELECT target list, for output-schema derivation.
        projection: Vec<Projection>,
    },
    /// Full-text search: `WHERE text_match(...)` matches, or a
    /// `bm25_score(...)` score scan over every row the filters admit.
    TextSearch(TextSearchPlan),
    HybridSearch(HybridSearchPlan),

    /// Three-source hybrid search: vector + BM25 text + graph BFS, fused via weighted RRF.
    ///
    /// Produced when the planner detects `rrf_score(vector_distance(...),
    /// bm25_score(...), graph_score(...))` with three source arguments.
    HybridSearchTriple(HybridSearchTriplePlan),
    SpatialScan {
        collection: String,
        field: String,
        predicate: SpatialPredicate,
        query_geometry: nodedb_types::geometry::Geometry,
        distance_meters: f64,
        attribute_filters: Vec<Filter>,
        limit: usize,
        projection: Vec<Projection>,
    },

    // ── Composite ──
    Union {
        inputs: Vec<SqlPlan>,
        distinct: bool,
    },
    Intersect {
        left: Box<SqlPlan>,
        right: Box<SqlPlan>,
        all: bool,
    },
    Except {
        left: Box<SqlPlan>,
        right: Box<SqlPlan>,
        all: bool,
    },
    RecursiveScan(RecursiveScanPlan),

    /// Value-generating recursive CTE (`WITH RECURSIVE name(cols) AS (anchor UNION [ALL] step)`).
    ///
    /// Unlike `RecursiveScan`, this variant carries no collection reference — the anchor
    /// row is produced entirely from literal expressions and each iteration applies the
    /// step expressions to the previous row.  The executor evaluates this iteratively
    /// in the Data Plane without touching storage.
    ///
    /// All expressions are stored as raw SQL text so they can be serialised across the
    /// SPSC bridge without requiring `SqlExpr` to implement `Serialize`.  The executor
    /// parses them at execution time via the same lightweight expression evaluator used
    /// by the procedural executor.
    RecursiveValue(RecursiveValuePlan),

    /// Non-recursive CTE: execute each definition, then the outer query.
    Cte(CtePlan),

    /// Relational post-processing over a subquery/derived-table body whose leaf
    /// plan cannot absorb the outer query's constraints.
    ///
    /// Produced by CTE / derived-table inlining when the referenced body is not
    /// a plain `Scan` (which carries its own filters/sort/limit/offset/distinct)
    /// and the outer reference adds constraints the body has no slot for — an
    /// `ORDER BY`, `OFFSET`, `DISTINCT`, or a `LIMIT` that must apply *after* a
    /// reorder. Without this node those constraints were silently dropped,
    /// returning the body's unordered/unoffset/undeduplicated rows.
    ///
    /// The Data Plane materializes `input`'s rows, then applies, in this order:
    /// filter → offset → sort → distinct → project → limit — matching
    /// `QueryOp::ProviderScan` semantics (which this lowers onto). Constraints
    /// the body CAN absorb (e.g. an unordered `LIMIT` folded into a vector
    /// search's `top_k`, or a `WHERE` pushed into the engine as a post-filter)
    /// are applied at the leaf during inlining and are NOT repeated here.
    Subquery {
        /// The subquery body whose rows are post-processed.
        input: Box<SqlPlan>,
        /// Predicates applied to the materialized rows (outer `WHERE` that the
        /// body leaf did not absorb).
        filters: Vec<Filter>,
        /// Outer projection (target list). Empty = inherit the body's columns.
        projection: Vec<Projection>,
        /// Window functions evaluated over the post-processed rows. Empty = none.
        window_functions: Vec<WindowSpec>,
        /// Outer `ORDER BY` keys applied over the materialized rows.
        sort_keys: Vec<SortKey>,
        /// Outer `OFFSET` (0 = none).
        offset: usize,
        /// Outer `DISTINCT`.
        distinct: bool,
        /// Outer `LIMIT` (`None` = unbounded).
        limit: Option<usize>,
    },

    // ── Array (ND sparse) ─────────────────────────────────────
    /// `CREATE ARRAY <name> DIMS (...) ATTRS (...) TILE_EXTENTS (...)`.
    /// AST is engine-agnostic — the Origin converter builds the typed
    /// `nodedb_array::ArraySchema` and persists the catalog row.
    CreateArray(CreateArrayPlan),
    /// `DROP ARRAY [IF EXISTS] <name>` — pure Control-Plane catalog
    /// mutation. Per-core array store cleanup happens lazily.
    DropArray {
        name: String,
        if_exists: bool,
    },
    /// `ALTER ARRAY <name> SET (audit_retain_ms = N, ...)`.
    ///
    /// Double-`Option` semantics for each diff field:
    /// - `None`          = key was absent from SET clause → field unchanged.
    /// - `Some(None)`    = key present with value `NULL` → field set to NULL.
    /// - `Some(Some(v))` = key present with integer value → field set to v.
    AlterArray(AlterArrayPlan),
    /// `INSERT INTO ARRAY <name> COORDS (...) VALUES (...) [, ...]`.
    InsertArray(InsertArrayPlan),
    /// `DELETE FROM ARRAY <name> WHERE COORDS IN ((...), (...))`.
    DeleteArray(DeleteArrayPlan),
    /// `SELECT * FROM ARRAY_SLICE(name, {dim:[lo,hi],..}, [attrs], limit)`.
    ArraySlice(ArraySlicePlan),
    /// `SELECT * FROM ARRAY_PROJECT(name, [attrs])`.
    ArrayProject(ArrayProjectPlan),
    /// `SELECT * FROM ARRAY_AGG(name, attr, reducer [, group_by_dim])`.
    ArrayAgg(ArrayAggPlan),
    /// `SELECT * FROM ARRAY_ELEMENTWISE(left, right, op, attr)`.
    ArrayElementwise(ArrayElementwisePlan),
    /// `SELECT ARRAY_FLUSH(name)` — returns one row `{result: BOOL}`.
    ArrayFlush {
        name: String,
    },
    /// `SELECT ARRAY_COMPACT(name)` — returns one row `{result: BOOL}`.
    ArrayCompact {
        name: String,
    },

    // ── MERGE ──────────────────────────────────────────────────────────
    /// `MERGE INTO target USING source ON ... WHEN ... THEN ...`
    ///
    /// Supported only for `document_schemaless` and `document_strict` engines.
    /// The Data Plane handler evaluates WHEN arms in declaration order and
    /// applies the first matching action to each joined or unmatched row.
    Merge(MergePlan),

    // ── Lateral joins ───────────────────────────────────────────────────
    /// LATERAL subquery that is equi-correlated and has ORDER BY + LIMIT k.
    ///
    /// Emitted when the inner subquery has an equi-key correlation to the outer
    /// table plus an `ORDER BY ... LIMIT k` clause. The Data Plane scans the
    /// inner collection once per outer row applying the equi-filter, sorts by
    /// `inner_order_by`, and retains at most `inner_limit` rows.
    ///
    /// `correlation_keys` is `(outer_col, inner_col)` — the equi-join pairs
    /// that correlate inner to outer.
    LateralTopK(LateralTopKPlan),

    /// General LATERAL subquery — per-outer-row correlated nested loop.
    ///
    /// Emitted for LATERAL subqueries that cannot be rewritten as equi-join
    /// hash joins or `LateralTopK`. The Control Plane drives execution: it
    /// materialises outer rows, then for each row substitutes the correlation
    /// values as additional filters on the inner plan and re-dispatches it.
    ///
    /// Bounded by `outer_row_cap`; queries that exceed the cap receive a typed
    /// `SqlError::Unsupported` before any data is returned.
    LateralLoop(LateralLoopPlan),

    // ── Vector-primary ──────────────────────────────────────────────────
    /// INSERT / UPSERT into a vector-primary collection.
    ///
    /// Emitted by the planner instead of the generic `Insert` / `Upsert`
    /// variants when the target collection has `primary =
    /// PrimaryEngine::Vector`. Each row lowers to one of
    /// `VectorOp::DirectInsert` / `DirectInsertIfAbsent` / `DirectUpsert`
    /// per `intent`, bypassing full-document MessagePack encoding.
    VectorPrimaryInsert(VectorPrimaryInsertPlan),
    /// DELETE on a vector-primary collection.
    ///
    /// `target_keys` carries the primary keys when the WHERE clause is a
    /// pure primary-key equality (or IN / OR of equalities). Otherwise
    /// `filters` is evaluated against every sidecar row on the Data Plane.
    VectorPrimaryDelete(VectorPrimaryDeletePlan),
    /// TRUNCATE on a vector-primary collection.
    ///
    /// Removes every row from the HNSW index and its payload sidecar.
    VectorPrimaryTruncate(VectorPrimaryTruncatePlan),
    /// UPDATE on a vector-primary collection.
    ///
    /// `new_vector` is the literal the statement assigns to the vector
    /// column, when it assigns one. Every other assignment stays in
    /// `assignments` and patches the payload sidecar.
    VectorPrimaryUpdate(VectorPrimaryUpdatePlan),

    // ── Index DDL ───────────────────────────────────────────────────────
    /// `CREATE [UNIQUE] INDEX [IF NOT EXISTS] name ON collection (field)`
    ///
    /// Registers a secondary index on the named field of a document or KV
    /// collection. The executor backtracks existing rows into the index so
    /// it is immediately consistent.
    CreateIndex(CreateIndexPlan),

    /// `DROP INDEX [IF EXISTS] name [ON collection]`
    ///
    /// Removes a secondary index from a collection. All index entries are
    /// erased and the index metadata is unregistered.
    DropIndex(DropIndexPlan),
}
