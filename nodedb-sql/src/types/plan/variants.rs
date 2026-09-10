// SPDX-License-Identifier: Apache-2.0

//! The `SqlPlan` enum — top-level plan produced by the SQL planner.

use crate::fts_types::FtsQuery;
use crate::temporal::TemporalScope;
use crate::types_array;
use crate::types_expr::{SqlExpr, SqlPayloadAtom, SqlValue};
pub use nodedb_types::vector_distance::DistanceMetric;

use crate::types::filter::Filter;
use crate::types::query::{
    AggOutputSlot, AggregateExpr, EngineType, JoinType, Projection, SortKey, SpatialPredicate,
    WindowSpec,
};

use super::merge_types::MergePlanClause;
use super::row_types::{KvInsertIntent, VectorPrimaryRow, WriteRoute};
use super::vector_opts::{ArrayPrefilter, VectorAnnOptions};

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
    DocumentIndexLookup {
        collection: String,
        alias: Option<String>,
        engine: EngineType,
        /// Indexed field path used for the lookup.
        field: String,
        /// Equality value from the WHERE clause.
        value: SqlValue,
        /// Remaining filters after extracting the equality used for lookup.
        filters: Vec<Filter>,
        projection: Vec<Projection>,
        sort_keys: Vec<SortKey>,
        limit: Option<usize>,
        offset: usize,
        distinct: bool,
        window_functions: Vec<WindowSpec>,
        /// Whether the chosen index is COLLATE NOCASE — the executor
        /// lowercases the lookup value before probing.
        case_insensitive: bool,
        /// Bitemporal qualifier — mirrors `Scan::temporal`. Document
        /// engines must honor it at the Ceiling stage.
        temporal: TemporalScope,
    },
    RangeScan {
        collection: String,
        field: String,
        lower: Option<SqlValue>,
        upper: Option<SqlValue>,
        limit: usize,
        /// Resolved SELECT target list, for output-schema derivation.
        projection: Vec<Projection>,
    },

    // ── Writes ──
    Insert {
        collection: String,
        engine: EngineType,
        /// The lowering these rows take, chosen by the engine's `EngineRules`.
        /// The conversion layer reads it instead of re-deciding from `engine`.
        route: WriteRoute,
        rows: Vec<Vec<(String, SqlValue)>>,
        /// Column defaults from schema: `(column_name, default_expr)`.
        /// Used to auto-generate values for missing columns (e.g. `id` with `UUID_V7`).
        column_defaults: Vec<(String, String)>,
        /// `ON CONFLICT DO NOTHING` semantics: when true, duplicate-PK rows
        /// are silently skipped instead of raising `unique_violation`. Plain
        /// `INSERT` (no `ON CONFLICT` clause) sets this to `false`.
        if_absent: bool,
        /// Raw column type strings from the catalog: `(column_name, type_str)`.
        /// Forwarded from `InsertParams::column_schema`. Used by columnar
        /// converters to reconstruct the exact `ColumnType` for columns whose
        /// `SqlDataType` is ambiguous (e.g. JSON and Bytes both map to Bytes).
        column_schema: Vec<(String, String)>,
        /// Declared `PRIMARY KEY` column name (if any), from
        /// `InsertParams::primary_key`. Used by the conversion layer to
        /// extract the document id from the correct column instead of
        /// guessing at `id`/`document_id`/`key`.
        primary_key: Option<String>,
    },
    /// KV INSERT: key and value are fundamentally separate.
    /// Each entry is `(key, value_columns)`.
    KvInsert {
        collection: String,
        entries: Vec<(SqlValue, Vec<(String, SqlValue)>)>,
        /// TTL in seconds (0 = no expiry). Extracted from `ttl` column if present.
        ttl_secs: u64,
        /// INSERT-vs-UPSERT distinction. `KvOp::Put` is a Redis-SET-style
        /// upsert by design; to honor SQL `INSERT` semantics the planner must
        /// tell the converter whether a duplicate key should raise (plain
        /// `INSERT`, `Insert`), be silently skipped (`ON CONFLICT DO NOTHING`,
        /// `InsertIfAbsent`), or overwrite (`UPSERT` / `ON CONFLICT DO
        /// UPDATE`, `Put`).
        intent: KvInsertIntent,
        /// `ON CONFLICT (key) DO UPDATE SET field = expr` assignments, carried
        /// through when `intent == Put` via the ON-CONFLICT-DO-UPDATE path.
        /// Empty for plain UPSERT (whole-value overwrite) and for INSERT
        /// variants.
        on_conflict_updates: Vec<(String, SqlExpr)>,
        /// Whether any DEFAULT materialized into `entries` came from a
        /// `Volatile` expression. The key-value planner evaluates declared
        /// defaults while building this plan, so a cached plan would replay
        /// one execution's value; a volatile plan is never cached.
        volatile_defaults: bool,
    },
    /// UPSERT: insert or merge if document exists.
    Upsert {
        collection: String,
        engine: EngineType,
        /// The lowering these rows take. Mirrors `Insert::route`.
        route: WriteRoute,
        rows: Vec<Vec<(String, SqlValue)>>,
        column_defaults: Vec<(String, String)>,
        /// `ON CONFLICT (...) DO UPDATE SET field = expr` assignments.
        /// When empty, upsert is a plain merge: new columns overwrite existing.
        /// When non-empty, the engine applies these per-row against the
        /// *existing* document instead of merging the inserted values.
        on_conflict_updates: Vec<(String, SqlExpr)>,
        /// Raw column type strings from the catalog: `(column_name, type_str)`.
        /// Mirrors `Insert::column_schema` — see that field for rationale.
        column_schema: Vec<(String, String)>,
        /// Declared `PRIMARY KEY` column name (if any). See `Insert::primary_key`.
        primary_key: Option<String>,
    },
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
    TimeseriesScan {
        collection: String,
        time_range: (i64, i64),
        bucket_interval_ms: i64,
        group_by: Vec<String>,
        aggregates: Vec<AggregateExpr>,
        filters: Vec<Filter>,
        projection: Vec<Projection>,
        gap_fill: String,
        limit: usize,
        /// ORDER BY applied to the scan result. Empty = the engine's natural
        /// order (ascending by the collection's time key).
        sort_keys: Vec<SortKey>,
        tiered: bool,
        /// Bitemporal system-time / valid-time scope. Only non-default
        /// on collections created `WITH BITEMPORAL`; `TimeseriesRules::plan_scan`
        /// rejects temporal scopes otherwise.
        temporal: TemporalScope,
    },
    TimeseriesIngest {
        collection: String,
        rows: Vec<Vec<(String, SqlValue)>>,
    },

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
    TextSearch {
        collection: String,
        /// Structured FTS query.  Use `FtsQuery::Plain { text, fuzzy }` for
        /// simple keyword search.  `FtsQuery::And/Or/Prefix` are supported;
        /// `FtsQuery::Phrase` and `FtsQuery::Not` are represented but rejected
        /// by the executor with `Unsupported`.
        query: FtsQuery,
        top_k: usize,
        filters: Vec<Filter>,
        /// When set, the SELECT list contains `bm25_score(field, term)` and the
        /// caller wants a full-collection scan with the score injected under this
        /// alias. The converter emits `TextOp::BM25ScoreScan` instead of
        /// `TextOp::Search` so that all documents — including non-matching ones —
        /// appear in the response with `null` for the score when they do not
        /// contain the term.
        score_alias: Option<String>,
        /// Resolved SELECT target list, for output-schema derivation.
        projection: Vec<Projection>,
    },
    HybridSearch {
        collection: String,
        query_vector: Vec<f32>,
        query_text: String,
        top_k: usize,
        ef_search: usize,
        vector_weight: f32,
        fuzzy: bool,
        /// SELECT-list alias the response should use for the RRF score
        /// column. `None` means the executor falls back to the fixed
        /// internal field name `rrf_score`. Set by the planner from the
        /// SELECT projection's `AS <alias>` for the `rrf_score(...)` call.
        score_alias: Option<String>,
        /// Resolved SELECT target list, for output-schema derivation.
        projection: Vec<Projection>,
    },

    /// Three-source hybrid search: vector + BM25 text + graph BFS, fused via weighted RRF.
    ///
    /// Produced when the planner detects `rrf_score(vector_distance(...),
    /// bm25_score(...), graph_score(...))` with three source arguments.
    HybridSearchTriple {
        collection: String,
        query_vector: Vec<f32>,
        query_text: String,
        /// Node id used as the BFS seed for the graph leg.
        graph_seed_id: String,
        /// Maximum BFS depth from the seed node.
        graph_depth: usize,
        /// Edge label filter for graph BFS. `None` = all edges.
        graph_edge_label: Option<String>,
        top_k: usize,
        ef_search: usize,
        fuzzy: bool,
        /// Per-source RRF k constants: (vector_k, text_k, graph_k).
        rrf_k: (f64, f64, f64),
        /// SELECT-list alias for the fused RRF score column.
        score_alias: Option<String>,
        /// Resolved SELECT target list, for output-schema derivation.
        projection: Vec<Projection>,
    },
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
    RecursiveScan {
        collection: String,
        base_filters: Vec<Filter>,
        recursive_filters: Vec<Filter>,
        /// Equi-join link for tree-traversal recursion:
        /// `(collection_field, working_table_field)`.
        /// e.g. `("parent_id", "id")` means each iteration finds rows
        /// where `collection.parent_id` matches a `working_table.id`.
        join_link: Option<(String, String)>,
        max_iterations: usize,
        distinct: bool,
        limit: usize,
        /// Resolved SELECT target list, for output-schema derivation.
        projection: Vec<Projection>,
    },

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
    RecursiveValue {
        /// CTE name (used in error messages).
        cte_name: String,
        /// Column names declared on the CTE (e.g. `(n)` in `c(n) AS ...`).
        columns: Vec<String>,
        /// Anchor SELECT expressions as raw SQL text (one per column).
        init_exprs: Vec<String>,
        /// Recursive step SELECT expressions as raw SQL text (one per column).
        /// May reference column names from `columns`.
        step_exprs: Vec<String>,
        /// Optional WHERE condition as raw SQL text applied to each new row
        /// to decide whether to continue.  `None` → run until fixed point.
        condition: Option<String>,
        /// Maximum iterations before a `RecursionDepthExceeded` error is raised.
        max_depth: usize,
        /// `false` → UNION ALL (keep duplicates); `true` → UNION (deduplicate).
        distinct: bool,
    },

    /// Non-recursive CTE: execute each definition, then the outer query.
    Cte {
        /// CTE definitions: `(name, subquery_plan)`.
        definitions: Vec<(String, SqlPlan)>,
        /// The outer query that references CTE names.
        outer: Box<SqlPlan>,
    },

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
    CreateArray {
        name: String,
        dims: Vec<types_array::ArrayDimAst>,
        attrs: Vec<types_array::ArrayAttrAst>,
        tile_extents: Vec<i64>,
        cell_order: types_array::ArrayCellOrderAst,
        tile_order: types_array::ArrayTileOrderAst,
        /// Hilbert-prefix bits for vShard routing (1–16, default 8).
        prefix_bits: u8,
        /// Audit-retention horizon in milliseconds. `None` = non-bitemporal.
        audit_retain_ms: Option<u64>,
        /// Compliance floor for `audit_retain_ms`. `None` = no floor.
        minimum_audit_retain_ms: Option<u64>,
    },
    /// `DROP ARRAY [IF EXISTS] <name>` — pure Control-Plane catalog
    /// mutation. Per-core array store cleanup happens lazily.
    DropArray { name: String, if_exists: bool },
    /// `ALTER ARRAY <name> SET (audit_retain_ms = N, ...)`.
    ///
    /// Double-`Option` semantics for each diff field:
    /// - `None`          = key was absent from SET clause → field unchanged.
    /// - `Some(None)`    = key present with value `NULL` → field set to NULL.
    /// - `Some(Some(v))` = key present with integer value → field set to v.
    AlterArray {
        name: String,
        /// New value for `audit_retain_ms`. `Some(None)` unregisters
        /// the array from the bitemporal retention registry.
        audit_retain_ms: Option<Option<i64>>,
        /// New value for `minimum_audit_retain_ms`. Cannot be NULL.
        minimum_audit_retain_ms: Option<u64>,
    },
    /// `INSERT INTO ARRAY <name> COORDS (...) VALUES (...) [, ...]`.
    InsertArray {
        name: String,
        rows: Vec<types_array::ArrayInsertRow>,
    },
    /// `DELETE FROM ARRAY <name> WHERE COORDS IN ((...), (...))`.
    DeleteArray {
        name: String,
        coords: Vec<Vec<types_array::ArrayCoordLiteral>>,
    },
    /// `SELECT * FROM ARRAY_SLICE(name, {dim:[lo,hi],..}, [attrs], limit)`.
    ArraySlice {
        name: String,
        slice: types_array::ArraySliceAst,
        /// Attribute names. Empty = all attrs.
        attr_projection: Vec<String>,
        /// 0 = unlimited.
        limit: u32,
        /// Bitemporal qualifier. When both axes are `None` / `Any`, the Data
        /// Plane returns the live (current) state — the default fast path.
        /// Populated from `AS OF SYSTEM TIME` / `AS OF VALID TIME` clauses.
        temporal: TemporalScope,
    },
    /// `SELECT * FROM ARRAY_PROJECT(name, [attrs])`.
    ArrayProject {
        name: String,
        /// Attribute names. Must be non-empty.
        attr_projection: Vec<String>,
    },
    /// `SELECT * FROM ARRAY_AGG(name, attr, reducer [, group_by_dim])`.
    ArrayAgg {
        name: String,
        attr: String,
        reducer: types_array::ArrayReducerAst,
        /// `None` = scalar fold; `Some(name)` = group by that dim.
        group_by_dim: Option<String>,
        /// Bitemporal qualifier. When both axes are `None` / `Any`, the Data
        /// Plane aggregates against the live (current) state — the default
        /// fast path. Populated from `AS OF SYSTEM TIME` / `AS OF VALID TIME`.
        temporal: TemporalScope,
    },
    /// `SELECT * FROM ARRAY_ELEMENTWISE(left, right, op, attr)`.
    ArrayElementwise {
        left: String,
        right: String,
        op: types_array::ArrayBinaryOpAst,
        attr: String,
    },
    /// `SELECT ARRAY_FLUSH(name)` — returns one row `{result: BOOL}`.
    ArrayFlush { name: String },
    /// `SELECT ARRAY_COMPACT(name)` — returns one row `{result: BOOL}`.
    ArrayCompact { name: String },

    // ── MERGE ──────────────────────────────────────────────────────────
    /// `MERGE INTO target USING source ON ... WHEN ... THEN ...`
    ///
    /// Supported only for `document_schemaless` and `document_strict` engines.
    /// The Data Plane handler evaluates WHEN arms in declaration order and
    /// applies the first matching action to each joined or unmatched row.
    Merge {
        target: String,
        engine: EngineType,
        /// Source plan (Scan, DocumentIndexLookup, or Join of a sub-select).
        source: Box<SqlPlan>,
        /// Column in the target used for the equi-join (from ON clause).
        target_join_col: String,
        /// Column in the source used for the equi-join (from ON clause).
        source_join_col: String,
        /// Alias used to qualify source columns in expressions (e.g. `src.col`).
        source_alias: String,
        /// WHEN arms in declaration order.
        clauses: Vec<MergePlanClause>,
        returning: bool,
    },

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
    LateralTopK {
        /// Plan producing outer rows.
        outer: Box<SqlPlan>,
        /// Alias used to qualify outer table columns (e.g. `"u"`).
        outer_alias: Option<String>,
        /// Inner collection to scan.
        inner_collection: String,
        /// Pre-filter applied to inner rows (non-correlated filters).
        inner_filters: Vec<Filter>,
        /// Sort keys for the inner per-outer-row result.
        inner_order_by: Vec<SortKey>,
        /// Maximum number of inner rows per outer row.
        inner_limit: usize,
        /// Equi-join pairs `(outer_col, inner_col)`.
        correlation_keys: Vec<(String, String)>,
        /// Alias under which inner rows are presented.
        lateral_alias: String,
        /// Post-lateral projection.
        projection: Vec<Projection>,
        /// LEFT join semantics: preserve outer rows even when inner is empty.
        left_join: bool,
    },

    /// General LATERAL subquery — per-outer-row correlated nested loop.
    ///
    /// Emitted for LATERAL subqueries that cannot be rewritten as equi-join
    /// hash joins or `LateralTopK`. The Control Plane drives execution: it
    /// materialises outer rows, then for each row substitutes the correlation
    /// values as additional filters on the inner plan and re-dispatches it.
    ///
    /// Bounded by `outer_row_cap`; queries that exceed the cap receive a typed
    /// `SqlError::Unsupported` before any data is returned.
    LateralLoop {
        /// Plan producing outer rows.
        outer: Box<SqlPlan>,
        /// Alias used to qualify outer table columns.
        outer_alias: Option<String>,
        /// Inner subquery plan (correlation predicates are injected at runtime).
        inner: Box<SqlPlan>,
        /// Correlated predicates extracted from the inner WHERE that reference
        /// outer columns.  Each entry is `(inner_field, outer_field)`.
        correlation_predicates: Vec<(String, String)>,
        /// Alias under which inner rows are presented.
        lateral_alias: String,
        /// Post-lateral projection.
        projection: Vec<Projection>,
        /// Maximum outer rows allowed. Queries exceeding this return an error.
        outer_row_cap: usize,
        /// LEFT join semantics: preserve outer rows even when inner is empty.
        left_join: bool,
    },

    // ── Vector-primary ──────────────────────────────────────────────────
    /// INSERT into a vector-primary collection.
    ///
    /// Emitted by the planner instead of the generic `Insert` variant when the
    /// target collection has `primary = PrimaryEngine::Vector`. The Data Plane
    /// routes each row through `VectorOp::DirectUpsert`, bypassing full-document
    /// MessagePack encoding.
    VectorPrimaryInsert {
        collection: String,
        /// Vector column name (matches `VectorPrimaryConfig::vector_field`).
        /// Plumbed to `VectorOp::DirectUpsert` so the Data Plane keys its
        /// HNSW index by `(tid, collection, field)` — the same key the SELECT
        /// path uses.
        field: String,
        /// Collection-level quantization. Applied via `set_quantization` on
        /// the first DirectUpsert so subsequent seals trigger codec-dispatch
        /// rebuilds against the configured codec.
        quantization: nodedb_types::VectorQuantization,
        /// Native storage dtype for vector values (F32 / F16 / BF16).
        storage_dtype: nodedb_types::VectorStorageDtype,
        /// Payload field names that get equality bitmap indexes. Registered
        /// via `payload.add_index` on the first DirectUpsert.
        payload_indexes: Vec<(String, nodedb_types::PayloadIndexKind)>,
        rows: Vec<VectorPrimaryRow>,
        /// Whether any row carries a value materialized from a volatile
        /// DEFAULT such as `nextval`. The value was evaluated while the plan
        /// was built, so caching the lowered tasks would replay one
        /// execution's value into every later one.
        volatile_defaults: bool,
    },

    // ── Index DDL ───────────────────────────────────────────────────────
    /// `CREATE [UNIQUE] INDEX [IF NOT EXISTS] name ON collection (field)`
    ///
    /// Registers a secondary index on the named field of a document or KV
    /// collection. The executor backtracks existing rows into the index so
    /// it is immediately consistent.
    CreateIndex {
        /// Name of the index. `None` requests an auto-generated name.
        index_name: Option<String>,
        /// Target collection.
        collection: String,
        /// Indexed field path.
        field: String,
        /// Whether the index enforces uniqueness.
        unique: bool,
        /// `IF NOT EXISTS` — succeed silently if the index already exists.
        if_not_exists: bool,
        /// Case-insensitive string collation (`COLLATE NOCASE`).
        case_insensitive: bool,
    },

    /// `DROP INDEX [IF EXISTS] name [ON collection]`
    ///
    /// Removes a secondary index from a collection. All index entries are
    /// erased and the index metadata is unregistered.
    DropIndex {
        /// Name of the index.
        index_name: String,
        /// Target collection (may be inferred from the index catalog).
        collection: Option<String>,
        /// `IF EXISTS` — succeed silently if the index does not exist.
        if_exists: bool,
    },
}
