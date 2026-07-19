// SPDX-License-Identifier: Apache-2.0

//! Query operations (joins, aggregates) dispatched to the Data Plane.

pub use nodedb_query::expr::GroupKeySpec;

/// Aggregate specification for Data Plane aggregate execution.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct AggregateSpec {
    pub function: String,
    /// Internal aggregate key used by HAVING and downstream references.
    pub alias: String,
    /// Optional user-facing SQL alias for final output naming.
    pub user_alias: Option<String>,
    /// Field name for simple field-based aggregates. `"*"` is used for COUNT(*).
    pub field: String,
    /// Optional expression to evaluate per-document before aggregating.
    pub expr: Option<nodedb_query::expr::SqlExpr>,
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct JoinProjection {
    pub source: String,
    pub output: String,
}

/// Query-level physical operations (joins, aggregates).
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum QueryOp {
    /// Coordinator-resolved data-movement wrapper. Defensive error if it reaches a core.
    Exchange(crate::physical_plan::ExchangeOp),

    /// Pre-materialized rows. `provider: Some(name)` => filled per-request
    /// (identity-scoped catalog table); `None` => `rows` is final (constant
    /// SELECT or gathered broadcast child). `rows` is a canonical msgpack array
    /// of map values (the `encode_binary_rows` shape).
    ///
    /// The executor applies predicate, offset, sort, distinct, projection, and
    /// limit in that order over the rows before emitting the response. Empty
    /// `filters`/`projection` and `None` limit are no-ops (all rows emitted
    /// unchanged through those stages).
    ProviderScan {
        /// Catalog provider name for deferred materialization by the coordinator.
        /// `Some(name)` is replaced with `None` after `materialize_providers`
        /// fills `rows`. Data-Plane cores must never see `Some`.
        provider: Option<String>,
        /// Msgpack array of plain map rows. Filled by the coordinator from the
        /// catalog producer or from a gathered broadcast child.
        rows: Vec<u8>,
        /// Serialized `Vec<ScanFilter>` (MessagePack). Empty = no predicate.
        #[serde(default)]
        filters: Vec<u8>,
        /// Output column names to keep. Empty = emit all columns.
        #[serde(default)]
        projection: Vec<String>,
        /// Sort keys: `(column_name, ascending)`. Empty = unordered.
        #[serde(default)]
        sort_keys: Vec<(String, bool)>,
        /// Maximum rows to emit after offset/sort/distinct/projection.
        /// `None` = unlimited.
        #[serde(default)]
        limit: Option<usize>,
        /// Number of rows to skip before applying limit.
        #[serde(default)]
        offset: usize,
        /// SQL DISTINCT: deduplicate on the projected row.
        #[serde(default)]
        distinct: bool,
    },

    /// Aggregate: GROUP BY + aggregate functions.
    Aggregate {
        collection: String,
        /// Optional sub-plan whose decoded rows are aggregated instead of
        /// scanning `collection` per-shard. `Some` currently means EXACTLY a
        /// catalog source (a `ProviderScan` lowered by the converter): the
        /// aggregate runs over the coordinator-materialized catalog rows and is
        /// therefore coordinator-local (never broadcast — see
        /// `is_sharded_source`). `None` = legacy path: scan the named
        /// `collection` on every shard. `collection` stays populated in both
        /// cases so downstream RLS / permission / classification continue to
        /// read it; the executor simply prefers `input` when present.
        #[serde(default)]
        input: Option<Box<crate::physical_plan::PhysicalPlan>>,
        group_by: Vec<GroupKeySpec>,
        aggregates: Vec<AggregateSpec>,
        filters: Vec<u8>,
        /// HAVING predicates applied post-aggregation.
        having: Vec<u8>,
        limit: usize,
        sub_group_by: Vec<String>,
        sub_aggregates: Vec<AggregateSpec>,
        /// ROLLUP / CUBE / GROUPING SETS expansion.  Each inner `Vec<u32>` is
        /// one grouping set — the indices into `group_by` that are *present*
        /// (non-NULL) for rows in that set.  Empty outer vec = plain single-set
        /// GROUP BY (no null-filling needed).
        grouping_sets: Vec<Vec<u32>>,
        /// Post-aggregation sort keys: `(column_name, ascending)`.
        /// Empty = preserve executor's natural order (hash-map iteration
        /// for plain GROUP BY). The executor applies the sort after all
        /// groups are finalized and HAVING is filtered.
        #[serde(default)]
        sort_keys: Vec<(String, bool)>,
    },

    /// Partial aggregate: each core computes locally, Control Plane merges.
    PartialAggregate {
        collection: String,
        group_by: Vec<String>,
        aggregates: Vec<AggregateSpec>,
        filters: Vec<u8>,
    },

    /// Partial aggregate STATE producer (distributed GROUP BY shuffle, map side).
    ///
    /// Accumulates exactly like [`QueryOp::PartialAggregate`] — scan the named
    /// `collection`, apply `filters`, build per-group `GroupState` accumulators
    /// keyed on `group_by` — but DOES NOT finalize. Instead it emits one row
    /// PER GROUP of the flat shape:
    ///
    /// ```text
    /// { <group_by[0]>: value_0, ..., "__agg_state": <bytes> }
    /// ```
    ///
    /// where `__agg_state` is the serialized partial `GroupState` for that
    /// group. A downstream [`QueryOp::ShuffleAggregateConsume`] merges these
    /// partial states (from every producer shard/node) and finalizes them.
    ///
    /// This op carries NO node-local paths — it reads only the named collection
    /// (or its `input` sub-plan) — so it is wire-shippable: a coordinator embeds
    /// it in a producer plan and dispatches it to a remote node's Data Plane.
    PartialAggregateState {
        collection: String,
        /// Optional sub-plan whose decoded rows are aggregated instead of
        /// scanning `collection` per-shard. Mirrors [`QueryOp::Aggregate`]'s
        /// `input`: `Some` runs the producer over the sub-plan's rows (e.g. a
        /// coordinator-materialized `ProviderScan`), `None` scans the named
        /// `collection`. `collection` stays populated either way.
        #[serde(default)]
        input: Option<Box<crate::physical_plan::PhysicalPlan>>,
        group_by: Vec<GroupKeySpec>,
        aggregates: Vec<AggregateSpec>,
        filters: Vec<u8>,
    },

    /// Hash join: build hash map on right, probe with left.
    ///
    /// `left_input`/`right_input` carry a resolved child plan (an Exchange
    /// child during planning, or an embedded `ProviderScan` after coordinator
    /// resolution). When `None` the side is scanned locally by collection name.
    HashJoin {
        left_collection: String,
        right_collection: String,
        left_alias: Option<String>,
        right_alias: Option<String>,
        on: Vec<(String, String)>,
        join_type: String,
        limit: usize,
        /// Post-join GROUP BY columns (empty = no aggregation).
        post_group_by: Vec<String>,
        /// Post-join aggregates: (op, field) pairs (empty = no aggregation).
        post_aggregates: Vec<(String, String)>,
        /// Post-join projection: column names to keep (empty = all).
        projection: Vec<JoinProjection>,
        /// MessagePack-encoded computed projection expressions. When present,
        /// these represent the complete SELECT list in output order.
        computed_projection: Vec<u8>,
        /// Residual `ON` predicates evaluated for each equi-key candidate
        /// before outer-join match accounting (MessagePack).
        join_filters: Vec<u8>,
        /// Post-join WHERE filter predicates (MessagePack).
        post_filters: Vec<u8>,
        /// Resolved child plan for the left side. An Exchange child during
        /// planning; a `ProviderScan` after coordinator resolution. `None`
        /// means the left side is scanned locally by `left_collection`.
        left_input: Option<Box<crate::physical_plan::PhysicalPlan>>,
        /// Resolved child plan for the right side. Same semantics as
        /// `left_input` but applied to `right_collection`.
        right_input: Option<Box<crate::physical_plan::PhysicalPlan>>,
        /// Bitmap-producer sub-plan for the left side. When set, the executor
        /// executes this plan first, collects surrogates from all returned rows,
        /// and injects the resulting bitmap into the probe-side prefilter before
        /// scanning. `None` = no bitmap pushdown for the left side.
        left_bitmap: Option<Box<crate::physical_plan::PhysicalPlan>>,
        /// Bitmap-producer sub-plan for the right side. Same semantics as
        /// `left_bitmap` but applied to the right (probe) collection.
        right_bitmap: Option<Box<crate::physical_plan::PhysicalPlan>>,
    },

    /// Cross-node shuffle-join CONSUMER (E4b): run the node-local grace-hash
    /// join over two already-staged shuffle frame files.
    ///
    /// This variant is BUILT LOCALLY by the part-owner node's consume hook with
    /// resolved local absolute paths and dispatched to the SAME node's Data
    /// Plane. It carries node-local filesystem paths (as plain strings) and must
    /// NEVER be serialized cross-node — the wire encoder rejects it for exactly
    /// this reason (see `physical_plan::wire::encode`).
    ///
    /// The owned fields mirror the borrowed `JoinParams` the Data-Plane handler
    /// (`execute_shuffle_join`) reconstructs: `on` (equi-join key pairs),
    /// `join_type`, `limit`, and the two column qualifiers. `build_path` /
    /// `probe_path` are the staged build (right) and probe (left) frame files.
    ShuffleJoinConsume {
        /// Local absolute path to the staged BUILD (right) side frame file.
        build_path: String,
        /// Local absolute path to the staged PROBE (left) side frame file.
        probe_path: String,
        /// Equi-join key pairs `(left_key, right_key)`.
        on: Vec<(String, String)>,
        /// Join type (`inner` / `left` / `right` / `full`). `cross`/keyless is
        /// rejected by the handler.
        join_type: String,
        /// Output row cap. `usize::MAX` = no explicit LIMIT (budget-bounded).
        limit: usize,
        /// Column qualifier (prefix) for probe-side (left) columns.
        probe_qualifier: String,
        /// Column qualifier (prefix) for build-side (right/index) columns.
        index_qualifier: String,
    },

    /// Distributed GROUP BY shuffle CONSUMER (reduce side): merge the staged
    /// partial `GroupState`s produced by [`QueryOp::PartialAggregateState`]
    /// producers, then finalize, HAVING-filter, sort, and LIMIT.
    ///
    /// `state_path` is a node-local frame file written by the shuffle receive
    /// path: a sequence of `[u32 LE len][row-bytes]` frames, one msgpack row per
    /// frame, each row the flat `{<group_by cols>, "__agg_state": <bytes>}` shape
    /// a `PartialAggregateState` producer emits. The handler rebuilds each
    /// group key (byte-identically to the accumulate path), decodes
    /// `__agg_state`, and `merge_from`s into a consolidated per-group map before
    /// running the same finalize tail as a normal aggregate.
    ///
    /// Like [`QueryOp::ShuffleJoinConsume`], this variant carries a node-local
    /// path and is BUILT LOCALLY by the consume hook for dispatch to the SAME
    /// node's Data Plane — it must NEVER be serialized cross-node (the wire
    /// encoder rejects it; see `physical_plan::wire::encode`).
    ShuffleAggregateConsume {
        /// Local absolute path to the staged partial-state frame file.
        state_path: String,
        /// GROUP BY columns (the key the partial states were grouped on).
        group_by: Vec<String>,
        /// Aggregate specs (must match the producers' specs, in order).
        aggregates: Vec<AggregateSpec>,
        /// HAVING predicates applied post-merge (serialized `Vec<ScanFilter>`).
        having: Vec<u8>,
        /// Output row cap after sort. `usize::MAX` = no explicit LIMIT.
        limit: usize,
        /// Post-aggregation sort keys: `(column_name, ascending)`.
        sort_keys: Vec<(String, bool)>,
    },

    /// Nested loop join: fallback for non-equi joins.
    NestedLoopJoin {
        left_collection: String,
        right_collection: String,
        /// Join condition as serialized `Vec<ScanFilter>`.
        condition: Vec<u8>,
        join_type: String,
        limit: usize,
    },

    /// Sort-merge join: both sides pre-sorted by join key.
    /// Optimal when both collections have index-ordered scans or
    /// when the planner sorts both sides before joining.
    SortMergeJoin {
        left_collection: String,
        right_collection: String,
        on: Vec<(String, String)>,
        join_type: String,
        limit: usize,
        /// If true, both sides are assumed pre-sorted by join key (skip sort phase).
        pre_sorted: bool,
    },

    /// Multi-facet aggregation: compute facet counts for multiple fields
    /// in a single query, sharing the filter evaluation across all facets.
    FacetCounts {
        collection: String,
        /// Serialized `Vec<ScanFilter>` predicates (MessagePack).
        filters: Vec<u8>,
        /// Field names to facet on (each produces a `[{value, count}]` array).
        fields: Vec<String>,
        /// Maximum number of values to return per facet field (0 = unlimited).
        limit_per_facet: usize,
    },

    /// Recursive CTE: iterative fixed-point execution.
    ///
    /// Executes the base query once, then repeatedly executes the recursive
    /// query using the previous iteration's results as the working table,
    /// until no new rows are produced (fixed point).
    RecursiveScan {
        /// Collection for the recursive scan.
        collection: String,
        /// Base query filters (seeded once).
        base_filters: Vec<u8>,
        /// Recursive step filters (applied to working table each iteration).
        recursive_filters: Vec<u8>,
        /// Equi-join link for tree-traversal recursion:
        /// `(collection_field, working_table_field)`.
        /// Each iteration finds rows where `collection_field` value
        /// matches a `working_table_field` value from the previous iteration.
        join_link: Option<(String, String)>,
        /// Maximum iterations to prevent infinite loops. Default: 100.
        max_iterations: usize,
        /// Whether to deduplicate results (UNION vs UNION ALL).
        distinct: bool,
        limit: usize,
    },

    /// Value-generating recursive CTE: iterative expression evaluation.
    ///
    /// No collection is needed.  The executor evaluates the anchor expressions
    /// once to produce the first row, then repeatedly applies the step
    /// expressions to the previous row until the condition becomes false,
    /// a fixed point is reached, or `max_depth` is exceeded (typed error).
    RecursiveValue {
        /// CTE name (used in error messages).
        cte_name: String,
        /// Column names (length == `init_exprs.len()` == `step_exprs.len()`).
        columns: Vec<String>,
        /// Anchor SELECT expressions as raw SQL text.
        init_exprs: Vec<String>,
        /// Recursive step SELECT expressions as raw SQL text.
        step_exprs: Vec<String>,
        /// Optional WHERE condition as raw SQL text.
        condition: Option<String>,
        /// Maximum iterations before returning a depth-exceeded error.
        max_depth: usize,
        /// Whether to deduplicate (UNION vs UNION ALL).
        distinct: bool,
    },

    /// LATERAL equi-correlated top-K: scan `inner_collection` once per outer
    /// row, applying the equi-correlation as an equality filter, then return
    /// the top `inner_limit` rows ordered by `inner_order_by`.
    ///
    /// The executor first runs `outer_plan` to materialise outer rows, then
    /// for each outer row injects the equi-correlation as an equality filter
    /// on `inner_collection`, applies `inner_order_by`, and keeps the top
    /// `inner_limit` rows.  Output rows are `(outer_row merged with inner_row)`.
    ///
    /// `correlation_keys` are `(outer_col, inner_col)` equi-join pairs.
    LateralTopK {
        /// Sub-plan that produces the outer (driving) rows.
        outer_plan: Box<crate::physical_plan::PhysicalPlan>,
        /// Alias qualifying the outer columns in output rows.
        outer_alias: String,
        /// Inner collection to scan per outer row.
        inner_collection: String,
        /// Non-correlated filters applied to every inner scan (msgpack bytes).
        inner_filters: Vec<u8>,
        /// Sort keys for the inner per-outer-row result.
        /// Each entry is `(field_name, ascending)`.
        inner_order_by: Vec<(String, bool)>,
        /// Maximum inner rows per outer row.
        inner_limit: usize,
        /// Equi-join pairs `(outer_col, inner_col)`.
        correlation_keys: Vec<(String, String)>,
        /// Alias qualifying inner columns in output rows.
        lateral_alias: String,
        /// Output projection (empty = all columns).
        projection: Vec<JoinProjection>,
        /// LEFT join semantics: preserve outer rows when inner is empty.
        left_join: bool,
    },

    /// LATERAL nested-loop: for each outer row, re-execute the inner plan
    /// with correlated values injected as additional equality filters.
    ///
    /// The executor runs `outer_plan`, then for each outer row reads the
    /// `outer_col` values from `correlation_predicates`, appends equality
    /// filters on the corresponding `inner_col` fields, and scans
    /// `inner_collection`.
    LateralLoop {
        /// Sub-plan that produces the outer (driving) rows.
        outer_plan: Box<crate::physical_plan::PhysicalPlan>,
        /// Alias qualifying the outer columns in output rows.
        outer_alias: String,
        /// Inner collection to scan per outer row.
        inner_collection: String,
        /// Base inner filters (non-correlated, msgpack bytes).
        inner_filters: Vec<u8>,
        /// Correlated predicates: `(inner_field, outer_field)`.
        correlation_predicates: Vec<(String, String)>,
        /// Alias qualifying inner columns in output rows.
        lateral_alias: String,
        /// Output projection (empty = all columns).
        projection: Vec<JoinProjection>,
        /// LEFT join semantics.
        left_join: bool,
        /// Hard cap on outer rows. Queries that exceed this cap return a
        /// typed `LateralCapExceeded` error instead of silently truncating.
        outer_row_cap: usize,
    },
}
