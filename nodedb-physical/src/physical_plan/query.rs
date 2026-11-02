// SPDX-License-Identifier: Apache-2.0

//! Query operations (joins, aggregates) dispatched to the Data Plane.

pub use nodedb_query::expr::GroupKeySpec;
use nodedb_types::QualifiedCollection;

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
    /// The constant parameter of a parameterized aggregate (see
    /// [`super::PARAMETERIZED_AGGREGATE_FUNCTIONS`]); `None` for every other
    /// function.
    #[serde(default)]
    pub param: Option<super::AggregateParam>,
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

    /// Pre-materialized rows. `provider: Some(name)` => a named relation (an
    /// identity-scoped catalog table or a continuous aggregate) whose rows
    /// the coordinator fills per request; `None` => `rows` is final
    /// (constant SELECT or a gathered child). `rows` is a canonical msgpack
    /// array of map values (the `encode_binary_rows` shape).
    ///
    /// The executor applies predicate, offset, sort, distinct, projection, and
    /// limit in that order over the rows before emitting the response. Empty
    /// `filters`/`projection` and `None` limit are no-ops (all rows emitted
    /// unchanged through those stages).
    ProviderScan {
        /// Named relation the coordinator materializes. The name stays after
        /// the coordinator fills `rows`, so the authorization of the filled
        /// plan demands Read on it. The Data Plane reads only `rows`.
        provider: Option<String>,
        /// Msgpack array of plain map rows. Filled by the coordinator from the
        /// catalog producer or from a gathered child.
        rows: Vec<u8>,
        /// What a gathered child read to produce `rows`. The authorization
        /// of the plan that embeds these rows demands each source's
        /// requirement. Empty for a named provider and for constant rows,
        /// which `provider` and the tenant-wide fallback cover.
        #[serde(default)]
        sources: Vec<crate::physical_plan::GatheredSource>,
        /// Serialized `Vec<ScanFilter>` (MessagePack). Empty = no predicate.
        #[serde(default)]
        filters: Vec<u8>,
        /// Output column names to keep. Empty = emit all columns.
        #[serde(default)]
        projection: Vec<String>,
        /// Serialized `Vec<ComputedColumn>` (MessagePack), same encoding as
        /// `DocumentOp::Scan::computed_columns`. Empty = none.
        #[serde(default)]
        computed_columns: Vec<u8>,
        /// Serialized `Vec<WindowFuncSpec>` (MessagePack), same encoding as
        /// `DocumentOp::Scan::window_functions`. Empty = none.
        #[serde(default)]
        window_functions: Vec<u8>,
        /// ORDER BY terms, each an expression. Empty = unordered.
        #[serde(default)]
        sort_keys: Vec<crate::physical_plan::SortKeySpec>,
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
        /// The system time a named relation is read at. The coordinator
        /// reads a continuous aggregate's rows at this cut, so a read inside
        /// a transaction block sees the block's snapshot. Catalog relations
        /// and filled rows ignore it.
        #[serde(default)]
        system_time: nodedb_types::SystemTimeScope,
    },

    /// Relational post-processing over a materialized child plan.
    ///
    /// Coordinator-resolved, exactly like [`QueryOp::Exchange`]: at resolve time
    /// the coordinator gathers `input`'s rows, flattens them to the relational
    /// row shape, and rewrites this node into a [`QueryOp::ProviderScan`]
    /// carrying the same relational parameters — which applies
    /// filter→offset→sort→distinct→project→limit on a single core. A Data-Plane
    /// core must NEVER see this node (defensive error if it does).
    ///
    /// Lowered from `SqlPlan::Subquery`: an outer `ORDER BY` / `OFFSET` /
    /// `DISTINCT` / post-reorder `LIMIT` over a subquery/derived-table body
    /// whose leaf plan could not absorb those constraints. Constraints the leaf
    /// DID absorb (a `WHERE` pushed into a search engine, an unordered `LIMIT`
    /// folded into `top_k`) are applied by `input` and not repeated here.
    PostProcess {
        /// The subquery body to materialize. Wrapped in `Exchange{Gather}` by
        /// the converter when the body is a sharded source, so the gather runs
        /// exactly once over the full union before post-processing.
        input: Box<crate::physical_plan::PhysicalPlan>,
        /// Serialized `Vec<ScanFilter>` (MessagePack). Empty = no predicate.
        #[serde(default)]
        filters: Vec<u8>,
        /// Output column names to keep. Empty = emit all columns.
        #[serde(default)]
        projection: Vec<String>,
        /// Serialized `Vec<ComputedColumn>` (MessagePack), same encoding as
        /// `DocumentOp::Scan::computed_columns`. Empty = none.
        #[serde(default)]
        computed_columns: Vec<u8>,
        /// Serialized `Vec<WindowFuncSpec>` (MessagePack), same encoding as
        /// `DocumentOp::Scan::window_functions`. Empty = none.
        #[serde(default)]
        window_functions: Vec<u8>,
        /// ORDER BY terms, each an expression. Empty = unordered.
        #[serde(default)]
        sort_keys: Vec<crate::physical_plan::SortKeySpec>,
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

    /// Set operation over N materialized children. Coordinator-only: the
    /// resolver materializes every child and merges the rows into one
    /// `ProviderScan` before dispatch. A Data-Plane core never sees this node.
    ///
    /// Lowered from a derived-table body that is `UNION [ALL]`,
    /// `INTERSECT [ALL]`, or `EXCEPT [ALL]`, so the body is one relation for
    /// an outer [`QueryOp::PostProcess`] or input-sourced [`QueryOp::Aggregate`].
    SetOp {
        /// Child relations in SQL order. Each sharded child is wrapped in
        /// `Exchange{Gather}` by the converter so its gather runs once.
        inputs: Vec<crate::physical_plan::PhysicalPlan>,
        /// Which set operation merges the inputs.
        op: crate::physical_plan::SetOpKind,
    },

    /// Aggregate: GROUP BY + aggregate functions.
    Aggregate {
        collection: QualifiedCollection,
        /// Optional sub-plan whose decoded rows are aggregated instead of
        /// scanning `collection` per-shard. `Some` = an input-sourced
        /// aggregate over a materialized relation: a catalog `ProviderScan`,
        /// or any derived-table body the coordinator materializes into a
        /// `ProviderScan` before dispatch. Coordinator-local, never broadcast
        /// (see `is_sharded_source`). `None` = scan the named `collection` on
        /// every shard. `collection` stays populated in both cases so
        /// downstream RLS / permission / classification continue to read it;
        /// the executor prefers `input` when present.
        #[serde(default)]
        input: Option<Box<crate::physical_plan::PhysicalPlan>>,
        group_by: Vec<GroupKeySpec>,
        aggregates: Vec<AggregateSpec>,
        filters: Vec<u8>,
        /// The system-time scope every row the aggregate folds is read at:
        /// `Current` for the newest committed version, `TxnSnapshot` for a
        /// transaction block's snapshot with its staged writes, `AsOf` /
        /// `Pinned` for committed history. Applies to the per-shard scan of
        /// `collection`. An `input` sub-plan carries its own scope.
        #[serde(default)]
        system_time: nodedb_types::SystemTimeScope,
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
        sort_keys: Vec<crate::physical_plan::SortKeySpec>,
        /// SELECT-list output alias of each GROUP BY key, parallel to
        /// `group_by`. A `Some` entry renames the key's `output_name` in every
        /// result row, after HAVING, sort, and limit. HAVING and the sort keys
        /// read the `output_name`. Empty, or a `None` entry, keeps the
        /// `output_name`.
        #[serde(default)]
        group_key_aliases: Vec<Option<String>>,
    },

    /// Partial aggregate: each core computes locally, Control Plane merges.
    PartialAggregate {
        collection: QualifiedCollection,
        group_by: Vec<String>,
        aggregates: Vec<AggregateSpec>,
        filters: Vec<u8>,
        /// The system-time scope every row the aggregate folds is read at:
        /// `Current` for the newest committed version, `TxnSnapshot` for a
        /// transaction block's snapshot with its staged writes, `AsOf` /
        /// `Pinned` for committed history. Applies to the per-shard scan of
        /// `collection`. An `input` sub-plan carries its own scope.
        #[serde(default)]
        system_time: nodedb_types::SystemTimeScope,
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
        collection: QualifiedCollection,
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
        /// The system-time scope every row the aggregate folds is read at:
        /// `Current` for the newest committed version, `TxnSnapshot` for a
        /// transaction block's snapshot with its staged writes, `AsOf` /
        /// `Pinned` for committed history. Applies to the per-shard scan of
        /// `collection`. An `input` sub-plan carries its own scope.
        #[serde(default)]
        system_time: nodedb_types::SystemTimeScope,
    },

    /// Hash join: build hash map on right, probe with left.
    ///
    /// `left_input`/`right_input` carry a resolved child plan (an Exchange
    /// child during planning, or an embedded `ProviderScan` after coordinator
    /// resolution). When `None` the side is scanned locally by collection name.
    HashJoin {
        left_collection: QualifiedCollection,
        right_collection: QualifiedCollection,
        left_alias: Option<String>,
        right_alias: Option<String>,
        on: Vec<(String, String)>,
        join_type: String,
        limit: usize,
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
        /// Row-level-security filters for rows scanned from
        /// `left_collection` locally — i.e. when `left_input` is `None`.
        ///
        /// When `left_input` is `Some`, the child plan carries its own RLS and
        /// this stays empty. The filters apply per side *before* the join, not
        /// after it: an excluded row must neither match a partner nor produce a
        /// null-extended outer row, and a post-join filter can do neither.
        left_rls_filters: Vec<u8>,
        /// Row-level-security filters for rows scanned from
        /// `right_collection` locally. Same semantics as `left_rls_filters`.
        right_rls_filters: Vec<u8>,
        /// Predicates from the left side's own `WHERE`, applied to rows
        /// scanned from `left_collection` locally. Empty when `left_input` is
        /// `Some`, because the child plan then carries its own predicates.
        /// Applied per side before the join, for the same reason as
        /// `left_rls_filters`.
        left_scan_filters: Vec<u8>,
        /// Predicates from the right side's own `WHERE`. Same semantics as
        /// `left_scan_filters`.
        right_scan_filters: Vec<u8>,
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
        /// Post-aggregation ORDER BY terms, each an expression.
        sort_keys: Vec<crate::physical_plan::SortKeySpec>,
    },

    /// Nested loop join: fallback for non-equi joins.
    NestedLoopJoin {
        left_collection: QualifiedCollection,
        right_collection: QualifiedCollection,
        /// Join condition as serialized `Vec<ScanFilter>`.
        condition: Vec<u8>,
        join_type: String,
        limit: usize,
        /// Row-level-security filters for rows scanned from `left_collection`.
        /// Applied per side before the join, for the same reason as
        /// `QueryOp::HashJoin::left_rls_filters`.
        left_rls_filters: Vec<u8>,
        /// Row-level-security filters for rows scanned from `right_collection`.
        right_rls_filters: Vec<u8>,
        /// Resolved rows for the left side, set by the coordinator inside a
        /// transaction so the side reads its staged writes. `None` means the
        /// side is scanned locally by `left_collection`. When set, the child
        /// carries the side's read policy and `left_rls_filters` is empty.
        left_input: Option<Box<crate::physical_plan::PhysicalPlan>>,
        /// Resolved rows for the right side. Same semantics as `left_input`.
        right_input: Option<Box<crate::physical_plan::PhysicalPlan>>,
        /// The system-time scope the by-name side scans read at: `Current`
        /// for the newest committed version of each row, `TxnSnapshot`
        /// inside a transaction block, `AsOf` / `Pinned` for committed
        /// history. A side whose `*_input` is set carries its own scope. The
        /// Data Plane scans a by-name side at `Current` only. The coordinator
        /// resolves every by-name side read at another scope to an input.
        #[serde(default)]
        system_time: nodedb_types::SystemTimeScope,
    },

    /// Sort-merge join: both sides pre-sorted by join key.
    /// Optimal when both collections have index-ordered scans or
    /// when the planner sorts both sides before joining.
    SortMergeJoin {
        left_collection: QualifiedCollection,
        right_collection: QualifiedCollection,
        on: Vec<(String, String)>,
        join_type: String,
        limit: usize,
        /// If true, both sides are assumed pre-sorted by join key (skip sort phase).
        pre_sorted: bool,
        /// Row-level-security filters for rows scanned from `left_collection`.
        left_rls_filters: Vec<u8>,
        /// Row-level-security filters for rows scanned from `right_collection`.
        right_rls_filters: Vec<u8>,
        /// Resolved rows for the left side. Same semantics as
        /// `QueryOp::NestedLoopJoin::left_input`.
        left_input: Option<Box<crate::physical_plan::PhysicalPlan>>,
        /// Resolved rows for the right side. Same semantics as `left_input`.
        right_input: Option<Box<crate::physical_plan::PhysicalPlan>>,
        /// See `QueryOp::NestedLoopJoin::system_time`.
        #[serde(default)]
        system_time: nodedb_types::SystemTimeScope,
    },

    /// Multi-facet aggregation: compute facet counts for multiple fields
    /// in a single query, sharing the filter evaluation across all facets.
    FacetCounts {
        collection: QualifiedCollection,
        /// Serialized `Vec<ScanFilter>` predicates (MessagePack).
        filters: Vec<u8>,
        /// Field names to facet on (each produces a `[{value, count}]` array).
        fields: Vec<String>,
        /// Maximum number of values to return per facet field (0 = unlimited).
        limit_per_facet: usize,
        /// The system-time scope the matched rows are read at: `Current` for
        /// the newest committed version of each row, `TxnSnapshot` inside a
        /// transaction block with its staged writes merged on top, `AsOf` /
        /// `Pinned` for committed history.
        #[serde(default)]
        system_time: nodedb_types::SystemTimeScope,
    },

    /// Recursive CTE: iterative fixed-point execution.
    ///
    /// Executes the base query once, then repeatedly executes the recursive
    /// query using the previous iteration's results as the working table,
    /// until no new rows are produced (fixed point).
    RecursiveScan {
        /// Collection for the recursive scan.
        collection: QualifiedCollection,
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
        /// The system-time scope `collection` is read at, once for the base
        /// and every recursive step. See `QueryOp::FacetCounts::system_time`.
        #[serde(default)]
        system_time: nodedb_types::SystemTimeScope,
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
        inner_collection: QualifiedCollection,
        /// Non-correlated filters applied to every inner scan (msgpack bytes).
        inner_filters: Vec<u8>,
        /// ORDER BY terms for the inner per-outer-row result.
        inner_order_by: Vec<crate::physical_plan::SortKeySpec>,
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
        /// The system-time scope every inner scan reads `inner_collection`
        /// at. `outer_plan` carries its own scope. See
        /// `QueryOp::FacetCounts::system_time`.
        #[serde(default)]
        inner_system_time: nodedb_types::SystemTimeScope,
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
        inner_collection: QualifiedCollection,
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
        /// See `QueryOp::LateralTopK::inner_system_time`.
        #[serde(default)]
        inner_system_time: nodedb_types::SystemTimeScope,
    },
}
