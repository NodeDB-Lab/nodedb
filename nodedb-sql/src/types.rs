//! SqlPlan intermediate representation types.
//!
//! These types represent the output of the nodedb-sql planner. Both Origin
//! (server) and Lite (embedded) map these to their own execution model.

/// The top-level plan produced by the SQL planner.
#[derive(Debug, Clone)]
pub enum SqlPlan {
    // ── Constant ──
    /// Query with no FROM clause: SELECT 1, SELECT 'hello' AS name, etc.
    /// Produces a single row with evaluated constant expressions.
    ConstantResult {
        columns: Vec<String>,
        values: Vec<SqlValue>,
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
    },
    PointGet {
        collection: String,
        alias: Option<String>,
        engine: EngineType,
        key_column: String,
        key_value: SqlValue,
    },
    RangeScan {
        collection: String,
        field: String,
        lower: Option<SqlValue>,
        upper: Option<SqlValue>,
        limit: usize,
    },

    // ── Writes ──
    Insert {
        collection: String,
        engine: EngineType,
        rows: Vec<Vec<(String, SqlValue)>>,
        /// Column defaults from schema: `(column_name, default_expr)`.
        /// Used to auto-generate values for missing columns (e.g. `id` with `UUID_V7`).
        column_defaults: Vec<(String, String)>,
    },
    /// KV INSERT: key and value are fundamentally separate.
    /// Each entry is `(key, value_columns)`.
    KvInsert {
        collection: String,
        entries: Vec<(SqlValue, Vec<(String, SqlValue)>)>,
        /// TTL in seconds (0 = no expiry). Extracted from `ttl` column if present.
        ttl_secs: u64,
    },
    /// UPSERT: insert or merge if document exists.
    Upsert {
        collection: String,
        engine: EngineType,
        rows: Vec<Vec<(String, SqlValue)>>,
        column_defaults: Vec<(String, String)>,
        /// `ON CONFLICT (...) DO UPDATE SET field = expr` assignments.
        /// When empty, upsert is a plain merge: new columns overwrite existing.
        /// When non-empty, the engine applies these per-row against the
        /// *existing* document instead of merging the inserted values.
        on_conflict_updates: Vec<(String, SqlExpr)>,
    },
    InsertSelect {
        target: String,
        source: Box<SqlPlan>,
        limit: usize,
    },
    Update {
        collection: String,
        engine: EngineType,
        assignments: Vec<(String, SqlExpr)>,
        filters: Vec<Filter>,
        target_keys: Vec<SqlValue>,
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
        limit: usize,
        /// Post-join projection: column names to keep (empty = all columns).
        projection: Vec<Projection>,
        /// Post-join filters (from WHERE clause).
        filters: Vec<Filter>,
    },

    // ── Aggregation ──
    Aggregate {
        input: Box<SqlPlan>,
        group_by: Vec<SqlExpr>,
        aggregates: Vec<AggregateExpr>,
        having: Vec<Filter>,
        limit: usize,
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
        tiered: bool,
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
        filters: Vec<Filter>,
    },
    MultiVectorSearch {
        collection: String,
        query_vector: Vec<f32>,
        top_k: usize,
        ef_search: usize,
    },
    TextSearch {
        collection: String,
        query: String,
        top_k: usize,
        fuzzy: bool,
        filters: Vec<Filter>,
    },
    HybridSearch {
        collection: String,
        query_vector: Vec<f32>,
        query_text: String,
        top_k: usize,
        ef_search: usize,
        vector_weight: f32,
        fuzzy: bool,
    },
    SpatialScan {
        collection: String,
        field: String,
        predicate: SpatialPredicate,
        query_geometry: Vec<u8>,
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
        max_iterations: usize,
        distinct: bool,
        limit: usize,
    },

    /// Non-recursive CTE: execute each definition, then the outer query.
    Cte {
        /// CTE definitions: `(name, subquery_plan)`.
        definitions: Vec<(String, SqlPlan)>,
        /// The outer query that references CTE names.
        outer: Box<SqlPlan>,
    },
}

/// Database engine type for a collection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EngineType {
    DocumentSchemaless,
    DocumentStrict,
    KeyValue,
    Columnar,
    Timeseries,
    Spatial,
}

/// SQL join type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Semi,
    Anti,
    Cross,
}

impl JoinType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Inner => "inner",
            Self::Left => "left",
            Self::Right => "right",
            Self::Full => "full",
            Self::Semi => "semi",
            Self::Anti => "anti",
            Self::Cross => "cross",
        }
    }
}

/// Spatial predicate types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpatialPredicate {
    DWithin,
    Contains,
    Intersects,
    Within,
}

/// A filter predicate.
#[derive(Debug, Clone)]
pub struct Filter {
    pub expr: FilterExpr,
}

/// Filter expression tree.
#[derive(Debug, Clone)]
pub enum FilterExpr {
    Comparison {
        field: String,
        op: CompareOp,
        value: SqlValue,
    },
    Like {
        field: String,
        pattern: String,
    },
    InList {
        field: String,
        values: Vec<SqlValue>,
    },
    Between {
        field: String,
        low: SqlValue,
        high: SqlValue,
    },
    IsNull {
        field: String,
    },
    IsNotNull {
        field: String,
    },
    And(Vec<Filter>),
    Or(Vec<Filter>),
    Not(Box<Filter>),
    /// Raw expression filter (for complex predicates that don't fit simple patterns).
    Expr(SqlExpr),
}

/// Comparison operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompareOp {
    Eq,
    Ne,
    Gt,
    Ge,
    Lt,
    Le,
}

/// Projection item in SELECT.
#[derive(Debug, Clone)]
pub enum Projection {
    /// Simple column reference: `SELECT name`
    Column(String),
    /// All columns: `SELECT *`
    Star,
    /// Qualified star: `SELECT t.*`
    QualifiedStar(String),
    /// Computed expression: `SELECT price * qty AS total`
    Computed { expr: SqlExpr, alias: String },
}

/// Sort key for ORDER BY.
#[derive(Debug, Clone)]
pub struct SortKey {
    pub expr: SqlExpr,
    pub ascending: bool,
    pub nulls_first: bool,
}

/// Aggregate expression: `COUNT(*)`, `SUM(amount)`, etc.
#[derive(Debug, Clone)]
pub struct AggregateExpr {
    pub function: String,
    pub args: Vec<SqlExpr>,
    pub alias: String,
    pub distinct: bool,
}

/// Window function specification.
#[derive(Debug, Clone)]
pub struct WindowSpec {
    pub function: String,
    pub args: Vec<SqlExpr>,
    pub partition_by: Vec<SqlExpr>,
    pub order_by: Vec<SortKey>,
    pub alias: String,
}

/// SQL value literal.
#[derive(Debug, Clone, PartialEq)]
pub enum SqlValue {
    Int(i64),
    Float(f64),
    String(String),
    Bool(bool),
    Null,
    Bytes(Vec<u8>),
    Array(Vec<SqlValue>),
}

/// SQL expression tree.
#[derive(Debug, Clone)]
pub enum SqlExpr {
    /// Column reference, optionally qualified: `name` or `users.name`
    Column { table: Option<String>, name: String },
    /// Literal value.
    Literal(SqlValue),
    /// Binary operation: `a + b`, `x > 5`
    BinaryOp {
        left: Box<SqlExpr>,
        op: BinaryOp,
        right: Box<SqlExpr>,
    },
    /// Unary operation: `-x`, `NOT flag`
    UnaryOp { op: UnaryOp, expr: Box<SqlExpr> },
    /// Function call: `COUNT(*)`, `vector_distance(field, ARRAY[...])`
    Function {
        name: String,
        args: Vec<SqlExpr>,
        distinct: bool,
    },
    /// CASE WHEN ... THEN ... ELSE ... END
    Case {
        operand: Option<Box<SqlExpr>>,
        when_then: Vec<(SqlExpr, SqlExpr)>,
        else_expr: Option<Box<SqlExpr>>,
    },
    /// CAST(expr AS type)
    Cast { expr: Box<SqlExpr>, to_type: String },
    /// Subquery expression (IN, EXISTS, scalar)
    Subquery(Box<SqlPlan>),
    /// Wildcard `*`
    Wildcard,
    /// `IS NULL` / `IS NOT NULL`
    IsNull { expr: Box<SqlExpr>, negated: bool },
    /// `expr IN (values)`
    InList {
        expr: Box<SqlExpr>,
        list: Vec<SqlExpr>,
        negated: bool,
    },
    /// `expr BETWEEN low AND high`
    Between {
        expr: Box<SqlExpr>,
        low: Box<SqlExpr>,
        high: Box<SqlExpr>,
        negated: bool,
    },
    /// `expr LIKE pattern`
    Like {
        expr: Box<SqlExpr>,
        pattern: Box<SqlExpr>,
        negated: bool,
    },
    /// Array literal: `ARRAY[1.0, 2.0, 3.0]`
    ArrayLiteral(Vec<SqlExpr>),
}

/// Binary operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOp {
    // Arithmetic
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    // Comparison
    Eq,
    Ne,
    Gt,
    Ge,
    Lt,
    Le,
    // Logical
    And,
    Or,
    // String
    Concat,
}

/// Unary operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    Neg,
    Not,
}

/// SQL data type for schema resolution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlDataType {
    Int64,
    Float64,
    String,
    Bool,
    Bytes,
    Timestamp,
    Decimal,
    Uuid,
    Vector(usize),
    Geometry,
}

// ── Catalog trait ──
// The `SqlCatalog` trait itself and its error type live in
// `crate::catalog` to keep this file under the 500-line limit.
// Re-exported here so downstream modules that `use crate::types::*`
// keep resolving `SqlCatalog` without changing their imports.
pub use crate::catalog::{SqlCatalog, SqlCatalogError};

/// Metadata about a collection for query planning.
#[derive(Debug, Clone)]
pub struct CollectionInfo {
    pub name: String,
    pub engine: EngineType,
    pub columns: Vec<ColumnInfo>,
    pub primary_key: Option<String>,
    pub has_auto_tier: bool,
}

/// Metadata about a single column.
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: SqlDataType,
    pub nullable: bool,
    pub is_primary_key: bool,
    /// Default value expression (e.g. "UUID_V7", "ULID", "NANOID(10)", "0", "'active'").
    pub default: Option<String>,
}
