// SPDX-License-Identifier: Apache-2.0

//! Error types for the nodedb-sql crate.

/// Errors produced during SQL parsing, resolution, or planning.
#[derive(Debug, PartialEq, thiserror::Error)]
pub enum SqlError {
    #[error("parse error: {detail}")]
    Parse { detail: String },

    #[error("table not found: {name}")]
    UnknownTable { name: String },

    /// A function call in an expression names no scalar, aggregate, or
    /// window function registered in the [`FunctionRegistry`](crate::functions::registry::FunctionRegistry).
    /// Caught at plan time in the resolver so the whole statement is
    /// rejected instead of the call silently evaluating to `NULL` for every
    /// row at runtime.
    #[error("function {name}(...) does not exist")]
    UndefinedFunction { name: String },

    #[error("unknown column '{column}' in table '{table}'")]
    UnknownColumn { table: String, column: String },

    /// A write supplied NULL (explicitly or by omission) for a declared
    /// PRIMARY KEY column. PRIMARY KEY implies NOT NULL on every engine;
    /// PostgreSQL rejects the same write with SQLSTATE `23502`.
    #[error("null value in column '{column}' violates not-null constraint in table '{table}'")]
    NotNullViolation { table: String, column: String },

    #[error("ambiguous column '{column}' — qualify with table name")]
    AmbiguousColumn { column: String },

    #[error("type mismatch: {detail}")]
    TypeMismatch { detail: String },

    /// A constant expression divided by zero at plan time. Distinct from the
    /// folder declining to fold: the expression *was* constant and evaluating
    /// it failed, so the statement must raise rather than yield NULL.
    #[error("division by zero")]
    DivisionByZero,

    /// A LIMIT, OFFSET, or FETCH FIRST clause resolved to a value outside
    /// `[0, usize::MAX]`, or to an expression the planner cannot read as a
    /// literal. PostgreSQL rejects the same input with SQLSTATE `2201W`.
    #[error("invalid {clause} value: {value}")]
    InvalidLimitValue { clause: &'static str, value: String },

    /// A constant expression overflowed at plan time. Same distinction as
    /// [`SqlError::DivisionByZero`].
    #[error("arithmetic overflow evaluating a constant expression: {detail}")]
    ConstantOverflow { detail: String },

    /// A write supplied an integer wider than the column's declared type.
    ///
    /// nodedb stores every integer as an `i64`, so this is not a storage
    /// limit — it is the constraint that makes the column's advertised wire
    /// type honest. A column declared `INTEGER` reports OID 23, and a pgwire
    /// client reading it in binary format decodes exactly four bytes; letting
    /// a wider value in would mean either truncating it on read or lying about
    /// the type. PostgreSQL rejects the same write with the same message.
    #[error("value {value} is out of range for column '{column}' of type {declared_type}")]
    IntegerOutOfRange {
        column: String,
        value: i64,
        declared_type: &'static str,
    },

    /// A write supplied a finite float that overflows to infinity when
    /// narrowed to the column's declared `REAL` width.
    ///
    /// Narrowing an `f64` to `f32` normally rounds — `1.1` becoming
    /// `1.10000002` is correct PostgreSQL `real` behaviour, never an error —
    /// so this is not the float mirror of [`SqlError::IntegerOutOfRange`]'s
    /// full-range check. The single case rejected is a finite value beyond
    /// `f32`'s range, which would otherwise silently become `Infinity`.
    /// PostgreSQL rejects the same write with the same message.
    #[error("value {value} is out of range for column '{column}' of type {declared_type}")]
    FloatOutOfRange {
        column: String,
        value: f64,
        declared_type: &'static str,
    },

    #[error("unsupported: {detail}")]
    Unsupported { detail: String },

    #[error("invalid function call: {detail}")]
    InvalidFunction { detail: String },

    #[error("invalid window frame: {detail}")]
    InvalidWindowFrame { detail: String },

    #[error("missing required field '{field}' for {context}")]
    MissingField { field: String, context: String },

    /// A positional `INSERT`/`UPSERT` `VALUES` row (no explicit column
    /// list) supplied more values than the target collection has declared
    /// columns. Binding the overflow value(s) to a synthetic `col{N}` name
    /// would silently store them under an unaddressable column, so this is
    /// rejected rather than guessed at.
    #[error(
        "INSERT/UPSERT into '{collection}': row has {given} value(s) but only \
         {declared} column(s) are declared; supply an explicit column list \
         or remove the extra value(s)"
    )]
    InsertColumnArityMismatch {
        collection: String,
        given: usize,
        declared: usize,
    },

    /// A positional `INSERT`/`UPSERT` into a Key-Value collection supplied
    /// no explicit column list. The KV path splits key/value by matching
    /// column *names* against `pk_col`/`"key"`/`"ttl"`, so without a column
    /// list there is no principled key/value split — binding positionally
    /// would silently write an empty-keyed, empty-valued row (all such rows
    /// collide). Rejected rather than guessed at, mirroring
    /// [`InsertColumnArityMismatch`].
    #[error(
        "INSERT/UPSERT into KV collection '{collection}': supply an explicit \
         column list (KV needs named key/value columns; a positional VALUES \
         row has no key to bind to)"
    )]
    PositionalKvInsertUnsupported { collection: String },

    /// A descriptor the planner depends on is being drained by
    /// an in-flight DDL. Callers (pgwire handlers) should retry
    /// the whole statement after a short backoff. Propagated
    /// from `SqlCatalogError::RetryableSchemaChanged`.
    #[error("retryable schema change on {descriptor}")]
    RetryableSchemaChanged { descriptor: String },

    /// Identifier violates NodeDB's canonical identifier rules.
    #[error("invalid identifier '{name}': {reason}")]
    InvalidIdentifier { name: String, reason: &'static str },

    /// Identifier is a NodeDB reserved keyword. Use a quoted identifier to bypass.
    #[error(
        "identifier '{name}' is reserved by NodeDB ({reason}); \
         use a quoted identifier (e.g., \"{name}\") to bypass"
    )]
    ReservedIdentifier { name: String, reason: &'static str },

    /// An unsupported SQL constraint was used in a DDL statement.
    ///
    /// Rendered as SQLSTATE `0A000` (feature_not_supported). The `feature` field
    /// names the constraint keyword and `hint` points to the NodeDB equivalent.
    #[error("unsupported constraint: {feature}; {hint}")]
    UnsupportedConstraint { feature: String, hint: String },

    /// Both a `WITH (engine='...')` clause and a trailing `ENGINE = ...`
    /// suffix were given on the same `CREATE COLLECTION` / `CREATE TABLE`
    /// statement, naming different engines.
    ///
    /// Rendered as SQLSTATE `0A000` (feature_not_supported), matching the
    /// existing duplicate-axis rejection precedent for `WITH (profile=...)`.
    #[error(
        "conflicting engine selection: WITH (engine='{with_engine}') and \
         ENGINE = {suffix_engine} name different engines; only one engine-selection \
         clause is allowed"
    )]
    ConflictingEngineClause {
        with_engine: String,
        suffix_engine: String,
    },

    /// WITH RECURSIVE used a set operator other than UNION or UNION ALL.
    ///
    /// Only `UNION` and `UNION ALL` are permitted in the recursive term of a
    /// `WITH RECURSIVE` CTE. `INTERSECT` and `EXCEPT` are rejected because
    /// they cannot guarantee termination in standard iterative evaluation.
    #[error(
        "WITH RECURSIVE: only UNION / UNION ALL are allowed in the recursive term; \
         {op} is not permitted"
    )]
    InvalidRecursiveSetOp { op: String },

    /// The recursive self-reference is absent, appears more than once, or
    /// appears inside a subquery, aggregate, or the nullable side of an outer join.
    #[error("WITH RECURSIVE: invalid self-reference to '{cte_name}' in recursive term: {reason}")]
    InvalidRecursiveSelfRef { cte_name: String, reason: String },

    /// The anchor SELECT produces a different number of columns than the
    /// column list declared on the CTE (or the recursive arm).
    #[error(
        "WITH RECURSIVE CTE '{cte_name}': anchor produces {anchor_cols} column(s) \
         but {declared_cols} were declared"
    )]
    RecursiveColumnMismatch {
        cte_name: String,
        anchor_cols: usize,
        declared_cols: usize,
    },

    /// Two of the CTE's columns carry the same name, either from a repeated
    /// entry in the declared column list or from two anchor items that infer
    /// the same output name. Rows are keyed by column name, so a duplicate
    /// drops one column's values instead of returning both.
    #[error(
        "WITH RECURSIVE CTE '{cte_name}': column '{column}' is declared or inferred \
         more than once; give each column a distinct name"
    )]
    DuplicateRecursiveColumn { cte_name: String, column: String },

    /// The recursive CTE exceeded the configured `max_recursion_depth`.
    ///
    /// This is a runtime error produced by the executor, not the planner.
    #[error(
        "WITH RECURSIVE CTE '{cte_name}' exceeded max recursion depth {max_depth}; \
         add a stricter termination condition or raise max_recursion_depth"
    )]
    RecursionDepthExceeded { cte_name: String, max_depth: usize },

    /// Collection is soft-deleted (within retention window).
    /// Propagated from `SqlCatalogError::CollectionDeactivated`;
    /// the pgwire layer renders this as sqlstate 42P01 with an
    /// `UNDROP COLLECTION <name>` hint in the message.
    #[error(
        "collection '{name}' was dropped; \
         restore with `{undrop_hint}` before retention elapses \
         at {retention_expires_at_ns} ns"
    )]
    CollectionDeactivated {
        name: String,
        retention_expires_at_ns: u64,
        undrop_hint: String,
    },
}

impl From<crate::catalog::SqlCatalogError> for SqlError {
    fn from(e: crate::catalog::SqlCatalogError) -> Self {
        match e {
            crate::catalog::SqlCatalogError::RetryableSchemaChanged { descriptor } => {
                Self::RetryableSchemaChanged { descriptor }
            }
            crate::catalog::SqlCatalogError::CollectionDeactivated {
                name,
                retention_expires_at_ns,
            } => {
                let undrop_hint = format!("UNDROP COLLECTION {name}");
                Self::CollectionDeactivated {
                    name,
                    retention_expires_at_ns,
                    undrop_hint,
                }
            }
        }
    }
}

impl From<nodedb_query::expr_parse::ExprParseError> for SqlError {
    fn from(e: nodedb_query::expr_parse::ExprParseError) -> Self {
        Self::Parse {
            detail: e.to_string(),
        }
    }
}

impl From<sqlparser::parser::ParserError> for SqlError {
    fn from(e: sqlparser::parser::ParserError) -> Self {
        Self::Parse {
            detail: e.to_string(),
        }
    }
}

pub type Result<T> = std::result::Result<T, SqlError>;
