//! Procedural SQL abstract syntax tree.
//!
//! Shared across functions, triggers, and procedures.
//! The parser produces these types; the compiler consumes them.

/// A complete procedural block: `BEGIN ... END`.
///
/// Optionally contains exception handlers that catch errors raised during
/// statement execution (similar to PL/pgSQL's `EXCEPTION WHEN ... THEN ...`).
#[derive(Debug, Clone, PartialEq)]
pub struct ProceduralBlock {
    pub statements: Vec<Statement>,
    /// Exception handlers, evaluated in order. If a statement raises an error,
    /// the first matching handler executes. If no handler matches, the error
    /// propagates. Empty if no EXCEPTION clause.
    pub exception_handlers: Vec<ExceptionHandler>,
}

/// An exception handler: catches errors matching a condition and executes a body.
#[derive(Debug, Clone, PartialEq)]
pub struct ExceptionHandler {
    /// The condition to match.
    pub condition: ExceptionCondition,
    /// Statements to execute when this handler matches.
    pub body: Vec<Statement>,
}

/// Exception condition for matching errors.
#[derive(Debug, Clone, PartialEq)]
pub enum ExceptionCondition {
    /// `WHEN OTHERS THEN` — catches any error.
    Others,
    /// `WHEN SQLSTATE '<code>' THEN` — matches a specific SQLSTATE code.
    SqlState(String),
    /// `WHEN <name> THEN` — matches a named condition (e.g., `UNIQUE_VIOLATION`).
    Named(String),
}

/// A single statement in a procedural block.
#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    /// `DECLARE name TYPE [:= default_expr];`
    Declare {
        name: String,
        data_type: String,
        default: Option<SqlExpr>,
    },

    /// `name := expr;`
    Assign { target: String, expr: SqlExpr },

    /// `IF cond THEN ... [ELSIF cond THEN ...] [ELSE ...] END IF;`
    If {
        condition: SqlExpr,
        then_block: Vec<Statement>,
        elsif_branches: Vec<ElsIfBranch>,
        else_block: Option<Vec<Statement>>,
    },

    /// `LOOP ... END LOOP;` (infinite — must contain BREAK or bounded by analysis)
    Loop { body: Vec<Statement> },

    /// `WHILE cond LOOP ... END LOOP;`
    While {
        condition: SqlExpr,
        body: Vec<Statement>,
    },

    /// `FOR var IN start..end LOOP ... END LOOP;`
    For {
        var: String,
        start: SqlExpr,
        end: SqlExpr,
        /// True for `REVERSE start..end`.
        reverse: bool,
        body: Vec<Statement>,
    },

    /// `BREAK;` — exit innermost LOOP/WHILE/FOR.
    Break,

    /// `CONTINUE;` — skip to next iteration of innermost LOOP/WHILE/FOR.
    Continue,

    /// `RETURN expr;` — return a scalar value.
    Return { expr: SqlExpr },

    /// `RETURN QUERY sql;` — return result set from a query.
    ReturnQuery { query: String },

    /// `RAISE EXCEPTION 'message';` — abort with error.
    Raise { level: RaiseLevel, message: SqlExpr },

    /// Raw DML statement (INSERT/UPDATE/DELETE). Rejected in function bodies
    /// at parse time. Used by triggers and procedures.
    Dml { sql: String },

    /// `COMMIT;` — commit current transaction.
    Commit,

    /// `ROLLBACK;` — rollback current transaction.
    Rollback,

    /// `SAVEPOINT name;` — record a savepoint.
    Savepoint { name: String },

    /// `ROLLBACK TO [SAVEPOINT] name;` — rollback to a savepoint.
    RollbackTo { name: String },

    /// `RELEASE [SAVEPOINT] name;` — release a savepoint.
    ReleaseSavepoint { name: String },
}

/// An ELSIF branch: condition + body.
#[derive(Debug, Clone, PartialEq)]
pub struct ElsIfBranch {
    pub condition: SqlExpr,
    pub body: Vec<Statement>,
}

/// A SQL expression embedded in procedural context.
///
/// Stored as raw SQL text — DataFusion parses it during compilation.
/// This avoids duplicating SQL expression parsing logic.
#[derive(Debug, Clone, PartialEq)]
pub struct SqlExpr {
    pub sql: String,
}

impl SqlExpr {
    pub fn new(sql: impl Into<String>) -> Self {
        Self { sql: sql.into() }
    }
}

/// Raise level for RAISE statements.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RaiseLevel {
    /// `RAISE NOTICE` — informational, continues execution.
    Notice,
    /// `RAISE WARNING` — warning, continues execution.
    Warning,
    /// `RAISE EXCEPTION` — aborts the current statement/transaction.
    Exception,
}

/// Classification of a function body: expression or procedural.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BodyKind {
    /// Single SQL expression: `SELECT LOWER(TRIM(email))`
    Expression,
    /// Procedural block: `BEGIN ... END`
    Procedural,
}

impl BodyKind {
    /// Detect body kind from the raw SQL text.
    pub fn detect(body_sql: &str) -> Self {
        let trimmed = body_sql.trim().to_uppercase();
        if trimmed.starts_with("BEGIN") {
            Self::Procedural
        } else {
            Self::Expression
        }
    }
}
