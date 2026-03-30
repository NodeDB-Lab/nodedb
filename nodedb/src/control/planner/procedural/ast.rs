//! Procedural SQL abstract syntax tree.
//!
//! Shared across Tier 2 (functions), Tier 3 (triggers), and Tier 4 (procedures).
//! The parser produces these types; the compiler consumes them.

/// A complete procedural block: `BEGIN ... END`.
#[derive(Debug, Clone, PartialEq)]
pub struct ProceduralBlock {
    pub statements: Vec<Statement>,
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

    /// `RETURN QUERY sql;` — return result set from a query (Tier 2 table-valued functions).
    ReturnQuery { query: String },

    /// `RAISE EXCEPTION 'message';` — abort with error.
    Raise { level: RaiseLevel, message: SqlExpr },

    // ── Tier 3/4 only (rejected in function bodies) ──────────────────
    /// Raw DML statement (INSERT/UPDATE/DELETE). Rejected in function bodies
    /// at parse time. Used by triggers (Tier 3) and procedures (Tier 4).
    Dml { sql: String },

    /// `COMMIT;` — commit current transaction (Tier 4 only).
    Commit,

    /// `ROLLBACK;` — rollback current transaction (Tier 4 only).
    Rollback,
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
