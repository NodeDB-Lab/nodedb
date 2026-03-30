//! Type definitions for user-defined function catalog storage.

/// Parameter definition for a user-defined function.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FunctionParam {
    pub name: String,
    /// SQL type name: "TEXT", "INT", "FLOAT", "BOOLEAN", "BIGINT", "DOUBLE", etc.
    pub data_type: String,
}

/// Volatility classification for user-defined functions.
///
/// Maps directly to DataFusion's `Volatility` enum.
/// - `Immutable`: result depends only on inputs — the optimizer can constant-fold
///   calls where all arguments are literals.
/// - `Stable`: result may change across statements but not within one query
///   (e.g., depends on session state like `current_user()`).
/// - `Volatile`: result can change between any two calls — never cached
///   (e.g., depends on `random()` or `now()`).
///
/// The classification is set at `CREATE FUNCTION` time (auto-inferred as `Immutable`
/// by default, or explicitly overridden). It is stored in the catalog and used both
/// by the `ScalarUDF` registration (DataFusion respects it for constant folding) and
/// by the inlining `AnalyzerRule` (see `inline_rewrite.rs` for details).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum FunctionVolatility {
    #[default]
    Immutable,
    Stable,
    Volatile,
}

impl FunctionVolatility {
    /// Parse from SQL keyword (case-insensitive).
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "IMMUTABLE" => Some(Self::Immutable),
            "STABLE" => Some(Self::Stable),
            "VOLATILE" => Some(Self::Volatile),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Immutable => "IMMUTABLE",
            Self::Stable => "STABLE",
            Self::Volatile => "VOLATILE",
        }
    }
}

/// Security mode for user-defined functions.
///
/// - `Invoker` (default): body executes with the **caller's** credentials.
///   Subqueries are subject to the caller's GRANT/DENY and RLS policies.
/// - `Definer`: body executes with the **function owner's** credentials.
///   Ships with Tier 3 (triggers + advanced security).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum FunctionSecurity {
    #[default]
    Invoker,
    Definer,
}

/// Serializable user-defined function record for redb storage.
///
/// Expression UDFs only (Tier 1). The body is a single SQL expression
/// that gets inlined into the DataFusion logical plan. SECURITY INVOKER
/// is enforced naturally because the inlined expression runs in the
/// caller's query context with their permissions and RLS policies.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct StoredFunction {
    pub tenant_id: u32,
    pub name: String,
    pub parameters: Vec<FunctionParam>,
    /// SQL return type name (e.g. "TEXT", "FLOAT", "BOOLEAN").
    pub return_type: String,
    /// Original SQL body (everything after AS in CREATE FUNCTION).
    /// For expression UDFs: `SELECT LOWER(TRIM(email))`.
    /// For procedural UDFs: `BEGIN IF ... END`.
    pub body_sql: String,
    /// Compiled SQL expression for procedural bodies.
    /// Produced by the procedural compiler at CREATE time.
    /// `None` for expression UDFs (body_sql is already an expression).
    /// When present, the UDF engine uses this instead of body_sql.
    #[serde(default)]
    pub compiled_body_sql: Option<String>,
    pub volatility: FunctionVolatility,
    /// Security mode: INVOKER (default) or DEFINER (Tier 3).
    #[serde(default)]
    pub security: FunctionSecurity,
    pub owner: String,
    pub created_at: u64,
}
