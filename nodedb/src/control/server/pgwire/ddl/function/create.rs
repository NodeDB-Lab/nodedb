//! `CREATE [OR REPLACE] FUNCTION` DDL handler.

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::catalog::{FunctionParam, FunctionVolatility, StoredFunction};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::types::{require_admin, sqlstate_error};
use super::parse::parse_function_header;
use super::validate::validate_function_body;

/// Handle `CREATE [OR REPLACE] FUNCTION <name>(<params>) RETURNS <type> [IMMUTABLE|STABLE|VOLATILE] AS <body>`
///
/// Requires superuser or tenant_admin — function bodies are SQL expressions
/// that can reference any collection, so creation is a privileged operation.
/// (Once SECURITY INVOKER enforcement is wired in, this can be relaxed to
/// users with a specific EXECUTE / CREATE FUNCTION grant.)
pub fn create_function(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    require_admin(identity, "create functions")?;

    let parsed = parse_create_function(sql)?;
    let tenant_id = identity.tenant_id.as_u32();

    let catalog = state
        .credentials
        .catalog()
        .as_ref()
        .ok_or_else(|| sqlstate_error("XX000", "system catalog not available"))?;

    // Check for existing function.
    if !parsed.or_replace
        && let Ok(Some(_)) = catalog.get_function(tenant_id, &parsed.name)
    {
        return Err(sqlstate_error(
            "42723",
            &format!("function '{}' already exists", parsed.name),
        ));
    }

    // Detect body kind and compile/validate accordingly.
    use crate::control::planner::procedural::ast::BodyKind;
    let compiled_body_sql = match BodyKind::detect(&parsed.body_sql) {
        BodyKind::Expression => {
            // Expression UDF: validate directly via DataFusion.
            validate_function_body(&parsed)?;
            None
        }
        BodyKind::Procedural => {
            // Procedural UDF: parse → validate → compile to SQL expression.
            let block = crate::control::planner::procedural::parse_block(&parsed.body_sql)
                .map_err(|e| sqlstate_error("42601", &format!("procedural parse error: {e}")))?;

            crate::control::planner::procedural::validate_function_block(&block)
                .map_err(|e| sqlstate_error("42601", &format!("procedural validation: {e}")))?;

            let compiled = crate::control::planner::procedural::compile_to_sql(&block)
                .map_err(|e| sqlstate_error("42601", &format!("procedural compile: {e}")))?;

            // Validate the compiled expression via DataFusion (type check, etc.).
            let compiled_parsed = ParsedCreateFunction {
                or_replace: parsed.or_replace,
                name: parsed.name.clone(),
                parameters: parsed.parameters.clone(),
                return_type: parsed.return_type.clone(),
                volatility: parsed.volatility,
                body_sql: compiled.clone(),
            };
            validate_function_body(&compiled_parsed)?;

            Some(compiled)
        }
    };

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| sqlstate_error("XX000", "system clock before UNIX epoch"))?
        .as_secs();

    let stored = StoredFunction {
        tenant_id,
        name: parsed.name.clone(),
        parameters: parsed.parameters,
        return_type: parsed.return_type,
        body_sql: parsed.body_sql,
        compiled_body_sql,
        volatility: parsed.volatility,
        security: crate::control::security::catalog::FunctionSecurity::default(),
        language: crate::control::security::catalog::function_types::FunctionLanguage::Sql,
        wasm_hash: None,
        wasm_fuel: 1_000_000,
        wasm_memory: 16 * 1024 * 1024,
        owner: identity.username.clone(),
        created_at: now,
    };

    catalog
        .put_function(&stored)
        .map_err(|e| sqlstate_error("XX000", &format!("catalog write: {e}")))?;

    // Set function ownership so the permission system can check it.
    state
        .permissions
        .set_owner(
            "function",
            identity.tenant_id,
            &stored.name,
            &identity.username,
            Some(catalog),
        )
        .map_err(|e| sqlstate_error("XX000", &format!("set ownership: {e}")))?;

    // Extract and store dependencies (referenced functions in the body).
    let deps = extract_dependencies(&stored);
    if !deps.is_empty() {
        let _ = catalog.put_dependencies("function", tenant_id, &stored.name, &deps);
    }

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("CREATE FUNCTION {}", stored.name),
    );

    Ok(vec![Response::Execution(Tag::new("CREATE FUNCTION"))])
}

// ─── Dependency extraction ───────────────────────────────────────────────────

use crate::control::security::catalog::dependencies::Dependency;

/// Extract dependency references from a function body.
///
/// Scans the body SQL for function calls that match other user-defined functions.
/// Collection references in subqueries (e.g., `SELECT x FROM users`) are not
/// extracted here — they are resolved at query time via the schema provider
/// and protected by collection-level permissions + RLS.
fn extract_dependencies(func: &StoredFunction) -> Vec<Dependency> {
    // For expression UDFs, dependencies are other function calls in the body.
    // We use a simple regex-free scan: look for `identifier(` patterns that
    // aren't SQL keywords or built-in functions.
    //
    // A more robust approach would parse the body into an AST and walk it,
    // but the validate step already did that. For now, we extract function
    // names from the body by finding word(...) patterns.
    let body = func.body_sql.to_lowercase();
    let param_names: std::collections::HashSet<&str> =
        func.parameters.iter().map(|p| p.name.as_str()).collect();

    let mut deps = Vec::new();
    let bytes = body.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        // Find start of an identifier.
        if bytes[i].is_ascii_alphabetic() || bytes[i] == b'_' {
            let start = i;
            while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                i += 1;
            }
            let word = &body[start..i];
            // Skip whitespace.
            let mut j = i;
            while j < bytes.len() && bytes[j].is_ascii_whitespace() {
                j += 1;
            }
            // If followed by '(', it's a function call.
            if j < bytes.len()
                && bytes[j] == b'('
                && !is_sql_keyword(word)
                && !param_names.contains(word)
            {
                deps.push(Dependency {
                    target_type: "function".into(),
                    target_name: word.to_string(),
                });
            }
        } else {
            i += 1;
        }
    }

    // Deduplicate.
    deps.sort_by(|a, b| a.target_name.cmp(&b.target_name));
    deps.dedup_by(|a, b| a.target_name == b.target_name && a.target_type == b.target_type);
    deps
}

/// Check if a word is a SQL keyword (not a function reference).
fn is_sql_keyword(word: &str) -> bool {
    matches!(
        word,
        "select"
            | "from"
            | "where"
            | "and"
            | "or"
            | "not"
            | "in"
            | "is"
            | "null"
            | "true"
            | "false"
            | "as"
            | "case"
            | "when"
            | "then"
            | "else"
            | "end"
            | "between"
            | "like"
            | "exists"
            | "cast"
            | "coalesce"
            | "nullif"
            | "if"
            | "limit"
            | "order"
            | "by"
            | "asc"
            | "desc"
            | "group"
            | "having"
            | "union"
            | "all"
            | "distinct"
            | "join"
            | "on"
            | "left"
            | "right"
            | "inner"
            | "outer"
            | "cross"
            | "values"
    )
}

// ─── Parsing ─────────────────────────────────────────────────────────────────

/// Parsed components of a CREATE FUNCTION statement.
pub(super) struct ParsedCreateFunction {
    pub or_replace: bool,
    pub name: String,
    pub parameters: Vec<FunctionParam>,
    pub return_type: String,
    pub volatility: FunctionVolatility,
    pub body_sql: String,
}

/// Parse a CREATE [OR REPLACE] FUNCTION statement.
///
/// Grammar:
/// ```text
/// CREATE [OR REPLACE] FUNCTION <name>(<param_name> <type> [, ...])
///   RETURNS <type>
///   [IMMUTABLE | STABLE | VOLATILE]
///   AS <sql_expression> ;
/// ```
pub(super) fn parse_create_function(sql: &str) -> PgWireResult<ParsedCreateFunction> {
    // Use shared header parser — SQL functions terminate return type at AS/volatility.
    let header = parse_function_header(sql, &[" AS ", " IMMUTABLE ", " STABLE ", " VOLATILE "])?;

    // Optional volatility keyword, then AS.
    let (volatility, body_part) = extract_volatility_and_body(&header.rest)?;

    // The body is the SQL expression after AS.
    let body_sql = body_part.trim().trim_end_matches(';').trim().to_string();
    if body_sql.is_empty() {
        return Err(sqlstate_error("42601", "function body is empty"));
    }

    Ok(ParsedCreateFunction {
        or_replace: header.or_replace,
        name: header.name,
        parameters: header.parameters,
        return_type: header.return_type,
        volatility,
        body_sql,
    })
}

/// Extract optional volatility keyword and the body after AS.
fn extract_volatility_and_body(s: &str) -> PgWireResult<(FunctionVolatility, &str)> {
    let upper = s.to_uppercase();
    let mut rest = s;
    let mut volatility = FunctionVolatility::Immutable; // default

    // Check for volatility keyword before AS.
    for kw in ["IMMUTABLE", "STABLE", "VOLATILE"] {
        if upper.starts_with(kw) {
            volatility = FunctionVolatility::parse(kw).unwrap_or_default();
            rest = s[kw.len()..].trim();
            break;
        }
    }

    // Expect AS.
    let rest_upper = rest.to_uppercase();
    if !rest_upper.starts_with("AS ") && !rest_upper.starts_with("AS\n") {
        if rest_upper.starts_with("AS") {
            return Err(sqlstate_error("42601", "expected function body after AS"));
        }
        return Err(sqlstate_error("42601", "expected AS <body>"));
    }
    let body = rest["AS".len()..].trim();

    Ok((volatility, body))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_simple_expression_function() {
        let sql =
            "CREATE FUNCTION normalize_email(email TEXT) RETURNS TEXT AS SELECT LOWER(TRIM(email))";
        let parsed = parse_create_function(sql).unwrap();
        assert_eq!(parsed.name, "normalize_email");
        assert!(!parsed.or_replace);
        assert_eq!(parsed.parameters.len(), 1);
        assert_eq!(parsed.parameters[0].name, "email");
        assert_eq!(parsed.parameters[0].data_type, "TEXT");
        assert_eq!(parsed.return_type, "TEXT");
        assert_eq!(parsed.body_sql, "SELECT LOWER(TRIM(email))");
        assert_eq!(parsed.volatility, FunctionVolatility::Immutable);
    }

    #[test]
    fn parse_or_replace() {
        let sql = "CREATE OR REPLACE FUNCTION f(x INT) RETURNS INT AS SELECT x + 1";
        let parsed = parse_create_function(sql).unwrap();
        assert!(parsed.or_replace);
        assert_eq!(parsed.name, "f");
    }

    #[test]
    fn parse_multi_param() {
        let sql = "CREATE FUNCTION add(a FLOAT, b FLOAT) RETURNS FLOAT AS SELECT a + b";
        let parsed = parse_create_function(sql).unwrap();
        assert_eq!(parsed.parameters.len(), 2);
        assert_eq!(parsed.parameters[0].name, "a");
        assert_eq!(parsed.parameters[1].name, "b");
        assert_eq!(parsed.return_type, "FLOAT");
    }

    #[test]
    fn parse_no_params() {
        let sql = "CREATE FUNCTION pi() RETURNS FLOAT AS SELECT 3.14159";
        let parsed = parse_create_function(sql).unwrap();
        assert!(parsed.parameters.is_empty());
        assert_eq!(parsed.body_sql, "SELECT 3.14159");
    }

    #[test]
    fn parse_explicit_volatility() {
        let sql = "CREATE FUNCTION f(x INT) RETURNS INT VOLATILE AS SELECT x";
        let parsed = parse_create_function(sql).unwrap();
        assert_eq!(parsed.volatility, FunctionVolatility::Volatile);
    }

    #[test]
    fn parse_stable_volatility() {
        let sql = "CREATE FUNCTION f(x INT) RETURNS INT STABLE AS SELECT x";
        let parsed = parse_create_function(sql).unwrap();
        assert_eq!(parsed.volatility, FunctionVolatility::Stable);
    }

    #[test]
    fn parse_with_semicolon() {
        let sql = "CREATE FUNCTION f(x INT) RETURNS INT AS SELECT x + 1;";
        let parsed = parse_create_function(sql).unwrap();
        assert_eq!(parsed.body_sql, "SELECT x + 1");
    }

    #[test]
    fn parse_error_no_returns() {
        let sql = "CREATE FUNCTION f(x INT) AS SELECT x";
        assert!(parse_create_function(sql).is_err());
    }

    #[test]
    fn parse_error_bad_type() {
        let sql = "CREATE FUNCTION f(x FOOBAR) RETURNS INT AS SELECT x";
        assert!(parse_create_function(sql).is_err());
    }

    #[test]
    fn parse_error_empty_body() {
        let sql = "CREATE FUNCTION f(x INT) RETURNS INT AS";
        assert!(parse_create_function(sql).is_err());
    }
}
