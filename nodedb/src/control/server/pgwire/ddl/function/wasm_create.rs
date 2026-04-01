//! `CREATE FUNCTION ... LANGUAGE WASM AS <base64>` DDL handler.
//!
//! Parses WASM function DDL, decodes the base64 binary, validates it,
//! stores it in the WASM module store, and registers the function.

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::planner::wasm;
use crate::control::security::catalog::{
    FunctionLanguage, FunctionParam, FunctionSecurity, FunctionVolatility, StoredFunction,
};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::types::{require_admin, sqlstate_error};
use super::parse::parse_function_header;

/// Handle `CREATE [OR REPLACE] FUNCTION ... LANGUAGE WASM AS '<base64>'`
pub fn create_wasm_function(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    require_admin(identity, "create WASM functions")?;

    let parsed = parse_wasm_create(sql)?;
    let tenant_id = identity.tenant_id.as_u32();

    let catalog = state
        .credentials
        .catalog()
        .as_ref()
        .ok_or_else(|| sqlstate_error("XX000", "system catalog not available"))?;

    if !parsed.or_replace
        && let Ok(Some(_)) = catalog.get_function(tenant_id, &parsed.name)
    {
        return Err(sqlstate_error(
            "42723",
            &format!("function '{}' already exists", parsed.name),
        ));
    }

    // Decode base64 binary.
    use base64::Engine;
    let wasm_bytes = base64::engine::general_purpose::STANDARD
        .decode(&parsed.base64_body)
        .map_err(|e| sqlstate_error("42601", &format!("invalid base64: {e}")))?;

    // Store the WASM binary (validates magic header + size limit).
    let config = wasm::WasmConfig::default();
    let hash = wasm::store::store_wasm_binary(catalog, &wasm_bytes, config.max_binary_size)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| sqlstate_error("XX000", "system clock before UNIX epoch"))?
        .as_secs();

    let stored = StoredFunction {
        tenant_id,
        name: parsed.name.clone(),
        parameters: parsed.parameters,
        return_type: parsed.return_type,
        body_sql: String::new(), // WASM functions have no SQL body
        compiled_body_sql: None,
        volatility: FunctionVolatility::Volatile, // WASM = always volatile
        security: FunctionSecurity::Invoker,
        language: FunctionLanguage::Wasm,
        wasm_hash: Some(hash),
        wasm_fuel: parsed.fuel.unwrap_or(config.default_fuel),
        wasm_memory: parsed.memory.unwrap_or(config.default_memory_bytes),
        owner: identity.username.clone(),
        created_at: now,
    };

    catalog
        .put_function(&stored)
        .map_err(|e| sqlstate_error("XX000", &format!("catalog write: {e}")))?;

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("CREATE FUNCTION {} LANGUAGE WASM", stored.name),
    );

    Ok(vec![Response::Execution(Tag::new("CREATE FUNCTION"))])
}

// ─── Parsing ────────────────────────────────────────────────────────────────

struct ParsedWasmCreate {
    or_replace: bool,
    name: String,
    parameters: Vec<FunctionParam>,
    return_type: String,
    base64_body: String,
    fuel: Option<u64>,
    memory: Option<usize>,
}

/// Parse: CREATE [OR REPLACE] FUNCTION name(params) RETURNS type LANGUAGE WASM
///        [WITH (FUEL = N, MEMORY = N)] AS '<base64>'
fn parse_wasm_create(sql: &str) -> PgWireResult<ParsedWasmCreate> {
    let header = parse_function_header(sql, &[" LANGUAGE "])?;

    // rest starts with "LANGUAGE ..."
    let after_lang_kw = header.rest.strip_prefix("LANGUAGE").unwrap_or(&header.rest);
    let after_lang = after_lang_kw.trim();
    let after_lang_upper = after_lang.to_uppercase();
    if !after_lang_upper.starts_with("WASM") {
        return Err(sqlstate_error("42601", "expected LANGUAGE WASM"));
    }
    let after_wasm = after_lang["WASM".len()..].trim();

    // Optional WITH (FUEL = N, MEMORY = N)
    let (fuel, memory, after_with) = parse_wasm_with(after_wasm)?;

    // AS '<base64>'
    let after_with_upper = after_with.to_uppercase();
    if !after_with_upper.starts_with("AS") {
        return Err(sqlstate_error("42601", "expected AS '<base64>'"));
    }
    let body_part = after_with["AS".len()..].trim();

    // Extract string literal.
    let base64_body = if body_part.starts_with('\'') && body_part.ends_with('\'') {
        body_part[1..body_part.len() - 1].replace("''", "'")
    } else {
        body_part.to_string()
    };

    if base64_body.is_empty() {
        return Err(sqlstate_error("42601", "WASM binary body is empty"));
    }

    Ok(ParsedWasmCreate {
        or_replace: header.or_replace,
        name: header.name,
        parameters: header.parameters,
        return_type: header.return_type,
        base64_body,
        fuel,
        memory,
    })
}

fn parse_wasm_with(s: &str) -> PgWireResult<(Option<u64>, Option<usize>, &str)> {
    let upper = s.to_uppercase();
    if !upper.starts_with("WITH") {
        return Ok((None, None, s));
    }
    let after = &s["WITH".len()..].trim_start();
    if !after.starts_with('(') {
        return Ok((None, None, s));
    }
    let close = after
        .find(')')
        .ok_or_else(|| sqlstate_error("42601", "unmatched '(' in WITH"))?;
    let inner = &after[1..close];
    let rest = after[close + 1..].trim();

    let mut fuel = None;
    let mut memory = None;
    for part in inner.split(',') {
        let kv: Vec<&str> = part.split('=').map(str::trim).collect();
        if kv.len() != 2 {
            continue;
        }
        match kv[0].to_uppercase().as_str() {
            "FUEL" => fuel = kv[1].parse().ok(),
            "MEMORY" => memory = kv[1].parse().ok(),
            _ => {}
        }
    }

    Ok((fuel, memory, rest))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_basic_wasm_create() {
        let sql = "CREATE FUNCTION add(a INT, b INT) RETURNS INT LANGUAGE WASM AS 'AGFzbQ=='";
        let parsed = parse_wasm_create(sql).unwrap();
        assert_eq!(parsed.name, "add");
        assert_eq!(parsed.parameters.len(), 2);
        assert_eq!(parsed.return_type, "INT");
        assert_eq!(parsed.base64_body, "AGFzbQ==");
        assert!(!parsed.or_replace);
    }

    #[test]
    fn parse_or_replace() {
        let sql = "CREATE OR REPLACE FUNCTION f() RETURNS INT LANGUAGE WASM AS 'AGFzbQ=='";
        let parsed = parse_wasm_create(sql).unwrap();
        assert!(parsed.or_replace);
    }

    #[test]
    fn parse_with_limits() {
        let sql = "CREATE FUNCTION f(x INT) RETURNS INT LANGUAGE WASM WITH (FUEL = 500000, MEMORY = 8388608) AS 'AGFzbQ=='";
        let parsed = parse_wasm_create(sql).unwrap();
        assert_eq!(parsed.fuel, Some(500000));
        assert_eq!(parsed.memory, Some(8388608));
    }

    #[test]
    fn parse_missing_language() {
        let sql = "CREATE FUNCTION f() RETURNS INT AS 'body'";
        assert!(parse_wasm_create(sql).is_err());
    }
}
