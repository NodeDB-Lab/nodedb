//! `CREATE PROCEDURE` DDL handler.
//!
//! Grammar:
//! ```text
//! CREATE [OR REPLACE] PROCEDURE <name>(<param> <type> [, ...])
//!   [WITH (MAX_ITERATIONS = N, TIMEOUT = N)]
//!   AS BEGIN ... END;
//! ```

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::catalog::procedure_types::*;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::types::{require_admin, sqlstate_error};

/// Handle `CREATE [OR REPLACE] PROCEDURE ...`
pub fn create_procedure(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    require_admin(identity, "create procedures")?;

    let parsed = parse_create_procedure(sql)?;
    let tenant_id = identity.tenant_id.as_u32();

    let catalog = state
        .credentials
        .catalog()
        .as_ref()
        .ok_or_else(|| sqlstate_error("XX000", "system catalog not available"))?;

    if !parsed.or_replace
        && let Ok(Some(_)) = catalog.get_procedure(tenant_id, &parsed.name)
    {
        return Err(sqlstate_error(
            "42723",
            &format!("procedure '{}' already exists", parsed.name),
        ));
    }

    // Validate body parses as procedural SQL.
    crate::control::planner::procedural::parse_block(&parsed.body_sql)
        .map_err(|e| sqlstate_error("42601", &format!("procedure body parse error: {e}")))?;

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| sqlstate_error("XX000", "system clock before UNIX epoch"))?
        .as_secs();

    let stored = StoredProcedure {
        tenant_id,
        name: parsed.name.clone(),
        parameters: parsed.parameters,
        body_sql: parsed.body_sql,
        max_iterations: parsed.max_iterations,
        timeout_secs: parsed.timeout_secs,
        owner: identity.username.clone(),
        created_at: now,
    };

    catalog
        .put_procedure(&stored)
        .map_err(|e| sqlstate_error("XX000", &format!("catalog write: {e}")))?;

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("CREATE PROCEDURE {}", stored.name),
    );

    Ok(vec![Response::Execution(Tag::new("CREATE PROCEDURE"))])
}

// ─── Parsing ─────────────────────────────────────────────────────────────────

struct ParsedCreateProcedure {
    or_replace: bool,
    name: String,
    parameters: Vec<ProcedureParam>,
    body_sql: String,
    max_iterations: u64,
    timeout_secs: u64,
}

fn parse_create_procedure(sql: &str) -> PgWireResult<ParsedCreateProcedure> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_uppercase();

    let (or_replace, rest) = if upper.starts_with("CREATE OR REPLACE PROCEDURE ") {
        (true, &trimmed["CREATE OR REPLACE PROCEDURE ".len()..])
    } else if upper.starts_with("CREATE PROCEDURE ") {
        (false, &trimmed["CREATE PROCEDURE ".len()..])
    } else {
        return Err(sqlstate_error("42601", "expected CREATE PROCEDURE"));
    };

    // Find param list in parens.
    let paren_open = rest
        .find('(')
        .ok_or_else(|| sqlstate_error("42601", "expected '(' after procedure name"))?;
    let name = rest[..paren_open].trim().to_lowercase();
    if name.is_empty() {
        return Err(sqlstate_error("42601", "procedure name required"));
    }

    let paren_close = super::super::parse_utils::find_matching_paren(rest, paren_open)
        .ok_or_else(|| sqlstate_error("42601", "unmatched '(' in parameter list"))?;
    let params_str = &rest[paren_open + 1..paren_close];
    let parameters = parse_procedure_params(params_str)?;

    let after_params = rest[paren_close + 1..].trim();

    // Optional WITH (...) clause.
    let (max_iterations, timeout_secs, after_with) = parse_with_clause(after_params)?;

    // Expect AS then BEGIN...END body.
    let rest_upper = after_with.to_uppercase();
    let body_start = if rest_upper.starts_with("AS ") || rest_upper.starts_with("AS\n") {
        after_with["AS".len()..].trim()
    } else {
        after_with
    };

    let body_sql = body_start.trim().trim_end_matches(';').trim().to_string();
    if body_sql.is_empty() || !body_sql.to_uppercase().starts_with("BEGIN") {
        return Err(sqlstate_error(
            "42601",
            "procedure body must start with BEGIN",
        ));
    }

    Ok(ParsedCreateProcedure {
        or_replace,
        name,
        parameters,
        body_sql,
        max_iterations,
        timeout_secs,
    })
}

fn parse_procedure_params(params_str: &str) -> PgWireResult<Vec<ProcedureParam>> {
    let trimmed = params_str.trim();
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }

    let mut params = Vec::new();
    for part in trimmed.split(',') {
        let tokens: Vec<&str> = part.split_whitespace().collect();
        if tokens.is_empty() {
            continue;
        }

        // Optional direction: IN/OUT/INOUT.
        let (direction, name_idx) = match tokens[0].to_uppercase().as_str() {
            "IN" if tokens.len() >= 3 => (ParamDirection::In, 1),
            "OUT" if tokens.len() >= 3 => (ParamDirection::Out, 1),
            "INOUT" if tokens.len() >= 3 => (ParamDirection::InOut, 1),
            _ => (ParamDirection::In, 0), // default IN
        };

        if name_idx + 1 >= tokens.len() {
            return Err(sqlstate_error(
                "42601",
                &format!("parameter must have name and type: '{}'", part.trim()),
            ));
        }

        let name = tokens[name_idx].to_lowercase();
        let data_type = tokens[name_idx + 1..].join(" ").to_uppercase();

        params.push(ProcedureParam {
            name,
            data_type,
            direction,
        });
    }
    Ok(params)
}

fn parse_with_clause(s: &str) -> PgWireResult<(u64, u64, &str)> {
    let upper = s.to_uppercase();
    if !upper.starts_with("WITH") {
        return Ok((1_000_000, 60, s));
    }

    let after_with = &s["WITH".len()..].trim_start();
    if !after_with.starts_with('(') {
        return Ok((1_000_000, 60, s));
    }

    let close = after_with
        .find(')')
        .ok_or_else(|| sqlstate_error("42601", "unmatched '(' in WITH clause"))?;
    let inner = &after_with[1..close];
    let rest = after_with[close + 1..].trim();

    let mut max_iter = 1_000_000u64;
    let mut timeout = 60u64;

    for part in inner.split(',') {
        let kv: Vec<&str> = part.split('=').map(str::trim).collect();
        if kv.len() != 2 {
            continue;
        }
        match kv[0].to_uppercase().as_str() {
            "MAX_ITERATIONS" => {
                max_iter = kv[1]
                    .parse()
                    .map_err(|_| sqlstate_error("42601", "invalid MAX_ITERATIONS value"))?;
            }
            "TIMEOUT" => {
                timeout = kv[1]
                    .parse()
                    .map_err(|_| sqlstate_error("42601", "invalid TIMEOUT value"))?;
            }
            _ => {}
        }
    }

    Ok((max_iter, timeout, rest))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_basic() {
        let sql =
            "CREATE PROCEDURE archive(cutoff INT) AS BEGIN DELETE FROM old WHERE age > cutoff; END";
        let parsed = parse_create_procedure(sql).unwrap();
        assert_eq!(parsed.name, "archive");
        assert_eq!(parsed.parameters.len(), 1);
        assert_eq!(parsed.parameters[0].name, "cutoff");
        assert_eq!(parsed.parameters[0].data_type, "INT");
        assert!(parsed.body_sql.starts_with("BEGIN"));
    }

    #[test]
    fn parse_or_replace() {
        let sql = "CREATE OR REPLACE PROCEDURE p() AS BEGIN RETURN; END";
        let parsed = parse_create_procedure(sql).unwrap();
        assert!(parsed.or_replace);
    }

    #[test]
    fn parse_with_clause() {
        let sql =
            "CREATE PROCEDURE p() WITH (MAX_ITERATIONS = 500, TIMEOUT = 30) AS BEGIN RETURN; END";
        let parsed = parse_create_procedure(sql).unwrap();
        assert_eq!(parsed.max_iterations, 500);
        assert_eq!(parsed.timeout_secs, 30);
    }

    #[test]
    fn parse_out_param() {
        let sql = "CREATE PROCEDURE p(IN x INT, OUT result TEXT) AS BEGIN RETURN; END";
        let parsed = parse_create_procedure(sql).unwrap();
        assert_eq!(parsed.parameters[0].direction, ParamDirection::In);
        assert_eq!(parsed.parameters[1].direction, ParamDirection::Out);
        assert_eq!(parsed.parameters[1].name, "result");
    }

    #[test]
    fn parse_no_params() {
        let sql = "CREATE PROCEDURE cleanup() AS BEGIN DELETE FROM temp; END";
        let parsed = parse_create_procedure(sql).unwrap();
        assert!(parsed.parameters.is_empty());
    }
}
