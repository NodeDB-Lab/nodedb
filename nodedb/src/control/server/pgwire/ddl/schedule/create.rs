//! `CREATE SCHEDULE` DDL handler.
//!
//! Syntax:
//! ```sql
//! CREATE SCHEDULE <name> CRON '<expr>' [SCOPE LOCAL]
//!   [WITH (ALLOW_OVERLAP = false, MISSED = SKIP|CATCH_UP|QUEUE)]
//!   AS <sql_body>
//! ```

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::event::scheduler::cron::CronExpr;
use crate::event::scheduler::types::{MissedPolicy, ScheduleDef, ScheduleScope};

use super::super::super::types::{require_admin, sqlstate_error};

pub fn create_schedule(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    require_admin(identity, "create schedules")?;

    let parsed = parse_create_schedule(sql)?;
    let tenant_id = identity.tenant_id.as_u32();

    // Validate cron expression.
    CronExpr::parse(&parsed.cron_expr)
        .map_err(|e| sqlstate_error("42601", &format!("invalid cron expression: {e}")))?;

    // Validate SQL body parses.
    crate::control::planner::procedural::parse_block(&parsed.body_sql)
        .map_err(|e| sqlstate_error("42601", &format!("schedule body parse error: {e}")))?;

    // Check for duplicate.
    if state
        .schedule_registry
        .get(tenant_id, &parsed.name)
        .is_some()
    {
        return Err(sqlstate_error(
            "42710",
            &format!("schedule '{}' already exists", parsed.name),
        ));
    }

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| sqlstate_error("XX000", "system clock error"))?
        .as_secs();

    let target_collection = extract_target_collection(&parsed.body_sql);

    let def = ScheduleDef {
        tenant_id,
        name: parsed.name.clone(),
        cron_expr: parsed.cron_expr,
        body_sql: parsed.body_sql,
        scope: parsed.scope,
        missed_policy: parsed.missed_policy,
        allow_overlap: parsed.allow_overlap,
        enabled: true,
        target_collection,
        owner: identity.username.clone(),
        created_at: now,
    };

    let catalog = state
        .credentials
        .catalog()
        .as_ref()
        .ok_or_else(|| sqlstate_error("XX000", "system catalog not available"))?;

    // Replicate through the metadata raft group. Every node's
    // applier writes the record to local redb and registers it in
    // the in-memory `schedule_registry` so the cron executor picks
    // it up on its next tick — cluster-wide.
    let entry = crate::control::catalog_entry::CatalogEntry::PutSchedule(Box::new(def.clone()));
    let log_index = crate::control::metadata_proposer::propose_catalog_entry(state, &entry)
        .map_err(|e| sqlstate_error("XX000", &format!("metadata propose: {e}")))?;
    if log_index == 0 {
        catalog
            .put_schedule(&def)
            .map_err(|e| sqlstate_error("XX000", &format!("catalog write: {e}")))?;
        state.schedule_registry.register(def.clone());
    }

    // Emit schedule definition as CRDT sync delta for Lite visibility.
    // Lite devices receive this but NEVER execute Origin schedules —
    // they display them via SHOW SCHEDULES for observability.
    {
        let delta_payload = zerompk::to_msgpack_vec(&def).unwrap_or_default();
        let delta = crate::event::crdt_sync::types::OutboundDelta {
            collection: "_schedules".into(),
            document_id: def.name.clone(),
            payload: delta_payload,
            op: crate::event::crdt_sync::types::DeltaOp::Upsert,
            lsn: 0,
            tenant_id,
            peer_id: state.node_id,
            sequence: 0,
        };
        state.crdt_sync_delivery.enqueue(tenant_id, delta);
    }

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("CREATE SCHEDULE {}", parsed.name),
    );

    Ok(vec![Response::Execution(Tag::new("CREATE SCHEDULE"))])
}

struct ParsedSchedule {
    name: String,
    cron_expr: String,
    body_sql: String,
    scope: ScheduleScope,
    missed_policy: MissedPolicy,
    allow_overlap: bool,
}

/// Parse `CREATE SCHEDULE <name> CRON '<expr>' [SCOPE LOCAL] [WITH (...)] AS <body>`
fn parse_create_schedule(sql: &str) -> PgWireResult<ParsedSchedule> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_uppercase();

    let prefix = "CREATE SCHEDULE ";
    if !upper.starts_with(prefix) {
        return Err(sqlstate_error("42601", "expected CREATE SCHEDULE"));
    }
    let rest = &trimmed[prefix.len()..];
    let tokens: Vec<&str> = rest.split_whitespace().collect();

    if tokens.len() < 4 {
        return Err(sqlstate_error(
            "42601",
            "expected CREATE SCHEDULE <name> CRON '<expr>' AS <body>",
        ));
    }

    let name = tokens[0].to_lowercase();
    let mut i = 1;

    // Parse CRON '<expr>'
    if i >= tokens.len() || !tokens[i].eq_ignore_ascii_case("CRON") {
        return Err(sqlstate_error("42601", "expected CRON after schedule name"));
    }
    i += 1;

    // Extract cron expression (may be quoted with single quotes).
    let cron_expr = extract_cron_expr(rest, &tokens, &mut i)?;

    // Parse optional SCOPE LOCAL
    let mut scope = ScheduleScope::Normal;
    if i < tokens.len() && tokens[i].eq_ignore_ascii_case("SCOPE") {
        i += 1;
        if i < tokens.len() && tokens[i].eq_ignore_ascii_case("LOCAL") {
            scope = ScheduleScope::Local;
            i += 1;
        }
    }

    // Parse optional WITH (...)
    let mut missed_policy = MissedPolicy::Skip;
    let mut allow_overlap = true;
    if i < tokens.len() && tokens[i].eq_ignore_ascii_case("WITH") {
        // Find the WITH clause in the original string and parse options.
        if let Some(with_pos) = upper.find(" WITH ") {
            let after_with = &trimmed[with_pos + 6..];
            if let Some(inner) = after_with
                .trim()
                .strip_prefix('(')
                .and_then(|s| s.split_once(')'))
                .map(|(inner, _)| inner)
            {
                for opt in inner.split(',') {
                    let opt = opt.trim();
                    if let Some((key, val)) = opt.split_once('=') {
                        let key = key.trim().to_uppercase();
                        let val = val.trim().trim_matches('\'').trim_matches('"');
                        match key.as_str() {
                            "ALLOW_OVERLAP" => {
                                allow_overlap = !val.eq_ignore_ascii_case("false");
                            }
                            "MISSED" => {
                                if let Some(p) = MissedPolicy::from_str_opt(val) {
                                    missed_policy = p;
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }
        }
    }

    // Find AS keyword and extract body.
    let as_pos = upper
        .rfind(" AS ")
        .ok_or_else(|| sqlstate_error("42601", "expected AS <sql_body>"))?;
    let body_sql = trimmed[as_pos + 4..].trim().to_string();

    if body_sql.is_empty() {
        return Err(sqlstate_error("42601", "schedule body is empty"));
    }

    Ok(ParsedSchedule {
        name,
        cron_expr,
        body_sql,
        scope,
        missed_policy,
        allow_overlap,
    })
}

/// Extract the cron expression, handling both quoted ('0 0 * * *') and unquoted forms.
fn extract_cron_expr(rest: &str, tokens: &[&str], i: &mut usize) -> PgWireResult<String> {
    // Look for quoted expression in the original string.
    let cron_keyword_pos = rest.to_uppercase().find("CRON").unwrap_or(0);
    let after_cron = rest[cron_keyword_pos + 4..].trim_start();

    if let Some(after_quote) = after_cron.strip_prefix('\'') {
        // Quoted: extract between single quotes.
        if let Some(end) = after_quote.find('\'') {
            let expr = after_quote[..end].to_string();
            // Advance token pointer past the quoted expression.
            // Skip until we're past the closing quote in the token stream.
            while *i < tokens.len() {
                let t = tokens[*i];
                *i += 1;
                if t.ends_with('\'') {
                    break;
                }
            }
            return Ok(expr);
        }
    }

    // Unquoted: take next 5 tokens as the cron fields.
    if *i + 4 < tokens.len() {
        let expr = format!(
            "{} {} {} {} {}",
            tokens[*i],
            tokens[*i + 1],
            tokens[*i + 2],
            tokens[*i + 3],
            tokens[*i + 4]
        );
        *i += 5;
        return Ok(expr);
    }

    Err(sqlstate_error(
        "42601",
        "expected cron expression after CRON keyword",
    ))
}

/// Extract the target collection from a schedule's SQL body.
///
/// Looks for the first collection name after FROM, INTO, UPDATE, ON keywords
/// in the procedural body. Returns `None` for cross-collection, dynamic SQL,
/// or CALL statements (conservative default → `_system` coordinator in cluster mode).
fn extract_target_collection(body_sql: &str) -> Option<String> {
    let tokens: Vec<&str> = body_sql.split_whitespace().collect();
    let upper_tokens: Vec<String> = tokens.iter().map(|t| t.to_uppercase()).collect();

    // Find the first occurrence of FROM/INTO/UPDATE/ON followed by a collection name.
    // Skip BEGIN, END, and SQL keywords that aren't collection references.
    let target_keywords = ["FROM", "INTO", "UPDATE", "ON"];
    let skip_after = [
        "BEGIN",
        "END",
        "EACH",
        "ROW",
        "STATEMENT",
        "RETURN",
        "IF",
        "THEN",
        "ELSE",
        "LOOP",
        "WHILE",
        "DECLARE",
        "SET",
        "WHERE",
        "AND",
        "OR",
        "NOT",
        "NULL",
        "TRUE",
        "FALSE",
    ];

    for (i, token) in upper_tokens.iter().enumerate() {
        if target_keywords.contains(&token.as_str()) && i + 1 < tokens.len() {
            let candidate = tokens[i + 1]
                .trim_end_matches([';', '(', ')'])
                .to_lowercase();
            if !candidate.is_empty()
                && !skip_after.contains(&candidate.to_uppercase().as_str())
                && !candidate.starts_with('$')
                && !candidate.starts_with('\'')
            {
                return Some(candidate);
            }
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_basic_schedule() {
        let sql = "CREATE SCHEDULE cleanup CRON '0 0 * * *' AS BEGIN DELETE FROM old; END";
        let parsed = parse_create_schedule(sql).unwrap();
        assert_eq!(parsed.name, "cleanup");
        assert_eq!(parsed.cron_expr, "0 0 * * *");
        assert!(parsed.body_sql.contains("DELETE FROM old"));
        assert_eq!(parsed.scope, ScheduleScope::Normal);
        assert!(parsed.allow_overlap);
    }

    #[test]
    fn parse_with_scope_local() {
        let sql = "CREATE SCHEDULE gc CRON '0 * * * *' SCOPE LOCAL AS BEGIN RETURN; END";
        let parsed = parse_create_schedule(sql).unwrap();
        assert_eq!(parsed.scope, ScheduleScope::Local);
    }

    #[test]
    fn parse_with_options() {
        let sql = "CREATE SCHEDULE agg CRON '*/5 * * * *' \
                    WITH (ALLOW_OVERLAP = false, MISSED = CATCH_UP) \
                    AS BEGIN INSERT INTO stats SELECT count(*) FROM events; END";
        let parsed = parse_create_schedule(sql).unwrap();
        assert!(!parsed.allow_overlap);
        assert_eq!(parsed.missed_policy, MissedPolicy::CatchUp);
    }
}
