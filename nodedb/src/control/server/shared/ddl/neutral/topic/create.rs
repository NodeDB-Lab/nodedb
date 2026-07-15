// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `CREATE TOPIC` DDL handler.
//!
//! Ported from the pgwire `ddl::topic::create` handler. The tenant-admin gate,
//! the duplicate-topic check, the optional `WITH (RETENTION = '…')` parse, the
//! `TopicDef` build, the direct `catalog.put_ep_topic` + `ep_topic_registry`
//! registration path (NOT `propose_and_apply` — this family writes the catalog
//! directly), and the `audit_record` call are preserved verbatim; only the
//! result construction changed from pgwire `Response` / `PgWireError` to the
//! protocol-neutral [`DdlResult`] / [`DdlError`].
//!
//! Syntax: `CREATE TOPIC <name> [WITH (RETENTION = '1 hour')]`

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::event::cdc::stream_def::RetentionConfig;
use crate::event::topic::TopicDef;
use nodedb_sql::parser::preprocess::lex::{find_ascii_case_insensitive, find_ascii_keyword};

use super::super::super::result::{DdlError, DdlResult};
use super::super::auth_support::{require_tenant_admin, status};

pub fn create_topic(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
    sql: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    require_tenant_admin(identity, "create topics")?;

    // parts: ["CREATE", "TOPIC", "<name>", ...]
    if parts.len() < 3 {
        return Err(DdlError {
            sqlstate: "42601".to_string(),
            message: "expected CREATE TOPIC <name>".to_string(),
        });
    }

    let name = parts[2].to_lowercase();
    let tenant_id = identity.tenant_id.as_u64();

    if state.ep_topic_registry.get(tenant_id, &name).is_some() {
        return Err(DdlError {
            sqlstate: "42710".to_string(),
            message: format!("topic '{name}' already exists"),
        });
    }

    // Parse optional retention from WITH clause.
    let mut retention = RetentionConfig {
        max_events: 10_000,
        max_age_secs: 3_600, // 1 hour default for topics.
    };

    if let Some(with_pos) = find_ascii_keyword(sql, "WITH") {
        let with_section = sql[with_pos + 4..].trim();
        if let Some(inner) = with_section
            .strip_prefix('(')
            .and_then(|s| s.split_once(')'))
            .map(|(inner, _)| inner)
            && let Some(ret_pos) = find_ascii_case_insensitive(inner, "RETENTION")
        {
            let after = inner[ret_pos + 9..].trim().trim_start_matches('=').trim();
            let val = after
                .trim_start_matches('\'')
                .split('\'')
                .next()
                .unwrap_or("");
            retention.max_age_secs = parse_duration_secs(val);
        }
    }

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| DdlError {
            sqlstate: "XX000".to_string(),
            message: "system clock error".to_string(),
        })?
        .as_secs();

    let def = TopicDef {
        tenant_id,
        name: name.clone(),
        retention,
        owner: identity.username.clone(),
        created_at: now,
    };

    let catalog = state.credentials.catalog();

    catalog.put_ep_topic(&def).map_err(|e| DdlError {
        sqlstate: "XX000".to_string(),
        message: format!("catalog write: {e}"),
    })?;

    state.ep_topic_registry.register(def);

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("CREATE TOPIC {name}"),
    );

    Ok(status("CREATE TOPIC"))
}

/// Parse a human-friendly duration string into seconds.
fn parse_duration_secs(s: &str) -> u64 {
    let s = s.trim().to_lowercase();
    if let Some(h) = s.strip_suffix("hour").or_else(|| s.strip_suffix("hours")) {
        return h.trim().parse::<u64>().unwrap_or(1) * 3600;
    }
    if let Some(m) = s
        .strip_suffix("minute")
        .or_else(|| s.strip_suffix("minutes"))
    {
        return m.trim().parse::<u64>().unwrap_or(1) * 60;
    }
    if let Some(d) = s.strip_suffix("day").or_else(|| s.strip_suffix("days")) {
        return d.trim().parse::<u64>().unwrap_or(1) * 86_400;
    }
    // Try raw seconds.
    s.parse::<u64>().unwrap_or(3_600)
}
