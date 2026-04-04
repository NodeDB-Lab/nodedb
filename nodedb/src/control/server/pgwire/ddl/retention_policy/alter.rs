//! `ALTER RETENTION POLICY` DDL handler.
//!
//! Syntax:
//! ```sql
//! ALTER RETENTION POLICY <name> ON <collection> ENABLE | DISABLE
//! ALTER RETENTION POLICY <name> ON <collection> SET AUTO_TIER = TRUE | FALSE
//! ALTER RETENTION POLICY <name> ON <collection> SET EVAL_INTERVAL = '<duration>'
//! ```

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::types::{require_admin, sqlstate_error};

pub fn alter_retention_policy(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    require_admin(identity, "alter retention policies")?;

    let trimmed = sql.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_uppercase();
    let tenant_id = identity.tenant_id.as_u32();

    // Extract name: "ALTER RETENTION POLICY <name> ..."
    let prefix = "ALTER RETENTION POLICY ";
    if !upper.starts_with(prefix) {
        return Err(sqlstate_error("42601", "expected ALTER RETENTION POLICY"));
    }
    let after_prefix = &trimmed[prefix.len()..];
    let name = after_prefix
        .split_whitespace()
        .next()
        .ok_or_else(|| sqlstate_error("42601", "missing policy name"))?
        .to_lowercase();

    // Load existing policy.
    let mut def = state
        .retention_policy_registry
        .get(tenant_id, &name)
        .ok_or_else(|| {
            sqlstate_error(
                "42704",
                &format!("retention policy '{name}' does not exist"),
            )
        })?;

    // Parse the action.
    if upper.contains(" ENABLE") && !upper.contains("DISABLE") {
        def.enabled = true;
    } else if upper.contains(" DISABLE") {
        def.enabled = false;
    } else if upper.contains("AUTO_TIER") {
        let val = extract_set_value(&upper, "AUTO_TIER")?;
        def.auto_tier = val.eq_ignore_ascii_case("TRUE");
    } else if upper.contains("EVAL_INTERVAL") {
        let val_str = extract_set_quoted_value(trimmed, "EVAL_INTERVAL")?;
        let ms = nodedb_types::kv_parsing::parse_interval_to_ms(&val_str)
            .map_err(|e| sqlstate_error("42601", &format!("invalid interval: {e}")))?;
        def.eval_interval_ms = ms;
    } else {
        return Err(sqlstate_error(
            "42601",
            "expected ENABLE, DISABLE, SET AUTO_TIER, or SET EVAL_INTERVAL",
        ));
    }

    // Persist updated policy.
    let catalog = state
        .credentials
        .catalog()
        .as_ref()
        .ok_or_else(|| sqlstate_error("XX000", "system catalog not available"))?;

    catalog
        .put_retention_policy(&def)
        .map_err(|e| sqlstate_error("XX000", &format!("catalog write: {e}")))?;

    // Update in-memory registry.
    state.retention_policy_registry.register(def);

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("ALTER RETENTION POLICY {name}"),
    );

    Ok(vec![Response::Execution(Tag::new(
        "ALTER RETENTION POLICY",
    ))])
}

/// Extract value from `SET KEY = VALUE` (unquoted).
fn extract_set_value(upper: &str, key: &str) -> PgWireResult<String> {
    let pos = upper
        .find(key)
        .ok_or_else(|| sqlstate_error("42601", &format!("expected {key}")))?;
    let after = upper[pos + key.len()..].trim_start();
    let after = after.strip_prefix('=').unwrap_or(after).trim_start();
    let val = after
        .split_whitespace()
        .next()
        .ok_or_else(|| sqlstate_error("42601", &format!("missing value for {key}")))?;
    Ok(val.to_string())
}

/// Extract value from `SET KEY = 'quoted_value'` (uses original case SQL).
fn extract_set_quoted_value(sql: &str, key: &str) -> PgWireResult<String> {
    let upper = sql.to_uppercase();
    let pos = upper
        .find(key)
        .ok_or_else(|| sqlstate_error("42601", &format!("expected {key}")))?;
    let after = &sql[pos + key.len()..];
    let after = after
        .trim_start()
        .strip_prefix('=')
        .unwrap_or(after)
        .trim_start();
    let start = after
        .find('\'')
        .ok_or_else(|| sqlstate_error("42601", "expected quoted value"))?;
    let end = after[start + 1..]
        .find('\'')
        .ok_or_else(|| sqlstate_error("42601", "missing closing quote"))?;
    Ok(after[start + 1..start + 1 + end].to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_set_value_basic() {
        let val = extract_set_value("SET AUTO_TIER = TRUE", "AUTO_TIER").unwrap();
        assert_eq!(val, "TRUE");
    }

    #[test]
    fn extract_set_quoted_value_basic() {
        let val = extract_set_quoted_value("SET EVAL_INTERVAL = '30m'", "EVAL_INTERVAL").unwrap();
        assert_eq!(val, "30m");
    }
}
