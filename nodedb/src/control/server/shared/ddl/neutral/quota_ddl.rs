// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral per-scope token quota DDL.
//!
//! ```sql
//! DEFINE QUOTA ON SCOPE 'ops:all' MAX 1000000 TOKENS PER 2592000 SECONDS
//!     ENFORCEMENT HARD [WARN AT 0.8]
//! DROP QUOTA ON SCOPE 'ops:all'
//! SHOW QUOTAS
//! ```
//!
//! Definitions are replicated catalog state. Each statement proposes a
//! `CatalogEntry`, so every node writes the row and installs the definition
//! in its own `QuotaManager`. A cap defined on one node enforces on all.

use serde_json::{Map, Value as JsonValue};

use crate::control::catalog_entry::entry::CatalogEntry;
use crate::control::catalog_entry::post_apply::scope_quota as scope_quota_post_apply;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::security::metering::quota::{QuotaDefinition, QuotaEnforcement};
use crate::control::server::response_shape::types::ShapedRows;
use crate::control::state::SharedState;

use super::super::result::{DdlError, DdlResult};
use super::replicate::propose_and_apply;

/// Default warning threshold when `WARN AT` is omitted, matching the
/// documented default on `QuotaDefinition::warning_threshold`.
const DEFAULT_WARNING_THRESHOLD: f64 = 0.8;

fn err(sqlstate: &str, message: impl Into<String>) -> DdlError {
    DdlError::new(sqlstate, message)
}

const DEFINE_SYNTAX: &str = "syntax: DEFINE QUOTA ON SCOPE '<scope>' MAX <tokens> TOKENS \
     PER <seconds> SECONDS ENFORCEMENT HARD|SOFT|THROTTLE|OVERAGE [WARN AT <0.0-1.0>]";

/// The token following `keyword`, or `None` when the keyword is absent or ends
/// the statement.
fn value_after<'a>(parts: &[&'a str], keyword: &str) -> Option<&'a str> {
    let at = parts.iter().position(|p| p.eq_ignore_ascii_case(keyword))?;
    parts.get(at + 1).copied()
}

/// Parse a required unsigned value, reporting the keyword that introduced it
/// so the operator knows which number was rejected.
fn required_u64(parts: &[&str], keyword: &str) -> Result<u64, DdlError> {
    let raw = value_after(parts, keyword).ok_or_else(|| err("42601", DEFINE_SYNTAX))?;
    raw.trim_matches('\'').parse::<u64>().map_err(|_| {
        err(
            "42601",
            format!("{keyword} expects a whole number, got '{raw}'"),
        )
    })
}

/// DEFINE QUOTA ON SCOPE '<scope>' MAX <n> TOKENS PER <secs> SECONDS
/// ENFORCEMENT <mode> [WARN AT <fraction>]
pub fn define_quota(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    if !identity.is_superuser {
        return Err(err("42501", "permission denied: requires superuser"));
    }

    let scope_name = value_after(parts, "SCOPE")
        .ok_or_else(|| err("42601", DEFINE_SYNTAX))?
        .trim_matches('\'')
        .to_string();
    if scope_name.is_empty() {
        return Err(err("42601", "quota scope name must not be empty"));
    }

    let max_tokens = required_u64(parts, "MAX")?;
    let period_secs = required_u64(parts, "PER")?;
    if period_secs == 0 {
        return Err(err(
            "42601",
            "quota period must be at least one second; a zero-length period never rolls over",
        ));
    }

    let enforcement_raw = value_after(parts, "ENFORCEMENT").ok_or_else(|| {
        err(
            "42601",
            "DEFINE QUOTA requires ENFORCEMENT HARD|SOFT|THROTTLE|OVERAGE",
        )
    })?;
    let enforcement = QuotaEnforcement::parse(enforcement_raw.trim_matches('\''))
        .map_err(|e| err("42601", e.to_string()))?;

    // `WARN AT 0.8` — the fraction follows AT, not WARN.
    let warning_threshold = match value_after(parts, "AT") {
        Some(raw) => {
            let parsed = raw
                .trim_matches('\'')
                .parse::<f64>()
                .map_err(|_| err("42601", format!("WARN AT expects a fraction, got '{raw}'")))?;
            if !(0.0..=1.0).contains(&parsed) {
                return Err(err(
                    "42601",
                    format!("WARN AT must be between 0.0 and 1.0, got {parsed}"),
                ));
            }
            parsed
        }
        None => DEFAULT_WARNING_THRESHOLD,
    };

    let stored = QuotaDefinition {
        scope_name: scope_name.clone(),
        max_tokens,
        period_secs,
        enforcement,
        warning_threshold,
    }
    .to_stored();

    // Replicated: every node writes the row and installs the definition in
    // its `QuotaManager` via post-apply.
    propose_and_apply(
        state,
        &CatalogEntry::PutScopeQuota(Box::new(stored.clone())),
        || {
            state
                .credentials
                .catalog()
                .put_scope_quota(&stored)
                .map_err(|e| err("XX000", e.to_string()))?;
            scope_quota_post_apply::put(&stored, state);
            Ok(())
        },
    )?;

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!(
            "defined quota on scope '{scope_name}': {max_tokens} tokens per {period_secs}s ({})",
            enforcement.as_str()
        ),
    );

    Ok(vec![DdlResult::Status {
        command: "DEFINE QUOTA".to_string(),
        rows_affected: None,
    }])
}

/// DROP QUOTA ON SCOPE '<scope>'
pub fn drop_quota(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    if !identity.is_superuser {
        return Err(err("42501", "permission denied: requires superuser"));
    }

    let scope_name = value_after(parts, "SCOPE")
        .ok_or_else(|| err("42601", "syntax: DROP QUOTA ON SCOPE '<scope>'"))?
        .trim_matches('\'')
        .to_string();

    // Absence is decided on the leader, before the propose: a rejection at
    // apply time would leave the followers disagreeing with the leader.
    if !state.quota_manager.has_quota(&scope_name) {
        return Err(err(
            "42704",
            format!("no quota defined on scope '{scope_name}'"),
        ));
    }

    propose_and_apply(
        state,
        &CatalogEntry::DeleteScopeQuota {
            scope_name: scope_name.clone(),
        },
        || {
            state
                .credentials
                .catalog()
                .delete_scope_quota(&scope_name)
                .map_err(|e| err("XX000", e.to_string()))?;
            scope_quota_post_apply::delete(&scope_name, state);
            Ok(())
        },
    )?;

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("dropped quota on scope '{scope_name}'"),
    );

    Ok(vec![DdlResult::Status {
        command: "DROP QUOTA".to_string(),
        rows_affected: None,
    }])
}

/// SHOW QUOTAS
pub fn show_quotas(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    _parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    if !identity.is_superuser {
        return Err(err("42501", "permission denied: requires superuser"));
    }

    let columns = vec![
        "scope".to_string(),
        "max_tokens".to_string(),
        "period_secs".to_string(),
        "enforcement".to_string(),
        "warning_threshold".to_string(),
    ];

    let rows: Vec<_> = state
        .quota_manager
        .list_quotas()
        .into_iter()
        .map(|quota| {
            let mut row = Map::new();
            row.insert("scope".to_string(), JsonValue::String(quota.scope_name));
            row.insert(
                "max_tokens".to_string(),
                JsonValue::String(quota.max_tokens.to_string()),
            );
            row.insert(
                "period_secs".to_string(),
                JsonValue::String(quota.period_secs.to_string()),
            );
            row.insert(
                "enforcement".to_string(),
                JsonValue::String(quota.enforcement.as_str().to_string()),
            );
            row.insert(
                "warning_threshold".to_string(),
                JsonValue::String(format!("{:.2}", quota.warning_threshold)),
            );
            row
        })
        .collect();

    Ok(vec![DdlResult::Rows(ShapedRows::text_rows(columns, rows))])
}
