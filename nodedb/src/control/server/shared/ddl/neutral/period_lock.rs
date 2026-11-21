// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral DDL handlers for ADD/DROP PERIOD LOCK.
//!
//! Syntax:
//! ```sql
//! ALTER COLLECTION journal_entries
//!     ADD PERIOD LOCK ON fiscal_period
//!     REFERENCES fiscal_periods(period_key)
//!     STATUS status
//!     ALLOW WRITE WHEN status IN ('OPEN', 'ADJUSTING');
//!
//! ALTER COLLECTION journal_entries DROP PERIOD LOCK;
//! ```
//!
//! The token parsing, catalog get/put, schema-version bump, and audit side
//! effects run here. The result is the protocol-neutral [`DdlResult`] with its
//! SQLSTATE codes, messages, and command tags.

use nodedb_types::DatabaseId;

use crate::control::catalog_entry::persist_collection_replicated;
use crate::control::security::catalog::PeriodLockDef;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::ddl::sql_parse::parse_ident_token;
use crate::control::state::SharedState;

use super::super::result::{DdlError, DdlResult};

/// Construct a [`DdlError`] from a SQLSTATE code and a message.
fn err(sqlstate: &str, message: impl Into<String>) -> DdlError {
    DdlError::new(sqlstate, message)
}

/// Handle `ALTER COLLECTION x ADD PERIOD LOCK ON col REFERENCES table(pk) ...`
pub async fn add_period_lock(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    sql: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let tenant_id = identity.tenant_id.as_u64();
    let parts: Vec<&str> = sql.split_whitespace().collect();
    let upper = sql.to_uppercase();

    // ALTER COLLECTION <name> ADD PERIOD LOCK ON <col> REFERENCES <table>(<pk>) ...
    let name = parse_ident_token(
        parts
            .get(2)
            .ok_or_else(|| err("42601", "missing collection name"))?,
    )?;

    crate::control::server::shared::ddl::neutral::object_permission::require_owner_or_admin(
        state,
        identity,
        database_id,
        &name,
        "change period locks",
    )?;

    // Find the period column: "ON <col>"
    let on_idx = parts
        .iter()
        .position(|p| p.eq_ignore_ascii_case("ON"))
        .ok_or_else(|| err("42601", "ADD PERIOD LOCK requires ON <column>"))?;
    let period_column = parse_ident_token(
        parts
            .get(on_idx + 1)
            .ok_or_else(|| err("42601", "missing column name after ON"))?,
    )?;

    // Find REFERENCES <table>(<pk>)
    let ref_idx = parts
        .iter()
        .position(|p| p.eq_ignore_ascii_case("REFERENCES"))
        .ok_or_else(|| err("42601", "ADD PERIOD LOCK requires REFERENCES <table>(<pk>)"))?;
    let ref_spec = parts
        .get(ref_idx + 1)
        .ok_or_else(|| err("42601", "missing table(pk) after REFERENCES"))?;
    let (ref_table, ref_pk) = parse_table_pk(ref_spec)?;

    // Find STATUS <col> (optional, defaults to "status").
    let status_column =
        if let Some(si) = parts.iter().position(|p| p.eq_ignore_ascii_case("STATUS")) {
            match parts.get(si + 1) {
                Some(token) => parse_ident_token(token)?,
                None => "status".to_string(),
            }
        } else {
            "status".into()
        };

    // Find ALLOW WRITE WHEN status IN ('OPEN', 'ADJUSTING')
    let allowed_statuses = parse_allowed_statuses(&upper)?;

    let def = PeriodLockDef {
        period_column,
        ref_table,
        ref_pk,
        status_column,
        allowed_statuses,
    };

    // Load the collection, update, and re-persist.
    let catalog = state.credentials.catalog();

    let mut coll = catalog
        .get_collection(database_id, tenant_id, &name)
        .map_err(|e| DdlError::from_error(&e))?
        .ok_or_else(|| err("42P01", format!("collection '{name}' not found")))?;

    coll.period_lock = Some(def);

    persist_collection_replicated(state, &coll)
        .await
        .map_err(|e| DdlError::from_error(&e))?;

    state.schema_version.bump();

    // Audit: ConfigChange → Standard audit level.
    state.audit_record(
        crate::control::security::audit::AuditEvent::ConfigChange,
        Some(identity.tenant_id),
        &identity.username,
        &format!("ADD PERIOD LOCK on {name}"),
    );

    Ok(vec![DdlResult::Status {
        command: "ALTER COLLECTION".to_string(),
        rows_affected: None,
    }])
}

/// Handle `ALTER COLLECTION x DROP PERIOD LOCK`.
pub async fn drop_period_lock(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    let tenant_id = identity.tenant_id.as_u64();
    let name = parse_ident_token(
        parts
            .get(2)
            .ok_or_else(|| err("42601", "missing collection name"))?,
    )?;

    crate::control::server::shared::ddl::neutral::object_permission::require_owner_or_admin(
        state,
        identity,
        database_id,
        &name,
        "change period locks",
    )?;

    let catalog = state.credentials.catalog();

    let mut coll = catalog
        .get_collection(database_id, tenant_id, &name)
        .map_err(|e| DdlError::from_error(&e))?
        .ok_or_else(|| err("42P01", format!("collection '{name}' not found")))?;

    coll.period_lock = None;

    persist_collection_replicated(state, &coll)
        .await
        .map_err(|e| DdlError::from_error(&e))?;

    state.schema_version.bump();

    // Audit: ConfigChange → Standard audit level.
    state.audit_record(
        crate::control::security::audit::AuditEvent::ConfigChange,
        Some(identity.tenant_id),
        &identity.username,
        &format!("DROP PERIOD LOCK on {name}"),
    );

    Ok(vec![DdlResult::Status {
        command: "ALTER COLLECTION".to_string(),
        rows_affected: None,
    }])
}

/// Parse `table(pk)` into `(table, pk)`.
fn parse_table_pk(spec: &str) -> Result<(String, String), DdlError> {
    let spec = spec.trim();
    if let Some(paren_start) = spec.find('(') {
        // An empty table or pk is rejected by the identifier check.
        let table = parse_ident_token(spec[..paren_start].trim())?;
        let pk = parse_ident_token(spec[paren_start + 1..].trim_end_matches(')').trim())?;
        Ok((table, pk))
    } else {
        Err(err(
            "42601",
            "REFERENCES requires table(pk) format, e.g. fiscal_periods(period_key)",
        ))
    }
}

/// Parse `ALLOW WRITE WHEN status IN ('OPEN', 'ADJUSTING')`.
fn parse_allowed_statuses(upper: &str) -> Result<Vec<String>, DdlError> {
    // Find "IN" keyword followed by "(" with optional whitespace.
    let in_kw = upper.find("IN").ok_or_else(|| {
        err(
            "42601",
            "ADD PERIOD LOCK requires ALLOW WRITE WHEN ... IN ('status1', ...)",
        )
    })?;
    let after_in = upper[in_kw + 2..].trim_start();
    let after = after_in
        .strip_prefix('(')
        .ok_or_else(|| err("42601", "expected '(' after IN keyword"))?;
    let Some(close) = after.find(')') else {
        return Err(err("42601", "missing closing ')' in IN clause"));
    };
    let inner = &after[..close];
    let statuses: Vec<String> = inner
        .split(',')
        .map(|s| s.trim().trim_matches('\'').trim_matches('"').to_string())
        .filter(|s| !s.is_empty())
        .collect();

    if statuses.is_empty() {
        return Err(err(
            "42601",
            "IN clause must list at least one status value",
        ));
    }

    Ok(statuses)
}
