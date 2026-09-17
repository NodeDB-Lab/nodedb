// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `REVOKE API KEY` / `LIST API KEYS` / `SHOW API KEYS`
//! handlers.
//!
//! Ported from the pgwire `ddl::apikey::revoke_api_key` / `list_api_keys`
//! handlers. The ownership checks, local pre-check, catalog propose /
//! single-node fallback, `revoke_key`, and `audit_record` side effects are
//! preserved verbatim; only the result construction changed from pgwire
//! `Response` / `QueryResponse` / `Tag` to the protocol-neutral [`DdlResult`]
//! over [`ShapedRows`].

use serde_json::{Map, Value as JsonValue};

use crate::control::security::audit::AuditEvent;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::types::{DdlColType, ShapedRows};
use crate::control::state::SharedState;

use super::super::super::result::{DdlError, DdlResult};
use super::super::auth_support::require_tenant_admin;
use super::parse::err;

/// REVOKE API KEY <key_id>
pub fn revoke_api_key(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    if parts.len() < 4 {
        return Err(err("42601", "syntax: REVOKE API KEY <key_id>"));
    }

    if !parts[1].eq_ignore_ascii_case("API") || !parts[2].eq_ignore_ascii_case("KEY") {
        return Err(err("42601", "syntax: REVOKE API KEY <key_id>"));
    }

    let key_id = parts[3];

    // Check if the key belongs to the current user or if they're admin.
    let keys = state.api_keys.list_keys_for_user(&identity.username);
    let owns_key = keys.iter().any(|k| k.key_id == key_id);
    if !owns_key {
        require_tenant_admin(identity, "revoke API keys for other users")?;
    }

    // Pre-check existence locally so "key not found" doesn't touch raft.
    let exists_before = state.api_keys.get_key(key_id).is_some();
    if !exists_before {
        return Err(err("42704", format!("API key '{key_id}' not found")));
    }

    let entry = crate::control::catalog_entry::CatalogEntry::RevokeApiKey {
        key_id: key_id.to_string(),
    };
    let outcome = crate::control::metadata_proposer::propose_catalog_entry(state, &entry)
        .map_err(|e| err("XX000", format!("metadata propose: {e}")))?;
    let revoked = if outcome.needs_local_apply() {
        let catalog = state.credentials.catalog();
        state
            .api_keys
            .revoke_key(key_id, Some(catalog))
            .map_err(|e| err("XX000", e.to_string()))?
    } else {
        // Cluster mode: trust the committed log index — the
        // in-memory cache update runs in a spawned tokio task and
        // may not be visible yet.
        true
    };

    if revoked {
        state.audit_record(
            AuditEvent::PrivilegeChange,
            Some(identity.tenant_id),
            &identity.username,
            &format!("revoked API key '{key_id}'"),
        );
        Ok(vec![DdlResult::Status {
            command: "REVOKE API KEY".to_string(),
            rows_affected: None,
        }])
    } else {
        Err(err("42704", format!("API key '{key_id}' not found")))
    }
}

/// LIST API KEYS [FOR <user>] / SHOW API KEYS [FOR <user>]
pub fn list_api_keys(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    // Normalise: both LIST and SHOW lead here; skip the command verb at parts[0]
    // and the "API KEYS" at parts[1..2]; optionally "FOR <user>" at parts[3..4].
    let target_username = if parts.len() >= 5 && parts[3].eq_ignore_ascii_case("FOR") {
        let target = parts[4];
        if target != identity.username {
            require_tenant_admin(identity, "list API keys for other users")?;
        }
        target.to_string()
    } else if parts.len() >= 4 && parts[3].eq_ignore_ascii_case("FOR") {
        return Err(err("42601", "expected username after FOR"));
    } else {
        // Default: list own keys (or all if superuser).
        identity.username.clone()
    };

    let keys = if identity.is_superuser && target_username == identity.username {
        state.api_keys.list_all_keys()
    } else {
        state.api_keys.list_keys_for_user(&target_username)
    };

    let columns = vec![
        "key_id".to_string(),
        "username".to_string(),
        "expires_at".to_string(),
        "is_revoked".to_string(),
        "databases".to_string(),
        "created_at".to_string(),
    ];
    let column_types = vec![
        DdlColType::Text,
        DdlColType::Text,
        DdlColType::Int8,
        DdlColType::Text,
        DdlColType::Text,
        DdlColType::Int8,
    ];

    let mut rows = Vec::with_capacity(keys.len());

    for key in &keys {
        let mut row = Map::new();
        row.insert("key_id".to_string(), JsonValue::String(key.key_id.clone()));
        row.insert(
            "username".to_string(),
            JsonValue::String(key.username.clone()),
        );
        row.insert(
            "expires_at".to_string(),
            JsonValue::String((key.expires_at as i64).to_string()),
        );
        row.insert(
            "is_revoked".to_string(),
            JsonValue::String(if key.is_revoked { "t" } else { "f" }.to_string()),
        );

        // Render the database access column.
        let db_display = if key.accessible_databases.is_empty() {
            "(inherit)".to_string()
        } else {
            let names: Vec<String> = key
                .accessible_databases
                .iter()
                .map(|&db_id| {
                    state
                        .credentials
                        .catalog()
                        .get_database_name_by_id(db_id)
                        .ok()
                        .flatten()
                        .unwrap_or_else(|| format!("<id:{}>", db_id.as_u64()))
                })
                .collect();
            names.join(",")
        };
        row.insert("databases".to_string(), JsonValue::String(db_display));

        row.insert(
            "created_at".to_string(),
            JsonValue::String((key.created_at as i64).to_string()),
        );
        rows.push(row);
    }

    Ok(vec![DdlResult::Rows(ShapedRows::from_json_rows(
        columns,
        column_types,
        rows,
    ))])
}
