// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `CREATE API KEY` handler.
//!
//! Ported from the pgwire `ddl::apikey::create_api_key` handler. The
//! permission checks, owner-subset validation, key preparation, catalog
//! propose / single-node fallback, `install_replicated_key`, and `audit_record`
//! side effects are preserved verbatim; only the result construction changed
//! from pgwire `Response` / `QueryResponse` to the protocol-neutral
//! [`DdlResult`] over [`ShapedRows`].

use serde_json::{Map, Value as JsonValue};

use crate::control::security::audit::AuditEvent;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::types::ShapedRows;
use crate::control::state::SharedState;

use super::super::super::result::{DdlError, DdlResult};
use super::super::auth_support::require_tenant_admin;
use super::parse::{
    build_owner_database_set_for_user, err, parse_key_scopes, parse_with_databases,
};

/// CREATE API KEY FOR <user> [EXPIRES <seconds>] [WITH SCOPES ...] [WITH DATABASES (db1, db2)]
///
/// Returns the full API key (shown once). Requires admin or self.
pub fn create_api_key(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    if parts.len() < 5 {
        return Err(err(
            "42601",
            "syntax: CREATE API KEY FOR <user> [EXPIRES <seconds>] [WITH DATABASES (db1, db2)]",
        ));
    }

    if !parts[1].eq_ignore_ascii_case("API")
        || !parts[2].eq_ignore_ascii_case("KEY")
        || !parts[3].eq_ignore_ascii_case("FOR")
    {
        return Err(err(
            "42601",
            "syntax: CREATE API KEY FOR <user> [EXPIRES <seconds>] [WITH DATABASES (db1, db2)]",
        ));
    }

    let target_username = parts[4];

    // Users can create keys for themselves; admin required for others.
    if target_username != identity.username {
        require_tenant_admin(identity, "create API keys for other users")?;
    }

    // Look up the target user.
    let target_user = state
        .credentials
        .get_user(target_username)
        .ok_or_else(|| err("42704", format!("user '{target_username}' not found")))?;

    // Parse optional EXPIRES.
    let mut expires_secs: u64 = 0;
    if let Some(expires_idx) = parts.iter().position(|p| p.eq_ignore_ascii_case("EXPIRES"))
        && let Some(secs_str) = parts.get(expires_idx + 1)
    {
        expires_secs = secs_str
            .parse()
            .map_err(|_| err("42601", "EXPIRES must be a number of seconds"))?;
    }

    // Parse optional WITH SCOPES.
    let key_scopes = parse_key_scopes(parts, state)?;

    // Parse optional WITH DATABASES (db1, db2, ...).
    let requested_db_ids = parse_with_databases(parts, state)?;

    // Build owner_set for subset validation at CREATE time.
    let owner_set = build_owner_database_set_for_user(state, &target_user)?;

    // Validate: requested set must be ⊆ owner_set.
    let accessible_databases = match requested_db_ids {
        None => {
            // No WITH DATABASES clause: inherit owner at bind time.
            vec![]
        }
        Some(ids) => {
            for &db_id in &ids {
                if !owner_set.contains(db_id) {
                    let db_name = state
                        .credentials
                        .catalog()
                        .get_database_name_by_id(db_id)
                        .ok()
                        .flatten()
                        .unwrap_or_else(|| format!("<id:{}>", db_id.as_u64()));
                    return Err(err(
                        "42501",
                        format!(
                            "permission denied: API key cannot have wider access than owner; \
                             database '{db_name}' not in owner's set"
                        ),
                    ));
                }
            }
            ids
        }
    };

    // Build the `StoredApiKey` on the proposer — generates key_id +
    // secret + SHA-256 hash. Only the returned `token` contains the
    // plaintext secret (shown once to the client). The hashed record
    // replicates through raft; every node's applier writes redb +
    // installs the record into the in-memory cache.
    let (stored, token) =
        state
            .api_keys
            .prepare_key(crate::control::security::apikey::CreateKeyParams {
                username: target_username,
                user_id: target_user.user_id,
                tenant_id: target_user.tenant_id,
                expires_secs,
                scope: key_scopes,
                accessible_databases,
            });
    let entry = crate::control::catalog_entry::CatalogEntry::PutApiKey(Box::new(stored.clone()));
    let outcome = crate::control::metadata_proposer::propose_catalog_entry(state, &entry)
        .map_err(|e| err("XX000", format!("metadata propose: {e}")))?;
    if outcome.needs_local_apply() {
        let catalog = state.credentials.catalog();
        catalog
            .put_api_key(&stored)
            .map_err(|e| err("XX000", format!("catalog write: {e}")))?;
        state.api_keys.install_replicated_key(&stored);
    }

    state.audit_record(
        AuditEvent::PrivilegeChange,
        Some(identity.tenant_id),
        &identity.username,
        &format!("created API key for user '{target_username}'"),
    );

    // Return the token as a query result (shown once).
    let mut row = Map::new();
    row.insert("api_key".to_string(), JsonValue::String(token));

    Ok(vec![DdlResult::Rows(ShapedRows::text_rows(
        vec!["api_key".to_string()],
        vec![row],
    ))])
}
