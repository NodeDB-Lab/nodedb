// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral EXPLAIN PERMISSION and policy introspection DDL commands.
//!
//! ```sql
//! EXPLAIN PERMISSION READ ON orders FOR AUTH USER 'user_123'
//! EXPLAIN SCOPE FOR AUTH USER 'user_123'
//! ```
//!
//! Ported from the pgwire `ddl::explain_ddl` handlers. The permission /
//! scope evaluation reads and the synthetic-identity construction are
//! preserved verbatim; only the result construction changed from pgwire
//! `Response` / `QueryResponse` to the protocol-neutral `DdlResult` over
//! `ShapedRows`.

use serde_json::{Map, Value as JsonValue};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::security::request_scope::RequestAuthScope;
use crate::control::server::response_shape::types::ShapedRows;
use crate::control::state::SharedState;

use super::super::result::{DdlError, DdlResult};

/// Build a [`DdlError`] from an ANSI SQLSTATE code and a message.
///
/// Preserves the exact SQLSTATE / message the pgwire explain handlers
/// produced (via `sqlstate_error`), so error parity stays byte-identical
/// after the migration off the pgwire router.
fn ddl_err(sqlstate: &str, message: impl Into<String>) -> DdlError {
    DdlError::new(sqlstate, message)
}

/// EXPLAIN PERMISSION <perm> ON <collection> FOR AUTH USER '<id>'
pub fn explain_permission(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    // EXPLAIN PERMISSION READ ON orders FOR AUTH USER 'user_123'
    if parts.len() < 8 {
        return Err(ddl_err(
            "42601",
            "syntax: EXPLAIN PERMISSION <perm> ON <collection> FOR AUTH USER '<id>'",
        ));
    }
    let perm = parts[2];
    let collection = parts[4];
    let user_id_str = parts.get(7).map(|s| s.trim_matches('\'')).unwrap_or("");

    // Build a synthetic identity for the target user.
    let target_identity = if let Some(user) = state.credentials.get_user(user_id_str) {
        {
            let is_su = user.is_superuser;
            crate::control::security::identity::AuthenticatedIdentity::from_catalog_principal(
                crate::control::security::identity::CatalogPrincipal {
                    user_id: user.user_id,
                    username: user.username.clone(),
                    tenant_id: user.tenant_id,
                    auth_method: crate::control::security::identity::AuthMethod::Trust,
                    roles: user.roles.clone(),
                    is_superuser: is_su,
                    default_database: None,
                    accessible_databases: crate::control::security::identity::AuthenticatedIdentity::default_database_set(is_su),
                },
            )
        }
    } else {
        // Unknown user — use the requesting identity.
        identity.clone()
    };

    let scope = RequestAuthScope::builder(&target_identity, state.auth_stores()).build();
    let explanation = crate::control::security::explain::explain_permission(
        perm,
        collection,
        &target_identity,
        scope.auth(),
        state,
    );

    let columns = vec![
        "check".to_string(),
        "result".to_string(),
        "source".to_string(),
    ];

    let mut rows: Vec<Map<String, JsonValue>> = explanation
        .steps
        .iter()
        .map(|s| {
            let mut row = Map::new();
            row.insert("check".to_string(), JsonValue::String(s.check.clone()));
            row.insert("result".to_string(), JsonValue::String(s.result.clone()));
            row.insert("source".to_string(), JsonValue::String(s.source.clone()));
            row
        })
        .collect();

    // Final result row.
    let mut row = Map::new();
    row.insert("check".to_string(), JsonValue::String("FINAL".to_string()));
    row.insert(
        "result".to_string(),
        JsonValue::String((if explanation.allowed { "ALLOW" } else { "DENY" }).to_string()),
    );
    row.insert(
        "source".to_string(),
        JsonValue::String(format!("{} on {}", perm, collection)),
    );
    rows.push(row);

    Ok(vec![DdlResult::Rows(ShapedRows::text_rows(columns, rows))])
}

/// EXPLAIN SCOPE FOR AUTH USER '<id>'
pub fn explain_scope(
    state: &SharedState,
    _identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    if parts.len() < 6 {
        return Err(ddl_err(
            "42601",
            "syntax: EXPLAIN SCOPE FOR AUTH USER '<id>'",
        ));
    }
    let user_id = parts.get(5).map(|s| s.trim_matches('\'')).unwrap_or("");
    let org_ids = state.orgs.orgs_for_user(user_id);
    let effective = state.scope_grants.effective_scopes(user_id, &org_ids);

    let columns = vec![
        "scope".to_string(),
        "source".to_string(),
        "resolved_grants".to_string(),
    ];

    let rows: Vec<Map<String, JsonValue>> = effective
        .iter()
        .map(|scope_name| {
            let source = if state
                .scope_grants
                .scopes_for("user", user_id)
                .contains(scope_name)
            {
                "direct (user)"
            } else {
                "inherited (org)"
            };
            let resolved = state.scope_defs.resolve(scope_name);
            let grants_str: Vec<String> = resolved
                .iter()
                .map(|(p, c)| format!("{p} ON {c}"))
                .collect();

            let mut row = Map::new();
            row.insert("scope".to_string(), JsonValue::String(scope_name.clone()));
            row.insert("source".to_string(), JsonValue::String(source.to_string()));
            row.insert(
                "resolved_grants".to_string(),
                JsonValue::String(grants_str.join(", ")),
            );
            row
        })
        .collect();

    Ok(vec![DdlResult::Rows(ShapedRows::text_rows(columns, rows))])
}

/// `SELECT nodedb_assert_visible('<collection>', '<row_id>', '<user_id>')`
///
/// Test helper: returns true/false whether a row is visible to a user under RLS.
pub fn assert_visible(
    state: &SharedState,
    _identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    // Parse: nodedb_assert_visible('collection', 'row_id', 'user_id')
    if parts.len() < 4 {
        return Err(ddl_err(
            "42601",
            "syntax: SELECT nodedb_assert_visible('<collection>', '<row_id>', '<user_id>')",
        ));
    }

    let collection = parts[1].trim_matches('\'').trim_end_matches(',');
    let _row_id = parts[2].trim_matches('\'').trim_end_matches(',');
    let user_id = parts[3].trim_matches('\'').trim_end_matches(')');

    // Build AuthContext for the target user.
    let target_identity = crate::control::security::identity::AuthenticatedIdentity::new_regular(
        user_id.parse().unwrap_or(0),
        user_id,
        crate::types::TenantId::new(1),
        crate::control::security::identity::AuthMethod::Trust,
        vec![crate::control::security::identity::Role::ReadWrite],
        None,
        crate::control::security::identity::DatabaseSet::Some(smallvec::smallvec![
            nodedb_types::id::DatabaseId::DEFAULT,
        ]),
    );
    let scope = RequestAuthScope::builder(&target_identity, state.auth_stores()).build();

    // Check if RLS policies would filter this user. An unencodable policy
    // is an error, not an answer.
    let rls_bytes = state
        .rls
        .combined_read_predicate_with_auth(
            target_identity.tenant_id.as_u64(),
            collection,
            scope.auth(),
        )
        .map_err(|e| DdlError::new("XX000", format!("rls compile: {e}")))?;

    let visible = rls_bytes.is_some_and(|b| b.is_empty()); // No filters = visible.

    let mut row = Map::new();
    row.insert(
        "visible".to_string(),
        JsonValue::String(visible.to_string()),
    );

    Ok(vec![DdlResult::Rows(ShapedRows::text_rows(
        vec!["visible".to_string()],
        vec![row],
    ))])
}
