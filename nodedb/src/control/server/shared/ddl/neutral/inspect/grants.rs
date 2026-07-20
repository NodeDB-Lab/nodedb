// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral grant / permission introspection: SHOW GRANTS,
//! SHOW PERMISSIONS.
//!
//! Ported from the pgwire `ddl::inspect` handlers. The credential /
//! permission-store reads and the target-key rendering are preserved
//! verbatim; only the result construction changed from pgwire `Response` /
//! `QueryResponse` to the protocol-neutral `DdlResult` over `ShapedRows`.

use serde_json::{Map, Value as JsonValue};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::types::{DdlColType, ShapedRows};
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::super::super::result::{DdlError, DdlResult};
use super::support::ddl_err;

/// SHOW GRANTS FOR <user>
pub fn show_grants(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    // SHOW GRANTS — show own grants
    // SHOW GRANTS FOR <user> — show another user's grants (admin only)
    let target_user = if parts.len() >= 4
        && parts[1].eq_ignore_ascii_case("GRANTS")
        && parts[2].eq_ignore_ascii_case("FOR")
    {
        let target = parts[3];
        if target != identity.username
            && !identity.is_superuser
            && !identity.has_role(&crate::control::security::identity::Role::TenantAdmin)
        {
            return Err(ddl_err(
                "42501",
                "permission denied: can only view your own grants, or be superuser/tenant_admin",
            ));
        }
        target.to_string()
    } else {
        identity.username.clone()
    };

    let columns = vec!["username".to_string(), "role".to_string()];
    let column_types = vec![DdlColType::Text, DdlColType::Text];

    let user = state.credentials.get_user(&target_user);
    let mut rows = Vec::new();

    if let Some(user) = user {
        for role in &user.roles {
            let mut row = Map::new();
            row.insert(
                "username".to_string(),
                JsonValue::String(user.username.clone()),
            );
            row.insert("role".to_string(), JsonValue::String(role.to_string()));
            rows.push(row);
        }
    }

    Ok(vec![DdlResult::Rows(ShapedRows {
        columns,
        column_types,
        rows,
        notice: None,
    })])
}

/// `SHOW PERMISSIONS [ON <collection>] [FOR <user|role>]`
///
/// - `SHOW PERMISSIONS` — all grants visible to the caller
/// - `SHOW PERMISSIONS ON <collection>` — grants on a specific collection plus its owner
/// - `SHOW PERMISSIONS FOR <grantee>` — direct grants to a specific user or role
/// - `SHOW PERMISSIONS ON <collection> FOR <grantee>` — intersection of the above
///
/// For `FOR <role>` only direct grants are returned; inheritance is not walked
/// (`EXPLAIN PERMISSION` owns the resolved-privilege view).
pub fn show_permissions(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    on_collection: Option<&str>,
    for_grantee: Option<&str>,
) -> Result<Vec<DdlResult>, DdlError> {
    // Non-admins may only view their own grants.
    if let Some(grantee) = for_grantee
        && grantee != identity.username
        && !identity.is_superuser
        && !identity.has_role(&crate::control::security::identity::Role::TenantAdmin)
    {
        return Err(ddl_err(
            "42501",
            "permission denied: can only view your own permissions, or be superuser/tenant_admin",
        ));
    }

    let columns = vec![
        "grantee".to_string(),
        "permission".to_string(),
        "target".to_string(),
        "type".to_string(),
    ];
    let column_types = vec![
        DdlColType::Text,
        DdlColType::Text,
        DdlColType::Text,
        DdlColType::Text,
    ];

    let mut rows = Vec::new();

    if let Some(collection) = on_collection {
        let target = format!("collection:{}:{collection}", identity.tenant_id.as_u64());

        // Show owner row (only when collection is specified).
        if for_grantee.is_none()
            && let Some(owner) = state.permissions.get_owner_in_database(
                "collection",
                database_id.as_u64(),
                identity.tenant_id,
                collection,
            )
        {
            let mut row = Map::new();
            row.insert("grantee".to_string(), JsonValue::String(owner));
            row.insert(
                "permission".to_string(),
                JsonValue::String("ALL (owner)".to_string()),
            );
            row.insert(
                "target".to_string(),
                JsonValue::String(collection.to_string()),
            );
            row.insert(
                "type".to_string(),
                JsonValue::String("ownership".to_string()),
            );
            rows.push(row);
        }

        // Show explicit grants on this collection.
        let grants = state.permissions.grants_on(&target);
        for grant in &grants {
            if let Some(g) = for_grantee
                && !grant.grantee.eq_ignore_ascii_case(g)
            {
                continue;
            }
            let mut row = Map::new();
            row.insert(
                "grantee".to_string(),
                JsonValue::String(grant.grantee.clone()),
            );
            row.insert(
                "permission".to_string(),
                JsonValue::String(format!("{:?}", grant.permission)),
            );
            row.insert(
                "target".to_string(),
                JsonValue::String(collection.to_string()),
            );
            row.insert("type".to_string(), JsonValue::String("grant".to_string()));
            rows.push(row);
        }
    } else if let Some(grantee) = for_grantee {
        // All grants for a specific grantee (direct grants only, no inheritance walk).
        let grants = state.permissions.grants_for(grantee);
        for grant in &grants {
            // Extract a human-readable target from the internal target key
            // (e.g. "collection:1:users" → "users").
            let display_target = grant
                .target
                .rsplit(':')
                .next()
                .unwrap_or(&grant.target)
                .to_string();
            let mut row = Map::new();
            row.insert(
                "grantee".to_string(),
                JsonValue::String(grant.grantee.clone()),
            );
            row.insert(
                "permission".to_string(),
                JsonValue::String(format!("{:?}", grant.permission)),
            );
            row.insert("target".to_string(), JsonValue::String(display_target));
            row.insert("type".to_string(), JsonValue::String("grant".to_string()));
            rows.push(row);
        }
    } else {
        // SHOW PERMISSIONS with no filter — show all grants for the current tenant.
        // Non-admins see only their own grants.
        let all_grants = if identity.is_superuser
            || identity.has_role(&crate::control::security::identity::Role::TenantAdmin)
        {
            state.permissions.all_grants(identity.tenant_id)
        } else {
            state.permissions.grants_for(&identity.username)
        };
        for grant in &all_grants {
            let display_target = grant
                .target
                .rsplit(':')
                .next()
                .unwrap_or(&grant.target)
                .to_string();
            let mut row = Map::new();
            row.insert(
                "grantee".to_string(),
                JsonValue::String(grant.grantee.clone()),
            );
            row.insert(
                "permission".to_string(),
                JsonValue::String(format!("{:?}", grant.permission)),
            );
            row.insert("target".to_string(), JsonValue::String(display_target));
            row.insert("type".to_string(), JsonValue::String("grant".to_string()));
            rows.push(row);
        }
    }

    Ok(vec![DdlResult::Rows(ShapedRows {
        columns,
        column_types,
        rows,
        notice: None,
    })])
}
