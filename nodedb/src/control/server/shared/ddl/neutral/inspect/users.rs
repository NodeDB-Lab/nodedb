// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral user / role / session introspection: SHOW USERS,
//! SHOW ROLES, SHOW SESSION.
//!
//! Ported from the pgwire `ddl::inspect` handlers. The credential / role /
//! identity reads are preserved verbatim; only the result construction
//! changed from pgwire `Response` / `QueryResponse` to the protocol-neutral
//! `DdlResult` over `ShapedRows`.

use serde_json::{Map, Value as JsonValue};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::types::{DdlColType, ShapedRows};
use crate::control::state::SharedState;

use super::super::super::result::{DdlError, DdlResult};

/// SHOW USERS — list all active users.
///
/// Superuser sees all users. Tenant admin sees users in their tenant.
pub fn show_users(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
) -> Result<Vec<DdlResult>, DdlError> {
    let columns = vec![
        "username".to_string(),
        "tenant_id".to_string(),
        "roles".to_string(),
        "is_superuser".to_string(),
    ];
    let column_types = vec![
        DdlColType::Text,
        DdlColType::Int8,
        DdlColType::Text,
        DdlColType::Text,
    ];

    let users = state.credentials.list_user_details();
    let mut rows = Vec::new();

    for user in &users {
        // Filter: superuser sees all, tenant_admin sees own tenant only.
        if !identity.is_superuser && user.tenant_id != identity.tenant_id {
            continue;
        }

        let mut row = Map::new();
        row.insert(
            "username".to_string(),
            JsonValue::String(user.username.clone()),
        );
        row.insert(
            "tenant_id".to_string(),
            JsonValue::String((user.tenant_id.as_u64() as i64).to_string()),
        );
        let roles_str: String = user
            .roles
            .iter()
            .map(|r| r.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        row.insert("roles".to_string(), JsonValue::String(roles_str));
        row.insert(
            "is_superuser".to_string(),
            JsonValue::String(if user.is_superuser { "t" } else { "f" }.to_string()),
        );
        rows.push(row);
    }

    Ok(vec![DdlResult::Rows(ShapedRows::from_json_rows(
        columns,
        column_types,
        rows,
    ))])
}

/// SHOW ROLES — list all custom roles. Built-in role enum is fixed
/// and not enumerated here; this lists the user-defined roles created
/// via `CREATE ROLE`.
///
/// Superuser sees all roles. Non-superusers see roles in their own
/// tenant only.
pub fn show_roles(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
) -> Result<Vec<DdlResult>, DdlError> {
    let columns = vec![
        "name".to_string(),
        "tenant_id".to_string(),
        "parent".to_string(),
        "created_at".to_string(),
    ];
    let column_types = vec![
        DdlColType::Text,
        DdlColType::Int8,
        DdlColType::Text,
        DdlColType::Int8,
    ];

    let roles = state.roles.list_roles();
    let mut rows = Vec::new();
    for role in &roles {
        if !identity.is_superuser && role.tenant_id != identity.tenant_id {
            continue;
        }
        let mut row = Map::new();
        row.insert("name".to_string(), JsonValue::String(role.name.clone()));
        row.insert(
            "tenant_id".to_string(),
            JsonValue::String((role.tenant_id.as_u64() as i64).to_string()),
        );
        row.insert(
            "parent".to_string(),
            JsonValue::String(role.parent.as_deref().unwrap_or("").to_string()),
        );
        row.insert(
            "created_at".to_string(),
            JsonValue::String((role.created_at as i64).to_string()),
        );
        rows.push(row);
    }

    Ok(vec![DdlResult::Rows(ShapedRows::from_json_rows(
        columns,
        column_types,
        rows,
    ))])
}

/// SHOW SESSION — display current session identity.
pub fn show_session(identity: &AuthenticatedIdentity) -> Result<Vec<DdlResult>, DdlError> {
    let columns = vec![
        "username".to_string(),
        "user_id".to_string(),
        "tenant_id".to_string(),
        "roles".to_string(),
        "auth_method".to_string(),
        "is_superuser".to_string(),
    ];
    let column_types = vec![
        DdlColType::Text,
        DdlColType::Int8,
        DdlColType::Int8,
        DdlColType::Text,
        DdlColType::Text,
        DdlColType::Text,
    ];

    let roles_str: String = identity
        .roles
        .iter()
        .map(|r| r.to_string())
        .collect::<Vec<_>>()
        .join(", ");

    let auth_method = format!("{:?}", identity.auth_method);

    let mut row = Map::new();
    row.insert(
        "username".to_string(),
        JsonValue::String(identity.username.clone()),
    );
    row.insert(
        "user_id".to_string(),
        JsonValue::String((identity.user_id as i64).to_string()),
    );
    row.insert(
        "tenant_id".to_string(),
        JsonValue::String((identity.tenant_id.as_u64() as i64).to_string()),
    );
    row.insert("roles".to_string(), JsonValue::String(roles_str));
    row.insert("auth_method".to_string(), JsonValue::String(auth_method));
    row.insert(
        "is_superuser".to_string(),
        JsonValue::String(if identity.is_superuser { "t" } else { "f" }.to_string()),
    );

    Ok(vec![DdlResult::Rows(ShapedRows::from_json_rows(
        columns,
        column_types,
        vec![row],
    ))])
}
