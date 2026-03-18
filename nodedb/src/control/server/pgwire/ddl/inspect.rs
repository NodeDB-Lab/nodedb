use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response};
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::types::{int8_field, sqlstate_error, text_field};

/// SHOW USERS — list all active users.
///
/// Superuser sees all users. Tenant admin sees users in their tenant.
pub fn show_users(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
) -> PgWireResult<Vec<Response>> {
    let schema = Arc::new(vec![
        text_field("username"),
        int8_field("tenant_id"),
        text_field("roles"),
        text_field("is_superuser"),
    ]);

    let users = state.credentials.list_user_details();
    let mut rows = Vec::new();
    let mut encoder = DataRowEncoder::new(schema.clone());

    for user in &users {
        // Filter: superuser sees all, tenant_admin sees own tenant only.
        if !identity.is_superuser && user.tenant_id != identity.tenant_id {
            continue;
        }

        encoder.encode_field(&user.username).map_err(encode_err)?;
        encoder
            .encode_field(&(user.tenant_id.as_u32() as i64))
            .map_err(encode_err)?;
        let roles_str: String = user
            .roles
            .iter()
            .map(|r| r.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        encoder.encode_field(&roles_str).map_err(encode_err)?;
        encoder
            .encode_field(&if user.is_superuser { "t" } else { "f" })
            .map_err(encode_err)?;
        rows.push(Ok(encoder.take_row()));
    }

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}

/// SHOW TENANTS — list all tenants with quotas.
///
/// Superuser only.
pub fn show_tenants(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
) -> PgWireResult<Vec<Response>> {
    if !identity.is_superuser {
        return Err(sqlstate_error(
            "42501",
            "permission denied: only superuser can list tenants",
        ));
    }

    let schema = Arc::new(vec![
        int8_field("tenant_id"),
        int8_field("active_requests"),
        int8_field("total_requests"),
        int8_field("rejected_requests"),
    ]);

    let tenants = match state.tenants.lock() {
        Ok(t) => t,
        Err(p) => p.into_inner(),
    };

    // Collect tenant IDs that have usage data.
    let mut rows = Vec::new();
    let mut encoder = DataRowEncoder::new(schema.clone());

    // We iterate through known users' tenants since TenantIsolation
    // doesn't expose a list method. Usage is tracked on first request.
    let user_details = state.credentials.list_user_details();
    let mut seen_tenants = std::collections::HashSet::new();

    for user in &user_details {
        let tid = user.tenant_id;
        if !seen_tenants.insert(tid) {
            continue;
        }

        let usage = tenants.usage(tid);
        encoder
            .encode_field(&(tid.as_u32() as i64))
            .map_err(encode_err)?;
        encoder
            .encode_field(&(usage.map_or(0, |u| u.active_requests as i64)))
            .map_err(encode_err)?;
        encoder
            .encode_field(&(usage.map_or(0, |u| u.total_requests as i64)))
            .map_err(encode_err)?;
        encoder
            .encode_field(&(usage.map_or(0, |u| u.rejected_requests as i64)))
            .map_err(encode_err)?;
        rows.push(Ok(encoder.take_row()));
    }

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}

/// SHOW SESSION — display current session identity.
pub fn show_session(identity: &AuthenticatedIdentity) -> PgWireResult<Vec<Response>> {
    let schema = Arc::new(vec![
        text_field("username"),
        int8_field("user_id"),
        int8_field("tenant_id"),
        text_field("roles"),
        text_field("auth_method"),
        text_field("is_superuser"),
    ]);

    let roles_str: String = identity
        .roles
        .iter()
        .map(|r| r.to_string())
        .collect::<Vec<_>>()
        .join(", ");

    let auth_method = format!("{:?}", identity.auth_method);

    let mut encoder = DataRowEncoder::new(schema.clone());
    encoder
        .encode_field(&identity.username)
        .map_err(encode_err)?;
    encoder
        .encode_field(&(identity.user_id as i64))
        .map_err(encode_err)?;
    encoder
        .encode_field(&(identity.tenant_id.as_u32() as i64))
        .map_err(encode_err)?;
    encoder.encode_field(&roles_str).map_err(encode_err)?;
    encoder.encode_field(&auth_method).map_err(encode_err)?;
    encoder
        .encode_field(&if identity.is_superuser { "t" } else { "f" })
        .map_err(encode_err)?;

    let row = encoder.take_row();
    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(vec![Ok(row)]),
    ))])
}

/// SHOW GRANTS FOR <user>
pub fn show_grants(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
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
            return Err(sqlstate_error(
                "42501",
                "permission denied: can only view your own grants, or be superuser/tenant_admin",
            ));
        }
        target.to_string()
    } else {
        identity.username.clone()
    };

    let schema = Arc::new(vec![text_field("username"), text_field("role")]);

    let user = state.credentials.get_user(&target_user);
    let mut rows = Vec::new();
    let mut encoder = DataRowEncoder::new(schema.clone());

    if let Some(user) = user {
        for role in &user.roles {
            encoder.encode_field(&user.username).map_err(encode_err)?;
            encoder
                .encode_field(&role.to_string())
                .map_err(encode_err)?;
            rows.push(Ok(encoder.take_row()));
        }
    }

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}

/// SHOW PERMISSIONS ON <collection>
///
/// Shows all grants and the owner for a specific collection.
pub fn show_permissions(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    // SHOW PERMISSIONS ON <collection>
    if parts.len() < 4
        || !parts[1].eq_ignore_ascii_case("PERMISSIONS")
        || !parts[2].eq_ignore_ascii_case("ON")
    {
        return Err(sqlstate_error(
            "42601",
            "syntax: SHOW PERMISSIONS ON <collection>",
        ));
    }

    let collection = parts[3];
    let target = format!("collection:{}:{collection}", identity.tenant_id.as_u32());

    let schema = Arc::new(vec![
        text_field("grantee"),
        text_field("permission"),
        text_field("type"),
    ]);

    let mut rows = Vec::new();
    let mut encoder = DataRowEncoder::new(schema.clone());

    // Show owner.
    if let Some(owner) = state
        .permissions
        .get_owner("collection", identity.tenant_id, collection)
    {
        encoder.encode_field(&owner).map_err(encode_err)?;
        encoder.encode_field(&"ALL (owner)").map_err(encode_err)?;
        encoder.encode_field(&"ownership").map_err(encode_err)?;
        rows.push(Ok(encoder.take_row()));
    }

    // Show explicit grants.
    let grants = state.permissions.grants_on(&target);
    for grant in &grants {
        encoder.encode_field(&grant.grantee).map_err(encode_err)?;
        encoder
            .encode_field(&format!("{:?}", grant.permission))
            .map_err(encode_err)?;
        encoder.encode_field(&"grant").map_err(encode_err)?;
        rows.push(Ok(encoder.take_row()));
    }

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}

/// Convert a pgwire encode error to a PgWireError.
fn encode_err(e: pgwire::error::PgWireError) -> pgwire::error::PgWireError {
    e
}
