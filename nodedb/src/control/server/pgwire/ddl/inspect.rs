use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, FieldInfo, QueryResponse, Response};
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::types::{int8_field, sqlstate_error, text_field};

/// Shared schema for both `show_audit_log` and `show_audit_log_memory`.
fn audit_schema() -> Arc<Vec<FieldInfo>> {
    Arc::new(vec![
        int8_field("seq"),
        int8_field("timestamp_us"),
        text_field("event"),
        int8_field("tenant_id"),
        text_field("source"),
        text_field("detail"),
    ])
}

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

        encoder.encode_field(&user.username)?;
        encoder.encode_field(&(user.tenant_id.as_u32() as i64))?;
        let roles_str: String = user
            .roles
            .iter()
            .map(|r| r.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        encoder.encode_field(&roles_str)?;
        encoder.encode_field(&if user.is_superuser { "t" } else { "f" })?;
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
        encoder.encode_field(&(tid.as_u32() as i64))?;
        encoder.encode_field(&(usage.map_or(0, |u| u.active_requests as i64)))?;
        encoder.encode_field(&(usage.map_or(0, |u| u.total_requests as i64)))?;
        encoder.encode_field(&(usage.map_or(0, |u| u.rejected_requests as i64)))?;
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
    encoder.encode_field(&identity.username)?;
    encoder.encode_field(&(identity.user_id as i64))?;
    encoder.encode_field(&(identity.tenant_id.as_u32() as i64))?;
    encoder.encode_field(&roles_str)?;
    encoder.encode_field(&auth_method)?;
    encoder.encode_field(&if identity.is_superuser { "t" } else { "f" })?;

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
            encoder.encode_field(&user.username)?;
            encoder.encode_field(&role.to_string())?;
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
        encoder.encode_field(&owner)?;
        encoder.encode_field(&"ALL (owner)")?;
        encoder.encode_field(&"ownership")?;
        rows.push(Ok(encoder.take_row()));
    }

    // Show explicit grants.
    let grants = state.permissions.grants_on(&target);
    for grant in &grants {
        encoder.encode_field(&grant.grantee)?;
        encoder.encode_field(&format!("{:?}", grant.permission))?;
        encoder.encode_field(&"grant")?;
        rows.push(Ok(encoder.take_row()));
    }

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}

/// SHOW AUDIT LOG [LIMIT <n>]
///
/// Shows recent persisted audit entries. Superuser only.
pub fn show_audit_log(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if !identity.is_superuser {
        return Err(sqlstate_error(
            "42501",
            "permission denied: only superuser can view audit log",
        ));
    }

    let limit = if parts.len() >= 5 && parts[3].eq_ignore_ascii_case("LIMIT") {
        parts[4].parse::<usize>().unwrap_or(100)
    } else {
        100
    };

    let catalog = match state.credentials.catalog() {
        Some(c) => c,
        None => {
            // No persistent catalog — show in-memory entries only.
            return show_audit_log_memory(state, limit);
        }
    };

    let entries = catalog
        .load_recent_audit_entries(limit)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;

    let schema = audit_schema();

    let mut rows = Vec::with_capacity(entries.len());
    let mut encoder = DataRowEncoder::new(schema.clone());

    for entry in entries.iter().rev() {
        // Most recent first.
        encoder.encode_field(&(entry.seq as i64))?;
        encoder.encode_field(&(entry.timestamp_us as i64))?;
        encoder.encode_field(&entry.event)?;
        encoder.encode_field(&(entry.tenant_id.unwrap_or(0) as i64))?;
        encoder.encode_field(&entry.source)?;
        encoder.encode_field(&entry.detail)?;
        rows.push(Ok(encoder.take_row()));
    }

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}

/// Show in-memory audit entries (when no persistent catalog).
fn show_audit_log_memory(state: &SharedState, limit: usize) -> PgWireResult<Vec<Response>> {
    let log = match state.audit.lock() {
        Ok(l) => l,
        Err(p) => p.into_inner(),
    };

    let schema = audit_schema();

    let all = log.all();
    let skip = if all.len() > limit {
        all.len() - limit
    } else {
        0
    };

    let mut rows = Vec::new();
    let mut encoder = DataRowEncoder::new(schema.clone());

    for entry in all.iter().skip(skip).rev() {
        encoder.encode_field(&(entry.seq as i64))?;
        encoder.encode_field(&(entry.timestamp_us as i64))?;
        encoder.encode_field(&format!("{:?}", entry.event))?;
        encoder.encode_field(&(entry.tenant_id.map_or(0i64, |t| t.as_u32() as i64)))?;
        encoder.encode_field(&entry.source)?;
        encoder.encode_field(&entry.detail)?;
        rows.push(Ok(encoder.take_row()));
    }

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}
