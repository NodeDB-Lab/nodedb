//! Collection DDL: CREATE COLLECTION, DROP COLLECTION, SHOW COLLECTIONS.

use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::audit::AuditEvent;
use crate::control::security::catalog::StoredCollection;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::types::{int8_field, sqlstate_error, text_field};

/// CREATE COLLECTION <name> [FIELDS (<field> <type>, ...)]
///
/// Creates a collection owned by the current user in the current tenant.
pub fn create_collection(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if parts.len() < 3 {
        return Err(sqlstate_error(
            "42601",
            "syntax: CREATE COLLECTION <name> [FIELDS (<field> <type>, ...)]",
        ));
    }

    let name = parts[2];
    let tenant_id = identity.tenant_id;

    // Check if collection already exists.
    if let Some(catalog) = state.credentials.catalog() {
        if let Ok(Some(existing)) = catalog.get_collection(tenant_id.as_u32(), name) {
            if existing.is_active {
                return Err(sqlstate_error(
                    "42P07",
                    &format!("collection '{name}' already exists"),
                ));
            }
        }
    }

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let coll = StoredCollection {
        tenant_id: tenant_id.as_u32(),
        name: name.to_string(),
        owner: identity.username.clone(),
        created_at: now,
        fields: Vec::new(), // TODO: parse FIELDS clause
        is_active: true,
    };

    // Persist to catalog.
    if let Some(catalog) = state.credentials.catalog() {
        catalog
            .put_collection(&coll)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
    }

    // Set ownership.
    let catalog = state.credentials.catalog();
    state
        .permissions
        .set_owner(
            "collection",
            tenant_id,
            name,
            &identity.username,
            catalog.as_ref(),
        )
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;

    state.audit_record(
        AuditEvent::AdminAction,
        Some(tenant_id),
        &identity.username,
        &format!("created collection '{name}'"),
    );

    Ok(vec![Response::Execution(Tag::new("CREATE COLLECTION"))])
}

/// DROP COLLECTION <name>
///
/// Marks collection as inactive. Requires owner or admin.
pub fn drop_collection(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if parts.len() < 3 {
        return Err(sqlstate_error("42601", "syntax: DROP COLLECTION <name>"));
    }

    let name = parts[2];
    let tenant_id = identity.tenant_id;

    // Check ownership or admin.
    let is_owner = state
        .permissions
        .get_owner("collection", tenant_id, name)
        .as_deref()
        == Some(&identity.username);

    if !is_owner
        && !identity.is_superuser
        && !identity.has_role(&crate::control::security::identity::Role::TenantAdmin)
    {
        return Err(sqlstate_error(
            "42501",
            "permission denied: only owner, superuser, or tenant_admin can drop collections",
        ));
    }

    // Mark as inactive in catalog.
    if let Some(catalog) = state.credentials.catalog() {
        if let Ok(Some(mut coll)) = catalog.get_collection(tenant_id.as_u32(), name) {
            coll.is_active = false;
            catalog
                .put_collection(&coll)
                .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        } else {
            return Err(sqlstate_error(
                "42P01",
                &format!("collection '{name}' does not exist"),
            ));
        }
    }

    state.audit_record(
        AuditEvent::AdminAction,
        Some(tenant_id),
        &identity.username,
        &format!("dropped collection '{name}'"),
    );

    Ok(vec![Response::Execution(Tag::new("DROP COLLECTION"))])
}

/// SHOW COLLECTIONS
///
/// Lists all active collections for the current tenant.
pub fn show_collections(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
) -> PgWireResult<Vec<Response>> {
    let tenant_id = identity.tenant_id;

    let schema = Arc::new(vec![
        text_field("name"),
        text_field("owner"),
        int8_field("created_at"),
    ]);

    let collections = if let Some(catalog) = state.credentials.catalog() {
        if identity.is_superuser {
            catalog
                .load_all_collections()
                .unwrap_or_default()
                .into_iter()
                .filter(|c| c.is_active)
                .collect::<Vec<_>>()
        } else {
            catalog
                .load_collections_for_tenant(tenant_id.as_u32())
                .unwrap_or_default()
        }
    } else {
        Vec::new()
    };

    let mut rows = Vec::with_capacity(collections.len());
    let mut encoder = DataRowEncoder::new(schema.clone());

    for coll in &collections {
        encoder
            .encode_field(&coll.name)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        encoder
            .encode_field(&coll.owner)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        encoder
            .encode_field(&(coll.created_at as i64))
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        rows.push(Ok(encoder.take_row()));
    }

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}
