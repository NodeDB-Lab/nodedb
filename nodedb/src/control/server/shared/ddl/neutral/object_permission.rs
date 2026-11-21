// SPDX-License-Identifier: BUSL-1.1

//! Permission checks shared by the schema-defining DDL handlers.
//!
//! The DDL router runs no permission check, so each handler calls one of
//! these two functions before it reads or changes anything.
//!
//! - [`require_create_permission`] guards the creation of a new object. The
//!   caller needs the `Create` permission on the database.
//! - [`require_owner_or_admin`] guards a change to an existing collection.
//!   The caller must own the collection, hold the `TenantAdmin` role, or be
//!   a superuser. This is the test `DROP COLLECTION` and `CREATE INDEX` apply.
//!
//! A refusal is SQLSTATE `42501`. The message names the missing right and
//! never reports whether the object exists.

use std::sync::Arc;

use nodedb_types::DatabaseId;

use crate::control::security::audit::ArcAuditEmitter;
use crate::control::security::identity::{AuthenticatedIdentity, Permission, Role};
use crate::control::server::shared::authorization::authorize_database_permission;
use crate::control::state::SharedState;

use super::super::result::DdlError;

/// Require the `Create` permission on `database_id` to create `what`
/// (for example `"collections"`).
pub(crate) fn require_create_permission(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    what: &str,
) -> Result<(), DdlError> {
    let audit = ArcAuditEmitter(Arc::clone(&state.audit));
    authorize_database_permission(
        identity,
        database_id,
        Permission::Create,
        state.credentials.catalog(),
        &audit,
    )
    .map_err(|_| {
        DdlError::new(
            "42501",
            format!("permission denied: the Create permission is required to create {what}"),
        )
    })
}

/// Require that the caller owns `collection`, holds `TenantAdmin`, or is a
/// superuser. `action` completes the refusal message, for example
/// `"alter collections"`.
pub(crate) fn require_owner_or_admin(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    collection: &str,
    action: &str,
) -> Result<(), DdlError> {
    if identity.is_superuser || identity.has_role(&Role::TenantAdmin) {
        return Ok(());
    }
    let tenant_id = identity.tenant_id;
    let registered_owner = state
        .permissions
        .get_owner("collection", database_id.as_u64(), tenant_id, collection);
    if registered_owner.as_deref() == Some(identity.username.as_str()) {
        return Ok(());
    }
    let catalog_owner = state
        .credentials
        .catalog()
        .get_collection(database_id, tenant_id.as_u64(), collection)
        .ok()
        .flatten()
        .map(|stored| stored.owner);
    if catalog_owner.as_deref() == Some(identity.username.as_str()) {
        return Ok(());
    }
    Err(DdlError::new(
        "42501",
        format!("permission denied: only the owner, superuser, or tenant_admin can {action}"),
    ))
}
