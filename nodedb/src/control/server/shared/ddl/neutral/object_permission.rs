// SPDX-License-Identifier: BUSL-1.1

//! The single authorization seam of the schema-defining DDL handlers.
//!
//! The DDL router runs no permission check, so each handler calls one of the
//! functions below before it reads or changes anything. All of them reach
//! [`authorize_ddl`], which applies the node setting `[auth] ddl_authorization`
//! ([`DdlAuthorizationMode`]). A superuser always passes.
//!
//! | Mode           | `CreateObject` | `ChangeObject`, `DropObject`        | `TenantWide`  |
//! |----------------|----------------|-------------------------------------|---------------|
//! | `open`         | pass           | pass                                | pass          |
//! | `tenant_admin` | TenantAdmin    | TenantAdmin                         | TenantAdmin   |
//! | `permission`   | `Create`       | `Alter` (`Drop` for a drop) on it   | `Admin`       |
//! | `owner`        | pass           | owner or TenantAdmin                | TenantAdmin   |
//! | `mix`          | `Create`       | owner or TenantAdmin                | TenantAdmin   |
//!
//! - [`require_create_permission`] is `CreateObject` on the database.
//! - [`require_owner_or_admin`] is `ChangeObject` on one collection.
//! - [`require_drop_permission`] is `DropObject` on one collection.
//! - [`require_tenant_wide`] is `TenantWide`.
//!
//! `DROP COLLECTION`, `CREATE INDEX` and `DROP INDEX` keep their own checks,
//! which apply in every mode and do not call this module.
//!
//! A refusal is SQLSTATE `42501`. The message names the missing right and
//! never reports whether the object exists.

use std::sync::Arc;

use nodedb_types::DatabaseId;

use crate::config::auth::DdlAuthorizationMode;
use crate::control::security::audit::ArcAuditEmitter;
use crate::control::security::identity::{AuthenticatedIdentity, Permission, Role};
use crate::control::server::shared::authorization::{
    authorize_collection, authorize_database_permission,
};
use crate::control::state::SharedState;

use super::super::result::DdlError;

/// The kind of change a DDL statement makes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DdlAction {
    /// Create a new object.
    CreateObject,
    /// Change an existing object.
    ChangeObject,
    /// Drop an existing object.
    DropObject,
    /// A statement that affects the whole tenant, not one object.
    TenantWide,
}

/// Authorize one DDL statement under the node setting.
///
/// `what` completes the refusal message, for example `"alter collections"`.
/// `object_name` names the collection for `ChangeObject` and `DropObject`. The
/// other actions ignore it.
pub(crate) fn authorize_ddl(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    action: DdlAction,
    object_name: &str,
    what: &str,
) -> Result<(), DdlError> {
    let mode = state.ddl_authorization();
    if mode == DdlAuthorizationMode::Open || identity.is_superuser {
        return Ok(());
    }
    authorize_in_mode(
        mode,
        state,
        identity,
        database_id,
        action,
        object_name,
        what,
    )
}

fn authorize_in_mode(
    mode: DdlAuthorizationMode,
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    action: DdlAction,
    object_name: &str,
    what: &str,
) -> Result<(), DdlError> {
    use DdlAuthorizationMode as Mode;
    match (mode, action) {
        (Mode::Open, _) | (Mode::Owner, DdlAction::CreateObject) => Ok(()),
        (Mode::TenantAdmin, _) | (Mode::Owner | Mode::Mix, DdlAction::TenantWide) => {
            require_tenant_admin(identity, what)
        }
        (Mode::Permission | Mode::Mix, DdlAction::CreateObject) => {
            require_database_create(state, identity, database_id, what)
        }
        (Mode::Permission, DdlAction::TenantWide) => {
            require_tenant_admin_permission(state, identity, what)
        }
        (Mode::Permission, DdlAction::ChangeObject) => require_object_permission(
            state,
            identity,
            database_id,
            object_name,
            Permission::Alter,
            what,
        ),
        (Mode::Permission, DdlAction::DropObject) => require_object_permission(
            state,
            identity,
            database_id,
            object_name,
            Permission::Drop,
            what,
        ),
        (Mode::Owner | Mode::Mix, DdlAction::ChangeObject | DdlAction::DropObject) => {
            require_owner_or_admin_always(state, identity, database_id, object_name, what)
        }
    }
}

fn denied(message: String) -> DdlError {
    DdlError::new("42501", message)
}

fn require_tenant_admin(identity: &AuthenticatedIdentity, what: &str) -> Result<(), DdlError> {
    if identity.is_superuser || identity.has_role(&Role::TenantAdmin) {
        return Ok(());
    }
    Err(denied(format!(
        "permission denied: only superuser or tenant_admin can {what}"
    )))
}

fn require_tenant_admin_permission(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    what: &str,
) -> Result<(), DdlError> {
    let audit = ArcAuditEmitter(Arc::clone(&state.audit));
    if state.permissions.check_tenant(
        identity,
        Permission::Admin,
        identity.tenant_id,
        &state.roles,
        &audit,
    ) {
        return Ok(());
    }
    Err(denied(format!(
        "permission denied: the Admin permission is required to {what}"
    )))
}

fn require_database_create(
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
        denied(format!(
            "permission denied: the Create permission is required to {what}"
        ))
    })
}

fn require_object_permission(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    object_name: &str,
    permission: Permission,
    what: &str,
) -> Result<(), DdlError> {
    let audit = ArcAuditEmitter(Arc::clone(&state.audit));
    authorize_collection(
        identity,
        database_id,
        object_name,
        permission,
        &state.permissions,
        &state.roles,
        &audit,
    )
    .map_err(|_| {
        denied(format!(
            "permission denied: the {permission:?} permission is required to {what}"
        ))
    })
}

/// Pass when the caller owns `collection`, holds `TenantAdmin`, or is a
/// superuser. The check ignores the node setting.
fn require_owner_or_admin_always(
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
    let registered_owner =
        state
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
    Err(denied(format!(
        "permission denied: only the owner, superuser, or tenant_admin can {action}"
    )))
}

/// Guard the creation of a new object (`CreateObject`). `what` names the
/// objects, for example `"collections"`.
pub(crate) fn require_create_permission(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    what: &str,
) -> Result<(), DdlError> {
    authorize_ddl(
        state,
        identity,
        database_id,
        DdlAction::CreateObject,
        "",
        &format!("create {what}"),
    )
}

/// Guard a change to an existing collection (`ChangeObject`). `action`
/// completes the refusal message, for example `"alter collections"`.
pub(crate) fn require_owner_or_admin(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    collection: &str,
    action: &str,
) -> Result<(), DdlError> {
    authorize_ddl(
        state,
        identity,
        database_id,
        DdlAction::ChangeObject,
        collection,
        action,
    )
}

/// Guard the drop of an existing collection or object that shares its name
/// (`DropObject`).
pub(crate) fn require_drop_permission(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    collection: &str,
    action: &str,
) -> Result<(), DdlError> {
    authorize_ddl(
        state,
        identity,
        database_id,
        DdlAction::DropObject,
        collection,
        action,
    )
}

/// Guard a statement that affects the whole tenant (`TenantWide`).
pub(crate) fn require_tenant_wide(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    action: &str,
) -> Result<(), DdlError> {
    authorize_ddl(
        state,
        identity,
        database_id,
        DdlAction::TenantWide,
        "",
        action,
    )
}
