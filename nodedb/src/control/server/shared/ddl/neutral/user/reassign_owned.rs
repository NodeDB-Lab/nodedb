// SPDX-License-Identifier: BUSL-1.1

//! Owner reassignment + grant sweep for `DROP USER`.
//!
//! Dropping a user that owns catalog objects, or that has grants made
//! *to* it, would leave dangling references behind:
//!
//! - every owned object's `StoredOwner` row (and its in-band `.owner`
//!   field) still names the deleted user — the boot integrity verifier
//!   flags each as `DanglingReference { from_kind: "owner" }`;
//! - every `StoredPermission` granted to the user still names it as
//!   `grantee` — flagged as `DanglingReference { from_kind:
//!   "permission" }`.
//!
//! Either class of dangling reference makes the boot catalog sanity
//! check reject startup with no repair path — a permanently unbootable
//! data directory. This module rewrites every owned object to the
//! tenant admin and revokes every grant made to the user *before* the
//! user row is removed. It is fail-closed: if any reassignment or
//! revoke fails the error propagates and the caller must NOT delete the
//! user (a partially-reassigned + deleted user is the very dangling-ref
//! bug this prevents).
//!
//! The per-kind reassignment match is exhaustive over every
//! owner-bearing object kind and carries no catch-all arm, so adding a
//! new owner-bearing kind is a compile error here until it is wired —
//! and an owner row whose `object_type` maps to no known kind is a hard
//! `DROP USER` error rather than a silently-skipped dangling reference.

use crate::control::catalog_entry::CatalogEntry;
use crate::control::metadata_proposer::propose_catalog_entry;
use crate::control::security::catalog::auth_types::object_type;
use crate::control::security::catalog::{StoredOwner, SystemCatalog};
use crate::control::state::SharedState;
use crate::types::TenantId;

use super::super::super::result::DdlError;

/// Every catalog object kind that carries an owner reference. The nine
/// parent-replicated kinds each own a primary `Stored*` record with an
/// in-band `owner` field; `Index` is the standalone path (a bare
/// `StoredOwner` row with no parent record).
enum OwnerKind {
    Collection,
    Function,
    Procedure,
    Trigger,
    MaterializedView,
    Sequence,
    Schedule,
    ChangeStream,
    ContinuousAggregate,
    Index,
}

impl OwnerKind {
    /// Map a persisted `StoredOwner.object_type` to its kind. `None`
    /// means the writer of that owner row introduced a kind this module
    /// does not yet handle — the caller turns that into a hard error so
    /// the drop fails closed instead of leaking a dangling reference.
    fn from_object_type(s: &str) -> Option<Self> {
        Some(match s {
            object_type::COLLECTION => Self::Collection,
            object_type::FUNCTION => Self::Function,
            object_type::PROCEDURE => Self::Procedure,
            object_type::TRIGGER => Self::Trigger,
            object_type::MATERIALIZED_VIEW => Self::MaterializedView,
            object_type::SEQUENCE => Self::Sequence,
            object_type::SCHEDULE => Self::Schedule,
            object_type::CHANGE_STREAM => Self::ChangeStream,
            object_type::CONTINUOUS_AGGREGATE => Self::ContinuousAggregate,
            object_type::INDEX => Self::Index,
            _ => return None,
        })
    }

    fn as_object_type(&self) -> &'static str {
        match self {
            Self::Collection => object_type::COLLECTION,
            Self::Function => object_type::FUNCTION,
            Self::Procedure => object_type::PROCEDURE,
            Self::Trigger => object_type::TRIGGER,
            Self::MaterializedView => object_type::MATERIALIZED_VIEW,
            Self::Sequence => object_type::SEQUENCE,
            Self::Schedule => object_type::SCHEDULE,
            Self::ChangeStream => object_type::CHANGE_STREAM,
            Self::ContinuousAggregate => object_type::CONTINUOUS_AGGREGATE,
            Self::Index => object_type::INDEX,
        }
    }
}

/// Reassign every object owned by `username` (within `user_tenant`) to the
/// tenant's validated ownership fallback, then revoke every grant made to the
/// user. A fallback is required only when owned objects exist, allowing tenant
/// teardown to remove an object-free final admin. Returns the selected target.
pub(super) fn reassign_owned_and_sweep_grants(
    state: &SharedState,
    username: &str,
    user_tenant: TenantId,
) -> Result<Option<String>, DdlError> {
    let catalog = state.credentials.catalog();

    let owned = catalog
        .owners_for_user(username, user_tenant.as_u64())
        .map_err(|e| ddl_err(format!("load owner rows: {e}")))?;
    if owned.is_empty() {
        sweep_grants(state, catalog, username)?;
        return Ok(None);
    }
    let admin_name = catalog
        .resolve_ownership_fallback(user_tenant.as_u64(), username)
        .map_err(|e| ddl_err(format!("resolve ownership fallback: {e}")))?
        .ok_or_else(|| DdlError {
            sqlstate: "55000".to_string(),
            message: format!(
                "cannot drop user '{username}': tenant {} has no active administrative \
                 principal available for ownership reassignment",
                user_tenant.as_u64()
            ),
        })?;
    for owner in &owned {
        let kind = OwnerKind::from_object_type(&owner.object_type).ok_or_else(|| {
            ddl_err(format!(
                "cannot reassign object of unknown owner type '{}' ('{}') owned by \
                 '{username}' — refusing to drop user to avoid a dangling owner reference",
                owner.object_type, owner.object_name
            ))
        })?;
        reassign_one(
            state,
            catalog,
            kind,
            owner.database_id,
            user_tenant,
            &owner.object_name,
            &admin_name,
        )?;
    }

    sweep_grants(state, catalog, username)?;
    Ok(Some(admin_name))
}

/// Reassign a single owned object to `admin_name`. Re-proposes the
/// object's `Put<Kind>` catalog entry with the rewritten in-band owner
/// so followers converge; in single-node mode (`log_index == 0`) the
/// primary row, the `StoredOwner` row, and the in-memory owner map are
/// all rewritten directly, since no raft apply runs to do it.
fn reassign_one(
    state: &SharedState,
    catalog: &SystemCatalog,
    kind: OwnerKind,
    database_id: u64,
    tenant: TenantId,
    name: &str,
    admin_name: &str,
) -> Result<(), DdlError> {
    let tenant_id = tenant.as_u64();
    let object_type = kind.as_object_type();
    match kind {
        OwnerKind::Collection => {
            let database_id = nodedb_types::DatabaseId::new(database_id);
            let mut stored = catalog
                .get_collection(database_id, tenant_id, name)
                .map_err(|e| ddl_err(format!("get collection '{name}': {e}")))?
                .ok_or_else(|| missing(object_type, name))?;
            stored.owner = admin_name.to_string();
            let entry = CatalogEntry::PutCollection(Box::new(stored.clone()));
            if propose(state, &entry)? == 0 {
                catalog
                    .put_collection(database_id, &stored)
                    .map_err(|e| ddl_err(format!("put collection '{name}': {e}")))?;
                persist_owner_local_in_database(
                    state,
                    catalog,
                    object_type,
                    database_id.as_u64(),
                    tenant_id,
                    name,
                    admin_name,
                )?;
            }
        }
        OwnerKind::Function => {
            let mut s = catalog
                .get_function(tenant_id, name)
                .map_err(|e| ddl_err(format!("get function '{name}': {e}")))?
                .ok_or_else(|| missing(object_type, name))?;
            s.owner = admin_name.to_string();
            let entry = CatalogEntry::PutFunction(Box::new(s.clone()));
            if propose(state, &entry)? == 0 {
                catalog
                    .put_function(&s)
                    .map_err(|e| ddl_err(format!("put function '{name}': {e}")))?;
                persist_owner_local(state, catalog, object_type, tenant_id, name, admin_name)?;
            }
        }
        OwnerKind::Procedure => {
            let mut s = catalog
                .get_procedure(tenant_id, name)
                .map_err(|e| ddl_err(format!("get procedure '{name}': {e}")))?
                .ok_or_else(|| missing(object_type, name))?;
            s.owner = admin_name.to_string();
            let entry = CatalogEntry::PutProcedure(Box::new(s.clone()));
            if propose(state, &entry)? == 0 {
                catalog
                    .put_procedure(&s)
                    .map_err(|e| ddl_err(format!("put procedure '{name}': {e}")))?;
                persist_owner_local(state, catalog, object_type, tenant_id, name, admin_name)?;
            }
        }
        OwnerKind::Trigger => {
            let mut s = catalog
                .get_trigger(tenant_id, name)
                .map_err(|e| ddl_err(format!("get trigger '{name}': {e}")))?
                .ok_or_else(|| missing(object_type, name))?;
            s.owner = admin_name.to_string();
            let entry = CatalogEntry::PutTrigger(Box::new(s.clone()));
            if propose(state, &entry)? == 0 {
                catalog
                    .put_trigger(&s)
                    .map_err(|e| ddl_err(format!("put trigger '{name}': {e}")))?;
                persist_owner_local(state, catalog, object_type, tenant_id, name, admin_name)?;
            }
        }
        OwnerKind::MaterializedView => {
            let mut s = catalog
                .get_materialized_view(tenant_id, name)
                .map_err(|e| ddl_err(format!("get materialized_view '{name}': {e}")))?
                .ok_or_else(|| missing(object_type, name))?;
            s.owner = admin_name.to_string();
            let entry = CatalogEntry::PutMaterializedView(Box::new(s.clone()));
            if propose(state, &entry)? == 0 {
                catalog
                    .put_materialized_view(&s)
                    .map_err(|e| ddl_err(format!("put materialized_view '{name}': {e}")))?;
                persist_owner_local(state, catalog, object_type, tenant_id, name, admin_name)?;
            }
        }
        OwnerKind::Sequence => {
            let mut s = catalog
                .get_sequence(tenant_id, name)
                .map_err(|e| ddl_err(format!("get sequence '{name}': {e}")))?
                .ok_or_else(|| missing(object_type, name))?;
            s.owner = admin_name.to_string();
            let entry = CatalogEntry::PutSequence(Box::new(s.clone()));
            if propose(state, &entry)? == 0 {
                catalog
                    .put_sequence(&s)
                    .map_err(|e| ddl_err(format!("put sequence '{name}': {e}")))?;
                persist_owner_local(state, catalog, object_type, tenant_id, name, admin_name)?;
            }
        }
        OwnerKind::Schedule => {
            // Schedules have no single-key getter; find within the tenant.
            let mut s = catalog
                .load_all_schedules()
                .map_err(|e| ddl_err(format!("load schedules: {e}")))?
                .into_iter()
                .find(|d| d.tenant_id == tenant_id && d.name == name)
                .ok_or_else(|| missing(object_type, name))?;
            s.owner = admin_name.to_string();
            let entry = CatalogEntry::PutSchedule(Box::new(s.clone()));
            if propose(state, &entry)? == 0 {
                catalog
                    .put_schedule(&s)
                    .map_err(|e| ddl_err(format!("put schedule '{name}': {e}")))?;
                persist_owner_local(state, catalog, object_type, tenant_id, name, admin_name)?;
            }
        }
        OwnerKind::ChangeStream => {
            let mut s = catalog
                .get_change_stream(tenant_id, name)
                .map_err(|e| ddl_err(format!("get change_stream '{name}': {e}")))?
                .ok_or_else(|| missing(object_type, name))?;
            s.owner = admin_name.to_string();
            let entry = CatalogEntry::PutChangeStream(Box::new(s.clone()));
            if propose(state, &entry)? == 0 {
                catalog
                    .put_change_stream(&s)
                    .map_err(|e| ddl_err(format!("put change_stream '{name}': {e}")))?;
                persist_owner_local(state, catalog, object_type, tenant_id, name, admin_name)?;
            }
        }
        OwnerKind::ContinuousAggregate => {
            let mut stored = catalog
                .get_continuous_aggregate(database_id, tenant_id, name)
                .map_err(|e| ddl_err(format!("get continuous_aggregate '{name}': {e}")))?
                .ok_or_else(|| missing(object_type, name))?;
            stored.owner = admin_name.to_string();
            let entry = CatalogEntry::PutContinuousAggregate(Box::new(stored.clone()));
            if propose(state, &entry)? == 0 {
                catalog
                    .put_continuous_aggregate(&stored)
                    .map_err(|e| ddl_err(format!("put continuous_aggregate '{name}': {e}")))?;
                persist_owner_local_in_database(
                    state,
                    catalog,
                    object_type,
                    database_id,
                    tenant_id,
                    name,
                    admin_name,
                )?;
            }
        }
        OwnerKind::Index => {
            // Standalone owner row — the `StoredOwner` row is the whole
            // object, so there is no parent primary to re-propose.
            let stored = StoredOwner {
                database_id,
                object_type: object_type.to_string(),
                object_name: name.to_string(),
                tenant_id,
                owner_username: admin_name.to_string(),
            };
            let entry = CatalogEntry::PutOwner(Box::new(stored.clone()));
            if propose(state, &entry)? == 0 {
                persist_owner_local_in_database(
                    state,
                    catalog,
                    object_type,
                    database_id,
                    tenant_id,
                    name,
                    admin_name,
                )?;
            }
        }
    }
    Ok(())
}

/// Revoke every grant whose grantee is the dropped user, so no
/// `permission.grantee → user` reference outlives the user row.
fn sweep_grants(
    state: &SharedState,
    catalog: &SystemCatalog,
    username: &str,
) -> Result<(), DdlError> {
    let grantee = format!("user:{username}");
    let grants = catalog
        .load_all_permissions()
        .map_err(|e| ddl_err(format!("load permissions: {e}")))?;
    for grant in grants.into_iter().filter(|grant| grant.grantee == grantee) {
        let entry = CatalogEntry::DeletePermission {
            target: grant.target.clone(),
            grantee: grantee.clone(),
            permission: grant.permission.clone(),
        };
        if propose(state, &entry)? == 0 {
            catalog
                .delete_permission(&grant.target, &grantee, &grant.permission)
                .map_err(|e| ddl_err(format!("delete permission on '{}': {e}", grant.target)))?;
            state
                .permissions
                .install_replicated_revoke(&grant.target, &grantee, &grant.permission);
        }
    }
    Ok(())
}

/// Rewrite the persistent `StoredOwner` row and the in-memory owner map
/// for a parent-replicated object in single-node mode. In cluster mode
/// the raft apply of the `Put<Kind>` entry does this on every node.
fn persist_owner_local(
    state: &SharedState,
    catalog: &SystemCatalog,
    object_type: &str,
    tenant_id: u64,
    name: &str,
    admin_name: &str,
) -> Result<(), DdlError> {
    persist_owner_local_in_database(state, catalog, object_type, 0, tenant_id, name, admin_name)
}

fn persist_owner_local_in_database(
    state: &SharedState,
    catalog: &SystemCatalog,
    object_type: &str,
    database_id: u64,
    tenant_id: u64,
    name: &str,
    admin_name: &str,
) -> Result<(), DdlError> {
    catalog
        .rewrite_object_owner(object_type, database_id, tenant_id, name, admin_name)
        .map_err(|e| ddl_err(format!("rewrite owner for {object_type} '{name}': {e}")))?;
    state.permissions.install_replicated_owner(&StoredOwner {
        database_id,
        object_type: object_type.to_string(),
        object_name: name.to_string(),
        tenant_id,
        owner_username: admin_name.to_string(),
    });
    Ok(())
}

fn propose(state: &SharedState, entry: &CatalogEntry) -> Result<u64, DdlError> {
    propose_catalog_entry(state, entry).map_err(|e| ddl_err(format!("metadata propose: {e}")))
}

fn missing(object_type: &str, name: &str) -> DdlError {
    ddl_err(format!(
        "owned {object_type} '{name}' has an owner row but no primary record — \
         cannot reassign; refusing to drop user"
    ))
}

fn ddl_err(message: String) -> DdlError {
    DdlError {
        sqlstate: "XX000".to_string(),
        message,
    }
}
