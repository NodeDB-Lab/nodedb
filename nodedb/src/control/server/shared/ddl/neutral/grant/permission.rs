// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `GRANT/REVOKE <perm> ON <object> TO/FROM <grantee>`
//! handlers.
//!
//! Ported from the pgwire `ddl::grant::permission` handlers. All non-return
//! logic (grantee canonicalization, target resolution incl. the cross-tenant
//! superuser gate, `ALL` expansion, tenant-admin gate, `prepare_permission`,
//! catalog propose + single-node fallback, `install_replicated_permission` /
//! `install_replicated_revoke`, and `audit_record`) is preserved verbatim;
//! only the result construction changed from pgwire `Response` / `PgWireError`
//! to the protocol-neutral [`DdlResult`] / [`DdlError`].
//!
//! Proposes `CatalogEntry::{PutPermission, DeletePermission}` so every
//! follower's `PermissionStore` and `OWNERS` redb stay in sync.

use crate::control::catalog_entry::CatalogEntry;
use crate::control::metadata_proposer::propose_catalog_entry;
use crate::control::security::audit::AuditEvent;
use crate::control::security::identity::{AuthenticatedIdentity, Permission, Role};
use crate::control::security::permission::{
    format_permission, function_target, parse_permission, procedure_target, tenant_target,
};
use crate::control::server::shared::ddl::sql_parse::{parse_ident_token, parse_relation_token};
use crate::control::state::SharedState;
use crate::types::TenantId;

use super::super::super::result::{DdlError, DdlResult};
use super::support::{require_tenant_admin, status};

/// Resolve a raw grantee token from the AST into its canonical grant-store
/// form: `user:<name>` for users, the bare role name for roles. Rejects
/// names that resolve to neither, so unresolved typos don't sink into the
/// store as silently unenforceable rows.
fn canonicalize_grantee(state: &SharedState, raw: &str) -> Result<String, DdlError> {
    // Users and roles the statement sees: created earlier in the
    // transaction counts, dropped earlier in it does not.
    if super::super::role_checks::visible_user(state, raw).is_some() {
        return Ok(format!("user:{raw}"));
    }
    let parsed: Role = match raw.parse() {
        Ok(r) => r,
        Err(e) => match e {},
    };
    let is_known_role = match &parsed {
        Role::Custom(name) => super::super::role_checks::visible_roles(state).contains_key(name),
        _ => true,
    };
    if is_known_role {
        return Ok(parsed.to_string());
    }
    Err(DdlError::new(
        "42704",
        format!("grantee '{raw}' is neither a user nor a role"),
    ))
}

fn propose_grant(
    state: &SharedState,
    target: &str,
    grantee: &str,
    perm: Permission,
    granted_by: &str,
) -> Result<(), DdlError> {
    let stored = state
        .permissions
        .prepare_permission(target, grantee, perm, granted_by);
    let entry = CatalogEntry::PutPermission(Box::new(stored.clone()));
    let outcome = propose_catalog_entry(state, &entry)
        .map_err(|e| DdlError::new("XX000", format!("metadata propose: {e}")))?;
    if outcome.needs_local_apply() {
        {
            let catalog = state.credentials.catalog();
            catalog
                .put_permission(&stored)
                .map_err(|e| DdlError::new("XX000", format!("catalog write: {e}")))?;
        }
        state.permissions.install_replicated_permission(&stored);
    }
    Ok(())
}

fn propose_revoke(
    state: &SharedState,
    target: &str,
    grantee: &str,
    perm: Permission,
) -> Result<(), DdlError> {
    let perm_str = format_permission(perm);
    let entry = CatalogEntry::DeletePermission {
        target: target.to_string(),
        grantee: grantee.to_string(),
        permission: perm_str.clone(),
    };
    let outcome = propose_catalog_entry(state, &entry)
        .map_err(|e| DdlError::new("XX000", format!("metadata propose: {e}")))?;
    if outcome.needs_local_apply() {
        {
            let catalog = state.credentials.catalog();
            catalog
                .delete_permission(target, grantee, &perm_str)
                .map_err(|e| DdlError::new("XX000", format!("catalog write: {e}")))?;
        }
        state
            .permissions
            .install_replicated_revoke(target, grantee, &perm_str);
    }
    Ok(())
}

/// Resolve a tenant name (or numeric id) to its `TenantId`.
///
/// A token that parses as an integer is taken as a literal tenant id;
/// otherwise the catalog is consulted for a tenant with a matching name.
fn resolve_tenant_id(state: &SharedState, name: &str) -> Result<TenantId, DdlError> {
    if let Ok(id) = name.parse::<u64>() {
        return Ok(TenantId::new(id));
    }
    let catalog = state.credentials.catalog();
    let tenants = catalog
        .load_all_tenants()
        .map_err(|e| DdlError::new("XX000", format!("tenant lookup: {e}")))?;
    tenants
        .into_iter()
        .find(|t| t.name.eq_ignore_ascii_case(name))
        .map(|t| TenantId::new(t.tenant_id))
        .ok_or_else(|| DdlError::new("42704", format!("tenant '{name}' does not exist")))
}

/// Resolve a `(target_type, target_name)` pair into the canonical grant
/// target string and a human-readable object description for audit logs.
///
/// `target_type` is `FUNCTION`, `PROCEDURE`, `TENANT`, or anything else
/// (treated as a collection).
fn resolve_target(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    target_type: &str,
    target_name: &str,
) -> Result<(String, String), DdlError> {
    if target_type.eq_ignore_ascii_case("FUNCTION") {
        let name = parse_ident_token(target_name)?;
        Ok((
            function_target(identity.tenant_id, &name),
            format!("function '{name}'"),
        ))
    } else if target_type.eq_ignore_ascii_case("PROCEDURE") {
        let name = parse_ident_token(target_name)?;
        Ok((
            procedure_target(identity.tenant_id, &name),
            format!("procedure '{name}'"),
        ))
    } else if target_type.eq_ignore_ascii_case("TENANT") {
        let tenant_id = resolve_tenant_id(state, target_name)?;
        // A tenant admin may only manage grants within their own tenant;
        // granting across tenant boundaries requires superuser.
        if tenant_id != identity.tenant_id && !identity.is_superuser {
            return Err(DdlError::new(
                "42501",
                "permission denied: managing permissions on another tenant requires superuser",
            ));
        }
        Ok((tenant_target(tenant_id), format!("tenant '{target_name}'")))
    } else {
        // A grant target can name a system catalog relation, so the qualified
        // `_system.` / `pg_catalog.` forms resolve here the way a SELECT
        // resolves them.
        let name = parse_relation_token(target_name)?;
        Ok((
            format!("collection:{}:{name}", identity.tenant_id.as_u64()),
            format!("collection '{name}'"),
        ))
    }
}

/// Resolve a list of permission tokens into concrete `Permission` values,
/// expanding `ALL`. Returns a typed error for any unknown token.
fn resolve_permissions(permissions: &[String]) -> Result<Vec<Permission>, DdlError> {
    let mut out = Vec::new();
    for p in permissions {
        if p.eq_ignore_ascii_case("ALL") {
            out.extend([
                Permission::Read,
                Permission::Write,
                Permission::Create,
                Permission::Drop,
                Permission::Alter,
            ]);
        } else {
            let perm = parse_permission(p)
                .ok_or_else(|| DdlError::new("42601", format!("unknown permission: {p}")))?;
            out.push(perm);
        }
    }
    Ok(out)
}

/// `GRANT <perm>[, ...] ON <collection|FUNCTION|PROCEDURE|TENANT name> TO <grantee>`
///
/// Called with typed fields from the AST router.
pub fn grant_permission(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    permissions: &[String],
    target_type: &str,
    target_name: &str,
    grantee: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let (target, object_desc) = resolve_target(state, identity, target_type, target_name)?;

    require_tenant_admin(identity, "grant permissions")?;

    let perms = resolve_permissions(permissions)?;
    let canonical = canonicalize_grantee(state, grantee)?;

    for perm in &perms {
        propose_grant(state, &target, &canonical, *perm, &identity.username)?;
    }

    state.audit_record(
        AuditEvent::PrivilegeChange,
        Some(identity.tenant_id),
        &identity.username,
        &format!(
            "granted {} on {object_desc} to '{grantee}'",
            permissions.join(", ")
        ),
    );

    Ok(status("GRANT"))
}

/// `REVOKE <perm>[, ...] ON <collection|FUNCTION|PROCEDURE|TENANT name> FROM <grantee>`
///
/// Called with typed fields from the AST router.
pub fn revoke_permission(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    permissions: &[String],
    target_type: &str,
    target_name: &str,
    grantee: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let (target, object_desc) = resolve_target(state, identity, target_type, target_name)?;

    require_tenant_admin(identity, "revoke permissions")?;

    let perms = resolve_permissions(permissions)?;
    let canonical = canonicalize_grantee(state, grantee)?;

    for perm in &perms {
        propose_revoke(state, &target, &canonical, *perm)?;
    }

    state.audit_record(
        AuditEvent::PrivilegeChange,
        Some(identity.tenant_id),
        &identity.username,
        &format!(
            "revoked {} on {object_desc} from '{grantee}'",
            permissions.join(", ")
        ),
    );

    Ok(status("REVOKE"))
}
