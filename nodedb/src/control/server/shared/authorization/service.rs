//! Authorization evaluation for fully-planned physical tasks.

use nodedb_physical::physical_task::PhysicalTask;
use nodedb_types::DatabaseId;

use crate::control::security::audit::{
    AuditEmitContext, AuditEmitter, AuditEvent, NoopAuditEmitter,
};
use crate::control::security::catalog::SystemCatalog;
use crate::control::security::identity::{
    AuthenticatedIdentity, Permission, Role, role_grants_permission,
};
use crate::control::security::permission::PermissionStore;
use crate::control::security::role::RoleStore;
use crate::control::target_identity::bare_collection_name;
use crate::types::TenantId;

use super::capability::{AuthorizedCollection, AuthorizedTaskSet};
use super::error::AuthorizationError;
use super::requirements::{AuthorizationRequirement, plan_requirements};

/// Ensure an identity can select `database_id`.
pub fn authorize_database(
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    emitter: &dyn AuditEmitter,
) -> Result<(), AuthorizationError> {
    if identity.can_access_database(database_id) {
        return Ok(());
    }

    deny(
        identity,
        emitter,
        format!(
            "permission denied for database: user '{}' does not have access to {}",
            identity.username,
            database_id.as_u64()
        ),
    )
}

/// Authorize a database-scoped permission before mutating its catalog.
///
/// Database grants use the catalog's persisted privilege names. Built-in and
/// database-scoped roles are evaluated against the requested database, so a
/// role for another database cannot authorize this operation.
pub fn authorize_database_permission(
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    permission: Permission,
    catalog: &SystemCatalog,
    emitter: &dyn AuditEmitter,
) -> Result<(), AuthorizationError> {
    authorize_database(identity, database_id, emitter)?;

    let scoped_identity = identity_for_database(identity, database_id);
    if scoped_identity
        .roles
        .iter()
        .any(|role| role_grants_permission(role, permission))
    {
        return Ok(());
    }

    let privilege = match permission {
        Permission::Create => "CREATE_COLLECTION",
        Permission::Read => "SELECT",
        Permission::Write
        | Permission::Drop
        | Permission::Alter
        | Permission::Admin
        | Permission::Monitor
        | Permission::Execute
        | Permission::Backup => {
            return deny_database(
                identity,
                database_id,
                emitter,
                format!(
                    "permission denied: database grants do not support {:?} on database {}",
                    permission,
                    database_id.as_u64()
                ),
            );
        }
    };

    match catalog.has_database_grant(database_id, identity.user_id, privilege) {
        Ok(true) => Ok(()),
        Ok(false) => deny_database(
            identity,
            database_id,
            emitter,
            format!(
                "permission denied: user '{}' lacks {:?} permission on database {}",
                identity.username,
                permission,
                database_id.as_u64()
            ),
        ),
        Err(error) => deny_database(
            identity,
            database_id,
            emitter,
            format!(
                "permission denied: unable to verify {:?} permission on database {}: {error}",
                permission,
                database_id.as_u64()
            ),
        ),
    }
}

/// Authorize one collection operation before work that precedes physical planning.
///
/// Trigger-capable DML uses this early gate to prevent unauthorized callers from
/// firing triggers or consuming sequence values. The final planned task set must
/// still be authorized separately because it can contain additional resources.
pub fn authorize_collection(
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    collection: &str,
    permission: Permission,
    permissions: &PermissionStore,
    roles: &RoleStore,
    emitter: &dyn AuditEmitter,
) -> Result<(), AuthorizationError> {
    authorize_database(identity, database_id, emitter)?;
    authorize_collection_requirement(
        identity,
        database_id,
        collection,
        permission,
        permissions,
        roles,
        emitter,
    )
}

/// Mint a collection-scoped capability for a non-physical side effect.
pub fn authorize_collection_capability(
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    collection: &str,
    permission: Permission,
    permissions: &PermissionStore,
    roles: &RoleStore,
    emitter: &dyn AuditEmitter,
) -> Result<AuthorizedCollection, AuthorizationError> {
    authorize_collection(
        identity,
        database_id,
        collection,
        permission,
        permissions,
        roles,
        emitter,
    )?;
    Ok(AuthorizedCollection::new(
        identity.tenant_id,
        database_id,
        collection,
        permission,
    ))
}

/// Authorize an entire physical task set before any task is dispatched.
///
/// Every task must belong to the authenticated tenant and selected database.
/// A plan without a collection target is checked at tenant scope rather than
/// being silently allowed.
pub fn authorize_task_set(
    identity: &AuthenticatedIdentity,
    tasks: &[PhysicalTask],
    permissions: &PermissionStore,
    roles: &RoleStore,
    emitter: &dyn AuditEmitter,
) -> Result<AuthorizedTaskSet, AuthorizationError> {
    for task in tasks {
        if task.tenant_id != identity.tenant_id && !identity.is_superuser {
            return deny(
                identity,
                emitter,
                format!(
                    "permission denied: task tenant {} is outside authenticated tenant",
                    task.tenant_id.as_u64()
                ),
            );
        }
        authorize_database(identity, task.database_id, emitter)?;
    }

    for task in tasks {
        let requirements = plan_requirements(&task.plan);
        if requirements.is_empty() {
            authorize_tenant_permission(
                identity,
                task.tenant_id,
                task.database_id,
                crate::control::security::identity::required_permission(&task.plan),
                permissions,
                roles,
                emitter,
            )?;
            continue;
        }
        for requirement in requirements {
            match requirement {
                AuthorizationRequirement::Collection {
                    collection,
                    permission,
                } => authorize_collection_requirement(
                    identity,
                    task.database_id,
                    &collection,
                    permission,
                    permissions,
                    roles,
                    emitter,
                )?,
                AuthorizationRequirement::Tenant { permission } => authorize_tenant_permission(
                    identity,
                    task.tenant_id,
                    task.database_id,
                    permission,
                    permissions,
                    roles,
                    emitter,
                )?,
            }
        }
    }
    Ok(AuthorizedTaskSet::new(tasks, identity))
}

fn authorize_collection_requirement(
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    collection: &str,
    permission: Permission,
    permissions: &PermissionStore,
    roles: &RoleStore,
    emitter: &dyn AuditEmitter,
) -> Result<(), AuthorizationError> {
    // Physical plans prefix non-default-database collection names, whereas
    // grants and ownership use the unqualified collection name.
    let grant_name = bare_collection_name(database_id, collection);
    if !identity.is_superuser && is_system_collection(&grant_name) {
        return deny(
            identity,
            emitter,
            format!("permission denied: system catalog access requires superuser ({collection})"),
        );
    }

    // PermissionStore's built-in role check is intentionally retained for
    // ownership, grants, and custom-role inheritance. Filter database-scoped
    // roles first because its legacy collection target has no database field.
    let scoped_identity = identity_for_database(identity, database_id);
    if permissions.check(
        &scoped_identity,
        permission,
        database_id,
        &grant_name,
        roles,
        &NoopAuditEmitter,
    ) {
        return Ok(());
    }

    deny(
        identity,
        emitter,
        format!(
            "permission denied: user '{}' lacks {:?} permission on '{}'",
            identity.username, permission, collection
        ),
    )
}

fn is_system_collection(collection: &str) -> bool {
    collection
        .get(.."_system".len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("_system"))
}

fn authorize_tenant_permission(
    identity: &AuthenticatedIdentity,
    tenant_id: TenantId,
    database_id: DatabaseId,
    permission: Permission,
    permissions: &PermissionStore,
    roles: &RoleStore,
    emitter: &dyn AuditEmitter,
) -> Result<(), AuthorizationError> {
    let scoped_identity = identity_for_database(identity, database_id);
    if permissions.check_tenant(
        &scoped_identity,
        permission,
        tenant_id,
        roles,
        &NoopAuditEmitter,
    ) {
        return Ok(());
    }
    deny(
        identity,
        emitter,
        format!(
            "permission denied: user '{}' lacks {:?} permission on tenant {}",
            identity.username,
            permission,
            tenant_id.as_u64()
        ),
    )
}

fn identity_for_database(
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
) -> AuthenticatedIdentity {
    let mut scoped = identity.clone();
    scoped.roles.retain(|role| match role {
        Role::DatabaseOwner(role_database)
        | Role::DatabaseEditor(role_database)
        | Role::DatabaseReader(role_database) => *role_database == database_id,
        Role::Superuser
        | Role::ClusterAdmin
        | Role::TenantAdmin
        | Role::ReadWrite
        | Role::ReadOnly
        | Role::Monitor
        | Role::Custom(_) => true,
    });
    scoped
}

fn deny<T>(
    identity: &AuthenticatedIdentity,
    emitter: &dyn AuditEmitter,
    detail: String,
) -> Result<T, AuthorizationError> {
    emit_denial(identity, None, emitter, &detail);
    Err(AuthorizationError::new(identity.tenant_id, detail))
}

fn deny_database<T>(
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    emitter: &dyn AuditEmitter,
    detail: String,
) -> Result<T, AuthorizationError> {
    emit_denial(identity, Some(database_id), emitter, &detail);
    Err(AuthorizationError::new(identity.tenant_id, detail))
}

fn emit_denial(
    identity: &AuthenticatedIdentity,
    database_id: Option<DatabaseId>,
    emitter: &dyn AuditEmitter,
    detail: &str,
) {
    emitter.emit(
        AuditEvent::PermissionDenied,
        &identity.username,
        detail,
        AuditEmitContext {
            tenant_id: Some(identity.tenant_id),
            database_id,
            auth_user_id: &identity.user_id.to_string(),
            auth_user_name: &identity.username,
        },
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::envelope::PhysicalPlan;
    use crate::control::security::identity::{AuthMethod, DatabaseSet};
    use crate::types::VShardId;
    use nodedb_physical::physical_plan::KvOp;

    fn identity(roles: Vec<Role>, databases: DatabaseSet) -> AuthenticatedIdentity {
        AuthenticatedIdentity::new_regular(
            7,
            "reader",
            TenantId::new(9),
            AuthMethod::Trust,
            roles,
            None,
            databases,
        )
    }

    fn read_task() -> PhysicalTask {
        PhysicalTask {
            tenant_id: TenantId::new(9),
            database_id: DatabaseId::DEFAULT,
            vshard_id: VShardId::new(0),
            plan: PhysicalPlan::Kv(KvOp::Get {
                collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, "orders"),
                key: Vec::new(),
                rls_filters: Vec::new(),
                surrogate_ceiling: None,
                system_time: nodedb_types::SystemTimeScope::Current,
            }),
            post_set_op: nodedb_physical::physical_task::PostSetOp::None,
            txn_id: None,
        }
    }

    #[test]
    fn database_scope_denial_is_typed() {
        let id = identity(
            Vec::new(),
            DatabaseSet::Some(smallvec::smallvec![DatabaseId::DEFAULT]),
        );
        let error = authorize_database(&id, DatabaseId::new(2), &NoopAuditEmitter)
            .expect_err("database outside identity scope must be denied");
        assert!(error.resource().contains("database"));
    }

    #[test]
    fn explicit_collection_grant_is_accepted() {
        let permissions = PermissionStore::new();
        let roles = RoleStore::new();
        permissions
            .grant(
                "collection:0:9:orders",
                "user:reader",
                Permission::Read,
                "admin",
                None,
            )
            .expect("in-memory grant must succeed");
        let id = identity(
            Vec::new(),
            DatabaseSet::Some(smallvec::smallvec![DatabaseId::DEFAULT]),
        );

        assert!(
            authorize_task_set(&id, &[read_task()], &permissions, &roles, &NoopAuditEmitter,)
                .is_ok()
        );
    }

    #[test]
    fn task_set_fails_closed_when_a_resource_is_missing_permission() {
        let id = identity(
            Vec::new(),
            DatabaseSet::Some(smallvec::smallvec![DatabaseId::DEFAULT]),
        );
        assert!(
            authorize_task_set(
                &id,
                &[read_task()],
                &PermissionStore::new(),
                &RoleStore::new(),
                &NoopAuditEmitter,
            )
            .is_err()
        );
    }

    #[test]
    fn system_collection_and_wrong_database_role_are_denied() {
        let permissions = PermissionStore::new();
        let roles = RoleStore::new();
        let id = identity(
            vec![Role::DatabaseReader(DatabaseId::new(3))],
            DatabaseSet::Some(smallvec::smallvec![DatabaseId::new(3), DatabaseId::new(4)]),
        );
        assert!(
            authorize_collection_requirement(
                &id,
                DatabaseId::new(3),
                "_SyStEm.audit_log",
                Permission::Read,
                &permissions,
                &roles,
                &NoopAuditEmitter,
            )
            .is_err()
        );
        assert!(
            authorize_collection_requirement(
                &id,
                DatabaseId::new(4),
                "orders",
                Permission::Read,
                &permissions,
                &roles,
                &NoopAuditEmitter,
            )
            .is_err()
        );
    }
}
