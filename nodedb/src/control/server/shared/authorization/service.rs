//! Authorization evaluation for fully-planned physical tasks.

use nodedb_physical::physical_task::PhysicalTask;
use nodedb_types::DatabaseId;

use crate::control::security::audit::{
    AuditEmitContext, AuditEmitter, AuditEvent, NoopAuditEmitter,
};
use crate::control::security::identity::{AuthenticatedIdentity, Permission, Role};
use crate::control::security::permission::PermissionStore;
use crate::control::security::role::RoleStore;
use crate::control::target_identity::bare_collection_name;
use crate::types::TenantId;

use super::error::AuthorizationError;
use super::requirements::{AuthorizationRequirement, plan_requirements};

/// Ensure an identity may select `database_id`.
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
) -> Result<(), AuthorizationError> {
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
    Ok(())
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
    emitter.emit(
        AuditEvent::PermissionDenied,
        &identity.username,
        &detail,
        AuditEmitContext::new(
            Some(identity.tenant_id),
            &identity.user_id.to_string(),
            &identity.username,
        ),
    );
    Err(AuthorizationError::new(identity.tenant_id, detail))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::envelope::PhysicalPlan;
    use crate::control::security::identity::{AuthMethod, DatabaseSet};
    use crate::types::VShardId;
    use nodedb_physical::physical_plan::KvOp;

    fn identity(roles: Vec<Role>, databases: DatabaseSet) -> AuthenticatedIdentity {
        AuthenticatedIdentity {
            user_id: 7,
            username: "reader".into(),
            tenant_id: TenantId::new(9),
            auth_method: AuthMethod::Trust,
            roles,
            is_superuser: false,
            default_database: None,
            accessible_databases: databases,
        }
    }

    fn read_task() -> PhysicalTask {
        PhysicalTask {
            tenant_id: TenantId::new(9),
            database_id: DatabaseId::DEFAULT,
            vshard_id: VShardId::new(0),
            plan: PhysicalPlan::Kv(KvOp::Get {
                collection: "orders".into(),
                key: Vec::new(),
                rls_filters: Vec::new(),
                surrogate_ceiling: None,
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
                "collection:9:orders",
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
