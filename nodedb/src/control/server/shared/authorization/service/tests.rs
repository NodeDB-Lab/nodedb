use super::*;
use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::identity::{AuthMethod, DatabaseSet, Permission};
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
        authorize_collection_requirement(
            &id,
            DatabaseId::DEFAULT,
            "orders",
            Permission::Read,
            &permissions,
            &roles,
            &NoopAuditEmitter,
        )
        .is_ok()
    );
}

#[test]
fn task_set_fails_closed_when_a_resource_is_missing_permission() {
    let permissions = PermissionStore::new();
    let roles = RoleStore::new();
    let id = identity(
        Vec::new(),
        DatabaseSet::Some(smallvec::smallvec![DatabaseId::DEFAULT]),
    );
    let tasks = vec![PhysicalTask {
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
    }];

    assert!(authorize_task_set(&id, &tasks, &permissions, &roles, &NoopAuditEmitter).is_err());
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
