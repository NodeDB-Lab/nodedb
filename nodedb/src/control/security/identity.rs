use std::str::FromStr;

use crate::types::TenantId;

/// A verified identity bound to a session after authentication.
///
/// This is the single source of truth for "who is this connection?"
/// Created during auth handshake, immutable for the session lifetime.
/// Tenant ID comes from here — never from client payload.
#[derive(Debug, Clone)]
pub struct AuthenticatedIdentity {
    /// Unique user identifier.
    pub user_id: u64,
    /// Username (for display, logging, audit).
    pub username: String,
    /// Tenant this user belongs to.
    pub tenant_id: TenantId,
    /// How the user authenticated.
    pub auth_method: AuthMethod,
    /// Assigned roles.
    pub roles: Vec<Role>,
    /// Whether this user is a superuser (bypasses all permission checks).
    pub is_superuser: bool,
}

impl AuthenticatedIdentity {
    /// Check if this identity has a specific role.
    pub fn has_role(&self, role: &Role) -> bool {
        self.is_superuser || self.roles.contains(role)
    }

    /// Check if this identity has any of the specified roles.
    pub fn has_any_role(&self, roles: &[Role]) -> bool {
        self.is_superuser || roles.iter().any(|r| self.roles.contains(r))
    }
}

/// How the client proved their identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthMethod {
    /// SCRAM-SHA-256 via pgwire.
    ScramSha256,
    /// Cleartext password (dev/testing only).
    CleartextPassword,
    /// API key (bearer token).
    ApiKey,
    /// mTLS client certificate.
    Certificate,
    /// Trust mode (no authentication — dev only).
    Trust,
}

/// Built-in and custom roles.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Role {
    /// Full access to everything, all tenants, system catalog.
    Superuser,
    /// Full access within own tenant. Can manage users/roles.
    TenantAdmin,
    /// Read + write on granted collections.
    ReadWrite,
    /// Read-only on granted collections.
    ReadOnly,
    /// Read metrics, health, audit. No data access.
    Monitor,
    /// Custom role defined by user.
    Custom(String),
}

impl std::fmt::Display for Role {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Role::Superuser => write!(f, "superuser"),
            Role::TenantAdmin => write!(f, "tenant_admin"),
            Role::ReadWrite => write!(f, "readwrite"),
            Role::ReadOnly => write!(f, "readonly"),
            Role::Monitor => write!(f, "monitor"),
            Role::Custom(name) => write!(f, "{name}"),
        }
    }
}

impl FromStr for Role {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        Ok(match s {
            "superuser" => Role::Superuser,
            "tenant_admin" => Role::TenantAdmin,
            "readwrite" => Role::ReadWrite,
            "readonly" => Role::ReadOnly,
            "monitor" => Role::Monitor,
            other => Role::Custom(other.to_string()),
        })
    }
}

/// Permission types for RBAC.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Permission {
    /// SELECT, point_get, vector_search, range_scan, crdt_read, graph queries.
    Read,
    /// INSERT, UPDATE, DELETE, crdt_apply, vector_insert, edge_put.
    Write,
    /// CREATE COLLECTION, CREATE INDEX.
    Create,
    /// DROP COLLECTION, DROP INDEX.
    Drop,
    /// ALTER COLLECTION, schema changes, policy changes.
    Alter,
    /// GRANT/REVOKE, user management within scope.
    Admin,
    /// Read metrics, health checks, EXPLAIN, slow query log.
    Monitor,
}

/// What the permission applies to.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PermissionTarget {
    /// Entire cluster (node management, topology).
    Cluster,
    /// All collections within a tenant.
    Tenant(TenantId),
    /// A specific collection within a tenant.
    Collection {
        tenant_id: TenantId,
        collection: String,
    },
    /// System catalog (superuser only).
    SystemCatalog,
}

/// Check if a role implicitly grants a permission on a target.
///
/// Superuser is checked before this function is called.
pub fn role_grants_permission(role: &Role, permission: Permission) -> bool {
    match role {
        Role::Superuser => true,
        Role::TenantAdmin => true,
        Role::ReadWrite => matches!(permission, Permission::Read | Permission::Write),
        Role::ReadOnly => matches!(permission, Permission::Read),
        Role::Monitor => matches!(permission, Permission::Monitor | Permission::Read),
        Role::Custom(_) => false, // Custom roles need explicit grants (future)
    }
}

/// Map a PhysicalPlan to the Permission required to execute it.
pub fn required_permission(plan: &crate::bridge::envelope::PhysicalPlan) -> Permission {
    use crate::bridge::envelope::PhysicalPlan;
    match plan {
        // Read operations.
        PhysicalPlan::PointGet { .. }
        | PhysicalPlan::RangeScan { .. }
        | PhysicalPlan::VectorSearch { .. }
        | PhysicalPlan::VectorMultiSearch { .. }
        | PhysicalPlan::CrdtRead { .. }
        | PhysicalPlan::GraphHop { .. }
        | PhysicalPlan::GraphNeighbors { .. }
        | PhysicalPlan::GraphPath { .. }
        | PhysicalPlan::GraphSubgraph { .. }
        | PhysicalPlan::GraphRagFusion { .. }
        | PhysicalPlan::DocumentScan { .. }
        | PhysicalPlan::Aggregate { .. }
        | PhysicalPlan::HashJoin { .. }
        | PhysicalPlan::TextSearch { .. }
        | PhysicalPlan::HybridSearch { .. }
        | PhysicalPlan::PartialAggregate { .. }
        | PhysicalPlan::BroadcastJoin { .. }
        | PhysicalPlan::ShuffleJoin { .. }
        | PhysicalPlan::SpatialScan { .. }
        | PhysicalPlan::TimeseriesScan { .. } => Permission::Read,

        // Write operations.
        PhysicalPlan::CrdtApply { .. }
        | PhysicalPlan::VectorInsert { .. }
        | PhysicalPlan::VectorBatchInsert { .. }
        | PhysicalPlan::VectorDelete { .. }
        | PhysicalPlan::DocumentBatchInsert { .. }
        | PhysicalPlan::PointPut { .. }
        | PhysicalPlan::PointDelete { .. }
        | PhysicalPlan::PointUpdate { .. }
        | PhysicalPlan::EdgePut { .. }
        | PhysicalPlan::EdgeDelete { .. }
        | PhysicalPlan::WalAppend { .. }
        | PhysicalPlan::BulkUpdate { .. }
        | PhysicalPlan::BulkDelete { .. }
        | PhysicalPlan::Upsert { .. }
        | PhysicalPlan::InsertSelect { .. }
        | PhysicalPlan::Truncate { .. }
        | PhysicalPlan::TimeseriesIngest { .. } => Permission::Write,

        // EstimateCount is a read operation.
        PhysicalPlan::EstimateCount { .. } => Permission::Read,

        // DDL / schema changes.
        PhysicalPlan::SetCollectionPolicy { .. } | PhysicalPlan::SetVectorParams { .. } => {
            Permission::Alter
        }

        // Control operations.
        PhysicalPlan::Cancel { .. } => Permission::Admin,

        // Nested loop join: read-only join operation.
        PhysicalPlan::NestedLoopJoin { .. } => Permission::Read,

        // Transaction batch: requires write (contains writes).
        PhysicalPlan::TransactionBatch { .. } => Permission::Write,

        // Graph algorithms and pattern matching: read-only computation on CSR.
        PhysicalPlan::GraphAlgo { .. } | PhysicalPlan::GraphMatch { .. } => Permission::Read,

        // System-level operations: require admin.
        PhysicalPlan::CreateSnapshot | PhysicalPlan::Compact | PhysicalPlan::Checkpoint => {
            Permission::Admin
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_identity(roles: Vec<Role>, superuser: bool) -> AuthenticatedIdentity {
        AuthenticatedIdentity {
            user_id: 1,
            username: "test".into(),
            tenant_id: TenantId::new(1),
            auth_method: AuthMethod::Trust,
            roles,
            is_superuser: superuser,
        }
    }

    #[test]
    fn superuser_has_all_roles() {
        let id = test_identity(vec![], true);
        assert!(id.has_role(&Role::ReadOnly));
        assert!(id.has_role(&Role::TenantAdmin));
        assert!(id.has_role(&Role::Custom("anything".into())));
    }

    #[test]
    fn readonly_only_has_readonly() {
        let id = test_identity(vec![Role::ReadOnly], false);
        assert!(id.has_role(&Role::ReadOnly));
        assert!(!id.has_role(&Role::ReadWrite));
        assert!(!id.has_role(&Role::TenantAdmin));
    }

    #[test]
    fn role_permission_mapping() {
        assert!(role_grants_permission(&Role::ReadOnly, Permission::Read));
        assert!(!role_grants_permission(&Role::ReadOnly, Permission::Write));

        assert!(role_grants_permission(&Role::ReadWrite, Permission::Read));
        assert!(role_grants_permission(&Role::ReadWrite, Permission::Write));
        assert!(!role_grants_permission(&Role::ReadWrite, Permission::Drop));

        assert!(role_grants_permission(
            &Role::TenantAdmin,
            Permission::Admin
        ));
        assert!(role_grants_permission(&Role::TenantAdmin, Permission::Drop));
    }

    #[test]
    fn role_display_roundtrip() {
        let roles = [
            Role::Superuser,
            Role::TenantAdmin,
            Role::ReadWrite,
            Role::ReadOnly,
            Role::Monitor,
        ];
        for role in &roles {
            let s = role.to_string();
            let parsed: Role = s.parse().unwrap();
            assert_eq!(*role, parsed);
        }
    }
}
