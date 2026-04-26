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
    /// Call a user-defined function (`SELECT func(...)`, UDF in expression).
    Execute,
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
        Role::ReadWrite => matches!(
            permission,
            Permission::Read | Permission::Write | Permission::Execute
        ),
        Role::ReadOnly => matches!(permission, Permission::Read | Permission::Execute),
        Role::Monitor => matches!(permission, Permission::Monitor | Permission::Read),
        Role::Custom(_) => false, // Custom roles need explicit grants
    }
}

/// Map a PhysicalPlan to the Permission required to execute it.
pub fn required_permission(plan: &crate::bridge::envelope::PhysicalPlan) -> Permission {
    use crate::bridge::envelope::PhysicalPlan;
    use crate::bridge::physical_plan::{
        ArrayOp, ColumnarOp, CrdtOp, DocumentOp, GraphOp, KvOp, MetaOp, QueryOp, SpatialOp, TextOp,
        TimeseriesOp, VectorOp,
    };
    match plan {
        // Read operations.
        PhysicalPlan::Document(
            DocumentOp::PointGet { .. }
            | DocumentOp::RangeScan { .. }
            | DocumentOp::Scan { .. }
            | DocumentOp::IndexLookup { .. }
            | DocumentOp::IndexedFetch { .. }
            | DocumentOp::EstimateCount { .. },
        ) => Permission::Read,

        PhysicalPlan::Vector(
            VectorOp::Search { .. }
            | VectorOp::MultiSearch { .. }
            | VectorOp::QueryStats { .. }
            | VectorOp::SparseSearch { .. }
            | VectorOp::MultiVectorScoreSearch { .. },
        ) => Permission::Read,

        PhysicalPlan::Crdt(
            CrdtOp::Read { .. }
            | CrdtOp::ReadAtVersion { .. }
            | CrdtOp::GetVersionVector
            | CrdtOp::ExportDelta { .. },
        ) => Permission::Read,

        PhysicalPlan::Graph(
            GraphOp::Hop { .. }
            | GraphOp::Neighbors { .. }
            | GraphOp::NeighborsMulti { .. }
            | GraphOp::Path { .. }
            | GraphOp::Subgraph { .. }
            | GraphOp::RagFusion { .. }
            | GraphOp::Algo { .. }
            | GraphOp::Match { .. }
            | GraphOp::TemporalNeighbors { .. }
            | GraphOp::TemporalAlgorithm { .. },
        ) => Permission::Read,

        PhysicalPlan::Query(
            QueryOp::Aggregate { .. }
            | QueryOp::HashJoin { .. }
            | QueryOp::InlineHashJoin { .. }
            | QueryOp::PartialAggregate { .. }
            | QueryOp::BroadcastJoin { .. }
            | QueryOp::ShuffleJoin { .. }
            | QueryOp::NestedLoopJoin { .. }
            | QueryOp::SortMergeJoin { .. }
            | QueryOp::RecursiveScan { .. }
            | QueryOp::FacetCounts { .. },
        ) => Permission::Read,

        PhysicalPlan::Text(TextOp::Search { .. } | TextOp::HybridSearch { .. }) => Permission::Read,

        PhysicalPlan::Spatial(SpatialOp::Scan { .. }) => Permission::Read,

        PhysicalPlan::Columnar(ColumnarOp::Scan { .. }) => Permission::Read,

        PhysicalPlan::Timeseries(TimeseriesOp::Scan { .. }) => Permission::Read,

        // Write operations.
        PhysicalPlan::Crdt(
            CrdtOp::Apply { .. }
            | CrdtOp::RestoreToVersion { .. }
            | CrdtOp::ListInsert { .. }
            | CrdtOp::ListDelete { .. }
            | CrdtOp::ListMove { .. },
        ) => Permission::Write,

        PhysicalPlan::Vector(
            VectorOp::Insert { .. }
            | VectorOp::BatchInsert { .. }
            | VectorOp::Delete { .. }
            | VectorOp::SparseInsert { .. }
            | VectorOp::SparseDelete { .. }
            | VectorOp::MultiVectorInsert { .. }
            | VectorOp::MultiVectorDelete { .. },
        ) => Permission::Write,

        PhysicalPlan::Document(
            DocumentOp::BatchInsert { .. }
            | DocumentOp::PointPut { .. }
            | DocumentOp::PointInsert { .. }
            | DocumentOp::PointDelete { .. }
            | DocumentOp::PointUpdate { .. }
            | DocumentOp::BulkUpdate { .. }
            | DocumentOp::BulkDelete { .. }
            | DocumentOp::Upsert { .. }
            | DocumentOp::InsertSelect { .. }
            | DocumentOp::Truncate { .. },
        ) => Permission::Write,

        PhysicalPlan::Graph(
            GraphOp::EdgePut { .. }
            | GraphOp::EdgePutBatch { .. }
            | GraphOp::EdgeDelete { .. }
            | GraphOp::EdgeDeleteBatch { .. }
            | GraphOp::SetNodeLabels { .. }
            | GraphOp::RemoveNodeLabels { .. },
        ) => Permission::Write,

        PhysicalPlan::Meta(MetaOp::WalAppend { .. }) => Permission::Write,

        PhysicalPlan::Columnar(
            ColumnarOp::Insert { .. } | ColumnarOp::Update { .. } | ColumnarOp::Delete { .. },
        ) => Permission::Write,

        PhysicalPlan::Timeseries(TimeseriesOp::Ingest { .. }) => Permission::Write,

        // Transaction batch: requires write (contains writes).
        PhysicalPlan::Meta(MetaOp::TransactionBatch { .. }) => Permission::Write,

        // DDL / schema changes.
        PhysicalPlan::Document(
            DocumentOp::Register { .. }
            | DocumentOp::DropIndex { .. }
            | DocumentOp::BackfillIndex { .. },
        ) => Permission::Alter,

        PhysicalPlan::Crdt(CrdtOp::SetPolicy { .. } | CrdtOp::CompactAtVersion { .. }) => {
            Permission::Alter
        }

        PhysicalPlan::Meta(
            MetaOp::RegisterContinuousAggregate { .. }
            | MetaOp::UnregisterContinuousAggregate { .. }
            | MetaOp::ListContinuousAggregates
            | MetaOp::ConvertCollection { .. },
        ) => Permission::Alter,

        PhysicalPlan::Vector(
            VectorOp::SetParams { .. }
            | VectorOp::Seal { .. }
            | VectorOp::CompactIndex { .. }
            | VectorOp::Rebuild { .. },
        ) => Permission::Alter,

        // Pre-computed responses (constant queries like SELECT 1).
        PhysicalPlan::Meta(MetaOp::RawResponse { .. }) => Permission::Read,

        // Control operations.
        PhysicalPlan::Meta(MetaOp::Cancel { .. }) => Permission::Admin,

        // System-level operations: require admin.
        PhysicalPlan::Meta(
            MetaOp::CreateSnapshot
            | MetaOp::Compact
            | MetaOp::Checkpoint
            | MetaOp::CreateTenantSnapshot { .. }
            | MetaOp::RestoreTenantSnapshot { .. }
            | MetaOp::UnregisterCollection { .. }
            | MetaOp::UnregisterMaterializedView { .. }
            | MetaOp::QueryCollectionSize { .. },
        ) => Permission::Admin,

        // KV engine: read operations.
        PhysicalPlan::Kv(
            KvOp::Get { .. }
            | KvOp::GetTtl { .. }
            | KvOp::Scan { .. }
            | KvOp::BatchGet { .. }
            | KvOp::FieldGet { .. }
            | KvOp::SortedIndexRank { .. }
            | KvOp::SortedIndexTopK { .. }
            | KvOp::SortedIndexRange { .. }
            | KvOp::SortedIndexCount { .. }
            | KvOp::SortedIndexScore { .. },
        ) => Permission::Read,

        // KV engine: write operations.
        PhysicalPlan::Kv(
            KvOp::Put { .. }
            | KvOp::Insert { .. }
            | KvOp::InsertIfAbsent { .. }
            | KvOp::InsertOnConflictUpdate { .. }
            | KvOp::Delete { .. }
            | KvOp::Expire { .. }
            | KvOp::Persist { .. }
            | KvOp::BatchPut { .. }
            | KvOp::RegisterIndex { .. }
            | KvOp::DropIndex { .. }
            | KvOp::FieldSet { .. }
            | KvOp::Truncate { .. }
            | KvOp::Incr { .. }
            | KvOp::IncrFloat { .. }
            | KvOp::Cas { .. }
            | KvOp::GetSet { .. }
            | KvOp::RegisterSortedIndex { .. }
            | KvOp::DropSortedIndex { .. }
            | KvOp::Transfer { .. }
            | KvOp::TransferItem { .. },
        ) => Permission::Write,

        // Tenant purge requires superuser (checked at DDL level); map to Write.
        PhysicalPlan::Meta(MetaOp::PurgeTenant { .. }) => Permission::Write,

        // Retention enforcement is admin-level (invoked by background tasks).
        PhysicalPlan::Meta(
            MetaOp::EnforceTimeseriesRetention { .. }
            | MetaOp::ApplyContinuousAggRetention
            | MetaOp::TemporalPurgeEdgeStore { .. }
            | MetaOp::TemporalPurgeDocumentStrict { .. }
            | MetaOp::TemporalPurgeColumnar { .. }
            | MetaOp::TemporalPurgeCrdt { .. },
        ) => Permission::Admin,

        // Watermark query is admin-level (invoked by enforcement loop).
        PhysicalPlan::Meta(MetaOp::QueryAggregateWatermark { .. }) => Permission::Admin,

        // Last-value cache queries are read operations.
        PhysicalPlan::Meta(MetaOp::QueryLastValues { .. } | MetaOp::QueryLastValue { .. }) => {
            Permission::Read
        }

        // Array engine: query operators are reads, put/delete are
        // writes, OpenArray is DDL, flush/compact are admin.
        PhysicalPlan::Array(
            ArrayOp::Slice { .. }
            | ArrayOp::SurrogateBitmapScan { .. }
            | ArrayOp::Project { .. }
            | ArrayOp::Aggregate { .. }
            | ArrayOp::Elementwise { .. },
        ) => Permission::Read,
        PhysicalPlan::Array(ArrayOp::Put { .. } | ArrayOp::Delete { .. }) => Permission::Write,
        PhysicalPlan::Array(ArrayOp::OpenArray { .. }) => Permission::Alter,
        PhysicalPlan::Array(
            ArrayOp::Flush { .. } | ArrayOp::Compact { .. } | ArrayOp::DropArray { .. },
        ) => Permission::Admin,

        // ClusterArray mirrors the local ArrayOp permission model.
        PhysicalPlan::ClusterArray(
            crate::bridge::physical_plan::ClusterArrayOp::Slice { .. }
            | crate::bridge::physical_plan::ClusterArrayOp::Agg { .. },
        ) => Permission::Read,
        PhysicalPlan::ClusterArray(
            crate::bridge::physical_plan::ClusterArrayOp::Put { .. }
            | crate::bridge::physical_plan::ClusterArrayOp::Delete { .. },
        ) => Permission::Write,
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
