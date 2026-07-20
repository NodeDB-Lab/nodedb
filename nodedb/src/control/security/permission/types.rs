// SPDX-License-Identifier: BUSL-1.1

//! Data shapes and string helpers shared across the
//! permission module.

use crate::control::security::identity::Permission;
use crate::types::TenantId;

/// A permission grant record (in-memory).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Grant {
    /// Target: "cluster", "tenant:1", "collection:1:users"
    pub target: String,
    /// Grantee: role name or "user:username"
    pub grantee: String,
    /// Permission type.
    pub permission: Permission,
}

/// Ownership record (in-memory).
#[derive(Debug, Clone)]
pub struct OwnerRecord {
    pub object_type: String,
    pub object_name: String,
    pub tenant_id: TenantId,
    pub owner_username: String,
}

/// Build a `collection:{tenant}:{name}` target string for grants
/// and ownership lookups.
pub fn collection_target(tenant_id: TenantId, collection: &str) -> String {
    format!("collection:{}:{}", tenant_id.as_u64(), collection)
}

/// Build a `function:{tenant}:{name}` target string.
pub fn function_target(tenant_id: TenantId, function_name: &str) -> String {
    format!("function:{}:{}", tenant_id.as_u64(), function_name)
}

/// Build a `procedure:{tenant}:{name}` target string.
pub fn procedure_target(tenant_id: TenantId, procedure_name: &str) -> String {
    format!("procedure:{}:{}", tenant_id.as_u64(), procedure_name)
}

/// Build a `tenant:{id}` target string for tenant-scoped grants — a
/// permission held here applies to every collection in the tenant.
pub fn tenant_target(tenant_id: TenantId) -> String {
    format!("tenant:{}", tenant_id.as_u64())
}

/// `{object_type}:{database_id}:{tenant_id}:{object_name}` owner key.
pub fn owner_key(object_type: &str, database_id: u64, tenant_id: u64, object_name: &str) -> String {
    crate::control::security::catalog::owner_key(object_type, database_id, tenant_id, object_name)
}

/// Parse a permission name (case-insensitive). Also accepts SQL aliases.
pub fn parse_permission(s: &str) -> Option<Permission> {
    match s.to_ascii_lowercase().as_str() {
        "read" | "select" => Some(Permission::Read),
        "write" | "insert" | "update" | "delete" => Some(Permission::Write),
        "create" => Some(Permission::Create),
        "drop" => Some(Permission::Drop),
        "alter" => Some(Permission::Alter),
        "admin" => Some(Permission::Admin),
        "monitor" => Some(Permission::Monitor),
        "execute" | "call" => Some(Permission::Execute),
        "backup" => Some(Permission::Backup),
        _ => None,
    }
}

/// Render a `Permission` back to the canonical lowercase name used
/// in catalog rows.
pub fn format_permission(p: Permission) -> String {
    match p {
        Permission::Read => "read".into(),
        Permission::Write => "write".into(),
        Permission::Create => "create".into(),
        Permission::Drop => "drop".into(),
        Permission::Alter => "alter".into(),
        Permission::Admin => "admin".into(),
        Permission::Monitor => "monitor".into(),
        Permission::Execute => "execute".into(),
        Permission::Backup => "backup".into(),
    }
}
