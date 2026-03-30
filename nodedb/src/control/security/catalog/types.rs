//! System catalog — redb-backed persistent storage for auth metadata.
//!
//! Stores users, roles, and permissions in `{data_dir}/system.redb`.
//! Lives on the Control Plane (Send + Sync). Uses redb's ACID transactions
//! for crash-safe writes, same technology as the sparse engine.

use std::path::Path;

use redb::{Database, TableDefinition};
use tracing::info;

/// Table: username (string) -> MessagePack-serialized user record.
pub(super) const USERS: TableDefinition<&str, &[u8]> = TableDefinition::new("_system.users");

/// Table: key_id (string) -> MessagePack-serialized API key record.
pub(super) const API_KEYS: TableDefinition<&str, &[u8]> = TableDefinition::new("_system.api_keys");

/// Table: tenant_id (string) -> MessagePack-serialized tenant record.
pub(super) const TENANTS: TableDefinition<&str, &[u8]> = TableDefinition::new("_system.tenants");

/// Table: seq (u64 as big-endian bytes) -> MessagePack-serialized audit entry.
pub(super) const AUDIT_LOG: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("_system.audit_log");

/// Table: role_name -> MessagePack-serialized custom role record.
pub(super) const ROLES: TableDefinition<&str, &[u8]> = TableDefinition::new("_system.roles");

/// Table: "target:role_or_user" -> MessagePack-serialized permission grant.
pub(super) const PERMISSIONS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("_system.permissions");

/// Table: "{object_type}:{tenant_id}:{object_name}" -> owner username.
pub(super) const OWNERS: TableDefinition<&str, &[u8]> = TableDefinition::new("_system.owners");

/// Table: "{tenant_id}:{name}" -> MessagePack-serialized collection metadata.
pub(super) const COLLECTIONS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("_system.collections");

/// Table: "{tenant_id}:{name}" -> MessagePack-serialized materialized view metadata.
pub(super) const MATERIALIZED_VIEWS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("_system.materialized_views");

/// Table: "{tenant_id}:{name}" -> MessagePack-serialized user function definition.
pub(super) const FUNCTIONS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("_system.functions");

/// Table: "{tenant_id}:{name}" -> MessagePack-serialized trigger definition.
pub(super) const TRIGGERS: TableDefinition<&str, &[u8]> = TableDefinition::new("_system.triggers");

/// Table: "{tenant_id}:{name}" -> MessagePack-serialized stored procedure definition.
pub(super) const PROCEDURES: TableDefinition<&str, &[u8]> =
    TableDefinition::new("_system.procedures");

/// Table: "{source_type}:{tenant_id}:{source_name}" -> MessagePack-serialized dependency list.
/// Tracks what objects a function/trigger/procedure references in its body.
pub(super) const DEPENDENCIES: TableDefinition<&str, &[u8]> =
    TableDefinition::new("_system.dependencies");

/// Table: metadata key -> value bytes (counters, config).
pub(super) const METADATA: TableDefinition<&str, &[u8]> = TableDefinition::new("_system.metadata");

/// Table: blacklist key (user_id or IP) -> MessagePack-serialized blacklist entry.
pub(super) const BLACKLIST: TableDefinition<&str, &[u8]> =
    TableDefinition::new("_system.blacklist");

/// Table: auth_user_id -> MessagePack-serialized auth user record (JIT-provisioned).
pub(super) const AUTH_USERS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("_system.auth_users");

/// Table: org_id -> MessagePack-serialized org record.
pub(super) const ORGS: TableDefinition<&str, &[u8]> = TableDefinition::new("_system.orgs");

/// Table: "{org_id}:{user_id}" -> MessagePack-serialized org membership.
pub(super) const ORG_MEMBERS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("_system.org_members");

/// Table: scope_name -> MessagePack-serialized scope definition.
pub(super) const SCOPES: TableDefinition<&str, &[u8]> = TableDefinition::new("_system.scopes");

/// Table: "{scope_name}:{grantee_type}:{grantee_id}" -> MessagePack-serialized scope grant.
pub(super) const SCOPE_GRANTS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("_system.scope_grants");

pub fn catalog_err<E: std::fmt::Display>(ctx: &str, e: E) -> crate::Error {
    crate::Error::Storage {
        engine: "catalog".into(),
        detail: format!("{ctx}: {e}"),
    }
}

/// Key format: "{object_type}:{tenant_id}:{object_name}"
pub fn owner_key(object_type: &str, tenant_id: u32, object_name: &str) -> String {
    format!("{object_type}:{tenant_id}:{object_name}")
}

/// Serializable user record for redb storage.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct StoredUser {
    pub user_id: u64,
    pub username: String,
    pub tenant_id: u32,
    pub password_hash: String,
    pub scram_salt: Vec<u8>,
    pub scram_salted_password: Vec<u8>,
    pub roles: Vec<String>,
    pub is_superuser: bool,
    pub is_active: bool,
    /// True if this is a service account (no password, API key auth only).
    #[serde(default)]
    pub is_service_account: bool,
    /// Unix timestamp (seconds) when the user was created.
    #[serde(default)]
    pub created_at: u64,
    /// Unix timestamp (seconds) when the user was last modified.
    #[serde(default)]
    pub updated_at: u64,
    /// Unix timestamp (seconds) when the password expires. 0 = no expiry.
    #[serde(default)]
    pub password_expires_at: u64,
    /// MD5 hash for pgwire MD5 auth: `md5(password + username)` as hex.
    #[serde(default)]
    pub md5_hash: String,
}

/// Serializable API key record for redb storage.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct StoredApiKey {
    /// Unique key identifier (used as prefix in the token).
    pub key_id: String,
    /// SHA-256 hash of the secret portion.
    pub secret_hash: Vec<u8>,
    /// User this key belongs to.
    pub username: String,
    pub user_id: u64,
    pub tenant_id: u32,
    /// Unix timestamp (seconds) when the key expires. 0 = no expiry.
    pub expires_at: u64,
    /// Whether this key has been revoked.
    pub is_revoked: bool,
    /// Unix timestamp (seconds) when the key was created.
    pub created_at: u64,
    /// Permission scope restriction. Empty = inherit all user permissions.
    /// Format: ["read:collection_name", "write:collection_name", ...]
    #[serde(default)]
    pub scope: Vec<String>,
}

/// Serializable tenant record for redb storage.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct StoredTenant {
    pub tenant_id: u32,
    pub name: String,
    pub created_at: u64,
    pub is_active: bool,
}

/// Serializable audit entry for redb storage.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct StoredAuditEntry {
    pub seq: u64,
    pub timestamp_us: u64,
    pub event: String,
    pub tenant_id: Option<u32>,
    pub source: String,
    pub detail: String,
    /// SHA-256 hash of the previous entry (hex). Empty for first entry.
    #[serde(default)]
    pub prev_hash: String,
}

/// Serializable custom role for redb storage.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct StoredRole {
    pub name: String,
    pub tenant_id: u32,
    /// Parent role name for inheritance. Empty = no parent.
    pub parent: String,
    pub created_at: u64,
}

/// Serializable permission grant for redb storage.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct StoredPermission {
    /// What the grant applies to: "cluster", "tenant:1", "collection:1:users"
    pub target: String,
    /// Who receives the grant: role name or "user:username"
    pub grantee: String,
    /// Permission type: "read", "write", "create", "drop", "alter", "admin", "monitor"
    pub permission: String,
    pub granted_by: String,
    pub granted_at: u64,
}

/// Serializable ownership record.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct StoredOwner {
    /// "collection", "index"
    pub object_type: String,
    /// Object name (e.g. collection name).
    pub object_name: String,
    pub tenant_id: u32,
    pub owner_username: String,
}

/// Persistent system catalog backed by redb.
/// Serializable collection metadata for redb storage.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct StoredCollection {
    pub tenant_id: u32,
    pub name: String,
    pub owner: String,
    pub created_at: u64,
    /// Optional field type declarations. Empty = schemaless.
    /// Format: [("field_name", "type_name"), ...]
    #[serde(default)]
    pub fields: Vec<(String, String)>,
    /// Extended field definitions with DEFAULT, VALUE (computed), ASSERT, TYPE.
    /// Keyed by field name. Stored alongside `fields` for backward compatibility.
    #[serde(default)]
    pub field_defs: Vec<FieldDefinition>,
    /// Event/trigger definitions (DEFINE EVENT).
    #[serde(default)]
    pub event_defs: Vec<EventDefinition>,
    /// Collection type: determines storage engine and query routing.
    #[serde(default)]
    pub collection_type: nodedb_types::CollectionType,
    /// Timeseries-specific configuration (JSON-serialized `TieredPartitionConfig`).
    ///
    /// Only populated when `collection_type == "timeseries"`. Contains
    /// partition interval, retention, merge settings, and compression config.
    /// Example: `{"partition_by":"3d","retention_period":"30d"}`.
    #[serde(default)]
    pub timeseries_config: Option<String>,
    pub is_active: bool,
}

impl StoredCollection {
    /// Create a minimal collection entry (schemaless document, no fields).
    pub fn new(tenant_id: u32, name: &str, owner: &str) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        Self {
            tenant_id,
            name: name.to_string(),
            owner: owner.to_string(),
            created_at: now,
            fields: Vec::new(),
            field_defs: Vec::new(),
            event_defs: Vec::new(),
            collection_type: nodedb_types::CollectionType::document(),
            timeseries_config: None,
            is_active: true,
        }
    }

    /// Parse the timeseries config JSON, if present.
    pub fn get_timeseries_config(&self) -> Option<serde_json::Value> {
        self.timeseries_config
            .as_ref()
            .and_then(|s| serde_json::from_str(s).ok())
    }
}

/// A materialized view: strict → columnar CDC bridge.
///
/// Created via `CREATE MATERIALIZED VIEW <name> ON <source> AS SELECT ...`.
/// The source must be a strict (or document) collection. The target is
/// an implicitly-created columnar collection with the view's schema.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct StoredMaterializedView {
    pub tenant_id: u32,
    /// View name (also the target columnar collection name).
    pub name: String,
    /// Source collection name.
    pub source: String,
    /// SQL body: the SELECT ... FROM ... part (used for schema + refresh).
    pub query_sql: String,
    /// Refresh mode: "auto" (on every write to source) or "manual".
    #[serde(default = "default_refresh_mode")]
    pub refresh_mode: String,
    pub owner: String,
    pub created_at: u64,
}

fn default_refresh_mode() -> String {
    "auto".into()
}

/// Extended field definition supporting DEFAULT, VALUE, ASSERT, and TYPE constraints.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FieldDefinition {
    pub name: String,
    /// Type constraint: "int", "float", "string", "bool", "array", "object", "datetime", etc.
    /// Empty = any type (schemaless).
    #[serde(default)]
    pub field_type: String,
    /// Default expression (evaluated when field is missing on insert).
    /// Stored as SqlExpr JSON string for deserialization on the Data Plane.
    #[serde(default)]
    pub default_expr: String,
    /// Computed value expression (evaluated on every read, not stored).
    #[serde(default)]
    pub value_expr: String,
    /// Assertion expression (must evaluate to true for writes to succeed).
    #[serde(default)]
    pub assert_expr: String,
    /// Whether the field is read-only (cannot be set by user).
    #[serde(default)]
    pub readonly: bool,
}

/// Table event/trigger definition.
///
/// Stored in the catalog alongside field definitions. Evaluated after
/// write operations (INSERT, UPDATE, DELETE) when the WHEN condition matches.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct EventDefinition {
    pub name: String,
    /// Collection this event is attached to.
    pub collection: String,
    /// WHEN condition: "INSERT", "UPDATE", "DELETE", or arbitrary expression.
    pub when_condition: String,
    /// THEN action: SQL statement(s) to execute when triggered.
    pub then_action: String,
}

/// Serializable blacklist entry for redb storage.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct StoredBlacklistEntry {
    /// Blacklist entry key: `"user:{user_id}"` or `"ip:{addr_or_cidr}"`.
    pub key: String,
    /// Entry kind: "user" or "ip".
    pub kind: String,
    /// Human-readable reason for blacklisting.
    pub reason: String,
    /// Who created this entry (admin username).
    pub created_by: String,
    /// Unix timestamp (seconds) when blacklisted.
    pub created_at: u64,
    /// Unix timestamp (seconds) when this entry expires. 0 = permanent.
    pub expires_at: u64,
}

/// Serializable JIT-provisioned auth user record for redb storage.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct StoredAuthUser {
    /// Unique identifier (from JWT `sub` or `user_id` claim).
    pub id: String,
    /// Username (display name).
    pub username: String,
    /// Email address (from JWT `email` claim).
    #[serde(default)]
    pub email: String,
    /// Tenant this user belongs to.
    pub tenant_id: u32,
    /// Identity provider name that provisioned this user.
    pub provider: String,
    /// Unix timestamp (seconds) of first authentication.
    pub first_seen: u64,
    /// Unix timestamp (seconds) of most recent authentication.
    pub last_seen: u64,
    /// Whether this user is active (can authenticate).
    pub is_active: bool,
    /// Account status: active, suspended, banned, restricted, read_only.
    #[serde(default = "default_status")]
    pub status: String,
    /// Whether this user was externally provisioned (no local password).
    #[serde(default = "default_true")]
    pub is_external: bool,
    /// Last synced JWT claims (for claim sync on each request).
    #[serde(default)]
    pub synced_claims: std::collections::HashMap<String, String>,
}

fn default_status() -> String {
    "active".into()
}
fn default_true() -> bool {
    true
}

pub struct SystemCatalog {
    pub(super) db: Database,
}

impl SystemCatalog {
    /// Open or create the system catalog at the given path.
    pub fn open(path: &Path) -> crate::Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let db = Database::create(path).map_err(|e| catalog_err("open", e))?;

        // Ensure tables exist.
        let write_txn = db.begin_write().map_err(|e| catalog_err("init txn", e))?;
        {
            let _ = write_txn
                .open_table(USERS)
                .map_err(|e| catalog_err("init users table", e))?;
            let _ = write_txn
                .open_table(API_KEYS)
                .map_err(|e| catalog_err("init api_keys table", e))?;
            let _ = write_txn
                .open_table(ROLES)
                .map_err(|e| catalog_err("init roles table", e))?;
            let _ = write_txn
                .open_table(PERMISSIONS)
                .map_err(|e| catalog_err("init permissions table", e))?;
            let _ = write_txn
                .open_table(OWNERS)
                .map_err(|e| catalog_err("init owners table", e))?;
            let _ = write_txn
                .open_table(TENANTS)
                .map_err(|e| catalog_err("init tenants table", e))?;
            let _ = write_txn
                .open_table(AUDIT_LOG)
                .map_err(|e| catalog_err("init audit_log table", e))?;
            let _ = write_txn
                .open_table(COLLECTIONS)
                .map_err(|e| catalog_err("init collections table", e))?;
            let _ = write_txn
                .open_table(METADATA)
                .map_err(|e| catalog_err("init metadata table", e))?;
            let _ = write_txn
                .open_table(BLACKLIST)
                .map_err(|e| catalog_err("init blacklist table", e))?;
            let _ = write_txn
                .open_table(AUTH_USERS)
                .map_err(|e| catalog_err("init auth_users table", e))?;
            let _ = write_txn
                .open_table(ORGS)
                .map_err(|e| catalog_err("init orgs table", e))?;
            let _ = write_txn
                .open_table(ORG_MEMBERS)
                .map_err(|e| catalog_err("init org_members table", e))?;
            let _ = write_txn
                .open_table(SCOPES)
                .map_err(|e| catalog_err("init scopes table", e))?;
            let _ = write_txn
                .open_table(SCOPE_GRANTS)
                .map_err(|e| catalog_err("init scope_grants table", e))?;
            let _ = write_txn
                .open_table(MATERIALIZED_VIEWS)
                .map_err(|e| catalog_err("init materialized_views table", e))?;
            let _ = write_txn
                .open_table(FUNCTIONS)
                .map_err(|e| catalog_err("init functions table", e))?;
            let _ = write_txn
                .open_table(TRIGGERS)
                .map_err(|e| catalog_err("init triggers table", e))?;
            let _ = write_txn
                .open_table(PROCEDURES)
                .map_err(|e| catalog_err("init procedures table", e))?;
            let _ = write_txn
                .open_table(DEPENDENCIES)
                .map_err(|e| catalog_err("init dependencies table", e))?;
        }
        write_txn
            .commit()
            .map_err(|e| catalog_err("init commit", e))?;

        info!(path = %path.display(), "system catalog opened");

        Ok(Self { db })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_and_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("system.redb");
        let catalog = SystemCatalog::open(&path).unwrap();

        let user = StoredUser {
            user_id: 1,
            username: "alice".into(),
            tenant_id: 1,
            password_hash: "$argon2id$test".into(),
            scram_salt: vec![1, 2, 3, 4],
            scram_salted_password: vec![5, 6, 7, 8],
            roles: vec!["readwrite".into()],
            is_superuser: false,
            is_active: true,
            is_service_account: false,
            created_at: 0,
            updated_at: 0,
            password_expires_at: 0,
            md5_hash: String::new(),
        };

        catalog.put_user(&user).unwrap();

        let loaded = catalog.load_all_users().unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].username, "alice");
        assert_eq!(loaded[0].tenant_id, 1);
    }

    #[test]
    fn delete_user() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = SystemCatalog::open(&dir.path().join("system.redb")).unwrap();

        let user = StoredUser {
            user_id: 1,
            username: "bob".into(),
            tenant_id: 1,
            password_hash: "hash".into(),
            scram_salt: vec![],
            scram_salted_password: vec![],
            roles: vec![],
            is_superuser: false,
            is_active: true,
            is_service_account: false,
            created_at: 0,
            updated_at: 0,
            password_expires_at: 0,
            md5_hash: String::new(),
        };

        catalog.put_user(&user).unwrap();
        catalog.delete_user("bob").unwrap();

        let loaded = catalog.load_all_users().unwrap();
        assert!(loaded.is_empty());
    }

    #[test]
    fn next_user_id_persists() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("system.redb");

        {
            let catalog = SystemCatalog::open(&path).unwrap();
            assert_eq!(catalog.load_next_user_id().unwrap(), 1); // Default.
            catalog.save_next_user_id(42).unwrap();
        }

        // Reopen — ID should persist.
        let catalog = SystemCatalog::open(&path).unwrap();
        assert_eq!(catalog.load_next_user_id().unwrap(), 42);
    }

    #[test]
    fn survives_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("system.redb");

        {
            let catalog = SystemCatalog::open(&path).unwrap();
            catalog
                .put_user(&StoredUser {
                    user_id: 5,
                    username: "persistent".into(),
                    tenant_id: 3,
                    password_hash: "hash".into(),
                    scram_salt: vec![1],
                    scram_salted_password: vec![2],
                    roles: vec!["readonly".into(), "monitor".into()],
                    is_superuser: false,
                    is_active: true,
                    is_service_account: false,
                    created_at: 0,
                    updated_at: 0,
                    password_expires_at: 0,
                    md5_hash: String::new(),
                })
                .unwrap();
        }

        // Reopen — user should survive.
        let catalog = SystemCatalog::open(&path).unwrap();
        let users = catalog.load_all_users().unwrap();
        assert_eq!(users.len(), 1);
        assert_eq!(users[0].username, "persistent");
        assert_eq!(users[0].user_id, 5);
        assert_eq!(users[0].roles, vec!["readonly", "monitor"]);
    }
}
