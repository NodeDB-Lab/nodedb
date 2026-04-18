//! Authentication and authorization type definitions for redb catalog storage.

/// Serializable user record for redb storage.
#[derive(zerompk::ToMessagePack, zerompk::FromMessagePack, Debug, Clone)]
#[msgpack(map)]
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
    #[msgpack(default)]
    pub is_service_account: bool,
    /// Unix timestamp (seconds) when the user was created.
    #[msgpack(default)]
    pub created_at: u64,
    /// Unix timestamp (seconds) when the user was last modified.
    #[msgpack(default)]
    pub updated_at: u64,
    /// Unix timestamp (seconds) when the password expires. 0 = no expiry.
    #[msgpack(default)]
    pub password_expires_at: u64,
    /// MD5 hash for pgwire MD5 auth: `md5(password + username)` as hex.
    #[msgpack(default)]
    pub md5_hash: String,
}

/// Serializable API key record for redb storage.
#[derive(zerompk::ToMessagePack, zerompk::FromMessagePack, Debug, Clone)]
#[msgpack(map)]
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
    #[msgpack(default)]
    pub scope: Vec<String>,
}

/// Serializable tenant record for redb storage.
#[derive(zerompk::ToMessagePack, zerompk::FromMessagePack, Debug, Clone)]
pub struct StoredTenant {
    pub tenant_id: u32,
    pub name: String,
    pub created_at: u64,
    pub is_active: bool,
}

/// Serializable audit entry for redb storage.
#[derive(zerompk::ToMessagePack, zerompk::FromMessagePack, Debug, Clone)]
#[msgpack(map)]
pub struct StoredAuditEntry {
    pub seq: u64,
    pub timestamp_us: u64,
    pub event: String,
    pub tenant_id: Option<u32>,
    pub source: String,
    pub detail: String,
    /// SHA-256 hash of the previous entry (hex). Empty for first entry.
    #[msgpack(default)]
    pub prev_hash: String,
}

/// Serializable custom role for redb storage.
#[derive(zerompk::ToMessagePack, zerompk::FromMessagePack, Debug, Clone)]
pub struct StoredRole {
    pub name: String,
    pub tenant_id: u32,
    /// Parent role name for inheritance. Empty = no parent.
    pub parent: String,
    pub created_at: u64,
}

/// Serializable permission grant for redb storage.
#[derive(zerompk::ToMessagePack, zerompk::FromMessagePack, Debug, Clone)]
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
#[derive(zerompk::ToMessagePack, zerompk::FromMessagePack, Debug, Clone)]
pub struct StoredOwner {
    /// "collection", "index"
    pub object_type: String,
    /// Object name (e.g. collection name).
    pub object_name: String,
    pub tenant_id: u32,
    pub owner_username: String,
}

/// Serializable blacklist entry for redb storage.
#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
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
#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
#[msgpack(map)]
pub struct StoredAuthUser {
    /// Unique identifier (from JWT `sub` or `user_id` claim).
    pub id: String,
    /// Username (display name).
    pub username: String,
    /// Email address (from JWT `email` claim).
    #[msgpack(default)]
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
    #[msgpack(default = "default_status")]
    pub status: String,
    /// Whether this user was externally provisioned (no local password).
    #[msgpack(default = "default_true")]
    pub is_external: bool,
    /// Last synced JWT claims (for claim sync on each request).
    #[msgpack(default)]
    pub synced_claims: std::collections::HashMap<String, String>,
}

fn default_status() -> String {
    "active".into()
}
fn default_true() -> bool {
    true
}
