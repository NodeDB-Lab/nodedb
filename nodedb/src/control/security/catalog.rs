//! System catalog — redb-backed persistent storage for auth metadata.
//!
//! Stores users, roles, and permissions in `{data_dir}/system.redb`.
//! Lives on the Control Plane (Send + Sync). Uses redb's ACID transactions
//! for crash-safe writes, same technology as the sparse engine.

use std::path::Path;

use redb::{Database, ReadableTableMetadata, TableDefinition};
use tracing::info;

/// Table: username (string) → MessagePack-serialized user record.
const USERS: TableDefinition<&str, &[u8]> = TableDefinition::new("_system.users");

/// Table: key_id (string) → MessagePack-serialized API key record.
const API_KEYS: TableDefinition<&str, &[u8]> = TableDefinition::new("_system.api_keys");

/// Table: seq (u64 as big-endian bytes) → MessagePack-serialized audit entry.
const AUDIT_LOG: TableDefinition<&[u8], &[u8]> = TableDefinition::new("_system.audit_log");

/// Table: role_name → MessagePack-serialized custom role record.
const ROLES: TableDefinition<&str, &[u8]> = TableDefinition::new("_system.roles");

/// Table: "target:role_or_user" → MessagePack-serialized permission grant.
const PERMISSIONS: TableDefinition<&str, &[u8]> = TableDefinition::new("_system.permissions");

/// Table: "{object_type}:{tenant_id}:{object_name}" → owner username.
const OWNERS: TableDefinition<&str, &[u8]> = TableDefinition::new("_system.owners");

/// Table: metadata key → value bytes (counters, config).
const METADATA: TableDefinition<&str, &[u8]> = TableDefinition::new("_system.metadata");

fn catalog_err<E: std::fmt::Display>(ctx: &str, e: E) -> crate::Error {
    crate::Error::Storage {
        engine: "catalog".into(),
        detail: format!("{ctx}: {e}"),
    }
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
pub struct SystemCatalog {
    db: Database,
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
                .open_table(AUDIT_LOG)
                .map_err(|e| catalog_err("init audit_log table", e))?;
            let _ = write_txn
                .open_table(METADATA)
                .map_err(|e| catalog_err("init metadata table", e))?;
        }
        write_txn
            .commit()
            .map_err(|e| catalog_err("init commit", e))?;

        info!(path = %path.display(), "system catalog opened");

        Ok(Self { db })
    }

    /// Load all active users from the catalog.
    pub fn load_all_users(&self) -> crate::Result<Vec<StoredUser>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(USERS)
            .map_err(|e| catalog_err("open users", e))?;

        let mut users = Vec::new();
        let range = table
            .range::<&str>(..)
            .map_err(|e| catalog_err("range users", e))?;
        for entry in range {
            let (_, value) = entry.map_err(|e| catalog_err("read entry", e))?;
            let user: StoredUser = rmp_serde::from_slice(value.value())
                .map_err(|e| catalog_err("deserialize user", e))?;
            users.push(user);
        }

        Ok(users)
    }

    /// Write a user record to the catalog (insert or update).
    pub fn put_user(&self, user: &StoredUser) -> crate::Result<()> {
        let bytes = rmp_serde::to_vec(user).map_err(|e| catalog_err("serialize user", e))?;

        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        {
            let mut table = write_txn
                .open_table(USERS)
                .map_err(|e| catalog_err("open users", e))?;
            table
                .insert(user.username.as_str(), bytes.as_slice())
                .map_err(|e| catalog_err("insert user", e))?;
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))?;

        Ok(())
    }

    /// Delete a user record from the catalog.
    pub fn delete_user(&self, username: &str) -> crate::Result<()> {
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        {
            let mut table = write_txn
                .open_table(USERS)
                .map_err(|e| catalog_err("open users", e))?;
            table
                .remove(username)
                .map_err(|e| catalog_err("remove user", e))?;
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))?;

        Ok(())
    }

    /// Load the next_user_id counter.
    pub fn load_next_user_id(&self) -> crate::Result<u64> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(METADATA)
            .map_err(|e| catalog_err("open metadata", e))?;

        match table
            .get("next_user_id")
            .map_err(|e| catalog_err("get next_user_id", e))?
        {
            Some(val) => {
                let bytes = val.value();
                if bytes.len() == 8 {
                    let mut arr = [0u8; 8];
                    arr.copy_from_slice(bytes);
                    Ok(u64::from_le_bytes(arr))
                } else {
                    Ok(1)
                }
            }
            None => Ok(1),
        }
    }

    /// Persist the next_user_id counter.
    pub fn save_next_user_id(&self, id: u64) -> crate::Result<()> {
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        {
            let mut table = write_txn
                .open_table(METADATA)
                .map_err(|e| catalog_err("open metadata", e))?;
            table
                .insert("next_user_id", id.to_le_bytes().as_slice())
                .map_err(|e| catalog_err("insert next_user_id", e))?;
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))?;

        Ok(())
    }

    // ── API Key operations ──────────────────────────────────────────

    /// Write an API key record.
    pub fn put_api_key(&self, key: &StoredApiKey) -> crate::Result<()> {
        let bytes = rmp_serde::to_vec(key).map_err(|e| catalog_err("serialize api key", e))?;

        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        {
            let mut table = write_txn
                .open_table(API_KEYS)
                .map_err(|e| catalog_err("open api_keys", e))?;
            table
                .insert(key.key_id.as_str(), bytes.as_slice())
                .map_err(|e| catalog_err("insert api key", e))?;
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))?;

        Ok(())
    }

    /// Load all API keys.
    pub fn load_all_api_keys(&self) -> crate::Result<Vec<StoredApiKey>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(API_KEYS)
            .map_err(|e| catalog_err("open api_keys", e))?;

        let mut keys = Vec::new();
        let range = table
            .range::<&str>(..)
            .map_err(|e| catalog_err("range api_keys", e))?;
        for entry in range {
            let (_, value) = entry.map_err(|e| catalog_err("read entry", e))?;
            let key: StoredApiKey = rmp_serde::from_slice(value.value())
                .map_err(|e| catalog_err("deserialize api key", e))?;
            keys.push(key);
        }

        Ok(keys)
    }

    /// Delete an API key by key_id.
    pub fn delete_api_key(&self, key_id: &str) -> crate::Result<()> {
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        {
            let mut table = write_txn
                .open_table(API_KEYS)
                .map_err(|e| catalog_err("open api_keys", e))?;
            table
                .remove(key_id)
                .map_err(|e| catalog_err("remove api key", e))?;
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))?;

        Ok(())
    }

    // ── Role operations ──────────────────────────────────────────────

    pub fn put_role(&self, role: &StoredRole) -> crate::Result<()> {
        let bytes = rmp_serde::to_vec(role).map_err(|e| catalog_err("serialize role", e))?;
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        {
            let mut table = write_txn
                .open_table(ROLES)
                .map_err(|e| catalog_err("open roles", e))?;
            table
                .insert(role.name.as_str(), bytes.as_slice())
                .map_err(|e| catalog_err("insert role", e))?;
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))
    }

    pub fn delete_role(&self, name: &str) -> crate::Result<()> {
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        {
            let mut table = write_txn
                .open_table(ROLES)
                .map_err(|e| catalog_err("open roles", e))?;
            table
                .remove(name)
                .map_err(|e| catalog_err("remove role", e))?;
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))
    }

    pub fn load_all_roles(&self) -> crate::Result<Vec<StoredRole>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(ROLES)
            .map_err(|e| catalog_err("open roles", e))?;
        let mut roles = Vec::new();
        for entry in table
            .range::<&str>(..)
            .map_err(|e| catalog_err("range roles", e))?
        {
            let (_, value) = entry.map_err(|e| catalog_err("read role", e))?;
            roles.push(
                rmp_serde::from_slice(value.value()).map_err(|e| catalog_err("deser role", e))?,
            );
        }
        Ok(roles)
    }

    // ── Permission operations ───────────────────────────────────────

    /// Key format: "{target}:{grantee}:{permission}"
    fn permission_key(target: &str, grantee: &str, permission: &str) -> String {
        format!("{target}:{grantee}:{permission}")
    }

    pub fn put_permission(&self, perm: &StoredPermission) -> crate::Result<()> {
        let key = Self::permission_key(&perm.target, &perm.grantee, &perm.permission);
        let bytes = rmp_serde::to_vec(perm).map_err(|e| catalog_err("serialize perm", e))?;
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        {
            let mut table = write_txn
                .open_table(PERMISSIONS)
                .map_err(|e| catalog_err("open perms", e))?;
            table
                .insert(key.as_str(), bytes.as_slice())
                .map_err(|e| catalog_err("insert perm", e))?;
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))
    }

    pub fn delete_permission(
        &self,
        target: &str,
        grantee: &str,
        permission: &str,
    ) -> crate::Result<()> {
        let key = Self::permission_key(target, grantee, permission);
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        {
            let mut table = write_txn
                .open_table(PERMISSIONS)
                .map_err(|e| catalog_err("open perms", e))?;
            table
                .remove(key.as_str())
                .map_err(|e| catalog_err("remove perm", e))?;
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))
    }

    pub fn load_all_permissions(&self) -> crate::Result<Vec<StoredPermission>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(PERMISSIONS)
            .map_err(|e| catalog_err("open perms", e))?;
        let mut perms = Vec::new();
        for entry in table
            .range::<&str>(..)
            .map_err(|e| catalog_err("range perms", e))?
        {
            let (_, value) = entry.map_err(|e| catalog_err("read perm", e))?;
            perms.push(
                rmp_serde::from_slice(value.value()).map_err(|e| catalog_err("deser perm", e))?,
            );
        }
        Ok(perms)
    }

    // ── Ownership operations ────────────────────────────────────────

    /// Key format: "{object_type}:{tenant_id}:{object_name}"
    pub fn owner_key(object_type: &str, tenant_id: u32, object_name: &str) -> String {
        format!("{object_type}:{tenant_id}:{object_name}")
    }

    pub fn put_owner(&self, owner: &StoredOwner) -> crate::Result<()> {
        let key = Self::owner_key(&owner.object_type, owner.tenant_id, &owner.object_name);
        let bytes = rmp_serde::to_vec(owner).map_err(|e| catalog_err("serialize owner", e))?;
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        {
            let mut table = write_txn
                .open_table(OWNERS)
                .map_err(|e| catalog_err("open owners", e))?;
            table
                .insert(key.as_str(), bytes.as_slice())
                .map_err(|e| catalog_err("insert owner", e))?;
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))
    }

    pub fn load_all_owners(&self) -> crate::Result<Vec<StoredOwner>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(OWNERS)
            .map_err(|e| catalog_err("open owners", e))?;
        let mut owners = Vec::new();
        for entry in table
            .range::<&str>(..)
            .map_err(|e| catalog_err("range owners", e))?
        {
            let (_, value) = entry.map_err(|e| catalog_err("read owner", e))?;
            owners.push(
                rmp_serde::from_slice(value.value()).map_err(|e| catalog_err("deser owner", e))?,
            );
        }
        Ok(owners)
    }

    // ── Audit log operations ────────────────────────────────────────

    /// Append a batch of audit entries. Used by the periodic flush.
    pub fn append_audit_entries(&self, entries: &[StoredAuditEntry]) -> crate::Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        {
            let mut table = write_txn
                .open_table(AUDIT_LOG)
                .map_err(|e| catalog_err("open audit_log", e))?;
            for entry in entries {
                let key = entry.seq.to_be_bytes();
                let value =
                    rmp_serde::to_vec(entry).map_err(|e| catalog_err("serialize audit", e))?;
                table
                    .insert(key.as_slice(), value.as_slice())
                    .map_err(|e| catalog_err("insert audit", e))?;
            }
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))?;

        Ok(())
    }

    /// Load the highest sequence number from the audit log.
    /// Used on startup to resume the sequence counter.
    pub fn load_audit_max_seq(&self) -> crate::Result<u64> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(AUDIT_LOG)
            .map_err(|e| catalog_err("open audit_log", e))?;

        // Scan all keys to find the maximum sequence number.
        // Audit log keys are u64 big-endian, so the last entry in
        // iteration order is the highest.
        let mut max_seq = 0u64;
        let range = table
            .range::<&[u8]>(..)
            .map_err(|e| catalog_err("range audit", e))?;
        for entry in range {
            let (key, _) = entry.map_err(|e| catalog_err("read audit key", e))?;
            let key_bytes: &[u8] = key.value();
            if key_bytes.len() == 8 {
                let mut arr = [0u8; 8];
                arr.copy_from_slice(key_bytes);
                let seq = u64::from_be_bytes(arr);
                if seq > max_seq {
                    max_seq = seq;
                }
            }
        }
        Ok(max_seq)
    }

    /// Count total audit entries (for diagnostics).
    pub fn audit_entry_count(&self) -> crate::Result<u64> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(AUDIT_LOG)
            .map_err(|e| catalog_err("open audit_log", e))?;
        table.len().map_err(|e| catalog_err("count audit", e))
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
