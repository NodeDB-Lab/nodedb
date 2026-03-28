use std::collections::HashMap;
use std::path::Path;
use std::sync::RwLock;
use tracing::info;

use crate::types::TenantId;

use super::super::catalog::SystemCatalog;
use super::super::identity::{AuthMethod, AuthenticatedIdentity, Role};
use super::super::time::now_secs;
use super::hash::{
    compute_md5_hash, compute_scram_salted_password, generate_scram_salt, hash_password_argon2,
    verify_argon2,
};
use super::lockout::LoginAttemptTracker;
use super::record::UserRecord;

/// Credential store with in-memory cache and redb persistence.
///
/// Reads hit the in-memory cache (fast). Writes go to redb first (ACID),
/// then update the cache. On startup, all records are loaded from redb.
///
/// Lives on the Control Plane (Send + Sync).
pub struct CredentialStore {
    pub(super) users: RwLock<HashMap<String, UserRecord>>,
    pub(super) next_user_id: RwLock<u64>,
    pub(super) catalog: Option<SystemCatalog>,
    /// Failed login tracking (in-memory only — clears on restart).
    pub(super) login_attempts: RwLock<HashMap<String, LoginAttemptTracker>>,
    /// Max failed logins before lockout (0 = disabled).
    pub(super) max_failed_logins: u32,
    /// Lockout duration.
    pub(super) lockout_duration: std::time::Duration,
    /// Password expiry in seconds (0 = no expiry).
    pub(super) password_expiry_secs: u64,
}

impl Default for CredentialStore {
    fn default() -> Self {
        Self::new()
    }
}

pub(super) fn read_lock<T>(lock: &RwLock<T>) -> crate::Result<std::sync::RwLockReadGuard<'_, T>> {
    lock.read().map_err(|e| {
        tracing::error!("credential store read lock poisoned: {e}");
        crate::Error::Internal {
            detail: "credential store lock poisoned".into(),
        }
    })
}

pub(super) fn write_lock<T>(lock: &RwLock<T>) -> crate::Result<std::sync::RwLockWriteGuard<'_, T>> {
    lock.write().map_err(|e| {
        tracing::error!("credential store write lock poisoned: {e}");
        crate::Error::Internal {
            detail: "credential store lock poisoned".into(),
        }
    })
}

impl CredentialStore {
    /// Create an in-memory-only credential store (for tests).
    pub fn new() -> Self {
        Self {
            users: RwLock::new(HashMap::new()),
            next_user_id: RwLock::new(1),
            catalog: None,
            login_attempts: RwLock::new(HashMap::new()),
            max_failed_logins: 0, // Disabled in tests.
            lockout_duration: std::time::Duration::from_secs(300),
            password_expiry_secs: 0,
        }
    }

    /// Open a persistent credential store backed by redb.
    ///
    /// `path` is the system catalog file (e.g. `{data_dir}/system.redb`).
    /// Loads all existing users into the in-memory cache.
    pub fn open(path: &Path) -> crate::Result<Self> {
        let catalog = SystemCatalog::open(path)?;

        // Load existing users.
        let stored_users = catalog.load_all_users()?;
        let next_id = catalog.load_next_user_id()?;

        let mut users = HashMap::with_capacity(stored_users.len());
        for stored in stored_users {
            let record = UserRecord::from_stored(stored);
            users.insert(record.username.clone(), record);
        }

        let count = users.len();
        if count > 0 {
            info!(count, "loaded users from system catalog");
        }

        Ok(Self {
            users: RwLock::new(users),
            next_user_id: RwLock::new(next_id),
            catalog: Some(catalog),
            login_attempts: RwLock::new(HashMap::new()),
            max_failed_logins: 0,
            lockout_duration: std::time::Duration::from_secs(300),
            password_expiry_secs: 0,
        })
    }

    // Lockout methods (set_lockout_policy, check_lockout, record_login_failure,
    // record_login_success) are in lockout.rs

    /// Persist a user record to the catalog (if persistent).
    /// Automatically updates `updated_at` timestamp.
    fn persist_user(&self, record: &mut UserRecord) -> crate::Result<()> {
        record.updated_at = now_secs();
        if let Some(ref catalog) = self.catalog {
            catalog.put_user(&record.to_stored())?;
        }
        Ok(())
    }

    /// Persist the next_user_id counter (if persistent).
    fn persist_next_id(&self, id: u64) -> crate::Result<()> {
        if let Some(ref catalog) = self.catalog {
            catalog.save_next_user_id(id)?;
        }
        Ok(())
    }

    /// Compute password expiry timestamp from current config.
    fn compute_expiry(&self) -> u64 {
        if self.password_expiry_secs > 0 {
            now_secs() + self.password_expiry_secs
        } else {
            0
        }
    }

    fn alloc_user_id(&self) -> crate::Result<u64> {
        let mut next = write_lock(&self.next_user_id)?;
        let id = *next;
        *next += 1;
        self.persist_next_id(*next)?;
        Ok(id)
    }

    /// Bootstrap the superuser from config. Called once on startup.
    /// If the user already exists (loaded from catalog), updates the password.
    pub fn bootstrap_superuser(&self, username: &str, password: &str) -> crate::Result<()> {
        let salt = generate_scram_salt();
        let scram_salted_password = compute_scram_salted_password(password, &salt);
        let password_hash = hash_password_argon2(password)?;

        let mut users = write_lock(&self.users)?;

        if let Some(existing) = users.get_mut(username) {
            // User exists from catalog — update password and ensure active + superuser.
            existing.password_hash = password_hash;
            existing.scram_salt = salt;
            existing.scram_salted_password = scram_salted_password;
            existing.is_superuser = true;
            existing.is_active = true;
            if !existing.roles.contains(&Role::Superuser) {
                existing.roles.push(Role::Superuser);
            }
            self.persist_user(existing)?;
        } else {
            let user_id = self.alloc_user_id()?;
            let mut record = UserRecord {
                user_id,
                username: username.to_string(),
                tenant_id: TenantId::new(0),
                password_hash,
                scram_salt: salt,
                scram_salted_password,
                roles: vec![Role::Superuser],
                is_superuser: true,
                is_active: true,
                is_service_account: false,
                created_at: now_secs(),
                updated_at: now_secs(),
                password_expires_at: self.compute_expiry(),
                md5_hash: compute_md5_hash(username, password),
            };
            self.persist_user(&mut record)?;
            users.insert(username.to_string(), record);
        }

        Ok(())
    }

    /// Create a new user. Returns the user_id.
    pub fn create_user(
        &self,
        username: &str,
        password: &str,
        tenant_id: TenantId,
        roles: Vec<Role>,
    ) -> crate::Result<u64> {
        let mut users = write_lock(&self.users)?;
        if users.contains_key(username) {
            return Err(crate::Error::BadRequest {
                detail: format!("user '{username}' already exists"),
            });
        }

        let salt = generate_scram_salt();
        let scram_salted_password = compute_scram_salted_password(password, &salt);
        let password_hash = hash_password_argon2(password)?;
        let user_id = self.alloc_user_id()?;

        let is_superuser = roles.contains(&Role::Superuser);
        let mut record = UserRecord {
            user_id,
            username: username.to_string(),
            tenant_id,
            password_hash,
            scram_salt: salt,
            scram_salted_password,
            roles,
            is_superuser,
            is_active: true,
            is_service_account: false,
            created_at: now_secs(),
            updated_at: now_secs(),
            password_expires_at: self.compute_expiry(),
            md5_hash: compute_md5_hash(username, password),
        };

        self.persist_user(&mut record)?;
        users.insert(username.to_string(), record);
        Ok(user_id)
    }

    /// Create a service account. No password — can only authenticate via API keys.
    /// Returns the user_id.
    pub fn create_service_account(
        &self,
        name: &str,
        tenant_id: TenantId,
        roles: Vec<Role>,
    ) -> crate::Result<u64> {
        let mut users = write_lock(&self.users)?;
        if users.contains_key(name) {
            return Err(crate::Error::BadRequest {
                detail: format!("user or service account '{name}' already exists"),
            });
        }

        let user_id = self.alloc_user_id()?;
        let is_superuser = roles.contains(&Role::Superuser);
        let mut record = UserRecord {
            user_id,
            username: name.to_string(),
            tenant_id,
            password_hash: String::new(), // No password.
            scram_salt: Vec::new(),
            scram_salted_password: Vec::new(),
            roles,
            is_superuser,
            is_active: true,
            is_service_account: true,
            created_at: now_secs(),
            updated_at: now_secs(),
            password_expires_at: 0,
            md5_hash: String::new(), // Service accounts have no password.
        };

        self.persist_user(&mut record)?;
        users.insert(name.to_string(), record);
        Ok(user_id)
    }

    /// Look up a user by username. Returns None if not found or inactive.
    pub fn get_user(&self, username: &str) -> Option<UserRecord> {
        let users = read_lock(&self.users).ok()?;
        users.get(username).filter(|u| u.is_active).cloned()
    }

    /// Get the SCRAM salt and salted password for pgwire SCRAM auth.
    /// Returns None for service accounts (can't login via pgwire) or expired passwords.
    pub fn get_scram_credentials(&self, username: &str) -> Option<(Vec<u8>, Vec<u8>)> {
        let users = read_lock(&self.users).ok()?;
        users
            .get(username)
            .filter(|u| u.is_active && !u.is_service_account)
            .filter(|u| {
                // Check password expiry.
                if u.password_expires_at > 0 && now_secs() >= u.password_expires_at {
                    tracing::warn!(username = u.username, "password expired, login denied");
                    return false;
                }
                true
            })
            .map(|u| (u.scram_salt.clone(), u.scram_salted_password.clone()))
    }

    /// Get the MD5 hash for pgwire MD5 auth.
    /// Returns `md5(password + username)` as stored during user creation.
    /// Returns None for service accounts, expired passwords, or missing users.
    pub fn get_md5_hash(&self, username: &str) -> Option<String> {
        let users = read_lock(&self.users).ok()?;
        users
            .get(username)
            .filter(|u| u.is_active && !u.is_service_account)
            .filter(|u| {
                if u.password_expires_at > 0 && now_secs() >= u.password_expires_at {
                    tracing::warn!(username = u.username, "password expired, MD5 login denied");
                    return false;
                }
                true
            })
            .filter(|u| !u.md5_hash.is_empty())
            .map(|u| u.md5_hash.clone())
    }

    /// Verify a cleartext password against the stored Argon2 hash.
    pub fn verify_password(&self, username: &str, password: &str) -> bool {
        let users = match read_lock(&self.users) {
            Ok(u) => u,
            Err(_) => {
                let _ = hash_password_argon2(password);
                return false;
            }
        };
        match users.get(username).filter(|u| u.is_active) {
            Some(record) => verify_argon2(&record.password_hash, password),
            None => {
                let _ = hash_password_argon2(password);
                false
            }
        }
    }

    /// Build an `AuthenticatedIdentity` for a verified user.
    pub fn to_identity(&self, username: &str, method: AuthMethod) -> Option<AuthenticatedIdentity> {
        self.get_user(username).map(|record| AuthenticatedIdentity {
            user_id: record.user_id,
            username: record.username,
            tenant_id: record.tenant_id,
            auth_method: method,
            roles: record.roles,
            is_superuser: record.is_superuser,
        })
    }

    /// Deactivate a user (soft delete). Persists the change.
    pub fn deactivate_user(&self, username: &str) -> crate::Result<bool> {
        let mut users = write_lock(&self.users)?;
        if let Some(record) = users.get_mut(username) {
            record.is_active = false;
            self.persist_user(record)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Update a user's password. Recomputes both Argon2 hash and SCRAM credentials.
    pub fn update_password(&self, username: &str, password: &str) -> crate::Result<()> {
        let mut users = write_lock(&self.users)?;
        let record = users
            .get_mut(username)
            .ok_or_else(|| crate::Error::BadRequest {
                detail: format!("user '{username}' not found"),
            })?;
        if !record.is_active {
            return Err(crate::Error::BadRequest {
                detail: format!("user '{username}' is inactive"),
            });
        }
        let salt = generate_scram_salt();
        record.scram_salted_password = compute_scram_salted_password(password, &salt);
        record.scram_salt = salt;
        record.password_hash = hash_password_argon2(password)?;
        record.password_expires_at = self.compute_expiry();
        record.md5_hash = compute_md5_hash(username, password);
        self.persist_user(record)?;
        Ok(())
    }

    /// Replace all roles for a user.
    pub fn update_roles(&self, username: &str, roles: Vec<Role>) -> crate::Result<()> {
        let mut users = write_lock(&self.users)?;
        let record = users
            .get_mut(username)
            .ok_or_else(|| crate::Error::BadRequest {
                detail: format!("user '{username}' not found"),
            })?;
        record.is_superuser = roles.contains(&Role::Superuser);
        record.roles = roles;
        self.persist_user(record)?;
        Ok(())
    }

    /// Add a role to a user (if not already present).
    pub fn add_role(&self, username: &str, role: Role) -> crate::Result<()> {
        let mut users = write_lock(&self.users)?;
        let record = users
            .get_mut(username)
            .ok_or_else(|| crate::Error::BadRequest {
                detail: format!("user '{username}' not found"),
            })?;
        if !record.roles.contains(&role) {
            record.roles.push(role.clone());
            if matches!(role, Role::Superuser) {
                record.is_superuser = true;
            }
        }
        self.persist_user(record)?;
        Ok(())
    }

    /// Remove a role from a user.
    pub fn remove_role(&self, username: &str, role: &Role) -> crate::Result<()> {
        let mut users = write_lock(&self.users)?;
        let record = users
            .get_mut(username)
            .ok_or_else(|| crate::Error::BadRequest {
                detail: format!("user '{username}' not found"),
            })?;
        record.roles.retain(|r| r != role);
        if matches!(role, Role::Superuser) {
            record.is_superuser = false;
        }
        self.persist_user(record)?;
        Ok(())
    }

    /// List all active users with full details (for SHOW USERS).
    pub fn list_user_details(&self) -> Vec<UserRecord> {
        let users = match read_lock(&self.users) {
            Ok(u) => u,
            Err(_) => return Vec::new(),
        };
        users.values().filter(|u| u.is_active).cloned().collect()
    }

    /// List all active usernames.
    pub fn list_users(&self) -> Vec<String> {
        let users = match read_lock(&self.users) {
            Ok(u) => u,
            Err(_) => return Vec::new(),
        };
        users
            .values()
            .filter(|u| u.is_active)
            .map(|u| u.username.clone())
            .collect()
    }

    /// Check if any users exist.
    pub fn is_empty(&self) -> bool {
        read_lock(&self.users).map(|u| u.is_empty()).unwrap_or(true)
    }

    /// Access the underlying system catalog (for API key persistence).
    pub fn catalog(&self) -> &Option<SystemCatalog> {
        &self.catalog
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn in_memory_create_and_verify() {
        let store = CredentialStore::new();
        store.bootstrap_superuser("admin", "secret").unwrap();
        assert!(store.verify_password("admin", "secret"));
        assert!(!store.verify_password("admin", "wrong"));
    }

    #[test]
    fn persistent_create_and_reload() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("system.redb");

        // Create user in persistent store.
        {
            let store = CredentialStore::open(&path).unwrap();
            store
                .create_user("alice", "pass123", TenantId::new(1), vec![Role::ReadWrite])
                .unwrap();
            store.bootstrap_superuser("admin", "secret").unwrap();
        }

        // Reopen — users should survive.
        {
            let store = CredentialStore::open(&path).unwrap();
            let alice = store.get_user("alice").unwrap();
            assert_eq!(alice.tenant_id, TenantId::new(1));
            assert!(alice.roles.contains(&Role::ReadWrite));

            // Verify password still works after reload.
            assert!(store.verify_password("alice", "pass123"));

            // Superuser should also exist.
            let admin = store.get_user("admin").unwrap();
            assert!(admin.is_superuser);
        }
    }

    #[test]
    fn persistent_deactivate_survives_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("system.redb");

        {
            let store = CredentialStore::open(&path).unwrap();
            store
                .create_user("bob", "pass", TenantId::new(1), vec![Role::ReadOnly])
                .unwrap();
            store.deactivate_user("bob").unwrap();
        }

        {
            let store = CredentialStore::open(&path).unwrap();
            // Deactivated user should not be visible.
            assert!(store.get_user("bob").is_none());
        }
    }

    #[test]
    fn persistent_role_changes_survive_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("system.redb");

        {
            let store = CredentialStore::open(&path).unwrap();
            store
                .create_user("carol", "pass", TenantId::new(1), vec![Role::ReadOnly])
                .unwrap();
            store.add_role("carol", Role::ReadWrite).unwrap();
            store.remove_role("carol", &Role::ReadOnly).unwrap();
        }

        {
            let store = CredentialStore::open(&path).unwrap();
            let carol = store.get_user("carol").unwrap();
            assert!(carol.roles.contains(&Role::ReadWrite));
            assert!(!carol.roles.contains(&Role::ReadOnly));
        }
    }

    #[test]
    fn persistent_password_change_survives_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("system.redb");

        {
            let store = CredentialStore::open(&path).unwrap();
            store
                .create_user("dave", "old_pass", TenantId::new(1), vec![Role::ReadWrite])
                .unwrap();
            store.update_password("dave", "new_pass").unwrap();
        }

        {
            let store = CredentialStore::open(&path).unwrap();
            assert!(store.verify_password("dave", "new_pass"));
            assert!(!store.verify_password("dave", "old_pass"));
        }
    }

    // Lockout tests are in lockout.rs

    #[test]
    fn user_id_counter_persists() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("system.redb");

        let first_id;
        {
            let store = CredentialStore::open(&path).unwrap();
            first_id = store
                .create_user("u1", "p", TenantId::new(1), vec![])
                .unwrap();
            store
                .create_user("u2", "p", TenantId::new(1), vec![])
                .unwrap();
        }

        {
            let store = CredentialStore::open(&path).unwrap();
            let next_id = store
                .create_user("u3", "p", TenantId::new(1), vec![])
                .unwrap();
            // Should continue from where we left off, not restart at 1.
            assert!(next_id > first_id + 1);
        }
    }
}
