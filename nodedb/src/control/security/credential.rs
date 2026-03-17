use std::collections::HashMap;
use std::sync::RwLock;

use crate::types::TenantId;

use super::identity::{AuthMethod, AuthenticatedIdentity, Role};

/// A stored user record.
#[derive(Debug, Clone)]
pub struct UserRecord {
    pub user_id: u64,
    pub username: String,
    pub tenant_id: TenantId,
    /// Argon2id password hash (PHC string format: `$argon2id$v=19$m=...&t=...&p=...$<salt>$<hash>`).
    pub password_hash: String,
    /// Salt used for SCRAM-SHA-256 (16 bytes, stored as raw bytes).
    pub scram_salt: Vec<u8>,
    /// SCRAM-SHA-256 salted password (for pgwire auth).
    pub scram_salted_password: Vec<u8>,
    pub roles: Vec<Role>,
    pub is_superuser: bool,
    pub is_active: bool,
}

/// In-memory credential store.
///
/// Bootstrap: loads superuser from config. Future: backed by `_system.users`
/// in the sparse engine for persistence.
///
/// Thread-safe — shared across Tokio tasks on the Control Plane.
pub struct CredentialStore {
    /// Username → UserRecord lookup.
    users: RwLock<HashMap<String, UserRecord>>,
    next_user_id: RwLock<u64>,
}

impl Default for CredentialStore {
    fn default() -> Self {
        Self::new()
    }
}

/// Acquire a read lock, converting poisoning into `crate::Error::Internal`.
fn read_lock<T>(lock: &RwLock<T>) -> crate::Result<std::sync::RwLockReadGuard<'_, T>> {
    lock.read().map_err(|e| {
        tracing::error!("credential store read lock poisoned: {e}");
        crate::Error::Internal {
            detail: "credential store lock poisoned".into(),
        }
    })
}

/// Acquire a write lock, converting poisoning into `crate::Error::Internal`.
fn write_lock<T>(lock: &RwLock<T>) -> crate::Result<std::sync::RwLockWriteGuard<'_, T>> {
    lock.write().map_err(|e| {
        tracing::error!("credential store write lock poisoned: {e}");
        crate::Error::Internal {
            detail: "credential store lock poisoned".into(),
        }
    })
}

impl CredentialStore {
    pub fn new() -> Self {
        Self {
            users: RwLock::new(HashMap::new()),
            next_user_id: RwLock::new(1),
        }
    }

    /// Bootstrap the superuser from config. Called once on startup.
    pub fn bootstrap_superuser(&self, username: &str, password: &str) -> crate::Result<()> {
        let salt = generate_scram_salt();
        let scram_salted_password = compute_scram_salted_password(password, &salt);
        let password_hash = hash_password_argon2(password)?;

        let user_id = {
            let mut next = write_lock(&self.next_user_id)?;
            let id = *next;
            *next += 1;
            id
        };

        let record = UserRecord {
            user_id,
            username: username.to_string(),
            tenant_id: TenantId::new(0), // Superuser spans all tenants.
            password_hash,
            scram_salt: salt,
            scram_salted_password,
            roles: vec![Role::Superuser],
            is_superuser: true,
            is_active: true,
        };

        write_lock(&self.users)?.insert(username.to_string(), record);

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

        let user_id = {
            let mut next = write_lock(&self.next_user_id)?;
            let id = *next;
            *next += 1;
            id
        };

        let is_superuser = roles.contains(&Role::Superuser);
        let record = UserRecord {
            user_id,
            username: username.to_string(),
            tenant_id,
            password_hash,
            scram_salt: salt,
            scram_salted_password,
            roles,
            is_superuser,
            is_active: true,
        };

        users.insert(username.to_string(), record);
        Ok(user_id)
    }

    /// Look up a user by username. Returns None if not found or inactive.
    pub fn get_user(&self, username: &str) -> Option<UserRecord> {
        let users = read_lock(&self.users).ok()?;
        users.get(username).filter(|u| u.is_active).cloned()
    }

    /// Get the SCRAM salt and salted password for pgwire SCRAM auth.
    pub fn get_scram_credentials(&self, username: &str) -> Option<(Vec<u8>, Vec<u8>)> {
        let users = read_lock(&self.users).ok()?;
        users
            .get(username)
            .filter(|u| u.is_active)
            .map(|u| (u.scram_salt.clone(), u.scram_salted_password.clone()))
    }

    /// Verify a cleartext password against the stored hash.
    pub fn verify_password(&self, username: &str, password: &str) -> bool {
        let users = match read_lock(&self.users) {
            Ok(u) => u,
            Err(_) => {
                // Lock poisoned — perform dummy hash to avoid timing oracle.
                let _ = hash_password_argon2(password);
                return false;
            }
        };
        match users.get(username).filter(|u| u.is_active) {
            Some(record) => verify_argon2(&record.password_hash, password),
            None => {
                // Constant-time: hash anyway to prevent user enumeration.
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

    /// List all active usernames (for admin).
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

    /// Deactivate a user (soft delete).
    pub fn deactivate_user(&self, username: &str) -> crate::Result<bool> {
        let mut users = write_lock(&self.users)?;
        if let Some(record) = users.get_mut(username) {
            record.is_active = false;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Check if any users exist (used to determine if bootstrap is needed).
    pub fn is_empty(&self) -> bool {
        read_lock(&self.users).map(|u| u.is_empty()).unwrap_or(true)
    }
}

// ── Password hashing ───────────────────────────────────────────────

use argon2::Argon2;
use argon2::password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString, rand_core::OsRng};

/// Generate a 16-byte cryptographic random salt for SCRAM.
fn generate_scram_salt() -> Vec<u8> {
    use argon2::password_hash::rand_core::RngCore;
    let mut salt = vec![0u8; 16];
    OsRng.fill_bytes(&mut salt);
    salt
}

/// Compute SCRAM-SHA-256 SaltedPassword for pgwire auth.
///
/// SaltedPassword := Hi(Normalize(password), salt, iterations)
/// where Hi is PBKDF2-HMAC-SHA256. This is a protocol requirement —
/// SCRAM mandates PBKDF2, not Argon2.
fn compute_scram_salted_password(password: &str, salt: &[u8]) -> Vec<u8> {
    pgwire::api::auth::sasl::scram::gen_salted_password(password, salt, 4096)
}

/// Hash a password with Argon2id for storage.
///
/// Uses the PHC string format: `$argon2id$v=19$m=19456,t=2,p=1$<salt>$<hash>`
/// This is the password verification hash — used for `verify_password()`,
/// API key auth, HTTP auth. Separate from the SCRAM salted password.
fn hash_password_argon2(password: &str) -> crate::Result<String> {
    let salt = SaltString::generate(&mut OsRng);
    let argon2 = Argon2::default();
    let hash = argon2
        .hash_password(password.as_bytes(), &salt)
        .map_err(|e| crate::Error::Internal {
            detail: format!("argon2 hashing failed: {e}"),
        })?;
    Ok(hash.to_string())
}

/// Verify a password against a stored Argon2id hash (PHC string format).
///
/// Uses constant-time comparison internally.
fn verify_argon2(stored_hash: &str, password: &str) -> bool {
    let parsed = match PasswordHash::new(stored_hash) {
        Ok(h) => h,
        Err(_) => return false,
    };
    Argon2::default()
        .verify_password(password.as_bytes(), &parsed)
        .is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bootstrap_superuser() {
        let store = CredentialStore::new();
        store.bootstrap_superuser("admin", "secret123").unwrap();

        let user = store.get_user("admin").expect("superuser should exist");
        assert!(user.is_superuser);
        assert!(user.is_active);
        assert_eq!(user.tenant_id, TenantId::new(0));
        assert!(user.roles.contains(&Role::Superuser));
    }

    #[test]
    fn verify_password_correct() {
        let store = CredentialStore::new();
        store
            .bootstrap_superuser("admin", "correct_password")
            .unwrap();
        assert!(store.verify_password("admin", "correct_password"));
    }

    #[test]
    fn verify_password_wrong() {
        let store = CredentialStore::new();
        store
            .bootstrap_superuser("admin", "correct_password")
            .unwrap();
        assert!(!store.verify_password("admin", "wrong_password"));
    }

    #[test]
    fn verify_password_nonexistent_user() {
        let store = CredentialStore::new();
        assert!(!store.verify_password("nobody", "password"));
    }

    #[test]
    fn create_user() {
        let store = CredentialStore::new();
        let id = store
            .create_user("alice", "pass", TenantId::new(1), vec![Role::ReadWrite])
            .unwrap();
        assert!(id > 0);

        let user = store.get_user("alice").unwrap();
        assert_eq!(user.tenant_id, TenantId::new(1));
        assert!(!user.is_superuser);
        assert!(user.roles.contains(&Role::ReadWrite));
    }

    #[test]
    fn duplicate_user_rejected() {
        let store = CredentialStore::new();
        store
            .create_user("alice", "pass", TenantId::new(1), vec![])
            .unwrap();
        let result = store.create_user("alice", "pass2", TenantId::new(1), vec![]);
        assert!(result.is_err());
    }

    #[test]
    fn deactivate_user() {
        let store = CredentialStore::new();
        store
            .create_user("bob", "pass", TenantId::new(1), vec![])
            .unwrap();
        assert!(store.get_user("bob").is_some());

        store.deactivate_user("bob").unwrap();
        assert!(store.get_user("bob").is_none());
    }

    #[test]
    fn scram_credentials_available() {
        let store = CredentialStore::new();
        store.bootstrap_superuser("admin", "secret").unwrap();

        let (salt, salted_pw) = store.get_scram_credentials("admin").unwrap();
        assert_eq!(salt.len(), 16);
        assert!(!salted_pw.is_empty());
    }

    #[test]
    fn to_identity() {
        let store = CredentialStore::new();
        store.bootstrap_superuser("admin", "secret").unwrap();

        let identity = store.to_identity("admin", AuthMethod::ScramSha256).unwrap();
        assert!(identity.is_superuser);
        assert_eq!(identity.username, "admin");
        assert_eq!(identity.auth_method, AuthMethod::ScramSha256);
    }
}
