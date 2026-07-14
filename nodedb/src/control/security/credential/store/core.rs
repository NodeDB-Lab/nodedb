// SPDX-License-Identifier: BUSL-1.1

//! `CredentialStore` struct + constructors + private helpers.
//!
//! Other concerns (crud, auth, list, replication) live in sibling
//! files under `store/` and extend this struct via their own `impl`
//! blocks. The struct fields are `pub(super)` so those siblings can
//! reach them without leaking internals beyond `credential`.

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, RwLock};
use tracing::info;

use crate::types::TenantId;

use super::super::super::catalog::SystemCatalog;
use super::super::super::identity::Role;
use super::super::super::time::now_secs;
use crate::config::auth::Argon2Config;

use super::super::hash::{
    compute_scram_salted_password, generate_scram_salt, hash_password_argon2,
};
use super::super::lockout::LoginAttemptTracker;
use super::super::record::{UserRecord, validate_stored_user_credentials};

/// Credential store with in-memory cache and redb persistence.
///
/// Reads hit the in-memory cache (fast). Writes go to redb first
/// (ACID), then update the cache. On startup, all records are
/// loaded from redb.
///
/// Lives on the Control Plane (`Send + Sync`).
pub struct CredentialStore {
    pub(in crate::control::security::credential) users: RwLock<HashMap<String, UserRecord>>,
    pub(in crate::control::security::credential) next_user_id: RwLock<u64>,
    pub(in crate::control::security::credential) catalog: SystemCatalog,
    /// Failed login tracking (in-memory only — clears on restart).
    pub(in crate::control::security::credential) login_attempts:
        RwLock<HashMap<String, LoginAttemptTracker>>,
    /// Max failed logins before lockout (0 = disabled).
    pub(in crate::control::security::credential) max_failed_logins: u32,
    /// Lockout duration.
    pub(in crate::control::security::credential) lockout_duration: std::time::Duration,
    /// Password expiry in seconds (0 = no expiry).
    pub(in crate::control::security::credential) password_expiry_secs: u64,
    /// Grace period in days after expiry during which login is still allowed
    /// but a warning is emitted. 0 = hard cutoff (no grace).
    pub(in crate::control::security::credential) password_expiry_grace_days: u32,
    /// Argon2id hashing parameters from server config.
    pub(in crate::control::security::credential) argon2_config: Argon2Config,
    /// Per-user credential version counters.  Bumped on every mutation.
    /// `RwLock` guards the map; the `AtomicU64` inside allows lock-free reads
    /// once the slot is known to exist.
    pub(in crate::control::security::credential) versions: RwLock<HashMap<u64, Arc<AtomicU64>>>,
    /// Session-invalidation bus (None until `set_buses` is called; None in test stores).
    pub(in crate::control::security::credential) si_bus:
        std::sync::OnceLock<Arc<crate::control::security::buses::SessionInvalidationBus>>,
    /// User-change bus (None until `set_buses` is called; None in test stores).
    pub(in crate::control::security::credential) uc_bus:
        std::sync::OnceLock<Arc<crate::control::security::buses::UserChangeBus>>,
}

pub(in crate::control::security::credential) fn read_lock<T>(
    lock: &RwLock<T>,
) -> crate::Result<std::sync::RwLockReadGuard<'_, T>> {
    lock.read().map_err(|e| {
        tracing::error!("credential store read lock poisoned: {e}");
        crate::Error::Internal {
            detail: "credential store lock poisoned".into(),
        }
    })
}

pub(in crate::control::security::credential) fn write_lock<T>(
    lock: &RwLock<T>,
) -> crate::Result<std::sync::RwLockWriteGuard<'_, T>> {
    lock.write().map_err(|e| {
        tracing::error!("credential store write lock poisoned: {e}");
        crate::Error::Internal {
            detail: "credential store lock poisoned".into(),
        }
    })
}

/// The principal receiving a password assignment.
pub(in crate::control::security::credential) enum PasswordPrincipal {
    New,
    Existing { is_service_account: bool },
}

/// Reject password assignments that cannot produce a valid user credential.
pub(in crate::control::security::credential) fn validate_password_assignment(
    password: &str,
    principal: PasswordPrincipal,
) -> crate::Result<()> {
    if password.is_empty() {
        return Err(crate::Error::BadRequest {
            detail: "password must not be empty".into(),
        });
    }
    if matches!(
        principal,
        PasswordPrincipal::Existing {
            is_service_account: true
        }
    ) {
        return Err(crate::Error::BadRequest {
            detail: "cannot assign a password to a service account".into(),
        });
    }
    Ok(())
}

impl CredentialStore {
    /// Create an in-memory-only credential store backed by an in-memory
    /// system catalog (for tests and in-process fixtures).
    pub fn new() -> crate::Result<Self> {
        Ok(Self {
            users: RwLock::new(HashMap::new()),
            next_user_id: RwLock::new(1),
            catalog: SystemCatalog::open_in_memory()?,
            login_attempts: RwLock::new(HashMap::new()),
            max_failed_logins: 0,
            lockout_duration: std::time::Duration::from_secs(300),
            password_expiry_secs: 0,
            password_expiry_grace_days: 0,
            argon2_config: Argon2Config::default(),
            versions: RwLock::new(HashMap::new()),
            si_bus: std::sync::OnceLock::new(),
            uc_bus: std::sync::OnceLock::new(),
        })
    }

    /// Open a persistent credential store backed by redb.
    ///
    /// `path` is the system catalog file (e.g. `{data_dir}/system.redb`).
    /// Loads all existing users into the in-memory cache.
    pub fn open(path: &Path) -> crate::Result<Self> {
        let catalog = SystemCatalog::open(path)?;

        let stored_users = catalog.load_all_users()?;
        let next_id = catalog.load_next_user_id()?;
        let argon2_config = Argon2Config::default();

        let mut users = HashMap::with_capacity(stored_users.len());
        for stored in stored_users {
            validate_stored_user_credentials(&stored, &argon2_config)?;
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
            catalog,
            login_attempts: RwLock::new(HashMap::new()),
            max_failed_logins: 0,
            lockout_duration: std::time::Duration::from_secs(300),
            password_expiry_secs: 0,
            password_expiry_grace_days: 0,
            argon2_config,
            versions: RwLock::new(HashMap::new()),
            si_bus: std::sync::OnceLock::new(),
            uc_bus: std::sync::OnceLock::new(),
        })
    }

    /// Persist a user record to the catalog (if persistent).
    /// Automatically updates `updated_at` timestamp.
    pub(in crate::control::security::credential) fn persist_user(
        &self,
        record: &mut UserRecord,
    ) -> crate::Result<()> {
        record.updated_at = now_secs();
        self.catalog.put_user(&record.to_stored())?;
        Ok(())
    }

    /// Persist the next_user_id counter (if persistent).
    pub(in crate::control::security::credential) fn persist_next_id(
        &self,
        id: u64,
    ) -> crate::Result<()> {
        self.catalog.save_next_user_id(id)?;
        Ok(())
    }

    /// Compute password expiry timestamp from current config.
    pub(in crate::control::security::credential) fn compute_expiry(&self) -> u64 {
        if self.password_expiry_secs > 0 {
            now_secs() + self.password_expiry_secs
        } else {
            0
        }
    }

    pub(in crate::control::security::credential) fn alloc_user_id(&self) -> crate::Result<u64> {
        let mut next = write_lock(&self.next_user_id)?;
        let id = *next;
        *next += 1;
        self.persist_next_id(*next)?;
        Ok(id)
    }

    /// Wire in the security buses.  Called once from `SharedState` construction
    /// after the `CredentialStore` has been wrapped in `Arc`.  May be called
    /// via `&self` because the fields use `OnceLock`.  Silently ignored if
    /// called more than once (test helpers that don't need buses skip this).
    pub fn set_buses(
        &self,
        si_bus: Arc<crate::control::security::buses::SessionInvalidationBus>,
        uc_bus: Arc<crate::control::security::buses::UserChangeBus>,
    ) {
        let _ = self.si_bus.set(si_bus);
        let _ = self.uc_bus.set(uc_bus);
    }

    /// Subscribe to user-change events.  Returns a broadcast receiver that
    /// fires whenever any user is mutated.  Primarily used in tests.
    pub fn subscribe_user_changes(
        &self,
    ) -> tokio::sync::broadcast::Receiver<crate::control::security::buses::UserChanged> {
        match self.uc_bus.get() {
            Some(bus) => bus.subscribe(),
            None => {
                // No bus wired — return a fresh dead-end channel.
                tokio::sync::broadcast::channel(1).1
            }
        }
    }

    /// Subscribe to session-invalidation events.  Returns a broadcast receiver
    /// that fires whenever a mutation triggers a hard or soft revoke.  Primarily
    /// used in tests.
    pub fn subscribe_session_invalidation(
        &self,
    ) -> tokio::sync::broadcast::Receiver<crate::control::security::buses::SessionInvalidated> {
        match self.si_bus.get() {
            Some(bus) => bus.subscribe(),
            None => tokio::sync::broadcast::channel(1).1,
        }
    }

    /// Bump the per-user version counter, inserting the slot if absent.
    /// Returns the new version value.
    pub(in crate::control::security::credential) fn bump_version(
        &self,
        user_id: u64,
    ) -> crate::Result<u64> {
        // Fast path: slot already exists — just fetch_add.
        {
            let map = read_lock(&self.versions)?;
            if let Some(ctr) = map.get(&user_id) {
                return Ok(ctr.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1);
            }
        }
        // Slow path: insert under write-lock (double-checked).
        let mut map = write_lock(&self.versions)?;
        let ctr = map
            .entry(user_id)
            .or_insert_with(|| Arc::new(AtomicU64::new(0)));
        Ok(ctr.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1)
    }

    /// Return the current version for a user.  Returns 0 if the user has
    /// never had a mutation recorded (e.g. loaded from a previous store that
    /// pre-dates versions).
    pub fn current_version(&self, user_id: u64) -> u64 {
        let map = self.versions.read().unwrap_or_else(|p| p.into_inner());
        match map.get(&user_id) {
            Some(ctr) => ctr.load(std::sync::atomic::Ordering::Relaxed),
            None => 0,
        }
    }

    /// Single-funnel for all user mutations that touch persisted state.
    ///
    /// In order:
    /// 1. Persist the `UserRecord` to redb via `persist_user`.
    /// 2. Bump the per-user version counter.
    /// 3. Publish `UserChanged` on the user-change bus.
    /// 4. If `invalidation` is `Some`, publish `SessionInvalidated` on the
    ///    session-invalidation bus.
    ///
    /// Both bus publishes are fire-and-forget — a return value of 0 (no
    /// active subscribers) is silently accepted.
    pub(in crate::control::security::credential) fn commit_user_mutation(
        &self,
        record: &mut UserRecord,
        invalidation: Option<crate::control::security::buses::SessionInvalidationReason>,
    ) -> crate::Result<()> {
        let user_id = record.user_id;

        // 1. Persist.
        self.persist_user(record)?;

        // 2. Version bump.
        self.bump_version(user_id)?;

        // 3. UserChanged.
        if let Some(bus) = self.uc_bus.get() {
            bus.publish(crate::control::security::buses::UserChanged { user_id });
        }

        // 4. SessionInvalidated (if reason given).
        if let Some(reason) = invalidation
            && let Some(bus) = self.si_bus.get()
        {
            bus.publish(crate::control::security::buses::SessionInvalidated { user_id, reason });
        }

        Ok(())
    }

    /// Fully retire a dropped user's persisted + in-process state.
    ///
    /// In order:
    /// 1. Delete the record from the redb catalog (idempotent — a
    ///    missing key is a harmless no-op).
    /// 2. Publish `UserChanged` on the user-change bus.
    /// 3. Publish `SessionInvalidated` with `UserDropped` so open
    ///    sessions are hard-revoked.
    /// 4. Discard the per-user version counter — the username may be
    ///    recreated later under a fresh `user_id`.
    ///
    /// The caller must have already removed the in-memory cache entry.
    pub(in crate::control::security::credential) fn purge_user(
        &self,
        record: &UserRecord,
    ) -> crate::Result<()> {
        let user_id = record.user_id;

        // 1. Delete from the persistent catalog.
        self.catalog.delete_user(&record.username)?;

        // 2. UserChanged.
        if let Some(bus) = self.uc_bus.get() {
            bus.publish(crate::control::security::buses::UserChanged { user_id });
        }

        // 3. SessionInvalidated — hard-revoke open sessions.
        if let Some(bus) = self.si_bus.get() {
            bus.publish(crate::control::security::buses::SessionInvalidated {
                user_id,
                reason: crate::control::security::buses::SessionInvalidationReason::UserDropped,
            });
        }

        // 4. Discard the per-user version counter.
        write_lock(&self.versions)?.remove(&user_id);

        Ok(())
    }

    /// Bootstrap the superuser from config. Called once on startup.
    /// If the user already exists (loaded from catalog), updates
    /// the password.
    pub fn bootstrap_superuser(&self, username: &str, password: &str) -> crate::Result<()> {
        let (observed_user_id, principal) = {
            let users = read_lock(&self.users)?;
            match users.get(username) {
                Some(record) => (
                    Some(record.user_id),
                    PasswordPrincipal::Existing {
                        is_service_account: record.is_service_account,
                    },
                ),
                None => (None, PasswordPrincipal::New),
            }
        };
        validate_password_assignment(password, principal)?;

        let salt = generate_scram_salt();
        let scram_salted_password = compute_scram_salted_password(password, &salt);
        let password_hash = hash_password_argon2(password, &self.argon2_config)?;

        let mut users = write_lock(&self.users)?;

        if observed_user_id.is_some() && !users.contains_key(username) {
            return Err(crate::Error::BadRequest {
                detail: format!("user '{username}' changed while password was being prepared"),
            });
        }

        if observed_user_id.is_none() && users.contains_key(username) {
            return Err(crate::Error::BadRequest {
                detail: format!("user '{username}' changed while password was being prepared"),
            });
        }

        if let Some(existing) = users.get_mut(username) {
            if let Some(observed_user_id) = observed_user_id
                && existing.user_id != observed_user_id
            {
                return Err(crate::Error::BadRequest {
                    detail: format!("user '{username}' changed while password was being prepared"),
                });
            }
            // The account may have changed since the lock-free snapshot used
            // before Argon2. Re-check the live principal before mutation.
            validate_password_assignment(
                password,
                PasswordPrincipal::Existing {
                    is_service_account: existing.is_service_account,
                },
            )?;

            // User exists from catalog — update password and
            // ensure active + superuser.
            existing.password_hash = password_hash;
            existing.scram_salt = salt;
            existing.scram_salted_password = scram_salted_password;
            existing.is_superuser = true;
            existing.is_active = true;
            existing.must_change_password = false;
            existing.password_changed_at = now_secs();
            if !existing.roles.contains(&Role::Superuser) {
                existing.roles.push(Role::Superuser);
            }
            self.persist_user(existing)?;
        } else {
            let user_id = self.alloc_user_id()?;
            let now = now_secs();
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
                created_at: now,
                updated_at: now,
                password_expires_at: self.compute_expiry(),
                must_change_password: false,
                password_changed_at: now,
                default_database_id: 0,
                accessible_databases: vec![],
            };
            self.persist_user(&mut record)?;
            users.insert(username.to_string(), record);
        }

        Ok(())
    }
}

#[cfg(test)]
pub(super) fn assert_bad_request(error: crate::Error) {
    match error {
        crate::Error::BadRequest { .. } => {}
        other => panic!("expected BadRequest, got {other:?}"),
    }
}

#[cfg(test)]
pub(super) fn assert_user_unchanged(before: &UserRecord, after: &UserRecord) {
    assert_eq!(after.user_id, before.user_id);
    assert_eq!(after.username, before.username);
    assert_eq!(after.tenant_id, before.tenant_id);
    assert_eq!(after.password_hash, before.password_hash);
    assert_eq!(after.scram_salt, before.scram_salt);
    assert_eq!(after.scram_salted_password, before.scram_salted_password);
    assert_eq!(after.roles, before.roles);
    assert_eq!(after.is_superuser, before.is_superuser);
    assert_eq!(after.is_active, before.is_active);
    assert_eq!(after.is_service_account, before.is_service_account);
    assert_eq!(after.created_at, before.created_at);
    assert_eq!(after.updated_at, before.updated_at);
    assert_eq!(after.password_expires_at, before.password_expires_at);
    assert_eq!(after.must_change_password, before.must_change_password);
    assert_eq!(after.password_changed_at, before.password_changed_at);
    assert_eq!(after.default_database_id, before.default_database_id);
    assert_eq!(after.accessible_databases, before.accessible_databases);
}

#[cfg(test)]
mod tests {
    use super::{
        CredentialStore, assert_bad_request, assert_user_unchanged, compute_scram_salted_password,
        generate_scram_salt, hash_password_argon2,
    };
    use crate::config::auth::Argon2Config;
    use crate::control::security::catalog::{StoredUser, SystemCatalog};
    use crate::control::security::identity::Role;
    use crate::types::TenantId;

    #[test]
    fn bootstrap_superuser_rejects_empty_password_for_absent_user_without_allocation() {
        let store = CredentialStore::new().expect("in-memory credential store");
        let next_user_id = *store.next_user_id.read().expect("next user ID lock");

        let error = store
            .bootstrap_superuser("empty-bootstrap", "")
            .expect_err("empty bootstrap password must be rejected");

        assert_bad_request(error);
        assert!(store.get_user("empty-bootstrap").is_none());
        assert_eq!(
            *store.next_user_id.read().expect("next user ID lock"),
            next_user_id,
            "rejected bootstrap must not allocate a user ID"
        );
        assert!(
            store
                .catalog()
                .get_user("empty-bootstrap")
                .expect("read catalog")
                .is_none(),
            "rejected bootstrap must not write the catalog"
        );
    }

    #[test]
    fn bootstrap_superuser_rejects_empty_password_for_existing_user_without_mutation() {
        let store = CredentialStore::new().expect("in-memory credential store");
        let user_id = store
            .create_user(
                "regular-bootstrap",
                "old-password",
                TenantId::new(3),
                vec![Role::ReadOnly],
            )
            .expect("create regular user");
        let before = store
            .get_user("regular-bootstrap")
            .expect("created user must exist");
        let next_user_id = *store.next_user_id.read().expect("next user ID lock");
        let version = store.current_version(user_id);

        let error = store
            .bootstrap_superuser("regular-bootstrap", "")
            .expect_err("empty bootstrap password must be rejected");

        assert_bad_request(error);
        let after = store
            .get_user("regular-bootstrap")
            .expect("user must remain present");
        assert_user_unchanged(&before, &after);
        assert_eq!(store.current_version(user_id), version);
        assert_eq!(
            *store.next_user_id.read().expect("next user ID lock"),
            next_user_id
        );
    }

    #[test]
    fn bootstrap_superuser_rejects_service_account_without_credential_or_elevation_mutation() {
        let store = CredentialStore::new().expect("in-memory credential store");
        let user_id = store
            .create_service_account(
                "bootstrap-api",
                TenantId::new(3),
                vec![Role::ReadOnly],
                vec![],
            )
            .expect("create service account");
        let before = store
            .get_user("bootstrap-api")
            .expect("created service account must exist");
        let version = store.current_version(user_id);

        let error = store
            .bootstrap_superuser("bootstrap-api", "non-empty-secret")
            .expect_err("service accounts must not be bootstrapped with passwords");

        assert_bad_request(error);
        let after = store
            .get_user("bootstrap-api")
            .expect("service account must remain present");
        assert!(after.is_service_account);
        assert!(after.password_hash.is_empty());
        assert!(after.scram_salt.is_empty());
        assert!(after.scram_salted_password.is_empty());
        assert_user_unchanged(&before, &after);
        assert_eq!(store.current_version(user_id), version);
    }

    #[test]
    fn open_rejects_persisted_regular_user_with_empty_derived_credentials() {
        let dir = tempfile::tempdir().expect("temporary catalog directory");
        let path = dir.path().join("system.redb");
        let salt = generate_scram_salt();
        let password_hash = hash_password_argon2("", &Argon2Config::default())
            .expect("hash empty password for legacy persisted user");
        let stored_hash = password_hash.clone();
        let scram_salted_password = compute_scram_salted_password("", &salt);

        // Direct seeding preserves a structurally valid legacy record for
        // CredentialStore::open to validate.
        let catalog = SystemCatalog::open(&path).expect("open persistent system catalog");
        catalog
            .put_user(&StoredUser {
                user_id: 1,
                username: "legacy-empty-password".to_string(),
                tenant_id: 3,
                password_hash,
                scram_salt: salt,
                scram_salted_password,
                roles: vec![Role::ReadOnly.to_string()],
                is_superuser: false,
                is_active: true,
                is_service_account: false,
                created_at: 1,
                updated_at: 1,
                password_expires_at: 0,
                must_change_password: false,
                password_changed_at: 1,
                default_database_id: 0,
                accessible_databases: vec![],
            })
            .expect("seed persisted regular user");
        drop(catalog);

        let error = match CredentialStore::open(&path) {
            Err(error) => error,
            Ok(_) => panic!("persisted empty-derived regular-user credentials must be rejected"),
        };

        match error {
            crate::Error::BadRequest { detail } => {
                assert_eq!(detail, "stored credential integrity check failed");
                assert!(
                    !detail.contains(&stored_hash),
                    "integrity error must not expose the persisted password hash"
                );
            }
            other => panic!("expected BadRequest, got {other:?}"),
        }
    }

    #[test]
    fn open_rejects_persisted_regular_user_with_empty_password_hash() {
        let dir = tempfile::tempdir().expect("temporary catalog directory");
        let path = dir.path().join("system.redb");
        let catalog = SystemCatalog::open(&path).expect("open persistent system catalog");
        catalog
            .put_user(&StoredUser {
                user_id: 1,
                username: "empty-password-hash".to_string(),
                tenant_id: 3,
                password_hash: String::new(),
                scram_salt: Vec::new(),
                scram_salted_password: Vec::new(),
                roles: vec![Role::ReadOnly.to_string()],
                is_superuser: false,
                is_active: true,
                is_service_account: false,
                created_at: 1,
                updated_at: 1,
                password_expires_at: 0,
                must_change_password: false,
                password_changed_at: 1,
                default_database_id: 0,
                accessible_databases: vec![],
            })
            .expect("seed regular user with empty password hash");
        drop(catalog);

        let error = match CredentialStore::open(&path) {
            Err(error) => error,
            Ok(_) => panic!("regular user with empty password hash must be rejected"),
        };

        assert_bad_request(error);
    }

    #[test]
    fn open_allows_persisted_passwordless_service_account() {
        let dir = tempfile::tempdir().expect("temporary catalog directory");
        let path = dir.path().join("system.redb");
        let catalog = SystemCatalog::open(&path).expect("open persistent system catalog");
        catalog
            .put_user(&StoredUser {
                user_id: 1,
                username: "passwordless-service".to_string(),
                tenant_id: 3,
                password_hash: String::new(),
                scram_salt: Vec::new(),
                scram_salted_password: Vec::new(),
                roles: vec![Role::ReadOnly.to_string()],
                is_superuser: false,
                is_active: true,
                is_service_account: true,
                created_at: 1,
                updated_at: 1,
                password_expires_at: 0,
                must_change_password: false,
                password_changed_at: 1,
                default_database_id: 0,
                accessible_databases: vec![],
            })
            .expect("seed persisted service account");
        drop(catalog);

        let store = CredentialStore::open(&path)
            .expect("passwordless persisted service account must be accepted");
        let account = store
            .get_user("passwordless-service")
            .expect("load persisted service account");

        assert!(account.is_service_account);
        assert!(!account.is_superuser);
        assert_eq!(account.roles, vec![Role::ReadOnly]);
        assert!(account.password_hash.is_empty());
        assert!(account.scram_salt.is_empty());
        assert!(account.scram_salted_password.is_empty());
    }

    #[test]
    fn bootstrap_superuser_updates_existing_regular_user_with_non_empty_password() {
        let store = CredentialStore::new().expect("in-memory credential store");
        store
            .create_user(
                "bootstrap-regular",
                "old-password",
                TenantId::new(3),
                vec![Role::ReadOnly],
            )
            .expect("create regular user");

        store
            .bootstrap_superuser("bootstrap-regular", "new-password")
            .expect("non-empty bootstrap password must remain valid");

        let user = store
            .get_user("bootstrap-regular")
            .expect("bootstrapped user must remain present");
        assert!(store.verify_password("bootstrap-regular", "new-password"));
        assert!(!store.verify_password("bootstrap-regular", "old-password"));
        assert!(user.is_active);
        assert!(user.is_superuser);
        assert!(user.roles.contains(&Role::Superuser));
        assert!(!user.is_service_account);
    }
}
