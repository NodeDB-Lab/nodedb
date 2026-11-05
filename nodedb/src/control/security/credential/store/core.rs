// SPDX-License-Identifier: BUSL-1.1

//! `CredentialStore` struct + constructors + private helpers.
//!
//! Other concerns (crud, auth, list, replication) live in sibling
//! files under `store/` and extend this struct via their own `impl`
//! blocks. The struct fields are `pub(super)` so those siblings can
//! reach them without leaking internals beyond `credential`.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use tracing::info;

use super::super::super::catalog::SystemCatalog;
use super::super::super::time::now_secs;
use crate::config::auth::Argon2Config;
use crate::control::security::buses::SessionInvalidationReason;
use crate::control::security::sessions::{KillReason, SessionRegistry};

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
    /// Configured durable principal used by protocols that auto-authenticate in trust mode.
    pub(in crate::control::security::credential) trust_superuser_name: RwLock<Option<String>>,
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
    /// The sessions a hard revoke ends (None in test stores with no sessions).
    pub(in crate::control::security::credential) session_registry:
        std::sync::OnceLock<Arc<SessionRegistry>>,
}

/// Acquire a poison-free read guard for security-critical credential state.
pub(in crate::control::security::credential) fn read_lock<T>(
    lock: &RwLock<T>,
) -> RwLockReadGuard<'_, T> {
    lock.read()
}

/// Acquire a poison-free write guard for security-critical credential state.
pub(in crate::control::security::credential) fn write_lock<T>(
    lock: &RwLock<T>,
) -> RwLockWriteGuard<'_, T> {
    lock.write()
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
            trust_superuser_name: RwLock::new(None),
            login_attempts: RwLock::new(HashMap::new()),
            max_failed_logins: 0,
            lockout_duration: std::time::Duration::from_secs(300),
            password_expiry_secs: 0,
            password_expiry_grace_days: 0,
            argon2_config: Argon2Config::default(),
            versions: RwLock::new(HashMap::new()),
            si_bus: std::sync::OnceLock::new(),
            uc_bus: std::sync::OnceLock::new(),
            session_registry: std::sync::OnceLock::new(),
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
            trust_superuser_name: RwLock::new(None),
            login_attempts: RwLock::new(HashMap::new()),
            max_failed_logins: 0,
            lockout_duration: std::time::Duration::from_secs(300),
            password_expiry_secs: 0,
            password_expiry_grace_days: 0,
            argon2_config,
            versions: RwLock::new(HashMap::new()),
            si_bus: std::sync::OnceLock::new(),
            uc_bus: std::sync::OnceLock::new(),
            session_registry: std::sync::OnceLock::new(),
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

    /// Atomically persist a newly allocated user and the following ID counter.
    pub(in crate::control::security::credential) fn persist_new_user_with_next_id(
        &self,
        record: &mut UserRecord,
        next_user_id: u64,
    ) -> crate::Result<()> {
        record.updated_at = now_secs();
        self.catalog
            .put_user_with_next_user_id(&record.to_stored(), next_user_id)
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
        let mut next = write_lock(&self.next_user_id);
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

    /// Wire in the session registry a hard revoke ends sessions through.
    /// Called once from `SharedState` construction.
    pub fn set_session_registry(&self, registry: Arc<SessionRegistry>) {
        let _ = self.session_registry.set(registry);
    }

    /// End every open session of `user_id` when `reason` is a hard revoke.
    ///
    /// This runs on the mutation's own path, with no bus and no audit write in
    /// front of it. The signal reaches every session however far the audit
    /// consumer lags. It takes a read lock and sends on a watch, so it does no
    /// I/O on the metadata apply path.
    pub(in crate::control::security::credential) fn end_sessions_on_hard_revoke(
        &self,
        user_id: u64,
        reason: &SessionInvalidationReason,
    ) {
        if !reason.is_hard_revoke() {
            return;
        }
        if let Some(registry) = self.session_registry.get() {
            registry.kill_sessions_for_user(user_id, KillReason::UserDropped);
        }
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
            let map = read_lock(&self.versions);
            if let Some(ctr) = map.get(&user_id) {
                return Ok(ctr.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1);
            }
        }
        // Slow path: insert under write-lock (double-checked).
        let mut map = write_lock(&self.versions);
        let ctr = map
            .entry(user_id)
            .or_insert_with(|| Arc::new(AtomicU64::new(0)));
        Ok(ctr.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1)
    }

    /// Return the current version for a user.  Returns 0 if the user has
    /// never had a mutation recorded (e.g. loaded from a previous store that
    /// pre-dates versions).
    pub fn current_version(&self, user_id: u64) -> u64 {
        let map = read_lock(&self.versions);
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
    /// 3. End the user's sessions on a hard revoke.
    /// 4. Publish `UserChanged` on the user-change bus.
    /// 5. If `invalidation` is `Some`, publish `SessionInvalidated` on the
    ///    session-invalidation bus.
    ///
    /// Both buses feed the audit log. Each publish is fire-and-forget — a
    /// return value of 0 (no active subscribers) is silently accepted.
    pub(in crate::control::security::credential) fn commit_user_mutation(
        &self,
        record: &mut UserRecord,
        invalidation: Option<SessionInvalidationReason>,
    ) -> crate::Result<()> {
        let user_id = record.user_id;

        // 1. Persist.
        self.persist_user(record)?;

        // 2. Version bump.
        self.bump_version(user_id)?;

        self.publish_user_mutation(user_id, record.is_active, invalidation);
        Ok(())
    }

    /// Steps 3 to 5 of [`Self::commit_user_mutation`], for a mutation whose
    /// record is already visible to readers. An inactive record is a hard
    /// revoke, whatever reason its mutation names.
    pub(in crate::control::security::credential) fn publish_user_mutation(
        &self,
        user_id: u64,
        is_active: bool,
        invalidation: Option<SessionInvalidationReason>,
    ) {
        let invalidation = if is_active {
            invalidation
        } else {
            Some(SessionInvalidationReason::UserDeactivated)
        };
        if let Some(reason) = &invalidation {
            self.end_sessions_on_hard_revoke(user_id, reason);
        }
        if let Some(bus) = self.uc_bus.get() {
            bus.publish(crate::control::security::buses::UserChanged { user_id });
        }
        if let Some(reason) = invalidation
            && let Some(bus) = self.si_bus.get()
        {
            bus.publish(crate::control::security::buses::SessionInvalidated { user_id, reason });
        }
    }

    /// Fully retire a dropped user's persisted + in-process state.
    ///
    /// In order:
    /// 1. End the user's sessions. This comes first: the cache no longer holds
    ///    the user, so a failed catalog delete must not leave a session open.
    /// 2. Delete the record from the redb catalog (idempotent — a
    ///    missing key is a harmless no-op).
    /// 3. Publish `UserChanged` on the user-change bus.
    /// 4. Publish `SessionInvalidated` with `UserDropped` for the audit log.
    /// 5. Discard the per-user version counter — the username may be
    ///    recreated later under a fresh `user_id`.
    ///
    /// The caller must have already removed the in-memory cache entry.
    pub(in crate::control::security::credential) fn purge_user(
        &self,
        record: &UserRecord,
    ) -> crate::Result<()> {
        let user_id = record.user_id;

        // 1. Hard-revoke open sessions.
        self.end_sessions_on_hard_revoke(user_id, &SessionInvalidationReason::UserDropped);

        // 2. Delete from the persistent catalog.
        self.catalog.delete_user(&record.username)?;

        // 3. UserChanged.
        if let Some(bus) = self.uc_bus.get() {
            bus.publish(crate::control::security::buses::UserChanged { user_id });
        }

        // 4. SessionInvalidated.
        if let Some(bus) = self.si_bus.get() {
            bus.publish(crate::control::security::buses::SessionInvalidated {
                user_id,
                reason: SessionInvalidationReason::UserDropped,
            });
        }

        // 5. Discard the per-user version counter.
        write_lock(&self.versions).remove(&user_id);

        Ok(())
    }
}

#[cfg(test)]
pub(super) fn assert_bad_request(error: crate::Error) {
    assert!(matches!(error, crate::Error::BadRequest { .. }));
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
    use std::panic::{AssertUnwindSafe, catch_unwind};

    use super::CredentialStore;
    use crate::config::auth::Argon2Config;
    use crate::control::security::catalog::{StoredUser, SystemCatalog};
    use crate::control::security::credential::hash::{
        compute_scram_salted_password, generate_scram_salt, hash_password_argon2,
    };
    use crate::control::security::identity::Role;
    use crate::types::TenantId;

    fn assert_bad_request(error: crate::Error) {
        assert!(matches!(error, crate::Error::BadRequest { .. }));
    }

    #[test]
    fn users_cache_remains_available_after_panic_while_write_locked() {
        let store = CredentialStore::new().expect("in-memory credential store");
        store
            .create_user(
                "poison-free-user",
                "correct-password",
                TenantId::new(4),
                vec![Role::ReadWrite],
            )
            .expect("create user");

        let result = catch_unwind(AssertUnwindSafe(|| {
            let _users = store.users.write();
            panic!("simulated interrupted credential cache mutation");
        }));
        assert!(result.is_err());

        assert!(store.verify_password("poison-free-user", "correct-password"));
        assert_eq!(store.list_users(), vec!["poison-free-user".to_string()]);
        store
            .add_role("poison-free-user", Role::ReadOnly)
            .expect("mutation after interrupted cache write");
    }

    #[test]
    fn open_rejects_persisted_regular_user_with_empty_derived_credentials() {
        let dir = tempfile::tempdir().expect("temporary catalog directory");
        let path = dir.path().join("system.redb");
        let salt = generate_scram_salt().expect("generate scram salt");
        let password_hash = hash_password_argon2("", &Argon2Config::default())
            .expect("hash empty password for the seeded user");
        let stored_hash = password_hash.clone();
        let scram_salted_password = compute_scram_salted_password("", &salt);

        // Direct seeding writes a structurally valid record for
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
    fn persistent_create_and_reload() {
        let dir = tempfile::tempdir().expect("temporary catalog directory");
        let path = dir.path().join("system.redb");

        {
            let store = CredentialStore::open(&path).expect("open credential store");
            store
                .create_user("alice", "pass123", TenantId::new(1), vec![Role::ReadWrite])
                .expect("create user");
            store
                .bootstrap_superuser("nodedb", "secret")
                .expect("bootstrap superuser");
        }

        let store = CredentialStore::open(&path).expect("reopen credential store");
        let alice = store.get_user("alice").expect("reloaded user");
        assert_eq!(alice.tenant_id, TenantId::new(1));
        assert!(alice.roles.contains(&Role::ReadWrite));
        assert!(store.verify_password("alice", "pass123"));
        assert!(
            store
                .get_user("nodedb")
                .is_some_and(|user| user.is_superuser)
        );
    }

    #[test]
    fn dropped_user_does_not_survive_restart() {
        let dir = tempfile::tempdir().expect("temporary catalog directory");
        let path = dir.path().join("system.redb");

        {
            let store = CredentialStore::open(&path).expect("open credential store");
            store
                .create_user("bob", "pass", TenantId::new(1), vec![Role::ReadOnly])
                .expect("create user");
            assert!(store.drop_user("bob").expect("drop user"));
        }

        let store = CredentialStore::open(&path).expect("reopen credential store");
        assert!(store.get_user("bob").is_none());
        store
            .create_user("bob", "pass2", TenantId::new(1), vec![Role::ReadOnly])
            .expect("dropped username must be reusable after restart");
    }

    #[test]
    fn persistent_role_changes_survive_restart() {
        let dir = tempfile::tempdir().expect("temporary catalog directory");
        let path = dir.path().join("system.redb");

        {
            let store = CredentialStore::open(&path).expect("open credential store");
            store
                .create_user("carol", "pass", TenantId::new(1), vec![Role::ReadOnly])
                .expect("create user");
            store.add_role("carol", Role::ReadWrite).expect("add role");
            store
                .remove_role("carol", &Role::ReadOnly)
                .expect("remove role");
        }

        let store = CredentialStore::open(&path).expect("reopen credential store");
        let carol = store.get_user("carol").expect("reloaded user");
        assert!(carol.roles.contains(&Role::ReadWrite));
        assert!(!carol.roles.contains(&Role::ReadOnly));
    }

    #[test]
    fn persistent_password_change_survives_restart() {
        let dir = tempfile::tempdir().expect("temporary catalog directory");
        let path = dir.path().join("system.redb");

        {
            let store = CredentialStore::open(&path).expect("open credential store");
            store
                .create_user("dave", "old_pass", TenantId::new(1), vec![Role::ReadWrite])
                .expect("create user");
            store
                .update_password("dave", "new_pass")
                .expect("update password");
        }

        let store = CredentialStore::open(&path).expect("reopen credential store");
        assert!(store.verify_password("dave", "new_pass"));
        assert!(!store.verify_password("dave", "old_pass"));
    }

    #[test]
    fn user_id_counter_persists() {
        let dir = tempfile::tempdir().expect("temporary catalog directory");
        let path = dir.path().join("system.redb");

        let first_id = {
            let store = CredentialStore::open(&path).expect("open credential store");
            let first_id = store
                .create_user("u1", "p", TenantId::new(1), vec![])
                .expect("create first user");
            store
                .create_user("u2", "p", TenantId::new(1), vec![])
                .expect("create second user");
            first_id
        };

        let store = CredentialStore::open(&path).expect("reopen credential store");
        let next_id = store
            .create_user("u3", "p", TenantId::new(1), vec![])
            .expect("create third user");
        assert!(next_id > first_id + 1);
    }
}
