// SPDX-License-Identifier: BUSL-1.1

//! Read-only accessors: list users, check emptiness, expose the
//! underlying `SystemCatalog`.

use std::collections::HashMap;

use super::super::super::buses::{SessionInvalidated, SessionInvalidationReason, UserChanged};
use super::super::super::catalog::SystemCatalog;
use super::super::record::{UserRecord, validate_stored_user_credentials};
use super::core::{CredentialStore, read_lock, write_lock};

impl CredentialStore {
    /// List all active users with full details (for SHOW USERS).
    pub fn list_user_details(&self) -> Vec<UserRecord> {
        let users = read_lock(&self.users);
        users.values().filter(|u| u.is_active).cloned().collect()
    }

    /// List ALL user records (active and inactive). Used by the
    /// recovery verifier for a complete redb↔memory comparison.
    pub fn list_all_user_details(&self) -> Vec<UserRecord> {
        let users = read_lock(&self.users);
        users.values().cloned().collect()
    }

    /// Reload all users from the given catalog into the in-memory cache.
    /// Used by the recovery verifier repair path and by a metadata image
    /// install.
    ///
    /// The next user id never moves down. It rises to the catalog's counter
    /// and past every loaded user id, so a user the catalog gained never
    /// shares an id with the next local `CREATE USER`.
    pub fn reload_from_catalog(&self, catalog: &SystemCatalog) -> crate::Result<()> {
        let stored_users = catalog.load_all_users()?;
        let mut floor = catalog.load_next_user_id()?;
        let mut replacement = HashMap::with_capacity(stored_users.len());
        for stored in stored_users {
            validate_stored_user_credentials(&stored, &self.argon2_config)?;
            floor = floor.max(stored.user_id.saturating_add(1));
            let record = UserRecord::from_stored(stored);
            replacement.insert(record.username.clone(), record);
        }

        let changes = {
            let mut users = write_lock(&self.users);
            let changes = reload_changes(&users, &replacement);
            *users = replacement;
            changes
        };
        {
            let mut next = write_lock(&self.next_user_id);
            *next = (*next).max(floor);
        }
        // Applied once the new records are visible, as a replicated apply does.
        // A node that catches up through an image applies no user entry, so a
        // revoke inside the image takes effect here.
        for dropped in changes.dropped {
            self.end_sessions_on_hard_revoke(dropped, &SessionInvalidationReason::UserDropped);
            write_lock(&self.versions).remove(&dropped);
            if let Some(bus) = self.uc_bus.get() {
                bus.publish(UserChanged { user_id: dropped });
            }
            if let Some(bus) = self.si_bus.get() {
                bus.publish(SessionInvalidated {
                    user_id: dropped,
                    reason: SessionInvalidationReason::UserDropped,
                });
            }
        }
        for changed in changes.changed {
            self.bump_version(changed.user_id)?;
            self.publish_user_mutation(
                changed.user_id,
                changed.is_active,
                Some(SessionInvalidationReason::RoleAltered),
            );
        }
        Ok(())
    }

    /// List all active usernames.
    pub fn list_users(&self) -> Vec<String> {
        let users = read_lock(&self.users);
        users
            .values()
            .filter(|u| u.is_active)
            .map(|u| u.username.clone())
            .collect()
    }

    /// Check if any users exist.
    pub fn is_empty(&self) -> bool {
        read_lock(&self.users).is_empty()
    }

    /// Access the underlying system catalog (for API key persistence
    /// and other subsystems that piggyback on the same redb).
    pub fn catalog(&self) -> &SystemCatalog {
        &self.catalog
    }
}

/// What a catalog reload changes for open sessions.
struct ReloadChanges {
    /// Users the reload removes. A same-name user under a new id counts: the
    /// old id's sessions must not inherit the new user.
    dropped: Vec<u64>,
    /// Users whose record the reload changes.
    changed: Vec<ChangedUser>,
}

struct ChangedUser {
    user_id: u64,
    is_active: bool,
}

fn reload_changes(
    before: &HashMap<String, UserRecord>,
    after: &HashMap<String, UserRecord>,
) -> ReloadChanges {
    let mut changes = ReloadChanges {
        dropped: Vec::new(),
        changed: Vec::new(),
    };
    for old in before.values() {
        match after.get(&old.username) {
            Some(new) if new.user_id == old.user_id => {
                if new.is_active != old.is_active
                    || new.updated_at != old.updated_at
                    || new.roles != old.roles
                {
                    changes.changed.push(ChangedUser {
                        user_id: new.user_id,
                        is_active: new.is_active,
                    });
                }
            }
            Some(_) | None => changes.dropped.push(old.user_id),
        }
    }
    changes
}

#[cfg(test)]
mod tests {
    use super::super::super::hash::{
        compute_scram_salted_password, generate_scram_salt, hash_password_argon2,
    };
    use super::super::core::{assert_bad_request, assert_user_unchanged};
    use super::CredentialStore;
    use crate::config::auth::Argon2Config;
    use crate::control::security::catalog::{StoredUser, SystemCatalog};
    use crate::control::security::identity::Role;
    use crate::types::TenantId;

    use crate::control::security::sessions::{KillReason, SessionParams, SessionRegistry};
    use std::sync::Arc;

    fn session_params(user_id: u64) -> SessionParams {
        SessionParams {
            user_id,
            username: "u".into(),
            db_user: "u".into(),
            peer_addr: "127.0.0.1:5000".into(),
            protocol: "native".into(),
            auth_method: "password".into(),
            tenant_id: 3,
            credential_version: 0,
            current_database: None,
            token_expiry_ms: None,
        }
    }

    /// A store whose cache holds `names`, with one registered session each.
    fn store_with_sessions(
        names: &[&str],
    ) -> (
        CredentialStore,
        Vec<(u64, tokio::sync::watch::Receiver<KillReason>)>,
    ) {
        let store = CredentialStore::new().expect("in-memory credential store");
        let registry = Arc::new(SessionRegistry::new());
        store.set_session_registry(Arc::clone(&registry));
        let mut sessions = Vec::new();
        for name in names {
            let user_id = store
                .create_user(
                    name,
                    "valid-password",
                    TenantId::new(3),
                    vec![Role::ReadOnly],
                )
                .expect("create user");
            let kill = registry
                .register(name, &session_params(user_id))
                .expect("register session");
            sessions.push((user_id, kill));
        }
        (store, sessions)
    }

    fn open_catalog(dir: &tempfile::TempDir) -> SystemCatalog {
        SystemCatalog::open(&dir.path().join("system.redb")).expect("open system catalog")
    }

    /// An image install ends the sessions of a user it removes, of a user it
    /// deactivates, and of a same-name user under another id. A user it keeps
    /// stays connected.
    #[test]
    fn a_reload_ends_the_sessions_of_revoked_users() {
        let (store, sessions) =
            store_with_sessions(&["kept", "removed", "deactivated", "recreated"]);
        let dir = tempfile::tempdir().expect("temporary catalog directory");
        let catalog = open_catalog(&dir);

        let kept = store.get_user("kept").expect("kept").to_stored();
        catalog.put_user(&kept).expect("put kept");
        let mut deactivated = store.get_user("deactivated").expect("user").to_stored();
        deactivated.is_active = false;
        catalog.put_user(&deactivated).expect("put deactivated");
        let mut recreated = store.get_user("recreated").expect("user").to_stored();
        recreated.user_id += 1_000;
        catalog.put_user(&recreated).expect("put recreated");

        store.reload_from_catalog(&catalog).expect("reload");

        let reasons: Vec<KillReason> = sessions.iter().map(|(_, kill)| *kill.borrow()).collect();
        assert_eq!(
            reasons,
            vec![
                KillReason::Alive,
                KillReason::UserDropped,
                KillReason::UserDropped,
                KillReason::UserDropped,
            ]
        );
    }

    /// A reload that changes a user's roles moves the version, so an open
    /// session rebuilds its identity at its next request.
    #[test]
    fn a_reload_moves_the_version_of_a_changed_user() {
        let (store, sessions) = store_with_sessions(&["altered"]);
        let (user_id, kill) = &sessions[0];
        let before = store.current_version(*user_id);
        let dir = tempfile::tempdir().expect("temporary catalog directory");
        let catalog = open_catalog(&dir);
        let mut altered = store.get_user("altered").expect("user").to_stored();
        altered.roles = vec![Role::ReadWrite.to_string()];
        catalog.put_user(&altered).expect("put altered");

        store.reload_from_catalog(&catalog).expect("reload");

        assert!(store.current_version(*user_id) > before);
        assert_eq!(*kill.borrow(), KillReason::Alive);
    }

    #[test]
    fn reload_from_catalog_rejects_invalid_user_without_replacing_cached_users() {
        let store = CredentialStore::new().expect("in-memory credential store");
        store
            .create_user(
                "cached-regular-user",
                "valid-password",
                TenantId::new(3),
                vec![Role::ReadOnly],
            )
            .expect("create cached regular user");
        let before = store
            .get_user("cached-regular-user")
            .expect("cached user must exist");

        let dir = tempfile::tempdir().expect("temporary catalog directory");
        let catalog = SystemCatalog::open(&dir.path().join("system.redb"))
            .expect("open persistent system catalog");
        let valid_salt = generate_scram_salt().expect("generate valid scram salt");
        catalog
            .put_user(&StoredUser {
                user_id: 1,
                // `load_all_users` walks the catalog in key order, so this
                // valid prefix exercises failure after replacement building
                // has already begun.
                username: "a-valid-catalog-user".to_string(),
                tenant_id: 3,
                password_hash: hash_password_argon2(
                    "valid-catalog-password",
                    &Argon2Config::default(),
                )
                .expect("hash valid persisted user password"),
                scram_salt: valid_salt.clone(),
                scram_salted_password: compute_scram_salted_password(
                    "valid-catalog-password",
                    &valid_salt,
                ),
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
            .expect("seed valid persisted regular user");
        let salt = generate_scram_salt().expect("generate scram salt");
        let password_hash = hash_password_argon2("", &Argon2Config::default())
            .expect("hash empty password for invalid persisted user");
        catalog
            .put_user(&StoredUser {
                user_id: 2,
                username: "z-invalid-empty-password".to_string(),
                tenant_id: 3,
                password_hash,
                scram_salt: salt.clone(),
                scram_salted_password: compute_scram_salted_password("", &salt),
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
            .expect("seed invalid persisted regular user");

        let error = store
            .reload_from_catalog(&catalog)
            .expect_err("invalid persisted credentials must reject reload");
        assert_bad_request(error);

        let after = store
            .get_user("cached-regular-user")
            .expect("cached user must remain present");
        assert_user_unchanged(&before, &after);
        assert!(
            store.get_user("a-valid-catalog-user").is_none(),
            "failed reload must not publish a valid catalog prefix"
        );
        assert!(store.get_user("z-invalid-empty-password").is_none());
    }
}
