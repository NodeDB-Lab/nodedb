// SPDX-License-Identifier: BUSL-1.1

//! Read-only accessors: list users, check emptiness, expose the
//! underlying `SystemCatalog`.

use super::super::super::catalog::SystemCatalog;
use super::super::record::{UserRecord, validate_stored_user_credentials};
use super::core::{CredentialStore, read_lock, write_lock};

impl CredentialStore {
    /// List all active users with full details (for SHOW USERS).
    pub fn list_user_details(&self) -> Vec<UserRecord> {
        let users = match read_lock(&self.users) {
            Ok(u) => u,
            Err(_) => return Vec::new(),
        };
        users.values().filter(|u| u.is_active).cloned().collect()
    }

    /// List ALL user records (active and inactive). Used by the
    /// recovery verifier for a complete redb↔memory comparison.
    pub fn list_all_user_details(&self) -> Vec<UserRecord> {
        let users = match read_lock(&self.users) {
            Ok(u) => u,
            Err(_) => return Vec::new(),
        };
        users.values().cloned().collect()
    }

    /// Reload all users from the given catalog into the in-memory cache.
    /// Used by the recovery verifier repair path.
    pub fn reload_from_catalog(&self, catalog: &SystemCatalog) -> crate::Result<()> {
        let stored_users = catalog.load_all_users()?;
        let mut replacement = std::collections::HashMap::with_capacity(stored_users.len());
        for stored in stored_users {
            validate_stored_user_credentials(&stored, &self.argon2_config)?;
            let record = UserRecord::from_stored(stored);
            replacement.insert(record.username.clone(), record);
        }

        let mut users = write_lock(&self.users)?;
        *users = replacement;
        Ok(())
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

    /// Access the underlying system catalog (for API key persistence
    /// and other subsystems that piggyback on the same redb).
    pub fn catalog(&self) -> &SystemCatalog {
        &self.catalog
    }
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
        let valid_salt = generate_scram_salt();
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
        let salt = generate_scram_salt();
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
