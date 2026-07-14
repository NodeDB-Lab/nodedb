// SPDX-License-Identifier: BUSL-1.1

//! User CRUD operations: create, deactivate, update password/roles.

use crate::types::TenantId;

use super::super::super::buses::SessionInvalidationReason;
use super::super::super::identity::Role;
use super::super::super::time::now_secs;
use super::super::hash::{
    compute_scram_salted_password, generate_scram_salt, hash_password_argon2,
};
use super::super::record::UserRecord;
use super::core::{
    CredentialStore, PasswordPrincipal, read_lock, validate_password_assignment, write_lock,
};

impl CredentialStore {
    /// Create a new user. Returns the user_id.
    pub fn create_user(
        &self,
        username: &str,
        password: &str,
        tenant_id: TenantId,
        roles: Vec<Role>,
    ) -> crate::Result<u64> {
        // Preserve duplicate precedence without holding the users write lock
        // during the intentionally expensive Argon2 computation.
        {
            let users = read_lock(&self.users)?;
            if users.contains_key(username) {
                return Err(crate::Error::BadRequest {
                    detail: format!("user '{username}' already exists"),
                });
            }
        }
        validate_password_assignment(password, PasswordPrincipal::New)?;

        let salt = generate_scram_salt();
        let scram_salted_password = compute_scram_salted_password(password, &salt);
        let password_hash = hash_password_argon2(password, &self.argon2_config)?;

        let mut users = write_lock(&self.users)?;
        // Another writer can create the user while Argon2 runs.
        if users.contains_key(username) {
            return Err(crate::Error::BadRequest {
                detail: format!("user '{username}' already exists"),
            });
        }
        let user_id = self.alloc_user_id()?;

        let is_superuser = roles.contains(&Role::Superuser);
        let now = now_secs();
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
            created_at: now,
            updated_at: now,
            password_expires_at: self.compute_expiry(),
            must_change_password: false,
            password_changed_at: now,
            default_database_id: 0,
            accessible_databases: vec![],
        };

        // create_user: no open sessions to invalidate — no invalidation reason.
        self.commit_user_mutation(&mut record, None)?;
        users.insert(username.to_string(), record);
        Ok(user_id)
    }

    /// Create a service account. No password — can only authenticate
    /// via API keys. Returns the user_id.
    ///
    /// `accessible_databases`: if non-empty, the service account is restricted
    /// to those databases. Empty = legacy = treated as `[DatabaseId::DEFAULT]`
    /// at auth time.
    pub fn create_service_account(
        &self,
        name: &str,
        tenant_id: TenantId,
        roles: Vec<Role>,
        accessible_databases: Vec<nodedb_types::id::DatabaseId>,
    ) -> crate::Result<u64> {
        let mut users = write_lock(&self.users)?;
        if users.contains_key(name) {
            return Err(crate::Error::BadRequest {
                detail: format!("user or service account '{name}' already exists"),
            });
        }

        let user_id = self.alloc_user_id()?;
        let is_superuser = roles.contains(&Role::Superuser);
        let now = now_secs();
        let mut record = UserRecord {
            user_id,
            username: name.to_string(),
            tenant_id,
            password_hash: String::new(),
            scram_salt: Vec::new(),
            scram_salted_password: Vec::new(),
            roles,
            is_superuser,
            is_active: true,
            is_service_account: true,
            created_at: now,
            updated_at: now,
            password_expires_at: 0,
            must_change_password: false,
            password_changed_at: now,
            default_database_id: 0,
            accessible_databases,
        };

        // Service-account creation: no open sessions — no invalidation reason.
        self.commit_user_mutation(&mut record, None)?;
        users.insert(name.to_string(), record);
        Ok(user_id)
    }

    /// Drop a user. Fully removes the identity record from the
    /// in-memory cache AND the redb catalog, then publishes
    /// `UserDropped` to hard-revoke open sessions. Returns `false`
    /// if no such user exists.
    ///
    /// A full delete (not a soft-delete tombstone) is required so the
    /// username is freed for reuse — a stale `is_active = false`
    /// record would still trip the `CREATE USER` uniqueness check.
    pub fn drop_user(&self, username: &str) -> crate::Result<bool> {
        let record = {
            let mut users = write_lock(&self.users)?;
            match users.remove(username) {
                Some(record) => record,
                None => return Ok(false),
            }
        };
        self.purge_user(&record)?;
        Ok(true)
    }

    /// Update a user's password. Recomputes both Argon2 hash and SCRAM
    /// credentials.  Password change is a credential mutation but does not
    /// change role/access — no session invalidation reason.
    pub fn update_password(&self, username: &str, password: &str) -> crate::Result<()> {
        let (observed_user_id, is_service_account) = {
            let users = read_lock(&self.users)?;
            let record = users
                .get(username)
                .ok_or_else(|| crate::Error::BadRequest {
                    detail: format!("user '{username}' not found"),
                })?;
            if !record.is_active {
                return Err(crate::Error::BadRequest {
                    detail: format!("user '{username}' is inactive"),
                });
            }
            (record.user_id, record.is_service_account)
        };
        validate_password_assignment(password, PasswordPrincipal::Existing { is_service_account })?;

        let salt = generate_scram_salt();
        let scram_salted_password = compute_scram_salted_password(password, &salt);
        let password_hash = hash_password_argon2(password, &self.argon2_config)?;

        let mut users = write_lock(&self.users)?;
        let record = users
            .get_mut(username)
            .ok_or_else(|| crate::Error::BadRequest {
                detail: format!("user '{username}' not found"),
            })?;
        if record.user_id != observed_user_id {
            return Err(crate::Error::BadRequest {
                detail: format!("user '{username}' changed while password was being prepared"),
            });
        }
        if !record.is_active {
            return Err(crate::Error::BadRequest {
                detail: format!("user '{username}' is inactive"),
            });
        }
        // The account can change while Argon2 runs; validate the live record
        // before assigning any password material.
        validate_password_assignment(
            password,
            PasswordPrincipal::Existing {
                is_service_account: record.is_service_account,
            },
        )?;
        record.scram_salted_password = scram_salted_password;
        record.scram_salt = salt;
        record.password_hash = password_hash;
        record.password_expires_at = self.compute_expiry();
        record.must_change_password = false;
        record.password_changed_at = now_secs();
        // Password change only — no role/access change, no session invalidation.
        self.commit_user_mutation(record, None)?;
        Ok(())
    }

    /// Mark a user as requiring a password change on next login.
    pub fn set_must_change_password(&self, username: &str, required: bool) -> crate::Result<()> {
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
        record.must_change_password = required;
        self.commit_user_mutation(record, None)?;
        Ok(())
    }

    /// Set password expiry to 0 (never expires) for a user.
    pub fn set_password_never_expires(&self, username: &str) -> crate::Result<()> {
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
        record.password_expires_at = 0;
        self.commit_user_mutation(record, None)?;
        Ok(())
    }

    /// Set a specific password expiry timestamp for a user.
    pub fn set_password_expires_at(&self, username: &str, expires_at: u64) -> crate::Result<()> {
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
        record.password_expires_at = expires_at;
        self.commit_user_mutation(record, None)?;
        Ok(())
    }

    /// Replace all roles for a user. Triggers identity rehydrate on open
    /// sessions via `RoleAltered`.
    pub fn update_roles(&self, username: &str, roles: Vec<Role>) -> crate::Result<()> {
        let mut users = write_lock(&self.users)?;
        let record = users
            .get_mut(username)
            .ok_or_else(|| crate::Error::BadRequest {
                detail: format!("user '{username}' not found"),
            })?;
        record.is_superuser = roles.contains(&Role::Superuser);
        record.roles = roles;
        self.commit_user_mutation(record, Some(SessionInvalidationReason::RoleAltered))?;
        Ok(())
    }

    /// Add a role to a user (if not already present). Triggers `RoleGranted`
    /// soft-revoke on open sessions.
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
        self.commit_user_mutation(record, Some(SessionInvalidationReason::RoleGranted))?;
        Ok(())
    }

    /// Replace the `accessible_databases` list on a service account.
    ///
    /// Requires the caller to have already verified superuser authority.
    /// For non-service-account users, returns an error.
    pub fn set_service_account_databases(
        &self,
        name: &str,
        databases: Vec<nodedb_types::id::DatabaseId>,
    ) -> crate::Result<()> {
        let mut users = write_lock(&self.users)?;
        let record = users
            .get_mut(name)
            .ok_or_else(|| crate::Error::BadRequest {
                detail: format!("service account '{name}' not found"),
            })?;
        if !record.is_service_account {
            return Err(crate::Error::BadRequest {
                detail: format!("'{name}' is a user, not a service account"),
            });
        }
        record.accessible_databases = databases;
        self.commit_user_mutation(record, Some(SessionInvalidationReason::RoleAltered))?;
        Ok(())
    }

    /// Remove a role from a user. Triggers `RoleRevoked` soft-revoke on
    /// open sessions.
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
        self.commit_user_mutation(record, Some(SessionInvalidationReason::RoleRevoked))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{
        super::core::{assert_bad_request, assert_user_unchanged},
        CredentialStore,
    };
    use crate::control::security::identity::Role;
    use crate::types::TenantId;

    #[test]
    fn create_user_rejects_empty_password_without_mutation() {
        let store = CredentialStore::new().expect("in-memory credential store");
        let next_user_id = *store.next_user_id.read().expect("next user ID lock");

        let error = store
            .create_user(
                "empty-password",
                "",
                TenantId::new(7),
                vec![Role::ReadWrite],
            )
            .expect_err("empty passwords must be rejected");

        assert_bad_request(error);
        assert!(store.get_user("empty-password").is_none());
        assert_eq!(
            *store.next_user_id.read().expect("next user ID lock"),
            next_user_id,
            "rejected create must not allocate a user ID"
        );
        assert!(
            store
                .catalog()
                .get_user("empty-password")
                .expect("read catalog")
                .is_none(),
            "rejected create must not write the catalog"
        );
    }

    #[test]
    fn update_password_rejects_empty_password_without_mutating_credential_or_policy() {
        let store = CredentialStore::new().expect("in-memory credential store");
        let user_id = store
            .create_user(
                "password-user",
                "old-password",
                TenantId::new(7),
                vec![Role::ReadWrite],
            )
            .expect("create user");
        store
            .set_must_change_password("password-user", true)
            .expect("set password policy");
        let before = store
            .get_user("password-user")
            .expect("created user must exist");
        let version = store.current_version(user_id);

        let error = store
            .update_password("password-user", "")
            .expect_err("empty passwords must be rejected");

        assert_bad_request(error);
        let after = store
            .get_user("password-user")
            .expect("user must remain present");
        assert_user_unchanged(&before, &after);
        assert_eq!(store.current_version(user_id), version);
    }

    #[test]
    fn update_password_rejects_service_account_without_password_material_or_state_change() {
        let store = CredentialStore::new().expect("in-memory credential store");
        let user_id = store
            .create_service_account("api-only", TenantId::new(7), vec![Role::ReadWrite], vec![])
            .expect("create service account");
        let before = store
            .get_user("api-only")
            .expect("created service account must exist");
        let version = store.current_version(user_id);

        let error = store
            .update_password("api-only", "not-allowed")
            .expect_err("service accounts must not accept passwords");

        assert_bad_request(error);
        let after = store
            .get_user("api-only")
            .expect("service account must remain present");
        assert!(after.is_service_account);
        assert!(after.password_hash.is_empty());
        assert!(after.scram_salt.is_empty());
        assert!(after.scram_salted_password.is_empty());
        assert_user_unchanged(&before, &after);
        assert_eq!(store.current_version(user_id), version);
    }
}
