// SPDX-License-Identifier: BUSL-1.1

//! Build the `StoredUser` a user-mutating statement proposes.
//!
//! Each builder takes the record it starts from rather than looking it up.
//! A statement inside a transaction starts from the user as the transaction
//! sees it: a user it created earlier is visible, and a user it dropped is
//! not. Outside a transaction that is the committed record, which
//! [`CredentialStore::stored_user`] returns. The builders touch neither the
//! in-memory map nor redb: the applier installs the record after commit.

use crate::types::TenantId;

use super::super::super::catalog::StoredUser;
use super::super::super::identity::Role;
use super::super::super::time::now_secs;
use super::super::hash::{
    compute_scram_salted_password, generate_scram_salt, hash_password_argon2,
};
use super::core::{CredentialStore, PasswordPrincipal, read_lock, validate_password_assignment};

impl CredentialStore {
    /// The committed record of active user `username`.
    pub fn stored_user(&self, username: &str) -> Option<StoredUser> {
        let users = read_lock(&self.users);
        users
            .get(username)
            .filter(|user| user.is_active)
            .map(|user| user.to_stored())
    }

    /// Build a new password user. Allocates a user_id and hashes the password
    /// (Argon2 + SCRAM salt). The caller has checked the name is free.
    pub fn prepare_new_user(
        &self,
        username: &str,
        password: &str,
        tenant_id: TenantId,
        roles: Vec<Role>,
    ) -> crate::Result<StoredUser> {
        validate_password_assignment(password, PasswordPrincipal::New)?;

        let salt = generate_scram_salt();
        let scram_salted_password = compute_scram_salted_password(password, &salt);
        let password_hash = hash_password_argon2(password, &self.argon2_config)?;
        let user_id = self.alloc_user_id()?;
        let is_superuser = roles.contains(&Role::Superuser);
        let now = now_secs();

        Ok(StoredUser {
            user_id,
            username: username.to_string(),
            tenant_id: tenant_id.as_u64(),
            password_hash,
            scram_salt: salt,
            scram_salted_password,
            roles: roles.iter().map(|r| r.to_string()).collect(),
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
        })
    }

    /// Build a new service account. It authenticates by API key only, so it
    /// carries no password hash. The caller has checked the name is free.
    pub fn prepare_new_service_account(
        &self,
        name: &str,
        tenant_id: TenantId,
        roles: Vec<Role>,
        accessible_databases: Vec<nodedb_types::id::DatabaseId>,
    ) -> crate::Result<StoredUser> {
        let user_id = self.alloc_user_id()?;
        let now = now_secs();
        Ok(StoredUser {
            user_id,
            username: name.to_string(),
            tenant_id: tenant_id.as_u64(),
            password_hash: String::new(),
            scram_salt: Vec::new(),
            scram_salted_password: Vec::new(),
            is_superuser: roles.contains(&Role::Superuser),
            roles: roles.iter().map(|r| r.to_string()).collect(),
            is_active: true,
            is_service_account: true,
            created_at: now,
            updated_at: now,
            password_expires_at: 0,
            must_change_password: false,
            password_changed_at: now,
            default_database_id: 0,
            accessible_databases: accessible_databases
                .iter()
                .map(|database_id| database_id.as_u64())
                .collect(),
        })
    }

    /// `base` with a new password and/or role set. Used by `ALTER USER SET
    /// PASSWORD`, `ALTER USER SET ROLE`, and `GRANT` / `REVOKE` of roles.
    pub fn prepare_user_update_from(
        &self,
        mut base: StoredUser,
        new_password: Option<&str>,
        new_roles: Option<Vec<Role>>,
    ) -> crate::Result<StoredUser> {
        if let Some(password) = new_password {
            validate_password_assignment(
                password,
                PasswordPrincipal::Existing {
                    is_service_account: base.is_service_account,
                },
            )?;
            let salt = generate_scram_salt();
            base.scram_salted_password = compute_scram_salted_password(password, &salt);
            base.scram_salt = salt;
            base.password_hash = hash_password_argon2(password, &self.argon2_config)?;
            base.password_expires_at = self.compute_expiry();
            base.must_change_password = false;
            base.password_changed_at = now_secs();
        }
        if let Some(roles) = new_roles {
            base.is_superuser = roles.contains(&Role::Superuser);
            base.roles = roles.iter().map(|r| r.to_string()).collect();
        }
        base.updated_at = now_secs();
        Ok(base)
    }

    /// `base` with `must_change_password` set to `required`.
    pub fn prepare_set_must_change_password_from(
        &self,
        mut base: StoredUser,
        required: bool,
    ) -> StoredUser {
        base.must_change_password = required;
        base.updated_at = now_secs();
        base
    }

    /// `base` with `password_expires_at`. `0` means "NEVER EXPIRES".
    pub fn prepare_set_password_expires_at_from(
        &self,
        mut base: StoredUser,
        expires_at: u64,
    ) -> StoredUser {
        base.password_expires_at = expires_at;
        base.updated_at = now_secs();
        base
    }

    /// `base` with `default_database_id`.
    pub fn prepare_set_default_database_from(
        &self,
        mut base: StoredUser,
        database_id: u64,
    ) -> StoredUser {
        base.default_database_id = database_id;
        base.updated_at = now_secs();
        base
    }

    /// Service account `base` restricted to `databases`. Refuses a password
    /// user.
    pub fn prepare_service_account_databases_from(
        &self,
        mut base: StoredUser,
        databases: Vec<nodedb_types::id::DatabaseId>,
    ) -> crate::Result<StoredUser> {
        if !base.is_service_account {
            return Err(crate::Error::BadRequest {
                detail: format!("'{}' is a user, not a service account", base.username),
            });
        }
        base.accessible_databases = databases
            .iter()
            .map(|database_id| database_id.as_u64())
            .collect();
        base.updated_at = now_secs();
        Ok(base)
    }

    /// The committed active record of `username`, or the error a
    /// username-addressed builder reports.
    pub(super) fn existing_active(&self, username: &str) -> crate::Result<StoredUser> {
        let users = read_lock(&self.users);
        let existing = users
            .get(username)
            .ok_or_else(|| crate::Error::BadRequest {
                detail: format!("user '{username}' not found"),
            })?;
        if !existing.is_active {
            return Err(crate::Error::BadRequest {
                detail: format!("user '{username}' is inactive"),
            });
        }
        Ok(existing.to_stored())
    }
}
