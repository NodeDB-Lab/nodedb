// SPDX-License-Identifier: BUSL-1.1

//! Cluster replication hooks for [`CredentialStore`].
//!
//! Powers the `CatalogEntry::PutUser` / `DropUser` pipeline.
//! Every method here is called from a specific point in the
//! replicated-DDL flow:
//!
//! - [`CredentialStore::prepare_user`] builds a complete
//!   [`StoredUser`] with fresh Argon2 hash + SCRAM salt +
//!   `user_id`, without touching the in-memory map or redb. The
//!   leader calls this before proposing the entry through raft.
//! - [`CredentialStore::prepare_user_update`] overlays changes on
//!   an existing user for `ALTER USER SET PASSWORD / SET ROLE`.
//! - [`CredentialStore::install_replicated_user`] upserts a
//!   `StoredUser` (computed on another node) into the in-memory
//!   cache, bumping `next_user_id` to stay ahead of replicated ids.
//! - [`CredentialStore::install_replicated_drop`] removes the
//!   in-memory record for `CatalogEntry::DropUser`.
//!
//! Password hashing + scram salt generation must happen on the
//! leader because followers cannot reproduce a random salt;
//! followers accept the leader's fully-computed `StoredUser`
//! verbatim.

use crate::types::TenantId;

use super::super::super::catalog::StoredUser;
use super::super::super::identity::Role;
use super::super::super::time::now_secs;
use super::super::hash::{
    compute_scram_salted_password, generate_scram_salt, hash_password_argon2,
};
use super::super::record::UserRecord;
use super::core::{CredentialStore, PasswordPrincipal, read_lock, validate_password_assignment};

impl CredentialStore {
    /// Build a `StoredUser` ready for replication via
    /// `CatalogEntry::PutUser`. Allocates a user_id, hashes the
    /// password (Argon2 + SCRAM salt), but does NOT insert
    /// into the in-memory map or write to redb — the applier does
    /// that on every node after the raft commit.
    pub fn prepare_user(
        &self,
        username: &str,
        password: &str,
        tenant_id: TenantId,
        roles: Vec<Role>,
    ) -> crate::Result<StoredUser> {
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

    /// Build an updated `StoredUser` from an existing user with
    /// specific fields replaced. Used by `ALTER USER SET PASSWORD`
    /// and `ALTER USER SET ROLE`. Returns the updated record
    /// ready for propose.
    pub fn prepare_user_update(
        &self,
        username: &str,
        new_password: Option<&str>,
        new_roles: Option<Vec<Role>>,
    ) -> crate::Result<StoredUser> {
        let users = read_lock(&self.users)?;
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
        if let Some(password) = new_password {
            validate_password_assignment(
                password,
                PasswordPrincipal::Existing {
                    is_service_account: existing.is_service_account,
                },
            )?;
        }
        let mut stored = existing.to_stored();
        drop(users);

        if let Some(pw) = new_password {
            let salt = generate_scram_salt();
            stored.scram_salted_password = compute_scram_salted_password(pw, &salt);
            stored.scram_salt = salt;
            stored.password_hash = hash_password_argon2(pw, &self.argon2_config)?;
            stored.password_expires_at = self.compute_expiry();
            stored.must_change_password = false;
            stored.password_changed_at = now_secs();
        }
        if let Some(roles) = new_roles {
            stored.is_superuser = roles.contains(&Role::Superuser);
            stored.roles = roles.iter().map(|r| r.to_string()).collect();
        }
        stored.updated_at = now_secs();
        Ok(stored)
    }

    /// Build an updated `StoredUser` that sets `must_change_password`.
    /// Used by `ALTER USER <name> MUST CHANGE PASSWORD`.
    pub fn prepare_set_must_change_password(
        &self,
        username: &str,
        required: bool,
    ) -> crate::Result<StoredUser> {
        let users = read_lock(&self.users)?;
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
        let mut stored = existing.to_stored();
        drop(users);
        stored.must_change_password = required;
        stored.updated_at = now_secs();
        Ok(stored)
    }

    /// Build an updated `StoredUser` that sets `password_expires_at`.
    /// Pass `expires_at = 0` for "NEVER EXPIRES".
    pub fn prepare_set_password_expires_at(
        &self,
        username: &str,
        expires_at: u64,
    ) -> crate::Result<StoredUser> {
        let users = read_lock(&self.users)?;
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
        let mut stored = existing.to_stored();
        drop(users);
        stored.password_expires_at = expires_at;
        stored.updated_at = now_secs();
        Ok(stored)
    }

    /// Build an updated `StoredUser` that sets `default_database_id`.
    /// Used by `ALTER USER <name> SET DEFAULT DATABASE <db>`.
    pub fn prepare_set_default_database(
        &self,
        username: &str,
        database_id: u64,
    ) -> crate::Result<StoredUser> {
        let users = read_lock(&self.users)?;
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
        let mut stored = existing.to_stored();
        drop(users);
        stored.default_database_id = database_id;
        stored.updated_at = now_secs();
        Ok(stored)
    }

    /// Install a replicated `StoredUser` into the in-memory cache and
    /// trigger bus publishes.
    ///
    /// `invalidation` carries the reason that the Raft proposer attached to
    /// the log entry (e.g. `RoleGranted`, `UserDropped`).  Pass `None` for
    /// plain `CREATE USER` entries where no open sessions exist.
    ///
    /// Never errors — a poisoned lock falls through to in-place recovery so a
    /// single bad user write doesn't stall raft.
    pub fn install_replicated_user(
        &self,
        stored: &StoredUser,
        invalidation: Option<super::super::super::buses::SessionInvalidationReason>,
    ) {
        let mut record = UserRecord::from_stored(stored.clone());

        // Bump next_user_id to stay ahead of replicated ids.
        {
            let mut next = self.next_user_id.write().unwrap_or_else(|p| p.into_inner());
            if stored.user_id + 1 > *next {
                *next = stored.user_id + 1;
            }
        }

        // Persist + version bump + bus publishes (errors are swallowed so
        // that a single bad write doesn't stall Raft).
        let _ = self.commit_user_mutation(&mut record, invalidation);

        let mut users = self.users.write().unwrap_or_else(|p| p.into_inner());
        users.insert(stored.username.clone(), record);
    }

    /// Remove a replicated dropped user from the in-memory cache and
    /// publish `UserDropped` so open sessions are hard-revoked.
    ///
    /// The redb delete is also performed by the catalog applier;
    /// `purge_user`'s catalog delete is idempotent, so a double
    /// delete is harmless.
    pub fn install_replicated_drop(&self, username: &str) {
        let record = {
            let mut users = self.users.write().unwrap_or_else(|p| p.into_inner());
            users.remove(username)
        };
        if let Some(record) = record {
            let _ = self.purge_user(&record);
        }
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
    fn prepare_user_rejects_empty_password_without_id_allocation_or_proposal() {
        let store = CredentialStore::new().expect("in-memory credential store");
        let next_user_id = *store.next_user_id.read().expect("next user ID lock");

        let error = store
            .prepare_user(
                "replicated-empty",
                "",
                TenantId::new(9),
                vec![Role::ReadWrite],
            )
            .expect_err("empty password must not produce a replicated user proposal");

        assert_bad_request(error);
        assert!(store.get_user("replicated-empty").is_none());
        assert_eq!(
            *store.next_user_id.read().expect("next user ID lock"),
            next_user_id,
            "rejected proposal preparation must not allocate a user ID"
        );
        assert!(
            store
                .catalog()
                .get_user("replicated-empty")
                .expect("read catalog")
                .is_none(),
            "proposal preparation must not write the catalog"
        );
    }

    #[test]
    fn prepare_user_update_rejects_empty_password_without_source_state_or_version_mutation() {
        let store = CredentialStore::new().expect("in-memory credential store");
        let user_id = store
            .create_user(
                "replicated-update",
                "old-password",
                TenantId::new(9),
                vec![Role::ReadWrite],
            )
            .expect("create user");
        store
            .set_must_change_password("replicated-update", true)
            .expect("set password policy");
        let before = store
            .get_user("replicated-update")
            .expect("created user must exist");
        let version = store.current_version(user_id);

        let error = store
            .prepare_user_update("replicated-update", Some(""), None)
            .expect_err("empty password must not produce an update proposal");

        assert_bad_request(error);
        let after = store
            .get_user("replicated-update")
            .expect("source user must remain present");
        assert_user_unchanged(&before, &after);
        assert_eq!(store.current_version(user_id), version);
    }

    #[test]
    fn prepare_user_update_rejects_service_account_password_without_proposal_or_source_mutation() {
        let store = CredentialStore::new().expect("in-memory credential store");
        let user_id = store
            .create_service_account(
                "replicated-api",
                TenantId::new(9),
                vec![Role::ReadWrite],
                vec![],
            )
            .expect("create service account");
        let before = store
            .get_user("replicated-api")
            .expect("created service account must exist");
        let version = store.current_version(user_id);

        let error = store
            .prepare_user_update("replicated-api", Some("non-empty-password"), None)
            .expect_err("service account password update must not produce a proposal");

        assert_bad_request(error);
        let after = store
            .get_user("replicated-api")
            .expect("source service account must remain present");
        assert!(after.is_service_account);
        assert!(after.password_hash.is_empty());
        assert!(after.scram_salt.is_empty());
        assert!(after.scram_salted_password.is_empty());
        assert_user_unchanged(&before, &after);
        assert_eq!(store.current_version(user_id), version);
    }
}
