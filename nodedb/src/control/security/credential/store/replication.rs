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
use super::super::record::UserRecord;
use super::core::{CredentialStore, read_lock, write_lock};

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
        if read_lock(&self.users).contains_key(username) {
            return Err(crate::Error::BadRequest {
                detail: format!("user '{username}' already exists"),
            });
        }
        self.prepare_new_user(username, password, tenant_id, roles)
    }

    /// Build a service-account `StoredUser` ready for replication via
    /// `CatalogEntry::PutUser`.
    pub fn prepare_service_account(
        &self,
        name: &str,
        tenant_id: TenantId,
        roles: Vec<Role>,
        accessible_databases: Vec<nodedb_types::id::DatabaseId>,
    ) -> crate::Result<StoredUser> {
        if read_lock(&self.users).contains_key(name) {
            return Err(crate::Error::BadRequest {
                detail: format!("user or service account '{name}' already exists"),
            });
        }
        self.prepare_new_service_account(name, tenant_id, roles, accessible_databases)
    }

    /// Build the updated `StoredUser` of committed service account `name`
    /// restricted to `databases`.
    pub fn prepare_service_account_databases(
        &self,
        name: &str,
        databases: Vec<nodedb_types::id::DatabaseId>,
    ) -> crate::Result<StoredUser> {
        let base = self.existing_active(name)?;
        self.prepare_service_account_databases_from(base, databases)
    }

    /// Build an updated `StoredUser` from committed user `username` with a
    /// new password and/or role set.
    pub fn prepare_user_update(
        &self,
        username: &str,
        new_password: Option<&str>,
        new_roles: Option<Vec<Role>>,
    ) -> crate::Result<StoredUser> {
        let base = self.existing_active(username)?;
        self.prepare_user_update_from(base, new_password, new_roles)
    }

    /// Build an updated `StoredUser` that sets `must_change_password`.
    pub fn prepare_set_must_change_password(
        &self,
        username: &str,
        required: bool,
    ) -> crate::Result<StoredUser> {
        let base = self.existing_active(username)?;
        Ok(self.prepare_set_must_change_password_from(base, required))
    }

    /// Build an updated `StoredUser` that sets `password_expires_at`.
    /// Pass `expires_at = 0` for "NEVER EXPIRES".
    pub fn prepare_set_password_expires_at(
        &self,
        username: &str,
        expires_at: u64,
    ) -> crate::Result<StoredUser> {
        let base = self.existing_active(username)?;
        Ok(self.prepare_set_password_expires_at_from(base, expires_at))
    }

    /// Build an updated `StoredUser` that sets `default_database_id`.
    pub fn prepare_set_default_database(
        &self,
        username: &str,
        database_id: u64,
    ) -> crate::Result<StoredUser> {
        let base = self.existing_active(username)?;
        Ok(self.prepare_set_default_database_from(base, database_id))
    }

    /// Install a replicated `StoredUser` into the in-memory cache and
    /// trigger bus publishes.
    ///
    /// `invalidation` carries the reason that the Raft proposer attached to
    /// the log entry (e.g. `RoleGranted`, `UserDropped`).  Pass `None` for
    /// plain `CREATE USER` entries where no open sessions exist.
    ///
    /// Uses poison-free cache locks, so a panic in a prior caller cannot stall
    /// Raft cache publication.
    pub fn install_replicated_user(
        &self,
        stored: &StoredUser,
        invalidation: Option<super::super::super::buses::SessionInvalidationReason>,
    ) {
        let record = UserRecord::from_stored(stored.clone());

        // Bump next_user_id to stay ahead of replicated ids.
        {
            let mut next = write_lock(&self.next_user_id);
            if stored.user_id + 1 > *next {
                *next = stored.user_id + 1;
            }
        }

        // Redb was committed by the catalog applier (or the single-node
        // caller) before this cache installation. Do not write it again: a
        // second write would mutate `updated_at` and could overwrite a
        // conditional catalog-apply failure. Publish invalidation only after
        // the new cache value is visible to observers.
        {
            let mut users = write_lock(&self.users);
            users.insert(stored.username.clone(), record);
        }
        let user_id = stored.user_id;
        if let Err(error) = self.bump_version(user_id) {
            tracing::error!(user_id, error = %error, "replicated user version update failed");
        }
        if let Some(bus) = self.uc_bus.get() {
            bus.publish(super::super::super::buses::UserChanged { user_id });
        }
        if let Some(reason) = invalidation
            && let Some(bus) = self.si_bus.get()
        {
            bus.publish(super::super::super::buses::SessionInvalidated { user_id, reason });
        }
    }

    /// Remove a replicated dropped user from the in-memory cache and
    /// publish `UserDropped` so open sessions are hard-revoked.
    ///
    /// The redb delete is also performed by the catalog applier;
    /// `purge_user`'s catalog delete is idempotent, so a double
    /// delete is harmless.
    pub fn install_replicated_drop(&self, username: &str) {
        let record = {
            let mut users = write_lock(&self.users);
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
        let next_user_id = *store.next_user_id.read();

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
            *store.next_user_id.read(),
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
