//! Read-only accessors: list users, check emptiness, expose the
//! underlying `SystemCatalog`.

use super::super::super::catalog::SystemCatalog;
use super::super::record::UserRecord;
use super::core::{CredentialStore, read_lock};

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
        use super::super::record::UserRecord;
        let stored_users = catalog.load_all_users()?;
        let mut users = match self.users.write() {
            Ok(u) => u,
            Err(_) => {
                return Err(crate::Error::Internal {
                    detail: "credential store write lock poisoned in reload_from_catalog".into(),
                });
            }
        };
        users.clear();
        for stored in stored_users {
            let record = UserRecord::from_stored(stored);
            users.insert(record.username.clone(), record);
        }
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
    pub fn catalog(&self) -> &Option<SystemCatalog> {
        &self.catalog
    }
}
