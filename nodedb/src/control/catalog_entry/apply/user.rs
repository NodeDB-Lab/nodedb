// SPDX-License-Identifier: BUSL-1.1

//! Apply User catalog entries to `SystemCatalog` redb.

use crate::control::security::catalog::{StoredUser, SystemCatalog, catalog_err};
use crate::control::security::role_assignment;

use super::outcome::ApplyOutcome;

/// Write the user, unless an active user names a role that is neither built
/// in nor defined in its tenant at this log position. The statement checked
/// before proposing; a role dropped after that check and before this entry
/// committed is caught here, on every node alike.
pub fn put(stored: &StoredUser, catalog: &SystemCatalog) -> crate::Result<ApplyOutcome> {
    if stored.is_active {
        let roles = catalog.load_all_roles()?;
        if let Err(refusal) = role_assignment::check_stored_user(stored, &roles) {
            tracing::warn!(
                user = %stored.username,
                %refusal,
                "catalog_entry: user entry refused; it names an undefined role"
            );
            return Ok(ApplyOutcome::Refused(refusal));
        }
    }
    catalog
        .put_user(stored)
        .map_err(|e| catalog_err(&format!("put_user '{}'", stored.username), e))?;
    Ok(ApplyOutcome::Applied)
}

/// Fully remove the user record from redb. `delete_user` is idempotent — a
/// missing record on a fresh follower succeeds (redb `remove` tolerates it).
pub fn delete(username: &str, catalog: &SystemCatalog) -> crate::Result<()> {
    catalog
        .delete_user(username)
        .map_err(|e| catalog_err(&format!("delete_user '{username}'"), e))
}
