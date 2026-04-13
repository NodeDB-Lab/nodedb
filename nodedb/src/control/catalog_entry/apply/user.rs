//! Apply User catalog entries to `SystemCatalog` redb.

use tracing::{debug, warn};

use crate::control::security::catalog::{StoredUser, SystemCatalog};

pub fn put(stored: &StoredUser, catalog: &SystemCatalog) {
    if let Err(e) = catalog.put_user(stored) {
        warn!(
            username = %stored.username,
            error = %e,
            "catalog_entry: put_user failed"
        );
    }
}

pub fn deactivate(username: &str, catalog: &SystemCatalog) {
    // No direct `deactivate_user(username)` on SystemCatalog —
    // load the record, flip the bit, write it back. Missing
    // record on a fresh follower is a silent no-op.
    match catalog.get_user(username) {
        Ok(Some(mut stored)) => {
            stored.is_active = false;
            if let Err(e) = catalog.put_user(&stored) {
                warn!(
                    username = %username,
                    error = %e,
                    "catalog_entry: deactivate_user put failed"
                );
            }
        }
        Ok(None) => {
            debug!(
                username = %username,
                "catalog_entry: deactivate on missing user (fresh follower)"
            );
        }
        Err(e) => warn!(
            username = %username,
            error = %e,
            "catalog_entry: deactivate_user get failed"
        ),
    }
}
