//! Apply ApiKey catalog entries to `SystemCatalog` redb.

use tracing::{debug, warn};

use crate::control::security::catalog::{StoredApiKey, SystemCatalog};

pub fn put(stored: &StoredApiKey, catalog: &SystemCatalog) {
    if let Err(e) = catalog.put_api_key(stored) {
        warn!(
            key_id = %stored.key_id,
            username = %stored.username,
            error = %e,
            "catalog_entry: put_api_key failed"
        );
    }
}

pub fn revoke(key_id: &str, catalog: &SystemCatalog) {
    // Load existing, flip `is_revoked`, put back. Missing record
    // on a fresh follower is a silent no-op matching the user /
    // collection drop pattern.
    match catalog.get_api_key(key_id) {
        Ok(Some(mut stored)) => {
            stored.is_revoked = true;
            if let Err(e) = catalog.put_api_key(&stored) {
                warn!(
                    key_id = %key_id,
                    error = %e,
                    "catalog_entry: revoke_api_key put failed"
                );
            }
        }
        Ok(None) => {
            debug!(
                key_id = %key_id,
                "catalog_entry: revoke on missing api_key (fresh follower)"
            );
        }
        Err(e) => warn!(
            key_id = %key_id,
            error = %e,
            "catalog_entry: revoke_api_key get failed"
        ),
    }
}
