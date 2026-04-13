//! ApiKey post-apply side effects — upsert / mark-revoked the
//! in-memory `api_keys` cache.

use std::sync::Arc;

use crate::control::security::catalog::StoredApiKey;
use crate::control::state::SharedState;

pub fn put(stored: StoredApiKey, shared: Arc<SharedState>) {
    shared.api_keys.install_replicated_key(&stored);
}

pub fn revoke(key_id: String, shared: Arc<SharedState>) {
    shared.api_keys.install_replicated_revoke(&key_id);
}
