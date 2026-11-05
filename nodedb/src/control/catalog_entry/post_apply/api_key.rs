// SPDX-License-Identifier: BUSL-1.1

//! ApiKey post-apply side effects — upsert / mark-revoked the
//! in-memory `api_keys` cache.

use std::sync::Arc;

use crate::control::security::catalog::StoredApiKey;
use crate::control::security::sessions::KillReason;
use crate::control::state::SharedState;

pub fn put(stored: StoredApiKey, shared: Arc<SharedState>) {
    shared.api_keys.install_replicated_key(&stored);
}

/// Mark the key revoked and end the sessions it opened, on every node.
pub fn revoke(key_id: String, shared: Arc<SharedState>) {
    shared.api_keys.install_replicated_revoke(&key_id);
    shared
        .session_registry
        .kill_sessions_for_api_key(&key_id, KillReason::AdminKill);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::dispatch::Dispatcher;
    use crate::control::security::apikey::CreateKeyParams;
    use crate::control::security::identity::Role;
    use crate::control::security::sessions::{SessionBindRequest, bind_session};
    use crate::control::server::session_auth::identity::verify_api_key_identity;
    use crate::types::TenantId;
    use crate::wal::WalManager;

    #[tokio::test]
    async fn revoking_a_key_ends_its_sessions_and_a_password_session_stays() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = Arc::new(WalManager::open_for_testing(&dir.path().join("t.wal")).expect("wal"));
        let (dispatcher, _) = Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).expect("state");
        let user_id = state
            .credentials
            .create_user("key_owner", "pw", TenantId::new(1), vec![Role::ReadOnly])
            .expect("create user");
        let token = state
            .api_keys
            .create_key(
                CreateKeyParams {
                    username: "key_owner",
                    user_id,
                    tenant_id: TenantId::new(1),
                    expires_secs: 0,
                    scope: vec![],
                    accessible_databases: vec![],
                },
                None,
            )
            .expect("create key");
        let by_key =
            verify_api_key_identity(&state, &token, "127.0.0.1:1", "test").expect("verify key");
        let key_id = by_key
            .api_key_id
            .clone()
            .expect("the identity names its key");
        let by_password = crate::control::server::session_auth::trust_identity(&state, "key_owner")
            .expect("identity");
        let mut key_session = bind_session(
            &state,
            SessionBindRequest::new(&by_key, "test", "127.0.0.1:1"),
        )
        .expect("bind");
        let password_session = bind_session(
            &state,
            SessionBindRequest::new(&by_password, "test", "127.0.0.1:2"),
        )
        .expect("bind");

        revoke(key_id, Arc::clone(&state));

        assert!(matches!(
            key_session.ended().await,
            crate::Error::SessionKilledByAdmin
        ));
        assert!(password_session.check().is_ok());
    }
}
