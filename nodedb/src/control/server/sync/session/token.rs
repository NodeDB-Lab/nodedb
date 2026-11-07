// SPDX-License-Identifier: BUSL-1.1

//! Token refresh: rotate JWT without reconnecting.

use std::sync::Arc;
use std::time::Instant;

use tracing::{info, warn};

use crate::control::state::SharedState;

use super::super::security::{
    UpgradeAuthResult, check_token_refresh_needed, validate_upgrade_token,
};
use super::super::wire::*;
use super::state::SyncSession;

/// Build a failed `TokenRefreshAckMsg` carrying `message` in `error`, so every
/// refusal branch below produces the same envelope shape.
fn refusal_ack(message: &str) -> Option<SyncFrame> {
    let ack = TokenRefreshAckMsg {
        success: false,
        error: Some(message.into()),
        expires_in_secs: 0,
    };
    SyncFrame::try_encode(SyncMessageType::TokenRefreshAck, &ack)
}

impl SyncSession {
    /// Handle a token refresh request. Validate the new JWT, and if
    /// it belongs to the same tenant, upgrade the session with the
    /// new credentials. Invalid tokens keep the existing session
    /// credentials and respond with an error.
    ///
    /// The replacement token is authenticated through the same gate as the
    /// handshake, so a session cannot rotate onto a credential the server
    /// would have refused at connect time.
    pub async fn handle_token_refresh(
        &mut self,
        msg: &TokenRefreshMsg,
        shared: Option<&Arc<SharedState>>,
    ) -> Option<SyncFrame> {
        self.last_activity = Instant::now();

        if msg.new_token.is_empty() {
            return refusal_ack("empty token");
        }

        // No verifier (no `SharedState`, or no `[auth.jwt]` provider) means the
        // replacement credential cannot be checked, so the rotation is refused
        // and the session keeps the identity it already proved.
        let validated = validate_upgrade_token(
            shared.map(Arc::as_ref),
            &msg.new_token,
            &self.device_metadata.remote_addr,
            &self.session_id,
        )
        .await;

        match validated {
            UpgradeAuthResult::Authenticated {
                identity: new_identity,
                expires_in_secs,
            } => {
                if let Some(current_tenant) = self.tenant_id
                    && new_identity.tenant_id != current_tenant
                {
                    warn!(
                        session = %self.session_id,
                        current_tenant = current_tenant.as_u64(),
                        new_tenant = new_identity.tenant_id.as_u64(),
                        "token refresh rejected: tenant mismatch"
                    );
                    return refusal_ack("tenant mismatch");
                }
                if let Some(current) = self.identity.as_ref() {
                    if new_identity.user_id != current.user_id {
                        warn!(
                            session = %self.session_id,
                            current_user_id = current.user_id,
                            new_user_id = new_identity.user_id,
                            "token refresh rejected: user mismatch"
                        );
                        return refusal_ack("user mismatch");
                    }

                    let current_database = current
                        .default_database
                        .unwrap_or(nodedb_types::DatabaseId::DEFAULT);
                    let new_database = new_identity
                        .default_database
                        .unwrap_or(nodedb_types::DatabaseId::DEFAULT);
                    if new_database != current_database {
                        warn!(
                            session = %self.session_id,
                            current_database = current_database.as_u64(),
                            new_database = new_database.as_u64(),
                            "token refresh rejected: database mismatch"
                        );
                        return refusal_ack("database mismatch");
                    }
                }
                self.username = Some(new_identity.username.clone());
                if let (Some(binding), Some(expiry_ms)) =
                    (self.binding.as_ref(), new_identity.token_expiry_ms)
                {
                    binding.set_token_expiry_ms(expiry_ms);
                }
                self.identity = Some(new_identity);
                info!(
                    session = %self.session_id,
                    "JWT token refreshed successfully"
                );
                let ack = TokenRefreshAckMsg {
                    success: true,
                    error: None,
                    expires_in_secs,
                };
                SyncFrame::try_encode(SyncMessageType::TokenRefreshAck, &ack)
            }
            UpgradeAuthResult::Rejected { reason } => {
                warn!(
                    session = %self.session_id,
                    "token refresh FAILED — keeping existing credentials"
                );
                refusal_ack(reason)
            }
        }
    }

    /// The `TokenRefreshRequired` hint for a token inside the refresh window.
    /// One hint goes out per token: a rotation brings a new expiry, and a new
    /// hint when that expiry nears.
    pub fn token_refresh_hint(
        &mut self,
        refresh_window_secs: u64,
        now_ms: u64,
    ) -> Option<SyncFrame> {
        let expiry_ms = self.identity.as_ref()?.token_expiry_ms;
        let expires_in_secs = check_token_refresh_needed(expiry_ms, refresh_window_secs, now_ms)?;
        if self.refresh_hinted_expiry_ms == expiry_ms {
            return None;
        }
        self.refresh_hinted_expiry_ms = expiry_ms;
        SyncFrame::try_encode(
            SyncMessageType::TokenRefreshRequired,
            &TokenRefreshRequiredMsg { expires_in_secs },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::identity::{AuthMethod, AuthenticatedIdentity};
    use crate::types::TenantId;

    const NOW_MS: u64 = 1_000_000;
    const WINDOW_SECS: u64 = 300;

    fn session_with_expiry(expiry_ms: Option<u64>) -> SyncSession {
        let mut identity = AuthenticatedIdentity::new_regular(
            1,
            "bearer",
            TenantId::new(1),
            AuthMethod::ApiKey,
            Vec::new(),
            None,
            AuthenticatedIdentity::default_database_set(false),
        );
        identity.token_expiry_ms = expiry_ms;
        let mut session = SyncSession::new("s".into());
        session.identity = Some(identity);
        session
    }

    #[test]
    fn one_hint_goes_out_per_token() {
        let near = Some(NOW_MS + 60_000);
        let mut session = session_with_expiry(near);

        let hint = session
            .token_refresh_hint(WINDOW_SECS, NOW_MS)
            .expect("a token inside the window draws a hint");
        assert_eq!(hint.msg_type, SyncMessageType::TokenRefreshRequired);
        let body: TokenRefreshRequiredMsg = hint.decode_body().expect("decode hint");
        assert_eq!(body.expires_in_secs, 60);
        assert!(session.token_refresh_hint(WINDOW_SECS, NOW_MS).is_none());

        // A rotation onto a token that also nears its expiry hints again.
        if let Some(identity) = session.identity.as_mut() {
            identity.token_expiry_ms = Some(NOW_MS + 120_000);
        }
        assert!(session.token_refresh_hint(WINDOW_SECS, NOW_MS).is_some());
    }

    #[test]
    fn no_hint_goes_out_for_a_healthy_or_absent_expiry() {
        let mut healthy = session_with_expiry(Some(NOW_MS + 3_600_000));
        assert!(healthy.token_refresh_hint(WINDOW_SECS, NOW_MS).is_none());
        let mut no_token = session_with_expiry(None);
        assert!(no_token.token_refresh_hint(WINDOW_SECS, NOW_MS).is_none());
        let mut unauthenticated = SyncSession::new("s".into());
        assert!(
            unauthenticated
                .token_refresh_hint(WINDOW_SECS, NOW_MS)
                .is_none()
        );
    }
}
