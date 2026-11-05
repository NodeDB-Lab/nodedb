// SPDX-License-Identifier: BUSL-1.1

//! Token refresh: rotate JWT without reconnecting.

use std::sync::Arc;
use std::time::Instant;

use tracing::{info, warn};

use crate::control::state::SharedState;

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
        let new_identity = match shared {
            Some(state) => crate::control::server::session_auth::authenticate_bearer_jwt(
                state,
                &msg.new_token,
                &self.device_metadata.remote_addr,
                &self.session_id,
            )
            .await
            .map(|(identity, _claims)| identity),
            None => None,
        };

        match new_identity {
            Some(new_identity) => {
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
                    expires_in_secs: 3600,
                };
                SyncFrame::try_encode(SyncMessageType::TokenRefreshAck, &ack)
            }
            None => {
                warn!(
                    session = %self.session_id,
                    "token refresh FAILED — keeping existing credentials"
                );
                // Generic on purpose, as on the handshake path: the client
                // learns the rotation failed, never why.
                refusal_ack("invalid bearer token")
            }
        }
    }
}
