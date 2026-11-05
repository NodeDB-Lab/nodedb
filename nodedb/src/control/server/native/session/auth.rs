// SPDX-License-Identifier: BUSL-1.1

//! Native-protocol auth handshake: authenticates the client, assembles the
//! three-level (global/database/tenant) admission permit, and builds the
//! auth response.

use nodedb_types::protocol::{NativeResponse, RequestFields};

use crate::control::security::audit::ArcAuditEmitter;
use crate::control::server::admission::{AdmissionError, ConnectionPermit, ScopedConnectionPermit};
use crate::control::server::shared::authorization::authorize_database;

use super::NativeSession;
use super::dispatch;
use crate::control::server::native::dispatch::{
    error_to_native_in_context, error_to_native_with_sqlstate,
};
use crate::control::server::native::sqlstate_code::sqlstate_error;

impl NativeSession {
    /// Handle authentication request.
    pub(super) async fn handle_auth(&mut self, seq: u64, fields: &RequestFields) -> NativeResponse {
        // Re-authentication is not supported on the native protocol. Once a
        // session has assembled its three-level admission permit, the identity
        // is fixed for the connection's lifetime — allowing re-auth would let
        // a client silently swap to a different (database, tenant) scope while
        // still holding the original scope's connection slots.
        if self.identity.is_some() || self.connection_permit.is_some() {
            return sqlstate_error(
                seq,
                "0A000",
                "already authenticated; reconnect to switch identity",
            );
        }

        let auth = match fields {
            RequestFields::Text(f) => match &f.auth {
                Some(a) => a,
                None => {
                    return sqlstate_error(seq, "28000", "missing 'auth' field");
                }
            },
            _ => {
                return sqlstate_error(seq, "0A000", "unsupported request fields variant");
            }
        };

        match dispatch::handle_auth(
            &self.state,
            &self.auth_mode,
            auth,
            &self.peer_addr.to_string(),
        )
        .await
        {
            Ok(dispatch::NativeAuthOutcome {
                identity,
                warning,
                verified_jwt,
            }) => {
                // The TLS policy is evaluated here, before any capacity is
                // acquired or session state is written: this is the first
                // point where the connection's negotiated transport and the
                // identity's superuser flag are both known, and a refused
                // connection must not have consumed an admission slot.
                if let Err(e) = crate::control::server::session_auth::check_transport_security(
                    &self.state,
                    &identity,
                    self.transport,
                    &self.peer_addr.to_string(),
                ) {
                    return error_to_native_with_sqlstate(seq, "28000", &e);
                }

                // Bind the requested database before acquiring any scoped
                // admission capacity. An absent or empty name uses the
                // authenticated identity's default, then the system default.
                let requested_database = match fields {
                    RequestFields::Text(f) => f.database.as_deref().filter(|name| !name.is_empty()),
                    _ => None,
                };
                let catalog = self.state.credentials.catalog();
                let db_id = match requested_database {
                    Some(name) => match catalog.get_database_id_by_name(name) {
                        Ok(Some(db_id)) => db_id,
                        Ok(None) => {
                            return sqlstate_error(
                                seq,
                                "3D000",
                                "selected database does not exist",
                            );
                        }
                        Err(e) => {
                            return error_to_native_in_context(
                                seq,
                                "database catalog lookup failed",
                                &e,
                            );
                        }
                    },
                    None => identity
                        .default_database
                        .unwrap_or(nodedb_types::DatabaseId::DEFAULT),
                };

                // Name lookup only validates the reverse catalog index. Confirm
                // the descriptor exists for both explicit and default/fallback
                // selections before authorization, admission, or session state.
                match catalog.get_database(db_id) {
                    Ok(Some(_)) => {}
                    Ok(None) => {
                        return sqlstate_error(seq, "3D000", "selected database does not exist");
                    }
                    Err(e) => {
                        return error_to_native_in_context(
                            seq,
                            "database catalog lookup failed",
                            &e,
                        );
                    }
                }

                // A selected database must be authorized before it can consume
                // database- or tenant-scoped capacity or mutate session state.
                let audit = ArcAuditEmitter(std::sync::Arc::clone(&self.state.audit));
                if authorize_database(&identity, db_id, &audit).is_err() {
                    return sqlstate_error(seq, "42501", "permission denied for database");
                }

                // OIDC bearer auth carries verified claims: build the initial
                // `AuthContext` with claim-derived enrichment (email, org,
                // groups, permissions, metadata) via `from_verified_jwt`, the
                // same constructor HTTP's bearer-token path uses. Authority
                // (superuser, tenant, roles) still comes from `identity`
                // alone — `from_verified_jwt` never lets the token elevate.
                // Every other auth method keeps the identity-only context.
                // The context is built before admission capacity is acquired,
                // so a session-id generation error leaves no state behind.
                // A bearer login reuses the session id its `AuthSuccess` entry recorded.
                let auth_context_result = match &verified_jwt {
                    Some(bearer) => Ok(
                        crate::control::security::auth_context::AuthContext::from_verified_jwt(
                            &bearer.claims,
                            &identity,
                            bearer.session_id.clone(),
                        ),
                    ),
                    None => super::super::super::session_auth::build_auth_context(&identity),
                };
                let mut auth_context = match auth_context_result {
                    Ok(ctx) => ctx,
                    Err(e) => {
                        return error_to_native_in_context(seq, "session id generation failed", &e);
                    }
                };
                // The selected database may differ from the identity default.
                // Keep RLS `$auth.database_id` aligned with the database bound
                // to this native connection.
                auth_context.database_id = Some(db_id);

                // Scoped admission: acquire per-database and per-tenant permits
                // only after authentication and database authorization succeed.
                // `acquire` releases the database slot itself when the tenant
                // slot is refused, so a rejected client holds no capacity.
                let tenant_id = identity.tenant_id;

                let scoped = match ScopedConnectionPermit::acquire(
                    &self.admission_registry,
                    db_id,
                    tenant_id,
                ) {
                    Ok(scoped) => scoped,
                    Err(e) => {
                        return NativeResponse::error_with_code(
                            seq,
                            nodedb_types::error::sqlstate::QUOTA_EXCEEDED,
                            format!("{e}"),
                            admission_error_code(&e),
                        );
                    }
                };

                // Assemble the three-level permit. The global slot moves from
                // `global_permit` into the `ConnectionPermit`. The re-auth
                // guard at the top of this function ensures `global_permit`
                // is still `Some` here — it is initialized at construction
                // and only consumed on the auth path.
                let Some(global) = self.global_permit.take() else {
                    // Release the freshly acquired scoped permits so they
                    // don't leak slots into the per-DB / per-tenant pools.
                    drop(scoped);
                    return sqlstate_error(
                        seq,
                        nodedb_types::error::sqlstate::INTERNAL_ERROR,
                        "internal error: global admission permit missing during auth assembly",
                    );
                };
                self.connection_permit = Some(ConnectionPermit::assemble(global, scoped));
                self.connection_guard = Some(self.state.connection_tracker.track_native(db_id));

                // Auth succeeds only once the selected database is persisted
                // for every subsequent SQL and direct native operation.
                self.sessions.ensure_session(self.peer_addr);
                self.sessions.set_current_database(self.peer_addr, db_id);

                let mut resp = NativeResponse::auth_ok(
                    seq,
                    identity.username.clone(),
                    identity.tenant_id.as_u64(),
                );
                if let Some(w) = warning {
                    resp.warnings.push(w);
                }
                self.auth_context = Some(auth_context);
                // Retained so every subsequent request on this connection can
                // rebuild its per-request `RequestAuthScope` with the same
                // claim-derived enrichment (see `session::request::handle_request`),
                // not just the auth frame's own response.
                self.verified_jwt = verified_jwt.map(|bearer| bearer.claims);
                self.cleanup.publish_identity(identity.clone());
                self.identity = Some(identity);
                resp
            }
            // A transient login rate-limit is distinct from a credential
            // failure: it maps to TOO_MANY_CONNECTIONS (53300), which clients
            // recognise as retryable, and carries a distinct message. Every
            // other auth error (wrong password, lockout, unknown user) stays
            // collapsed into the generic invalid-password 28P01 so none can be
            // distinguished from the others.
            Err(e @ crate::Error::RateExceeded { .. }) => error_to_native_with_sqlstate(
                seq,
                nodedb_types::error::sqlstate::TOO_MANY_CONNECTIONS,
                &e,
            ),
            Err(e) => sqlstate_error(seq, "28P01", format!("{e}")),
        }
    }
}

/// Native error code for an admission refusal, per exhausted scope.
fn admission_error_code(error: &AdmissionError) -> u16 {
    match error {
        AdmissionError::DatabaseCapExhausted { .. } => {
            nodedb_types::error::ErrorCode::DATABASE_QUOTA_EXCEEDED.0
        }
        AdmissionError::TenantCapExhausted { .. } => {
            nodedb_types::error::ErrorCode::TENANT_QUOTA_EXCEEDED.0
        }
    }
}
