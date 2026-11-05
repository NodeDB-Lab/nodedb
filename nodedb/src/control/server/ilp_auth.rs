// SPDX-License-Identifier: BUSL-1.1

//! Native-protocol authentication adapter for the ILP listener.
//!
//! ILP has no credential grammar of its own. A connection must therefore
//! complete the native Hello and one native `Auth` request before its raw ILP
//! bytes can be accepted by the listener that owns the connection afterwards.

use nodedb_types::protocol::{NativeRequest, NativeResponse, OpCode, RequestFields};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite};
use tokio::time::timeout;

use crate::config::auth::AuthMode;
use crate::control::security::audit::ArcAuditEmitter;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::native::{codec, handshake};
use crate::control::server::shared::authorization::authorize_database;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

/// Maximum native Auth frame accepted before switching to raw ILP input.
pub(crate) const ILP_AUTH_MAX_FRAME_SIZE: u32 = 64 * 1024;

/// Authentication result bound to one ILP connection.
///
/// The fields remain private so callers cannot substitute a tenant or database
/// after authentication. The listener must derive all ingest scope from this
/// context.
#[derive(Debug, Clone)]
pub(crate) struct AuthenticatedIlpContext {
    identity: AuthenticatedIdentity,
    database_id: DatabaseId,
    format: codec::FrameFormat,
    auth_seq: u64,
    peer_addr: String,
}

impl AuthenticatedIlpContext {
    pub(crate) fn identity(&self) -> &AuthenticatedIdentity {
        &self.identity
    }

    /// The identity, for the session binding's refresh. It rebuilds the identity
    /// from the credential store and keeps the same user id.
    pub(crate) fn identity_mut(&mut self) -> &mut AuthenticatedIdentity {
        &mut self.identity
    }

    pub(crate) fn database_id(&self) -> DatabaseId {
        self.database_id
    }

    /// Remote peer address supplied to `authenticate_ilp_connection`, stored
    /// so later per-batch admission checks (`check_request_admission`) have
    /// a real address for the IP-blacklist half of the check.
    pub(crate) fn peer_addr(&self) -> &str {
        &self.peer_addr
    }
}

/// Fail-closed outcomes for the one-shot native authentication prelude.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IlpAuthenticationError {
    HelloFailed,
    CertificateAuthenticationUnsupported,
    MissingAuthFrame,
    InvalidAuthFrame,
    AuthFrameTimeout,
    AuthRequired,
    AuthenticationFailed,
    DatabaseResolutionFailed,
    DatabaseAccessDenied,
    ResponseWriteFailed,
}

impl IlpAuthenticationError {
    /// Every externally visible ILP auth rejection uses one indistinguishable
    /// code. In particular, do not disclose whether a requested database
    /// exists or is merely inaccessible to the supplied identity.
    fn response_code(self) -> &'static str {
        "28000"
    }
}

/// Authenticate an ILP connection with the native Hello + one Auth request.
///
/// On every adapter failure, emits a generic framed response when the frame
/// format is known and then returns a typed error. It deliberately consumes no
/// raw ILP bytes: the caller may begin ILP parsing only after this returns a
/// context successfully.
pub(crate) async fn authenticate_ilp_connection<S>(
    stream: &mut S,
    state: &SharedState,
    auth_mode: &AuthMode,
    peer_addr: &str,
) -> Result<AuthenticatedIlpContext, IlpAuthenticationError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    handshake::perform_server_handshake(stream, &state.limits)
        .await
        .map_err(|_| IlpAuthenticationError::HelloFailed)?;

    let payload = match timeout(
        Duration::from_secs(5),
        codec::read_frame_with_max(stream, ILP_AUTH_MAX_FRAME_SIZE),
    )
    .await
    {
        Ok(Ok(Some(payload))) => payload,
        Ok(Ok(None)) => {
            write_safe_failure(
                stream,
                codec::FrameFormat::MessagePack,
                IlpAuthenticationError::MissingAuthFrame,
                0,
            )
            .await;
            return Err(IlpAuthenticationError::MissingAuthFrame);
        }
        Ok(Err(_)) => {
            write_safe_failure(
                stream,
                codec::FrameFormat::MessagePack,
                IlpAuthenticationError::InvalidAuthFrame,
                0,
            )
            .await;
            return Err(IlpAuthenticationError::InvalidAuthFrame);
        }
        Err(_) => {
            write_safe_failure(
                stream,
                codec::FrameFormat::MessagePack,
                IlpAuthenticationError::AuthFrameTimeout,
                0,
            )
            .await;
            return Err(IlpAuthenticationError::AuthFrameTimeout);
        }
    };
    if payload.is_empty() {
        write_safe_failure(
            stream,
            codec::FrameFormat::MessagePack,
            IlpAuthenticationError::InvalidAuthFrame,
            0,
        )
        .await;
        return Err(IlpAuthenticationError::InvalidAuthFrame);
    }
    let format = codec::FrameFormat::detect_payload(&payload);
    let request = match codec::decode_request(&payload, format) {
        Ok(request) => request,
        Err(_) => {
            write_safe_failure(stream, format, IlpAuthenticationError::InvalidAuthFrame, 0).await;
            return Err(IlpAuthenticationError::InvalidAuthFrame);
        }
    };
    let seq = request.seq;

    // ILP has no certificate-identity plumbing. Still consume and validate the
    // one bounded Auth frame so this path cannot be confused with raw ILP,
    // then reject it without inspecting or accepting its credentials.
    if *auth_mode == AuthMode::Certificate {
        if extract_auth_fields(request).is_err() {
            write_safe_failure(
                stream,
                format,
                IlpAuthenticationError::InvalidAuthFrame,
                seq,
            )
            .await;
            return Err(IlpAuthenticationError::InvalidAuthFrame);
        }
        write_safe_failure(
            stream,
            format,
            IlpAuthenticationError::CertificateAuthenticationUnsupported,
            seq,
        )
        .await;
        return Err(IlpAuthenticationError::CertificateAuthenticationUnsupported);
    }

    let (identity, database_id) =
        match authenticate_request(state, auth_mode, peer_addr, request).await {
            Ok(context) => context,
            Err(error) => {
                write_safe_failure(stream, format, error, seq).await;
                return Err(error);
            }
        };
    Ok(AuthenticatedIlpContext {
        identity,
        database_id,
        format,
        auth_seq: seq,
        peer_addr: peer_addr.to_string(),
    })
}

async fn authenticate_request(
    state: &SharedState,
    auth_mode: &AuthMode,
    peer_addr: &str,
    request: NativeRequest,
) -> Result<(AuthenticatedIdentity, DatabaseId), IlpAuthenticationError> {
    let fields = extract_auth_fields(request)?;
    let auth = fields
        .auth
        .as_ref()
        .ok_or(IlpAuthenticationError::AuthRequired)?;
    // ILP ingest has no RLS/`$auth.*` surface to enrich, so the verified-JWT
    // proof `handle_auth` returns for OIDC bearer auth is discarded here —
    // only the resulting identity (and its authority) matters for this path.
    let identity =
        crate::control::server::native::dispatch::handle_auth(state, auth_mode, auth, peer_addr)
            .await
            .map_err(|_| IlpAuthenticationError::AuthenticationFailed)?
            .identity;

    let database_id = resolve_database(state, &identity, fields.database.as_deref())?;
    let audit = ArcAuditEmitter(std::sync::Arc::clone(&state.audit));
    authorize_database(&identity, database_id, &audit)
        .map_err(|_| IlpAuthenticationError::DatabaseAccessDenied)?;

    Ok((identity, database_id))
}

fn extract_auth_fields(
    request: NativeRequest,
) -> Result<nodedb_types::protocol::TextFields, IlpAuthenticationError> {
    if request.op != OpCode::Auth {
        return Err(IlpAuthenticationError::AuthRequired);
    }
    match request.fields {
        RequestFields::Text(fields) => Ok(fields),
        _ => Err(IlpAuthenticationError::InvalidAuthFrame),
    }
}

fn resolve_database(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    requested_name: Option<&str>,
) -> Result<DatabaseId, IlpAuthenticationError> {
    let requested_name = requested_name.filter(|name| !name.is_empty());
    let database_id = match requested_name {
        Some(name) => state
            .credentials
            .catalog()
            .get_database_id_by_name(name)
            .map_err(|_| IlpAuthenticationError::DatabaseResolutionFailed)?
            .ok_or(IlpAuthenticationError::DatabaseResolutionFailed)?,
        None => default_database(identity),
    };

    // A stored user default can outlive a dropped database. Resolve by ID too,
    // so every accepted connection is pinned to a currently extant database.
    state
        .credentials
        .catalog()
        .get_database(database_id)
        .map_err(|_| IlpAuthenticationError::DatabaseResolutionFailed)?
        .ok_or(IlpAuthenticationError::DatabaseResolutionFailed)?;
    Ok(database_id)
}

fn default_database(identity: &AuthenticatedIdentity) -> DatabaseId {
    identity.default_database.unwrap_or(DatabaseId::DEFAULT)
}

/// Send the success response only after connection admission has succeeded.
pub(crate) async fn write_ilp_auth_success<S>(
    stream: &mut S,
    context: &AuthenticatedIlpContext,
) -> Result<(), IlpAuthenticationError>
where
    S: AsyncWrite + Unpin,
{
    let response = NativeResponse::auth_ok(
        context.auth_seq,
        context.identity.username.clone(),
        context.identity.tenant_id.as_u64(),
    );
    let bytes = codec::encode_response(&response, context.format)
        .map_err(|_| IlpAuthenticationError::ResponseWriteFailed)?;
    codec::write_frame(stream, &bytes)
        .await
        .map_err(|_| IlpAuthenticationError::ResponseWriteFailed)
}

pub(crate) async fn write_ilp_auth_failure<S>(stream: &mut S, context: &AuthenticatedIlpContext)
where
    S: AsyncWrite + Unpin,
{
    write_safe_failure(
        stream,
        context.format,
        IlpAuthenticationError::AuthenticationFailed,
        context.auth_seq,
    )
    .await;
}

async fn write_safe_failure<S>(
    stream: &mut S,
    format: codec::FrameFormat,
    error: IlpAuthenticationError,
    seq: u64,
) where
    S: AsyncWrite + Unpin,
{
    let response = NativeResponse::error(seq, error.response_code(), "authentication failed");
    if let Ok(bytes) = codec::encode_response(&response, format) {
        let _ = codec::write_frame(stream, &bytes).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::protocol::{AuthMethod, RequestFields, TextFields};

    use crate::control::security::audit::NoopAuditEmitter;
    use crate::control::server::shared::authorization::authorize_database;

    fn request(op: OpCode, auth: Option<AuthMethod>) -> NativeRequest {
        NativeRequest {
            op,
            seq: 7,
            fields: RequestFields::Text(TextFields {
                auth,
                ..Default::default()
            }),
        }
    }

    #[test]
    fn only_auth_opcode_is_accepted_as_first_request() {
        let auth = AuthMethod::ApiKey {
            token: "not-used".into(),
        };
        assert!(extract_auth_fields(request(OpCode::Auth, Some(auth))).is_ok());
        assert!(matches!(
            extract_auth_fields(request(OpCode::Ping, None)),
            Err(IlpAuthenticationError::AuthRequired)
        ));
        assert!(matches!(
            authenticate_request_fields_missing_auth(),
            Err(IlpAuthenticationError::AuthRequired)
        ));
    }

    #[test]
    fn default_database_prefers_identity_default() {
        let identity = AuthenticatedIdentity::new_regular(
            1,
            "ingest",
            crate::types::TenantId::new(4),
            crate::control::security::identity::AuthMethod::ApiKey,
            Vec::new(),
            Some(DatabaseId::new(8)),
            AuthenticatedIdentity::default_database_set(false),
        );
        assert_eq!(default_database(&identity), DatabaseId::new(8));
        assert!(authorize_database(&identity, DatabaseId::new(8), &NoopAuditEmitter).is_err());
    }

    fn authenticate_request_fields_missing_auth()
    -> Result<nodedb_types::protocol::TextFields, IlpAuthenticationError> {
        let fields = extract_auth_fields(request(OpCode::Auth, None))?;
        fields
            .auth
            .as_ref()
            .ok_or(IlpAuthenticationError::AuthRequired)?;
        Ok(fields)
    }

    #[test]
    fn safe_failures_never_expose_internal_reason() {
        let response = NativeResponse::error(
            0,
            IlpAuthenticationError::DatabaseAccessDenied.response_code(),
            "authentication failed",
        );
        assert_eq!(
            response.error.as_ref().map(|error| error.message.as_str()),
            Some("authentication failed")
        );
        assert_eq!(
            response.error.as_ref().map(|error| error.code.as_str()),
            Some("28000")
        );
        assert_eq!(
            IlpAuthenticationError::DatabaseResolutionFailed.response_code(),
            IlpAuthenticationError::DatabaseAccessDenied.response_code()
        );
    }

    #[test]
    fn certificate_mode_has_explicit_fail_closed_outcome() {
        assert_eq!(
            IlpAuthenticationError::CertificateAuthenticationUnsupported.response_code(),
            "28000"
        );
    }

    #[test]
    fn malformed_or_eof_and_timeout_are_distinct_typed_outcomes() {
        assert_ne!(
            IlpAuthenticationError::MissingAuthFrame,
            IlpAuthenticationError::InvalidAuthFrame
        );
        assert_ne!(
            IlpAuthenticationError::AuthFrameTimeout,
            IlpAuthenticationError::InvalidAuthFrame
        );
        assert_eq!(
            IlpAuthenticationError::AuthFrameTimeout.response_code(),
            "28000"
        );
    }
}
