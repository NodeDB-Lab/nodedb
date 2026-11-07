// SPDX-License-Identifier: BUSL-1.1

//! Sync-layer security: bearer validation, token refresh hints, and silent
//! rejection logging.
//!
//! CRDT RLS is deliberately evaluated by Control admission against the exact
//! Data-Plane post-merge preview, not raw delta bytes.
//!
//! ## JWT on the handshake
//!
//! A `Handshake` frame and a `TokenRefresh` frame each carry a bearer token.
//! Both pass the gate every bearer route uses: the `[auth.jwt]` and catalog
//! providers, then the stateful JWT policy. The session keeps the identity and
//! its expiry. When the expiry enters the refresh window, the server sends a
//! `TokenRefreshRequired` hint, so the client can re-authenticate without
//! disconnection.
//!
//! ## Silent Rejection
//!
//! Rate-limited mutations may be dropped without a response while a full
//! forensic audit event records the session, principal, target, reason, and
//! payload hash. Authorization/RLS denials use typed `DeltaReject` responses.

use std::sync::Mutex;

use sha2::{Digest, Sha256};
use tracing::warn;

use super::wire::DeltaPushMsg;
use crate::control::security::audit::{
    AuditAuth, AuditEvent, AuditLog, AuditRecordInput, record_concurrent,
};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::session_auth::authenticate_bearer_jwt;
use crate::control::state::SharedState;

/// The reason a client receives for a refused bearer token. It is generic on
/// purpose, as on the HTTP bearer route: the client learns it is
/// unauthenticated, never why.
const INVALID_BEARER_TOKEN: &str = "invalid bearer token";

/// Result of validating a bearer token a sync client presents.
#[derive(Debug)]
pub enum UpgradeAuthResult {
    /// Authentication succeeded; session may proceed.
    Authenticated {
        identity: AuthenticatedIdentity,
        /// Seconds until token expires (for refresh scheduling).
        expires_in_secs: u64,
    },
    /// Authentication failed. The session stays unauthenticated.
    Rejected { reason: &'static str },
}

/// Validate a bearer token from a `Handshake` or `TokenRefresh` frame.
///
/// With no `SharedState` nothing can verify the token, so it is rejected.
pub async fn validate_upgrade_token(
    shared: Option<&SharedState>,
    token: &str,
    peer_addr: &str,
    session_id: &str,
) -> UpgradeAuthResult {
    let verified = match shared {
        Some(state) => authenticate_bearer_jwt(state, token, peer_addr, session_id).await,
        None => None,
    };
    match verified {
        Some((identity, _claims)) => UpgradeAuthResult::Authenticated {
            expires_in_secs: identity
                .token_expiry_ms
                .map_or(0, |expiry_ms| remaining_secs(expiry_ms, now_epoch_ms())),
            identity,
        },
        None => UpgradeAuthResult::Rejected {
            reason: INVALID_BEARER_TOKEN,
        },
    }
}

/// Check if a session's token needs refresh (within refresh window of expiry).
///
/// Returns `Some(remaining_secs)` if the token should be refreshed. Returns
/// `None` if it is still healthy, or if the identity has no token expiry.
pub fn check_token_refresh_needed(
    token_expiry_ms: Option<u64>,
    refresh_window_secs: u64,
    now_ms: u64,
) -> Option<u64> {
    let remaining = remaining_secs(token_expiry_ms?, now_ms);
    (remaining <= refresh_window_secs).then_some(remaining)
}

/// Whole seconds from `now_ms` to `expiry_ms`, `0` once it passed.
fn remaining_secs(expiry_ms: u64, now_ms: u64) -> u64 {
    expiry_ms.saturating_sub(now_ms) / 1_000
}

/// Reason a sync delta was silently rejected.
#[derive(Debug, Clone)]
pub enum SyncRejectionReason {
    /// Rate limit exceeded.
    RateLimited { retry_after_ms: u64 },
    /// Token expired mid-session.
    TokenExpired,
}

impl std::fmt::Display for SyncRejectionReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RateLimited { retry_after_ms } => {
                write!(f, "rate limited (retry after {retry_after_ms}ms)")
            }
            Self::TokenExpired => write!(f, "token expired"),
        }
    }
}

/// Log a silent rejection to the audit log with full forensic detail.
///
/// No DeltaReject frame is sent to the client — this prevents information
/// leakage about which policies exist or what conditions triggered rejection.
pub fn log_silent_rejection(
    audit_log: &Mutex<AuditLog>,
    session_id: &str,
    identity: &AuthenticatedIdentity,
    delta: &DeltaPushMsg,
    reason: &SyncRejectionReason,
) {
    let delta_hash = sha256_hex(&delta.delta);

    let detail = format!(
        "sync silent reject: session={}, user={}, tenant={}, collection={}, doc={}, mutation_id={}, reason={}, delta_hash={}, delta_len={}",
        session_id,
        identity.username,
        identity.tenant_id.as_u64(),
        delta.collection,
        delta.document_id,
        delta.mutation_id,
        reason,
        delta_hash,
        delta.delta.len(),
    );

    record_concurrent(
        audit_log,
        AuditRecordInput {
            event: AuditEvent::AuthzDenied,
            tenant_id: Some(identity.tenant_id),
            database_id: None,
            source: session_id,
            detail: &detail,
            auth: &AuditAuth::default(),
        },
    );

    warn!(
        session = session_id,
        user = %identity.username,
        collection = %delta.collection,
        doc = %delta.document_id,
        mutation_id = delta.mutation_id,
        reason = %reason,
        "sync delta silently rejected"
    );
}

/// SHA-256 hex digest of arbitrary bytes (for forensic logging).
fn sha256_hex(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    format!("{:x}", hasher.finalize())
}

/// Current epoch milliseconds.
pub(super) fn now_epoch_ms() -> u64 {
    nodedb_types::wall_now_ns() / nodedb_types::NANOS_PER_MS as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::TenantId;

    fn test_identity(tenant: u64, username: &str) -> AuthenticatedIdentity {
        AuthenticatedIdentity::new_regular(
            1,
            username,
            TenantId::new(tenant),
            crate::control::security::identity::AuthMethod::ApiKey,
            vec![crate::control::security::identity::Role::ReadWrite],
            None,
            AuthenticatedIdentity::default_database_set(false),
        )
    }

    fn make_delta(collection: &str, doc_id: &str, data: &serde_json::Value) -> DeltaPushMsg {
        DeltaPushMsg {
            collection: collection.into(),
            document_id: doc_id.into(),
            delta: nodedb_types::json_to_msgpack(data).unwrap(),
            peer_id: 1,
            mutation_id: 42,
            device_id: 0,
            delta_signature: [0; 32],
            collection_incarnation: nodedb_types::Hlc::ZERO,
            checksum: 0,
            device_valid_time_ms: None,
            producer_id: 0,
            epoch: 0,
            seq: 0,
        }
    }

    #[test]
    fn silent_rejection_logs_audit() {
        let audit_log = Mutex::new(AuditLog::new(100));
        let identity = test_identity(1, "alice");
        let delta = make_delta("orders", "o1", &serde_json::json!({"x": 1}));
        let reason = SyncRejectionReason::RateLimited { retry_after_ms: 25 };

        log_silent_rejection(&audit_log, "sess-1", &identity, &delta, &reason);

        let audit_log = audit_log.lock().expect("audit lock");
        assert_eq!(audit_log.len(), 1);
        let entry = &audit_log.all()[0];
        assert_eq!(entry.event, AuditEvent::AuthzDenied);
        assert!(entry.detail.contains("rate limited"));
        assert!(entry.detail.contains("alice"));
        assert!(entry.detail.contains("orders"));
        assert!(entry.detail.contains("delta_hash="));
    }

    #[test]
    fn silent_rejection_entry_survives_a_crash() {
        use crate::control::security::audit::restore::{audit_with_test_wal, replay_after_crash};

        let dir = tempfile::tempdir().expect("tempdir");
        let (audit, wal) = audit_with_test_wal(dir.path());
        let identity = test_identity(1, "carol");
        let delta = make_delta("orders", "o2", &serde_json::json!({"x": 2}));
        let reason = SyncRejectionReason::RateLimited { retry_after_ms: 10 };
        log_silent_rejection(&audit, "sess-crash", &identity, &delta, &reason);
        drop(audit);

        let replayed = replay_after_crash(&wal);
        assert!(
            replayed
                .iter()
                .any(|e| e.source == "sess-crash" && e.event == AuditEvent::AuthzDenied),
            "the sync rejection entry must replay after a crash: {replayed:?}"
        );
    }

    #[tokio::test]
    async fn a_token_with_nothing_to_verify_it_is_rejected() {
        let result = validate_upgrade_token(None, "bad.token.here", "", "s").await;
        assert!(matches!(
            result,
            UpgradeAuthResult::Rejected {
                reason: INVALID_BEARER_TOKEN
            }
        ));
    }

    #[test]
    fn a_refresh_is_due_only_inside_the_window() {
        const NOW_MS: u64 = 1_000_000;
        assert_eq!(check_token_refresh_needed(None, 300, NOW_MS), None);
        let expiry = |secs: u64| Some(NOW_MS + secs * 1_000);
        assert_eq!(check_token_refresh_needed(expiry(301), 300, NOW_MS), None);
        assert_eq!(
            check_token_refresh_needed(expiry(300), 300, NOW_MS),
            Some(300)
        );
        assert_eq!(check_token_refresh_needed(expiry(5), 300, NOW_MS), Some(5));
        assert_eq!(
            check_token_refresh_needed(Some(NOW_MS - 1), 300, NOW_MS),
            Some(0),
            "an expired token is due"
        );
    }

    #[test]
    fn sha256_hex_deterministic() {
        let h1 = sha256_hex(b"hello");
        let h2 = sha256_hex(b"hello");
        assert_eq!(h1, h2);
        assert_eq!(h1.len(), 64); // SHA-256 = 32 bytes = 64 hex chars.

        let h3 = sha256_hex(b"world");
        assert_ne!(h1, h3);
    }
}
