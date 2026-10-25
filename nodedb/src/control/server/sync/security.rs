// SPDX-License-Identifier: BUSL-1.1

//! Sync-layer security: JWT upgrade validation and silent rejection logging.
//!
//! CRDT RLS is deliberately evaluated by Control admission against the exact
//! Data-Plane post-merge preview, not raw delta bytes.
//!
//! ## JWT on WebSocket Upgrade
//!
//! The JWT token is validated on initial WebSocket connection (via query
//! parameter `?token=<jwt>` or the handshake message). The extracted
//! `AuthenticatedIdentity` is stored for the session lifetime. Periodic
//! refresh checks are performed: when the token's `exp` is within the
//! refresh window, the server sends a `TokenRefreshRequired` hint so
//! the client can re-authenticate without disconnection.
//!
//! ## Silent Rejection
//!
//! Rate-limited mutations may be dropped without a response while a full
//! forensic audit event records the session, principal, target, reason, and
//! payload hash. Authorization/RLS denials use typed `DeltaReject` responses.

use std::time::{SystemTime, UNIX_EPOCH};

use sha2::{Digest, Sha256};
use tracing::warn;

use super::wire::DeltaPushMsg;
use crate::control::security::audit::{AuditEvent, AuditLog};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::security::jwt::{JwtConfig, JwtError, JwtValidator};
use crate::control::security::util::base64_url_decode;

/// Result of JWT validation on WebSocket upgrade.
#[derive(Debug)]
pub enum UpgradeAuthResult {
    /// Authentication succeeded; session may proceed.
    Authenticated {
        identity: AuthenticatedIdentity,
        /// Seconds until token expires (for refresh scheduling).
        expires_in_secs: u64,
    },
    /// Authentication failed; connection should be closed.
    Rejected { reason: String },
}

/// Validate a JWT token during WebSocket upgrade.
///
/// Called before the sync session is created. If this returns `Rejected`,
/// the WebSocket connection is closed immediately with a 4001 close code.
pub fn validate_upgrade_token(token: &str, config: &JwtConfig) -> UpgradeAuthResult {
    let validator = JwtValidator::new(config.clone());
    match validator.validate(token) {
        Ok(identity) => {
            // Decode claims again to get raw `exp` for refresh scheduling.
            let expires_in = extract_exp_from_token(token).unwrap_or(0);
            let now = now_epoch_secs();
            let remaining = expires_in.saturating_sub(now);

            UpgradeAuthResult::Authenticated {
                identity,
                expires_in_secs: remaining,
            }
        }
        Err(e) => UpgradeAuthResult::Rejected {
            reason: e.to_string(),
        },
    }
}

/// Check if a token needs refresh (within refresh window of expiry).
///
/// Returns `Some(remaining_secs)` if the token should be refreshed,
/// `None` if still healthy.
pub fn check_token_refresh_needed(
    token: &str,
    config: &JwtConfig,
    shared: &crate::control::state::SharedState,
) -> Option<u64> {
    let token_refresh_window_secs = shared.tuning.network.token_refresh_window_secs;
    // Re-validate to check if still valid.
    let validator = JwtValidator::new(config.clone());
    match validator.validate(token) {
        Ok(_) => {
            let exp = extract_exp_from_token(token).unwrap_or(0);
            if exp == 0 {
                return None; // No expiry set.
            }
            let now = now_epoch_secs();
            let remaining = exp.saturating_sub(now);
            if remaining <= token_refresh_window_secs {
                Some(remaining)
            } else {
                None
            }
        }
        Err(JwtError::Expired) => Some(0), // Already expired.
        Err(_) => None,                    // Other errors — not a refresh issue.
    }
}

/// Extract the `exp` claim from a JWT without full validation.
fn extract_exp_from_token(token: &str) -> Option<u64> {
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return None;
    }
    let payload = base64_url_decode(parts[1])?;
    let claims: serde_json::Value = crate::util::bounded_json::from_slice(&payload).ok()?;
    claims.get("exp")?.as_u64()
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
    audit_log: &mut AuditLog,
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

    audit_log.record(
        AuditEvent::AuthzDenied,
        Some(identity.tenant_id),
        session_id,
        &detail,
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

/// Current epoch seconds.
fn now_epoch_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
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
        let mut audit_log = AuditLog::new(100);
        let identity = test_identity(1, "alice");
        let delta = make_delta("orders", "o1", &serde_json::json!({"x": 1}));
        let reason = SyncRejectionReason::RateLimited { retry_after_ms: 25 };

        log_silent_rejection(&mut audit_log, "sess-1", &identity, &delta, &reason);

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
        {
            let mut log = audit.lock().expect("audit lock");
            log_silent_rejection(&mut log, "sess-crash", &identity, &delta, &reason);
        }
        drop(audit);

        let replayed = replay_after_crash(&wal);
        assert!(
            replayed
                .iter()
                .any(|e| e.source == "sess-crash" && e.event == AuditEvent::AuthzDenied),
            "the sync rejection entry must replay after a crash: {replayed:?}"
        );
    }

    #[test]
    fn upgrade_rejects_bad_token() {
        let config = JwtConfig::default();
        let result = validate_upgrade_token("bad.token.here", &config);
        assert!(matches!(result, UpgradeAuthResult::Rejected { .. }));
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
