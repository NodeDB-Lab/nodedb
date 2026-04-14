//! Sync-layer security: JWT upgrade validation, per-delta RLS enforcement,
//! and silent rejection with forensic audit logging.
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
//! ## Per-Delta RLS
//!
//! Each incoming `DeltaPush` is evaluated against the tenant's RLS write
//! policies for the target collection. The delta's document content is
//! deserialized and checked against all active write predicates. If any
//! predicate rejects the delta, it is silently dropped.
//!
//! ## Silent Rejection
//!
//! Unauthorized mutations are dropped without sending a `DeltaReject`
//! frame to the client (no information leakage). A full forensic audit
//! event is logged with: session_id, tenant_id, username, collection,
//! document_id, mutation_id, rejection reason, and delta payload hash.

use std::time::{SystemTime, UNIX_EPOCH};

use sha2::{Digest, Sha256};
use sonic_rs;
use tracing::warn;

use super::wire::DeltaPushMsg;
use crate::control::security::audit::{AuditEvent, AuditLog};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::security::jwt::{JwtConfig, JwtError, JwtValidator};
use crate::control::security::rls::RlsPolicyStore;
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
    let claims: serde_json::Value = sonic_rs::from_slice(&payload).ok()?;
    claims.get("exp")?.as_u64()
}

/// Reason a sync delta was silently rejected.
#[derive(Debug, Clone)]
pub enum SyncRejectionReason {
    /// RLS write policy rejected the delta.
    RlsPolicyViolation { policy_name: String },
    /// User lacks write permission on the collection.
    InsufficientPermission,
    /// Rate limit exceeded.
    RateLimited { retry_after_ms: u64 },
    /// Token expired mid-session.
    TokenExpired,
}

impl std::fmt::Display for SyncRejectionReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RlsPolicyViolation { policy_name } => {
                write!(f, "RLS policy '{policy_name}' rejected")
            }
            Self::InsufficientPermission => write!(f, "insufficient write permission"),
            Self::RateLimited { retry_after_ms } => {
                write!(f, "rate limited (retry after {retry_after_ms}ms)")
            }
            Self::TokenExpired => write!(f, "token expired"),
        }
    }
}

/// Evaluate a delta push against RLS write policies.
///
/// Returns `Ok(())` if the delta is allowed, or `Err(reason)` if rejected.
/// The caller should silently drop rejected deltas and log via `log_silent_rejection`.
pub fn enforce_rls_on_delta(
    delta: &DeltaPushMsg,
    identity: &AuthenticatedIdentity,
    rls_store: &RlsPolicyStore,
) -> Result<(), SyncRejectionReason> {
    let tenant_id = identity.tenant_id.as_u32();

    // Get write policies for this collection.
    let write_policies = rls_store.write_policies(tenant_id, &delta.collection);
    if write_policies.is_empty() {
        return Ok(()); // No write policies → allow.
    }

    // Convert delta to msgpack bytes for binary filter evaluation.
    // If already msgpack, use as-is. Otherwise encode from JSON/CRDT.
    let doc_bytes = delta_to_msgpack_bytes(&delta.delta);

    for policy in &write_policies {
        if policy.predicate.is_empty() {
            continue; // No predicate = allow-all policy.
        }

        let filters: Vec<crate::bridge::scan_filter::ScanFilter> =
            match zerompk::from_msgpack(&policy.predicate) {
                Ok(f) => f,
                Err(_) => continue, // Skip unparseable policies.
            };

        let passes = filters.iter().all(|f| f.matches_binary(&doc_bytes));
        if !passes {
            return Err(SyncRejectionReason::RlsPolicyViolation {
                policy_name: policy.name.clone(),
            });
        }
    }

    Ok(())
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
        identity.tenant_id.as_u32(),
        delta.collection,
        delta.document_id,
        delta.mutation_id,
        reason,
        delta_hash,
        delta.delta.len(),
    );

    let event = match reason {
        SyncRejectionReason::RlsPolicyViolation { .. }
        | SyncRejectionReason::InsufficientPermission => AuditEvent::AuthzDenied,
        SyncRejectionReason::RateLimited { .. } | SyncRejectionReason::TokenExpired => {
            AuditEvent::AuthzDenied
        }
    };

    audit_log.record(event, Some(identity.tenant_id), session_id, &detail);

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

/// Try to extract a JSON document value from CRDT delta bytes.
///
/// Falls back to an empty object if the delta can't be parsed.
/// This allows RLS predicates to evaluate against the document content
/// when possible, while still allowing through deltas that are pure
/// CRDT operations (which can't be evaluated against field-level predicates).
/// Convert delta bytes to msgpack for binary filter evaluation.
///
/// If already valid msgpack map, returns as-is. Otherwise tries JSON parse
/// and re-encodes to msgpack. Falls back to empty msgpack map.
fn delta_to_msgpack_bytes(delta_bytes: &[u8]) -> Vec<u8> {
    // If already a valid msgpack map, use directly.
    if !delta_bytes.is_empty() {
        let first = delta_bytes[0];
        if (0x80..=0x8F).contains(&first) || first == 0xDE || first == 0xDF {
            return delta_bytes.to_vec();
        }
    }
    // Try raw JSON → re-encode to msgpack.
    if let Ok(json) = sonic_rs::from_slice::<serde_json::Value>(delta_bytes)
        && let Ok(mp) = nodedb_types::json_msgpack::json_to_msgpack(&json)
    {
        return mp;
    }
    // Opaque CRDT delta — return empty msgpack map (all field predicates pass vacuously).
    vec![0x80] // fixmap with 0 entries
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
    use crate::control::security::rls::{PolicyType, RlsPolicy};
    use crate::types::TenantId;

    fn test_identity(tenant: u32, username: &str) -> AuthenticatedIdentity {
        AuthenticatedIdentity {
            user_id: 1,
            username: username.into(),
            tenant_id: TenantId::new(tenant),
            auth_method: crate::control::security::identity::AuthMethod::ApiKey,
            roles: vec![crate::control::security::identity::Role::ReadWrite],
            is_superuser: false,
        }
    }

    fn make_delta(collection: &str, doc_id: &str, data: &serde_json::Value) -> DeltaPushMsg {
        DeltaPushMsg {
            collection: collection.into(),
            document_id: doc_id.into(),
            delta: nodedb_types::json_to_msgpack(data).unwrap(),
            peer_id: 1,
            mutation_id: 42,
            checksum: 0,
        }
    }

    #[test]
    fn rls_allows_when_no_policies() {
        let store = RlsPolicyStore::new();
        let identity = test_identity(1, "alice");
        let delta = make_delta("orders", "o1", &serde_json::json!({"status": "active"}));
        assert!(enforce_rls_on_delta(&delta, &identity, &store).is_ok());
    }

    #[test]
    fn rls_allows_matching_delta() {
        let store = RlsPolicyStore::new();

        let filter = crate::bridge::scan_filter::ScanFilter {
            field: "status".into(),
            op: "eq".into(),
            value: nodedb_types::Value::String("active".into()),
            clauses: Vec::new(),
            expr: None,
        };
        let predicate = zerompk::to_msgpack_vec(&vec![filter]).unwrap();

        store
            .create_policy(RlsPolicy {
                name: "require_active".into(),
                collection: "orders".into(),
                tenant_id: 1,
                policy_type: PolicyType::Write,
                predicate,
                compiled_predicate: None,
                mode: crate::control::security::predicate::PolicyMode::default(),
                on_deny: Default::default(),
                enabled: true,
                created_by: "admin".into(),
                created_at: 0,
            })
            .unwrap();

        let identity = test_identity(1, "alice");
        let delta = make_delta("orders", "o1", &serde_json::json!({"status": "active"}));
        assert!(enforce_rls_on_delta(&delta, &identity, &store).is_ok());
    }

    #[test]
    fn rls_rejects_non_matching_delta() {
        let store = RlsPolicyStore::new();

        let filter = crate::bridge::scan_filter::ScanFilter {
            field: "status".into(),
            op: "eq".into(),
            value: nodedb_types::Value::String("active".into()),
            clauses: Vec::new(),
            expr: None,
        };
        let predicate = zerompk::to_msgpack_vec(&vec![filter]).unwrap();

        store
            .create_policy(RlsPolicy {
                name: "require_active".into(),
                collection: "orders".into(),
                tenant_id: 1,
                policy_type: PolicyType::Write,
                predicate,
                compiled_predicate: None,
                mode: crate::control::security::predicate::PolicyMode::default(),
                on_deny: Default::default(),
                enabled: true,
                created_by: "admin".into(),
                created_at: 0,
            })
            .unwrap();

        let identity = test_identity(1, "alice");
        let delta = make_delta("orders", "o1", &serde_json::json!({"status": "draft"}));
        let result = enforce_rls_on_delta(&delta, &identity, &store);
        assert!(result.is_err());
        if let Err(SyncRejectionReason::RlsPolicyViolation { policy_name }) = result {
            assert_eq!(policy_name, "require_active");
        } else {
            panic!("expected RlsPolicyViolation");
        }
    }

    #[test]
    fn silent_rejection_logs_audit() {
        let mut audit_log = AuditLog::new(100);
        let identity = test_identity(1, "alice");
        let delta = make_delta("orders", "o1", &serde_json::json!({"x": 1}));
        let reason = SyncRejectionReason::RlsPolicyViolation {
            policy_name: "test_policy".into(),
        };

        log_silent_rejection(&mut audit_log, "sess-1", &identity, &delta, &reason);

        assert_eq!(audit_log.len(), 1);
        let entry = &audit_log.all()[0];
        assert_eq!(entry.event, AuditEvent::AuthzDenied);
        assert!(entry.detail.contains("test_policy"));
        assert!(entry.detail.contains("alice"));
        assert!(entry.detail.contains("orders"));
        assert!(entry.detail.contains("delta_hash="));
    }

    #[test]
    fn upgrade_rejects_bad_token() {
        let config = JwtConfig::default();
        let result = validate_upgrade_token("bad.token.here", &config);
        assert!(matches!(result, UpgradeAuthResult::Rejected { .. }));
    }

    #[test]
    fn delta_to_msgpack_handles_opaque_bytes() {
        // Truly invalid data → empty msgpack map (0x80).
        let mp = delta_to_msgpack_bytes(&[0xFF, 0xFF, 0xFF]);
        assert_eq!(mp, vec![0x80]);

        // Valid MessagePack map → returned as-is.
        let data = serde_json::json!({"key": "value"});
        let msgpack = nodedb_types::json_to_msgpack(&data).unwrap();
        let mp = delta_to_msgpack_bytes(&msgpack);
        assert_eq!(mp, msgpack);
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
