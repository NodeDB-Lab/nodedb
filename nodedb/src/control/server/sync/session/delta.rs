//! Delta push: rate limit, CRC32C integrity, RLS, replay dedup.

use std::time::Instant;

use tracing::{debug, warn};

use crate::control::security::audit::AuditLog;
use crate::control::security::rls::RlsPolicyStore;

use super::super::dlq::{DlqEnqueueParams, SyncDlq, ViolationType};
use super::super::security::{SyncRejectionReason, enforce_rls_on_delta, log_silent_rejection};
use super::super::wire::*;
use super::state::SyncSession;

impl SyncSession {
    /// Process a delta push: validate, enforce security, and prepare
    /// for WAL commit. Returns `Some(SyncFrame)` with DeltaAck /
    /// DeltaReject, `None` when the mutation is silently dropped
    /// (security rejection).
    pub fn handle_delta_push(
        &mut self,
        msg: &DeltaPushMsg,
        rls_store: Option<&RlsPolicyStore>,
        audit_log: Option<&mut AuditLog>,
        dlq: Option<&mut SyncDlq>,
    ) -> Option<SyncFrame> {
        self.last_activity = Instant::now();

        if !self.authenticated {
            self.mutations_rejected += 1;
            let reject = DeltaRejectMsg {
                mutation_id: msg.mutation_id,
                reason: "not authenticated".into(),
                compensation: Some(CompensationHint::PermissionDenied),
            };
            return SyncFrame::try_encode(SyncMessageType::DeltaReject, &reject);
        }

        if msg.delta.is_empty() {
            self.mutations_rejected += 1;
            let reject = DeltaRejectMsg {
                mutation_id: msg.mutation_id,
                reason: "empty delta".into(),
                compensation: None,
            };
            return SyncFrame::try_encode(SyncMessageType::DeltaReject, &reject);
        }

        // CRC32C integrity check (skip for legacy clients with checksum=0).
        if msg.checksum != 0 {
            let computed = crc32c::crc32c(&msg.delta);
            if computed != msg.checksum {
                self.mutations_rejected += 1;
                warn!(
                    session = %self.session_id,
                    mutation_id = msg.mutation_id,
                    expected = msg.checksum,
                    computed,
                    "CRC32C checksum mismatch on delta payload"
                );
                let reject = DeltaRejectMsg {
                    mutation_id: msg.mutation_id,
                    reason: format!(
                        "CRC32C mismatch: expected {:#010x}, computed {:#010x}",
                        msg.checksum, computed
                    ),
                    compensation: Some(CompensationHint::IntegrityViolation),
                };
                return SyncFrame::try_encode(SyncMessageType::DeltaReject, &reject);
            }
        }

        // Update device metadata peer_id on first delta.
        if self.device_metadata.peer_id == 0 {
            self.device_metadata.peer_id = msg.peer_id;
        }

        // Replay deduplication.
        if let Some(&last_seen) = self.last_seen_mutation.get(&msg.peer_id)
            && msg.mutation_id <= last_seen
        {
            debug!(
                session = %self.session_id,
                peer_id = msg.peer_id,
                mutation_id = msg.mutation_id,
                last_seen,
                "replay dedup: skipping already-processed delta"
            );
            let ack = DeltaAckMsg {
                mutation_id: msg.mutation_id,
                lsn: 0,
                clock_skew_warning_ms: None,
            };
            return SyncFrame::try_encode(SyncMessageType::DeltaAck, &ack);
        }

        let identity = match &self.identity {
            Some(id) => id.clone(),
            None => {
                self.mutations_rejected += 1;
                let reject = DeltaRejectMsg {
                    mutation_id: msg.mutation_id,
                    reason: "identity not established".into(),
                    compensation: Some(CompensationHint::PermissionDenied),
                };
                return SyncFrame::try_encode(SyncMessageType::DeltaReject, &reject);
            }
        };

        // Rate limiting.
        if let Err(retry_after_ms) = self.rate_limiter.try_acquire() {
            let reason = SyncRejectionReason::RateLimited { retry_after_ms };
            if let Some(audit) = audit_log {
                log_silent_rejection(audit, &self.session_id, &identity, msg, &reason);
            }
            if let Some(q) = dlq {
                q.enqueue(DlqEnqueueParams {
                    session_id: self.session_id.clone(),
                    tenant_id: identity.tenant_id.as_u32(),
                    username: identity.username.clone(),
                    collection: msg.collection.clone(),
                    document_id: msg.document_id.clone(),
                    mutation_id: msg.mutation_id,
                    peer_id: msg.peer_id,
                    delta: msg.delta.clone(),
                    violation_type: ViolationType::RateLimited,
                    compensation: Some(CompensationHint::RateLimited { retry_after_ms }),
                    device_metadata: self.device_metadata.clone(),
                });
            }
            self.mutations_silent_dropped += 1;
            return None;
        }

        // RLS enforcement.
        if let Some(rls) = rls_store
            && let Err(reason) = enforce_rls_on_delta(msg, &identity, rls)
        {
            if let Some(audit) = audit_log {
                log_silent_rejection(audit, &self.session_id, &identity, msg, &reason);
            }
            if let Some(q) = dlq {
                let violation = match &reason {
                    SyncRejectionReason::RlsPolicyViolation { policy_name } => {
                        ViolationType::RlsPolicyViolation {
                            policy_name: policy_name.clone(),
                        }
                    }
                    _ => ViolationType::PermissionDenied,
                };
                q.enqueue(DlqEnqueueParams {
                    session_id: self.session_id.clone(),
                    tenant_id: identity.tenant_id.as_u32(),
                    username: identity.username.clone(),
                    collection: msg.collection.clone(),
                    document_id: msg.document_id.clone(),
                    mutation_id: msg.mutation_id,
                    peer_id: msg.peer_id,
                    delta: msg.delta.clone(),
                    violation_type: violation,
                    compensation: Some(CompensationHint::PermissionDenied),
                    device_metadata: self.device_metadata.clone(),
                });
            }
            self.mutations_silent_dropped += 1;
            return None;
        }

        self.mutations_processed += 1;
        self.last_seen_mutation
            .entry(msg.peer_id)
            .and_modify(|v| *v = (*v).max(msg.mutation_id))
            .or_insert(msg.mutation_id);

        // Record subscription so the Origin `CollectionPurged`
        // broadcast notifies this session on hard-delete of the
        // collection the client just wrote to.
        let tenant_u32 = identity.tenant_id.as_u32();
        self.track_collection(tenant_u32, &msg.collection);

        debug!(
            session = %self.session_id,
            collection = %msg.collection,
            doc = %msg.document_id,
            mutation_id = msg.mutation_id,
            delta_bytes = msg.delta.len(),
            "delta push accepted"
        );

        let clock_skew_warning_ms = compute_clock_skew_warning(msg.device_valid_time_ms);
        if let Some(skew) = clock_skew_warning_ms {
            warn!(
                session = %self.session_id,
                mutation_id = msg.mutation_id,
                skew_ms = skew,
                "device clock skew exceeds 24h tolerance"
            );
        }

        let ack = DeltaAckMsg {
            mutation_id: msg.mutation_id,
            lsn: 0,
            clock_skew_warning_ms,
        };
        SyncFrame::try_encode(SyncMessageType::DeltaAck, &ack)
    }
}

/// Return `Some(skew_ms)` when the device-reported valid-time deviates
/// from the Origin wall clock by more than 24 hours. Returning `None`
/// inside tolerance keeps the ack payload small on the common path.
fn compute_clock_skew_warning(device_valid_time_ms: Option<i64>) -> Option<i64> {
    let device_ms = device_valid_time_ms?;
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0);
    let skew = now_ms - device_ms;
    const TOLERANCE_MS: i64 = 24 * 60 * 60 * 1000;
    if skew.abs() > TOLERANCE_MS {
        Some(skew)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::compute_clock_skew_warning;

    #[test]
    fn none_device_time_returns_none() {
        assert_eq!(compute_clock_skew_warning(None), None);
    }

    #[test]
    fn within_tolerance_returns_none() {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        // 1 hour off — within 24h tolerance.
        assert_eq!(compute_clock_skew_warning(Some(now - 3_600_000)), None);
    }

    #[test]
    fn past_tolerance_returns_skew() {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        // 25 hours in the past.
        let device = now - 25 * 3_600_000;
        let skew = compute_clock_skew_warning(Some(device)).expect("should warn");
        assert!(skew > 24 * 3_600_000);
    }

    #[test]
    fn future_past_tolerance_returns_negative_skew() {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        // 25 hours in the future.
        let device = now + 25 * 3_600_000;
        let skew = compute_clock_skew_warning(Some(device)).expect("should warn");
        assert!(skew < -24 * 3_600_000);
    }
}
