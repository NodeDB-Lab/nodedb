// SPDX-License-Identifier: BUSL-1.1

//! Delta push: bounded envelope validation, rate limit, CRC32C integrity, replay dedup.
//!
//! Exact RLS is evaluated later by CRDT admission against the authoritative
//! post-merge preview, never against raw attacker-controlled delta bytes.

use std::time::Instant;

use tracing::{debug, warn};

use crate::control::security::audit::AuditLog;
use crate::control::security::rls::RlsPolicyStore;

use super::super::dlq::{DlqEnqueueParams, SyncDlq, ViolationType};
use super::super::security::{SyncRejectionReason, log_silent_rejection};
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
        _rls_store: Option<&RlsPolicyStore>,
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

        if msg.delta.len() > nodedb_crdt::DEFAULT_MAX_DELTA_BYTES {
            self.mutations_rejected += 1;
            let reject = DeltaRejectMsg {
                mutation_id: msg.mutation_id,
                reason: "CRDT delta exceeds maximum size".into(),
                compensation: Some(CompensationHint::IntegrityViolation),
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

        // Replay deduplication is handled durably by the Data-Plane idempotent
        // gate (`sync_admit`) keyed on the producer-assigned (producer_id,
        // stream_id, seq). For unfenced clients (producer_id == 0) Loro merge is
        // idempotent, so a re-applied delta converges to the same state. No
        // in-memory per-session dedup map is needed (it could not survive a
        // reconnect anyway).
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
                    tenant_id: identity.tenant_id.as_u64(),
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

        // Raw delta bytes do not describe the post-merge row. The admission
        // policy receives the authoritative preview and performs exact RLS
        // before WAL.

        self.mutations_processed += 1;

        // Record subscription so the Origin `CollectionPurged`
        // broadcast notifies this session on hard-delete of the
        // collection the client just wrote to.
        let tenant_u32 = identity.tenant_id.as_u64();
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

        // Provisional: the delta has passed envelope validation and been
        // admitted, but the durable apply has not run yet and may still refuse
        // it. Claiming `Applied` here would let a sender retire a write that
        // never landed, so this reports `Accepted` and the caller overwrites
        // the status with the real outcome once the Data Plane replies.
        let ack = DeltaAckMsg {
            mutation_id: msg.mutation_id,
            lsn: 0,
            clock_skew_warning_ms,
            applied_seq: 0,
            status: nodedb_types::sync::wire::AckStatus::Accepted,
        };
        SyncFrame::try_encode(SyncMessageType::DeltaAck, &ack)
    }
}

impl SyncSession {
    /// Record the terminal outcome of a delta whose refusal or success was
    /// decided downstream of [`Self::handle_delta_push`].
    ///
    /// The durable apply runs outside the session, so without this the session
    /// counters can only ever see admission — which is how a session that
    /// applied one write out of hundreds still closed reporting
    /// `rejected=0`. Every terminal frame returned to the client is routed
    /// through here so the close line reflects what actually happened.
    pub fn record_delta_outcome(&mut self, frame: &SyncFrame) {
        match frame.msg_type {
            SyncMessageType::DeltaReject => {
                self.mutations_rejected += 1;
            }
            SyncMessageType::DeltaAck => {
                let Some(ack) = frame.decode_body::<DeltaAckMsg>() else {
                    // An ack we cannot read is not evidence of an apply.
                    self.mutations_not_applied += 1;
                    return;
                };
                match ack.status {
                    AckStatus::Applied => self.mutations_applied += 1,
                    // A duplicate applied nothing — its operations were already
                    // there. Counting it as an apply is what made a session
                    // whose every delta was discarded close indistinguishable
                    // from one that landed every write.
                    AckStatus::Duplicate => self.mutations_deduplicated += 1,
                    // `Gap` is a retryable refusal: nothing applied, and the
                    // client is expected to re-push. It belongs with the other
                    // not-applied outcomes, not with the rejections — counting
                    // it as rejected is what made a held stream look like a
                    // permanent refusal in the session's close line.
                    AckStatus::Accepted | AckStatus::Fenced | AckStatus::Gap { .. } => {
                        self.mutations_not_applied += 1
                    }
                    AckStatus::Rejected { .. } => self.mutations_rejected += 1,
                }
            }
            _ => {}
        }
    }

    /// Record what the CRDT admission preview measured about one delta before
    /// it was applied.
    ///
    /// The ack says what the server decided; this says what the delta actually
    /// carried. Only the second can distinguish a client whose writes are being
    /// discarded from one that is merely re-sending history, because both
    /// produce the same ack.
    pub fn record_delta_admission(&mut self, trimmed_ops: u64) {
        self.ops_trimmed = self.ops_trimmed.saturating_add(trimmed_ops);
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
    use super::SyncSession;
    use super::compute_clock_skew_warning;
    use crate::control::security::audit::AuditLog;
    use crate::control::security::identity::AuthenticatedIdentity;
    use crate::control::server::sync::dlq::{DlqConfig, SyncDlq};
    use crate::control::server::sync::rate_limit::RateLimitConfig;
    use crate::control::server::sync::wire::*;
    use crate::types::TenantId;
    use nodedb_types::sync::wire::AckStatus;

    fn make_session() -> SyncSession {
        SyncSession::new("test-session-1".into())
    }

    #[test]
    fn delta_push_rejected_before_auth() {
        let mut session = make_session();

        let msg = DeltaPushMsg {
            collection: "docs".into(),
            document_id: "d1".into(),
            delta: vec![1, 2, 3],
            peer_id: 1,
            mutation_id: 100,
            device_id: 0,
            delta_signature: [0; 32],
            collection_incarnation: nodedb_types::Hlc::ZERO,
            checksum: 0,
            device_valid_time_ms: None,
            producer_id: 0,
            epoch: 0,
            seq: 0,
        };

        let response = session.handle_delta_push(&msg, None, None, None);
        assert!(response.is_some());
        let frame = response.unwrap();
        assert_eq!(frame.msg_type, SyncMessageType::DeltaReject);
        assert_eq!(session.mutations_rejected, 1);
    }

    #[test]
    fn delta_push_accepted_when_authenticated() {
        let mut session = make_authenticated_session();

        let data = serde_json::json!({"status": "active"});
        let msg = DeltaPushMsg {
            collection: "orders".into(),
            device_id: 0,
            delta_signature: [0; 32],
            collection_incarnation: nodedb_types::Hlc::ZERO,
            document_id: "o1".into(),
            delta: nodedb_types::json_to_msgpack(&data).unwrap(),
            peer_id: 1,
            mutation_id: 42,
            checksum: 0,
            device_valid_time_ms: None,
            producer_id: 0,
            epoch: 0,
            seq: 0,
        };

        let response = session.handle_delta_push(&msg, None, None, None);
        assert!(response.is_some());
        assert_eq!(response.unwrap().msg_type, SyncMessageType::DeltaAck);
        assert_eq!(session.mutations_processed, 1);
        // The subscription tracker picked up the collection.
        assert!(
            session
                .tracked_collections
                .contains(&(1, "orders".to_string()))
        );
    }

    #[test]
    fn delta_push_defers_rls_until_authoritative_admission() {
        let mut session = make_authenticated_session();

        use crate::control::security::predicate::{CompareOp, PredicateValue, RlsPredicate};
        use crate::control::security::rls::{PolicyType, RlsPolicy, RlsPolicyStore};

        let rls_store = RlsPolicyStore::new();
        let predicate = RlsPredicate::Compare {
            field: "status".into(),
            op: CompareOp::Eq,
            value: PredicateValue::Literal(serde_json::json!("active")),
        };
        rls_store
            .create_policy(RlsPolicy {
                name: "require_active".into(),
                collection: "orders".into(),
                display_collection: "orders".into(),
                tenant_id: 1,
                policy_type: PolicyType::Write,
                compiled_predicate: Some(predicate),
                mode: crate::control::security::predicate::PolicyMode::default(),
                on_deny: Default::default(),
                enabled: true,
                created_by: "admin".into(),
                created_at: 0,
            })
            .unwrap();

        let mut audit_log = AuditLog::new(100);
        let mut dlq = SyncDlq::new(DlqConfig::default());

        let data = serde_json::json!({"status": "draft"});
        let msg = DeltaPushMsg {
            collection: "orders".into(),
            device_id: 0,
            delta_signature: [0; 32],
            collection_incarnation: nodedb_types::Hlc::ZERO,
            document_id: "o1".into(),
            delta: nodedb_types::json_to_msgpack(&data).unwrap(),
            peer_id: 1,
            mutation_id: 42,
            checksum: 0,
            device_valid_time_ms: None,
            producer_id: 0,
            epoch: 0,
            seq: 0,
        };

        let response =
            session.handle_delta_push(&msg, Some(&rls_store), Some(&mut audit_log), Some(&mut dlq));

        assert_eq!(
            response.expect("preliminary ack").msg_type,
            SyncMessageType::DeltaAck
        );
        assert_eq!(session.mutations_silent_dropped, 0);
        assert_eq!(session.mutations_processed, 1);
        assert_eq!(audit_log.len(), 0);
        assert_eq!(dlq.total_entries(), 0);
    }

    #[test]
    fn oversized_delta_rejects_before_rate_limit_or_dlq_clone() {
        let mut session = make_authenticated_session();
        let mut audit_log = AuditLog::new(100);
        let mut dlq = SyncDlq::new(DlqConfig::default());
        let msg = DeltaPushMsg {
            collection: "orders".into(),
            document_id: "o1".into(),
            delta: vec![0; nodedb_crdt::DEFAULT_MAX_DELTA_BYTES + 1],
            peer_id: 1,
            mutation_id: 43,
            device_id: 0,
            delta_signature: [0; 32],
            collection_incarnation: nodedb_types::Hlc::ZERO,
            checksum: 0,
            device_valid_time_ms: None,
            producer_id: 0,
            epoch: 0,
            seq: 0,
        };

        let response = session
            .handle_delta_push(&msg, None, Some(&mut audit_log), Some(&mut dlq))
            .expect("oversized reject");
        assert_eq!(response.msg_type, SyncMessageType::DeltaReject);
        assert_eq!(session.mutations_rejected, 1);
        assert_eq!(session.mutations_processed, 0);
        assert_eq!(session.mutations_silent_dropped, 0);
        assert_eq!(audit_log.len(), 0);
        assert_eq!(dlq.total_entries(), 0);
    }

    #[test]
    fn delta_push_rate_limited_silent_drop() {
        let rate_config = RateLimitConfig {
            rate_per_sec: 0.0,
            burst: 1,
        };
        let mut session = SyncSession::with_rate_limit("rate-test".into(), &rate_config);
        session.authenticated = true;
        session.tenant_id = Some(TenantId::new(1));
        session.username = Some("bob".into());
        session.identity = Some(AuthenticatedIdentity::new_regular(
            2,
            "bob",
            TenantId::new(1),
            crate::control::security::identity::AuthMethod::ApiKey,
            vec![crate::control::security::identity::Role::ReadWrite],
            None,
            AuthenticatedIdentity::default_database_set(false),
        ));

        let data = serde_json::json!({"key": "value"});
        let msg = DeltaPushMsg {
            collection: "docs".into(),
            document_id: "d1".into(),
            delta: nodedb_types::json_to_msgpack(&data).unwrap(),
            peer_id: 1,
            mutation_id: 1,
            device_id: 0,
            delta_signature: [0; 32],
            collection_incarnation: nodedb_types::Hlc::ZERO,
            checksum: 0,
            device_valid_time_ms: None,
            producer_id: 0,
            epoch: 0,
            seq: 0,
        };

        let r1 = session.handle_delta_push(&msg, None, None, None);
        assert!(r1.is_some());
        assert_eq!(session.mutations_processed, 1);

        let mut audit_log = AuditLog::new(100);
        let mut dlq = SyncDlq::new(DlqConfig::default());

        let msg2 = DeltaPushMsg {
            collection: "docs".into(),
            document_id: "d2".into(),
            delta: nodedb_types::json_to_msgpack(&data).unwrap(),
            peer_id: 1,
            mutation_id: 2,
            device_id: 0,
            delta_signature: [0; 32],
            collection_incarnation: nodedb_types::Hlc::ZERO,
            checksum: 0,
            device_valid_time_ms: None,
            producer_id: 0,
            epoch: 0,
            seq: 0,
        };
        let r2 = session.handle_delta_push(&msg2, None, Some(&mut audit_log), Some(&mut dlq));
        assert!(r2.is_none());
        assert_eq!(session.mutations_silent_dropped, 1);
        assert_eq!(dlq.total_entries(), 1);
    }

    /// The Control Plane no longer keeps an in-memory replay-dedup map: every
    /// `handle_delta_push` is processed. Idempotency is enforced durably at the
    /// Data-Plane gate (`sync_admit`, keyed on producer-assigned seq — see the
    /// `sync_gate` tests); for unfenced clients (producer_id == 0, as here) Loro
    /// merge is idempotent, so re-applying converges to the same state. This test
    /// pins the new contract: no CP-side short-circuit on a repeated mutation_id.
    #[test]
    fn delta_push_has_no_cp_side_dedup() {
        let mut session = make_authenticated_session();

        let data = serde_json::json!({"key": "value"});
        let delta = nodedb_types::json_to_msgpack(&data).unwrap();

        let make = |mutation_id: u64, doc: &str| DeltaPushMsg {
            collection: "docs".into(),
            document_id: doc.into(),
            delta: delta.clone(),
            peer_id: 42,
            mutation_id,
            device_id: 0,
            delta_signature: [0; 32],
            collection_incarnation: nodedb_types::Hlc::ZERO,
            checksum: 0,
            device_valid_time_ms: None,
            producer_id: 0,
            epoch: 0,
            seq: 0,
        };

        // Same mutation_id twice, then an older id, then a newer id — every one is
        // processed by the CP now (dedup is the gate's job, not the session's).
        for (mid, doc) in [(5u64, "d1"), (5, "d1"), (3, "d0"), (6, "d2")] {
            let r = session.handle_delta_push(&make(mid, doc), None, None, None);
            assert_eq!(
                r.expect("delta ack").msg_type,
                SyncMessageType::DeltaAck,
                "every delta push is acked"
            );
        }
        assert_eq!(
            session.mutations_processed, 4,
            "CP processes every delta — no in-memory replay-dedup short-circuit"
        );
    }

    #[test]
    fn crc32c_mismatch_rejects_delta() {
        let mut session = make_authenticated_session();

        let data = serde_json::json!({"key": "value"});
        let delta = nodedb_types::json_to_msgpack(&data).unwrap();

        let valid_checksum = crc32c::crc32c(&delta);
        let msg_ok = DeltaPushMsg {
            collection: "docs".into(),
            document_id: "d1".into(),
            delta: delta.clone(),
            peer_id: 1,
            mutation_id: 1,
            device_id: 0,
            delta_signature: [0; 32],
            collection_incarnation: nodedb_types::Hlc::ZERO,
            checksum: valid_checksum,
            device_valid_time_ms: None,
            producer_id: 0,
            epoch: 0,
            seq: 0,
        };
        let r1 = session.handle_delta_push(&msg_ok, None, None, None);
        assert!(r1.is_some());
        assert_eq!(r1.unwrap().msg_type, SyncMessageType::DeltaAck);

        let msg_bad = DeltaPushMsg {
            collection: "docs".into(),
            document_id: "d2".into(),
            delta,
            peer_id: 1,
            mutation_id: 2,
            device_id: 0,
            delta_signature: [0; 32],
            collection_incarnation: nodedb_types::Hlc::ZERO,
            checksum: valid_checksum ^ 0xDEAD,
            device_valid_time_ms: None,
            producer_id: 0,
            epoch: 0,
            seq: 0,
        };
        let r2 = session.handle_delta_push(&msg_bad, None, None, None);
        assert!(r2.is_some());
        assert_eq!(r2.unwrap().msg_type, SyncMessageType::DeltaReject);
        assert_eq!(session.mutations_rejected, 1);
    }

    /// The session emits its `DeltaAck` before the delta has been dispatched to
    /// the Data Plane, and stamps it `AckStatus::Applied`. Nothing has been
    /// applied at that point — the durable apply happens afterwards, and may be
    /// refused. If the connection drops in that window, or the caller forwards the
    /// provisional frame, the client records a write that does not exist.
    ///
    /// An acknowledgement must never claim `Applied` before an apply has occurred.
    #[test]
    fn provisional_delta_ack_does_not_claim_applied() {
        let mut session = make_authenticated_session();

        let data = serde_json::json!({"status": "active"});
        let msg = DeltaPushMsg {
            collection: "orders".into(),
            document_id: "o1".into(),
            delta: nodedb_types::json_to_msgpack(&data).unwrap(),
            peer_id: 1,
            mutation_id: 42,
            checksum: 0,
            device_valid_time_ms: None,
            producer_id: 0,
            epoch: 0,
            seq: 0,
            device_id: 0,
            delta_signature: [0; 32],
            collection_incarnation: nodedb_types::Hlc::ZERO,
        };

        let frame = session
            .handle_delta_push(&msg, None, None, None)
            .expect("an accepted push returns a frame");
        assert_eq!(frame.msg_type, SyncMessageType::DeltaAck);

        let ack: DeltaAckMsg = frame.decode_body().expect("ack body decodes");
        assert_ne!(
            ack.status,
            AckStatus::Applied,
            "the session acknowledged a delta as Applied before it was dispatched \
             to the Data Plane, let alone applied"
        );
    }

    /// What a session's close line says about the deltas it handled.
    ///
    /// These counters are the operator's only window onto a sync session, so the
    /// distinctions they draw are the only ones anybody downstream can act on.
    /// Every test here pins a pair of outcomes that produce the same client-visible
    /// ack but opposite facts about the database.
    fn make_authenticated_session() -> SyncSession {
        let mut session = SyncSession::new("counter-session".into());
        session.authenticated = true;
        session.tenant_id = Some(TenantId::new(1));
        session.username = Some("alice".into());
        session.identity = Some(AuthenticatedIdentity::new_regular(
            1,
            "alice",
            TenantId::new(1),
            crate::control::security::identity::AuthMethod::ApiKey,
            vec![crate::control::security::identity::Role::ReadWrite],
            None,
            AuthenticatedIdentity::default_database_set(false),
        ));
        session
    }

    /// The counters a session closes with have to distinguish a client whose
    /// writes landed from one whose writes were absorbed. Both produce a
    /// successful ack, so folding `Duplicate` into `applied` made the second
    /// indistinguishable from the first — which is how a session that
    /// materialized nothing closed reporting hundreds of applied mutations.
    #[test]
    fn a_deduplicated_delta_is_not_counted_as_applied() {
        let mut session = make_authenticated_session();

        let applied = SyncFrame::try_encode(
            SyncMessageType::DeltaAck,
            &DeltaAckMsg {
                mutation_id: 1,
                lsn: 0,
                clock_skew_warning_ms: None,
                applied_seq: 1,
                status: AckStatus::Applied,
            },
        )
        .expect("ack encodes");
        let duplicate = SyncFrame::try_encode(
            SyncMessageType::DeltaAck,
            &DeltaAckMsg {
                mutation_id: 2,
                lsn: 0,
                clock_skew_warning_ms: None,
                applied_seq: 2,
                status: AckStatus::Duplicate,
            },
        )
        .expect("ack encodes");

        session.record_delta_outcome(&applied);
        session.record_delta_outcome(&duplicate);

        assert_eq!(session.mutations_applied, 1);
        assert_eq!(session.mutations_deduplicated, 1);
        assert_eq!(session.mutations_rejected, 0);
    }

    /// The trim count is what turns "every delta was deduplicated" from an
    /// indistinguishable state into a visible one, so it must accumulate across
    /// the session rather than reporting only the last delta.
    #[test]
    fn trimmed_operations_accumulate_across_the_session() {
        let mut session = make_authenticated_session();
        assert_eq!(session.ops_trimmed, 0);
        session.record_delta_admission(3);
        session.record_delta_admission(4);
        assert_eq!(session.ops_trimmed, 7);
    }

    /// A delta that trimmed nothing must leave the counter alone: a resync that
    /// re-sends known history is normal, and a counter that ticked on every delta
    /// would say nothing about which sessions to look at.
    #[test]
    fn a_delta_that_trims_nothing_leaves_the_counter_untouched() {
        let mut session = make_authenticated_session();
        session.record_delta_admission(0);
        assert_eq!(session.ops_trimmed, 0);
    }

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
