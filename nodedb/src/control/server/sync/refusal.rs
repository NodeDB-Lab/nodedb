// SPDX-License-Identifier: BUSL-1.1

//! Whether a failed sync dispatch is retryable or terminal.
//!
//! Every engine ack path faces the same question and must answer it the same
//! way: the sender retires its durable entry on a terminal refusal and re-sends
//! on a retryable one, so getting this backwards either loses the write or
//! spins forever re-sending one that will never land.
//!
//! The judgment lives here, in one place, for the same reason the CRDT delta
//! path routes all three of its outcomes through a single function. Deciding
//! terminality per-channel lets two paths disagree about the same error. A
//! shared classifier cannot disagree with itself.

use nodedb_types::sync::wire::AckStatus;

use crate::bridge::envelope::ErrorCode;

/// The reason text when `error` means "nothing applied, re-send the same frame".
///
/// Matched on the typed error only — never by substring-matching the human
/// message, which is how a rewording silently turns a retry into a loss.
pub(super) fn retryable_refusal_reason(error: &crate::Error) -> Option<&str> {
    match error {
        crate::Error::RetryableRefusal { reason } => Some(reason),
        crate::Error::DataPlane(code) => match code {
            ErrorCode::RetryableRefusal { reason } => Some(reason),
            ErrorCode::DeadlineExceeded
            | ErrorCode::RejectedConstraint { .. }
            | ErrorCode::RejectedPrevalidation { .. }
            | ErrorCode::SyncRejected { .. }
            | ErrorCode::SyncNotApplied { .. }
            | ErrorCode::NotFound
            | ErrorCode::RejectedAuthz { .. }
            | ErrorCode::ConflictRetry
            | ErrorCode::CrdtFrontierMismatch { .. }
            | ErrorCode::ResourcesExhausted
            | ErrorCode::RejectedDanglingEdge { .. }
            | ErrorCode::DuplicateWrite
            | ErrorCode::AppendOnlyViolation { .. }
            | ErrorCode::BalanceViolation { .. }
            | ErrorCode::PeriodLocked { .. }
            | ErrorCode::PeriodLockMisconfigured { .. }
            | ErrorCode::RetentionViolation { .. }
            | ErrorCode::LegalHoldActive { .. }
            | ErrorCode::StateTransitionViolation { .. }
            | ErrorCode::TransitionCheckViolation { .. }
            | ErrorCode::TypeGuardViolation { .. }
            | ErrorCode::TypeMismatch { .. }
            | ErrorCode::CounterFault { .. }
            | ErrorCode::InsufficientBalance { .. }
            | ErrorCode::RateExceeded { .. }
            | ErrorCode::CollectionDraining { .. }
            | ErrorCode::RecursionDepthExceeded { .. }
            | ErrorCode::UndefinedColumn { .. }
            | ErrorCode::TextColumn { .. }
            | ErrorCode::Internal { .. }
            | ErrorCode::Unsupported { .. }
            | ErrorCode::RollbackFailed { .. }
            | ErrorCode::OllpRetryRequired
            | ErrorCode::TxnOverlayMemoryExceeded { .. }
            | ErrorCode::DivisionByZero
            | ErrorCode::UndefinedFunction { .. }
            | ErrorCode::DataException { .. }
            | ErrorCode::NumericValueOutOfRange { .. }
            | ErrorCode::DispatchCapacity { .. }
            | ErrorCode::ExpiredBeforeExecution
            | ErrorCode::BadRequest { .. }
            | ErrorCode::TransactionRollback { .. }
            | ErrorCode::ActiveSqlTransaction { .. }
            | ErrorCode::DependentObjectsExist { .. } => None,
        },
        crate::Error::RejectedConstraint { .. }
        | crate::Error::TxnOverlayMemoryExceeded { .. }
        | crate::Error::RejectedAuthz { .. }
        | crate::Error::OffsetRegression { .. }
        | crate::Error::DeadlineExceeded { .. }
        | crate::Error::ConflictRetry { .. }
        | crate::Error::CalvinSerializationConflict
        | crate::Error::CalvinParticipantError
        | crate::Error::RejectedPrevalidation { .. }
        | crate::Error::AppendOnlyViolation { .. }
        | crate::Error::BalanceViolation { .. }
        | crate::Error::MaterializedSumTargetNotFound { .. }
        | crate::Error::MaterializedSumResolutionMissing { .. }
        | crate::Error::PeriodLocked { .. }
        | crate::Error::PeriodLockMisconfigured { .. }
        | crate::Error::RetentionViolation { .. }
        | crate::Error::LegalHoldActive { .. }
        | crate::Error::StateTransitionViolation { .. }
        | crate::Error::TransitionCheckViolation { .. }
        | crate::Error::TypeGuardViolation { .. }
        | crate::Error::TypeMismatch { .. }
        | crate::Error::InsufficientBalance { .. }
        | crate::Error::RateExceeded { .. }
        | crate::Error::CollectionNotFound { .. }
        | crate::Error::DocumentNotFound { .. }
        | crate::Error::CollectionDeactivated { .. }
        | crate::Error::VShardAdmissionCapacityExceeded { .. }
        | crate::Error::CrdtAdmissionRetriesExhausted { .. }
        | crate::Error::CrdtAdmissionInvalidPlan { .. }
        | crate::Error::CrdtAdmissionCallerFence
        | crate::Error::CrdtApplyRequiresAdmission
        | crate::Error::CrdtApplyForbiddenInTransaction
        | crate::Error::NotInTransactionBlock { .. }
        | crate::Error::CrdtAdmissionTimeout { .. }
        | crate::Error::NoLeader { .. }
        | crate::Error::NotLeader { .. }
        | crate::Error::CrossCollectionNotColocated { .. }
        | crate::Error::CloneWriteRequiresMaterialize { .. }
        | crate::Error::BadRequest { .. }
        | crate::Error::BackupTenantMismatch { .. }
        | crate::Error::BackupKeyMismatch
        | crate::Error::QuotaOvercommit { .. }
        | crate::Error::PlanError { .. }
        | crate::Error::FeatureNotSupported { .. }
        | crate::Error::UndefinedFunction { .. }
        | crate::Error::UndefinedObject { .. }
        | crate::Error::ObjectNotInPrerequisiteState { .. }
        | crate::Error::UndefinedColumn { .. }
        | crate::Error::TextColumn { .. }
        | crate::Error::AmbiguousColumn { .. }
        | crate::Error::UnknownStrictField { .. }
        | crate::Error::DivisionByZero
        | crate::Error::DataException { .. }
        | crate::Error::NumericValueOutOfRange { .. }
        | crate::Error::InvalidLimitValue { .. }
        | crate::Error::RetryableSchemaChanged { .. }
        | crate::Error::RetryableLeaderChange { .. }
        | crate::Error::CommittedResultUnavailable { .. }
        | crate::Error::ProposalOutcomeUnknown { .. }
        | crate::Error::GroupQuorumUnavailable { .. }
        | crate::Error::GroupMarksUnavailable { .. }
        | crate::Error::BackupCaptureMoved { .. }
        | crate::Error::MetadataLeaderUnavailable
        | crate::Error::AuthorizationStateBehind { .. }
        | crate::Error::LinearizableReadRefused { .. }
        | crate::Error::ExecutionLimitExceeded { .. }
        | crate::Error::LimitExceeded { .. }
        | crate::Error::Wal(_)
        | crate::Error::Dispatch { .. }
        | crate::Error::DispatchCapacity { .. }
        | crate::Error::Storage { .. }
        | crate::Error::ColdStorage { .. }
        | crate::Error::Serialization { .. }
        | crate::Error::Codec { .. }
        | crate::Error::SegmentCorrupted { .. }
        | crate::Error::MemoryExhausted { .. }
        | crate::Error::Backpressure { .. }
        | crate::Error::Crdt(_)
        | crate::Error::Io(_)
        | crate::Error::Config { .. }
        | crate::Error::Encryption { .. }
        | crate::Error::Bridge { .. }
        | crate::Error::VersionCompat { .. }
        | crate::Error::RestoreTargetNotEmpty { .. }
        | crate::Error::RestoreVerificationFailed { .. }
        | crate::Error::Internal { .. }
        | crate::Error::Shaping(_)
        | crate::Error::RemoteTyped { .. }
        | crate::Error::Ddl(_)
        | crate::Error::DescriptorVersionAnomaly { .. }
        | crate::Error::CollectionPurgeRowMissing { .. }
        | crate::Error::CollectionUnstamped { .. }
        | crate::Error::CatalogIntegrityViolation { .. }
        | crate::Error::Promql(_)
        | crate::Error::DependentObjectsExist { .. }
        | crate::Error::RoleInUse { .. }
        | crate::Error::CascadeCycle { .. }
        | crate::Error::CrossShardInExplicitTransaction
        | crate::Error::SequencerUnavailable
        | crate::Error::SessionCapExceeded { .. }
        | crate::Error::SessionIdleTimeout
        | crate::Error::SessionTokenExpired
        | crate::Error::SessionKilledByAdmin
        | crate::Error::SessionUserDropped
        | crate::Error::OidcProviderTenantUnbound
        | crate::Error::OidcProviderTenantUnavailable { .. }
        | crate::Error::ExternalRoleUndefined { .. }
        | crate::Error::OidcNoDefaultDatabase { .. }
        | crate::Error::TenantVectorDimExceeded { .. }
        | crate::Error::TenantGraphDepthExceeded { .. }
        | crate::Error::RoleInheritanceCycle { .. }
        | crate::Error::RoleInheritanceDepthExceeded { .. }
        | crate::Error::OllpExhausted { .. }
        | crate::Error::MirrorReadOnly { .. }
        | crate::Error::StaleReadNotLeader { .. } => None,
    }
}

/// Whether `error` means the write never got a verdict, as opposed to being
/// refused on its merits.
///
/// These are the failures where the cluster never judged the write at all — it
/// timed out, the leader moved, a quorum or the sequencer was absent, memory or
/// rate pressure shed it, the dispatcher refused it at capacity, or a
/// concurrent change aborted it before it applied. Nothing about the write
/// itself is wrong, so the same bytes are expected to land once the condition
/// clears.
fn is_indeterminate(error: &crate::Error) -> bool {
    match error {
        crate::Error::DeadlineExceeded { .. }
        | crate::Error::CrdtAdmissionTimeout { .. }
        | crate::Error::NoLeader { .. }
        | crate::Error::NotLeader { .. }
        | crate::Error::StaleReadNotLeader { .. }
        | crate::Error::SequencerUnavailable
        | crate::Error::Backpressure { .. }
        | crate::Error::DispatchCapacity { .. }
        | crate::Error::ConflictRetry { .. }
        | crate::Error::RetryableRefusal { .. }
        // A concurrent change aborted the write before it applied: the
        // retryable class `40`.
        | crate::Error::RetryableSchemaChanged { .. }
        | crate::Error::CrdtAdmissionRetriesExhausted { .. }
        | crate::Error::CalvinSerializationConflict
        | crate::Error::CalvinParticipantError
        // No leader or no quorum took the write: the retryable leader class.
        | crate::Error::RetryableLeaderChange { .. }
        | crate::Error::GroupQuorumUnavailable { .. }
        | crate::Error::GroupMarksUnavailable { .. }
        | crate::Error::BackupCaptureMoved { .. }
        | crate::Error::MetadataLeaderUnavailable
        | crate::Error::AuthorizationStateBehind { .. }
        | crate::Error::LinearizableReadRefused { .. }
        // Shed by a resource or rate gate: the class `53`.
        | crate::Error::VShardAdmissionCapacityExceeded { .. }
        | crate::Error::MemoryExhausted { .. }
        | crate::Error::RateExceeded { .. } => true,
        crate::Error::DataPlane(code) => is_indeterminate_code(code),
        crate::Error::OllpExhausted { cause, .. } => match cause {
            crate::OllpExhaustedCause::PredicateDrift
            | crate::OllpExhaustedCause::AdmissionRefused { .. } => true,
            crate::OllpExhaustedCause::PreAdmission(inner) => is_indeterminate(inner),
        },
        // The write committed: pushing the same bytes again applies it twice.
        crate::Error::CommittedResultUnavailable { .. }
        | crate::Error::ProposalOutcomeUnknown { .. } => false,
        // Refused on its merits, or a fault the same bytes reproduce.
        crate::Error::RejectedConstraint { .. }
        | crate::Error::TxnOverlayMemoryExceeded { .. }
        | crate::Error::RejectedAuthz { .. }
        | crate::Error::OffsetRegression { .. }
        | crate::Error::RejectedPrevalidation { .. }
        | crate::Error::AppendOnlyViolation { .. }
        | crate::Error::BalanceViolation { .. }
        | crate::Error::MaterializedSumTargetNotFound { .. }
        | crate::Error::MaterializedSumResolutionMissing { .. }
        | crate::Error::PeriodLocked { .. }
        | crate::Error::PeriodLockMisconfigured { .. }
        | crate::Error::RetentionViolation { .. }
        | crate::Error::LegalHoldActive { .. }
        | crate::Error::StateTransitionViolation { .. }
        | crate::Error::TransitionCheckViolation { .. }
        | crate::Error::TypeGuardViolation { .. }
        | crate::Error::TypeMismatch { .. }
        | crate::Error::InsufficientBalance { .. }
        | crate::Error::CollectionNotFound { .. }
        | crate::Error::DocumentNotFound { .. }
        | crate::Error::CollectionDeactivated { .. }
        | crate::Error::CrdtAdmissionInvalidPlan { .. }
        | crate::Error::CrdtAdmissionCallerFence
        | crate::Error::CrdtApplyRequiresAdmission
        | crate::Error::CrdtApplyForbiddenInTransaction
        | crate::Error::NotInTransactionBlock { .. }
        | crate::Error::CrossCollectionNotColocated { .. }
        | crate::Error::CloneWriteRequiresMaterialize { .. }
        | crate::Error::BadRequest { .. }
        | crate::Error::BackupTenantMismatch { .. }
        | crate::Error::BackupKeyMismatch
        | crate::Error::QuotaOvercommit { .. }
        | crate::Error::PlanError { .. }
        | crate::Error::FeatureNotSupported { .. }
        | crate::Error::UndefinedFunction { .. }
        | crate::Error::UndefinedObject { .. }
        | crate::Error::ObjectNotInPrerequisiteState { .. }
        | crate::Error::UndefinedColumn { .. }
        | crate::Error::TextColumn { .. }
        | crate::Error::AmbiguousColumn { .. }
        | crate::Error::UnknownStrictField { .. }
        | crate::Error::DivisionByZero
        | crate::Error::DataException { .. }
        | crate::Error::NumericValueOutOfRange { .. }
        | crate::Error::InvalidLimitValue { .. }
        | crate::Error::ExecutionLimitExceeded { .. }
        | crate::Error::LimitExceeded { .. }
        | crate::Error::Wal(_)
        | crate::Error::Dispatch { .. }
        | crate::Error::Storage { .. }
        | crate::Error::ColdStorage { .. }
        | crate::Error::Serialization { .. }
        | crate::Error::Codec { .. }
        | crate::Error::SegmentCorrupted { .. }
        | crate::Error::Crdt(_)
        | crate::Error::Io(_)
        | crate::Error::Config { .. }
        | crate::Error::Encryption { .. }
        | crate::Error::Bridge { .. }
        | crate::Error::VersionCompat { .. }
        | crate::Error::RestoreTargetNotEmpty { .. }
        | crate::Error::RestoreVerificationFailed { .. }
        | crate::Error::Internal { .. }
        | crate::Error::Shaping(_)
        | crate::Error::RemoteTyped { .. }
        | crate::Error::Ddl(_)
        | crate::Error::DescriptorVersionAnomaly { .. }
        | crate::Error::CollectionPurgeRowMissing { .. }
        | crate::Error::CollectionUnstamped { .. }
        | crate::Error::CatalogIntegrityViolation { .. }
        | crate::Error::Promql(_)
        | crate::Error::DependentObjectsExist { .. }
        | crate::Error::RoleInUse { .. }
        | crate::Error::CascadeCycle { .. }
        | crate::Error::CrossShardInExplicitTransaction
        | crate::Error::SessionCapExceeded { .. }
        | crate::Error::SessionIdleTimeout
        | crate::Error::SessionTokenExpired
        | crate::Error::SessionKilledByAdmin
        | crate::Error::SessionUserDropped
        | crate::Error::OidcProviderTenantUnbound
        | crate::Error::OidcProviderTenantUnavailable { .. }
        | crate::Error::ExternalRoleUndefined { .. }
        | crate::Error::OidcNoDefaultDatabase { .. }
        | crate::Error::TenantVectorDimExceeded { .. }
        | crate::Error::TenantGraphDepthExceeded { .. }
        | crate::Error::RoleInheritanceCycle { .. }
        | crate::Error::RoleInheritanceDepthExceeded { .. }
        | crate::Error::MirrorReadOnly { .. } => false,
    }
}

/// [`is_indeterminate`] for a Data-Plane verdict.
fn is_indeterminate_code(code: &ErrorCode) -> bool {
    match code {
        ErrorCode::DeadlineExceeded
        | ErrorCode::ExpiredBeforeExecution
        | ErrorCode::ResourcesExhausted
        | ErrorCode::DispatchCapacity { .. }
        | ErrorCode::ConflictRetry
        | ErrorCode::RetryableRefusal { .. }
        | ErrorCode::OllpRetryRequired
        | ErrorCode::CrdtFrontierMismatch { .. }
        | ErrorCode::CollectionDraining { .. }
        | ErrorCode::RateExceeded { .. }
        | ErrorCode::TransactionRollback { .. } => true,
        // A sync hold is decided by the session that owns the stream before
        // it reaches this classifier. Every other code is a verdict.
        ErrorCode::RejectedConstraint { .. }
        | ErrorCode::RejectedPrevalidation { .. }
        | ErrorCode::SyncRejected { .. }
        | ErrorCode::SyncNotApplied { .. }
        | ErrorCode::NotFound
        | ErrorCode::RejectedAuthz { .. }
        | ErrorCode::RejectedDanglingEdge { .. }
        | ErrorCode::DuplicateWrite
        | ErrorCode::AppendOnlyViolation { .. }
        | ErrorCode::BalanceViolation { .. }
        | ErrorCode::PeriodLocked { .. }
        | ErrorCode::PeriodLockMisconfigured { .. }
        | ErrorCode::RetentionViolation { .. }
        | ErrorCode::LegalHoldActive { .. }
        | ErrorCode::StateTransitionViolation { .. }
        | ErrorCode::TransitionCheckViolation { .. }
        | ErrorCode::TypeGuardViolation { .. }
        | ErrorCode::TypeMismatch { .. }
        | ErrorCode::CounterFault { .. }
        | ErrorCode::InsufficientBalance { .. }
        | ErrorCode::RecursionDepthExceeded { .. }
        | ErrorCode::UndefinedColumn { .. }
        | ErrorCode::TextColumn { .. }
        | ErrorCode::Internal { .. }
        | ErrorCode::Unsupported { .. }
        | ErrorCode::RollbackFailed { .. }
        | ErrorCode::TxnOverlayMemoryExceeded { .. }
        | ErrorCode::DivisionByZero
        | ErrorCode::UndefinedFunction { .. }
        | ErrorCode::DataException { .. }
        | ErrorCode::NumericValueOutOfRange { .. }
        | ErrorCode::BadRequest { .. }
        | ErrorCode::ActiveSqlTransaction { .. }
        | ErrorCode::DependentObjectsExist { .. } => false,
    }
}

/// The [`AckStatus`] an engine ack must carry when its dispatch failed.
///
/// A dispatch can fail because the write was refused on its merits (terminal —
/// the sender must compensate) or because it never got a verdict at all: a
/// timeout, a moved leader, shed load. The second kind is retryable, and
/// reporting it as [`AckStatus::Rejected`] tells the sender to permanently drop
/// a write the cluster never actually refused — a silent loss on every
/// transient failure, which are precisely the failures that do occur in normal
/// operation.
///
/// `next_seq` is the sequence the sender resumes from — its own seq for
/// this batch, since nothing applied.
pub(super) fn ack_status_for_dispatch_error(error: &crate::Error, next_seq: u64) -> AckStatus {
    if retryable_refusal_reason(error).is_some() || is_indeterminate(error) {
        return AckStatus::Gap { expected: next_seq };
    }
    AckStatus::Rejected {
        reason: error.to_string(),
    }
}

/// The `reject_reason` field that belongs beside `status` on an engine ack.
///
/// Derived from the status rather than passed alongside it, so the two cannot
/// disagree: only a terminal refusal carries a reason, because only a terminal
/// refusal asks the sender to compensate. A reason attached to a retryable
/// status reads as "give up" to any receiver that checks the field first.
pub(super) fn reject_reason_for(status: &AckStatus) -> Option<String> {
    match status {
        AckStatus::Rejected { reason } => Some(reason.clone()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::envelope::ErrorCode;

    #[test]
    fn a_retryable_refusal_becomes_a_gap_at_the_senders_own_seq() {
        // Nothing applied, so the sender resumes at the seq it sent —
        // not one past it, which will skip the batch entirely.
        let error = crate::Error::DataPlane(ErrorCode::RetryableRefusal {
            reason: "shard is rebalancing".into(),
        });
        assert_eq!(
            ack_status_for_dispatch_error(&error, 9),
            AckStatus::Gap { expected: 9 }
        );
    }

    #[test]
    fn a_refusal_typed_at_the_control_plane_is_retryable_too() {
        // The same refusal reaches this code already typed on some paths and
        // wrapped in DataPlane on others; both must classify identically.
        let error = crate::Error::RetryableRefusal {
            reason: "shard is rebalancing".into(),
        };
        assert_eq!(
            ack_status_for_dispatch_error(&error, 3),
            AckStatus::Gap { expected: 3 }
        );
    }

    #[test]
    fn a_genuine_refusal_stays_terminal_and_carries_its_reason() {
        let error = crate::Error::DataPlane(ErrorCode::RejectedAuthz {
            resource: "RLS write policy on 'orders' rejected the row".into(),
        });
        match ack_status_for_dispatch_error(&error, 4) {
            AckStatus::Rejected { reason } => assert!(!reason.is_empty()),
            other => panic!("expected a terminal rejection, got {other:?}"),
        }
    }

    #[test]
    fn a_timeout_is_retryable_because_it_refused_nothing() {
        // The reachable case: a dispatch that timed out never judged the write.
        // Reporting it terminal makes the sender drop a batch on every blip.
        let error = crate::Error::DeadlineExceeded {
            request_id: crate::types::RequestId::new(1),
        };
        assert_eq!(
            ack_status_for_dispatch_error(&error, 6),
            AckStatus::Gap { expected: 6 }
        );
    }

    #[test]
    fn a_moved_leader_is_retryable() {
        let error = crate::Error::NotLeader {
            vshard_id: crate::types::VShardId::new(0),
            leader_node: 2,
            leader_addr: "10.0.0.2:9000".into(),
            leader_term: 3,
        };
        assert_eq!(
            ack_status_for_dispatch_error(&error, 2),
            AckStatus::Gap { expected: 2 }
        );
    }

    #[test]
    fn shed_load_is_retryable_not_a_refusal_of_the_write() {
        let error = crate::Error::Backpressure {
            engine: nodedb_mem::EngineId::Timeseries,
        };
        assert_eq!(
            ack_status_for_dispatch_error(&error, 4),
            AckStatus::Gap { expected: 4 }
        );
    }

    #[test]
    fn a_dispatch_refused_at_capacity_is_retryable_not_a_refusal_of_the_write() {
        let error = crate::Error::DispatchCapacity {
            scope: crate::DispatchCapacityScope::TenantInflight {
                tenant_id: crate::types::TenantId::new(1),
                inflight: 64,
                cap: 64,
            },
        };
        assert_eq!(
            ack_status_for_dispatch_error(&error, 4),
            AckStatus::Gap { expected: 4 }
        );
    }

    #[test]
    fn a_retryable_status_carries_no_reject_reason() {
        // A reason beside a retryable status reads as "give up" to a receiver
        // that checks the field before the status.
        assert_eq!(reject_reason_for(&AckStatus::Gap { expected: 2 }), None);
        assert_eq!(reject_reason_for(&AckStatus::Applied), None);
        assert_eq!(
            reject_reason_for(&AckStatus::Rejected {
                reason: "schema mismatch".into()
            }),
            Some("schema mismatch".to_string())
        );
    }

    #[test]
    fn an_unclassified_error_is_never_reported_as_applied() {
        // The failure mode this replaces: a dispatch error acked as `Applied`,
        // which retires a write that never landed.
        let error = crate::Error::Internal {
            detail: "bridge closed".into(),
        };
        assert_ne!(
            ack_status_for_dispatch_error(&error, 1),
            AckStatus::Applied,
            "a failed dispatch must never be reported as applied"
        );
    }

    /// A write aborted by a concurrent change, or not taken for want of a
    /// quorum or under a rate gate, never got a verdict, so it is retried.
    #[test]
    fn retry_class_failures_are_retryable() {
        let errors = [
            crate::Error::CalvinSerializationConflict,
            crate::Error::GroupQuorumUnavailable {
                group_id: 1,
                voters: vec![1, 2, 3],
                unreachable: vec![2, 3],
            },
            crate::Error::RateExceeded {
                gate: "sync".into(),
                detail: "over budget".into(),
                retry_after_ms: 10,
            },
            crate::Error::DataPlane(ErrorCode::OllpRetryRequired),
            crate::Error::OllpExhausted {
                retries: 3,
                cause: crate::OllpExhaustedCause::PredicateDrift,
            },
        ];
        for error in errors {
            assert_eq!(
                ack_status_for_dispatch_error(&error, 5),
                AckStatus::Gap { expected: 5 },
                "{error:?}"
            );
        }
    }

    /// Retry exhaustion before admission takes the verdict of its cause.
    #[test]
    fn pre_admission_exhaustion_follows_its_cause() {
        let error = crate::Error::OllpExhausted {
            retries: 3,
            cause: crate::OllpExhaustedCause::PreAdmission(Box::new(crate::Error::BadRequest {
                detail: "bad key".into(),
            })),
        };
        assert!(matches!(
            ack_status_for_dispatch_error(&error, 5),
            AckStatus::Rejected { .. }
        ));
    }
}
