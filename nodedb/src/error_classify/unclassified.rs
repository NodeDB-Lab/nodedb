// SPDX-License-Identifier: BUSL-1.1

//! Which internal errors carry no classification a caller acts on.

use crate::error::Error;

/// True when the error carries neither a client-matchable classification from
/// [`super::classify`] nor a retry contract a caller matches by variant. Only these
/// may be re-wrapped in a transport error.
pub(crate) fn is_unclassified_failure(e: &Error) -> bool {
    match e {
        Error::Wal(_)
        | Error::Dispatch { .. }
        | Error::Storage { .. }
        | Error::ColdStorage { .. }
        | Error::Serialization { .. }
        | Error::Codec { .. }
        | Error::SegmentCorrupted { .. }
        | Error::Crdt(_)
        | Error::Io(_)
        | Error::Config { .. }
        | Error::Encryption { .. }
        | Error::Bridge { .. }
        | Error::VersionCompat { .. }
        | Error::RestoreTargetNotEmpty { .. }
        | Error::RestoreVerificationFailed { .. }
        | Error::Internal { .. }
        | Error::DescriptorVersionAnomaly { .. }
        | Error::CatalogIntegrityViolation { .. }
        | Error::CollectionPurgeRowMissing { .. }
        | Error::CollectionUnstamped { .. }
        | Error::MaterializedSumResolutionMissing { .. }
        | Error::CascadeCycle { .. } => true,
        // A client-matchable class or a retry contract a caller matches by
        // variant.
        Error::RejectedConstraint { .. }
        | Error::TxnOverlayMemoryExceeded { .. }
        | Error::RejectedAuthz { .. }
        | Error::OffsetRegression { .. }
        | Error::DeadlineExceeded { .. }
        | Error::ConflictRetry { .. }
        | Error::CalvinSerializationConflict
        | Error::CalvinParticipantError
        | Error::RejectedPrevalidation { .. }
        | Error::RetryableRefusal { .. }
        | Error::AppendOnlyViolation { .. }
        | Error::BalanceViolation { .. }
        | Error::MaterializedSumTargetNotFound { .. }
        | Error::PeriodLocked { .. }
        | Error::PeriodLockMisconfigured { .. }
        | Error::RetentionViolation { .. }
        | Error::LegalHoldActive { .. }
        | Error::StateTransitionViolation { .. }
        | Error::TransitionCheckViolation { .. }
        | Error::TypeGuardViolation { .. }
        | Error::TypeMismatch { .. }
        | Error::InsufficientBalance { .. }
        | Error::RateExceeded { .. }
        | Error::CollectionNotFound { .. }
        | Error::DocumentNotFound { .. }
        | Error::CollectionDeactivated { .. }
        | Error::VShardAdmissionCapacityExceeded { .. }
        | Error::CrdtAdmissionRetriesExhausted { .. }
        | Error::CrdtAdmissionInvalidPlan { .. }
        | Error::CrdtAdmissionCallerFence
        | Error::CrdtApplyRequiresAdmission
        | Error::CrdtApplyForbiddenInTransaction
        | Error::NotInTransactionBlock { .. }
        | Error::CrdtAdmissionTimeout { .. }
        | Error::NoLeader { .. }
        | Error::NotLeader { .. }
        | Error::CrossCollectionNotColocated { .. }
        | Error::CloneWriteRequiresMaterialize { .. }
        | Error::BadRequest { .. }
        | Error::BackupTenantMismatch { .. }
        | Error::BackupKeyMismatch
        | Error::QuotaOvercommit { .. }
        | Error::PlanError { .. }
        | Error::FeatureNotSupported { .. }
        | Error::UndefinedFunction { .. }
        | Error::UndefinedObject { .. }
        | Error::ObjectNotInPrerequisiteState { .. }
        | Error::UndefinedColumn { .. }
        | Error::TextColumn { .. }
        | Error::AmbiguousColumn { .. }
        | Error::UnknownStrictField { .. }
        | Error::DivisionByZero
        | Error::DataException { .. }
        | Error::InvalidLimitValue { .. }
        | Error::RetryableSchemaChanged { .. }
        | Error::RetryableLeaderChange { .. }
        | Error::CommittedResultUnavailable { .. }
        | Error::ProposalOutcomeUnknown { .. }
        | Error::GroupQuorumUnavailable { .. }
        | Error::GroupMarksUnavailable { .. }
        | Error::BackupCaptureMoved { .. }
        | Error::MetadataLeaderUnavailable
        | Error::AuthorizationStateBehind { .. }
        | Error::LinearizableReadRefused { .. }
        | Error::ExecutionLimitExceeded { .. }
        | Error::LimitExceeded { .. }
        | Error::DispatchCapacity { .. }
        | Error::MemoryExhausted { .. }
        | Error::Backpressure { .. }
        | Error::Shaping(_)
        | Error::Ddl(_)
        | Error::RemoteTyped { .. }
        | Error::DataPlane(_)
        | Error::Promql(_)
        | Error::DependentObjectsExist { .. }
        | Error::RoleInUse { .. }
        | Error::CrossShardInExplicitTransaction
        | Error::SequencerUnavailable
        | Error::SessionCapExceeded { .. }
        | Error::SessionIdleTimeout
        | Error::SessionTokenExpired
        | Error::SessionKilledByAdmin
        | Error::SessionUserDropped
        | Error::OidcProviderTenantUnbound
        | Error::OidcProviderTenantUnavailable { .. }
        | Error::ExternalRoleUndefined { .. }
        | Error::OidcNoDefaultDatabase { .. }
        | Error::TenantVectorDimExceeded { .. }
        | Error::TenantGraphDepthExceeded { .. }
        | Error::RoleInheritanceCycle { .. }
        | Error::RoleInheritanceDepthExceeded { .. }
        | Error::OllpExhausted { .. }
        | Error::MirrorReadOnly { .. }
        | Error::StaleReadNotLeader { .. } => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::TenantId;

    /// Verdicts the state machine reached must never be re-wrapped as
    /// transport failures; machinery failures may be.
    #[test]
    fn only_machinery_failures_are_unclassified() {
        assert!(!is_unclassified_failure(&Error::RejectedConstraint {
            collection: "docs".to_owned(),
            constraint: "unique".to_owned(),
            detail: "duplicate key value 'dup'".to_owned(),
        }));
        assert!(!is_unclassified_failure(&Error::RejectedAuthz {
            tenant_id: TenantId::new(1),
            resource: "docs".to_owned(),
        }));
        assert!(is_unclassified_failure(&Error::Internal {
            detail: "apply error".to_owned(),
        }));
    }
}
