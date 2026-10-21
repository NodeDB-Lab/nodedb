// SPDX-License-Identifier: BUSL-1.1

//! The one mapping every cross-node executor uses to answer a remote caller
//! with a local execution error.
//!
//! The match is exhaustive with no catch-all, so a new `Error` variant fails
//! to compile here until it picks how it crosses the node hop.

use nodedb_cluster::rpc_codec::{DataPlaneErrorCode, TypedClusterError};

/// Map a local-execution [`crate::Error`] to the wire error a remote caller
/// receives.
///
/// A Data-Plane verdict crosses verbatim as `TypedClusterError::DataPlane`, so
/// the coordinator rebuilds `Error::DataPlane(code)` and renders the SQLSTATE
/// single-node execution renders. Every other error keeps its own numeric
/// classification from `NodeDbError::from(err).code()` — never a hardcoded
/// plan-decode code, which will misname what failed.
pub(crate) fn execution_error_to_typed(err: crate::Error) -> TypedClusterError {
    match err {
        crate::Error::DataPlane(code) => TypedClusterError::DataPlane { code: code.into() },
        // A CRDT error crosses as its Data-Plane verdict, so the coordinator
        // answers the SQLSTATE a local execution answers.
        crate::Error::Crdt(crdt) => TypedClusterError::DataPlane {
            code: crate::bridge::envelope::ErrorCode::from(&crdt).into(),
        },
        // A statement that ran out of time keeps the wire's own deadline
        // variant, which the coordinator rebuilds as `Error::DeadlineExceeded`.
        // Folding it into `Internal` will report a client's own timeout as an
        // internal failure once it crossed a node boundary.
        crate::Error::DeadlineExceeded { .. } => {
            TypedClusterError::DeadlineExceeded { elapsed_ms: 0 }
        }
        // A redirect crosses as the wire's own redirect, with the leader and
        // the term this node knows it at, so the coordinator moves its
        // routing hint and retries against that leader.
        not_leader @ crate::Error::NotLeader { .. } => TypedClusterError::from(not_leader),
        // A Calvin abort keeps its verdict and a schema change stays
        // retryable, so a routed submit answers as a local one does.
        typed @ (crate::Error::CalvinSerializationConflict
        | crate::Error::CalvinParticipantError
        | crate::Error::RetryableSchemaChanged { .. }) => TypedClusterError::from(typed),
        // A Control-Plane constraint refusal crosses verbatim, same as a
        // Data-Plane verdict, so the coordinator answers 23502 vs 23505
        // instead of flattening both into one numeric class.
        crate::Error::RejectedConstraint {
            collection,
            constraint,
            detail,
        } => TypedClusterError::RejectedConstraint {
            collection,
            constraint,
            detail,
        },
        // A capacity refusal crosses as its own verdict, so the coordinator
        // answers the retryable overload class.
        capacity @ crate::Error::DispatchCapacity { .. } => TypedClusterError::DataPlane {
            code: DataPlaneErrorCode::DispatchCapacity {
                reason: capacity.to_string(),
            },
        },
        // A value refusal crosses as the Data-Plane verdict of the same name,
        // so the coordinator answers its exact SQLSTATE.
        crate::Error::InvalidTextRepresentation { detail } => TypedClusterError::DataPlane {
            code: DataPlaneErrorCode::InvalidTextRepresentation { detail },
        },
        crate::Error::DatatypeMismatch { detail } => TypedClusterError::DataPlane {
            code: DataPlaneErrorCode::DatatypeMismatch { detail },
        },
        crate::Error::InvalidDatetimeFormat { detail } => TypedClusterError::DataPlane {
            code: DataPlaneErrorCode::InvalidDatetimeFormat { detail },
        },
        crate::Error::DatetimeFieldOverflow { detail } => TypedClusterError::DataPlane {
            code: DataPlaneErrorCode::DatetimeFieldOverflow { detail },
        },
        // Every other error crosses as its public numeric code and message.
        // The coordinator renders the SQLSTATE that code maps to.
        other @ (crate::Error::TxnOverlayMemoryExceeded { .. }
        | crate::Error::RejectedAuthz { .. }
        | crate::Error::OffsetRegression { .. }
        | crate::Error::ConflictRetry { .. }
        | crate::Error::RejectedPrevalidation { .. }
        | crate::Error::RetryableRefusal { .. }
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
        | crate::Error::WrongObjectType { .. }
        | crate::Error::UndefinedColumn { .. }
        | crate::Error::TextColumn { .. }
        | crate::Error::AmbiguousColumn { .. }
        | crate::Error::UngroupedColumn { .. }
        | crate::Error::UnknownStrictField { .. }
        | crate::Error::DivisionByZero
        | crate::Error::DataException { .. }
        | crate::Error::MixedSrid { .. }
        | crate::Error::UnknownSrid { .. }
        | crate::Error::NumericValueOutOfRange { .. }
        | crate::Error::InvalidLimitValue { .. }
        | crate::Error::RetryableLeaderChange { .. }
        | crate::Error::CommittedResultUnavailable { .. }
        | crate::Error::ProposalOutcomeUnknown { .. }
        | crate::Error::GroupQuorumUnavailable { .. }
        | crate::Error::GroupMarksUnavailable { .. }
        | crate::Error::BackupCaptureMoved { .. }
        | crate::Error::MetadataLeaderUnavailable
        | crate::Error::MetadataGroupGone { .. }
        | crate::Error::MetadataDdlSuperseded { .. }
        | crate::Error::AuthorizationStateBehind { .. }
        | crate::Error::LinearizableReadRefused { .. }
        | crate::Error::ExecutionLimitExceeded { .. }
        | crate::Error::LimitExceeded { .. }
        | crate::Error::Wal(_)
        | crate::Error::Dispatch { .. }
        | crate::Error::Storage { .. }
        | crate::Error::ColdStorage { .. }
        | crate::Error::Serialization { .. }
        | crate::Error::Codec { .. }
        | crate::Error::SegmentCorrupted { .. }
        | crate::Error::MemoryExhausted { .. }
        | crate::Error::Backpressure { .. }
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
        | crate::Error::StaleReadNotLeader { .. }) => numeric_typed(other),
    }
}

/// The wire error for a local error with no typed wire carrier: its public
/// numeric code from `NodeDbError::from(err).code()`, and its message. The
/// coordinator rebuilds it as `Error::RemoteTyped`.
pub(crate) fn numeric_typed(err: crate::Error) -> TypedClusterError {
    let message = err.to_string();
    let code = u32::from(nodedb_types::error::NodeDbError::from(err).code().0);
    TypedClusterError::Internal { code, message }
}
