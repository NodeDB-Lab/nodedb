// SPDX-License-Identifier: BUSL-1.1

//! Conversions into the Data-Plane [`ErrorCode`].

use super::error_code::ErrorCode;

/// An expression evaluation failure, as the Data Plane reports it.
///
/// Exhaustive, so a new evaluator error picks its own code rather than
/// defaulting to one.
impl From<nodedb_query::EvalError> for ErrorCode {
    fn from(e: nodedb_query::EvalError) -> Self {
        match e {
            nodedb_query::EvalError::DivisionByZero => Self::DivisionByZero,
            nodedb_query::EvalError::UnknownFunction { name } => Self::UndefinedFunction { name },
            e @ (nodedb_query::EvalError::VectorDimensionMismatch { .. }
            | nodedb_query::EvalError::ArgumentType { .. }
            | nodedb_query::EvalError::InvalidJsonPath { .. }
            | nodedb_query::EvalError::Crs { .. }
            | nodedb_query::EvalError::MixedSrid { .. }
            | nodedb_query::EvalError::Transform { .. }
            | nodedb_query::EvalError::Geometry { .. }
            | nodedb_query::EvalError::ArgumentValue { .. }) => Self::DataException {
                detail: e.to_string(),
            },
            e @ nodedb_query::EvalError::NumericOverflow { .. } => Self::NumericValueOutOfRange {
                detail: e.to_string(),
            },
        }
    }
}

/// A KV write that binds a row to `Surrogate::ZERO` fails prevalidation, the
/// same refusal the dispatch-time check returns.
impl From<crate::engine::kv::UnboundKvWrite> for ErrorCode {
    fn from(e: crate::engine::kv::UnboundKvWrite) -> Self {
        Self::RejectedPrevalidation {
            reason: e.to_string(),
        }
    }
}

impl From<crate::Error> for ErrorCode {
    fn from(e: crate::Error) -> Self {
        match e {
            crate::Error::DeadlineExceeded { .. } => Self::DeadlineExceeded,
            crate::Error::RejectedConstraint {
                constraint, detail, ..
            } => Self::RejectedConstraint { constraint, detail },
            crate::Error::RejectedPrevalidation { reason, .. } => {
                Self::RejectedPrevalidation { reason }
            }
            crate::Error::RetryableRefusal { reason } => Self::RetryableRefusal { reason },
            crate::Error::CollectionNotFound { .. }
            | crate::Error::CollectionDeactivated { .. }
            | crate::Error::DocumentNotFound { .. } => Self::NotFound,
            crate::Error::RejectedAuthz { resource, .. } => Self::RejectedAuthz { resource },
            // The Control Plane gives both `40001` (serialization_failure).
            crate::Error::ConflictRetry { .. } | crate::Error::CalvinSerializationConflict => {
                Self::ConflictRetry
            }
            crate::Error::MemoryExhausted { .. } => Self::ResourcesExhausted,
            crate::Error::Backpressure { .. } => Self::ResourcesExhausted,
            crate::Error::AppendOnlyViolation { collection, .. } => {
                Self::AppendOnlyViolation { collection }
            }
            crate::Error::BalanceViolation {
                collection, detail, ..
            } => Self::BalanceViolation { collection, detail },
            // A materialized-sum target that cannot be addressed breaks the
            // balance invariant the target collection maintains, so it crosses
            // the bridge as the same class of violation the Control Plane
            // already renders it as — not as a generic `Internal`, which would
            // reach the client as SQLSTATE `XX000` and lose the target
            // collection, join column, and join value the message names.
            crate::Error::MaterializedSumTargetNotFound {
                target_collection,
                join_column,
                join_value,
            } => Self::BalanceViolation {
                collection: target_collection,
                detail: format!(
                    "no row with primary key '{join_value}', referenced by join column \
                     '{join_column}'"
                ),
            },
            crate::Error::PeriodLocked { collection, .. } => Self::PeriodLocked { collection },
            crate::Error::PeriodLockMisconfigured {
                collection,
                ref_table,
                status_column,
                row_identity,
            } => Self::PeriodLockMisconfigured {
                collection,
                ref_table,
                status_column,
                row_identity,
            },
            crate::Error::RetentionViolation { collection, .. } => {
                Self::RetentionViolation { collection }
            }
            crate::Error::LegalHoldActive { collection, .. } => {
                Self::LegalHoldActive { collection }
            }
            crate::Error::StateTransitionViolation {
                collection, detail, ..
            } => Self::StateTransitionViolation { collection, detail },
            crate::Error::TransitionCheckViolation { collection, detail } => {
                Self::TransitionCheckViolation { collection, detail }
            }
            crate::Error::TypeGuardViolation {
                collection, detail, ..
            } => Self::TypeGuardViolation { collection, detail },
            crate::Error::TypeMismatch {
                collection, detail, ..
            } => Self::TypeMismatch { collection, detail },
            crate::Error::InsufficientBalance {
                collection, detail, ..
            } => Self::InsufficientBalance { collection, detail },
            crate::Error::RateExceeded {
                gate,
                retry_after_ms,
                ..
            } => Self::RateExceeded {
                gate,
                retry_after_ms,
            },
            // A capacity refusal enqueued nothing, and the same request
            // succeeds once capacity frees.
            capacity @ crate::Error::DispatchCapacity { .. } => Self::DispatchCapacity {
                reason: capacity.to_string(),
            },
            crate::Error::TxnOverlayMemoryExceeded { limit } => {
                Self::TxnOverlayMemoryExceeded { limit }
            }
            crate::Error::DivisionByZero => Self::DivisionByZero,
            crate::Error::UndefinedFunction { name } => Self::UndefinedFunction { name },
            crate::Error::DataException { detail } => Self::DataException { detail },
            // `42601` (syntax_error), as the Control Plane gives both.
            crate::Error::BadRequest { detail } | crate::Error::PlanError { detail } => {
                Self::BadRequest { detail }
            }
            // `0A000` (feature_not_supported), as the Control Plane gives both.
            crate::Error::FeatureNotSupported { detail } => Self::Unsupported { detail },
            unsupported @ crate::Error::CrossCollectionNotColocated { .. } => Self::Unsupported {
                detail: unsupported.to_string(),
            },
            crate::Error::UndefinedColumn { column } => Self::UndefinedColumn { column },
            crate::Error::TextColumn {
                collection,
                column,
                fault,
            } => Self::TextColumn {
                collection,
                column,
                fault,
            },
            // Same condition an undefined column reports at plan time, raised
            // here by the strict encoder for a transport the planner never
            // sees (native client, `COPY FROM`, CRDT delta merge).
            crate::Error::UnknownStrictField { column, .. } => Self::UndefinedColumn { column },
            // Already a Data-Plane verdict: hand back the same code rather
            // than re-wrapping it as `Internal` and losing its SQLSTATE.
            crate::Error::DataPlane(code) => code,
            // Class `22`, the class the Control Plane gives both.
            e @ (crate::Error::OffsetRegression { .. }
            | crate::Error::BackupTenantMismatch { .. }
            | crate::Error::InvalidLimitValue { .. }
            | crate::Error::MixedSrid { .. }
            | crate::Error::UnknownSrid { .. }) => Self::DataException {
                detail: e.to_string(),
            },
            // `22003`, as the Control Plane gives it.
            crate::Error::NumericValueOutOfRange { detail } => {
                Self::NumericValueOutOfRange { detail }
            }
            // `22P02`, `42804`, `22007` and `22008`, as the Control Plane
            // gives them.
            crate::Error::InvalidTextRepresentation { detail } => {
                Self::InvalidTextRepresentation { detail }
            }
            crate::Error::DatatypeMismatch { detail } => Self::DatatypeMismatch { detail },
            crate::Error::InvalidDatetimeFormat { detail } => {
                Self::InvalidDatetimeFormat { detail }
            }
            crate::Error::DatetimeFieldOverflow { detail } => {
                Self::DatetimeFieldOverflow { detail }
            }
            // `40000`, as the Control Plane gives it.
            e @ crate::Error::CalvinParticipantError => Self::TransactionRollback {
                detail: e.to_string(),
            },
            // `40001`: the client retries the statement.
            e @ crate::Error::RetryableSchemaChanged { .. } => Self::RetryableRefusal {
                reason: e.to_string(),
            },
            // `25001`, as the Control Plane gives all three.
            e @ (crate::Error::CrdtApplyForbiddenInTransaction
            | crate::Error::NotInTransactionBlock { .. }
            | crate::Error::CrossShardInExplicitTransaction) => Self::ActiveSqlTransaction {
                detail: e.to_string(),
            },
            // `2BP01`, as the Control Plane gives both. The detail is the
            // public message the Control Plane renders.
            crate::Error::DependentObjectsExist {
                root_kind,
                root_name,
                dependent_count,
                dependents,
                ..
            } => {
                let (object, detail) = crate::error_classify::dependent_objects_text(
                    root_kind,
                    &root_name,
                    dependent_count,
                    &dependents,
                );
                Self::DependentObjectsExist { object, detail }
            }
            crate::Error::RoleInUse { role, dependents } => {
                let object = format!("role \"{role}\"");
                let detail = crate::Error::RoleInUse { role, dependents }.to_string();
                Self::DependentObjectsExist { object, detail }
            }
            crate::Error::CrdtAdmissionRetriesExhausted { .. } => Self::ConflictRetry,
            // Retryable refusals whose class (`55P03`) no Data-Plane code has.
            // The retry contract survives: nothing was applied.
            e @ (crate::Error::NoLeader { .. }
            | crate::Error::GroupQuorumUnavailable { .. }
            | crate::Error::GroupMarksUnavailable { .. }
            | crate::Error::BackupCaptureMoved { .. }
            | crate::Error::AuthorizationStateBehind { .. }
            | crate::Error::LinearizableReadRefused { .. }
            | crate::Error::StaleReadNotLeader { .. }) => Self::RetryableRefusal {
                reason: e.to_string(),
            },
            // Class `57`: the client retries once the leader settles.
            e @ crate::Error::NotLeader { .. } => Self::DispatchCapacity {
                reason: e.to_string(),
            },
            crate::Error::CrdtAdmissionTimeout { .. } => Self::DeadlineExceeded,
            e @ crate::Error::VShardAdmissionCapacityExceeded { .. } => Self::RateExceeded {
                gate: e.to_string(),
                retry_after_ms: 0,
            },
            // Class `53`: a configured resource ceiling.
            crate::Error::QuotaOvercommit { .. }
            | crate::Error::TenantVectorDimExceeded { .. }
            | crate::Error::TenantGraphDepthExceeded { .. } => Self::ResourcesExhausted,
            // Class `28` has no Data-Plane code. The nearest is the access
            // refusal, which keeps it a client error the client cannot retry.
            e @ (crate::Error::BackupKeyMismatch | crate::Error::SessionTokenExpired) => {
                Self::RejectedAuthz {
                    resource: e.to_string(),
                }
            }
            // `42704` and `55000`, as the Control Plane gives them.
            crate::Error::UndefinedObject { kind, name } => Self::UndefinedObject {
                object: format!("{kind} \"{name}\""),
            },
            crate::Error::ObjectNotInPrerequisiteState { object, detail } => {
                Self::ObjectNotInPrerequisiteState { object, detail }
            }
            // Each CRDT error keeps the class of what caused it.
            crate::Error::Crdt(crdt) => Self::from(&crdt),
            // Client errors of class `42`, and client errors whose dedicated
            // SQLSTATE (`25006`, a `55` code of their own) no Data-Plane code
            // has. `BadRequest` is the class their public code has.
            e @ (crate::Error::CrdtAdmissionInvalidPlan { .. }
            | crate::Error::CrdtAdmissionCallerFence
            | crate::Error::CrdtApplyRequiresAdmission
            | crate::Error::CloneWriteRequiresMaterialize { .. }
            | crate::Error::MirrorReadOnly { .. }
            | crate::Error::WrongObjectType { .. }
            | crate::Error::AmbiguousColumn { .. }
            | crate::Error::UngroupedColumn { .. }
            | crate::Error::ExecutionLimitExceeded { .. }
            | crate::Error::LimitExceeded { .. }
            | crate::Error::Promql(_)
            | crate::Error::SequencerUnavailable
            | crate::Error::SessionCapExceeded { .. }
            | crate::Error::SessionIdleTimeout
            | crate::Error::SessionKilledByAdmin
            | crate::Error::SessionUserDropped
            | crate::Error::OidcProviderTenantUnbound
            | crate::Error::OidcProviderTenantUnavailable { .. }
            | crate::Error::ExternalRoleUndefined { .. }
            | crate::Error::OidcNoDefaultDatabase { .. }
            | crate::Error::RoleInheritanceCycle { .. }
            | crate::Error::RoleInheritanceDepthExceeded { .. }) => Self::BadRequest {
                detail: e.to_string(),
            },
            // Retry exhaustion takes the code of its cause.
            crate::Error::OllpExhausted { cause, .. } => match cause {
                crate::OllpExhaustedCause::PredicateDrift => Self::ConflictRetry,
                crate::OllpExhaustedCause::PreAdmission(inner) => Self::from(*inner),
                crate::OllpExhaustedCause::AdmissionRefused { detail } => Self::RateExceeded {
                    gate: detail,
                    retry_after_ms: 0,
                },
            },
            // Server-side faults and system defects. `Shaping`,
            // `RemoteTyped` and `Ddl` carry a public numeric code that has no
            // Data-Plane twin, and none is raised on the Data Plane.
            e @ (crate::Error::MaterializedSumResolutionMissing { .. }
            | crate::Error::RetryableLeaderChange { .. }
            | crate::Error::CommittedResultUnavailable { .. }
            | crate::Error::ProposalOutcomeUnknown { .. }
            | crate::Error::MetadataLeaderUnavailable
            | crate::Error::MetadataGroupGone { .. }
            | crate::Error::MetadataDdlSuperseded { .. }
            | crate::Error::Wal(_)
            | crate::Error::Dispatch { .. }
            | crate::Error::Storage { .. }
            | crate::Error::ColdStorage { .. }
            | crate::Error::Serialization { .. }
            | crate::Error::Codec { .. }
            | crate::Error::SegmentCorrupted { .. }
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
            | crate::Error::CascadeCycle { .. }) => Self::Internal {
                detail: e.to_string(),
            },
        }
    }
}
