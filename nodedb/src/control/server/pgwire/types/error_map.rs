// SPDX-License-Identifier: BUSL-1.1

//! NodeDB `Error` and Data Plane `ErrorCode` to PostgreSQL SQLSTATE mapping.

use nodedb_types::error::sqlstate;
use pgwire::error::{ErrorInfo, PgWireError};

use crate::OllpExhaustedCause;
use crate::bridge::envelope::{ErrorCode, Status};
use crate::control::server::response_shape::types::DmlFoldError;

pub(crate) use super::numeric_sqlstate::numeric_code_to_sqlstate;

/// Create a pgwire ErrorResponse with a SQLSTATE code.
pub fn sqlstate_error(code: &str, message: &str) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        code.to_owned(),
        message.to_owned(),
    )))
}

/// Map a statement-tag fold refusal to the pgwire error the client reads.
/// Two tasks of one statement disagreeing on their verb is a planner bug,
/// so it surfaces as an internal error.
pub fn dml_fold_error_to_pg(e: &DmlFoldError) -> PgWireError {
    sqlstate_error(sqlstate::INTERNAL_ERROR, &e.to_string())
}

/// Map a NodeDB `Error` to the pgwire error the client reads, through the
/// one SQLSTATE table [`error_to_sqlstate`] owns.
pub fn error_to_pg(err: &crate::Error) -> PgWireError {
    let (severity, code, message) = error_to_sqlstate(err);
    PgWireError::UserError(Box::new(ErrorInfo::new(
        severity.to_owned(),
        code.to_owned(),
        message,
    )))
}

/// Map a NodeDB `Error` to the pgwire error the client reads, with `context`
/// before its message. The SQLSTATE stays the error's own.
pub fn error_to_pg_in_context(context: &str, err: &crate::Error) -> PgWireError {
    let (severity, code, message) = error_to_sqlstate(err);
    PgWireError::UserError(Box::new(ErrorInfo::new(
        severity.to_owned(),
        code.to_owned(),
        format!("{context}: {message}"),
    )))
}

/// Map an error raised while shaping a response to the pgwire error the
/// client reads, with the SQLSTATE its numeric code maps to. A per-row
/// sequence accessor refusal (`42704`, `55000`) or a division by zero
/// (`22012`) keeps its class instead of collapsing to `XX000`.
pub fn shape_error_to_pg(e: &nodedb_types::NodeDbError) -> PgWireError {
    sqlstate_error(numeric_code_to_sqlstate(e.code()), e.message())
}

/// Map a NodeDB `Error` to a PostgreSQL SQLSTATE code + message.
pub fn error_to_sqlstate(err: &crate::Error) -> (&'static str, &'static str, String) {
    match err {
        crate::Error::BadRequest { detail } => ("ERROR", sqlstate::SYNTAX_ERROR, detail.clone()),
        crate::Error::BackupTenantMismatch { .. } => {
            ("ERROR", sqlstate::BACKUP_TENANT_MISMATCH, err.to_string())
        }
        crate::Error::BackupKeyMismatch => {
            ("ERROR", sqlstate::BACKUP_KEY_MISMATCH.0, err.to_string())
        }
        crate::Error::PlanError { detail } => ("ERROR", sqlstate::SYNTAX_ERROR, detail.clone()),
        crate::Error::CollectionNotFound { collection, .. } => (
            "ERROR",
            sqlstate::UNDEFINED_TABLE,
            format!("collection \"{collection}\" does not exist"),
        ),
        crate::Error::CollectionDeactivated { collection, .. } => (
            "ERROR",
            // UNDEFINED_TABLE is the canonical pg code; the distinct message
            // carries the UNDROP hint so client UX can surface a restore
            // button without a custom sqlstate.
            sqlstate::UNDEFINED_TABLE,
            format!(
                "collection \"{collection}\" was dropped and is within its retention \
                 window; restore it with `UNDROP COLLECTION {collection}` before \
                 it is hard-deleted"
            ),
        ),
        crate::Error::FeatureNotSupported { detail } => {
            ("ERROR", sqlstate::FEATURE_NOT_SUPPORTED, detail.clone())
        }
        crate::Error::NotInTransactionBlock { .. } => {
            ("ERROR", sqlstate::ACTIVE_SQL_TRANSACTION, err.to_string())
        }
        crate::Error::UndefinedFunction { name } => (
            "ERROR",
            sqlstate::UNDEFINED_FUNCTION,
            format!("function {name}(...) does not exist"),
        ),
        crate::Error::UndefinedObject { .. } => {
            ("ERROR", sqlstate::UNDEFINED_OBJECT, err.to_string())
        }
        crate::Error::ObjectNotInPrerequisiteState { detail, .. } => (
            "ERROR",
            sqlstate::OBJECT_NOT_IN_PREREQUISITE_STATE,
            detail.clone(),
        ),
        crate::Error::UndefinedColumn { column } => (
            "ERROR",
            sqlstate::UNDEFINED_COLUMN,
            format!("column \"{column}\" does not exist"),
        ),
        crate::Error::TextColumn { fault, .. } => ("ERROR", fault.sqlstate(), err.to_string()),
        crate::Error::AmbiguousColumn { column } => (
            "ERROR",
            sqlstate::AMBIGUOUS_COLUMN,
            format!("column reference \"{column}\" is ambiguous"),
        ),
        crate::Error::UnknownStrictField { .. } => {
            ("ERROR", sqlstate::UNDEFINED_COLUMN, err.to_string())
        }
        crate::Error::DivisionByZero => ("ERROR", sqlstate::DIVISION_BY_ZERO, err.to_string()),
        crate::Error::DataException { detail } => {
            ("ERROR", sqlstate::DATA_EXCEPTION, detail.clone())
        }
        crate::Error::InvalidLimitValue { .. } => {
            ("ERROR", sqlstate::INVALID_LIMIT_VALUE, err.to_string())
        }
        crate::Error::DocumentNotFound {
            collection,
            document_id,
        } => (
            "ERROR",
            sqlstate::NO_DATA,
            format!("document \"{document_id}\" not found in \"{collection}\""),
        ),
        crate::Error::RejectedConstraint {
            constraint, detail, ..
        } => (
            "ERROR",
            crate::control::server::shared::ddl::sqlstate::constraint_sqlstate(constraint),
            detail.clone(),
        ),
        crate::Error::TxnOverlayMemoryExceeded { .. } => {
            ("ERROR", sqlstate::PROGRAM_LIMIT_EXCEEDED, err.to_string())
        }
        // Control-Plane twins of Data-Plane codes take the SQLSTATE their
        // Data-Plane code has, so one condition answers one class wherever
        // it is detected.
        crate::Error::RejectedPrevalidation { .. } | crate::Error::InsufficientBalance { .. } => {
            ("ERROR", sqlstate::CHECK_VIOLATION, err.to_string())
        }
        crate::Error::RetryableRefusal { .. } => {
            ("ERROR", sqlstate::SERIALIZATION_FAILURE, err.to_string())
        }
        crate::Error::AppendOnlyViolation { .. } => {
            ("ERROR", sqlstate::APPEND_ONLY_VIOLATION, err.to_string())
        }
        crate::Error::BalanceViolation { .. } => {
            ("ERROR", sqlstate::BALANCE_VIOLATION, err.to_string())
        }
        crate::Error::PeriodLocked { .. } => ("ERROR", sqlstate::PERIOD_LOCKED, err.to_string()),
        crate::Error::PeriodLockMisconfigured { .. } => (
            "ERROR",
            sqlstate::PERIOD_LOCK_MISCONFIGURED,
            err.to_string(),
        ),
        crate::Error::RetentionViolation { .. } => {
            ("ERROR", sqlstate::RETENTION_VIOLATION, err.to_string())
        }
        crate::Error::LegalHoldActive { .. } => {
            ("ERROR", sqlstate::LEGAL_HOLD_ACTIVE, err.to_string())
        }
        crate::Error::StateTransitionViolation { .. } => (
            "ERROR",
            sqlstate::STATE_TRANSITION_VIOLATION,
            err.to_string(),
        ),
        crate::Error::TransitionCheckViolation { .. } => (
            "ERROR",
            sqlstate::TRANSITION_CHECK_VIOLATION,
            err.to_string(),
        ),
        crate::Error::TypeGuardViolation { .. } => {
            ("ERROR", sqlstate::TYPE_GUARD_VIOLATION, err.to_string())
        }
        crate::Error::TypeMismatch { .. } => ("ERROR", sqlstate::CANNOT_COERCE, err.to_string()),
        crate::Error::DeadlineExceeded { .. } => {
            ("ERROR", sqlstate::QUERY_CANCELED.0, err.to_string())
        }
        // Nothing ran, and a retry plans against caught-up state.
        crate::Error::AuthorizationStateBehind { .. } => {
            ("ERROR", sqlstate::STALE_READ_NOT_LEADER, err.to_string())
        }
        // Nothing was read, and a retry succeeds once a leader confirms a read
        // index this node has applied.
        crate::Error::LinearizableReadRefused { .. } => {
            ("ERROR", sqlstate::STALE_READ_NOT_LEADER, err.to_string())
        }
        // Nothing was applied, and a retry succeeds once the group's majority
        // is reachable again.
        crate::Error::GroupQuorumUnavailable { .. } => {
            ("ERROR", sqlstate::LOCK_NOT_AVAILABLE, err.to_string())
        }
        // Nothing was restored, and a retry succeeds once a replica of the
        // group answers.
        crate::Error::GroupMarksUnavailable { .. } => {
            ("ERROR", sqlstate::LOCK_NOT_AVAILABLE, err.to_string())
        }
        // Nothing was captured, and a retry succeeds once the group's
        // leadership settles.
        crate::Error::BackupCaptureMoved { .. } => {
            ("ERROR", sqlstate::LOCK_NOT_AVAILABLE, err.to_string())
        }
        crate::Error::ConflictRetry { .. } => {
            ("ERROR", sqlstate::SERIALIZATION_FAILURE, err.to_string())
        }
        // A cross-shard Calvin OCC abort is a serialization failure — the client
        // must retry the whole transaction.
        crate::Error::CalvinSerializationConflict => {
            ("ERROR", sqlstate::SERIALIZATION_FAILURE, err.to_string())
        }
        // A participant error aborted the transaction before any read-set was
        // validated, so it is NOT a serialization conflict. TRANSACTION_ROLLBACK
        // (40000) keeps it in the retryable class 40 without claiming 40001.
        crate::Error::CalvinParticipantError => {
            ("ERROR", sqlstate::TRANSACTION_ROLLBACK, err.to_string())
        }
        // A descriptor changed under the statement and the server's own
        // retries ran out. The client retries the statement, so it takes
        // SERIALIZATION_FAILURE (40001), the SQLSTATE drivers retry on.
        crate::Error::RetryableSchemaChanged { .. } => {
            ("ERROR", sqlstate::SERIALIZATION_FAILURE, err.to_string())
        }
        // The session's bearer token expired. The client re-authenticates.
        crate::Error::SessionTokenExpired => {
            ("ERROR", sqlstate::AUTH_TOKEN_EXPIRED.0, err.to_string())
        }
        crate::Error::CloneWriteRequiresMaterialize { .. } => (
            "ERROR",
            sqlstate::CLONE_WRITE_REQUIRES_MATERIALIZE.0,
            err.to_string(),
        ),
        crate::Error::RejectedAuthz { .. } => {
            ("ERROR", sqlstate::INSUFFICIENT_PRIVILEGE, err.to_string())
        }
        // A rate-limit rejection is transient/retryable, distinct from a
        // credential or privilege failure. TOO_MANY_CONNECTIONS (53300) is the
        // canonical retryable code clients recognise.
        crate::Error::RateExceeded { .. } => {
            ("ERROR", sqlstate::TOO_MANY_CONNECTIONS, err.to_string())
        }
        // A dispatcher capacity refusal enqueued nothing. SERVER_OVERLOAD
        // (57P03) is transient: the client retries after a backoff.
        crate::Error::DispatchCapacity { .. } => {
            ("ERROR", sqlstate::SERVER_OVERLOAD, err.to_string())
        }
        crate::Error::MemoryExhausted { .. } => ("ERROR", sqlstate::OUT_OF_MEMORY, err.to_string()),
        crate::Error::Backpressure { .. } => ("ERROR", sqlstate::OUT_OF_MEMORY, err.to_string()),
        // A cross-collection write refused because source and target are not
        // co-resident is a not-yet-supported operation, NOT a transient/internal
        // fault — surface FEATURE_NOT_SUPPORTED (0A000) so clients do not retry.
        crate::Error::CrossCollectionNotColocated { .. } => {
            ("ERROR", sqlstate::FEATURE_NOT_SUPPORTED, err.to_string())
        }
        crate::Error::NoLeader { .. } => ("ERROR", sqlstate::LOCK_NOT_AVAILABLE, err.to_string()),
        // DATABASE_DROPPED (57P04) — the closest Postgres canonical code for
        // "try again later, different node". Client libraries that recognise
        // the 57P* family treat this as retryable transient unavailability,
        // which is exactly the semantics we want. The message carries the
        // hinted leader address so an operator inspecting logs can see the
        // redirect target.
        crate::Error::NotLeader { leader_addr, .. } => (
            "ERROR",
            sqlstate::DATABASE_DROPPED,
            format!("cluster in leader election; leader hint: {leader_addr}"),
        ),
        // OLLP retry exhaustion is retryable only when it exhausted on real
        // drift. A pre-admission cause keeps ITS OWN sqlstate — telling a client
        // to retry a deterministic failure burns another round trip — and a
        // refused admission gate is transient like any other load rejection.
        crate::Error::OllpExhausted { cause, .. } => match cause {
            OllpExhaustedCause::PredicateDrift => {
                ("ERROR", sqlstate::SERIALIZATION_FAILURE, err.to_string())
            }
            OllpExhaustedCause::PreAdmission(inner) => {
                let (severity, code, _) = error_to_sqlstate(inner);
                (severity, code, err.to_string())
            }
            OllpExhaustedCause::AdmissionRefused { .. } => {
                ("ERROR", sqlstate::TOO_MANY_CONNECTIONS, err.to_string())
            }
        },
        crate::Error::RemoteTyped { code, message } => {
            ("ERROR", numeric_code_to_sqlstate(*code), message.clone())
        }
        // A materialized-sum join key that names no target row breaks the
        // balance invariant, so it carries the same SQLSTATE the Data Plane's
        // `BalanceViolation` does rather than a generic internal error.
        crate::Error::MaterializedSumTargetNotFound { .. } => {
            ("ERROR", sqlstate::BALANCE_VIOLATION, err.to_string())
        }
        // A Data Plane verdict that travelled back as a typed code keeps the
        // SQLSTATE it has on the direct dispatch path.
        crate::Error::DataPlane(code) => {
            crate::control::server::shared::ddl::sqlstate::error_code_to_sqlstate(code)
        }
        crate::Error::Shaping(e) => (
            "ERROR",
            numeric_code_to_sqlstate(e.code()),
            e.message().to_string(),
        ),
        // A DDL error keeps the exact SQLSTATE its statement reports.
        crate::Error::Ddl(ddl) => (
            "ERROR",
            crate::control::server::shared::ddl::static_sqlstate::static_sqlstate(&ddl.sqlstate),
            ddl.message.clone(),
        ),
        // The DDL path renders a regressed consumer offset as an invalid
        // parameter value, so the typed error takes that class too.
        crate::Error::OffsetRegression { .. } => {
            ("ERROR", sqlstate::INVALID_PARAMETER_VALUE, err.to_string())
        }
        // A full admission queue is a rate refusal, its public code's class.
        crate::Error::VShardAdmissionCapacityExceeded { .. } => {
            ("ERROR", sqlstate::TOO_MANY_CONNECTIONS, err.to_string())
        }
        // The CRDT frontier kept moving. The client retries the write.
        crate::Error::CrdtAdmissionRetriesExhausted { .. } => {
            ("ERROR", sqlstate::SERIALIZATION_FAILURE, err.to_string())
        }
        crate::Error::CrdtAdmissionTimeout { .. } => {
            ("ERROR", sqlstate::QUERY_CANCELED.0, err.to_string())
        }
        // Statements refused inside an explicit transaction block share
        // the class of `NotInTransactionBlock`.
        crate::Error::CrdtApplyForbiddenInTransaction
        | crate::Error::CrossShardInExplicitTransaction => {
            ("ERROR", sqlstate::ACTIVE_SQL_TRANSACTION, err.to_string())
        }
        // Each variant here has the public code `BAD_REQUEST`, so pgwire
        // renders the class that code renders on native and across nodes.
        crate::Error::CrdtAdmissionInvalidPlan { .. }
        | crate::Error::CrdtAdmissionCallerFence
        | crate::Error::CrdtApplyRequiresAdmission
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
        | crate::Error::RoleInheritanceDepthExceeded { .. } => {
            ("ERROR", sqlstate::SYNTAX_ERROR, err.to_string())
        }
        crate::Error::DependentObjectsExist { .. } | crate::Error::RoleInUse { .. } => (
            "ERROR",
            sqlstate::DEPENDENT_OBJECTS_STILL_EXIST,
            err.to_string(),
        ),
        crate::Error::QuotaOvercommit { .. } => {
            ("ERROR", sqlstate::QUOTA_OVERCOMMIT, err.to_string())
        }
        crate::Error::TenantVectorDimExceeded { .. }
        | crate::Error::TenantGraphDepthExceeded { .. } => {
            ("ERROR", sqlstate::QUOTA_EXCEEDED, err.to_string())
        }
        crate::Error::MirrorReadOnly { .. } => (
            "ERROR",
            sqlstate::READ_ONLY_SQL_TRANSACTION,
            err.to_string(),
        ),
        // The client redirects the strong read to the source cluster.
        crate::Error::StaleReadNotLeader { .. } => {
            ("ERROR", sqlstate::STALE_READ_NOT_LEADER, err.to_string())
        }
        // The write committed and its result is gone, or its outcome is
        // unknown. The code base has no class for either, and the standard
        // ones (`08007`, `40003`) sit in classes drivers and pools retry.
        // Class `XX` is never treated as transient, so no client re-proposes
        // a write that can have committed.
        crate::Error::CommittedResultUnavailable { .. }
        | crate::Error::ProposalOutcomeUnknown { .. } => {
            ("ERROR", sqlstate::INTERNAL_ERROR, err.to_string())
        }
        // Server-side faults and system defects. The client can act on none
        // of them, and their public codes are internal classes.
        crate::Error::MaterializedSumResolutionMissing { .. }
        | crate::Error::RetryableLeaderChange { .. }
        | crate::Error::MetadataLeaderUnavailable
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
        | crate::Error::DescriptorVersionAnomaly { .. }
        | crate::Error::CatalogIntegrityViolation { .. }
        | crate::Error::CollectionPurgeRowMissing { .. }
        | crate::Error::CollectionUnstamped { .. }
        | crate::Error::CascadeCycle { .. } => ("ERROR", sqlstate::INTERNAL_ERROR, err.to_string()),
    }
}

/// Create a notice response (WARNING level).
pub fn notice_warning(message: &str) -> pgwire::messages::response::NoticeResponse {
    pgwire::messages::response::NoticeResponse::from(pgwire::error::ErrorInfo::new(
        "WARNING".to_owned(),
        sqlstate::WARNING.to_owned(),
        message.to_owned(),
    ))
}

/// Map a Data Plane response status + error code to a SQLSTATE triple.
pub fn response_status_to_sqlstate(
    status: Status,
    error_code: Option<&ErrorCode>,
) -> Option<(&'static str, &'static str, String)> {
    match status {
        Status::Ok | Status::Partial => None,
        Status::Error => {
            if let Some(code) = error_code {
                Some(crate::control::server::shared::ddl::sqlstate::error_code_to_sqlstate(code))
            } else {
                Some((
                    "ERROR",
                    sqlstate::INTERNAL_ERROR,
                    "unknown data plane error".into(),
                ))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A typed error behind a context prefix keeps its own SQLSTATE.
    #[test]
    fn an_error_in_context_keeps_its_sqlstate() {
        let missing = crate::Error::CollectionNotFound {
            tenant_id: crate::types::TenantId::new(1),
            collection: "orders".into(),
        };
        match error_to_pg_in_context("catalog read", &missing) {
            PgWireError::UserError(info) => {
                assert_eq!(info.code, sqlstate::UNDEFINED_TABLE);
                assert!(
                    info.message.starts_with("catalog read: "),
                    "{}",
                    info.message
                );
            }
            other => panic!("expected a user error, got {other:?}"),
        }
    }
}
