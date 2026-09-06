// SPDX-License-Identifier: BUSL-1.1

//! NodeDB `Error` and Data Plane `ErrorCode` to PostgreSQL SQLSTATE mapping.

use nodedb_types::error::sqlstate;
use pgwire::error::{ErrorInfo, PgWireError};

use crate::OllpExhaustedCause;
use crate::bridge::envelope::{ErrorCode, Status};

/// Create a pgwire ErrorResponse with a SQLSTATE code.
pub fn sqlstate_error(code: &str, message: &str) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        code.to_owned(),
        message.to_owned(),
    )))
}

/// Map a NodeDB `Error` to a PostgreSQL SQLSTATE code + message.
pub fn error_to_sqlstate(err: &crate::Error) -> (&'static str, &'static str, String) {
    match err {
        crate::Error::BadRequest { detail } => ("ERROR", sqlstate::SYNTAX_ERROR, detail.clone()),
        crate::Error::BackupTenantMismatch { .. } => {
            ("ERROR", sqlstate::BACKUP_TENANT_MISMATCH, err.to_string())
        }
        crate::Error::BackupKeyMismatch => {
            ("ERROR", sqlstate::BACKUP_KEY_MISMATCH, err.to_string())
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
        crate::Error::UndefinedFunction { name } => (
            "ERROR",
            sqlstate::UNDEFINED_FUNCTION,
            format!("function {name}(...) does not exist"),
        ),
        crate::Error::NotNullViolation { table, column } => (
            "ERROR",
            sqlstate::NOT_NULL_VIOLATION,
            format!(
                "null value in column '{column}' violates not-null constraint in table '{table}'"
            ),
        ),
        crate::Error::DivisionByZero => ("ERROR", sqlstate::DIVISION_BY_ZERO, err.to_string()),
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
        crate::Error::RejectedConstraint { detail, .. } => {
            ("ERROR", sqlstate::UNIQUE_VIOLATION, detail.clone())
        }
        crate::Error::TxnOverlayMemoryExceeded { .. } => {
            ("ERROR", sqlstate::PROGRAM_LIMIT_EXCEEDED, err.to_string())
        }
        crate::Error::DeadlineExceeded { .. } => {
            ("ERROR", sqlstate::QUERY_CANCELED, err.to_string())
        }
        crate::Error::ConflictRetry { .. } => {
            ("ERROR", sqlstate::SERIALIZATION_FAILURE, err.to_string())
        }
        // A cross-shard Calvin OCC abort is a serialization failure — the client
        // should retry the whole transaction.
        crate::Error::CalvinSerializationConflict => {
            ("ERROR", sqlstate::SERIALIZATION_FAILURE, err.to_string())
        }
        // A participant error aborted the transaction before any read-set was
        // validated, so it is NOT a serialization conflict. TRANSACTION_ROLLBACK
        // (40000) keeps it in the retryable class 40 without claiming 40001.
        crate::Error::CalvinParticipantError => {
            ("ERROR", sqlstate::TRANSACTION_ROLLBACK, err.to_string())
        }
        crate::Error::SourceFrozen { .. } => {
            ("ERROR", sqlstate::SERIALIZATION_FAILURE, err.to_string())
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
        crate::Error::MemoryExhausted { .. } => ("ERROR", sqlstate::OUT_OF_MEMORY, err.to_string()),
        crate::Error::Backpressure { .. } => ("ERROR", sqlstate::OUT_OF_MEMORY, err.to_string()),
        crate::Error::FanOutExceeded { .. } => {
            ("ERROR", sqlstate::STATEMENT_TOO_COMPLEX, err.to_string())
        }
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
        // to retry a deterministic failure just burns another round trip — and a
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
        // SQLSTATE it would have had on the direct dispatch path.
        crate::Error::DataPlane(code) => {
            crate::control::server::shared::ddl::sqlstate::error_code_to_sqlstate(code)
        }
        _ => ("ERROR", sqlstate::INTERNAL_ERROR, err.to_string()),
    }
}

/// Map a numeric `ErrorCode` received from a remote node back to a SQLSTATE.
/// Local errors map by variant identity above; a remote error arrives as a bare
/// numeric code, so this recovers the classification. Each bucket mirrors the
/// sqlstate the corresponding local variant arm chooses above for the same
/// numeric code, so a constraint violation (say) maps to the same SQLSTATE
/// whether it happened locally or on a remote node. Unmapped/unknown codes
/// fall back to INTERNAL_ERROR — the behaviour before codes were preserved.
pub(crate) fn numeric_code_to_sqlstate(code: nodedb_types::error::ErrorCode) -> &'static str {
    use nodedb_types::error::ErrorCode as Ec;
    match code {
        // Mirrors the `RejectedConstraint` arm.
        Ec::CONSTRAINT_VIOLATION => sqlstate::UNIQUE_VIOLATION,
        // Mirrors the `ConflictRetry` / `CalvinSerializationConflict` /
        // `SourceFrozen` arms, and `OllpExhausted` when it exhausted on drift.
        Ec::WRITE_CONFLICT => sqlstate::SERIALIZATION_FAILURE,
        // Mirrors the `DeadlineExceeded` arm.
        Ec::DEADLINE_EXCEEDED => sqlstate::QUERY_CANCELED,
        // Mirrors the `CollectionNotFound` / `CollectionDeactivated` arms.
        Ec::COLLECTION_NOT_FOUND | Ec::COLLECTION_DEACTIVATED => sqlstate::UNDEFINED_TABLE,
        // Mirrors the `DocumentNotFound` arm.
        Ec::DOCUMENT_NOT_FOUND => sqlstate::NO_DATA,
        // Mirrors the `BadRequest` / `PlanError` arms.
        Ec::BAD_REQUEST | Ec::PLAN_ERROR => sqlstate::SYNTAX_ERROR,
        // Mirrors the `UndefinedFunction` arm.
        Ec::UNDEFINED_FUNCTION => sqlstate::UNDEFINED_FUNCTION,
        // Mirrors the `DivisionByZero` arm.
        Ec::DIVISION_BY_ZERO => sqlstate::DIVISION_BY_ZERO,
        // Mirrors the `InvalidLimitValue` arm.
        Ec::INVALID_LIMIT_VALUE => sqlstate::INVALID_LIMIT_VALUE,
        // Mirrors the `FanOutExceeded` arm.
        Ec::FAN_OUT_EXCEEDED => sqlstate::STATEMENT_TOO_COMPLEX,
        // Mirrors the `RejectedAuthz` arm.
        Ec::AUTHORIZATION_DENIED => sqlstate::INSUFFICIENT_PRIVILEGE,
        // Mirrors the `RateExceeded` arm.
        Ec::RATE_EXCEEDED => sqlstate::TOO_MANY_CONNECTIONS,
        // Mirrors the `MemoryExhausted` / `Backpressure` arms.
        Ec::MEMORY_EXHAUSTED => sqlstate::OUT_OF_MEMORY,
        // Mirrors the `NoLeader` arm.
        Ec::NO_LEADER => sqlstate::LOCK_NOT_AVAILABLE,
        // Mirrors the `NotLeader` arm.
        Ec::NOT_LEADER => sqlstate::DATABASE_DROPPED,
        // Mirrors the `CloneWriteRequiresMaterialize` arm.
        Ec::CLONE_WRITE_REQUIRES_MATERIALIZE => sqlstate::CLONE_WRITE_REQUIRES_MATERIALIZE.0,
        _ => sqlstate::INTERNAL_ERROR,
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
                Some(("ERROR", "XX000", "unknown data plane error".into()))
            }
        }
    }
}
