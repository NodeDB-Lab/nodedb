// SPDX-License-Identifier: BUSL-1.1

//! NodeDB `Error` and Data Plane `ErrorCode` to PostgreSQL SQLSTATE mapping.

use nodedb_types::error::sqlstate;
use pgwire::error::{ErrorInfo, PgWireError};

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
        crate::Error::SourceFrozen { .. } => {
            ("ERROR", sqlstate::SERIALIZATION_FAILURE, err.to_string())
        }
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
        // OLLP dependent-read retry exhaustion is client-retryable: the predicate's
        // matching set kept drifting across fresh re-scans. Surface as a
        // serialization failure (40001) so clients retry, not as an opaque XX000.
        crate::Error::OllpExhausted { .. } => {
            ("ERROR", sqlstate::SERIALIZATION_FAILURE, err.to_string())
        }
        _ => ("ERROR", sqlstate::INTERNAL_ERROR, err.to_string()),
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
    error_code: &Option<ErrorCode>,
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
