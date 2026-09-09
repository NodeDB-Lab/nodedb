// SPDX-License-Identifier: Apache-2.0

//! [`NodeDbError`] struct + accessors + category predicates + `From<io::Error>`.

use std::fmt;

use serde::{Deserialize, Serialize};

use super::code::ErrorCode;
use super::details::ErrorDetails;

/// Public error type returned by all `NodeDb` trait methods.
///
/// Separates machine-readable data ([`ErrorCode`] + [`ErrorDetails`]) from
/// the human-readable `message`. Optional `cause` preserves the error chain.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeDbError {
    pub(super) code: ErrorCode,
    pub(super) message: String,
    pub(super) details: ErrorDetails,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) cause: Option<Box<NodeDbError>>,
}

impl fmt::Display for NodeDbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}] {}", self.code, self.message)
    }
}

impl std::error::Error for NodeDbError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.cause.as_deref().map(|e| e as &dyn std::error::Error)
    }
}

// ── Accessors ──

impl NodeDbError {
    /// The stable numeric error code.
    pub fn code(&self) -> ErrorCode {
        self.code
    }

    /// Human-readable error message.
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Machine-matchable error details.
    pub fn details(&self) -> &ErrorDetails {
        &self.details
    }

    /// The chained cause, if any.
    pub fn cause(&self) -> Option<&NodeDbError> {
        self.cause.as_deref()
    }

    /// Attach a cause to this error.
    pub fn with_cause(mut self, cause: NodeDbError) -> Self {
        self.cause = Some(Box::new(cause));
        self
    }

    /// Whether this error is retriable by the client.
    pub fn is_retriable(&self) -> bool {
        matches!(
            self.details,
            ErrorDetails::WriteConflict { .. }
                | ErrorDetails::DeadlineExceeded
                | ErrorDetails::NoLeader
                | ErrorDetails::NotLeader { .. }
                | ErrorDetails::MigrationInProgress
                | ErrorDetails::NodeUnreachable
                | ErrorDetails::SyncConnectionFailed
                | ErrorDetails::Bridge { .. }
                | ErrorDetails::MemoryExhausted { .. }
        )
    }

    /// Whether this error indicates the client sent invalid input.
    pub fn is_client_error(&self) -> bool {
        matches!(
            self.details,
            ErrorDetails::BadRequest
                | ErrorDetails::ConstraintViolation { .. }
                | ErrorDetails::AppendOnlyViolation { .. }
                | ErrorDetails::BalanceViolation { .. }
                | ErrorDetails::PeriodLocked { .. }
                | ErrorDetails::StateTransitionViolation { .. }
                | ErrorDetails::TransitionCheckViolation { .. }
                | ErrorDetails::RetentionViolation { .. }
                | ErrorDetails::LegalHoldActive { .. }
                | ErrorDetails::CollectionNotFound { .. }
                | ErrorDetails::DocumentNotFound { .. }
                | ErrorDetails::AuthorizationDenied { .. }
                | ErrorDetails::AuthExpired
                | ErrorDetails::Config
                | ErrorDetails::SqlNotEnabled
                | ErrorDetails::UndefinedFunction { .. }
                | ErrorDetails::UndefinedColumn { .. }
                | ErrorDetails::AmbiguousColumn { .. }
                | ErrorDetails::DivisionByZero
                | ErrorDetails::InvalidLimitValue { .. }
                | ErrorDetails::BackupTenantMismatch { .. }
                | ErrorDetails::BackupKeyMismatch
        )
    }
}

// ── Category predicates ──

impl NodeDbError {
    pub fn is_constraint_violation(&self) -> bool {
        matches!(self.details, ErrorDetails::ConstraintViolation { .. })
    }
    pub fn is_not_found(&self) -> bool {
        matches!(
            self.details,
            ErrorDetails::CollectionNotFound { .. }
                | ErrorDetails::DocumentNotFound { .. }
                | ErrorDetails::DatabaseNotFound { .. }
        )
    }
    pub fn is_auth_denied(&self) -> bool {
        matches!(self.details, ErrorDetails::AuthorizationDenied { .. })
    }
    pub fn is_storage(&self) -> bool {
        matches!(
            self.details,
            ErrorDetails::Storage { .. }
                | ErrorDetails::SegmentCorrupted { .. }
                | ErrorDetails::ColdStorage { .. }
                | ErrorDetails::Wal { .. }
        )
    }
    pub fn is_internal(&self) -> bool {
        matches!(self.details, ErrorDetails::Internal { .. })
    }
    pub fn is_type_mismatch(&self) -> bool {
        matches!(self.details, ErrorDetails::TypeMismatch { .. })
    }
    pub fn is_type_guard_violation(&self) -> bool {
        matches!(self.details, ErrorDetails::TypeGuardViolation { .. })
    }
    pub fn is_overflow(&self) -> bool {
        matches!(self.details, ErrorDetails::Overflow { .. })
    }
    pub fn is_insufficient_balance(&self) -> bool {
        matches!(self.details, ErrorDetails::InsufficientBalance { .. })
    }
    pub fn is_rate_exceeded(&self) -> bool {
        matches!(self.details, ErrorDetails::RateExceeded { .. })
    }
    pub fn is_cluster(&self) -> bool {
        matches!(
            self.details,
            ErrorDetails::NoLeader
                | ErrorDetails::NotLeader { .. }
                | ErrorDetails::MigrationInProgress
                | ErrorDetails::NodeUnreachable
                | ErrorDetails::Cluster
        )
    }
}

/// Result alias for NodeDb operations.
pub type NodeDbResult<T> = std::result::Result<T, NodeDbError>;

impl From<std::io::Error> for NodeDbError {
    fn from(e: std::io::Error) -> Self {
        Self::storage(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_display_includes_code() {
        let e = NodeDbError::constraint_violation("users", "unique", "duplicate email");
        let msg = e.to_string();
        assert!(msg.contains("NDB-1000"));
        assert!(msg.contains("constraint violation"));
        assert!(msg.contains("users"));
    }

    #[test]
    fn error_code_accessor() {
        let e = NodeDbError::write_conflict("orders", "abc");
        assert_eq!(e.code(), ErrorCode::WRITE_CONFLICT);
        assert_eq!(e.code().0, 1001);
    }

    #[test]
    fn details_matching() {
        let e = NodeDbError::collection_not_found("users");
        assert!(matches!(
            e.details(),
            ErrorDetails::CollectionNotFound { collection } if collection == "users"
        ));
        assert!(e.is_not_found());
    }

    #[test]
    fn error_cause_chaining() {
        let inner = NodeDbError::storage("disk full");
        let outer = NodeDbError::internal("write failed").with_cause(inner);
        assert!(outer.cause().is_some());
        assert!(outer.cause().unwrap().is_storage());
        assert!(outer.cause().unwrap().message().contains("disk full"));
    }

    #[test]
    fn io_error_converts() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file missing");
        let e: NodeDbError = io_err.into();
        assert!(e.is_storage());
        assert_eq!(e.code(), ErrorCode::STORAGE);
    }

    #[test]
    fn retriable_errors() {
        assert!(NodeDbError::write_conflict("x", "y").is_retriable());
        assert!(NodeDbError::deadline_exceeded().is_retriable());
        assert!(!NodeDbError::bad_request("bad").is_retriable());
    }

    #[test]
    fn client_errors() {
        assert!(NodeDbError::bad_request("bad").is_client_error());
        assert!(!NodeDbError::internal("oops").is_client_error());
    }

    #[test]
    fn json_serialization() {
        let e = NodeDbError::collection_not_found("users");
        let json = serde_json::to_value(&e).unwrap();
        assert_eq!(json["code"], 1100);
        assert!(json["message"].as_str().unwrap().contains("users"));
        assert_eq!(json["details"]["kind"], "collection_not_found");
        assert_eq!(json["details"]["collection"], "users");
    }

    #[test]
    fn sync_delta_rejected_ctor() {
        let e = NodeDbError::sync_delta_rejected(
            "unique violation",
            Some(
                crate::sync::compensation::CompensationHint::UniqueViolation {
                    field: "email".into(),
                    conflicting_value: "a@b.com".into(),
                },
            ),
        );
        assert!(e.to_string().contains("NDB-3001"));
        assert!(e.to_string().contains("sync delta rejected"));
    }

    #[test]
    fn sql_not_enabled_ctor() {
        let e = NodeDbError::sql_not_enabled();
        assert!(e.to_string().contains("SQL not enabled"));
        assert_eq!(e.code(), ErrorCode::SQL_NOT_ENABLED);
    }
}
