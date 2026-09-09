// SPDX-License-Identifier: Apache-2.0

//! Write-path error constructors (codes 1000–1099 + accounting + type enforcement).

use std::fmt;

use super::super::code::ErrorCode;
use super::super::details::ErrorDetails;
use super::super::types::NodeDbError;

impl NodeDbError {
    /// `constraint` names the constraint kind (`"not_null"`, `"unique"`,
    /// ...) so every hop — local or reconstructed from a remote node — can
    /// pick the right SQLSTATE instead of guessing unique_violation for
    /// everything.
    pub fn constraint_violation(
        collection: impl Into<String>,
        constraint: impl Into<String>,
        detail: impl fmt::Display,
    ) -> Self {
        let collection = collection.into();
        let constraint = constraint.into();
        Self {
            code: ErrorCode::CONSTRAINT_VIOLATION,
            message: format!("constraint violation on {collection} ({constraint}): {detail}"),
            details: ErrorDetails::ConstraintViolation {
                collection,
                constraint,
            },
            cause: None,
        }
    }

    pub fn write_conflict(collection: impl Into<String>, document_id: impl Into<String>) -> Self {
        let collection = collection.into();
        let document_id = document_id.into();
        Self {
            code: ErrorCode::WRITE_CONFLICT,
            message: format!(
                "write conflict on {collection}/{document_id}, retry with idempotency key"
            ),
            details: ErrorDetails::WriteConflict {
                collection,
                document_id,
            },
            cause: None,
        }
    }

    pub fn deadline_exceeded() -> Self {
        Self {
            code: ErrorCode::DEADLINE_EXCEEDED,
            message: "request exceeded deadline".into(),
            details: ErrorDetails::DeadlineExceeded,
            cause: None,
        }
    }

    pub fn prevalidation_rejected(
        constraint: impl Into<String>,
        reason: impl fmt::Display,
    ) -> Self {
        let constraint = constraint.into();
        Self {
            code: ErrorCode::PREVALIDATION_REJECTED,
            message: format!("pre-validation rejected: {constraint} — {reason}"),
            details: ErrorDetails::PrevalidationRejected { constraint },
            cause: None,
        }
    }

    pub fn append_only_violation(collection: impl Into<String>, detail: impl fmt::Display) -> Self {
        let collection = collection.into();
        Self {
            code: ErrorCode::APPEND_ONLY_VIOLATION,
            message: format!("append-only violation on {collection}: {detail}"),
            details: ErrorDetails::AppendOnlyViolation { collection },
            cause: None,
        }
    }

    pub fn balance_violation(collection: impl Into<String>, detail: impl fmt::Display) -> Self {
        let collection = collection.into();
        Self {
            code: ErrorCode::BALANCE_VIOLATION,
            message: format!("balance violation on {collection}: {detail}"),
            details: ErrorDetails::BalanceViolation { collection },
            cause: None,
        }
    }

    pub fn period_locked(collection: impl Into<String>, detail: impl fmt::Display) -> Self {
        let collection = collection.into();
        Self {
            code: ErrorCode::PERIOD_LOCKED,
            message: format!("period locked on {collection}: {detail}"),
            details: ErrorDetails::PeriodLocked { collection },
            cause: None,
        }
    }

    pub fn state_transition_violation(
        collection: impl Into<String>,
        detail: impl fmt::Display,
    ) -> Self {
        let collection = collection.into();
        Self {
            code: ErrorCode::STATE_TRANSITION_VIOLATION,
            message: format!("state transition violation on {collection}: {detail}"),
            details: ErrorDetails::StateTransitionViolation { collection },
            cause: None,
        }
    }

    pub fn transition_check_violation(
        collection: impl Into<String>,
        detail: impl fmt::Display,
    ) -> Self {
        let collection = collection.into();
        Self {
            code: ErrorCode::TRANSITION_CHECK_VIOLATION,
            message: format!("transition check violation on {collection}: {detail}"),
            details: ErrorDetails::TransitionCheckViolation { collection },
            cause: None,
        }
    }

    pub fn type_guard_violation(collection: impl Into<String>, detail: impl fmt::Display) -> Self {
        let collection = collection.into();
        Self {
            code: ErrorCode::TYPE_GUARD_VIOLATION,
            message: format!("type guard violation on {collection}: {detail}"),
            details: ErrorDetails::TypeGuardViolation { collection },
            cause: None,
        }
    }

    pub fn retention_violation(collection: impl Into<String>, detail: impl fmt::Display) -> Self {
        let collection = collection.into();
        Self {
            code: ErrorCode::RETENTION_VIOLATION,
            message: format!("retention violation on {collection}: {detail}"),
            details: ErrorDetails::RetentionViolation { collection },
            cause: None,
        }
    }

    pub fn legal_hold_active(collection: impl Into<String>, detail: impl fmt::Display) -> Self {
        let collection = collection.into();
        Self {
            code: ErrorCode::LEGAL_HOLD_ACTIVE,
            message: format!("legal hold active on {collection}: {detail}"),
            details: ErrorDetails::LegalHoldActive { collection },
            cause: None,
        }
    }

    pub fn type_mismatch(collection: impl Into<String>, detail: impl fmt::Display) -> Self {
        let collection = collection.into();
        Self {
            code: ErrorCode::TYPE_MISMATCH,
            message: format!("type mismatch on {collection}: {detail}"),
            details: ErrorDetails::TypeMismatch { collection },
            cause: None,
        }
    }

    pub fn overflow(collection: impl Into<String>, detail: impl fmt::Display) -> Self {
        let collection = collection.into();
        Self {
            code: ErrorCode::OVERFLOW,
            message: format!("arithmetic overflow on {collection}: {detail}"),
            details: ErrorDetails::Overflow { collection },
            cause: None,
        }
    }

    pub fn insufficient_balance(collection: impl Into<String>, detail: impl fmt::Display) -> Self {
        let collection = collection.into();
        Self {
            code: ErrorCode::INSUFFICIENT_BALANCE,
            message: format!("insufficient balance on {collection}: {detail}"),
            details: ErrorDetails::InsufficientBalance { collection },
            cause: None,
        }
    }

    pub fn rate_exceeded(gate: impl Into<String>, detail: impl fmt::Display) -> Self {
        let gate = gate.into();
        Self {
            code: ErrorCode::RATE_EXCEEDED,
            message: format!("rate limit exceeded for {gate}: {detail}"),
            details: ErrorDetails::RateExceeded { gate },
            cause: None,
        }
    }
}
