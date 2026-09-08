// SPDX-License-Identifier: BUSL-1.1

//! Data Plane `ErrorCode` to PostgreSQL SQLSTATE mapping (protocol-neutral).

use nodedb_types::error::sqlstate;

use crate::bridge::envelope::ErrorCode;

/// Map a Data Plane `ErrorCode` to SQLSTATE.
pub fn error_code_to_sqlstate(code: &ErrorCode) -> (&'static str, &'static str, String) {
    match code {
        ErrorCode::DeadlineExceeded => (
            "ERROR",
            sqlstate::QUERY_CANCELED,
            "query cancelled due to deadline".into(),
        ),
        ErrorCode::RejectedConstraint { constraint, detail } => {
            let code = if constraint == "not_null" {
                sqlstate::NOT_NULL_VIOLATION
            } else {
                sqlstate::UNIQUE_VIOLATION
            };
            (
                "ERROR",
                code,
                if detail.is_empty() {
                    format!("constraint violation: {constraint}")
                } else {
                    format!("constraint violation: {constraint}: {detail}")
                },
            )
        }
        ErrorCode::RejectedPrevalidation { reason } => (
            "ERROR",
            sqlstate::CHECK_VIOLATION,
            format!("pre-validation rejected: {reason}"),
        ),
        // Nothing applied and the identical statement is expected to succeed
        // later, so drivers get the same class they already retry on rather
        // than a check violation they would surface as permanent.
        ErrorCode::RetryableRefusal { reason } => (
            "ERROR",
            sqlstate::SERIALIZATION_FAILURE,
            format!("write refused without applying, retry: {reason}"),
        ),
        ErrorCode::NotFound => ("ERROR", sqlstate::NO_DATA, "not found".into()),
        // `resource` is what makes the denial actionable: it says whether a
        // row-level-security policy refused the row or a grant is missing, and
        // on which collection. A bare "authorization denied" tells the client
        // nothing it can respond to.
        ErrorCode::RejectedAuthz { resource } => (
            "ERROR",
            sqlstate::INSUFFICIENT_PRIVILEGE,
            format!("authorization denied: {resource}"),
        ),
        ErrorCode::ConflictRetry => (
            "ERROR",
            sqlstate::SERIALIZATION_FAILURE,
            "write conflict, retry".into(),
        ),
        ErrorCode::FanOutExceeded => (
            "ERROR",
            sqlstate::STATEMENT_TOO_COMPLEX,
            "fan-out limit exceeded".into(),
        ),
        ErrorCode::ResourcesExhausted => (
            "ERROR",
            sqlstate::OUT_OF_MEMORY,
            "query result exceeded the scan memory budget; add a LIMIT clause \
             or a more selective filter, or raise \
             [tuning.query] max_scan_result_bytes"
                .into(),
        ),
        ErrorCode::RejectedDanglingEdge { missing_node } => (
            "ERROR",
            sqlstate::FOREIGN_KEY_VIOLATION,
            format!("edge rejected: node \"{missing_node}\" does not exist"),
        ),
        ErrorCode::DuplicateWrite => (
            "ERROR",
            sqlstate::UNIQUE_VIOLATION,
            "duplicate write detected via idempotency key".into(),
        ),
        ErrorCode::AppendOnlyViolation { collection } => (
            "ERROR",
            sqlstate::APPEND_ONLY_VIOLATION,
            format!("append-only violation: UPDATE/DELETE not allowed on {collection}"),
        ),
        ErrorCode::BalanceViolation { collection, detail } => (
            "ERROR",
            sqlstate::BALANCE_VIOLATION,
            format!("balance violation on {collection}: {detail}"),
        ),
        ErrorCode::PeriodLocked { collection } => (
            "ERROR",
            sqlstate::PERIOD_LOCKED,
            format!("period locked: writes rejected on {collection}"),
        ),
        ErrorCode::RetentionViolation { collection } => (
            "ERROR",
            sqlstate::RETENTION_VIOLATION,
            format!("retention violation: cannot delete from {collection}"),
        ),
        ErrorCode::LegalHoldActive { collection } => (
            "ERROR",
            sqlstate::LEGAL_HOLD_ACTIVE,
            format!("legal hold active: cannot delete from {collection}"),
        ),
        ErrorCode::StateTransitionViolation { collection, detail } => (
            "ERROR",
            sqlstate::STATE_TRANSITION_VIOLATION,
            format!("state transition violation on {collection}: {detail}"),
        ),
        ErrorCode::TransitionCheckViolation { collection, detail } => (
            "ERROR",
            sqlstate::TRANSITION_CHECK_VIOLATION,
            format!("transition check violation on {collection}: {detail}"),
        ),
        ErrorCode::TypeGuardViolation { collection, detail } => (
            "ERROR",
            sqlstate::TYPE_GUARD_VIOLATION,
            format!("type guard violation on {collection}: {detail}"),
        ),
        ErrorCode::TypeMismatch { collection, detail } => (
            "ERROR",
            sqlstate::CANNOT_COERCE,
            format!("type mismatch on {collection}: {detail}"),
        ),
        ErrorCode::OverflowError { collection } => (
            "ERROR",
            sqlstate::NUMERIC_VALUE_OUT_OF_RANGE,
            format!("arithmetic overflow on {collection}"),
        ),
        ErrorCode::InsufficientBalance { collection, detail } => (
            "ERROR",
            sqlstate::CHECK_VIOLATION,
            format!("insufficient balance on {collection}: {detail}"),
        ),
        ErrorCode::RateExceeded {
            gate,
            retry_after_ms,
        } => (
            "ERROR",
            sqlstate::STATEMENT_TOO_COMPLEX,
            format!("rate limit exceeded for {gate}, retry after {retry_after_ms}ms"),
        ),
        ErrorCode::CollectionDraining { collection } => (
            "ERROR",
            sqlstate::CANNOT_CONNECT_NOW,
            format!(
                "collection '{collection}' is draining for hard-delete; retry after purge completes"
            ),
        ),
        ErrorCode::RecursionDepthExceeded {
            cte_name,
            max_depth,
        } => (
            "ERROR",
            sqlstate::PROGRAM_LIMIT_EXCEEDED,
            format!(
                "WITH RECURSIVE CTE '{cte_name}' exceeded max recursion depth {max_depth}; \
                 add a stricter termination condition or raise max_recursion_depth"
            ),
        ),
        ErrorCode::UndefinedColumn { column } => (
            "ERROR",
            sqlstate::UNDEFINED_COLUMN,
            format!("column \"{column}\" does not exist"),
        ),
        ErrorCode::Internal { detail } => ("ERROR", sqlstate::INTERNAL_ERROR, detail.clone()),
        // Division/modulo by zero.
        ErrorCode::DivisionByZero => (
            "ERROR",
            sqlstate::DIVISION_BY_ZERO,
            "division by zero".into(),
        ),
        ErrorCode::Unsupported { detail } => {
            ("ERROR", sqlstate::FEATURE_NOT_SUPPORTED, detail.clone())
        }
        ErrorCode::RollbackFailed {
            entry_index,
            detail,
        } => (
            "ERROR",
            sqlstate::INTERNAL_ERROR,
            format!(
                "transaction rollback failed at undo entry {entry_index}: {detail}; \
                 shard state is unknown — restart required"
            ),
        ),
        // OllpRetryRequired is an internal scheduler signal and should not
        // reach the pgwire layer as a user-visible error. If it does, surface
        // it as a serialization failure so clients retry automatically.
        ErrorCode::OllpRetryRequired => (
            "ERROR",
            sqlstate::SERIALIZATION_FAILURE,
            "optimistic predicate retry required; transaction will be retried".into(),
        ),
        ErrorCode::CrdtFrontierMismatch { .. } => (
            "ERROR",
            sqlstate::SERIALIZATION_FAILURE,
            "CRDT state changed after preview; retry the write".into(),
        ),
        ErrorCode::TxnOverlayMemoryExceeded { limit } => (
            "ERROR",
            sqlstate::PROGRAM_LIMIT_EXCEEDED,
            format!(
                "transaction staging overlay exceeded its {limit}-byte per-core budget; \
                 split the transaction into smaller batches"
            ),
        ),
    }
}
