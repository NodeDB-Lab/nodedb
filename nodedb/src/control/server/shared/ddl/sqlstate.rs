// SPDX-License-Identifier: BUSL-1.1

//! Data Plane `ErrorCode` to PostgreSQL SQLSTATE mapping (protocol-neutral).

use nodedb_types::error::sqlstate;

use crate::bridge::envelope::ErrorCode;

/// The SQLSTATE for a rejected constraint of kind `constraint`.
///
/// `not_null` and `unique` keep their specific codes; `generated_always`
/// (a write to a generated column) is `428C9`. A CRDT delta refusal carries
/// its violation kind: `fk_missing` is `23503`, and `rls_policy` /
/// `permission_denied` are `42501`. Every other kind is the generic
/// integrity class `23000`, never `unique_violation`.
pub fn constraint_sqlstate(constraint: &str) -> &'static str {
    match constraint {
        "not_null" => sqlstate::NOT_NULL_VIOLATION,
        "unique" => sqlstate::UNIQUE_VIOLATION,
        "generated_always" => sqlstate::GENERATED_ALWAYS,
        "fk_missing" => sqlstate::FOREIGN_KEY_VIOLATION,
        "rls_policy" | "permission_denied" => sqlstate::INSUFFICIENT_PRIVILEGE,
        _ => sqlstate::INTEGRITY_CONSTRAINT_VIOLATION,
    }
}

/// Map a Data Plane `ErrorCode` to SQLSTATE.
pub fn error_code_to_sqlstate(code: &ErrorCode) -> (&'static str, &'static str, String) {
    match code {
        ErrorCode::DeadlineExceeded | ErrorCode::ExpiredBeforeExecution => (
            "ERROR",
            sqlstate::QUERY_CANCELED.0,
            "query cancelled due to deadline".into(),
        ),
        ErrorCode::RejectedConstraint { constraint, detail } => {
            let code = constraint_sqlstate(constraint);
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
        ErrorCode::SyncRejected { violation, .. } => (
            "ERROR",
            sqlstate::CHECK_VIOLATION,
            format!("sync frame rejected: {violation}"),
        ),
        // Nothing applied, and the sender re-sends or retires the frame by
        // the hold, so it takes the class drivers already retry on.
        ErrorCode::SyncNotApplied { hold, .. } => (
            "ERROR",
            sqlstate::SERIALIZATION_FAILURE,
            format!("sync frame not applied: {hold}"),
        ),
        // Nothing applied and the identical statement is expected to succeed
        // later, so drivers get the same class they already retry on rather
        // than a check violation they will surface as permanent.
        ErrorCode::RetryableRefusal { reason } => (
            "ERROR",
            sqlstate::SERIALIZATION_FAILURE,
            format!("write refused without applying, retry: {reason}"),
        ),
        // The fail-stopped core applied nothing, and another replica or a
        // restart serves the statement: the class drivers retry on.
        ErrorCode::CoreFailStopped { core_id, detail } => (
            "ERROR",
            sqlstate::SERIALIZATION_FAILURE,
            format!("core {core_id} is fail-stopped and applied nothing, retry: {detail}"),
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
        ErrorCode::PeriodLockMisconfigured {
            collection,
            ref_table,
            status_column,
            row_identity,
        } => (
            "ERROR",
            sqlstate::PERIOD_LOCK_MISCONFIGURED,
            format!(
                "period lock on {collection} misconfigured: reference table \
                 '{ref_table}' row '{row_identity}' has no column '{status_column}'"
            ),
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
        ErrorCode::CounterFault { collection, fault } => (
            "ERROR",
            fault.sqlstate(),
            format!("{} on {collection}", fault.message()),
        ),
        ErrorCode::InsufficientBalance { collection, detail } => (
            "ERROR",
            sqlstate::CHECK_VIOLATION,
            format!("insufficient balance on {collection}: {detail}"),
        ),
        // The transient, retryable class, the same SQLSTATE the Control
        // Plane gives `crate::Error::RateExceeded`.
        ErrorCode::RateExceeded {
            gate,
            retry_after_ms,
        } => (
            "ERROR",
            sqlstate::TOO_MANY_CONNECTIONS,
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
        ErrorCode::TextColumn {
            collection,
            column,
            fault,
        } => (
            "ERROR",
            fault.sqlstate(),
            format!("column \"{column}\" of collection \"{collection}\" {fault}"),
        ),
        ErrorCode::Internal { detail } => ("ERROR", sqlstate::INTERNAL_ERROR, detail.clone()),
        // Division/modulo by zero.
        ErrorCode::DivisionByZero => (
            "ERROR",
            sqlstate::DIVISION_BY_ZERO,
            "division by zero".into(),
        ),
        ErrorCode::UndefinedFunction { name } => (
            "ERROR",
            sqlstate::UNDEFINED_FUNCTION,
            format!("function {name}() does not exist"),
        ),
        ErrorCode::DataException { detail } => ("ERROR", sqlstate::DATA_EXCEPTION, detail.clone()),
        ErrorCode::NumericValueOutOfRange { detail } => (
            "ERROR",
            sqlstate::NUMERIC_VALUE_OUT_OF_RANGE,
            detail.clone(),
        ),
        ErrorCode::InvalidTextRepresentation { detail } => (
            "ERROR",
            sqlstate::INVALID_TEXT_REPRESENTATION,
            detail.clone(),
        ),
        ErrorCode::DatatypeMismatch { detail } => {
            ("ERROR", sqlstate::DATATYPE_MISMATCH, detail.clone())
        }
        ErrorCode::InvalidDatetimeFormat { detail } => {
            ("ERROR", sqlstate::INVALID_DATETIME_FORMAT, detail.clone())
        }
        ErrorCode::DatetimeFieldOverflow { detail } => {
            ("ERROR", sqlstate::DATETIME_FIELD_OVERFLOW, detail.clone())
        }
        ErrorCode::UndefinedObject { object } => (
            "ERROR",
            sqlstate::UNDEFINED_OBJECT,
            format!("{object} does not exist"),
        ),
        ErrorCode::ObjectNotInPrerequisiteState { detail, .. } => (
            "ERROR",
            sqlstate::OBJECT_NOT_IN_PREREQUISITE_STATE,
            detail.clone(),
        ),
        // The same SQLSTATE the Control Plane gives `crate::Error::BadRequest`.
        ErrorCode::BadRequest { detail } => ("ERROR", sqlstate::SYNTAX_ERROR, detail.clone()),
        ErrorCode::TransactionRollback { detail } => {
            ("ERROR", sqlstate::TRANSACTION_ROLLBACK, detail.clone())
        }
        ErrorCode::ActiveSqlTransaction { detail } => {
            ("ERROR", sqlstate::ACTIVE_SQL_TRANSACTION, detail.clone())
        }
        ErrorCode::DependentObjectsExist { detail, .. } => (
            "ERROR",
            sqlstate::DEPENDENT_OBJECTS_STILL_EXIST,
            detail.clone(),
        ),
        // Transient: the client retries after a backoff.
        ErrorCode::DispatchCapacity { reason } => {
            ("ERROR", sqlstate::SERVER_OVERLOAD, reason.clone())
        }
        ErrorCode::Unsupported { detail } => {
            ("ERROR", sqlstate::FEATURE_NOT_SUPPORTED, detail.clone())
        }
        ErrorCode::RollbackFailed {
            entry_index,
            detail,
            cause,
        } => {
            // The message of the typed cause, as its own code renders it.
            let because = cause
                .as_deref()
                .map(|cause| format!(" ({})", error_code_to_sqlstate(cause).2))
                .unwrap_or_default();
            (
                "ERROR",
                sqlstate::INTERNAL_ERROR,
                format!(
                    "transaction rollback failed at undo entry {entry_index}: \
                     {detail}{because}; shard state is unknown — restart required"
                ),
            )
        }
        // OllpRetryRequired is an internal scheduler signal and must not
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
        ErrorCode::NodeLabelLimit { node, label, limit } => (
            "ERROR",
            sqlstate::PROGRAM_LIMIT_EXCEEDED,
            crate::error_from_data_plane::node_label_limit_message(node, label, *limit),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Only a unique-key refusal is `23505`; every other constraint kind
    /// keeps its own class.
    #[test]
    fn constraint_kinds_map_to_their_own_sqlstate() {
        assert_eq!(constraint_sqlstate("unique"), sqlstate::UNIQUE_VIOLATION);
        assert_eq!(
            constraint_sqlstate("not_null"),
            sqlstate::NOT_NULL_VIOLATION
        );
        assert_eq!(
            constraint_sqlstate("generated_always"),
            sqlstate::GENERATED_ALWAYS
        );
        assert_eq!(
            constraint_sqlstate("fk_missing"),
            sqlstate::FOREIGN_KEY_VIOLATION
        );
        assert_eq!(
            constraint_sqlstate("rls_policy"),
            sqlstate::INSUFFICIENT_PRIVILEGE
        );
        assert_eq!(
            constraint_sqlstate("crdt_single_document_delta"),
            sqlstate::INTEGRITY_CONSTRAINT_VIOLATION
        );
    }

    /// A vector of the wrong width is a data exception, not a constraint.
    #[test]
    fn vector_dimension_mismatch_is_a_data_exception() {
        let code = ErrorCode::DataException {
            detail: nodedb_vector::error::VectorError::DimensionMismatch {
                expected: 3,
                got: 2,
            }
            .to_string(),
        };
        let (_, state, message) = error_code_to_sqlstate(&code);
        assert_eq!(state, sqlstate::DATA_EXCEPTION);
        assert_eq!(message, "vector dimension mismatch: expected 3, got 2");
    }

    /// A request the Data Plane rejects as malformed is a syntax error, the
    /// same SQLSTATE the Control Plane returns for it.
    #[test]
    fn a_bad_request_from_the_data_plane_is_a_syntax_error() {
        let code = ErrorCode::from(crate::Error::BadRequest {
            detail: "bad text query".into(),
        });
        let (_, state, _) = error_code_to_sqlstate(&code);
        assert_eq!(state, sqlstate::SYNTAX_ERROR);
    }
}
