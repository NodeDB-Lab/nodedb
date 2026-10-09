// SPDX-License-Identifier: BUSL-1.1

//! Data-Plane [`ErrorCode`] to public [`NodeDbError`] conversion.
//!
//! Split out of `error_from` because the match is exhaustive over every
//! Data-Plane code and would otherwise push that file well past the size
//! limit. Exhaustiveness is the point: a code that degrades to
//! `NodeDbError::internal` reaches the client as NDB-9000, where a duplicate
//! key is indistinguishable from a crashed database — so the compiler is made
//! to name every new variant here instead of a catch-all absorbing it.

use nodedb_types::error::{ErrorCode as PublicCode, NodeDbError, sqlstate};

use crate::bridge::envelope::ErrorCode;
use crate::control::server::shared::ddl::sqlstate::constraint_sqlstate;

/// Convert a deterministic Data-Plane code into the public error a client
/// can classify.
///
/// Codes with a structured public counterpart use the dedicated constructor
/// so the collection / gate travels in `ErrorDetails` as well as the message.
/// The rest go through [`NodeDbError::from_wire`], which pairs the numeric
/// code with a rendered message — the same single mapping table, not a second
/// one.
pub(crate) fn data_plane_code_to_public(code: ErrorCode) -> NodeDbError {
    match code {
        ErrorCode::DeadlineExceeded | ErrorCode::ExpiredBeforeExecution => {
            NodeDbError::deadline_exceeded()
        }
        // The Data Plane's `RejectedConstraint` carries no collection name,
        // only the constraint kind and detail — leave collection blank
        // rather than misreport the kind string as the collection.
        ErrorCode::RejectedConstraint { constraint, detail } => {
            rejected_constraint_to_public(String::new(), constraint, detail)
        }
        ErrorCode::RejectedPrevalidation { reason } => {
            NodeDbError::prevalidation_rejected("data plane", reason)
        }
        // A sync frame the validator refused is a constraint verdict on the
        // frame.
        ErrorCode::SyncRejected { violation, .. } => {
            NodeDbError::constraint_violation("", "sync", violation.to_string())
        }
        // The gate held the frame back without applying it. The sender
        // re-sends or retires it by the hold, so it presents as the
        // retriable class.
        ErrorCode::SyncNotApplied { hold, .. } => NodeDbError::from_wire(
            PublicCode::WRITE_CONFLICT,
            format!("sync frame not applied: {hold}"),
        ),
        // Nothing was applied and the identical frame is expected to succeed
        // once the transient precondition resolves, so it presents as the
        // retriable class rather than a permanent refusal.
        ErrorCode::RetryableRefusal { reason } => NodeDbError::from_wire(
            PublicCode::WRITE_CONFLICT,
            format!("write refused without applying, retry: {reason}"),
        ),
        // The core applied nothing, and another replica or a restart serves
        // the request: the retriable class.
        ErrorCode::CoreFailStopped { core_id, detail } => NodeDbError::from_wire(
            PublicCode::WRITE_CONFLICT,
            format!("core {core_id} is fail-stopped and applied nothing, retry: {detail}"),
        ),
        // The Data Plane cannot distinguish an absent collection from an
        // absent row through this code, and `document_not_found` is the
        // narrower of the two claims: it never asserts the collection is
        // gone. Both answer `is_not_found()`.
        ErrorCode::NotFound => NodeDbError::from_wire(PublicCode::DOCUMENT_NOT_FOUND, "not found"),
        // `resource` says what refused the request and why — an RLS policy on
        // a named collection, a missing grant. It lands in
        // `ErrorDetails::AuthorizationDenied { resource }`, so a client can
        // match on it instead of parsing prose.
        ErrorCode::RejectedAuthz { resource } => NodeDbError::authorization_denied(resource),
        ErrorCode::ConflictRetry => NodeDbError::write_conflict("", ""),
        ErrorCode::CrdtFrontierMismatch { .. } => NodeDbError::from_wire(
            PublicCode::WRITE_CONFLICT,
            "CRDT state changed after preview; retry the write",
        ),
        ErrorCode::ResourcesExhausted => NodeDbError::memory_exhausted("query"),
        // A dangling edge is a referential-integrity refusal, which the
        // public surface expresses as a constraint violation.
        ErrorCode::RejectedDanglingEdge { missing_node } => NodeDbError::constraint_violation(
            "",
            "foreign_key",
            format!("edge rejected: node '{missing_node}' does not exist"),
        ),
        ErrorCode::DuplicateWrite => NodeDbError::constraint_violation(
            "",
            "unique",
            "duplicate write detected via idempotency key",
        ),
        ErrorCode::AppendOnlyViolation { collection } => {
            NodeDbError::append_only_violation(collection, "UPDATE/DELETE not allowed")
        }
        ErrorCode::BalanceViolation { collection, detail } => {
            NodeDbError::balance_violation(collection, detail)
        }
        ErrorCode::PeriodLocked { collection } => {
            NodeDbError::period_locked(collection, "writes rejected")
        }
        ErrorCode::PeriodLockMisconfigured {
            collection,
            ref_table,
            status_column,
            row_identity,
        } => NodeDbError::period_lock_misconfigured(
            collection,
            ref_table,
            status_column,
            row_identity,
        ),
        ErrorCode::RetentionViolation { collection } => {
            NodeDbError::retention_violation(collection, "retention period has not expired")
        }
        ErrorCode::LegalHoldActive { collection } => {
            NodeDbError::legal_hold_active(collection, "delete rejected")
        }
        ErrorCode::StateTransitionViolation { collection, detail } => {
            NodeDbError::state_transition_violation(collection, detail)
        }
        ErrorCode::TransitionCheckViolation { collection, detail } => {
            NodeDbError::transition_check_violation(collection, detail)
        }
        ErrorCode::TypeGuardViolation { collection, detail } => {
            NodeDbError::type_guard_violation(collection, detail)
        }
        ErrorCode::TypeMismatch { collection, detail } => {
            NodeDbError::type_mismatch(collection, detail)
        }
        // The same text the SQL surfaces send, with the collection in the
        // details. RESP renders the bare Redis text from the code itself.
        ErrorCode::CounterFault { collection, fault } => {
            NodeDbError::kv_counter_fault(collection, fault.message(), fault.is_out_of_range())
        }
        ErrorCode::InsufficientBalance { collection, detail } => {
            NodeDbError::insufficient_balance(collection, detail)
        }
        ErrorCode::RateExceeded {
            gate,
            retry_after_ms,
        } => NodeDbError::rate_exceeded(gate, format!("retry after {retry_after_ms}ms")),
        ErrorCode::CollectionDraining { collection } => {
            NodeDbError::collection_draining(collection)
        }
        ErrorCode::RecursionDepthExceeded {
            cte_name,
            max_depth,
        } => NodeDbError::program_limit_exceeded(format!(
            "WITH RECURSIVE CTE '{cte_name}' exceeded max recursion depth {max_depth}; \
             add a stricter termination condition or raise max_recursion_depth"
        )),
        ErrorCode::UndefinedColumn { column } => NodeDbError::undefined_column(column),
        ErrorCode::TextColumn {
            collection,
            column,
            fault,
        } => text_column_to_public(&collection, &column, &fault),
        // `0A000` (feature_not_supported). `SQL_NOT_ENABLED` is the class
        // every bare `0A000` refusal carries.
        ErrorCode::Unsupported { detail } => {
            NodeDbError::from_wire(PublicCode::SQL_NOT_ENABLED, detail)
        }
        ErrorCode::DivisionByZero => NodeDbError::division_by_zero(),
        ErrorCode::UndefinedFunction { name } => NodeDbError::undefined_function(name),
        ErrorCode::DataException { detail } => NodeDbError::data_exception(detail),
        ErrorCode::NumericValueOutOfRange { detail } => {
            NodeDbError::numeric_value_out_of_range(detail)
        }
        ErrorCode::BadRequest { detail } => NodeDbError::bad_request(detail),
        // The public class `code_for_sqlstate` gives `22P02`, `22007`,
        // `22008` and `42804`.
        ErrorCode::InvalidTextRepresentation { detail }
        | ErrorCode::InvalidDatetimeFormat { detail }
        | ErrorCode::DatetimeFieldOverflow { detail } => NodeDbError::data_exception(detail),
        ErrorCode::DatatypeMismatch { detail } => {
            NodeDbError::from_wire(PublicCode::BAD_REQUEST, detail)
        }
        ErrorCode::TransactionRollback { detail } => NodeDbError::transaction_rollback(detail),
        ErrorCode::ActiveSqlTransaction { detail } => NodeDbError::active_sql_transaction(detail),
        ErrorCode::DependentObjectsExist { object, detail } => {
            NodeDbError::dependent_objects_exist(object, detail)
        }
        ErrorCode::UndefinedObject { object } => NodeDbError::undefined_object(object),
        ErrorCode::ObjectNotInPrerequisiteState { object, detail } => {
            NodeDbError::object_not_ready(object, detail)
        }
        // Nothing was enqueued, and the same request succeeds once capacity
        // frees: the retryable overload class.
        ErrorCode::DispatchCapacity { reason } => NodeDbError::server_overload(reason),
        ErrorCode::TxnOverlayMemoryExceeded { limit } => {
            NodeDbError::program_limit_exceeded(format!(
                "transaction staging overlay exceeded its {limit}-byte per-core budget; \
                 split the transaction into smaller batches"
            ))
        }
        ErrorCode::NodeLabelLimit { node, label, limit } => {
            NodeDbError::program_limit_exceeded(node_label_limit_message(&node, &label, limit))
        }
        // Genuinely internal: the shard is in an unknown or faulted state.
        // These are the only codes for which NDB-9000 is the truth.
        ErrorCode::Internal { detail } => NodeDbError::internal(detail),
        // The typed cause of the failed reverse write chains onto the error
        // and names itself in the message.
        ErrorCode::RollbackFailed {
            entry_index,
            detail,
            cause,
        } => {
            let cause = cause.map(|cause| data_plane_code_to_public(*cause));
            let because = cause
                .as_ref()
                .map(|cause| format!(" ({cause})"))
                .unwrap_or_default();
            let error = NodeDbError::internal(format!(
                "transaction rollback failed at undo entry {entry_index}: {detail}{because}; \
                 shard state is unknown — restart required"
            ));
            match cause {
                Some(cause) => error.with_cause(cause),
                None => error,
            }
        }
        // A scheduler signal that reached a client: nothing was written, and
        // the retry that the signal asks for succeeds, so it takes the
        // retriable class the SQL surfaces send (`40001`).
        ErrorCode::OllpRetryRequired => NodeDbError::from_wire(
            PublicCode::WRITE_CONFLICT,
            "optimistic predicate retry required; retry the transaction",
        ),
    }
}

/// The public error for a rejected constraint of kind `constraint`.
///
/// The class follows the SQLSTATE the SQL surfaces send for the kind
/// ([`constraint_sqlstate`]): an RLS or permission refusal is an
/// authorization denial (`42501`), a write to a generated column is a bad
/// request (`428C9`), and every other kind is a constraint violation (`23`).
/// Shared by the Data-Plane code and the Control-Plane variant, so both
/// classify one kind the same way.
pub(crate) fn rejected_constraint_to_public(
    collection: String,
    constraint: String,
    detail: String,
) -> NodeDbError {
    match constraint_sqlstate(&constraint) {
        sqlstate::INSUFFICIENT_PRIVILEGE => NodeDbError::authorization_denied(detail),
        sqlstate::GENERATED_ALWAYS => NodeDbError::bad_request(detail),
        _ => NodeDbError::constraint_violation(collection, constraint, detail),
    }
}

/// The public error of a full-text column fault. A field that does not exist
/// as text is an undefined column (`42703`). An argument that is not a text
/// column is a type mismatch (class `42`). Shared by the Data-Plane code and
/// the Control-Plane variant, so both render one message.
pub(crate) fn text_column_to_public(
    collection: &str,
    column: &str,
    fault: &nodedb_types::text_search::TextColumnFault,
) -> NodeDbError {
    use nodedb_types::text_search::TextColumnFault;
    let code = match fault {
        TextColumnFault::Undeclared | TextColumnFault::NotIndexed => PublicCode::UNDEFINED_COLUMN,
        TextColumnFault::NotText { .. } | TextColumnFault::NotAColumn => PublicCode::TYPE_MISMATCH,
    };
    NodeDbError::from_wire(
        code,
        format!("column \"{column}\" of collection \"{collection}\" {fault}"),
    )
}

/// The message of a refused node-label write. Shared by the public error and
/// the SQLSTATE table, so every protocol renders one text.
pub(crate) fn node_label_limit_message(node: &str, label: &str, limit: usize) -> String {
    format!(
        "label \"{label}\" on node \"{node}\" exceeds the {limit} distinct node-label \
         limit of the graph partition; no label of the statement was applied"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::envelope::CounterFault;

    #[test]
    fn constraint_code_classifies_as_constraint_violation() {
        let e = data_plane_code_to_public(ErrorCode::RejectedConstraint {
            constraint: "unique".into(),
            detail: "unique index 'idx_users_email' violation".into(),
        });
        assert!(e.is_constraint_violation());
        assert_eq!(e.code(), PublicCode::CONSTRAINT_VIOLATION);
    }

    #[test]
    fn not_found_code_classifies_as_not_found() {
        assert!(data_plane_code_to_public(ErrorCode::NotFound).is_not_found());
    }

    #[test]
    fn authz_and_rate_codes_keep_their_categories() {
        assert!(
            data_plane_code_to_public(ErrorCode::RejectedAuthz {
                resource: "RLS write policy on 'orders'".into(),
            })
            .is_auth_denied()
        );
        assert!(
            data_plane_code_to_public(ErrorCode::RateExceeded {
                gate: "login".into(),
                retry_after_ms: 500,
            })
            .is_rate_exceeded()
        );
    }

    #[test]
    fn counter_fault_carries_the_collection() {
        let e = data_plane_code_to_public(ErrorCode::CounterFault {
            collection: "counters".into(),
            fault: CounterFault::NotAnInteger,
        });
        assert_eq!(e.code(), PublicCode::DATA_EXCEPTION);
        assert_eq!(
            e.message(),
            "value is not an integer or out of range on counters"
        );
        assert_eq!(
            e.details(),
            &nodedb_types::error::ErrorDetails::DataException {
                detail: "value is not an integer or out of range on counters".into()
            }
        );

        let e = data_plane_code_to_public(ErrorCode::CounterFault {
            collection: "counters".into(),
            fault: CounterFault::IntegerOverflow,
        });
        assert_eq!(e.code(), PublicCode::OVERFLOW);
        assert_eq!(
            e.message(),
            "increment or decrement would overflow on counters"
        );
        assert_eq!(
            e.details(),
            &nodedb_types::error::ErrorDetails::Overflow {
                collection: "counters".into()
            }
        );
    }

    #[test]
    fn internal_stays_internal() {
        let e = data_plane_code_to_public(ErrorCode::Internal {
            detail: "io_uring submission failed".into(),
        });
        assert!(e.is_internal());
        assert_eq!(e.code(), PublicCode::INTERNAL);
    }
}
