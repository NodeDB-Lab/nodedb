// SPDX-License-Identifier: BUSL-1.1

//! Classification of a Data-Plane failure into "the write was refused and
//! nothing was applied" versus "the write may in fact have landed".
//!
//! The write funnel appends a write's redo record BEFORE the Data Plane decides
//! whether to accept it, so a refusal always arrives with the record already in
//! the log. Left alone, restart replay re-applies it and a write the server told
//! the client it refused comes back. The cure is a `WriteAborted` marker naming
//! the forward record's LSN — but emitting one for a failure whose write
//! actually landed is strictly worse than the bug: recovery would then DELETE
//! committed data.
//!
//! So the predicate below is deliberately one-directional. A code earns an abort
//! only when the code itself is proof that nothing was installed. Anything whose
//! outcome is ambiguous — the shard state is unknown, the failure could have
//! occurred part way through apply, or the code is an opaque catch-all — keeps
//! the current behaviour and replays. That is the safe side of the trade: a
//! refused write that survives a restart is a bug, a committed write erased by
//! recovery is data loss.
//!
//! The match is exhaustive on purpose. A new [`ErrorCode`] must be classified by
//! whoever adds it, not silently inherit either answer.
//!
//! `minted::resolve_on_response` is the one place that acts on that
//! verdict. Every write that mints a record for a Data-Plane dispatch
//! resolves its records there.

use crate::bridge::envelope::ErrorCode;

/// Whether a replicated proposal refused with `code` is refused for good: a
/// redelivery of the same entry against the same state refuses it again.
///
/// A verdict that depends on this node's momentary load or on a transient
/// precondition is not final: another replica can apply the same entry, and
/// a redelivery here can too. That covers admission and capacity verdicts,
/// a task that expired before it started, concurrency retries, the staging
/// byte budget, and `RetryableRefusal`,
/// which a committed-redo apply answers with after it rolled a failed
/// install back.
pub(crate) fn refusal_is_final(code: &ErrorCode) -> bool {
    write_definitely_not_applied(code)
        && !matches!(
            code,
            ErrorCode::RetryableRefusal { .. }
                | ErrorCode::RateExceeded { .. }
                | ErrorCode::CollectionDraining { .. }
                | ErrorCode::DispatchCapacity { .. }
                | ErrorCode::ExpiredBeforeExecution
                | ErrorCode::ConflictRetry
                | ErrorCode::OllpRetryRequired
                | ErrorCode::TxnOverlayMemoryExceeded { .. }
        )
}

/// Whether a committed proposal's apply `error` is a final refusal: the
/// entry's outcome, which a redelivery must answer with and never apply.
pub(crate) fn error_is_final_refusal(error: &crate::Error) -> bool {
    matches!(error, crate::Error::DataPlane(code) if refusal_is_final(code))
}

/// Whether `code` proves the write was refused without applying anything.
///
/// `false` means "not established", not "the write applied".
pub(crate) fn write_definitely_not_applied(code: &ErrorCode) -> bool {
    match code {
        // Validation and policy verdicts. Each is decided against the row
        // before any engine state is mutated, and the handler returns the
        // refusal instead of installing the write.
        ErrorCode::RejectedConstraint { .. }
        | ErrorCode::RejectedPrevalidation { .. }
        // The sync gate refused the frame before its delta installed.
        | ErrorCode::SyncRejected { .. }
        | ErrorCode::RejectedAuthz { .. }
        | ErrorCode::RejectedDanglingEdge { .. }
        | ErrorCode::AppendOnlyViolation { .. }
        | ErrorCode::BalanceViolation { .. }
        | ErrorCode::PeriodLocked { .. }
        | ErrorCode::PeriodLockMisconfigured { .. }
        | ErrorCode::RetentionViolation { .. }
        | ErrorCode::LegalHoldActive { .. }
        | ErrorCode::StateTransitionViolation { .. }
        | ErrorCode::TransitionCheckViolation { .. }
        | ErrorCode::TypeGuardViolation { .. }
        | ErrorCode::TypeMismatch { .. }
        | ErrorCode::CounterFault { .. }
        | ErrorCode::InsufficientBalance { .. }
        // Admission verdicts: the request never reached the mutation at all.
        | ErrorCode::RateExceeded { .. }
        | ErrorCode::CollectionDraining { .. }
        | ErrorCode::DispatchCapacity { .. }
        | ErrorCode::Unsupported { .. }
        // The deadline passed before the core started the task.
        | ErrorCode::ExpiredBeforeExecution
        // The target row or collection did not exist, so the write had nothing
        // to mutate.
        | ErrorCode::NotFound
        // Documented as applying nothing: the identical frame is expected to be
        // re-sent, and the retry carries its own record.
        | ErrorCode::RetryableRefusal { .. }
        // Concurrency verdicts that abort the whole attempt before install.
        | ErrorCode::ConflictRetry
        | ErrorCode::OllpRetryRequired
        // The staging overlay hit its byte budget, so the transaction's writes
        // were discarded from the overlay and never installed.
        | ErrorCode::TxnOverlayMemoryExceeded { .. }
        // Expression evaluation failed before producing a value to write.
        | ErrorCode::DivisionByZero
        | ErrorCode::UndefinedColumn { .. } => true,

        // NOT established — every one of these can be reported by a request
        // whose write reached, or may have reached, engine state. Emitting an
        // abort for one risks deleting a committed write on recovery.
        //
        // * `DeadlineExceeded` — the Data Plane may still be applying.
        // * `RollbackFailed` — documented as leaving shard state unknown; the
        //   forward record is precisely what recovery needs.
        // * `ResourcesExhausted` — memory can run out part way through apply.
        // * `Internal` — opaque; covers io_uring and corruption faults that can
        //   strike mid-write.
        // * `CrdtFrontierMismatch` — a mismatch detected against an applied
        //   Loro frontier; whether the local doc absorbed the delta is not
        //   decidable from the code.
        // * `FanOutExceeded` / `RecursionDepthExceeded` — a limit tripped part
        //   way through a multi-step plan, which may already have written rows.
        // * `DuplicateWrite` — the idempotency gate fired because the write
        //   ALREADY applied under the original request; nothing to undo, and
        //   the duplicate record replays to the same state.
        ErrorCode::DeadlineExceeded
        | ErrorCode::RollbackFailed { .. }
        | ErrorCode::ResourcesExhausted
        | ErrorCode::Internal { .. }
        | ErrorCode::CrdtFrontierMismatch { .. }
        | ErrorCode::FanOutExceeded
        | ErrorCode::RecursionDepthExceeded { .. }
        | ErrorCode::DuplicateWrite => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn policy_and_constraint_verdicts_abort_the_record() {
        assert!(write_definitely_not_applied(&ErrorCode::RejectedAuthz {
            resource: "RLS write policy on 'orders' rejected the row".into(),
        }));
        assert!(write_definitely_not_applied(
            &ErrorCode::RejectedConstraint {
                constraint: "unique".into(),
                detail: "duplicate key".into(),
            }
        ));
        assert!(write_definitely_not_applied(
            &ErrorCode::TypeGuardViolation {
                collection: "orders".into(),
                detail: "qty must be int".into(),
            }
        ));
        assert!(write_definitely_not_applied(
            &ErrorCode::AppendOnlyViolation {
                collection: "ledger".into(),
            }
        ));
    }

    /// A dispatcher capacity refusal enqueued nothing, so the record aborts.
    #[test]
    fn dispatch_capacity_refusal_aborts_the_record() {
        assert!(write_definitely_not_applied(&ErrorCode::DispatchCapacity {
            reason: "core 0 queue is full at 64 requests".into(),
        }));
    }

    /// A task that expired before its core started it ran nothing, so the
    /// record aborts. A redelivery can still run it, so the refusal is not
    /// final.
    #[test]
    fn a_task_that_never_started_aborts_the_record_but_is_not_final() {
        assert!(write_definitely_not_applied(
            &ErrorCode::ExpiredBeforeExecution
        ));
        assert!(!refusal_is_final(&ErrorCode::ExpiredBeforeExecution));
    }

    /// The asymmetry that keeps this safe: an ambiguous outcome must never
    /// produce an abort, because the write it would erase may have landed.
    #[test]
    fn ambiguous_outcomes_never_abort_the_record() {
        assert!(!write_definitely_not_applied(&ErrorCode::DeadlineExceeded));
        assert!(!write_definitely_not_applied(&ErrorCode::RollbackFailed {
            entry_index: 3,
            detail: "undo failed".into(),
        }));
        assert!(!write_definitely_not_applied(
            &ErrorCode::ResourcesExhausted
        ));
        assert!(!write_definitely_not_applied(&ErrorCode::Internal {
            detail: "io_uring".into(),
        }));
        assert!(!write_definitely_not_applied(&ErrorCode::DuplicateWrite));
    }

    #[test]
    fn a_constraint_verdict_is_final_and_a_retryable_one_is_not() {
        assert!(refusal_is_final(&ErrorCode::RejectedPrevalidation {
            reason: "sub-record does not decode".into(),
        }));
        assert!(!refusal_is_final(&ErrorCode::RetryableRefusal {
            reason: "install rolled back".into(),
        }));
        assert!(!refusal_is_final(&ErrorCode::DispatchCapacity {
            reason: "core 0 queue is full".into(),
        }));
        assert!(!refusal_is_final(&ErrorCode::Internal {
            detail: "io_uring".into(),
        }));
    }
}
