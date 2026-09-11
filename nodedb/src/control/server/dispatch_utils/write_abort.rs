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
//! [`abort_refused_write`] is the one place that acts on that verdict, and the
//! write funnel is its only caller — every `AppendHere` write in every engine,
//! including the Raft apply loop, passes through there.

use crate::bridge::envelope::{ErrorCode, Response};
use crate::control::state::SharedState;
use crate::types::{DatabaseId, Lsn, TenantId, VShardId};

/// Identity of the forward record an abort marker would name.
pub(crate) struct AbortTarget {
    pub tenant_id: TenantId,
    pub database_id: DatabaseId,
    pub vshard_id: VShardId,
    /// The forward write's redo LSN, if one was minted at all.
    pub wal_lsn: Option<Lsn>,
    /// Whether the write funnel appended the forward record. A caller that
    /// recorded durability elsewhere owns the undo semantics of its own record.
    pub appends_here: bool,
}

/// Cancel a forward write record the Data Plane refused.
///
/// The write funnel appends the redo record before the Data Plane has decided,
/// so a refusal arrives with the record already in the log and restart replay
/// would re-apply the very write the client was told was rejected. This writes a
/// `WriteAborted` marker naming that record, then waits for it to be fsynced
/// BEFORE the error is returned: the refusal path performs no fsync of its own,
/// so an abort left buffered is volatile while the forward record may already be
/// durable via a concurrent writer's group commit.
///
/// Cost: a rejected write now pays a WAL append plus an fsync wait it did not
/// pay before. That is a deliberate trade of refusal latency for the guarantee
/// that a refusal, once acknowledged, stays refused.
///
/// **Known residual, not closed:** a crash BEFORE the abort record is durable
/// can still resurrect the write, because the forward record's durability is not
/// gated on the verdict. Closing that window means holding the per-key order
/// guard across the whole Data-Plane round trip, which would serialize same-key
/// writes on every engine's common path. What this guarantees is the ACKED case:
/// once the client has been told the write was refused, a restart cannot make it
/// appear.
///
/// An append or fsync failure here is propagated, not logged: continuing would
/// return the refusal while leaving the forward record replayable, which is
/// exactly the bug this exists to prevent.
pub(crate) async fn abort_refused_write(
    shared: &SharedState,
    target: AbortTarget,
    response: &Response,
) -> crate::Result<()> {
    if !target.appends_here {
        return Ok(());
    }
    let Some(wal_lsn) = target.wal_lsn else {
        return Ok(());
    };
    // A rejection is only cancellable when the verdict itself proves nothing
    // was installed. An ambiguous failure keeps its forward record, because
    // erasing a write that actually landed is worse than replaying one that
    // did not.
    let Some(code) = response.error_code.as_deref() else {
        return Ok(());
    };
    if !write_definitely_not_applied(code) {
        return Ok(());
    }

    let abort_lsn = shared.wal.append_write_aborted(
        target.tenant_id,
        target.vshard_id,
        target.database_id,
        wal_lsn,
    )?;
    shared.wal.wait_durable(abort_lsn).await?;
    tracing::debug!(
        aborted_lsn = wal_lsn.as_u64(),
        abort_lsn = abort_lsn.as_u64(),
        "refused write cancelled in the WAL"
    );
    Ok(())
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
        | ErrorCode::OverflowError { .. }
        | ErrorCode::InsufficientBalance { .. }
        // Admission verdicts: the request never reached the mutation at all.
        | ErrorCode::RateExceeded { .. }
        | ErrorCode::CollectionDraining { .. }
        | ErrorCode::Unsupported { .. }
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
}
