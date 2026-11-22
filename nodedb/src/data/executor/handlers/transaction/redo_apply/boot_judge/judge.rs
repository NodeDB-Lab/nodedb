// SPDX-License-Identifier: BUSL-1.1

//! Judge one committed record whose install has no outcome, as its live
//! install would have.
//!
//! The judge runs the live install's validate: the whole-record validate,
//! or the streamed validate of a stream marker's content. It reads the
//! engine state replay holds at the record's place, and its commit boundary
//! from the WAL. It writes nothing.
//!
//! The verdict follows the live funnel (`minted::resolve_on_response`): a
//! refusal that proves nothing applied cancels the record, and its marker
//! keeps the proposal key when the refusal is final. An ambiguous failure
//! keeps the record, and replay applies it. A refusal that comes from this
//! node, not the record, such as a store that does not read, decides
//! nothing: it stops the boot.

use std::time::{Duration, Instant};

use nodedb_wal::WalRecord;

use crate::bridge::envelope::{Admission, ErrorCode, PhysicalPlan, Priority, Request};
use crate::control::server::dispatch_utils::{refusal_is_final, write_definitely_not_applied};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::fail_stop::FailStopCause;
use crate::data::executor::task::ExecutionTask;
use crate::types::{DatabaseId, Lsn, ReadConsistency, RequestId, TenantId, TraceId, VShardId};
use crate::wal::{BootVerdictOutcome, JudgedRedo, RedoJudgeError, RedoRecord, RedoStreamRef};

use super::super::entry::RedoSnapshot;
use super::super::passes::final_refusal;
use super::super::stream_validate::{StreamLimits, StreamValidateInput};
use super::super::whole_validate::WholeValidateInput;

/// How long the judge's task lives. Boot runs it at once, so no deadline
/// expires it.
const JUDGE_DEADLINE: Duration = Duration::from_secs(24 * 60 * 60);

impl CoreLoop {
    /// Judge the committed record `record`, which `judged` names.
    pub(super) fn judge_boot_redo(
        &mut self,
        record: &WalRecord,
        judged: &JudgedRedo,
    ) -> crate::Result<BootVerdictOutcome> {
        // A record that does not decode is refused alike on every replica.
        let decoded = RedoRecord::from_bytes(&record.payload).and_then(|redo| {
            let reference = RedoStreamRef::of_marker(&redo)?;
            Ok((redo, reference))
        });
        let (redo, reference) = match decoded {
            Ok(decoded) => decoded,
            Err(error) => return self.close_refusal(judged.lsn, &final_refusal(error.into())),
        };
        let task = judge_task(record, judged);
        // Each value the validate reads comes from the stores replay wrote.
        self.forget_newest_ordinals();
        // The validate reads the ordinal the live task carried, as the
        // task's execution sets it.
        let previous = std::mem::replace(
            &mut self.apply_scope.replicated_ordinal,
            task.request.system_ordinal,
        );
        let verdict = self.judge_validate(JudgeInput {
            task: &task,
            redo: &redo,
            redo_bytes: &record.payload,
            reference: reference.as_ref(),
            judged,
        });
        self.apply_scope.replicated_ordinal = previous;
        match verdict {
            Ok(()) => Ok(BootVerdictOutcome::Passed),
            Err(code) => self.close_refusal(judged.lsn, &code),
        }
    }

    /// The verdict a refusal with `code` of the record at `lsn` gives. A
    /// refusal this node caused decides nothing: the core fail-stops here,
    /// the one site that finds it, and the boot stops.
    fn close_refusal(&mut self, lsn: u64, code: &ErrorCode) -> crate::Result<BootVerdictOutcome> {
        match outcome_of_refusal(code) {
            Some(outcome) => Ok(outcome),
            None => {
                let error = RedoJudgeError::Unreadable {
                    lsn,
                    detail: format!("{code:?}"),
                };
                self.fail_stop_core(FailStopCause::BootJudgeUnreadable, &error.to_string());
                Err(error.into())
            }
        }
    }

    /// Run the live validate of the record `input` names. The record is
    /// judged under the pins its WAL copy carries, and its judged view
    /// drops when the validate ends.
    fn judge_validate(&mut self, input: JudgeInput<'_>) -> Result<(), ErrorCode> {
        let held = self.hold_judging();
        let verdict = self.judge_validate_record(input);
        self.restore_judging(held);
        verdict
    }

    /// [`Self::judge_validate`] inside the record's judged scope.
    fn judge_validate_record(&mut self, input: JudgeInput<'_>) -> Result<(), ErrorCode> {
        let JudgeInput {
            task,
            redo,
            redo_bytes,
            reference,
            judged,
        } = input;
        let tid = task.request.tenant_id.as_u64();
        let lsn = Lsn::new(judged.lsn);
        let boundary = &judged.boundary;
        // The instant the funnel fixed when it appended the record.
        let judged_at_ms = boundary.judged_at_ms;
        match reference {
            Some(reference) => {
                let input = StreamValidateInput {
                    task,
                    tid,
                    marker: redo,
                    reference,
                    origin: boundary.origin,
                    snapshot: boundary.snapshot_ordinal,
                    judged_at_ms,
                    lsn,
                    limits: StreamLimits {
                        batch_bytes: self.redo_apply.stream_batch_bytes,
                        spill_threshold: self.query_tuning.max_txn_overlay_bytes,
                    },
                };
                self.validate_redo_stream(&input).map(drop)
            }
            None => self
                .validate_whole_redo(&WholeValidateInput {
                    task,
                    tid,
                    lsn,
                    redo,
                    redo_bytes,
                    sum_targets: &redo.sum_targets,
                    pins: &redo.pins,
                    origin: boundary.origin,
                    snapshot: boundary.snapshot_ordinal.map(|ordinal| RedoSnapshot {
                        reads: &boundary.reads,
                        ordinal,
                    }),
                    judged_at_ms,
                })
                .map(drop),
        }
    }
}

/// What the judge validates: the record, its task and its boundary.
struct JudgeInput<'a> {
    task: &'a ExecutionTask,
    redo: &'a RedoRecord,
    /// The record's WAL payload, which the replay arms decode.
    redo_bytes: &'a [u8],
    /// The stream a marker names. `None` for a whole record.
    reference: Option<&'a RedoStreamRef>,
    judged: &'a JudgedRedo,
}

/// The outcome of a refusal with `code`, as the live funnel closes it.
/// `None` for a refusal this node caused.
fn outcome_of_refusal(code: &ErrorCode) -> Option<BootVerdictOutcome> {
    if node_local(code) {
        return None;
    }
    if write_definitely_not_applied(code) {
        return Some(BootVerdictOutcome::Refused {
            final_refusal: refusal_is_final(code),
        });
    }
    Some(BootVerdictOutcome::Passed)
}

/// Whether `code` reports a failure of this node, not a verdict on the
/// record: a store or a stream that does not read, a core that stopped.
fn node_local(code: &ErrorCode) -> bool {
    matches!(
        code,
        ErrorCode::RetryableRefusal { .. } | ErrorCode::CoreFailStopped { .. }
    )
}

/// The task the live install of `record` ran with, rebuilt from its WAL
/// header and commit boundary.
fn judge_task(record: &WalRecord, judged: &JudgedRedo) -> ExecutionTask {
    let header = &record.header;
    let boundary = &judged.boundary;
    let system_ordinal =
        (header.system_ordinal != nodedb_wal::NO_SYSTEM_ORDINAL).then_some(header.system_ordinal);
    let plan = PhysicalPlan::Meta(
        nodedb_physical::physical_plan::MetaOp::ApplyTransactionRedo {
            redo: record.payload.clone(),
            collections: Vec::new(),
            sum_targets: Vec::new(),
            origin: boundary.origin,
            calvin: None,
            reads: boundary.reads.clone(),
            snapshot_ordinal: boundary.snapshot_ordinal,
            pins: Default::default(),
        },
    );
    ExecutionTask::new(Request {
        request_id: RequestId::new(0),
        tenant_id: TenantId::new(header.tenant_id),
        database_id: DatabaseId::new(header.database_id),
        vshard_id: VShardId::new(header.vshard_id),
        // A boot judge replays a redo and orders against no client connection.
        stream: crate::bridge::envelope::RequestStream::Internal,
        plan,
        deadline: Instant::now() + JUDGE_DEADLINE,
        priority: Priority::Normal,
        trace_id: TraceId::ZERO,
        consistency: ReadConsistency::Strong,
        idempotency_key: None,
        event_source: judged.event_source,
        user_roles: Vec::new(),
        user_id: None,
        statement_digest: None,
        txn_id: None,
        wal_lsn: Some(Lsn::new(header.lsn)),
        resolved_now_ms: u64::try_from(boundary.judged_at_ms).ok(),
        commit_hlc: (header.commit_hlc != 0).then_some(header.commit_hlc),
        system_ordinal,
        config_pins: None,
        admission: Admission::Admitted,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A refusal that proves nothing applied cancels the record, final or
    /// not, as the live funnel cancels it. An ambiguous failure keeps it.
    /// A refusal of this node decides nothing.
    #[test]
    fn the_verdict_follows_the_live_funnel() {
        let constraint = ErrorCode::RejectedConstraint {
            constraint: "unique".into(),
            detail: "duplicate key".into(),
        };
        assert_eq!(
            outcome_of_refusal(&constraint),
            Some(BootVerdictOutcome::Refused {
                final_refusal: true
            })
        );
        let conflict = ErrorCode::SerializationConflict {
            detail: "moved".into(),
        };
        assert_eq!(
            outcome_of_refusal(&conflict),
            Some(BootVerdictOutcome::Refused {
                final_refusal: false
            })
        );
        let internal = ErrorCode::Internal {
            detail: "io".into(),
        };
        assert_eq!(
            outcome_of_refusal(&internal),
            Some(BootVerdictOutcome::Passed)
        );
        let unreadable = ErrorCode::RetryableRefusal {
            reason: "chunks unreadable".into(),
        };
        assert_eq!(outcome_of_refusal(&unreadable), None);
    }
}
