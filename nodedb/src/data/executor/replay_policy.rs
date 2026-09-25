// SPDX-License-Identifier: BUSL-1.1

//! How a replay arm settles a per-record decision in each of its two passes.
//!
//! The per-engine replay arms run in two passes:
//!
//! * **Restart replay** rebuilds engine state from the WAL at boot.
//! * **The committed-redo apply** (`handlers::transaction::redo_apply`)
//!   installs one committed transaction's redo record on a live core. It
//!   opens a redo-apply scope for the duration of the record.
//!
//! The two passes differ in four decisions. Every arm routes them through
//! this file so the difference lives in one place.
//!
//! ## Validate, then install
//!
//! A committed-redo apply runs the arms twice over the record. In the
//! validate pass an arm decodes, routes and checks each sub-record it owns,
//! then claims it and skips the write (`claim_for_validation`). In the
//! install pass an arm writes, and records the undo entries that reverse the
//! write (`record_redo_undo`). A failure in the install pass rolls the
//! record back from them, so every replica holds all of the record or none
//! of it. Restart replay runs neither.
//!
//! ## Watermark skips
//!
//! A restart skips a record at or below an engine watermark: a restored
//! checkpoint's LSN, a flushed timeseries partition's LSN, an array
//! manifest's durable LSN. Each watermark says what engine state already
//! holds before replay starts. An online apply installs a record exactly once,
//! and the apply loop's proposal ledger guarantees that. A live flush or
//! checkpoint can mint a watermark above the LSN of a record that has not
//! applied yet. Such a watermark says nothing about that record, so an online
//! apply never consults one.
//!
//! ## Records that cannot be applied
//!
//! Restart policy: a committed record an arm cannot decode, cannot route, or
//! whose handler rejects stops recovery (`replay_abort::abort_replay`).
//! Starting with a hole in the replayed suffix is worse than not starting.
//! Online policy: the error fails the apply response. The funnel then refuses
//! or aborts the entry, and the process keeps serving.
//!
//! ## Records restart skips with a warning
//!
//! Restart policy: some arms skip a record whose live re-execution fails, log
//! it with its LSN, and continue booting. Each site states why a skip is the
//! chosen outcome there. Examples are a record inapplicable to an index
//! rebuilt at a new dimension, and a resolved-row drift that replay has no
//! caller to retry against. Online policy: the same failure is an error of
//! this apply and fails its response.

use crate::bridge::envelope::ErrorCode;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::transaction::redo_apply::RedoApplyPass;
use crate::data::executor::handlers::transaction::undo::UndoEntry;
use crate::data::executor::replay_abort::abort_replay;

impl CoreLoop {
    /// Whether a committed-redo apply is driving the replay arms.
    pub(in crate::data::executor) fn applying_committed_redo(&self) -> bool {
        self.redo_apply.scope.is_some()
    }

    /// Whether the validate pass of a committed-redo apply is driving the
    /// replay arms. That pass writes nothing.
    pub(in crate::data::executor) fn validating_committed_redo(&self) -> bool {
        self.redo_apply
            .scope
            .as_ref()
            .is_some_and(|scope| scope.pass == RedoApplyPass::Validate)
    }

    /// Whether the arm must skip the write it reached because the apply is
    /// validating. Claims the sub-record when it is. Call once per
    /// sub-record, after its decode, routing and checks.
    pub(in crate::data::executor) fn claim_for_validation(&mut self) -> bool {
        match self.redo_apply.scope.as_mut() {
            Some(scope) if scope.pass == RedoApplyPass::Validate => {
                scope.claimed += 1;
                true
            }
            _ => false,
        }
    }

    /// Whether the arm must record undo entries for the writes it makes:
    /// `true` only in the install pass of a committed-redo apply.
    pub(in crate::data::executor) fn recording_redo_undo(&self) -> bool {
        self.redo_apply
            .scope
            .as_ref()
            .is_some_and(|scope| scope.pass == RedoApplyPass::Install)
    }

    /// Record the entries that reverse one write of the install pass. Restart
    /// replay records nothing.
    pub(in crate::data::executor) fn record_redo_undo(
        &mut self,
        entries: impl IntoIterator<Item = UndoEntry>,
    ) {
        if let Some(scope) = self.redo_apply.scope.as_mut()
            && scope.pass == RedoApplyPass::Install
        {
            scope.undo.extend(entries);
        }
    }

    /// Record the undo entries a pre-image capture produced, or keep its
    /// error on the apply. Returns whether the write may proceed: `false`
    /// when the pre-image could not be read, so the write is skipped and the
    /// install pass rolls back.
    pub(in crate::data::executor) fn record_redo_capture<I>(
        &mut self,
        captured: crate::Result<I>,
    ) -> bool
    where
        I: IntoIterator<Item = UndoEntry>,
    {
        match captured {
            Ok(entries) => {
                self.record_redo_undo(entries);
                true
            }
            Err(error) => {
                if let Some(scope) = self.redo_apply.scope.as_mut() {
                    scope.record_error(error);
                }
                false
            }
        }
    }

    /// Whether an engine watermark skips a record. `covered` is the arm's
    /// own watermark test. Always `false` during a committed-redo apply.
    pub(in crate::data::executor) fn replay_watermark_skips(&self, covered: bool) -> bool {
        covered && !self.applying_committed_redo()
    }

    /// Whether restart replay skips a vector-index record (HNSW, multi-vector,
    /// direct row): the restored vector checkpoint holds it. Every vector
    /// skip site asks here, and the answer is `ReplayStamp::skips`.
    pub(in crate::data::executor) fn vector_replay_skips(&self, record_lsn: u64) -> bool {
        self.replay_watermark_skips(self.floors.replay_floors.vector.covers(record_lsn))
    }

    /// Whether restart replay skips a sparse-vector record: the restored
    /// sparse-vector checkpoint holds it.
    pub(in crate::data::executor) fn sparse_vector_replay_skips(&self, record_lsn: u64) -> bool {
        self.replay_watermark_skips(self.floors.replay_floors.sparse_vector.covers(record_lsn))
    }

    /// A committed record cannot be applied. Restart replay stops recovery
    /// and never returns. A committed-redo apply records the error for its
    /// response and returns, and the caller skips the record.
    pub(in crate::data::executor) fn replay_record_unapplied(
        &mut self,
        engine: &str,
        stage: &str,
        record_lsn: u64,
        detail: &str,
    ) {
        match self.redo_apply.scope.as_mut() {
            Some(scope) => scope.record_error(ErrorCode::Internal {
                detail: format!(
                    "committed redo record at lsn {record_lsn} cannot be applied ({engine} \
                     {stage}): {detail}"
                ),
            }),
            None => abort_replay(engine, stage, self.core_id, record_lsn, detail),
        }
    }

    /// A handler rejected a record. Restart replay logs it with its LSN and
    /// skips it. A committed-redo apply records `error` for its response.
    /// `error` is the handler's own error code when it reported one.
    pub(in crate::data::executor) fn replay_record_rejected(
        &mut self,
        engine: &str,
        record_lsn: u64,
        error: Option<Box<ErrorCode>>,
        detail: &str,
    ) {
        match self.redo_apply.scope.as_mut() {
            Some(scope) => scope.record_error(error.map_or_else(
                || ErrorCode::Internal {
                    detail: format!(
                        "committed redo record at lsn {record_lsn} was rejected ({engine}): \
                         {detail}"
                    ),
                },
                |error| *error,
            )),
            None => tracing::warn!(
                core = self.core_id,
                engine,
                lsn = record_lsn,
                error = ?error,
                "{detail}; skipping WAL record"
            ),
        }
    }
}
