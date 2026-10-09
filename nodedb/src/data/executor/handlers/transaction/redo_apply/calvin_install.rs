// SPDX-License-Identifier: BUSL-1.1

//! The install of a committed Calvin slice from its stamped redo entry.
//!
//! Every replica runs it from the vShard's data-group log, in log order. The
//! install is the committed-redo install every transaction takes. Around it
//! the slice adds:
//!
//! 1. the slice's ordinal on the core clock, so a later local write stamps
//!    above the record's graph versions;
//! 2. the end of the slice's staged state on a core that staged it: the
//!    staged entry and its synthetic overlay go;
//! 3. the reply the leader's stage decided, rendered at the epoch instant
//!    from the entry's [`CalvinReplySpec`], so every replica answers the same
//!    bytes.
//!
//! The fold target rows stay in `Response::write_set`, so the funnel
//! journals them with the record.

use nodedb_physical::physical_plan::{CalvinInstall, RedoOrigin};
use tracing::info_span;

use crate::bridge::envelope::{Payload, Response, Status};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::control::calvin_reply::CalvinReply;
use crate::data::executor::task::ExecutionTask;

use super::entry::CommittedRedo;

impl CoreLoop {
    /// Install the committed Calvin slice `install` names from its redo
    /// record `committed`, and answer the slice's reply.
    ///
    /// A refused install rolled every write back and leaves any staged entry
    /// in place. A reply that fails to render leaves the install `Ok`, since
    /// the record installed. Its error travels in `error_code`.
    pub(in crate::data::executor) fn install_calvin_redo(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        committed: CommittedRedo<'_>,
        origin: RedoOrigin,
        install: &CalvinInstall,
    ) -> Response {
        let CalvinInstall {
            epoch,
            position,
            epoch_system_ms,
            reply,
        } = install;
        let (epoch, position, epoch_system_ms) = (*epoch, *position, *epoch_system_ms);
        let vshard_id = task.request.vshard_id.as_u32();
        let _apply_span = info_span!(
            "executor_apply",
            epoch,
            position,
            vshard = vshard_id,
            tenant_id = tid,
            trace_id = ?task.request.trace_id,
        )
        .entered();
        // The record's graph versions stamp at least the slice's ordinal, so
        // a later local write stamps above them.
        let txn_ordinal = nodedb_types::calvin_txn_ordinal(epoch_system_ms, position);
        self.hlc.update_from_remote(txn_ordinal);

        // What each resolved timeseries batch of the record stored: the
        // reply answers the install's counts and stored rows.
        let mut ts_installs = Vec::new();
        let mut response =
            self.install_committed_redo_into(task, tid, committed, origin, &mut ts_installs);
        if response.status != Status::Ok {
            return response;
        }
        // The record installed: a staged entry of the slice and its overlay
        // are spent. Writes waiting on the rows the entry owned run next.
        let key = (epoch, position, vshard_id);
        if self.calvin.commit_pending.remove(&key).is_some() {
            self.calvin.fence.note_resolved(key, task.wal_lsn());
        }
        self.drop_calvin_synthetic_overlay(epoch, position, vshard_id);

        let prev_epoch_ms = self.epoch_system_ms;
        self.epoch_system_ms = Some(epoch_system_ms);
        let rendered =
            self.calvin_reply_payload(task, tid, CalvinReply::from(reply.clone()), &ts_installs);
        self.epoch_system_ms = prev_epoch_ms;
        match rendered {
            Ok(payload) => response.payload = Payload::from_vec(payload),
            Err(error) => response.error_code = Some(Box::new(error)),
        }
        response
    }
}
